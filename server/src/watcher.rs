//! Filesystem events are hints; durable scans and periodic reconciliation are authoritative.
use crate::{
    jobs::{Folder, Jobs},
    store::Store,
    worker::maintain_revisions,
};
use anyhow::{Context, Result};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

/// Run on a dedicated blocking thread. Periodic full reconciliation remains in the worker.
pub fn run(jobs: Arc<Jobs>, store: Arc<Store>, stop: Arc<AtomicBool>) -> Result<()> {
    let (sender, receiver) = mpsc::sync_channel(4096);
    let overflow = Arc::new(AtomicBool::new(false));
    let callback_overflow = overflow.clone();
    let mut watcher = notify::recommended_watcher(move |event| {
        if sender.try_send(event).is_err() {
            callback_overflow.store(true, Ordering::Relaxed);
        }
    })
    .context("initialize filesystem watcher")?;
    let mut watched = HashSet::new();
    let mut folders: Vec<Folder> = Vec::new();
    let mut structure_changed = true;
    let mut next_reconcile = Instant::now();
    let mut refresh = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        if Instant::now() >= refresh {
            maintain_revisions(&store, &jobs)?;
            let active: Vec<_> = jobs.folders()?.into_iter().filter(|f| f.active).collect();
            let changed = active.iter().map(|f| &f.id).collect::<Vec<_>>()
                != folders.iter().map(|f| &f.id).collect::<Vec<_>>();
            folders = active;
            if changed || structure_changed || Instant::now() >= next_reconcile {
                synchronize(&jobs, &mut watcher, &mut watched, &folders)?;
                structure_changed = false;
                next_reconcile = Instant::now() + Duration::from_secs(300);
            }
            refresh = Instant::now() + Duration::from_secs(5);
        }
        if overflow.swap(false, Ordering::Relaxed) {
            reconcile(&jobs, &folders)?;
        }
        match receiver.recv_timeout(Duration::from_millis(250)) {
            Ok(Ok(event)) => {
                if event.need_rescan() {
                    reconcile(&jobs, &folders)?;
                } else if !matches!(event.kind, EventKind::Access(_)) {
                    for path in &event.paths {
                        queue_path_kind(
                            &store,
                            &jobs,
                            &folders,
                            path,
                            matches!(
                                event.kind,
                                EventKind::Create(_)
                                    | EventKind::Remove(_)
                                    | EventKind::Modify(notify::event::ModifyKind::Name(_))
                            ),
                        )?;
                    }
                    if matches!(
                        event.kind,
                        EventKind::Create(_)
                            | EventKind::Remove(_)
                            | EventKind::Modify(notify::event::ModifyKind::Name(_))
                    ) && event
                        .paths
                        .iter()
                        .any(|path| watched.contains(path) || path.is_dir())
                    {
                        structure_changed = true;
                        refresh = Instant::now();
                    }
                }
            }
            Ok(Err(error)) => {
                tracing::error!(error = %error, "filesystem watch failed; scheduling reconciliation");
                reconcile(&jobs, &folders)?;
                structure_changed = true;
                refresh = Instant::now();
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(error) => return Err(error).context("filesystem watcher disconnected"),
        }
    }
    Ok(())
}

fn reconcile(jobs: &Jobs, folders: &[Folder]) -> Result<()> {
    for folder in folders {
        if let Err(error) = jobs.enqueue_external_scan(&folder.id)
            && jobs.is_active(&folder.id)?
        {
            tracing::error!(folder_id = %folder.id, error = %format!("{error:#}"), "could not queue reconciliation");
        }
    }
    Ok(())
}

fn enqueue_if_active(
    store: &Store,
    jobs: &Jobs,
    folder: &Folder,
    path: &Path,
    delay: i64,
    path_sync: bool,
) -> Result<()> {
    let queued = if path_sync {
        jobs.enqueue_path_change(&folder.id, path, delay)
    } else {
        jobs.enqueue_change(&folder.id, path, delay)
    };
    match queued {
        Ok(job) => store.note_revision_activity(&job.library_id, &job.id, &job.path)?,
        Err(error) if jobs.is_active(&folder.id)? => {
            // An unavailable NAS mount must not kill monitoring for healthy roots.
            tracing::error!(folder_id = %folder.id, error = %format!("{error:#}"), "could not queue filesystem change");
        }
        Err(_) => {}
    }
    Ok(())
}

#[cfg(test)]
fn queue_path(store: &Store, jobs: &Jobs, folders: &[Folder], path: &Path) -> Result<()> {
    queue_path_kind(store, jobs, folders, path, true)
}

fn queue_path_kind(
    store: &Store,
    jobs: &Jobs,
    folders: &[Folder],
    path: &Path,
    path_sync: bool,
) -> Result<()> {
    if excluded_path(jobs.root(), path) {
        return Ok(());
    }
    for folder in folders {
        let root = Path::new(&folder.path);
        if !path.starts_with(root) {
            continue;
        }
        // Scan the parent also for removals and renames, where the target no longer exists.
        let mut scope = if path == root {
            root
        } else {
            path.parent().unwrap_or(root)
        };
        while scope.starts_with(root) && !scope.is_dir() {
            scope = scope.parent().unwrap_or(root);
            if scope == root {
                break;
            }
        }
        enqueue_if_active(store, jobs, folder, scope, 2, path_sync)?;
    }
    Ok(())
}

fn excluded_path(root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return true;
    };
    let mut cursor = root.to_path_buf();
    for part in relative.components() {
        let std::path::Component::Normal(name) = part else {
            return true;
        };
        let name = name.to_string_lossy();
        if name.starts_with('.') || name == "@eaDir" || name == "#recycle" {
            return true;
        }
        cursor.push(part);
        if std::fs::symlink_metadata(&cursor).is_ok_and(|m| m.file_type().is_symlink()) {
            return true;
        }
    }
    false
}

fn synchronize(
    jobs: &Jobs,
    watcher: &mut RecommendedWatcher,
    watched: &mut HashSet<PathBuf>,
    folders: &[Folder],
) -> Result<()> {
    let mut desired = HashSet::new();
    let mut pending: Vec<PathBuf> = folders.iter().map(|f| PathBuf::from(&f.path)).collect();
    while let Some(path) = pending.pop() {
        if excluded_path(jobs.root(), &path) || !path.is_dir() || !desired.insert(path.clone()) {
            continue;
        }
        let entries = match std::fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) => {
                tracing::error!(path = %path.display(), error = %error, "cannot enumerate watch directories");
                continue;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "cannot read watch directory entry");
                    continue;
                }
            };
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => pending.push(entry.path()),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(path = %entry.path().display(), error = %error, "cannot inspect watch entry")
                }
            }
        }
    }
    for path in watched.difference(&desired).cloned().collect::<Vec<_>>() {
        if let Err(error) = watcher.unwatch(&path) {
            tracing::warn!(path = %path.display(), error = %error, "remove filesystem watch");
        }
        watched.remove(&path);
    }
    let newly_watched: HashSet<_> = desired.difference(watched).cloned().collect();
    for path in &newly_watched {
        match watcher.watch(path, RecursiveMode::NonRecursive) {
            Ok(()) => {
                watched.insert(path.clone());
                // Close the gap before a newly discovered directory obtained its watch.
                if !path
                    .parent()
                    .is_some_and(|parent| newly_watched.contains(parent))
                {
                    for folder in folders.iter().filter(|f| path.starts_with(&f.path)) {
                        if let Err(error) = (if path == Path::new(&folder.path) {
                            jobs.enqueue_external_scan(&folder.id)
                        } else {
                            jobs.enqueue_external_reconcile_scope(&folder.id, path)
                        }) && jobs.is_active(&folder.id)?
                        {
                            tracing::error!(path = %path.display(), error = %format!("{error:#}"), "could not queue new watch reconciliation");
                        }
                    }
                }
            }
            Err(error) => {
                tracing::error!(path = %path.display(), error = %error, "register filesystem watch; periodic reconciliation remains active")
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_paths_ignore_hidden_and_symlink_targets_and_queue_deleted_parent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("originals");
        std::fs::create_dir(&root).unwrap();
        let jobs = Jobs::open(&dir.path().join("jobs.sqlite"), &root).unwrap();
        let store = Store::open(&dir.path().join("catalog.sqlite"), true).unwrap();
        let folder = jobs.add_folder("library", ".").unwrap();
        let root = jobs.root().to_path_buf();
        queue_path(
            &store,
            &jobs,
            std::slice::from_ref(&folder),
            &root.join("@eaDir/a.jpg"),
        )
        .unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(dir.path(), root.join("link")).unwrap();
            queue_path(
                &store,
                &jobs,
                std::slice::from_ref(&folder),
                &root.join("link/outside.jpg"),
            )
            .unwrap();
        }
        assert!(jobs.jobs(10).unwrap().is_empty());
        queue_path(&store, &jobs, &[folder], &root.join("deleted.jpg")).unwrap();
        assert_eq!(jobs.jobs(10).unwrap()[0].path, root.to_str().unwrap());
        assert!(jobs.jobs(10).unwrap()[0].path_sync);
        assert!(
            jobs.path_sync_pending("library", &root.join("deleted.jpg"))
                .unwrap()
        );
    }

    #[test]
    fn metadata_event_does_not_mark_path_sync_but_rename_does() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("originals");
        std::fs::create_dir_all(root.join("destination")).unwrap();
        let jobs = Jobs::open(&dir.path().join("jobs.sqlite"), &root).unwrap();
        let store = Store::open(&dir.path().join("catalog.sqlite"), true).unwrap();
        let folder = jobs.add_folder("library", ".").unwrap();
        let target = jobs.root().join("destination/a.raw");
        queue_path_kind(&store, &jobs, std::slice::from_ref(&folder), &target, false).unwrap();
        assert!(!jobs.path_sync_pending("library", &target).unwrap());
        queue_path_kind(&store, &jobs, &[folder], &target, true).unwrap();
        assert!(jobs.path_sync_pending("library", &target).unwrap());
        assert!(jobs.claim_next().unwrap().is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_watcher_observes_file_written_after_start() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("originals");
        std::fs::create_dir(&root).unwrap();
        let jobs = Arc::new(Jobs::open(&dir.path().join("jobs.sqlite"), &root).unwrap());
        jobs.add_folder("library", ".").unwrap();
        let root = jobs.root().to_path_buf();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_jobs = jobs.clone();
        let worker_stop = stop.clone();
        let store = Arc::new(Store::open(&dir.path().join("catalog.sqlite"), true).unwrap());
        let handle = std::thread::spawn(move || run(worker_jobs, store, worker_stop));
        let deadline = Instant::now() + Duration::from_secs(10);
        while jobs.claim_next().unwrap().is_none() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(50));
        }
        let first = jobs.jobs(1).unwrap().remove(0);
        jobs.finish(&first.id, None).unwrap();
        std::fs::write(root.join("new.jpg"), b"sample").unwrap();
        while jobs
            .jobs(10)
            .unwrap()
            .iter()
            .all(|job| job.status == "completed")
        {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(50));
        }
        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap().unwrap();
    }
}
