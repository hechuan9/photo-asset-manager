use crate::{
    jobs::{Job, Jobs},
    media::{self, MediaProcessor},
    previews::PreviewStorage,
    store::Store,
};
use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, UNIX_EPOCH},
};

/// Run on one dedicated blocking thread. A stopped in-flight job stays running so
/// Jobs::open can recover it after restart without claiming a partial scan succeeded.
pub fn run(
    store: Arc<Store>,
    jobs: Arc<Jobs>,
    previews: Arc<PreviewStorage>,
    stop: Arc<AtomicBool>,
    interval: Duration,
) -> Result<()> {
    MediaProcessor::new()
        .probe()
        .context("NAS media runtime unavailable")?;
    let interval = interval.max(Duration::from_secs(1));
    let mut next_scan = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        if Instant::now() >= next_scan {
            for folder in jobs.folders()?.into_iter().filter(|folder| folder.active) {
                // A concurrent stop-tracking request can legitimately win this race.
                if let Err(error) = jobs.enqueue_scan(&folder.id)
                    && jobs.is_active(&folder.id)?
                {
                    return Err(error).context("schedule tracked directory");
                }
            }
            next_scan = Instant::now() + interval;
        }
        if let Some(job) = jobs.claim_next()? {
            let result = run_one(&store, &jobs, &previews, &job, &stop);
            if stop.load(Ordering::Relaxed) {
                break;
            }
            match result {
                Ok(()) => jobs.finish(&job.id, None)?,
                Err(error) => {
                    let trace = format!("{error:#}");
                    tracing::error!(job_id = %job.id, error = %trace, "NAS scan failed");
                    jobs.finish(&job.id, Some(&trace))?;
                }
            }
        } else {
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    Ok(())
}

#[derive(Default)]
struct Progress {
    processed: i64,
    skipped: i64,
    failed: i64,
    last_error: Option<String>,
}

pub fn run_one(
    store: &Store,
    jobs: &Jobs,
    previews: &PreviewStorage,
    job: &Job,
    stop: &AtomicBool,
) -> Result<()> {
    if stopped(jobs, job, stop)? {
        return Ok(());
    }
    let root = jobs
        .validate_path(Path::new(&job.path))
        .context("validate tracked directory")?;
    let mut directories = vec![root.clone()];
    let mut progress = Progress::default();
    while let Some(directory) = directories.pop() {
        if stopped(jobs, job, stop)? {
            return Ok(());
        }
        jobs.validate_path(&directory)
            .with_context(|| format!("validate scan directory {}", directory.display()))?;
        for entry in fs::read_dir(&directory)
            .with_context(|| format!("read directory {}", directory.display()))?
        {
            if stopped(jobs, job, stop)? {
                return Ok(());
            }
            let entry = entry.with_context(|| format!("read entry in {}", directory.display()))?;
            let name = entry.file_name();
            if excluded(&name.to_string_lossy()) {
                continue;
            }
            let kind = entry
                .file_type()
                .with_context(|| format!("read file type {}", entry.path().display()))?;
            if kind.is_symlink() {
                continue;
            }
            let path = entry.path();
            if kind.is_dir() {
                directories.push(path);
                continue;
            }
            if !kind.is_file() || !media::is_photo(&path) {
                continue;
            }
            let path_text = path.to_str().context("original path must be UTF-8")?;
            publish(jobs, job, &progress, Some(path_text))?;
            match process_file(store, jobs, previews, job, &path, stop) {
                Ok(true) => progress.processed += 1,
                Ok(false) => progress.skipped += 1,
                Err(error) => {
                    let trace = format!("processing {}: {error:#}", path.display());
                    tracing::error!(job_id = %job.id, error = %trace, "NAS photo processing failed");
                    progress.failed += 1;
                    progress.last_error = Some(trace);
                }
            }
            publish(jobs, job, &progress, None)?;
        }
    }
    if stopped(jobs, job, stop)? {
        return Ok(());
    }
    store.mark_missing_under(
        &job.library_id,
        root.to_str().context("original path must be UTF-8")?,
    )?;
    if let Some(error) = progress.last_error {
        bail!("{} files failed; latest failure: {error}", progress.failed);
    }
    Ok(())
}

fn stopped(jobs: &Jobs, job: &Job, stop: &AtomicBool) -> Result<bool> {
    Ok(stop.load(Ordering::Relaxed) || !jobs.is_active(&job.folder_id)?)
}
fn excluded(name: &str) -> bool {
    name.starts_with('.') || name == "@eaDir" || name == "#recycle"
}
fn publish(jobs: &Jobs, job: &Job, progress: &Progress, path: Option<&str>) -> Result<()> {
    jobs.update_progress(
        &job.id,
        progress.processed,
        progress.skipped,
        progress.failed,
        path,
    )
}
fn file_stamp(path: &Path) -> Result<(i64, i64)> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("stat {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "original is no longer a regular file: {}",
        path.display()
    );
    let size = i64::try_from(metadata.len()).context("original size exceeds supported range")?;
    let mtime = i64::try_from(metadata.modified()?.duration_since(UNIX_EPOCH)?.as_nanos())
        .context("original modification time exceeds supported range")?;
    Ok((size, mtime))
}

fn process_file(
    store: &Store,
    jobs: &Jobs,
    previews: &PreviewStorage,
    job: &Job,
    path: &Path,
    stop: &AtomicBool,
) -> Result<bool> {
    jobs.validate_path(path.parent().context("original has no parent")?)?;
    let text = path.to_str().context("original path must be UTF-8")?;
    let (size, mtime) = file_stamp(path)?;
    let previous = jobs.file_state(&job.folder_id, text)?;
    let mut asset_id = previous
        .as_ref()
        .map(|p| p.asset_id.clone())
        .unwrap_or_default();
    let mut hash = previous
        .as_ref()
        .map(|p| p.version.clone())
        .unwrap_or_default();
    if let Some(previous) = &previous
        && previous.size == size
        && previous.mtime_ns == mtime
        && previous.error.is_none()
        && !previous.asset_id.is_empty()
        && store.original_is_online(&job.library_id, text, &previous.version)?
    {
        let asset = store.asset(&job.library_id, &previous.asset_id)?;
        if let Some(object) = asset["_preview"].get("objectRef")
            && previews.contains(object)?
        {
            return Ok(false);
        }
    }
    let result: Result<bool> = (|| {
        let processor = MediaProcessor::new();
        hash = media::sha256_file(path)?;
        let metadata = processor.extract(path)?;
        if stopped(jobs, job, stop)? {
            return Ok(false);
        }
        ensure!(
            file_stamp(path)? == (size, mtime),
            "original changed during metadata extraction"
        );
        let now = Utc::now().to_rfc3339();
        let mut snapshot = json!({
            "cameraMake": metadata.camera_make, "cameraModel": metadata.camera_model,
            "lensModel": metadata.lens_model, "originalFilename": path.file_name().context("original filename missing")?.to_str().context("original filename must be UTF-8")?,
            "contentFingerprint": hash, "metadataFingerprint": metadata.fingerprint(path),
            "rating": metadata.rating, "flagState":"unflagged", "tags":[], "createdAt":now, "updatedAt":now,
        });
        if let Some(capture_time) = metadata.capture_time {
            snapshot["captureTime"] = json!(capture_time);
        }
        let file = json!({"contentHash":hash,"sizeBytes":size,"role":if media::is_raw(path){"raw_original"}else{"jpeg_original"}});
        asset_id = store.ingest_original(&job.library_id, text, &file, &snapshot)?;
        // Imported historical previews already represent these originals. A new
        // worker index should not trigger a complete RAW decode of the library.
        if previous.as_ref().is_none_or(|state| state.version == hash) {
            let asset = store.asset(&job.library_id, &asset_id)?;
            if let Some(object) = asset["_preview"].get("objectRef")
                && previews.contains(object)?
            {
                return Ok(true);
            }
        }
        if stopped(jobs, job, stop)? {
            return Ok(false);
        }
        let scratch = tempfile::tempdir().context("create preview staging directory")?;
        let target: PathBuf = scratch.path().join("preview.heic");
        let generated = processor.generate_preview(path, &target)?;
        if stopped(jobs, job, stop)? {
            return Ok(false);
        }
        ensure!(
            file_stamp(path)? == (size, mtime),
            "original changed during preview generation"
        );
        let object =
            previews.put_generated(&job.library_id, &asset_id, &generated.sha256, &target)?;
        let derivative = json!({"assetID":asset_id,"role":"preview","fileObject":{"contentHash":generated.sha256,"sizeBytes":generated.size_bytes,"role":"preview"},"objectRef":object,"pixelSize":{"width":generated.width,"height":generated.height}});
        store.declare_generated_preview(&job.library_id, &asset_id, &derivative)?;
        Ok(true)
    })();
    match &result {
        Ok(true) => jobs.record_file(&job.folder_id, text, size, mtime, &asset_id, &hash, None)?,
        Err(error) => jobs.record_file(
            &job.folder_id,
            text,
            size,
            mtime,
            &asset_id,
            &hash,
            Some(&format!("{error:#}")),
        )?,
        Ok(false) => {}
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traversal_excludes_hidden_recycle_and_symlinks_without_touching_files() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let originals = directory.path().join("originals");
        fs::create_dir(&originals)?;
        for name in [".hidden", "@eaDir", "#recycle"] {
            fs::create_dir(originals.join(name))?;
            fs::write(
                originals.join(name).join("not-a-real-photo.jpg"),
                b"original",
            )?;
        }
        let outside = directory.path().join("outside.jpg");
        fs::write(&outside, b"outside")?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, originals.join("linked.jpg"))?;
        let store = Store::open(
            &directory.path().join("catalog.sqlite"),
            true,
            Default::default(),
        )?;
        let jobs = Jobs::open(&directory.path().join("jobs.sqlite"), &originals)?;
        let previews = PreviewStorage::new(
            &directory.path().join("keeps"),
            Some(&originals),
            "http://localhost:2283",
            "test-key",
        )?;
        let folder = jobs.add_folder("library", ".")?;
        jobs.enqueue_scan(&folder.id)?;
        let job = jobs.claim_next()?.unwrap();
        run_one(&store, &jobs, &previews, &job, &AtomicBool::new(false))?;
        assert_eq!(store.counts("library", false)?["all"], 0);
        assert_eq!(fs::read(outside)?, b"outside");
        for name in [".hidden", "@eaDir", "#recycle"] {
            assert_eq!(
                fs::read(originals.join(name).join("not-a-real-photo.jpg"))?,
                b"original"
            );
        }
        Ok(())
    }

    #[test]
    fn persisted_incremental_state_skips_unchanged_original_after_restart() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let originals = directory.path().join("originals");
        fs::create_dir(&originals)?;
        let original = originals.join("photo.jpg");
        fs::write(&original, b"original fixture deliberately not decodable")?;
        // The worker starts from Jobs' canonical root; seed exactly that path,
        // including macOS's /var -> /private/var resolution.
        let original = original.canonicalize()?;
        let path = original.to_str().unwrap();
        let store = Store::open(
            &directory.path().join("catalog.sqlite"),
            true,
            Default::default(),
        )?;
        let jobs_path = directory.path().join("jobs.sqlite");
        let jobs = Jobs::open(&jobs_path, &originals)?;
        let previews = PreviewStorage::new(
            &directory.path().join("keeps"),
            Some(&originals),
            "http://localhost:2283",
            "test-key",
        )?;
        let folder = jobs.add_folder("library", ".")?;
        let (size, mtime) = file_stamp(&original)?;
        let hash = media::sha256_file(&original)?;
        let file = json!({"contentHash":hash,"sizeBytes":size,"role":"jpeg_original"});
        let snapshot = json!({"cameraMake":"Test","cameraModel":"Test","lensModel":"Test","originalFilename":"photo.jpg","contentFingerprint":hash,"metadataFingerprint":"fixture","rating":0,"flagState":"unflagged","tags":[],"createdAt":"2024-01-01T00:00:00Z","updatedAt":"2024-01-01T00:00:00Z"});
        let id = store.ingest_original("library", path, &file, &snapshot)?;
        let fake_preview = directory.path().join("preview.heic");
        fs::write(&fake_preview, b"preview fixture")?;
        let object = previews.put_generated("library", &id, "preview-hash", &fake_preview)?;
        store.declare_generated_preview("library", &id, &json!({"assetID":id,"role":"preview","fileObject":{"contentHash":"preview-hash","sizeBytes":15,"role":"preview"},"objectRef":object,"pixelSize":{"width":100,"height":100}}))?;
        jobs.record_file(&folder.id, path, size, mtime, &id, &hash, None)?;
        jobs.enqueue_scan(&folder.id)?;
        let claimed = jobs.claim_next()?.unwrap();
        drop(jobs);
        let jobs = Jobs::open(&jobs_path, &originals)?;
        let recovered = jobs.claim_next()?.unwrap();
        assert_eq!(recovered.id, claimed.id);
        run_one(
            &store,
            &jobs,
            &previews,
            &recovered,
            &AtomicBool::new(false),
        )?;
        let progress = jobs.jobs(1)?.remove(0);
        assert_eq!(
            (progress.processed, progress.skipped, progress.failed),
            (0, 1, 0)
        );
        assert_eq!(media::sha256_file(&original)?, hash);
        Ok(())
    }

    #[test]
    fn stopped_job_does_not_inspect_media() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let originals = directory.path().join("originals");
        fs::create_dir(&originals)?;
        fs::write(originals.join("unprocessed.jpg"), b"original")?;
        let store = Store::open(
            &directory.path().join("catalog.sqlite"),
            true,
            Default::default(),
        )?;
        let jobs = Jobs::open(&directory.path().join("jobs.sqlite"), &originals)?;
        let previews = PreviewStorage::new(
            &directory.path().join("keeps"),
            Some(&originals),
            "http://localhost:2283",
            "test-key",
        )?;
        let folder = jobs.add_folder("library", ".")?;
        jobs.enqueue_scan(&folder.id)?;
        let job = jobs.claim_next()?.unwrap();
        jobs.remove_folder(&folder.id)?;
        run_one(&store, &jobs, &previews, &job, &AtomicBool::new(false))?;
        assert_eq!(jobs.jobs(1)?[0].status, "cancelled");
        assert_eq!(store.counts("library", false)?["all"], 0);
        assert_eq!(fs::read(originals.join("unprocessed.jpg"))?, b"original");
        Ok(())
    }
}
