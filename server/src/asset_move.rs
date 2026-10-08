//! Durable NAS file moves for selected assets, optionally scoped to a directory.
use crate::{
    directory_move,
    jobs::Jobs,
    store::{Store, StoreError},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
};

fn error(status: u16, message: impl Into<String>) -> anyhow::Error {
    StoreError {
        status,
        code: "asset_move_failed".into(),
        message: message.into(),
    }
    .into()
}
pub(crate) fn initialize(db: &Connection) -> Result<()> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS asset_move_tasks(id TEXT PRIMARY KEY,library_id TEXT NOT NULL,request TEXT NOT NULL,status TEXT NOT NULL,phase TEXT NOT NULL,error TEXT,plan TEXT,created_at INTEGER NOT NULL DEFAULT(unixepoch()),updated_at INTEGER NOT NULL DEFAULT(unixepoch()),finished_at INTEGER)")?;
    Ok(())
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    #[serde(rename = "requestID")]
    pub request_id: String,
    #[serde(rename = "assetIDs")]
    pub asset_ids: Vec<String>,
    pub source_path: Option<String>,
    pub parent_path: String,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: String,
    #[serde(rename = "assetIDs")]
    pub asset_ids: Vec<String>,
    pub source_path: Option<String>,
    pub parent_path: String,
    pub status: String,
    pub phase: String,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub finished_at: Option<i64>,
}
pub(crate) fn get(jobs: &Jobs, library: &str, id: &str) -> Result<Task> {
    let id = uuid::Uuid::parse_str(id)
        .map_err(|_| error(422, "requestID must be a UUID"))?
        .to_string();
    let db = jobs.trash_db.lock().unwrap();
    let row=db.query_row("SELECT request,status,phase,error,created_at,updated_at,finished_at FROM asset_move_tasks WHERE id=?1 AND library_id=?2",params![id,library],|r|Ok((r.get::<_,String>(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?.ok_or_else(||error(404,"Photo move task not found"))?;
    let request: Request = serde_json::from_str(&row.0)?;
    Ok(Task {
        id,
        asset_ids: request.asset_ids,
        source_path: request.source_path,
        parent_path: request.parent_path,
        status: row.1,
        phase: row.2,
        error: row.3,
        created_at: row.4,
        updated_at: row.5,
        finished_at: row.6,
    })
}
pub(crate) fn submit(jobs: &Jobs, library: &str, mut request: Request) -> Result<Task> {
    request.request_id = uuid::Uuid::parse_str(&request.request_id)
        .map_err(|_| error(422, "requestID must be a UUID"))?
        .to_string();
    request.asset_ids = request
        .asset_ids
        .iter()
        .map(|id| {
            uuid::Uuid::parse_str(id)
                .map(|id| id.to_string())
                .map_err(|_| error(422, "assetIDs must contain UUIDs"))
        })
        .collect::<Result<Vec<_>>>()?;
    request.asset_ids.sort();
    request.asset_ids.dedup();
    ensure!(
        !request.asset_ids.is_empty(),
        error(422, "Select at least one photo")
    );
    ensure!(
        request
            .source_path
            .as_ref()
            .map(|path| crate::revisions::normalize_directory(path)
                .map(|normalized| normalized == *path))
            .transpose()?
            .unwrap_or(true)
            && crate::revisions::normalize_directory(&request.parent_path)? == request.parent_path,
        error(422, "Paths must be canonical and absolute")
    );
    ensure!(
        request.source_path.as_ref() != Some(&request.parent_path),
        error(422, "Choose a different destination folder")
    );
    let encoded = serde_json::to_string(&request)?;
    {
        let db = jobs.trash_db.lock().unwrap();
        db.execute("INSERT OR IGNORE INTO asset_move_tasks(id,library_id,request,status,phase) VALUES(?1,?2,?3,'pending','waiting')",params![request.request_id,library,encoded])?;
        let previous: (String, String) = db.query_row(
            "SELECT library_id,request FROM asset_move_tasks WHERE id=?1",
            [&request.request_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            previous == (library.into(), encoded),
            error(409, "requestID belongs to another move request")
        );
        db.execute("UPDATE asset_move_tasks SET status='pending',phase='waiting',error=NULL,finished_at=NULL,updated_at=unixepoch() WHERE id=?1 AND status='failed'",[&request.request_id])?;
    }
    get(jobs, library, &request.request_id)
}
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct FileMove {
    pub(crate) id: String,
    pub(crate) source: String,
    pub(crate) destination: String,
}
fn transition(jobs: &Jobs, id: &str, phase: &str, message: Option<&str>) -> Result<()> {
    let status = match phase {
        "completed" => "completed",
        "failed" => "failed",
        _ => "running",
    };
    jobs.trash_db.lock().unwrap().execute("UPDATE asset_move_tasks SET status=?2,phase=?3,error=?4,updated_at=unixepoch(),finished_at=CASE WHEN ?2 IN ('completed','failed') THEN unixepoch() ELSE NULL END WHERE id=?1",params![id,status,phase,message])?;
    Ok(())
}
fn validate_tracked_directories(
    jobs: &Jobs,
    library: &str,
    paths: impl IntoIterator<Item = impl AsRef<Path>>,
) -> Result<()> {
    let folders = jobs.folders()?;
    let paths: BTreeSet<PathBuf> = paths
        .into_iter()
        .map(|path| path.as_ref().to_path_buf())
        .collect();
    for path in paths {
        let canonical = jobs
            .validate_path(&path)
            .map_err(|e| error(422, format!("{e:#}")))?;
        ensure!(
            canonical == path,
            error(422, "Paths must be canonical and absolute")
        );
        ensure!(
            folders
                .iter()
                .any(|f| f.active && f.library_id == library && canonical.starts_with(&f.path)),
            error(404, "Both directories must be tracked in this library")
        );
        ensure!(
            !folders.iter().any(|f| f.active
                && f.library_id != library
                && (canonical.starts_with(&f.path) || Path::new(&f.path).starts_with(&canonical))),
            error(409, "Directory overlaps another library")
        );
    }
    Ok(())
}
fn validate_directories(jobs: &Jobs, library: &str, request: &Request) -> Result<()> {
    validate_tracked_directories(
        jobs,
        library,
        request
            .source_path
            .iter()
            .chain(std::iter::once(&request.parent_path)),
    )?;
    let active_trash: bool = jobs.trash_db.lock().unwrap().query_row(
        "SELECT EXISTS(SELECT 1 FROM directory_trash_tasks WHERE status IN ('pending','running'))",
        [],
        |r| r.get(0),
    )?;
    ensure!(
        !active_trash,
        error(409, "Wait for active folder deletion to finish")
    );
    let active_import: bool = jobs.db.lock().unwrap().query_row(
        "SELECT EXISTS(SELECT 1 FROM imports WHERE json_extract(manifest,'$.finished')=0)",
        [],
        |r| r.get(0),
    )?;
    ensure!(
        !active_import,
        error(409, "Finish active imports before moving photos")
    );
    Ok(())
}
fn plan(jobs: &Jobs, store: &Store, library: &str, request: &Request) -> Result<Vec<FileMove>> {
    let mut paths = BTreeSet::<PathBuf>::new();
    {
        let db = store.lock()?;
        let mut query=db.prepare("SELECT path FROM (SELECT path FROM catalog_paths WHERE library_id=?1 AND asset_id=?2 UNION SELECT path FROM catalog_version_paths WHERE library_id=?1 AND asset_id=?2 AND available=1) p WHERE NOT EXISTS(SELECT 1 FROM catalog_deprecated_files d WHERE d.library_id=?1 AND d.path=p.path)")?;
        for asset in &request.asset_ids {
            let active:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM catalog_assets WHERE library_id=?1 AND id=?2 AND trashed=0)",params![library,asset],|r|r.get(0))?;
            ensure!(active, error(409, "Photo is missing or in the recycle bin"));
            let candidates = query
                .query_map(params![library, asset], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let scoped: Vec<PathBuf> = candidates
                .into_iter()
                .map(PathBuf::from)
                .filter(|p| {
                    request
                        .source_path
                        .as_ref()
                        .is_none_or(|source| p.starts_with(source))
                })
                .collect();
            ensure!(
                !scoped.is_empty(),
                error(
                    409,
                    format!("Photo {asset} is no longer in the source directory")
                )
            );
            paths.extend(scoped);
        }
    }
    validate_tracked_directories(jobs, library, paths.iter().filter_map(|path| path.parent()))?;
    let originals = paths.clone();
    for path in &originals {
        for sidecar in crate::media::sidecars(path)? {
            paths.insert(sidecar.canonicalize()?);
        }
    }
    validate_tracked_directories(jobs, library, paths.iter().filter_map(|path| path.parent()))?;
    // A shared stem XMP must stay with every photo that depends on it.
    let sidecars: HashSet<_> = paths
        .iter()
        .filter(|p| p.extension().is_some_and(|e| e.eq_ignore_ascii_case("xmp")))
        .cloned()
        .collect();
    let parents: BTreeSet<_> = originals.iter().filter_map(|p| p.parent()).collect();
    for parent in parents {
        for entry in fs::read_dir(parent)? {
            let path = entry?.path();
            if originals.contains(&path)
                || paths.contains(&path)
                || !path.is_file()
                || !(crate::media::is_photo(&path) || crate::media::is_video(&path))
            {
                continue;
            }
            ensure!(
                !crate::media::sidecars(&path)?
                    .iter()
                    .any(|p| p.canonicalize().is_ok_and(|p| sidecars.contains(&p))),
                error(
                    409,
                    "A selected photo shares an XMP sidecar with an unselected photo; select both photos"
                )
            );
        }
    }
    let mut targets = HashSet::new();
    let mut result = Vec::new();
    for source in paths {
        let destination =
            Path::new(&request.parent_path).join(source.file_name().context("file has no name")?);
        if source == destination {
            continue;
        }
        ensure!(
            targets.insert(destination.clone()),
            error(
                409,
                "Selected photos have duplicate filenames in the destination"
            )
        );
        result.push(FileMove {
            id: uuid::Uuid::new_v4().to_string(),
            source: source.to_str().context("path must be UTF-8")?.into(),
            destination: destination.to_str().context("path must be UTF-8")?.into(),
        });
    }
    ensure!(
        !result.is_empty(),
        error(409, "Selected photos are already in the destination")
    );
    Ok(result)
}
fn preflight(jobs: &Jobs, store: &Store, library: &str, plan: &[FileMove]) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let mut pending = Vec::new();
    for item in plan {
        let completed: bool = jobs.db.lock().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM directory_moves WHERE id=?1 AND completed=1)",
            [&item.id],
            |r| r.get(0),
        )?;
        if completed {
            continue;
        }
        pending.push(item);
        let source = Path::new(&item.source);
        ensure!(
            jobs.validate_path(source.parent().context("missing source parent")?)?
                == source.parent().unwrap(),
            error(422, "Source directory must be canonical")
        );
        let info = fs::symlink_metadata(source)
            .with_context(|| format!("inspect {}", source.display()))?;
        ensure!(
            info.file_type().is_file(),
            error(422, "Only regular files can be moved")
        );
        let destination = Path::new(&item.destination);
        ensure!(
            fs::symlink_metadata(destination)
                .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
            error(
                409,
                format!(
                    "Destination already exists or is inaccessible: {}",
                    destination.display()
                )
            )
        );
        ensure!(
            info.dev()
                == fs::metadata(destination.parent().context("missing destination parent")?)?.dev(),
            error(422, "Cross-filesystem photo moves are unsupported")
        );
    }
    validate_tracked_directories(
        jobs,
        library,
        pending
            .iter()
            .filter_map(|item| Path::new(&item.source).parent()),
    )?;
    let db = store.lock()?;
    for item in &pending {
        for table in [
            "catalog_paths",
            "catalog_version_paths",
            "catalog_deprecated_files",
        ] {
            let conflict: bool = db.query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE library_id=?1 AND path=?2)"),
                params![library, item.destination],
                |r| r.get(0),
            )?;
            ensure!(
                !conflict,
                error(
                    409,
                    format!("Destination is already indexed: {}", item.destination)
                )
            );
        }
    }
    drop(db);
    let db = jobs.db.lock().unwrap();
    for item in &pending {
        let conflict: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM files WHERE path=?2 AND folder_id IN (SELECT id FROM folders WHERE library_id=?1))", params![library,item.destination], |r| r.get(0))?;
        ensure!(
            !conflict,
            error(
                409,
                format!("Destination is already tracked: {}", item.destination)
            )
        );
    }
    Ok(())
}

const RELOCATE_CACHE: &str = "UPDATE media_cache SET standard=json_set(standard,'$.path',(SELECT destination FROM photo_move_paths WHERE source=json_extract(standard,'$.path'))) WHERE library_id=?1 AND asset_id IN (SELECT asset_id FROM photo_move_assets) AND json_extract(standard,'$.path') IN (SELECT source FROM photo_move_paths)";

fn mapping(db: &Connection, moves: &[FileMove]) -> Result<()> {
    db.execute_batch("CREATE TEMP TABLE photo_move_paths(id TEXT NOT NULL,source TEXT PRIMARY KEY,destination TEXT NOT NULL UNIQUE)")?;
    let mut insert = db.prepare("INSERT INTO photo_move_paths VALUES(?1,?2,?3)")?;
    for item in moves {
        insert.execute(params![item.id, item.source, item.destination])?;
    }
    Ok(())
}

pub(crate) fn reconcile_files(
    jobs: &Jobs,
    store: &Store,
    library: &str,
    moves: &[FileMove],
) -> Result<()> {
    if moves.is_empty() {
        return Ok(());
    }
    let parents: BTreeSet<_> = moves
        .iter()
        .flat_map(|m| {
            [
                Path::new(&m.source).parent(),
                Path::new(&m.destination).parent(),
            ]
        })
        .flatten()
        .collect();
    for parent in parents {
        fs::File::open(parent)?
            .sync_all()
            .with_context(|| format!("sync moved file parent {}", parent.display()))?;
    }
    {
        let mut db = store.lock()?;
        let tx = db.transaction()?;
        mapping(&tx, moves)?;
        // Restrict cache lookup to the moved identities, including destination paths on replay.
        tx.execute_batch("CREATE TEMP TABLE photo_move_assets(asset_id TEXT PRIMARY KEY)")?;
        for table in ["catalog_paths", "catalog_version_paths"] {
            tx.execute(&format!("INSERT OR IGNORE INTO photo_move_assets SELECT asset_id FROM {table} WHERE library_id=?1 AND path IN (SELECT source FROM photo_move_paths UNION ALL SELECT destination FROM photo_move_paths)"), [library])?;
        }
        tx.execute(RELOCATE_CACHE, [library])?;
        for (table, column) in [
            ("catalog_paths", "path"),
            ("catalog_version_paths", "path"),
            ("catalog_deprecated_files", "path"),
            ("catalog_deprecated_files", "retained_path"),
            ("remote_cache_tasks", "input_path"),
        ] {
            tx.execute(&format!("UPDATE {table} SET {column}=(SELECT destination FROM photo_move_paths WHERE source={table}.{column}) WHERE library_id=?1 AND {column} IN (SELECT source FROM photo_move_paths)"), [library])?;
        }
        tx.execute("INSERT OR IGNORE INTO catalog_revision_updates(library_id,path,owner,last_changed_at) SELECT r.library_id,m.destination,r.owner,r.last_changed_at FROM photo_move_paths m JOIN catalog_revision_updates r ON r.library_id=?1 AND r.path=m.source", [library])?;
        crate::revisions::flush(&tx)?;
        tx.execute_batch("DROP TABLE photo_move_assets; DROP TABLE photo_move_paths")?;
        tx.commit()?;
    }
    let mut db = jobs.db.lock().unwrap();
    let tx = db.transaction()?;
    mapping(&tx, moves)?;
    tx.execute("UPDATE files SET path=(SELECT destination FROM photo_move_paths WHERE source=files.path),folder_id=(SELECT id FROM folders WHERE library_id=?1 AND active=1 AND ((SELECT destination FROM photo_move_paths WHERE source=files.path)>=path || '/' AND (SELECT destination FROM photo_move_paths WHERE source=files.path)<path || '0') ORDER BY length(path) DESC LIMIT 1) WHERE folder_id IN (SELECT id FROM folders WHERE library_id=?1) AND path IN (SELECT source FROM photo_move_paths)", [library])?;
    for column in ["scope_path", "current_path"] {
        tx.execute(&format!("UPDATE jobs SET {column}=(SELECT destination FROM photo_move_paths WHERE source=jobs.{column}) WHERE folder_id IN (SELECT id FROM folders WHERE library_id=?1) AND {column} IN (SELECT source FROM photo_move_paths)"), [library])?;
    }
    tx.execute("UPDATE jobs SET checkpoint=NULL WHERE folder_id IN (SELECT id FROM folders WHERE library_id=?1) AND status IN ('pending','running')", [library])?;
    tx.execute("UPDATE worker_photo_files SET path=(SELECT destination FROM photo_move_paths WHERE source=worker_photo_files.path) WHERE path IN (SELECT source FROM photo_move_paths)", [])?;
    tx.execute("UPDATE worker_activity SET current_photo=(SELECT destination FROM photo_move_paths WHERE source=worker_activity.current_photo) WHERE library_id=?1 AND current_photo IN (SELECT source FROM photo_move_paths)", [library])?;
    tx.execute(
        "UPDATE directory_moves SET completed=1 WHERE id IN (SELECT id FROM photo_move_paths)",
        [],
    )?;
    tx.execute_batch("DROP TABLE photo_move_paths")?;
    tx.commit()?;
    Ok(())
}

fn execute(jobs: &Jobs, store: &Store, library: &str, request: &Request) -> Result<()> {
    let _guard = jobs.directory_mutation.write().unwrap();
    transition(jobs, &request.request_id, "validating", None)?;
    directory_move::recover_locked(jobs, store)?;
    validate_directories(jobs, library, request)?;
    let saved: Option<String> = jobs.trash_db.lock().unwrap().query_row(
        "SELECT plan FROM asset_move_tasks WHERE id=?1",
        [&request.request_id],
        |r| r.get(0),
    )?;
    let moves: Vec<FileMove> = match saved {
        Some(saved) => serde_json::from_str(&saved)?,
        None => {
            let moves = plan(jobs, store, library, request)?;
            jobs.trash_db.lock().unwrap().execute(
                "UPDATE asset_move_tasks SET plan=?2 WHERE id=?1",
                params![request.request_id, serde_json::to_string(&moves)?],
            )?;
            moves
        }
    };
    preflight(jobs, store, library, &moves)?;
    transition(jobs, &request.request_id, "moving", None)?;
    move_files(jobs, store, library, &request.request_id, moves)
}

fn move_files(
    jobs: &Jobs,
    store: &Store,
    library: &str,
    task: &str,
    moves: Vec<FileMove>,
) -> Result<()> {
    let mut moved = Vec::new();
    let movement = (|| -> Result<()> {
        for item in moves {
            let completed: bool = jobs.db.lock().unwrap().query_row(
                "SELECT EXISTS(SELECT 1 FROM directory_moves WHERE id=?1 AND completed=1)",
                [&item.id],
                |r| r.get(0),
            )?;
            if completed {
                continue;
            }
            jobs.db.lock().unwrap().execute(
                "INSERT INTO directory_moves(id,library_id,source,destination) VALUES(?1,?2,?3,?4)",
                params![item.id, library, item.source, item.destination],
            )?;
            if let Err(failure) = directory_move::rename_exclusive(
                Path::new(&item.source),
                Path::new(&item.destination),
            ) {
                jobs.db
                    .lock()
                    .unwrap()
                    .execute("DELETE FROM directory_moves WHERE id=?1", [&item.id])?;
                return Err(failure)
                    .with_context(|| format!("move {} to {}", item.source, item.destination));
            }
            moved.push(item);
        }
        Ok(())
    })();
    transition(jobs, task, "catalog", None)?;
    let reconciliation = reconcile_files(jobs, store, library, &moved)
        .context("Photos moved on NAS; index reconciliation will resume on retry or restart");
    if let Err(failure) = movement {
        return match reconciliation {
            Ok(()) => Err(failure),
            Err(recovery) => {
                Err(failure).context(format!("Index reconciliation also failed: {recovery:#}"))
            }
        };
    }
    reconciliation
}

pub(crate) fn process_next(jobs: &Jobs, store: &Store) -> Result<bool> {
    let next:Option<(String,String)>=jobs.trash_db.lock().unwrap().query_row("SELECT library_id,request FROM asset_move_tasks WHERE status IN ('pending','running') ORDER BY created_at,id LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((library, encoded)) = next else {
        return Ok(false);
    };
    let request: Request = serde_json::from_str(&encoded)?;
    match execute(jobs, store, &library, &request) {
        Ok(()) => transition(jobs, &request.request_id, "completed", None)?,
        Err(failure) => {
            tracing::error!(id=%request.request_id,error=%format!("{failure:#}"),"Photo move task failed");
            transition(
                jobs,
                &request.request_id,
                "failed",
                Some(&format!("{failure:#}")),
            )?;
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn setup() -> Result<(tempfile::TempDir, Jobs, Store, Request)> {
        let tmp = tempfile::tempdir()?;
        let root = tmp.path().join("originals");
        fs::create_dir_all(root.join("source/nested"))?;
        fs::create_dir(root.join("target"))?;
        let jobs = Jobs::open(&tmp.path().join("jobs.sqlite"), &root)?;
        jobs.add_folder("lib", jobs.root().to_str().unwrap())?;
        let store = Store::open(&tmp.path().join("catalog.sqlite"), true)?;
        let request = Request {
            request_id: uuid::Uuid::new_v4().to_string(),
            asset_ids: vec![],
            source_path: Some(jobs.root().join("source").to_str().unwrap().into()),
            parent_path: jobs.root().join("target").to_str().unwrap().into(),
        };
        Ok((tmp, jobs, store, request))
    }
    fn ingest(store: &Store, path: &Path, hash: &str) -> Result<String> {
        fs::write(path, b"original bytes")?;
        let identity: Option<String> = store
            .lock()?
            .query_row(
                "SELECT asset_id FROM catalog_paths WHERE content_hash=?1 LIMIT 1",
                [hash],
                |r| r.get(0),
            )
            .optional()?;
        store.ingest_original("lib",path.to_str().unwrap(),&json!({"contentHash":hash,"sizeBytes":14,"role":"jpeg_original"}),&json!({"originalDocumentID":identity,"contentFingerprint":hash,"metadataFingerprint":"meta","createdAt":"2024-01-01T00:00:00Z","updatedAt":"2024-01-01T00:00:00Z","originalFilename":path.file_name().unwrap().to_str().unwrap(),"rating":3,"flagState":"unflagged","tags":["travel"]}))?;
        Ok(store.lock()?.query_row(
            "SELECT asset_id FROM catalog_paths WHERE path=?1",
            [path.to_str().unwrap()],
            |r| r.get(0),
        )?)
    }
    #[test]
    fn optional_source_preserves_old_requests_and_accepts_omission() -> Result<()> {
        let (_tmp, _jobs, _store, request) = setup()?;
        let mut json = serde_json::to_value(&request)?;
        assert_eq!(serde_json::from_value::<Request>(json.clone())?, request);
        json.as_object_mut().unwrap().remove("sourcePath");
        assert!(
            serde_json::from_value::<Request>(json)?
                .source_path
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn all_photos_moves_multiple_sources_and_leaves_existing_target_alias() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        let first = jobs.root().join("source/a.jpg");
        let second = jobs.root().join("source/nested/b.jpg");
        let existing = jobs.root().join("target/existing.jpg");
        request.asset_ids = vec![ingest(&store, &first, "a")?, ingest(&store, &second, "b")?];
        ingest(&store, &existing, "a")?;
        fs::write(first.with_extension("xmp"), b"sidecar")?;
        request.source_path = None;
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        let task = get(&jobs, "lib", &request.request_id)?;
        assert_eq!(task.status, "completed", "{task:?}");
        assert!(task.source_path.is_none());
        assert!(existing.exists());
        assert!(jobs.root().join("target/a.jpg").exists());
        assert!(jobs.root().join("target/a.xmp").exists());
        assert!(jobs.root().join("target/b.jpg").exists());
        assert!(!first.exists() && !second.exists());
        Ok(())
    }

    #[test]
    fn all_photos_rejects_untracked_and_other_library_sources() -> Result<()> {
        for other_library in [false, true] {
            let (_tmp, jobs, store, mut request) = setup()?;
            jobs.db
                .lock()
                .unwrap()
                .execute("UPDATE folders SET active=0", [])?;
            jobs.add_folder("lib", &request.parent_path)?;
            let source = jobs.root().join("source/photo.jpg");
            request.asset_ids = vec![ingest(&store, &source, "photo")?];
            if other_library {
                jobs.add_folder("other", source.parent().unwrap().to_str().unwrap())?;
            }
            request.source_path = None;
            submit(&jobs, "lib", request.clone())?;
            process_next(&jobs, &store)?;
            let task = get(&jobs, "lib", &request.request_id)?;
            assert_eq!(task.status, "failed", "{task:?}");
            assert!(task.error.unwrap().contains("tracked in this library"));
            assert!(source.exists());
            assert!(!jobs.root().join("target/photo.jpg").exists());
        }
        Ok(())
    }

    #[test]
    fn all_photos_rejects_cross_source_filename_collision_before_move() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        let first = jobs.root().join("source/a.jpg");
        let second = jobs.root().join("source/nested/a.jpg");
        request.asset_ids = vec![ingest(&store, &first, "a")?, ingest(&store, &second, "b")?];
        request.source_path = None;
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        assert!(
            get(&jobs, "lib", &request.request_id)?
                .error
                .unwrap()
                .contains("duplicate filenames")
        );
        assert!(first.exists() && second.exists());
        Ok(())
    }

    #[test]
    fn planning_does_not_simulate_catalog_writes() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        let source = Path::new(request.source_path.as_deref().unwrap()).join("photo.jpg");
        request.asset_ids = vec![ingest(&store, &source, "planning")?];
        store.lock()?.execute_batch("CREATE TRIGGER reject_simulated_move BEFORE UPDATE OF path ON catalog_paths BEGIN SELECT RAISE(ABORT,'preflight must be read only'); END")?;
        assert_eq!(plan(&jobs, &store, "lib", &request)?.len(), 1);
        Ok(())
    }

    #[test]
    fn batch_preserves_cache_and_publishes_one_revision() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        for name in ["a.jpg", "b.jpg"] {
            let source = Path::new(request.source_path.as_deref().unwrap()).join(name);
            let id = ingest(&store, &source, name)?;
            store.lock()?.execute("INSERT INTO media_cache(library_id,asset_id,source_hash,spec,status,thumbnail,standard) VALUES('lib',?1,?2,'spec','ready',?3,?4) ON CONFLICT(library_id,asset_id) DO UPDATE SET thumbnail=excluded.thumbnail,standard=excluded.standard", params![id,name,json!({"objectRef":{"key":"existing-thumbnail"}}).to_string(),json!({"path":source,"other":"preserved"}).to_string()])?;
            request.asset_ids.push(id);
        }
        let before: i64 = store.lock()?.query_row(
            "SELECT revision FROM catalog_version_revision WHERE library_id='lib'",
            [],
            |r| r.get(0),
        )?;
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        assert_eq!(get(&jobs, "lib", &request.request_id)?.status, "completed");
        let db = store.lock()?;
        assert_eq!(
            db.query_row(
                "SELECT revision FROM catalog_version_revision WHERE library_id='lib'",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            before + 1
        );
        let rows = db
            .prepare("SELECT standard,thumbnail FROM media_cache WHERE library_id='lib'")?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (standard, thumbnail) in rows {
            let standard: serde_json::Value = serde_json::from_str(&standard)?;
            assert!(
                Path::new(standard["path"].as_str().unwrap()).starts_with(&request.parent_path)
            );
            assert_eq!(standard["other"], "preserved");
            assert!(thumbnail.contains("existing-thumbnail"));
        }
        Ok(())
    }

    #[test]
    fn indexed_destination_conflict_prevents_all_renames() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        let source = Path::new(request.source_path.as_deref().unwrap()).join("photo.jpg");
        request.asset_ids.push(ingest(&store, &source, "source")?);
        let destination = Path::new(&request.parent_path).join("photo.jpg");
        ingest(&store, &destination, "stale")?;
        fs::rename(&destination, destination.with_extension("backup"))?;
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        assert!(
            get(&jobs, "lib", &request.request_id)?
                .error
                .unwrap()
                .contains("already indexed")
        );
        assert!(source.exists());
        Ok(())
    }

    #[test]
    fn cache_relocation_work_is_independent_of_unrelated_library_size() -> Result<()> {
        let (_tmp, _jobs, store, _) = setup()?;
        let db = store.lock()?;
        db.execute_batch("CREATE TEMP TABLE photo_move_assets(asset_id TEXT PRIMARY KEY); INSERT INTO photo_move_assets VALUES('moved'); WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<128000) INSERT INTO media_cache(library_id,asset_id,source_hash,spec,standard) SELECT 'lib','unrelated-'||i,'hash','spec','{}' FROM n; INSERT INTO media_cache(library_id,asset_id,source_hash,spec,standard) VALUES('lib','moved','hash','spec','{\"path\":\"/source/a.jpg\"}')")?;
        mapping(
            &db,
            &[FileMove {
                id: "move".into(),
                source: "/source/a.jpg".into(),
                destination: "/target/a.jpg".into(),
            }],
        )?;
        let mut statement = db.prepare(RELOCATE_CACHE)?;
        assert_eq!(statement.execute(["lib"])?, 1);
        assert!(
            statement.get_status(rusqlite::StatementStatus::VmStep) < 1000,
            "cache relocation must seek moved asset IDs rather than scan the library"
        );
        Ok(())
    }

    #[test]
    fn recovery_replays_after_catalog_commit_without_losing_cache_path() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        let source = Path::new(request.source_path.as_deref().unwrap()).join("photo.jpg");
        request.asset_ids = vec![ingest(&store, &source, "recover")?];
        submit(&jobs, "lib", request.clone())?;
        jobs.db.lock().unwrap().execute_batch("CREATE TRIGGER interrupt_reconcile BEFORE UPDATE OF completed ON directory_moves BEGIN SELECT RAISE(ABORT,'simulate tracking commit failure'); END")?;
        process_next(&jobs, &store)?;
        assert_eq!(get(&jobs, "lib", &request.request_id)?.status, "failed");
        assert!(!source.exists());
        assert!(Path::new(&request.parent_path).join("photo.jpg").exists());
        jobs.db
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER interrupt_reconcile")?;
        directory_move::recover(&jobs, &store)?;
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        assert_eq!(get(&jobs, "lib", &request.request_id)?.status, "completed");
        directory_move::ensure_reconciled(&jobs)?;
        Ok(())
    }

    #[test]
    fn late_conflict_reconciles_already_moved_files() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        for name in ["a.jpg", "b.jpg"] {
            request.asset_ids.push(ingest(
                &store,
                &Path::new(request.source_path.as_deref().unwrap()).join(name),
                name,
            )?);
        }
        submit(&jobs, "lib", request.clone())?;
        let moves = plan(&jobs, &store, "lib", &request)?;
        preflight(&jobs, &store, "lib", &moves)?;
        fs::write(&moves[1].destination, b"late conflict")?;
        let first_destination = moves[0].destination.clone();
        assert!(move_files(&jobs, &store, "lib", &request.request_id, moves).is_err());
        directory_move::ensure_reconciled(&jobs)?;
        assert!(
            Path::new(request.source_path.as_deref().unwrap())
                .join("b.jpg")
                .exists()
        );
        assert_eq!(
            fs::read(Path::new(&request.parent_path).join("b.jpg"))?,
            b"late conflict"
        );
        assert!(store.lock()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM catalog_paths WHERE library_id='lib' AND path=?1)",
            [first_destination],
            |r| r.get::<_, bool>(0)
        )?);
        Ok(())
    }

    #[test]
    fn moves_scoped_files_sidecars_and_identity_without_other_aliases() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        let source = Path::new(request.source_path.as_deref().unwrap()).join("nested/photo.jpg");
        let id = ingest(&store, &source, "same")?;
        let outside = jobs.root().join("outside.jpg");
        assert_eq!(ingest(&store, &outside, "same")?, id);
        fs::write(source.with_extension("jpg.xmp"), b"identity metadata")?;
        let snapshot: String = store.lock()?.query_row(
            "SELECT snapshot FROM catalog_assets WHERE id=?1",
            [&id],
            |r| r.get(0),
        )?;
        request.asset_ids = vec![id.to_uppercase()];
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        assert_eq!(
            get(&jobs, "lib", &request.request_id)?.status,
            "completed",
            "{:?}",
            get(&jobs, "lib", &request.request_id)?
        );
        assert_eq!(
            fs::read(Path::new(&request.parent_path).join("photo.jpg"))?,
            b"original bytes"
        );
        assert_eq!(
            fs::read(Path::new(&request.parent_path).join("photo.jpg.xmp"))?,
            b"identity metadata"
        );
        assert!(!source.exists());
        assert!(outside.exists());
        let db = store.lock()?;
        let asset: String = db.query_row(
            "SELECT asset_id FROM catalog_paths WHERE path=?1",
            [Path::new(&request.parent_path)
                .join("photo.jpg")
                .to_str()
                .unwrap()],
            |r| r.get(0),
        )?;
        assert_eq!(asset, id);
        assert_eq!(
            db.query_row(
                "SELECT snapshot FROM catalog_assets WHERE id=?1",
                [&id],
                |r| r.get::<_, String>(0)
            )?,
            snapshot
        );
        assert_eq!(db.query_row("SELECT count(*) FROM catalog_version_paths WHERE asset_id=?1 AND available=1 AND path=?2",params![id,Path::new(&request.parent_path).join("photo.jpg").to_str().unwrap()],|r|r.get::<_,i64>(0))?,1);
        drop(db);
        assert_eq!(submit(&jobs, "lib", request.clone())?.status, "completed");
        assert!(get(&jobs, "other", &request.request_id).is_err());
        Ok(())
    }
    #[test]
    fn collision_is_preflighted_before_any_file_moves_and_retry_recovers() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        let a = Path::new(request.source_path.as_deref().unwrap()).join("a.jpg");
        let b = Path::new(request.source_path.as_deref().unwrap()).join("b.jpg");
        request.asset_ids = vec![ingest(&store, &a, "a")?, ingest(&store, &b, "b")?];
        let collision = Path::new(&request.parent_path).join("b.jpg");
        fs::write(&collision, b"existing")?;
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        assert_eq!(get(&jobs, "lib", &request.request_id)?.status, "failed");
        assert!(a.exists() && b.exists());
        assert_eq!(fs::read(&collision)?, b"existing");
        // Move the fixture conflict aside, just as a user would choose another name.
        fs::rename(&collision, collision.with_extension("backup"))?;
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        assert_eq!(
            get(&jobs, "lib", &request.request_id)?.status,
            "completed",
            "{:?}",
            get(&jobs, "lib", &request.request_id)?
        );
        Ok(())
    }
    #[test]
    fn interrupted_batch_resumes_after_one_rename_and_reopen() -> Result<()> {
        let (tmp, jobs, store, mut request) = setup()?;
        let a = Path::new(request.source_path.as_deref().unwrap()).join("a.jpg");
        let b = Path::new(request.source_path.as_deref().unwrap()).join("b.jpg");
        request.asset_ids = vec![ingest(&store, &a, "a")?, ingest(&store, &b, "b")?];
        submit(&jobs, "lib", request.clone())?;
        let moves = plan(&jobs, &store, "lib", &request)?;
        jobs.trash_db.lock().unwrap().execute(
            "UPDATE asset_move_tasks SET status='running',plan=?2 WHERE id=?1",
            params![request.request_id, serde_json::to_string(&moves)?],
        )?;
        let first = &moves[0];
        jobs.db.lock().unwrap().execute(
            "INSERT INTO directory_moves(id,library_id,source,destination) VALUES(?1,'lib',?2,?3)",
            params![first.id, first.source, first.destination],
        )?;
        directory_move::rename_exclusive(Path::new(&first.source), Path::new(&first.destination))?;
        drop(jobs);
        let jobs = Jobs::open(
            &tmp.path().join("jobs.sqlite"),
            &tmp.path().join("originals"),
        )?;
        directory_move::recover(&jobs, &store)?;
        process_next(&jobs, &store)?;
        assert_eq!(
            get(&jobs, "lib", &request.request_id)?.status,
            "completed",
            "{:?}",
            get(&jobs, "lib", &request.request_id)?
        );
        assert!(
            moves
                .iter()
                .all(|m| Path::new(&m.destination).exists() && !Path::new(&m.source).exists())
        );
        Ok(())
    }
    #[test]
    fn shared_sidecar_and_duplicate_flattened_names_are_rejected() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        let a = Path::new(request.source_path.as_deref().unwrap()).join("a.jpg");
        request.asset_ids = vec![ingest(&store, &a, "a")?];
        fs::write(a.with_extension("raw"), b"unselected")?;
        fs::write(a.with_extension("xmp"), b"shared")?;
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        assert!(
            get(&jobs, "lib", &request.request_id)?
                .error
                .unwrap()
                .contains("shares an XMP")
        );
        assert!(a.exists() && a.with_extension("xmp").exists());
        let nested = Path::new(request.source_path.as_deref().unwrap()).join("nested/a.jpg");
        request.asset_ids.push(ingest(&store, &nested, "b")?);
        request
            .asset_ids
            .push(ingest(&store, &a.with_extension("raw"), "c")?);
        request.request_id = uuid::Uuid::new_v4().to_string();
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        assert!(
            get(&jobs, "lib", &request.request_id)?
                .error
                .unwrap()
                .contains("duplicate filenames")
        );
        assert!(a.exists() && nested.exists());
        Ok(())
    }
    #[test]
    fn rebinds_file_tracking_to_destination_root() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        let source_folder = jobs.add_folder("lib", request.source_path.as_deref().unwrap())?;
        let target_folder = jobs.add_folder("lib", &request.parent_path)?;
        let source = Path::new(request.source_path.as_deref().unwrap()).join("photo.jpg");
        let id = ingest(&store, &source, "tracking")?;
        jobs.record_file(
            &source_folder.id,
            source.to_str().unwrap(),
            14,
            100,
            &id,
            "tracking",
            None,
        )?;
        request.asset_ids = vec![id.clone()];
        submit(&jobs, "lib", request.clone())?;
        process_next(&jobs, &store)?;
        assert_eq!(get(&jobs, "lib", &request.request_id)?.status, "completed");
        let destination = Path::new(&request.parent_path).join("photo.jpg");
        assert!(
            jobs.file_state(&source_folder.id, destination.to_str().unwrap())?
                .is_none()
        );
        assert_eq!(
            jobs.file_state(&target_folder.id, destination.to_str().unwrap())?
                .unwrap()
                .asset_id,
            id
        );
        assert_eq!(ingest(&store, &destination, "tracking")?, id);
        Ok(())
    }
}
