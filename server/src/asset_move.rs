//! Durable NAS file moves scoped to the directory the user is browsing.
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
    pub source_path: String,
    pub parent_path: String,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: String,
    #[serde(rename = "assetIDs")]
    pub asset_ids: Vec<String>,
    pub source_path: String,
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
        crate::revisions::normalize_directory(&request.source_path)? == request.source_path
            && crate::revisions::normalize_directory(&request.parent_path)? == request.parent_path,
        error(422, "Paths must be canonical and absolute")
    );
    ensure!(
        request.source_path != request.parent_path,
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
struct FileMove {
    id: String,
    source: String,
    destination: String,
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
fn validate_directories(jobs: &Jobs, library: &str, request: &Request) -> Result<()> {
    let folders = jobs.folders()?;
    for path in [&request.source_path, &request.parent_path] {
        let canonical = jobs
            .validate_path(Path::new(path))
            .map_err(|e| error(422, format!("{e:#}")))?;
        ensure!(
            canonical == Path::new(path),
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
        let mut query=db.prepare("SELECT path FROM (SELECT path,asset_id FROM catalog_paths WHERE library_id=?1 UNION SELECT path,asset_id FROM catalog_version_paths WHERE library_id=?1 AND available=1) p WHERE asset_id=?2 AND NOT EXISTS(SELECT 1 FROM catalog_deprecated_files d WHERE d.library_id=?1 AND d.path=p.path)")?;
        for asset in &request.asset_ids {
            let active:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM catalog_assets WHERE library_id=?1 AND id=?2 AND trashed=0)",params![library,asset],|r|r.get(0))?;
            ensure!(active, error(409, "Photo is missing or in the recycle bin"));
            let candidates = query
                .query_map(params![library, asset], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let scoped: Vec<PathBuf> = candidates
                .into_iter()
                .map(PathBuf::from)
                .filter(|p| p.starts_with(&request.source_path))
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
    let originals = paths.clone();
    for path in &originals {
        for sidecar in crate::media::sidecars(path)? {
            paths.insert(sidecar.canonicalize()?);
        }
    }
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
    preflight(jobs, store, library, &result)?;
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
    let mut db = store.lock()?;
    let tx = db.transaction()?;
    for item in &pending {
        directory_move::catalog(&tx, library, &item.source, &item.destination)?;
    }
    tx.rollback()?;
    let mut db = jobs.db.lock().unwrap();
    let tx = db.transaction()?;
    for item in &pending {
        directory_move::tracking(&tx, library, &item.source, &item.destination)?;
    }
    tx.rollback()?;
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
        if let Err(failure) =
            directory_move::rename_exclusive(Path::new(&item.source), Path::new(&item.destination))
        {
            jobs.db
                .lock()
                .unwrap()
                .execute("DELETE FROM directory_moves WHERE id=?1", [&item.id])?;
            return Err(failure)
                .with_context(|| format!("move {} to {}", item.source, item.destination));
        }
        directory_move::reconcile(
            jobs,
            store,
            &item.id,
            library,
            &item.source,
            &item.destination,
        )
        .context("Photo moved on NAS; index reconciliation will resume on retry or restart")?;
    }
    Ok(())
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
            source_path: jobs.root().join("source").to_str().unwrap().into(),
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
    fn moves_scoped_files_sidecars_and_identity_without_other_aliases() -> Result<()> {
        let (_tmp, jobs, store, mut request) = setup()?;
        let source = Path::new(&request.source_path).join("nested/photo.jpg");
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
        let a = Path::new(&request.source_path).join("a.jpg");
        let b = Path::new(&request.source_path).join("b.jpg");
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
        let a = Path::new(&request.source_path).join("a.jpg");
        let b = Path::new(&request.source_path).join("b.jpg");
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
        let a = Path::new(&request.source_path).join("a.jpg");
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
        let nested = Path::new(&request.source_path).join("nested/a.jpg");
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
        let source_folder = jobs.add_folder("lib", &request.source_path)?;
        let target_folder = jobs.add_folder("lib", &request.parent_path)?;
        let source = Path::new(&request.source_path).join("photo.jpg");
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
