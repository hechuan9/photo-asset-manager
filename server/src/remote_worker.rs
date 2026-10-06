use crate::{
    api::{ApiError, AppState},
    cache_pipeline as cache,
    media::{self, MediaProcessor},
    store::Store,
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{fs, path::PathBuf, sync::Arc};
const LEASE: i64 = 1800;
static COMMIT: std::sync::Mutex<()> = std::sync::Mutex::new(());
pub fn migrate(db: &Connection) -> Result<()> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS remote_cache_tasks(id TEXT PRIMARY KEY,library_id TEXT NOT NULL,asset_id TEXT NOT NULL,source_hash TEXT NOT NULL,input_hash TEXT NOT NULL,input_path TEXT NOT NULL,worker_id TEXT NOT NULL,expires_at INTEGER NOT NULL,completed INTEGER NOT NULL DEFAULT 0); CREATE INDEX IF NOT EXISTS remote_cache_expiry ON remote_cache_tasks(completed,expires_at);")?;
    Ok(())
}
pub fn expire(db: &Connection) -> Result<()> {
    db.execute("UPDATE media_cache SET status=CASE WHEN attempts>=4 THEN 'failed' ELSE 'pending' END,available_at=unixepoch()+60,last_error='remote worker lease expired' WHERE status='processing' AND EXISTS(SELECT 1 FROM remote_cache_tasks r WHERE r.library_id=media_cache.library_id AND r.asset_id=media_cache.asset_id AND r.completed=0 AND r.expires_at<=unixepoch())",[])?;
    db.execute(
        "UPDATE remote_cache_tasks SET completed=-1 WHERE completed=0 AND expires_at<=unixepoch()",
        [],
    )?;
    Ok(())
}
#[derive(Clone)]
struct Task {
    id: String,
    asset: String,
    hash: String,
    input_hash: String,
    path: PathBuf,
    completed: i64,
}
fn conflict() -> anyhow::Error {
    crate::store::StoreError {
        status: 409,
        code: "worker_lease_expired".into(),
        message: "worker lease expired or task changed".into(),
    }
    .into()
}
fn task(store: &Store, library: &str, id: &str) -> Result<Task> {
    let db = store.lock()?;
    let row=db.query_row("SELECT asset_id,source_hash,input_hash,input_path,completed,expires_at FROM remote_cache_tasks WHERE id=? AND library_id=?",params![id,library],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,i64>(4)?,r.get::<_,i64>(5)?))).optional()?.ok_or_else(conflict)?;
    if row.4 != 1 && (row.4 != 0 || row.5 <= chrono::Utc::now().timestamp()) {
        return Err(conflict());
    }
    let state = if row.4 == 1 { "ready" } else { "processing" };
    let valid: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM media_cache WHERE library_id=? AND asset_id=? AND source_hash=? AND spec=? AND status=?)",
        params![library,row.0,row.1,cache::spec()?,state], |r|r.get(0))?;
    ensure!(valid, conflict());
    Ok(Task {
        id: id.into(),
        asset: row.0,
        hash: row.1,
        input_hash: row.2,
        path: row.3.into(),
        completed: row.4,
    })
}
fn scratch(state: &AppState, t: &Task) -> Result<PathBuf> {
    let root = PathBuf::from(state.store.lock()?.path().context("database path")?)
        .parent()
        .context("database parent")?
        .parent()
        .context("keeps root")?
        .join("tmp")
        .join("remote-worker")
        .join(&t.id);
    fs::create_dir_all(&root)?;
    Ok(root)
}
fn cleanup_expired(state: &AppState) -> Result<()> {
    let ids = {
        let db = state.store.lock()?;
        db.prepare("SELECT id FROM remote_cache_tasks WHERE completed=-1 LIMIT 20")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    for id in ids {
        let t = Task {
            id: id.clone(),
            asset: String::new(),
            hash: String::new(),
            input_hash: String::new(),
            path: PathBuf::new(),
            completed: -1,
        };
        fs::remove_dir_all(scratch(state, &t)?)?;
        state.store.lock()?.execute(
            "UPDATE remote_cache_tasks SET completed=-2 WHERE id=? AND completed=-1",
            [id],
        )?;
    }
    Ok(())
}
fn validate(state: &AppState, t: &Task) -> Result<()> {
    state
        .jobs
        .validate_path(t.path.parent().context("source parent")?)?;
    ensure!(
        !fs::symlink_metadata(&t.path)?.file_type().is_symlink(),
        "source symlink forbidden"
    );
    ensure!(
        media::sha256_file(&t.path)? == t.input_hash,
        "source changed"
    );
    Ok(())
}
fn needs_standard(t: &Task) -> bool {
    media::is_raw(&t.path) && !deferred(t)
}
fn deferred(t: &Task) -> bool {
    t.path
        .extension()
        .and_then(|v| v.to_str())
        .is_some_and(|v| v.eq_ignore_ascii_case("3fr"))
}
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/libraries/{library}/worker/claim", post(claim))
        .route("/libraries/{library}/worker/tasks/{id}/source", get(source))
        .route(
            "/libraries/{library}/worker/tasks/{id}/heartbeat",
            post(heartbeat),
        )
        .route(
            "/libraries/{library}/worker/tasks/{id}/thumbnail",
            put(thumbnail).layer(DefaultBodyLimit::max(256 * 1024 * 1024)),
        )
        .route(
            "/libraries/{library}/worker/tasks/{id}/standard",
            put(standard).layer(DefaultBodyLimit::max(256 * 1024 * 1024)),
        )
        .route(
            "/libraries/{library}/worker/tasks/{id}/complete",
            post(complete),
        )
        .route("/libraries/{library}/worker/tasks/{id}/fail", post(fail))
}
async fn run<F>(f: F) -> Result<Json<Value>, ApiError>
where
    F: FnOnce() -> Result<Value> + Send + 'static,
{
    Ok(Json(
        tokio::task::spawn_blocking(f)
            .await
            .map_err(anyhow::Error::from)??,
    ))
}
async fn claim(
    State(s): State<Arc<AppState>>,
    Path(lib): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    run(move || {
        let enabled = std::env::var("KEEPS_REMOTE_WORKER_ENABLED").ok();
        claim_sync(&s, &lib, body, enabled.as_deref())
    })
    .await
}
fn claim_pending(
    store: &Store,
    lib: &str,
    worker: &str,
) -> Result<Option<(String, String, String)>> {
    let mut db = store.lock()?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    expire(&tx)?;
    let row = tx.query_row("SELECT asset_id,source_hash FROM media_cache WHERE library_id=? AND status='pending' AND available_at<=unixepoch() AND EXISTS(SELECT 1 FROM catalog_assets a WHERE a.library_id=media_cache.library_id AND a.id=media_cache.asset_id AND a.trashed=0) LIMIT 1", [lib], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))).optional()?;
    let Some((asset, hash)) = row else {
        tx.commit()?;
        return Ok(None);
    };
    let id = uuid::Uuid::new_v4().to_string();
    tx.execute("UPDATE media_cache SET status='processing',attempts=attempts+1,updated_at=unixepoch() WHERE library_id=? AND asset_id=?", params![lib, asset])?;
    tx.execute(
        "INSERT INTO remote_cache_tasks VALUES(?,?,?,?,?,'',?,unixepoch()+?,0)",
        params![id, lib, asset, hash, hash, worker, LEASE],
    )?;
    tx.commit()?;
    Ok(Some((id, asset, hash)))
}
fn remote_worker_enabled(value: Option<&str>) -> Result<bool> {
    match value {
        None | Some("0" | "false") => Ok(false),
        Some("1" | "true") => Ok(true),
        _ => anyhow::bail!("KEEPS_REMOTE_WORKER_ENABLED must be 0 or 1"),
    }
}
fn claim_sync(s: &AppState, lib: &str, body: Value, enabled: Option<&str>) -> Result<Value> {
    if !remote_worker_enabled(enabled)? {
        return Ok(json!({"task":null,"enabled":false}));
    }
    {
        let _guard = COMMIT
            .lock()
            .map_err(|_| anyhow::anyhow!("worker commit poisoned"))?;
        cleanup_expired(s)?;
    }
    let worker = body["workerID"].as_str().context("workerID required")?;
    ensure!(
        !worker.is_empty() && worker.len() <= 128,
        "invalid workerID"
    );
    ensure!(
        s.previews.free_bytes()? > 2 * 1024 * 1024 * 1024,
        "cache disk low"
    );
    let Some((id, asset, hash)) = claim_pending(&s.store, lib, worker)? else {
        return Ok(json!({"task":null}));
    };
    let prepared = (|| -> Result<Option<Task>> {
        let selected = s.store.default_version(lib, &asset)?;
        let selected = match selected {
            Some(v) => v,
            None => s.store.lock()?.query_row(
                "SELECT content_hash,path FROM catalog_paths WHERE library_id=? AND asset_id=? AND content_hash=? ORDER BY path LIMIT 1",
                params![lib,asset,hash], |r| Ok(crate::versions::DefaultVersion { content_hash:r.get(0)?, path:r.get(1)? })
            ).optional()?.context("no indexed source path")?,
        };
        ensure!(selected.content_hash == hash, "source changed");
        if !std::path::Path::new(&selected.path).try_exists()?
            && s.jobs
                .path_sync_pending(lib, std::path::Path::new(&selected.path))?
        {
            s.store.defer_cache_for_path_sync(lib, &asset)?;
            return Ok(None);
        }
        let input = cache::select_standard_source(&s.store, lib, &asset, &selected)?;
        s.store.lock()?.execute(
            "UPDATE remote_cache_tasks SET input_hash=?,input_path=? WHERE id=?",
            params![input.content_hash, input.path, id],
        )?;
        let t = task(&s.store, lib, &id)?;
        validate(s, &t)?;
        Ok(Some(t))
    })();
    match prepared {
        Ok(None) => Ok(json!({"task":null})),
        Ok(Some(t)) => Ok(json!({
        "task":{
        "taskID":t.id,"assetID":t.asset,"sourceHash":t.hash,"inputHash":t.input_hash,"filename":t.path.file_name().context("filename")?.to_str().context("filename utf8")?,"sizeBytes":fs::metadata(&t.path)?.len(),"mediaType":if media::is_video(&t.path) { "video" } else { "photo" },"generateStandard":needs_standard(&t),"deferred":deferred(&t),"thumbnailEdge":512,"thumbnailQuality":cache::thumbnail_quality()?,"leaseSeconds":LEASE}
        }
        )),
        Err(e) => {
            fail_task(&s.store, lib, &id, &format!("{e:#}"))?;
            Err(e)
        }
    }
}
fn byte_range(value: &str, size: u64) -> Result<(u64, u64)> {
    ensure!(size > 0, "empty source");
    let (start, end) = value
        .strip_prefix("bytes=")
        .context("byte range required")?
        .split_once('-')
        .context("invalid byte range")?;
    if start.is_empty() {
        let suffix: u64 = end.parse()?;
        ensure!(suffix > 0, "empty suffix");
        return Ok((size.saturating_sub(suffix), size - 1));
    }
    let start: u64 = start.parse()?;
    let end = if end.is_empty() {
        size - 1
    } else {
        end.parse::<u64>()?.min(size - 1)
    };
    ensure!(start < size && end >= start, "range outside source");
    Ok((start, end))
}
async fn source(
    State(s): State<Arc<AppState>>,
    Path((lib, id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let t = task(&s.store, &lib, &id)?;
    s.jobs
        .validate_path(t.path.parent().context("source parent")?)?;
    if fs::symlink_metadata(&t.path)
        .map_err(anyhow::Error::from)?
        .file_type()
        .is_symlink()
    {
        return Err(anyhow::anyhow!("source symlink forbidden").into());
    }
    let mut file = tokio::fs::File::open(&t.path)
        .await
        .map_err(anyhow::Error::from)?;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let size = file.metadata().await.map_err(anyhow::Error::from)?.len();
    let range = headers.get("range").and_then(|v| v.to_str().ok());
    let selected = range.map(|v| byte_range(v, size)).transpose();
    let (start, end, partial) = match selected {
        Ok(Some((start, end))) => (start, end, true),
        Ok(None) if size > 0 => (0, size - 1, false),
        _ => {
            return Ok((
                StatusCode::RANGE_NOT_SATISFIABLE,
                [("content-range", format!("bytes */{size}"))],
            )
                .into_response());
        }
    };
    file.seek(std::io::SeekFrom::Start(start))
        .await
        .map_err(anyhow::Error::from)?;
    let length = end - start + 1;
    let mut response =
        Body::from_stream(tokio_util::io::ReaderStream::new(file.take(length))).into_response();
    *response.status_mut() = if partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    response
        .headers_mut()
        .insert("accept-ranges", "bytes".parse().unwrap());
    response
        .headers_mut()
        .insert("content-length", length.to_string().parse().unwrap());
    if partial {
        response.headers_mut().insert(
            "content-range",
            format!("bytes {start}-{end}/{size}").parse().unwrap(),
        );
    }
    Ok(response)
}
async fn heartbeat(
    State(s): State<Arc<AppState>>,
    Path((lib, id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    run(move || {
        let t = task(&s.store, &lib, &id)?;
        if t.completed == 0 {
            s.store.lock()?.execute(
                "UPDATE remote_cache_tasks SET expires_at=unixepoch()+? WHERE id=?",
                params![LEASE, id],
            )?;
        }
        Ok(json!({
        "ok":true,"leaseSeconds":LEASE}
        ))
    })
    .await
}
async fn upload(
    s: Arc<AppState>,
    lib: String,
    id: String,
    mut body: Body,
    role: &'static str,
) -> Result<Json<Value>, ApiError> {
    use http_body_util::BodyExt;
    use tokio::io::AsyncWriteExt;
    let t = task(&s.store, &lib, &id)?;
    if t.completed == 1 {
        return Ok(Json(json!({"ok":true})));
    }
    if role == "standard" && !needs_standard(&t) {
        return Err(anyhow::anyhow!("standard not requested").into());
    }
    let root = scratch(&s, &t)?;
    let file = tempfile::NamedTempFile::new_in(&root).map_err(anyhow::Error::from)?;
    let mut output = tokio::fs::File::from_std(file.reopen().map_err(anyhow::Error::from)?);
    let mut size = 0;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(anyhow::Error::from)?;
        if let Ok(bytes) = frame.into_data() {
            size += bytes.len();
            if size > 256 * 1024 * 1024 {
                return Err(anyhow::anyhow!("upload exceeds 256 MiB").into());
            }
            output
                .write_all(&bytes)
                .await
                .map_err(anyhow::Error::from)?;
        }
    }
    output.flush().await.map_err(anyhow::Error::from)?;
    drop(output);
    run(move || {
        task(&s.store, &lib, &id)?;
        let mut header = [0u8; 12];
        use std::io::Read;
        fs::File::open(file.path())?.read_exact(&mut header)?;
        ensure!(
            &header[4..8] == b"ftyp"
                && [b"heic", b"heix", b"mif1"]
                    .iter()
                    .any(|brand| brand.as_slice() == &header[8..12]),
            "HEIC required"
        );
        file.persist(root.join(format!("{role}.heic")))
            .map_err(|e| e.error)?;
        Ok(json!({"ok":true}))
    })
    .await
}
async fn thumbnail(
    State(s): State<Arc<AppState>>,
    Path((lib, id)): Path<(String, String)>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    upload(s, lib, id, body, "thumbnail").await
}
async fn standard(
    State(s): State<Arc<AppState>>,
    Path((lib, id)): Path<(String, String)>,
    body: Body,
) -> Result<Json<Value>, ApiError> {
    upload(s, lib, id, body, "standard").await
}
async fn complete(
    State(s): State<Arc<AppState>>,
    Path((lib, id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    run(move || complete_sync(&s, &lib, &id)).await
}
fn complete_sync(s: &AppState, lib: &str, id: &str) -> Result<Value> {
    let _guard = COMMIT
        .lock()
        .map_err(|_| anyhow::anyhow!("worker commit poisoned"))?;
    let t = task(&s.store, lib, id)?;
    if t.completed == 1 {
        return Ok(json!({"ok":true}));
    }
    s.store.lock()?.execute(
        "UPDATE remote_cache_tasks SET expires_at=unixepoch()+? WHERE id=? AND completed=0",
        params![LEASE, id],
    )?;
    validate(s, &t)?;
    let root = scratch(s, &t)?;
    let thumb = root.join("thumbnail.heic");
    let tm = MediaProcessor::new().extract(&thumb)?;
    ensure!(
        tm.width > 0 && tm.height > 0 && tm.width <= 512 && tm.height <= 512,
        "invalid thumbnail"
    );
    let standard = if needs_standard(&t) {
        let target = s
            .store
            .generated_standard_target(lib, &t.asset, &t.path, &t.input_hash)?;
        if !target.exists() {
            ensure!(
                cache::free_bytes(t.path.parent().context("parent")?)? > 2 * 1024 * 1024 * 1024,
                "standard disk low"
            );
            let uploaded = root.join("standard.heic");
            let m = MediaProcessor::new().extract(&uploaded)?;
            ensure!(m.width > 0 && m.height > 0, "image dimensions missing");
            let original = MediaProcessor::new().extract(&t.path)?;
            ensure!(
                m.width.max(m.height) >= original.width.max(original.height) * 9 / 10
                    && m.width.min(m.height) >= original.width.min(original.height) * 9 / 10,
                "standard photo is smaller than source dimensions"
            );
            let staging = tempfile::tempdir_in(t.path.parent().context("parent")?)?;
            let staged = staging.path().join("standard.heic");
            fs::copy(&uploaded, &staged)?;
            crate::identity::generated(&staged, &s.store.root_id(lib, &t.asset)?)?;
            s.store.publish_generated_standard(
                lib,
                &t.asset,
                &t.input_hash,
                &staged,
                &target,
                (m.width, m.height),
            )?;
        }
        ensure!(
            s.store.generated_standard_matches(lib, &t.asset, &target)?,
            "existing standard is not owned by this asset"
        );
        target
    } else {
        t.path.clone()
    };
    let sm = MediaProcessor::new().extract(&standard)?;
    let hash = media::sha256_file(&thumb)?;
    let object = s
        .previews
        .put_generated_role(lib, &t.asset, &hash, &thumb, "thumbnail")?;
    let thumbnail_descriptor = json!({
        "objectRef": object, "width": tm.width, "height": tm.height,
        "version": format!("{}:{}",cache::spec()?,hash),
        "sizeBytes":fs::metadata(&thumb)?.len()
    });
    let standard_descriptor = json!({
        "path":standard,"width":sm.width,"height":sm.height,
        "version":media::sha256_file(&standard)?,"sizeBytes":fs::metadata(&standard)?.len(),
        "mtimeNs":cache::file_mtime(&standard)?,"deferred":deferred(&t)
    });
    s.store.cache_publish(
        lib,
        &t.asset,
        &t.hash,
        &thumbnail_descriptor,
        &standard_descriptor,
    )?;
    ensure!(
        task(&s.store, lib, id)?.completed == 1,
        "source changed before publication"
    );
    fs::remove_dir_all(root)?;
    Ok(json!({"ok":true}))
}
fn fail_task(store: &Store, lib: &str, id: &str, error: &str) -> Result<()> {
    let mut db = store.lock()?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let asset:Option<String>=tx.query_row("SELECT asset_id FROM remote_cache_tasks WHERE id=? AND library_id=? AND completed=0 AND expires_at>unixepoch()",params![id,lib],|r|r.get(0)).optional()?;
    let asset = asset.ok_or_else(conflict)?;
    tx.execute("UPDATE media_cache SET status=CASE WHEN attempts>=4 THEN 'failed' ELSE 'pending' END,last_error=?,available_at=unixepoch()+60*attempts*attempts,updated_at=unixepoch() WHERE library_id=? AND asset_id=? AND status='processing'",params![error,lib,asset])?;
    tx.execute(
        "UPDATE remote_cache_tasks SET completed=-1 WHERE id=?",
        [id],
    )?;
    tx.commit()?;
    Ok(())
}
async fn fail(
    State(s): State<Arc<AppState>>,
    Path((lib, id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    run(move || {
        let _guard = COMMIT
            .lock()
            .map_err(|_| anyhow::anyhow!("worker commit poisoned"))?;
        let t = task(&s.store, &lib, &id)?;
        if t.completed == 0 {
            let error = body["error"].as_str().context("error required")?;
            fail_task(&s.store, &lib, &id, error)?;
            let root = scratch(&s, &t)?;
            fs::remove_dir_all(root)?;
        }
        Ok(json!({"ok":true}))
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn remote_workers_require_explicit_enablement() -> Result<()> {
        for value in [None, Some("0"), Some("false")] {
            assert!(!remote_worker_enabled(value)?);
        }
        for value in [Some("1"), Some("true")] {
            assert!(remote_worker_enabled(value)?);
        }
        assert!(remote_worker_enabled(Some("yes")).is_err());
        Ok(())
    }
    #[test]
    fn single_byte_ranges_support_ffmpeg_seeking() -> Result<()> {
        assert_eq!(byte_range("bytes=0-99", 1000)?, (0, 99));
        assert_eq!(byte_range("bytes=800-", 1000)?, (800, 999));
        assert_eq!(byte_range("bytes=-100", 1000)?, (900, 999));
        assert_eq!(byte_range("bytes=900-2000", 1000)?, (900, 999));
        for invalid in ["bytes=1000-", "bytes=9-2", "bytes=0-1,5-6", "bytes=-0"] {
            assert!(byte_range(invalid, 1000).is_err());
        }
        Ok(())
    }
    fn setup() -> Result<(tempfile::TempDir, Store)> {
        let root = tempfile::tempdir()?;
        let store = Store::open(&root.path().join("db.sqlite"), true)?;
        store.lock()?.execute("INSERT INTO catalog_assets VALUES('lib','asset','{}','hash','fingerprint','2024-01-01','photo.jpg',0,'none',NULL,0)",[])?;
        store.reconcile_cache()?;
        store
            .lock()?
            .execute("UPDATE media_cache SET status='processing'", [])?;
        store.lock()?.execute("INSERT INTO remote_cache_tasks VALUES('task','lib','asset','hash','hash','/photo/test.jpg','worker',unixepoch()+1800,0)",[])?;
        Ok((root, store))
    }
    fn pending_setup() -> Result<(tempfile::TempDir, Store)> {
        let (root, store) = setup()?;
        store
            .lock()?
            .execute("DELETE FROM remote_cache_tasks", [])?;
        store
            .lock()?
            .execute("UPDATE media_cache SET status='pending'", [])?;
        Ok((root, store))
    }
    #[test]
    fn claims_skip_trashed_assets() -> Result<()> {
        let (_root, store) = pending_setup()?;
        store
            .lock()?
            .execute("UPDATE catalog_assets SET trashed=1", [])?;
        assert!(claim_pending(&store, "lib", "worker")?.is_none());
        Ok(())
    }
    #[test]
    fn pending_path_sync_defers_missing_source_without_spending_attempts() -> Result<()> {
        let (root, store) = pending_setup()?;
        let originals = root.path().join("photos");
        fs::create_dir_all(originals.join("moved"))?;
        let jobs = crate::jobs::Jobs::open(&root.path().join("jobs.sqlite"), &originals)?;
        let folder = jobs.add_folder("lib", ".")?;
        let scope = jobs.root().join("moved");
        let missing = scope.join("photo.jpg");
        jobs.enqueue_path_change(&folder.id, &scope, 0)?;
        store.lock()?.execute(
            "INSERT INTO catalog_paths VALUES('lib',?,'asset','hash','jpeg_original')",
            [missing.to_str().unwrap()],
        )?;
        let state = AppState {
            previews: Arc::new(crate::previews::PreviewStorage::new(
                &root.path().join("keeps"),
                Some(&originals),
                "http://localhost",
                "test",
            )?),
            store: Arc::new(store),
            jobs: Arc::new(jobs),
            access_token: "test".into(),
            library_id: "lib".into(),
            original_root_names: Default::default(),
        };
        for disabled in [None, Some("0"), Some("false")] {
            assert_eq!(
                claim_sync(&state, "lib", json!({"workerID":"worker"}), disabled)?,
                json!({"task":null,"enabled":false})
            );
        }
        assert_eq!(
            state.store.lock()?.query_row(
                "SELECT status,attempts,available_at FROM media_cache WHERE asset_id='asset'",
                [],
                |r| Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?
                ))
            )?,
            ("pending".into(), 0, 0)
        );
        assert_eq!(
            state
                .store
                .lock()?
                .query_row("SELECT count(*) FROM remote_cache_tasks", [], |r| r
                    .get::<_, i64>(0))?,
            0
        );
        assert!(
            claim_sync(&state, "lib", json!({"workerID":"worker"}), Some("1"))?["task"].is_null()
        );
        let row = state.store.lock()?.query_row("SELECT status,attempts,last_error,available_at>unixepoch() FROM media_cache WHERE asset_id='asset'",[],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,bool>(3)?)))?;
        assert_eq!(row, ("pending".into(), 0, None, true));
        assert_eq!(
            state.store.lock()?.query_row(
                "SELECT count(*) FROM remote_cache_tasks WHERE completed=0",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
        state.store.lock()?.execute("INSERT INTO catalog_assets VALUES('lib','other','{}','otherhash','fp','2024-01-01','other.jpg',0,'none',NULL,0)",[])?;
        state.store.reconcile_cache()?;
        assert_eq!(
            claim_pending(&state.store, "lib", "worker")?.unwrap().1,
            "other"
        );
        Ok(())
    }

    #[test]
    fn claims_skip_processing_and_not_yet_available_work() -> Result<()> {
        for change in ["status='processing'", "available_at=unixepoch()+60"] {
            let (_root, store) = pending_setup()?;
            store
                .lock()?
                .execute(&format!("UPDATE media_cache SET {change}"), [])?;
            assert!(claim_pending(&store, "lib", "worker")?.is_none());
            assert_eq!(
                store
                    .lock()?
                    .query_row("SELECT count(*) FROM remote_cache_tasks", [], |r| r
                        .get::<_, i64>(0))?,
                0
            );
        }
        Ok(())
    }
    #[test]
    fn concurrent_claims_lease_each_asset_once() -> Result<()> {
        let (_root, store) = pending_setup()?;
        for i in 0..24 {
            store.lock()?.execute("INSERT INTO catalog_assets SELECT library_id,?1,snapshot,?1,?1,sort_time,filename,rating,flag,color,trashed FROM catalog_assets WHERE id='asset'", [format!("asset-{i}")])?;
        }
        store.reconcile_cache()?;
        let store = Arc::new(store);
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let store = store.clone();
                std::thread::spawn(move || -> Result<Vec<String>> {
                    let mut assets = Vec::new();
                    while let Some((_, asset, _)) = claim_pending(&store, "lib", "worker")? {
                        assets.push(asset);
                    }
                    Ok(assets)
                })
            })
            .collect();
        let mut assets = Vec::new();
        for thread in threads {
            assets.extend(thread.join().unwrap()?);
        }
        assert_eq!(assets.len(), 25);
        assets.sort();
        assets.dedup();
        assert_eq!(assets.len(), 25);
        Ok(())
    }
    #[test]
    fn startup_preserves_active_lease_and_expiry_requeues_once() -> Result<()> {
        let (_root, store) = setup()?;
        store.recover_cache()?;
        assert!(task(&store, "lib", "task").is_ok());
        store
            .lock()?
            .execute("UPDATE remote_cache_tasks SET expires_at=0", [])?;
        assert!(task(&store, "lib", "task").is_err());
        expire(&*store.lock()?)?;
        assert_eq!(store.cache_status("lib")?["counts"]["pending"], 1);
        store
            .lock()?
            .execute("UPDATE media_cache SET status='processing'", [])?;
        expire(&*store.lock()?)?;
        assert_eq!(store.cache_status("lib")?["counts"]["processing"], 1);
        Ok(())
    }
    #[test]
    fn publish_commits_receipt_with_cache_and_retry_survives_expiry() -> Result<()> {
        let (_root, store) = setup()?;
        let thumbnail = json!({"objectRef":{"bucket":"keeps-previews","key":"lib/asset/thumbnail/hash.heic"},"width":10,"height":10});
        store.cache_publish("lib", "asset", "hash", &thumbnail, &json!({}))?;
        assert_eq!(task(&store, "lib", "task")?.completed, 1);
        store
            .lock()?
            .execute("UPDATE remote_cache_tasks SET expires_at=0", [])?;
        assert_eq!(task(&store, "lib", "task")?.completed, 1);
        assert_eq!(store.cache_status("lib")?["counts"]["ready"], 1);
        Ok(())
    }
    #[test]
    fn expired_fourth_attempt_is_failed_and_stale_failure_cannot_touch_replacement() -> Result<()> {
        let (_root, store) = setup()?;
        store
            .lock()?
            .execute("UPDATE media_cache SET attempts=4", [])?;
        store
            .lock()?
            .execute("UPDATE remote_cache_tasks SET expires_at=0", [])?;
        expire(&*store.lock()?)?;
        assert_eq!(store.cache_status("lib")?["counts"]["failed"], 1);
        store
            .lock()?
            .execute("UPDATE media_cache SET status='processing'", [])?;
        assert!(fail_task(&store, "lib", "task", "late error").is_err());
        assert_eq!(store.cache_status("lib")?["counts"]["processing"], 1);
        Ok(())
    }
    #[test]
    fn replaced_source_rejects_old_lease() -> Result<()> {
        let (_root, store) = setup()?;
        store
            .lock()?
            .execute("UPDATE media_cache SET source_hash='other'", [])?;
        assert!(task(&store, "lib", "task").is_err());
        Ok(())
    }
}
