//! Durable scan scheduling and incremental file state. Originals are never mutated.
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use std::{
    path::{Component, Path},
    sync::Mutex,
};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Folder {
    pub id: String,
    #[serde(rename = "libraryID")]
    pub library_id: String,
    pub path: String,
    pub active: bool,
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub id: String,
    #[serde(rename = "folderID")]
    pub folder_id: String,
    #[serde(rename = "libraryID")]
    pub library_id: String,
    pub path: String,
    pub refresh_metadata: bool,
    pub path_sync: bool,
    pub wait_for_quiet: bool,
    pub status: String,
    pub error: Option<String>,
    pub processed: i64,
    pub skipped: i64,
    pub failed: i64,
    pub current_path: Option<String>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
}
#[derive(Debug, Clone)]
pub struct FileState {
    pub metadata_stamp: String,
    pub size: i64,
    pub mtime_ns: i64,
    pub asset_id: String,
    pub version: String,
    pub error: Option<String>,
}
pub struct Jobs {
    db: Mutex<Connection>,
    root: std::path::PathBuf,
}

fn folder(row: &rusqlite::Row<'_>) -> rusqlite::Result<Folder> {
    Ok(Folder {
        id: row.get(0)?,
        library_id: row.get(1)?,
        path: row.get(2)?,
        active: row.get(3)?,
    })
}
fn job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    Ok(Job {
        id: row.get(0)?,
        folder_id: row.get(1)?,
        library_id: row.get(2)?,
        path: row.get(3)?,
        status: row.get(4)?,
        error: row.get(5)?,
        processed: row.get(6)?,
        skipped: row.get(7)?,
        failed: row.get(8)?,
        current_path: row.get(9)?,
        started_at: row.get(10)?,
        finished_at: row.get(11)?,
        refresh_metadata: row.get(12)?,
        wait_for_quiet: row.get(13)?,
        path_sync: row.get(14)?,
    })
}
const JOB_SELECT: &str = "SELECT j.id,j.folder_id,f.library_id,COALESCE(j.scope_path,f.path),j.status,j.error,j.processed,j.skipped,j.failed,j.current_path,j.started_at,j.finished_at,j.refresh_metadata,j.wait_for_quiet,j.path_sync FROM jobs j JOIN folders f ON f.id=j.folder_id";

impl Jobs {
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn job(&self, id: &str) -> Result<Option<Job>> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .query_row(&format!("{JOB_SELECT} WHERE j.id=?1"), [id], job)
            .optional()?)
    }
    pub fn jobs_for_library(&self, library: &str, limit: usize) -> Result<Vec<Job>> {
        let db = self.db.lock().unwrap();
        Ok(db.prepare(&format!("{JOB_SELECT} WHERE f.library_id=?1 ORDER BY j.created_at DESC,j.rowid DESC LIMIT ?2"))?.query_map(params![library,limit.min(1000) as i64],job)?.collect::<rusqlite::Result<_>>()?)
    }

    pub fn open(path: &Path, original_root: &Path) -> Result<Self> {
        let root = original_root
            .canonicalize()
            .context("resolve originals root")?;
        if !root.is_dir() {
            bail!("originals root must be a directory");
        }
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(30))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
          CREATE TABLE IF NOT EXISTS folders(id TEXT PRIMARY KEY,library_id TEXT NOT NULL,path TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1,UNIQUE(library_id,path));
          CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,folder_id TEXT NOT NULL REFERENCES folders(id),status TEXT NOT NULL,error TEXT,processed INTEGER NOT NULL DEFAULT 0,skipped INTEGER NOT NULL DEFAULT 0,failed INTEGER NOT NULL DEFAULT 0,current_path TEXT,started_at INTEGER,finished_at INTEGER,created_at INTEGER NOT NULL DEFAULT(unixepoch()),updated_at INTEGER NOT NULL DEFAULT(unixepoch()));
          CREATE TABLE IF NOT EXISTS files(folder_id TEXT NOT NULL REFERENCES folders(id),path TEXT NOT NULL,size INTEGER NOT NULL,mtime_ns INTEGER NOT NULL,asset_id TEXT NOT NULL,version TEXT NOT NULL,error TEXT,PRIMARY KEY(folder_id,path));
          UPDATE jobs SET status='pending',updated_at=unixepoch() WHERE status='running';")?;
        let columns: Vec<String> = db
            .prepare("PRAGMA table_info(jobs)")?
            .query_map([], |row| row.get(1))?
            .collect::<rusqlite::Result<_>>()?;
        for (name, definition) in [
            ("scope_path", "TEXT"),
            ("refresh_metadata", "INTEGER NOT NULL DEFAULT 0"),
            ("path_sync", "INTEGER NOT NULL DEFAULT 0"),
            ("wait_for_quiet", "INTEGER NOT NULL DEFAULT 1"),
            ("rerun", "INTEGER NOT NULL DEFAULT 0"),
            ("available_at", "INTEGER NOT NULL DEFAULT 0"),
            ("attempts", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            if !columns.iter().any(|column| column == name) {
                db.execute_batch(&format!("ALTER TABLE jobs ADD COLUMN {name} {definition}"))?;
            }
        }
        if !columns.iter().any(|column| column == "path_sync") {
            // Existing scoped discovery jobs are the pending directory moves that
            // metadata refresh used to starve. Root periodic scans stay background.
            db.execute_batch("UPDATE jobs SET path_sync=1 WHERE status='pending' AND refresh_metadata=0 AND scope_path<>(SELECT path FROM folders WHERE id=jobs.folder_id)")?;
        }
        let file_columns: Vec<String> = db
            .prepare("PRAGMA table_info(files)")?
            .query_map([], |row| row.get(1))?
            .collect::<rusqlite::Result<_>>()?;
        if !file_columns.iter().any(|column| column == "metadata_stamp") {
            db.execute_batch(
                "ALTER TABLE files ADD COLUMN metadata_stamp TEXT NOT NULL DEFAULT ''",
            )?;
        }
        for (name, definition) in [
            ("identity_status", "TEXT NOT NULL DEFAULT 'pending'"),
            ("identity_error", "TEXT"),
        ] {
            if !file_columns.iter().any(|column| column == name) {
                db.execute_batch(&format!("ALTER TABLE files ADD COLUMN {name} {definition}"))?;
            }
        }
        db.execute_batch(
            "CREATE INDEX IF NOT EXISTS files_identity_pending ON files(identity_status)",
        )?;
        db.execute_batch("DROP INDEX IF EXISTS jobs_live;
            UPDATE jobs SET scope_path=(SELECT path FROM folders WHERE id=jobs.folder_id) WHERE scope_path IS NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS jobs_live_scope ON jobs(folder_id,scope_path) WHERE status IN ('pending','running');")?;
        Ok(Self {
            db: Mutex::new(db),
            root,
        })
    }
    pub fn validate_path(&self, path: &Path) -> Result<std::path::PathBuf> {
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        };
        let relative = absolute
            .strip_prefix(&self.root)
            .context("folder must be under originals root")?;
        let mut cursor = self.root.clone();
        for component in relative.components() {
            match component {
                Component::Normal(name) => {
                    let name_text = name.to_string_lossy();
                    if name_text.starts_with('.')
                        || name_text == "@eaDir"
                        || name_text == "#recycle"
                    {
                        bail!("excluded folder component");
                    }
                    cursor.push(name);
                    if std::fs::symlink_metadata(&cursor)?.file_type().is_symlink() {
                        bail!("symlinks are not allowed");
                    }
                }
                Component::CurDir => {}
                _ => bail!("invalid folder component"),
            }
        }
        let resolved = absolute.canonicalize()?;
        if !resolved.starts_with(&self.root) || !resolved.is_dir() {
            bail!("folder must be a directory under originals root");
        }
        Ok(resolved)
    }
    pub fn add_folder(&self, library_id: &str, path: &str) -> Result<Folder> {
        if library_id.trim().is_empty() {
            bail!("libraryID is required");
        }
        let path = self.validate_path(Path::new(path))?;
        let path = path.to_str().context("folder path must be UTF-8")?;
        let db = self.db.lock().unwrap();
        db.execute("INSERT INTO folders(id,library_id,path) VALUES(?1,?2,?3) ON CONFLICT(library_id,path) DO UPDATE SET active=1",params![Uuid::new_v4().to_string(),library_id,path])?;
        Ok(db.query_row(
            "SELECT id,library_id,path,active FROM folders WHERE library_id=?1 AND path=?2",
            params![library_id, path],
            folder,
        )?)
    }
    pub fn folders(&self) -> Result<Vec<Folder>> {
        let db = self.db.lock().unwrap();
        Ok(db
            .prepare("SELECT id,library_id,path,active FROM folders ORDER BY path")?
            .query_map([], folder)?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn remove_folder(&self, id: &str) -> Result<bool> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let found = tx.execute("UPDATE folders SET active=0 WHERE id=?1", [id])? > 0;
        tx.execute("UPDATE jobs SET status='cancelled',finished_at=unixepoch(),updated_at=unixepoch() WHERE folder_id=?1 AND status IN ('pending','running')",[id])?;
        tx.commit()?;
        Ok(found)
    }
    pub fn enqueue_scan(&self, folder_id: &str) -> Result<Job> {
        self.enqueue_scan_with_quiet(folder_id, false)
    }
    pub fn enqueue_external_scan(&self, folder_id: &str) -> Result<Job> {
        self.enqueue_scan_with_quiet(folder_id, true)
    }
    fn enqueue_scan_with_quiet(&self, folder_id: &str, wait_for_quiet: bool) -> Result<Job> {
        let path = self.db.lock().unwrap().query_row(
            "SELECT path FROM folders WHERE id=?1",
            [folder_id],
            |r| r.get::<_, String>(0),
        )?;
        self.enqueue_scope(folder_id, Path::new(&path), 0, false, wait_for_quiet, false)
    }
    /// A running scan receives another pass instead of swallowing a concurrent event.
    pub fn enqueue_change(
        &self,
        folder_id: &str,
        path: &Path,
        debounce_seconds: i64,
    ) -> Result<Job> {
        self.enqueue_scope(folder_id, path, debounce_seconds, true, true, false)
    }
    pub fn enqueue_reconcile_scope(&self, folder_id: &str, path: &Path) -> Result<Job> {
        self.enqueue_scope(folder_id, path, 0, false, false, false)
    }
    pub fn enqueue_external_reconcile_scope(&self, folder_id: &str, path: &Path) -> Result<Job> {
        self.enqueue_scope(folder_id, path, 0, false, true, true)
    }
    pub fn enqueue_path_change(
        &self,
        folder_id: &str,
        path: &Path,
        debounce_seconds: i64,
    ) -> Result<Job> {
        self.enqueue_scope(folder_id, path, debounce_seconds, false, true, true)
    }
    #[allow(clippy::too_many_arguments)]
    fn enqueue_scope(
        &self,
        folder_id: &str,
        path: &Path,
        debounce_seconds: i64,
        refresh_metadata: bool,
        wait_for_quiet: bool,
        path_sync: bool,
    ) -> Result<Job> {
        let path = self.validate_path(path)?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let tracked: Option<String> = tx
            .query_row(
                "SELECT path FROM folders WHERE id=?1 AND active=1",
                [folder_id],
                |r| r.get(0),
            )
            .optional()?;
        let tracked = tracked.context("folder is missing or inactive")?;
        if !path.starts_with(&tracked) {
            bail!("scan scope must be within tracked folder");
        }
        let scope = path.to_str().context("scan scope must be UTF-8")?;
        let available = chrono::Utc::now().timestamp() + debounce_seconds.max(0);
        tx.execute("INSERT OR IGNORE INTO jobs(id,folder_id,scope_path,status,available_at,wait_for_quiet) VALUES(?1,?2,?3,'pending',?4,?5)",
            params![Uuid::new_v4().to_string(), folder_id, scope, available, wait_for_quiet])?;
        tx.execute("UPDATE jobs SET path_sync=MAX(path_sync,?6),refresh_metadata=MAX(refresh_metadata,?4),wait_for_quiet=MAX(wait_for_quiet,?5),rerun=CASE WHEN status='running' THEN 1 ELSE rerun END,available_at=MAX(available_at,?3),updated_at=unixepoch() WHERE folder_id=?1 AND scope_path=?2 AND status IN ('pending','running')", params![folder_id,scope,available,refresh_metadata,wait_for_quiet,path_sync])?;
        let result = tx.query_row(&format!("{JOB_SELECT} WHERE j.folder_id=?1 AND j.scope_path=?2 AND j.status IN ('pending','running')"), params![folder_id,scope],job)?;
        tx.commit()?;
        Ok(result)
    }
    pub fn jobs(&self, limit: usize) -> Result<Vec<Job>> {
        let db = self.db.lock().unwrap();
        Ok(db
            .prepare(&format!(
                "{JOB_SELECT} ORDER BY j.created_at DESC,j.rowid DESC LIMIT ?1"
            ))?
            .query_map([limit.min(1000) as i64], job)?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn retry(&self, id: &str) -> Result<Job> {
        let db = self.db.lock().unwrap();
        let changed = db.execute("UPDATE jobs SET status='pending',attempts=0,available_at=0,error=NULL,processed=0,skipped=0,failed=0,current_path=NULL,started_at=NULL,finished_at=NULL,updated_at=unixepoch() WHERE id=?1 AND status='failed' AND EXISTS(SELECT 1 FROM folders WHERE folders.id=jobs.folder_id AND active=1)",[id])?;
        if changed == 0 {
            bail!("only failed jobs of active folders can be retried");
        }
        Ok(db.query_row(&format!("{JOB_SELECT} WHERE j.id=?1"), [id], job)?)
    }
    pub fn has_pending_changes(&self, excluding_id: &str) -> Result<bool> {
        Ok(self.db.lock().unwrap().query_row("SELECT EXISTS(SELECT 1 FROM jobs j JOIN folders f ON f.id=j.folder_id WHERE j.refresh_metadata=1 AND j.available_at<=unixepoch() AND f.active=1 AND ((j.id<>?1 AND j.status='pending') OR (j.id=?1 AND j.status='running' AND j.rerun=1)))", [excluding_id], |row| row.get(0))?)
    }
    pub fn path_sync_pending(&self, library: &str, path: &Path) -> Result<bool> {
        let text = path.to_str().context("asset path must be UTF-8")?;
        Ok(self.db.lock().unwrap().query_row("SELECT EXISTS(SELECT 1 FROM jobs j JOIN folders f ON f.id=j.folder_id WHERE f.library_id=?1 AND f.active=1 AND j.path_sync=1 AND j.status IN ('pending','running') AND (?2=j.scope_path OR substr(?2,1,length(rtrim(j.scope_path,'/'))+1)=rtrim(j.scope_path,'/')||'/'))", params![library,text], |r| r.get(0))?)
    }
    pub fn should_yield(&self, current: &Job) -> Result<bool> {
        let db = self.db.lock().unwrap();
        Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM jobs j JOIN folders f ON f.id=j.folder_id WHERE f.active=1 AND ((j.status='pending' AND j.id<>?1) OR (j.status='running' AND j.id=?1 AND j.rerun=1)) AND j.available_at<=unixepoch() AND (j.path_sync>?2 OR (j.path_sync=0 AND ?2=0 AND j.refresh_metadata>?3)))", params![current.id,current.path_sync,current.refresh_metadata], |r| r.get(0))?)
    }
    pub fn claim_next(&self) -> Result<Option<Job>> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let next = tx.query_row(&format!("{JOB_SELECT} WHERE j.status='pending' AND j.available_at<=unixepoch() AND f.active=1 ORDER BY j.path_sync DESC,CASE WHEN j.path_sync=0 THEN j.refresh_metadata ELSE 0 END DESC,j.created_at,j.rowid LIMIT 1"),[],job).optional()?;
        let Some(mut next) = next else {
            return Ok(None);
        };
        tx.execute("UPDATE jobs SET status='running',rerun=0,processed=0,skipped=0,failed=0,current_path=NULL,started_at=unixepoch(),finished_at=NULL,updated_at=unixepoch() WHERE id=?1",[&next.id])?;
        tx.commit()?;
        next.status = "running".into();
        next.processed = 0;
        next.skipped = 0;
        next.failed = 0;
        next.current_path = None;
        next.started_at = Some(chrono::Utc::now().timestamp());
        next.finished_at = None;
        Ok(Some(next))
    }
    pub fn yield_job(&self, id: &str) -> Result<()> {
        self.db.lock().unwrap().execute("UPDATE jobs SET status='pending',current_path=NULL,updated_at=unixepoch() WHERE id=?1 AND status='running'", [id])?;
        Ok(())
    }
    pub fn finish(&self, id: &str, error: Option<&str>) -> Result<()> {
        self.db.lock().unwrap().execute("UPDATE jobs SET
            status=CASE WHEN rerun=1 OR (?2 IS NOT NULL AND attempts<3) THEN 'pending' WHEN ?2 IS NOT NULL THEN 'failed' ELSE 'completed' END,
            available_at=CASE WHEN ?2 IS NOT NULL THEN MAX(available_at,unixepoch() + 5 * (1 << attempts)) ELSE available_at END,
            attempts=CASE WHEN rerun=1 OR ?2 IS NULL THEN 0 ELSE attempts+1 END,
            error=?2,current_path=NULL,finished_at=unixepoch(),updated_at=unixepoch()
            WHERE id=?1 AND status='running'",params![id,error])?;
        Ok(())
    }
    pub fn update_progress(
        &self,
        id: &str,
        processed: i64,
        skipped: i64,
        failed: i64,
        current_path: Option<&str>,
    ) -> Result<()> {
        self.db.lock().unwrap().execute("UPDATE jobs SET processed=?2,skipped=?3,failed=?4,current_path=?5,updated_at=unixepoch() WHERE id=?1 AND status='running'",params![id,processed,skipped,failed,current_path])?;
        Ok(())
    }
    pub fn is_active(&self, folder_id: &str) -> Result<bool> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .query_row("SELECT active FROM folders WHERE id=?1", [folder_id], |r| {
                r.get(0)
            })
            .optional()?
            .unwrap_or(false))
    }
    pub fn file_state(&self, folder_id: &str, path: &str) -> Result<Option<FileState>> {
        Ok(self.db.lock().unwrap().query_row("SELECT size,mtime_ns,asset_id,version,error,metadata_stamp FROM files WHERE folder_id=?1 AND path=?2",params![folder_id,path],|r|Ok(FileState {size:r.get(0)?,mtime_ns:r.get(1)?,asset_id:r.get(2)?,version:r.get(3)?,error:r.get(4)?,metadata_stamp:r.get(5)?})).optional()?)
    }
    pub fn has_path_sync(&self) -> Result<bool> {
        Ok(self.db.lock().unwrap().query_row("SELECT EXISTS(SELECT 1 FROM jobs j JOIN folders f ON f.id=j.folder_id WHERE f.active=1 AND j.path_sync=1 AND j.status IN ('pending','running'))",[],|r|r.get(0))?)
    }
    pub fn pending_identities(&self, limit: usize) -> Result<Vec<(Folder, String)>> {
        let db = self.db.lock().unwrap();
        Ok(db.prepare("SELECT f.id,f.library_id,f.path,f.active,p.path FROM files p JOIN folders f ON f.id=p.folder_id WHERE p.identity_status='pending' AND p.error IS NULL AND f.active=1 LIMIT ?")?.query_map([limit as i64], |r| Ok((folder(r)?,r.get(4)?)))?.collect::<rusqlite::Result<_>>()?)
    }
    pub fn identity_missing(&self, folder_id: &str, path: &str) -> Result<()> {
        self.db.lock().unwrap().execute("UPDATE files SET identity_status='missing',identity_error=NULL WHERE folder_id=? AND path=?",params![folder_id,path])?;
        Ok(())
    }
    pub fn identity_pending(&self, folder_id: &str, path: &str) -> Result<()> {
        self.db.lock().unwrap().execute("UPDATE files SET identity_status='pending',identity_error=NULL WHERE folder_id=? AND path=?",params![folder_id,path])?;
        Ok(())
    }
    pub fn identity_result(&self, folder_id: &str, path: &str, error: Option<&str>) -> Result<()> {
        self.db.lock().unwrap().execute(
            "UPDATE files SET identity_status=?,identity_error=? WHERE folder_id=? AND path=?",
            params![
                if error.is_some() { "failed" } else { "ready" },
                error,
                folder_id,
                path
            ],
        )?;
        Ok(())
    }
    pub fn identity_status(&self, library: &str) -> Result<serde_json::Value> {
        let db = self.db.lock().unwrap();
        let mut counts = serde_json::json!({"pending":0,"ready":0,"failed":0,"batchLimit":20});
        for row in db.prepare("SELECT identity_status,count(*) FROM files p JOIN folders f ON f.id=p.folder_id WHERE f.library_id=? AND f.active=1 GROUP BY identity_status")?.query_map([library], |r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)))? {
            let (status,count)=row?;counts[status]=serde_json::json!(count);
        }
        let errors = db.prepare("SELECT p.path,p.identity_error FROM files p JOIN folders f ON f.id=p.folder_id WHERE f.library_id=? AND f.active=1 AND p.identity_status='failed' LIMIT 20")?.query_map([library], |r| Ok(serde_json::json!({"path":r.get::<_,String>(0)?,"error":r.get::<_,String>(1)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
        counts["errors"] = serde_json::json!(errors);
        Ok(counts)
    }
    pub fn record_metadata_stamp(&self, folder_id: &str, path: &str, stamp: &str) -> Result<()> {
        let changed = self.db.lock().unwrap().execute(
            "UPDATE files SET metadata_stamp=?3 WHERE folder_id=?1 AND path=?2",
            params![folder_id, path, stamp],
        )?;
        if changed == 0 {
            bail!("file must be recorded before its metadata stamp");
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub fn record_file(
        &self,
        folder_id: &str,
        path: &str,
        size: i64,
        mtime_ns: i64,
        asset_id: &str,
        version: &str,
        error: Option<&str>,
    ) -> Result<()> {
        self.db.lock().unwrap().execute("INSERT INTO files(folder_id,path,size,mtime_ns,asset_id,version,error) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(folder_id,path) DO UPDATE SET size=excluded.size,mtime_ns=excluded.mtime_ns,asset_id=excluded.asset_id,version=excluded.version,error=excluded.error",params![folder_id,path,size,mtime_ns,asset_id,version,error])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup() -> (tempfile::TempDir, Jobs, Folder) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("originals");
        std::fs::create_dir(&root).unwrap();
        let jobs = Jobs::open(&dir.path().join("jobs.sqlite"), &root).unwrap();
        let folder = jobs.add_folder("library", ".").unwrap();
        (dir, jobs, folder)
    }
    #[test]
    fn path_changes_preempt_metadata_and_only_block_their_scope() {
        let (dir, jobs, folder) = setup();
        let moved = jobs.root().join("moved");
        std::fs::create_dir(&moved).unwrap();
        let metadata = jobs
            .enqueue_change(&folder.id, Path::new(&folder.path), 0)
            .unwrap();
        let running = jobs.claim_next().unwrap().unwrap();
        let path_job = jobs
            .enqueue_external_reconcile_scope(&folder.id, &moved)
            .unwrap();
        assert!(jobs.should_yield(&running).unwrap());
        assert!(
            jobs.path_sync_pending("library", &moved.join("a.raw"))
                .unwrap()
        );
        assert!(
            !jobs
                .path_sync_pending("library", &jobs.root().join("moved-other/a.raw"))
                .unwrap()
        );
        assert!(
            !jobs
                .path_sync_pending("other", &moved.join("a.raw"))
                .unwrap()
        );
        jobs.yield_job(&metadata.id).unwrap();
        let priority = jobs.claim_next().unwrap().unwrap();
        assert_eq!(priority.id, path_job.id);
        assert!(!jobs.should_yield(&priority).unwrap());
        jobs.finish(&priority.id, None).unwrap();
        assert!(
            !jobs
                .path_sync_pending("library", &moved.join("a.raw"))
                .unwrap()
        );
        assert_eq!(jobs.claim_next().unwrap().unwrap().id, metadata.id);
        drop(jobs);
        let jobs = Jobs::open(
            &dir.path().join("jobs.sqlite"),
            &dir.path().join("originals"),
        )
        .unwrap();
        assert!(
            !jobs
                .path_sync_pending("library", &moved.join("a.raw"))
                .unwrap()
        );
    }
    #[test]
    fn path_sync_does_not_yield_to_another_path_sync_with_metadata() {
        let (_dir, jobs, folder) = setup();
        let first = jobs.root().join("first");
        let second = jobs.root().join("second");
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&second).unwrap();
        let initial = jobs.enqueue_path_change(&folder.id, &first, 0).unwrap();
        let running = jobs.claim_next().unwrap().unwrap();
        jobs.enqueue_path_change(&folder.id, &second, 0).unwrap();
        jobs.enqueue_change(&folder.id, &second, 0).unwrap();
        assert!(!jobs.should_yield(&running).unwrap());
        jobs.yield_job(&running.id).unwrap();
        assert_eq!(jobs.claim_next().unwrap().unwrap().id, initial.id);
    }
    #[test]
    fn legacy_scoped_discovery_is_promoted_but_periodic_root_is_not() {
        let (dir, jobs, folder) = setup();
        jobs.enqueue_external_scan(&folder.id).unwrap();
        let child = jobs.root().join("child");
        std::fs::create_dir(&child).unwrap();
        let scoped = jobs
            .enqueue_external_reconcile_scope(&folder.id, &child)
            .unwrap();
        jobs.db
            .lock()
            .unwrap()
            .execute_batch("ALTER TABLE jobs DROP COLUMN path_sync")
            .unwrap();
        drop(jobs);
        let jobs = Jobs::open(
            &dir.path().join("jobs.sqlite"),
            &dir.path().join("originals"),
        )
        .unwrap();
        assert_eq!(jobs.claim_next().unwrap().unwrap().id, scoped.id);
        assert!(
            !jobs
                .path_sync_pending("library", &jobs.root().join("elsewhere.raw"))
                .unwrap()
        );
    }
    #[test]
    fn revision_source_is_sticky_across_merges_yields_and_restart() {
        let (dir, jobs, folder) = setup();
        let manual = jobs.enqueue_scan(&folder.id).unwrap();
        assert!(!manual.wait_for_quiet);
        assert!(!manual.refresh_metadata);
        let external = jobs.enqueue_external_scan(&folder.id).unwrap();
        assert_eq!(external.id, manual.id);
        assert!(external.wait_for_quiet);
        assert!(!external.refresh_metadata);
        assert!(jobs.enqueue_scan(&folder.id).unwrap().wait_for_quiet);
        let running = jobs.claim_next().unwrap().unwrap();
        jobs.yield_job(&running.id).unwrap();
        assert_eq!(jobs.job(&running.id).unwrap().unwrap().status, "pending");
        drop(jobs);
        let jobs = Jobs::open(
            &dir.path().join("jobs.sqlite"),
            &dir.path().join("originals"),
        )
        .unwrap();
        let recovered = jobs.claim_next().unwrap().unwrap();
        assert_eq!(recovered.id, manual.id);
        assert!(recovered.wait_for_quiet);
        jobs.finish(&recovered.id, None).unwrap();
        let manual = jobs
            .enqueue_reconcile_scope(&folder.id, Path::new(&folder.path))
            .unwrap();
        assert!(!manual.wait_for_quiet);
        assert!(
            jobs.enqueue_external_reconcile_scope(&folder.id, Path::new(&folder.path))
                .unwrap()
                .wait_for_quiet
        );
        assert!(
            jobs.enqueue_change(&folder.id, Path::new(&folder.path), 0)
                .unwrap()
                .wait_for_quiet
        );
    }
    #[test]
    fn legacy_jobs_default_to_quiet_waiting() {
        let (dir, jobs, folder) = setup();
        let queued = jobs.enqueue_scan(&folder.id).unwrap();
        jobs.db
            .lock()
            .unwrap()
            .execute_batch("ALTER TABLE jobs DROP COLUMN wait_for_quiet")
            .unwrap();
        drop(jobs);
        let jobs = Jobs::open(
            &dir.path().join("jobs.sqlite"),
            &dir.path().join("originals"),
        )
        .unwrap();
        assert!(jobs.job(&queued.id).unwrap().unwrap().wait_for_quiet);
    }
    #[test]
    fn queue_deduplicates_and_recovers_after_restart() {
        let (dir, jobs, folder) = setup();
        let a = jobs.enqueue_scan(&folder.id).unwrap();
        assert_eq!(a.id, jobs.enqueue_scan(&folder.id).unwrap().id);
        assert_eq!(jobs.claim_next().unwrap().unwrap().id, a.id);
        assert!(jobs.claim_next().unwrap().is_none());
        drop(jobs);
        let jobs = Jobs::open(
            &dir.path().join("jobs.sqlite"),
            &dir.path().join("originals"),
        )
        .unwrap();
        assert_eq!(jobs.claim_next().unwrap().unwrap().id, a.id);
        jobs.finish(&a.id, None).unwrap();
        assert_eq!(jobs.jobs(10).unwrap()[0].status, "completed");
        assert_ne!(jobs.enqueue_scan(&folder.id).unwrap().id, a.id);
    }
    #[test]
    fn change_during_scan_gets_another_pass_and_scopes_survive_restart() {
        let (dir, jobs, folder) = setup();
        let scope = jobs.root().join("child");
        std::fs::create_dir(&scope).unwrap();
        let queued = jobs.enqueue_change(&folder.id, &scope, 0).unwrap();
        let running = jobs.claim_next().unwrap().unwrap();
        assert_eq!(running.path, scope.to_str().unwrap());
        assert_eq!(
            jobs.enqueue_change(&folder.id, &scope, 0).unwrap().id,
            queued.id
        );
        jobs.finish(&running.id, None).unwrap();
        drop(jobs);
        let jobs = Jobs::open(
            &dir.path().join("jobs.sqlite"),
            &dir.path().join("originals"),
        )
        .unwrap();
        let rerun = jobs.claim_next().unwrap().unwrap();
        assert_eq!(rerun.path, scope.to_str().unwrap());
        jobs.finish(&rerun.id, None).unwrap();
        assert!(jobs.claim_next().unwrap().is_none());
        jobs.enqueue_change(&folder.id, &scope, 60).unwrap();
        assert!(jobs.claim_next().unwrap().is_none());
        jobs.remove_folder(&folder.id).unwrap();
        assert!(jobs.claim_next().unwrap().is_none());
    }
    #[test]
    fn root_change_interrupts_incremental_scan_and_preserves_refresh() {
        let (_dir, jobs, folder) = setup();
        let initial = jobs.enqueue_scan(&folder.id).unwrap();
        let running = jobs.claim_next().unwrap().unwrap();
        assert!(!running.refresh_metadata);
        assert!(!jobs.has_pending_changes(&running.id).unwrap());
        jobs.enqueue_change(&folder.id, Path::new(&folder.path), 0)
            .unwrap();
        assert!(jobs.has_pending_changes(&running.id).unwrap());
        jobs.enqueue_scan(&folder.id).unwrap();
        jobs.finish(&initial.id, None).unwrap();
        assert!(jobs.claim_next().unwrap().unwrap().refresh_metadata);
    }
    #[test]
    fn removal_cancels_without_touching_originals() {
        let (dir, jobs, folder) = setup();
        let original = dir.path().join("originals/a.jpg");
        std::fs::write(&original, b"original").unwrap();
        let a = jobs.enqueue_scan(&folder.id).unwrap();
        jobs.claim_next().unwrap();
        jobs.remove_folder(&folder.id).unwrap();
        jobs.finish(&a.id, None).unwrap();
        assert_eq!(jobs.jobs(10).unwrap()[0].status, "cancelled");
        assert!(!jobs.is_active(&folder.id).unwrap());
        assert!(jobs.enqueue_scan(&folder.id).is_err());
        assert_eq!(std::fs::read(&original).unwrap(), b"original");
    }
    #[test]
    fn failed_jobs_retry_and_file_changes_are_persisted() {
        let (_dir, jobs, folder) = setup();
        let a = jobs.enqueue_scan(&folder.id).unwrap();
        jobs.claim_next().unwrap();
        jobs.update_progress(&a.id, 1, 2, 3, Some("a.jpg")).unwrap();
        jobs.finish(&a.id, Some("decoder failed")).unwrap();
        for _ in 0..3 {
            jobs.db
                .lock()
                .unwrap()
                .execute("UPDATE jobs SET available_at=0", [])
                .unwrap();
            jobs.claim_next().unwrap().unwrap();
            jobs.update_progress(&a.id, 1, 2, 3, Some("a.jpg")).unwrap();
            jobs.finish(&a.id, Some("decoder failed")).unwrap();
        }
        let failed = jobs.jobs(1).unwrap().remove(0);
        assert_eq!((failed.processed, failed.skipped, failed.failed), (1, 2, 3));
        assert!(failed.finished_at.is_some());
        assert_eq!(jobs.retry(&a.id).unwrap().status, "pending");
        assert!(jobs.retry(&a.id).is_err());
        jobs.record_file(&folder.id, "a.jpg", 10, 100, "asset", "hash1", None)
            .unwrap();
        assert_eq!(
            jobs.file_state(&folder.id, "a.jpg")
                .unwrap()
                .unwrap()
                .metadata_stamp,
            ""
        );
        jobs.record_metadata_stamp(&folder.id, "a.jpg", "sidecar:10:100")
            .unwrap();
        let before = jobs.file_state(&folder.id, "a.jpg").unwrap().unwrap();
        jobs.record_file(
            &folder.id,
            "a.jpg",
            20,
            200,
            "asset",
            "hash2",
            Some("error"),
        )
        .unwrap();
        let after = jobs.file_state(&folder.id, "a.jpg").unwrap().unwrap();
        assert_ne!((before.size, before.mtime_ns), (after.size, after.mtime_ns));
        assert_eq!(after.metadata_stamp, "sidecar:10:100");
        assert_eq!(after.version, "hash2");
        assert_eq!(after.asset_id, "asset");
        assert_eq!(after.error.as_deref(), Some("error"));
    }
    #[test]
    fn paths_cannot_escape_or_enter_excluded_directories() {
        let (dir, jobs, _folder) = setup();
        for name in [".hidden", "@eaDir", "#recycle"] {
            std::fs::create_dir(dir.path().join("originals").join(name)).unwrap();
            assert!(jobs.add_folder("library", name).is_err());
        }
        assert!(jobs.add_folder("library", "..").is_err());
        assert!(
            jobs.add_folder("library", dir.path().to_str().unwrap())
                .is_err()
        );
        assert!(jobs.add_folder("library", "missing").is_err());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(dir.path(), dir.path().join("originals/link")).unwrap();
            assert!(jobs.add_folder("library", "link").is_err());
        }
    }
}
