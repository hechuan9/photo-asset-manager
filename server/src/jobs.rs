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
    })
}
const JOB_SELECT: &str = "SELECT j.id,j.folder_id,f.library_id,f.path,j.status,j.error,j.processed,j.skipped,j.failed,j.current_path,j.started_at,j.finished_at FROM jobs j JOIN folders f ON f.id=j.folder_id";

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
          CREATE UNIQUE INDEX IF NOT EXISTS jobs_live ON jobs(folder_id) WHERE status IN ('pending','running');
          CREATE TABLE IF NOT EXISTS files(folder_id TEXT NOT NULL REFERENCES folders(id),path TEXT NOT NULL,size INTEGER NOT NULL,mtime_ns INTEGER NOT NULL,asset_id TEXT NOT NULL,version TEXT NOT NULL,error TEXT,PRIMARY KEY(folder_id,path));
          UPDATE jobs SET status='pending',updated_at=unixepoch() WHERE status='running';")?;
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
        let db = self.db.lock().unwrap();
        let active = db
            .query_row("SELECT active FROM folders WHERE id=?1", [folder_id], |r| {
                r.get::<_, bool>(0)
            })
            .optional()?
            .unwrap_or(false);
        if !active {
            bail!("folder is missing or inactive");
        }
        db.execute(
            "INSERT OR IGNORE INTO jobs(id,folder_id,status) VALUES(?1,?2,'pending')",
            params![Uuid::new_v4().to_string(), folder_id],
        )?;
        Ok(db.query_row(
            &format!("{JOB_SELECT} WHERE j.folder_id=?1 AND j.status IN ('pending','running')"),
            [folder_id],
            job,
        )?)
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
        let changed = db.execute("UPDATE jobs SET status='pending',error=NULL,processed=0,skipped=0,failed=0,current_path=NULL,started_at=NULL,finished_at=NULL,updated_at=unixepoch() WHERE id=?1 AND status='failed' AND EXISTS(SELECT 1 FROM folders WHERE folders.id=jobs.folder_id AND active=1)",[id])?;
        if changed == 0 {
            bail!("only failed jobs of active folders can be retried");
        }
        Ok(db.query_row(&format!("{JOB_SELECT} WHERE j.id=?1"), [id], job)?)
    }
    pub fn claim_next(&self) -> Result<Option<Job>> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let next = tx.query_row(&format!("{JOB_SELECT} WHERE j.status='pending' AND f.active=1 ORDER BY j.created_at,j.rowid LIMIT 1"),[],job).optional()?;
        let Some(mut next) = next else {
            return Ok(None);
        };
        tx.execute("UPDATE jobs SET status='running',processed=0,skipped=0,failed=0,current_path=NULL,started_at=unixepoch(),finished_at=NULL,updated_at=unixepoch() WHERE id=?1",[&next.id])?;
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
    pub fn finish(&self, id: &str, error: Option<&str>) -> Result<()> {
        self.db.lock().unwrap().execute("UPDATE jobs SET status=?2,error=?3,current_path=NULL,finished_at=unixepoch(),updated_at=unixepoch() WHERE id=?1 AND status='running'",params![id,if error.is_some(){"failed"}else{"completed"},error])?;
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
        Ok(self.db.lock().unwrap().query_row("SELECT size,mtime_ns,asset_id,version,error FROM files WHERE folder_id=?1 AND path=?2",params![folder_id,path],|r|Ok(FileState {size:r.get(0)?,mtime_ns:r.get(1)?,asset_id:r.get(2)?,version:r.get(3)?,error:r.get(4)?})).optional()?)
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
        let failed = jobs.jobs(1).unwrap().remove(0);
        assert_eq!((failed.processed, failed.skipped, failed.failed), (1, 2, 3));
        assert!(failed.finished_at.is_some());
        assert_eq!(jobs.retry(&a.id).unwrap().status, "pending");
        assert!(jobs.retry(&a.id).is_err());
        jobs.record_file(&folder.id, "a.jpg", 10, 100, "asset", "hash1", None)
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
