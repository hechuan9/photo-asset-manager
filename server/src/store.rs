use anyhow::{Result, anyhow};
use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

#[derive(Debug)]
pub struct StoreError {
    pub status: u16,
    pub code: String,
    pub message: String,
}
impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for StoreError {}
fn invalid(status: u16, code: &str, message: &str) -> anyhow::Error {
    StoreError {
        status,
        code: code.into(),
        message: message.into(),
    }
    .into()
}
fn parse_json(row: &Row<'_>, index: usize) -> rusqlite::Result<Value> {
    let s: String = row.get(index)?;
    serde_json::from_str(&s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(e))
    })
}

pub struct Store {
    connection: Mutex<Connection>,
}
impl Store {
    pub fn open(path: &Path, auto_create: bool) -> Result<Self> {
        if auto_create && let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | if auto_create {
                rusqlite::OpenFlags::SQLITE_OPEN_CREATE
            } else {
                rusqlite::OpenFlags::empty()
            };
        let mut connection = Connection::open_with_flags(path, flags)?;
        connection.busy_timeout(Duration::from_secs(30))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        crate::catalog::ensure_schema(&mut connection, auto_create)?;
        // Offline maintenance SQL is captured by persistent triggers; publish before serving.
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        crate::revisions::flush(&tx)?;
        tx.commit()?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }
    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("database mutex poisoned"))
    }
    pub fn derivative_metadata(
        &self,
        library: Option<&str>,
        asset: &str,
        role: &str,
    ) -> Result<Option<Value>> {
        let db = self.lock()?;
        let mut stmt=db.prepare("SELECT asset_id,role,file_object,object_bucket,object_key,object_etag,pixel_width,pixel_height FROM derivative_objects WHERE asset_id=?1 AND role=?2 AND (?3 IS NULL OR library_id=?3) ORDER BY updated_at DESC")?;
        let rows=stmt.query_map(params![asset,role,library],|r|Ok(json!({"assetID":r.get::<_,String>(0)?,"role":r.get::<_,String>(1)?,"fileObject":parse_json(r,2)?,"objectRef":{"bucket":r.get::<_,String>(3)?,"key":r.get::<_,String>(4)?,"eTag":r.get::<_,Option<String>>(5)?},"pixelSize":{"width":r.get::<_,i64>(6)?,"height":r.get::<_,i64>(7)?}})))?.collect::<rusqlite::Result<Vec<_>>>()?;
        if library.is_none() && rows.len() > 1 {
            return Err(invalid(
                400,
                "library_required",
                "libraryID required when the asset exists in multiple libraries",
            ));
        }
        Ok(rows.into_iter().next())
    }
    pub fn remove_derivative(
        &self,
        library: &str,
        asset: &str,
        role: &str,
    ) -> Result<Option<Value>> {
        let mut db = self.lock()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let object=tx.query_row("SELECT object_bucket,object_key,object_etag FROM derivative_objects WHERE library_id=? AND asset_id=? AND role=?",params![library,asset,role],|r|Ok(json!({"bucket":r.get::<_,String>(0)?,"key":r.get::<_,String>(1)?,"eTag":r.get::<_,Option<String>>(2)?}))).optional()?;
        tx.execute(
            "DELETE FROM derivative_objects WHERE library_id=? AND asset_id=? AND role=?",
            params![library, asset, role],
        )?;
        crate::revisions::flush(&tx)?;
        tx.commit()?;
        Ok(object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(store: &Store) -> Result<String> {
        store.ingest_original("lib", "/photos/photo.jpg",
            &json!({"contentHash":"original","sizeBytes":4,"role":"jpeg_original"}),
            &json!({"contentFingerprint":"original","metadataFingerprint":"metadata","originalFilename":"photo.jpg","rating":0,"tags":[],"flagState":"unflagged","createdAt":"2024-01-01T00:00:00Z","updatedAt":"2024-01-01T00:00:00Z"}))
    }

    #[test]
    fn schema_six_migration_preserves_current_state_without_replaying_removed_assets() -> Result<()>
    {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("catalog.sqlite");
        let store = Store::open(&path, true)?;
        let id = seed(&store)?;
        store.patch_asset("lib", &id, &json!({"rating":5,"tags":["retain"]}))?;
        store.set_trashed("lib", &id, true)?;
        store.declare_generated_preview("lib", &id, &json!({"assetID":id,"role":"preview","fileObject":{"contentHash":"thumbnail","sizeBytes":4,"role":"preview"},"objectRef":{"bucket":"previews","key":"a.heic"},"pixelSize":{"width":512,"height":512}}))?;
        let snapshot = store.asset("lib", &id)?;
        let revision = store.library_revision("lib")?;
        let derivative = store.derivative_metadata(Some("lib"), &id, "preview")?;
        let versions = store.versions("lib", &id)?;
        {
            let db = store.lock()?;
            db.execute_batch(include_str!("../tests/fixtures/legacy_schema.sql"))?;
            db.execute_batch("ALTER TABLE derivative_objects ADD COLUMN declared_event_seq INTEGER NOT NULL DEFAULT 42; DROP TABLE catalog_identity_roots; DROP TABLE catalog_identity_aliases; PRAGMA user_version=6;")?;
            db.execute("INSERT INTO ledger_events VALUES('lib',1,'old-op','old-device',1,'{}','user','asset','removed-asset','asset_snapshot_declared',?,'hash',NULL,'2024-01-01T00:00:00Z')", [json!({"assetSnapshotDeclared":{"snapshot":{"assetID":"removed-asset","originalFilename":"/volume2/myphoto/removed.jpg"}}}).to_string()])?;
        }
        drop(store);
        assert!(Store::open(&path, false).is_err());
        let store = Store::open(&path, true)?;
        assert_eq!(store.asset("lib", &id)?, snapshot);
        assert_eq!(store.library_revision("lib")?, revision);
        assert_eq!(
            store.derivative_metadata(Some("lib"), &id, "preview")?,
            derivative
        );
        assert_eq!(store.versions("lib", &id)?, versions);
        assert!(store.asset("lib", "removed-asset").is_err());
        {
            let db = store.lock()?;
            assert_eq!(
                db.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))?,
                10
            );
            let legacy_count: i64 = db.query_row("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN ('ledger_events','ledger_sequence_counters','device_states','archive_receipts','sync_conflicts')", [], |r| r.get(0))?;
            assert_eq!(legacy_count, 0);
            assert!(
                db.prepare("SELECT declared_event_seq FROM derivative_objects")
                    .is_err()
            );
            assert_eq!(
                db.query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0))?,
                "ok"
            );
        }
        drop(store);
        let store = Store::open(&path, false)?;
        store.patch_asset("lib", &id, &json!({"rating":4}))?;
        assert_eq!(store.asset("lib", &id)?["rating"], 4);
        Ok(())
    }

    #[test]
    fn populated_ledger_only_database_is_rejected_without_modification() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("catalog.sqlite");
        let db = Connection::open(&path)?;
        db.execute_batch(include_str!("../tests/fixtures/python_ledger.sql"))?;
        let events: i64 = db.query_row("SELECT count(*) FROM ledger_events", [], |r| r.get(0))?;
        drop(db);
        let error = Store::open(&path, true)
            .err()
            .expect("legacy ledger-only migration must fail");
        assert!(error.to_string().contains("legacy ledger-only"));
        let db = Connection::open(&path)?;
        assert_eq!(
            db.query_row("SELECT count(*) FROM ledger_events", [], |r| r
                .get::<_, i64>(0))?,
            events
        );
        assert_eq!(
            db.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))?,
            0
        );
        assert!(db.prepare("SELECT * FROM catalog_assets").is_err());
        Ok(())
    }

    #[test]
    fn metadata_transaction_rolls_back_when_revision_publication_fails() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("catalog.sqlite"), true)?;
        let id = seed(&store)?;
        let before = store.asset("lib", &id)?;
        let revision = store.library_revision("lib")?;
        store.lock()?.execute_batch("CREATE TRIGGER fail_revision BEFORE UPDATE ON catalog_version_revision BEGIN SELECT RAISE(ABORT,'injected revision failure'); END;")?;
        assert!(store.patch_asset("lib", &id, &json!({"rating":5})).is_err());
        assert_eq!(store.asset("lib", &id)?, before);
        assert_eq!(store.library_revision("lib")?, revision);
        Ok(())
    }

    #[test]
    fn current_schema_is_validated_and_missing_database_is_not_created() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("catalog.sqlite");
        assert!(Store::open(&path, false).is_err());
        assert!(!path.exists());
        let store = Store::open(&path, true)?;
        store
            .lock()?
            .execute_batch("DROP TABLE derivative_objects;")?;
        drop(store);
        assert!(Store::open(&path, false).is_err());
        Ok(())
    }
}
