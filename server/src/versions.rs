//! Version evidence and stable display selection. Migration never regroups existing assets.
use crate::store::{Store, StoreError};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Default, Debug, Clone)]
pub struct VersionEvidence {
    /// Exact image content only: never a perceptual or downsampled hash.
    pub visual_hash: Option<String>,
    pub width: i64,
    pub height: i64,
    pub edited: bool,
    pub camera_serial: String,
    pub capture_original: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DefaultVersion {
    pub content_hash: String,
    pub path: String,
}
pub(crate) fn migrate(db: &Connection) -> Result<()> {
    db.execute_batch("CREATE TABLE catalog_versions(library_id TEXT NOT NULL,asset_id TEXT NOT NULL,content_hash TEXT NOT NULL,visual_hash TEXT,capture_key TEXT,width INTEGER NOT NULL,height INTEGER NOT NULL,priority INTEGER NOT NULL,evidence TEXT NOT NULL,PRIMARY KEY(library_id,asset_id,content_hash));
        CREATE INDEX version_visual ON catalog_versions(library_id,visual_hash);
        CREATE INDEX version_capture ON catalog_versions(library_id,capture_key);
        CREATE TABLE catalog_version_paths(library_id TEXT NOT NULL,path TEXT NOT NULL,asset_id TEXT NOT NULL,content_hash TEXT NOT NULL,available INTEGER NOT NULL,PRIMARY KEY(library_id,path));
        CREATE INDEX catalog_version_paths_asset ON catalog_version_paths(library_id,asset_id,content_hash,available);
        CREATE TABLE catalog_defaults(library_id TEXT NOT NULL,asset_id TEXT NOT NULL,content_hash TEXT NOT NULL,user_selected INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(library_id,asset_id));
        CREATE TABLE IF NOT EXISTS catalog_version_revision(library_id TEXT PRIMARY KEY,revision INTEGER NOT NULL);")?;
    Ok(())
}
fn touch(db: &Connection, lib: &str, id: &str) -> Result<()> {
    db.execute("UPDATE catalog_assets SET snapshot=json_set(snapshot,'$.updatedAt',?) WHERE library_id=? AND id=?",params![chrono::Utc::now().to_rfc3339(),lib,id])?;
    Ok(())
}
fn bump(db: &Connection, lib: &str) -> Result<()> {
    db.execute("INSERT INTO catalog_version_revision VALUES(?,1) ON CONFLICT(library_id) DO UPDATE SET revision=revision+1", [lib])?;
    Ok(())
}
fn capture_key(snapshot: &Value) -> Option<String> {
    let fields = ["captureTime", "cameraMake", "cameraModel", "lensModel"];
    let values: Option<Vec<&str>> = fields
        .iter()
        .map(|key| snapshot[key].as_str().filter(|s| !s.trim().is_empty()))
        .collect();
    values.map(|v| json!(v).to_string())
}
pub(crate) fn exact_match(
    db: &Connection,
    lib: &str,
    directory_prefix: &str,
    evidence: &VersionEvidence,
) -> Result<Option<String>> {
    let Some(hash) = evidence.visual_hash.as_deref().filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    Ok(db.query_row("SELECT DISTINCT v.asset_id FROM catalog_versions v JOIN catalog_paths p ON p.library_id=v.library_id AND p.asset_id=v.asset_id WHERE v.library_id=?1 AND v.visual_hash=?2 AND p.content_hash=v.content_hash AND p.role IN ('jpeg_original','raw_original') AND substr(p.path,1,length(?3))=?3 AND instr(substr(p.path,length(?3)+1),'/')=0 ORDER BY v.asset_id LIMIT 1", params![lib,hash,directory_prefix], |r|r.get(0)).optional()?)
}
pub(crate) fn register(
    db: &Connection,
    lib: &str,
    id: &str,
    path: &str,
    file: &Value,
    snapshot: &Value,
    evidence: &VersionEvidence,
) -> Result<()> {
    if file["role"] == "sidecar" {
        return Ok(());
    }
    let hash = file["contentHash"]
        .as_str()
        .context("missing contentHash")?;
    let priority = if evidence.edited {
        3
    } else if file["role"] == "raw_original" {
        1
    } else {
        2
    };
    db.execute("INSERT INTO catalog_versions VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(library_id,asset_id,content_hash) DO UPDATE SET visual_hash=coalesce(excluded.visual_hash,visual_hash),capture_key=coalesce(excluded.capture_key,capture_key),width=max(width,excluded.width),height=max(height,excluded.height),priority=max(priority,excluded.priority),evidence=excluded.evidence", params![lib,id,hash,evidence.visual_hash,capture_key(snapshot),evidence.width,evidence.height,priority,json!({"exactVisualHash":evidence.visual_hash,"edited":evidence.edited,"cameraSerial":evidence.camera_serial,"captureOriginal":evidence.capture_original,"capture":snapshot}).to_string()])?;
    let previous: Option<String> = db
        .query_row(
            "SELECT asset_id FROM catalog_version_paths WHERE library_id=? AND path=?",
            params![lib, path],
            |r| r.get(0),
        )
        .optional()?;
    db.execute("INSERT INTO catalog_version_paths VALUES(?,?,?,?,1) ON CONFLICT(library_id,path) DO UPDATE SET asset_id=excluded.asset_id,content_hash=excluded.content_hash,available=1",params![lib,path,id,hash])?;
    choose(db, lib, id)?;
    if let Some(previous) = previous.filter(|p| p != id) {
        choose(db, lib, &previous)?;
    }
    touch(db, lib, id)?;
    bump(db, lib)?;
    Ok(())
}
fn choose(db: &Connection, lib: &str, id: &str) -> Result<()> {
    let current:Option<(String,i64,i64)>=db.query_row("SELECT d.content_hash,d.user_selected,v.priority FROM catalog_defaults d JOIN catalog_versions v USING(library_id,asset_id,content_hash) WHERE d.library_id=? AND d.asset_id=? AND EXISTS(SELECT 1 FROM catalog_version_paths p WHERE p.library_id=d.library_id AND p.asset_id=d.asset_id AND p.content_hash=d.content_hash AND p.available=1)",params![lib,id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let best:Option<(String,i64)>=db.query_row("SELECT v.content_hash,v.priority FROM catalog_versions v WHERE v.library_id=? AND v.asset_id=? AND EXISTS(SELECT 1 FROM catalog_version_paths p WHERE p.library_id=v.library_id AND p.asset_id=v.asset_id AND p.content_hash=v.content_hash AND p.available=1) ORDER BY v.priority DESC,v.width*v.height DESC,v.content_hash LIMIT 1",params![lib,id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    if let Some((_, user, priority)) = &current
        && (*user == 1 || best.as_ref().is_some_and(|b| b.1 <= *priority))
    {
        return Ok(());
    }
    let hash = best.map(|b| b.0);
    let old: Option<String> = db
        .query_row(
            "SELECT content_hash FROM catalog_defaults WHERE library_id=? AND asset_id=?",
            params![lib, id],
            |r| r.get(0),
        )
        .optional()?;
    if old == hash {
        return Ok(());
    }
    db.execute(
        "DELETE FROM catalog_defaults WHERE library_id=? AND asset_id=?",
        params![lib, id],
    )?;
    if let Some(hash) = hash {
        db.execute(
            "INSERT INTO catalog_defaults VALUES(?,?,?,0)",
            params![lib, id, hash],
        )?;
    }
    db.execute(
        "DELETE FROM derivative_objects WHERE library_id=? AND asset_id=? AND role='preview'",
        params![lib, id],
    )?;
    Ok(())
}
pub(crate) fn missing_path(db: &Connection, lib: &str, id: &str, path: &str) -> Result<()> {
    db.execute(
        "UPDATE catalog_version_paths SET available=0 WHERE library_id=? AND path=?",
        params![lib, path],
    )?;
    choose(db, lib, id)?;
    touch(db, lib, id)?;
    bump(db, lib)?;
    Ok(())
}
fn invalid(message: &str) -> anyhow::Error {
    StoreError {
        status: 422,
        code: "invalid_version".into(),
        message: message.into(),
    }
    .into()
}
impl Store {
    pub fn defaults_needing_previews(&self, lib: &str, scope: &str) -> Result<Vec<String>> {
        let db = self.lock()?;
        let root = scope.trim_end_matches('/');
        let mut statement=db.prepare("SELECT d.asset_id FROM catalog_defaults d WHERE d.library_id=? AND d.asset_id IN (SELECT p.asset_id FROM catalog_version_paths p WHERE p.library_id=? AND p.path>=? AND p.path<?) AND EXISTS(SELECT 1 FROM catalog_version_paths p WHERE p.library_id=d.library_id AND p.asset_id=d.asset_id AND p.content_hash=d.content_hash AND p.available=1) AND NOT EXISTS(SELECT 1 FROM derivative_objects o WHERE o.library_id=d.library_id AND o.asset_id=d.asset_id AND o.role='preview') ORDER BY d.asset_id")?;
        Ok(statement
            .query_map(
                params![lib, lib, format!("{root}/"), format!("{root}0")],
                |r| r.get(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    pub fn library_revision(&self, lib: &str) -> Result<Value> {
        let db = self.lock()?;
        let revision:i64=db.query_row("SELECT coalesce((SELECT max(global_seq) FROM ledger_events WHERE library_id=?),0)+coalesce((SELECT revision FROM catalog_version_revision WHERE library_id=?),0)",params![lib,lib],|r|r.get(0))?;
        Ok(json!({"revision":revision}))
    }
    pub fn default_version(&self, lib: &str, id: &str) -> Result<Option<DefaultVersion>> {
        Ok(self.lock()?.query_row("SELECT d.content_hash,p.path FROM catalog_defaults d JOIN catalog_version_paths p USING(library_id,asset_id,content_hash) WHERE d.library_id=? AND d.asset_id=? AND p.available=1 ORDER BY p.path LIMIT 1",params![lib,id],|r|Ok(DefaultVersion{content_hash:r.get(0)?,path:r.get(1)?})).optional()?)
    }
    pub fn versions(&self, lib: &str, id: &str) -> Result<Value> {
        self.asset(lib, id)?;
        let db = self.lock()?;
        let mut stmt=db.prepare("SELECT v.content_hash,v.width,v.height,v.priority,v.evidence,coalesce(d.content_hash=v.content_hash,0),coalesce(d.user_selected,0),EXISTS(SELECT 1 FROM catalog_version_paths p WHERE p.library_id=v.library_id AND p.asset_id=v.asset_id AND p.content_hash=v.content_hash AND p.available=1) FROM catalog_versions v LEFT JOIN catalog_defaults d USING(library_id,asset_id) WHERE v.library_id=? AND v.asset_id=? ORDER BY v.priority DESC,v.width*v.height DESC,v.content_hash")?;
        let rows = stmt
            .query_map(params![lib, id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, bool>(5)?,
                    r.get::<_, bool>(6)?,
                    r.get::<_, bool>(7)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut items = Vec::new();
        for (hash, width, height, priority, evidence, default, user, available) in rows {
            let mut paths=db.prepare("SELECT path,available FROM catalog_version_paths WHERE library_id=? AND asset_id=? AND content_hash=? ORDER BY path")?;
            let paths = paths
                .query_map(params![lib, id, hash], |r| {
                    Ok(json!({"path":r.get::<_,String>(0)?,"available":r.get::<_,bool>(1)?}))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            items.push(json!({"contentHash":hash,"width":width,"height":height,"priority":priority,"evidence":serde_json::from_str::<Value>(&evidence)?,"isDefault":default,"userSelected":default&&user,"available":available,"paths":paths}));
        }
        Ok(json!({"items":items}))
    }
    pub fn version_candidates(&self, lib: &str, id: &str) -> Result<Value> {
        self.asset(lib, id)?;
        let db = self.lock()?;
        let mut stmt=db.prepare("SELECT DISTINCT b.asset_id FROM catalog_versions a JOIN catalog_versions b ON a.library_id=b.library_id AND a.capture_key=b.capture_key WHERE a.library_id=? AND a.asset_id=? AND b.asset_id<>a.asset_id AND (coalesce(json_extract(a.evidence,'$.cameraSerial'),'')='' OR coalesce(json_extract(b.evidence,'$.cameraSerial'),'')='' OR json_extract(a.evidence,'$.cameraSerial')=json_extract(b.evidence,'$.cameraSerial')) ORDER BY b.asset_id")?;
        let items=stmt.query_map(params![lib,id],|r|Ok(json!({"assetID":r.get::<_,String>(0)?,"evidence":"matching_capture_metadata","requiresVisualConfirmation":true})))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(json!({"items":items}))
    }
    pub fn set_default_version(&self, lib: &str, id: &str, hash: &str) -> Result<Value> {
        self.asset(lib, id)?;
        let mut db = self.lock()?;
        let tx = db.transaction()?;
        let available:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM catalog_version_paths WHERE library_id=? AND asset_id=? AND content_hash=? AND available=1)",params![lib,id,hash],|r|r.get(0))?;
        if !available {
            return Err(invalid("version is not available for this asset"));
        }
        let old: Option<String> = tx
            .query_row(
                "SELECT content_hash FROM catalog_defaults WHERE library_id=? AND asset_id=?",
                params![lib, id],
                |r| r.get(0),
            )
            .optional()?;
        tx.execute("INSERT INTO catalog_defaults VALUES(?,?,?,1) ON CONFLICT(library_id,asset_id) DO UPDATE SET content_hash=excluded.content_hash,user_selected=1",params![lib,id,hash])?;
        if old.as_deref() != Some(hash) {
            tx.execute("DELETE FROM derivative_objects WHERE library_id=? AND asset_id=? AND role='preview'",params![lib,id])?;
        }
        touch(&tx, lib, id)?;
        bump(&tx, lib)?;
        tx.commit()?;
        drop(db);
        self.versions(lib, id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot() -> Value {
        json!({"assetID":uuid::Uuid::new_v4(),"captureTime":"2024-01-01T00:00:00Z","cameraMake":"Canon","cameraModel":"R3","lensModel":"50mm","originalFilename":"same.jpg","contentFingerprint":"unused","metadataFingerprint":"same","rating":0,"flagState":"unflagged","tags":[],"createdAt":"2024-01-01T00:00:00Z","updatedAt":"2024-01-01T00:00:00Z"})
    }
    fn file(hash: &str, raw: bool) -> Value {
        json!({"contentHash":hash,"sizeBytes":100,"role":if raw{"raw_original"}else{"jpeg_original"}})
    }
    fn evidence(visual: &str) -> VersionEvidence {
        VersionEvidence {
            visual_hash: Some(visual.into()),
            width: 100,
            height: 100,
            ..Default::default()
        }
    }
    #[test]
    fn new_hash_and_visual_matches_stay_in_direct_parent_directory() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("db"), true, Default::default())?;
        let mut ids = Vec::new();
        for path in ["/photos/a.jpg", "/photos/sub/a.jpg", "/photos-other/a.jpg"] {
            ids.push(store.ingest_original_with_evidence(
                "lib",
                path,
                &file("same", false),
                &snapshot(),
                &evidence("same-image"),
            )?);
        }
        assert_ne!(ids[0], ids[1]);
        assert_ne!(ids[0], ids[2]);
        assert_ne!(ids[1], ids[2]);
        for (index, directory) in ["/photos", "/photos/sub", "/photos-other"]
            .iter()
            .enumerate()
        {
            assert_eq!(
                store.ingest_original_with_evidence(
                    "lib",
                    &format!("{directory}/copy.jpg"),
                    &file("same", false),
                    &snapshot(),
                    &evidence("same-image")
                )?,
                ids[index]
            );
            assert_eq!(
                store.ingest_original_with_evidence(
                    "lib",
                    &format!("{directory}/edited-metadata.jpg"),
                    &file(&format!("changed-{index}"), false),
                    &snapshot(),
                    &evidence("same-image")
                )?,
                ids[index]
            );
        }
        let elsewhere = store.ingest_original_with_evidence(
            "lib",
            "/elsewhere/new.jpg",
            &file("other", false),
            &snapshot(),
            &evidence("same-image"),
        )?;
        assert!(!ids.contains(&elsewhere));
        Ok(())
    }
    #[test]
    fn historical_cross_directory_assets_do_not_bridge_content_evidence() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("db"), true, Default::default())?;
        let id = store.ingest_original_with_evidence(
            "lib",
            "/one/a.jpg",
            &file("a", false),
            &snapshot(),
            &evidence("image-a"),
        )?;
        store.lock()?.execute(
            "INSERT INTO catalog_paths VALUES(?,?,?,?,?)",
            params!["lib", "/two/b.jpg", id, "b", "jpeg_original"],
        )?;
        assert_eq!(
            store.ingest_original_with_evidence(
                "lib",
                "/two/b.jpg",
                &file("b", false),
                &snapshot(),
                &evidence("image-b")
            )?,
            id
        );
        let visual = store.ingest_original_with_evidence(
            "lib",
            "/one/metadata-copy.jpg",
            &file("b-metadata", false),
            &snapshot(),
            &evidence("image-b"),
        )?;
        assert_ne!(visual, id);
        let hash = store.ingest_original_with_evidence(
            "lib",
            "/one/hash-copy.jpg",
            &file("b", false),
            &snapshot(),
            &VersionEvidence::default(),
        )?;
        assert_ne!(hash, id);
        assert_ne!(hash, visual);
        assert_eq!(
            store.ingest_original_with_evidence(
                "lib",
                "/two/copy.jpg",
                &file("b", false),
                &snapshot(),
                &evidence("image-b")
            )?,
            id
        );
        Ok(())
    }
    #[test]
    fn capture_metadata_is_only_candidate_and_serial_conflicts_are_excluded() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("db"), true, Default::default())?;
        let a = store.ingest_original_with_evidence(
            "lib",
            "/a.jpg",
            &file("a", false),
            &snapshot(),
            &evidence("image-a"),
        )?;
        let b = store.ingest_original_with_evidence(
            "lib",
            "/b.jpg",
            &file("b", false),
            &snapshot(),
            &evidence("image-b"),
        )?;
        assert_ne!(a, b);
        assert_eq!(
            store.version_candidates("lib", &a)?["items"][0]["assetID"],
            b
        );
        let mut ev = evidence("image-c");
        ev.camera_serial = "serial1".into();
        let c = store.ingest_original_with_evidence(
            "lib",
            "/c.jpg",
            &file("c", false),
            &snapshot(),
            &ev,
        )?;
        ev.camera_serial = "serial2".into();
        ev.visual_hash = Some("image-d".into());
        let d = store.ingest_original_with_evidence(
            "lib",
            "/d.jpg",
            &file("d", false),
            &snapshot(),
            &ev,
        )?;
        assert!(
            !store.version_candidates("lib", &c)?["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["assetID"] == d)
        );
        let mut unknown = snapshot();
        unknown["lensModel"] = json!("");
        let e = store.ingest_original("lib", "/e.jpg", &file("e", false), &unknown)?;
        assert!(
            store.version_candidates("lib", &e)?["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        Ok(())
    }
    #[test]
    fn exact_content_groups_and_default_remains_stable_with_user_override() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("db"), true, Default::default())?;
        let mut ev = evidence("same-pixels");
        let id = store.ingest_original_with_evidence(
            "lib",
            "/a.raw",
            &file("a", true),
            &snapshot(),
            &ev,
        )?;
        let before = store.library_revision("lib")?["revision"].as_i64().unwrap();
        assert_eq!(
            id,
            store.ingest_original_with_evidence(
                "lib",
                "/b.jpg",
                &file("b", false),
                &snapshot(),
                &ev
            )?
        );
        assert_eq!(
            store.default_version("lib", &id)?.unwrap().content_hash,
            "b"
        );
        ev.width = 200;
        store.ingest_original_with_evidence(
            "lib",
            "/c.jpg",
            &file("c", false),
            &snapshot(),
            &ev,
        )?;
        assert_eq!(
            store.default_version("lib", &id)?.unwrap().content_hash,
            "b"
        );
        {
            let db = store.lock()?;
            db.execute("INSERT INTO derivative_objects VALUES(?,?,'preview','{}','previews','old',NULL,10,10,0,'now')",params!["lib",id])?;
        }
        assert!(!store.asset("lib", &id)?["_preview"].is_null());
        store.set_default_version("lib", &id, "a")?;
        assert!(store.asset("lib", &id)?["_preview"].is_null());
        ev.edited = true;
        store.ingest_original_with_evidence(
            "lib",
            "/d.jpg",
            &file("d", false),
            &snapshot(),
            &ev,
        )?;
        assert_eq!(
            store.default_version("lib", &id)?.unwrap().content_hash,
            "a"
        );
        assert_eq!(
            store.versions("lib", &id)?["items"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
        assert!(store.library_revision("lib")?["revision"].as_i64().unwrap() > before);
        assert!(store.set_default_version("lib", &id, "foreign").is_err());
        assert!(!store.declare_generated_preview_for_version("lib", &id, "b", &json!({}))?);
        {
            let db = store.lock()?;
            missing_path(&db, "lib", &id, "/a.raw")?;
        }
        assert_eq!(
            store.default_version("lib", &id)?.unwrap().content_hash,
            "d"
        );
        Ok(())
    }
    #[test]
    fn missing_default_preview_is_repaired_even_when_replacement_is_outside_scope() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("db"), true, Default::default())?;
        let ev = evidence("same");
        let id = store.ingest_original_with_evidence(
            "lib",
            "/a/one.jpg",
            &file("one", false),
            &snapshot(),
            &ev,
        )?;
        // Seed a legacy association; new cross-directory files now stay separate.
        store.lock()?.execute(
            "INSERT INTO catalog_paths VALUES(?,?,?,?,?)",
            params!["lib", "/b/two.jpg", id, "two", "jpeg_original"],
        )?;
        store.ingest_original_with_evidence(
            "lib",
            "/b/two.jpg",
            &file("two", false),
            &snapshot(),
            &ev,
        )?;
        {
            let db = store.lock()?;
            db.execute("INSERT INTO derivative_objects VALUES(?,?,'preview','{}','previews','old',NULL,10,10,0,'now')",params!["lib",id])?;
        }
        assert!(store.defaults_needing_previews("lib", "/a")?.is_empty());
        {
            let db = store.lock()?;
            missing_path(&db, "lib", &id, "/a/one.jpg")?;
        }
        assert_eq!(
            store.default_version("lib", &id)?.unwrap().path,
            "/b/two.jpg"
        );
        assert_eq!(store.defaults_needing_previews("lib", "/a")?, vec![id]);
        assert!(
            store
                .defaults_needing_previews("lib", "/unrelated")?
                .is_empty()
        );
        Ok(())
    }
    #[test]
    fn v2_migration_does_not_regroup_or_backfill_legacy_assets() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let store = Store::open(&path, true, Default::default())?;
        let a = store.ingest_original("lib", "/a.jpg", &file("a", false), &snapshot())?;
        let b = store.ingest_original("lib", "/b.jpg", &file("b", false), &snapshot())?;
        assert_ne!(a, b);
        {
            let db = store.lock()?;
            db.execute_batch("DROP TABLE catalog_versions;DROP TABLE catalog_defaults;DROP TABLE catalog_version_paths;DROP TABLE catalog_version_revision;PRAGMA user_version=2;")?;
        }
        drop(store);
        assert!(Store::open(&path, false, Default::default()).is_err());
        let store = Store::open(&path, true, Default::default())?;
        assert_eq!(store.counts("lib", false)?["all"], 2);
        assert_eq!(store.versions("lib", &a)?["items"], json!([]));
        assert_eq!(
            store.ingest_original("lib", "/a.jpg", &file("a", false), &snapshot())?,
            a
        );
        Ok(())
    }
}
