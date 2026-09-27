use crate::store::{Store, StoreError, append_one};
use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use uuid::Uuid;

const VERSION: i64 = 2;
const DIRECTORY_ASSET_SET: &str =
    " AND a.id IN (SELECT f.asset_id FROM catalog_paths f WHERE f.library_id=? AND ";
const SCHEMA: &str = r#"
CREATE TABLE catalog_assets(library_id TEXT NOT NULL,id TEXT NOT NULL,snapshot TEXT NOT NULL,content_hash TEXT NOT NULL,fingerprint TEXT NOT NULL,sort_time TEXT NOT NULL,filename TEXT NOT NULL,rating INTEGER NOT NULL,flag TEXT NOT NULL,color TEXT,trashed INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(library_id,id));
CREATE INDEX catalog_hash ON catalog_assets(library_id,content_hash);
CREATE INDEX catalog_fingerprint ON catalog_assets(library_id,fingerprint);
CREATE INDEX catalog_sort ON catalog_assets(library_id,trashed,sort_time,id);
CREATE TABLE catalog_files(library_id TEXT NOT NULL,asset_id TEXT NOT NULL,content_hash TEXT NOT NULL,size INTEGER NOT NULL,role TEXT NOT NULL,holder TEXT NOT NULL,availability TEXT NOT NULL,PRIMARY KEY(library_id,asset_id,content_hash,role,holder));
CREATE INDEX catalog_file_hash ON catalog_files(library_id,content_hash);
CREATE TABLE catalog_paths(library_id TEXT NOT NULL,path TEXT NOT NULL,asset_id TEXT NOT NULL,content_hash TEXT NOT NULL,role TEXT NOT NULL,PRIMARY KEY(library_id,path));
CREATE INDEX catalog_paths_asset ON catalog_paths(library_id,asset_id);
"#;

pub fn ensure_schema(db: &mut Connection, allow_migrate: bool) -> Result<()> {
    let version: i64 = db.pragma_query_value(None, "user_version", |r| r.get(0))?;
    ensure!(
        version <= VERSION,
        "database schema {version} is newer than this server supports ({VERSION})"
    );
    if version == VERSION {
        db.prepare("SELECT snapshot,content_hash,fingerprint,sort_time,filename,rating,flag,color,trashed FROM catalog_assets LIMIT 0")?;
        db.prepare(
            "SELECT asset_id,content_hash,size,role,holder,availability FROM catalog_files LIMIT 0",
        )?;
        db.prepare("SELECT library_id,path,asset_id,content_hash,role FROM catalog_paths LIMIT 0")?;
        db.prepare("SELECT library_id,path FROM catalog_hidden_directories LIMIT 0")?;
        return Ok(());
    }
    ensure!(
        allow_migrate,
        "database migration required: stop the service, back up SQLite, then run keeps-server migrate"
    );
    tracing::info!(from = version, to = VERSION, "migrating NAS catalog");
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if version == 0 {
        tx.execute_batch(SCHEMA)?;
        {
            let mut statement=tx.prepare("SELECT library_id,global_seq,op_type,payload_json,committed_at FROM ledger_events ORDER BY library_id,global_seq")?;
            let mut rows = statement.query([])?;
            let mut count = 0usize;
            while let Some(row) = rows.next()? {
                let lib: String = row.get(0)?;
                let seq: i64 = row.get(1)?;
                let kind: String = row.get(2)?;
                let payload: Value = serde_json::from_str(&row.get::<_, String>(3)?)?;
                project(
                    &tx,
                    &lib,
                    seq,
                    &json!({"opType":kind,"payload":payload,"committedAt":row.get::<_,String>(4)?}),
                )?;
                count += 1;
                if count.is_multiple_of(50000) {
                    tracing::info!(events = count, "replaying existing ledger into NAS catalog");
                }
            }
            tracing::info!(events = count, "NAS catalog replay complete");
        }
    }
    tx.execute_batch("CREATE TABLE catalog_hidden_directories(library_id TEXT NOT NULL,path TEXT NOT NULL,PRIMARY KEY(library_id,path));")?;
    tx.pragma_update(None, "user_version", VERSION)?;
    tx.commit()?;
    Ok(())
}

fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v[key].as_str().with_context(|| format!("missing {key}"))
}
fn existing(db: &Connection, lib: &str, id: &str) -> Result<Option<Value>> {
    let s: Option<String> = db
        .query_row(
            "SELECT snapshot FROM catalog_assets WHERE library_id=? AND id=?",
            params![lib, id],
            |r| r.get(0),
        )
        .optional()?;
    s.map(|s| serde_json::from_str(&s).map_err(Into::into))
        .transpose()
}
fn save(db: &Connection, lib: &str, a: &Value) -> Result<()> {
    db.execute("INSERT INTO catalog_assets VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(library_id,id) DO UPDATE SET snapshot=excluded.snapshot,content_hash=excluded.content_hash,fingerprint=excluded.fingerprint,sort_time=excluded.sort_time,filename=excluded.filename,rating=excluded.rating,flag=excluded.flag,color=excluded.color,trashed=excluded.trashed",params![lib,text(a,"id")?,a.to_string(),text(a,"contentFingerprint")?,text(a,"metadataFingerprint")?,a["captureTime"].as_str().unwrap_or(text(a,"createdAt")?),text(a,"originalFilename")?,a["rating"].as_i64().unwrap_or(0),a["flagState"].as_str().unwrap_or("unflagged"),a["colorLabel"].as_str(),a["trashed"].as_bool().unwrap_or(false)])?;
    Ok(())
}

pub(crate) fn project(db: &Connection, lib: &str, seq: i64, op: &Value) -> Result<()> {
    let p = &op["payload"];
    match op["opType"].as_str().unwrap_or("") {
        "asset_snapshot_declared" => {
            let mut a = p["assetSnapshotDeclared"]["snapshot"].clone();
            let id = text(&a, "assetID")?.to_ascii_lowercase();
            a.as_object_mut()
                .context("snapshot is not an object")?
                .remove("assetID");
            a["id"] = json!(id);
            let lifecycle:Option<String>=db.query_row("SELECT op_type FROM ledger_events WHERE library_id=? AND entity_type='asset' AND entity_id IN (?,?) AND global_seq<=? AND op_type IN ('move_to_trash','restore_from_trash') ORDER BY global_seq DESC LIMIT 1",params![lib,id,id.to_ascii_uppercase(),seq],|r|r.get(0)).optional()?;
            a["trashed"] = json!(lifecycle.as_deref() == Some("move_to_trash"));
            save(db, lib, &a)?;
        }
        "metadata_set" | "tags_updated" | "move_to_trash" | "restore_from_trash" => {
            let case = match op["opType"].as_str().unwrap() {
                "metadata_set" => "metadataSet",
                "tags_updated" => "tagsUpdated",
                "move_to_trash" => "moveToTrash",
                _ => "restoreFromTrash",
            };
            let change = &p[case];
            let id = text(change, "assetID")?.to_ascii_lowercase();
            if let Some(mut a) = existing(db, lib, &id)? {
                match case {
                    "metadataSet" => {
                        let field = match change["field"].as_str().unwrap_or("") {
                            "rating" => "rating",
                            "flag_state" => "flagState",
                            "color_label" => "colorLabel",
                            "caption" => "caption",
                            _ => bail!("invalid metadata field"),
                        };
                        let v = &change["value"];
                        let value = if !v["int"].is_null() {
                            v["int"]["_0"].clone()
                        } else if !v["string"].is_null() {
                            v["string"]["_0"].clone()
                        } else if !v["intValue"].is_null() {
                            v["intValue"].clone()
                        } else if !v["stringValue"].is_null() {
                            v["stringValue"].clone()
                        } else {
                            Value::Null
                        };
                        let valid = match field {
                            "rating" => value.as_i64().is_some(),
                            "flagState" => value
                                .as_str()
                                .is_some_and(|s| ["unflagged", "picked", "rejected"].contains(&s)),
                            "colorLabel" => {
                                value.is_null()
                                    || value.as_str().is_some_and(|s| {
                                        ["red", "yellow", "green", "blue", "purple"].contains(&s)
                                    })
                            }
                            _ => value.is_null() || value.is_string(),
                        };
                        if valid {
                            a[field] = value;
                        }
                    }
                    "tagsUpdated" => {
                        let mut tags: BTreeSet<String> = a["tags"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect();
                        for tag in change["remove"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                        {
                            tags.remove(tag);
                        }
                        for tag in change["add"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Value::as_str)
                        {
                            tags.insert(tag.into());
                        }
                        a["tags"] = json!(tags);
                    }
                    "moveToTrash" => a["trashed"] = json!(true),
                    _ => a["trashed"] = json!(false),
                }
                a["updatedAt"] = op
                    .get("committedAt")
                    .or_else(|| op.get("createdAt"))
                    .cloned()
                    .unwrap_or_else(|| a["updatedAt"].clone());
                save(db, lib, &a)?;
            }
        }
        "file_placement_snapshot_declared"
        | "imported_original_declared"
        | "original_archive_receipt_recorded" => {
            let (case, placement) = match op["opType"].as_str().unwrap() {
                "file_placement_snapshot_declared" => {
                    ("filePlacementSnapshotDeclared", "placement")
                }
                "imported_original_declared" => ("importedOriginalDeclared", "placement"),
                _ => ("originalArchiveReceiptRecorded", "serverPlacement"),
            };
            let p = &p[case];
            let file = &p["fileObject"];
            let place = &p[placement];
            db.execute("INSERT INTO catalog_files(library_id,asset_id,content_hash,size,role,holder,availability) VALUES (?,?,?,?,?,?,?) ON CONFLICT(library_id,asset_id,content_hash,role,holder) DO UPDATE SET availability=excluded.availability",params![lib,text(p,"assetID")?.to_ascii_lowercase(),text(file,"contentHash")?,file["sizeBytes"].as_i64().context("file size missing")?,text(file,"role")?,text(place,"holderID")?,text(place,"availability")?])?;
        }
        _ => {}
    }
    Ok(())
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetQuery {
    pub q: Option<String>,
    pub min_rating: Option<i64>,
    pub flag_state: Option<String>,
    pub color_label: Option<String>,
    pub tag: Option<String>,
    pub trashed: Option<bool>,
    pub sort: Option<String>,
    pub directory: Option<String>,
    pub recursive: Option<bool>,
    pub show_hidden: Option<bool>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    #[serde(rename = "folderID")]
    pub folder_id: Option<String>,
}
fn invalid(message: &str) -> anyhow::Error {
    StoreError {
        status: 422,
        code: "invalid_request".into(),
        message: message.into(),
    }
    .into()
}
fn not_found() -> anyhow::Error {
    StoreError {
        status: 404,
        code: "asset_not_found".into(),
        message: "asset not found".into(),
    }
    .into()
}
fn decorate(db: &Connection, lib: &str, mut a: Value) -> Result<Value> {
    let id = text(&a, "id")?;
    let preview:Option<String>=db.query_row("SELECT json_object('objectRef',json_object('bucket',object_bucket,'key',object_key),'width',pixel_width,'height',pixel_height,'version',json_extract(file_object,'$.contentHash')) FROM derivative_objects WHERE library_id=? AND asset_id=? AND role='preview'",params![lib,id],|r|r.get(0)).optional()?;
    a["_preview"] = preview
        .map(|s| serde_json::from_str(&s))
        .transpose()?
        .unwrap_or(Value::Null);
    Ok(a)
}

fn hidden_filter(
    filter: &mut String,
    values: &mut Vec<rusqlite::types::Value>,
    lib: &str,
    directory: Option<&str>,
) {
    filter.push_str(" AND a.id NOT IN (SELECT p.asset_id FROM catalog_hidden_directories h JOIN catalog_paths p ON p.library_id=h.library_id AND p.path>=rtrim(h.path,'/') || '/' AND p.path<rtrim(h.path,'/') || '0' WHERE h.library_id=? AND NOT (?=h.path OR substr(?,1,length(rtrim(h.path,'/'))+1)=rtrim(h.path,'/') || '/'))");
    values.push(lib.to_string().into());
    let directory = directory
        .map(|p| if p == "/" { p } else { p.trim_end_matches('/') })
        .unwrap_or("");
    values.push(directory.to_string().into());
    values.push(directory.to_string().into());
}

impl Store {
    pub fn hidden_directories(&self, lib: &str) -> Result<Value> {
        let db = self.lock()?;
        let mut statement = db.prepare(
            "SELECT path FROM catalog_hidden_directories WHERE library_id=? ORDER BY path",
        )?;
        let paths = statement
            .query_map([lib], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(json!({"paths": paths}))
    }
    pub fn set_hidden_directory(&self, lib: &str, path: &str, hidden: bool) -> Result<Value> {
        if !path.starts_with('/')
            || path.contains('\0')
            || path.split('/').any(|part| part == "." || part == "..")
            || path.contains("//")
        {
            return Err(invalid(
                "path must be an absolute normalized directory path",
            ));
        }
        let path = if path == "/" {
            path
        } else {
            path.trim_end_matches('/')
        };
        {
            let db = self.lock()?;
            if hidden {
                db.execute("INSERT INTO catalog_hidden_directories(library_id,path) VALUES (?,?) ON CONFLICT DO NOTHING", params![lib,path])?;
            } else {
                db.execute(
                    "DELETE FROM catalog_hidden_directories WHERE library_id=? AND path=?",
                    params![lib, path],
                )?;
            }
        }
        self.hidden_directories(lib)
    }
    pub fn query_assets(&self, lib: &str, q: &AssetQuery) -> Result<Value> {
        let offset = q
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<i64>()
            .map_err(|_| invalid("invalid cursor"))?;
        if offset < 0 {
            return Err(invalid("invalid cursor"));
        }
        let limit = q.limit.unwrap_or(100).clamp(1, 200);
        let order = match q.sort.as_deref().unwrap_or("capture_desc") {
            "capture_desc" => "a.sort_time DESC,a.id",
            "capture_asc" => "a.sort_time ASC,a.id",
            "filename" => "a.filename COLLATE NOCASE,a.id",
            "rating_desc" => "a.rating DESC,a.sort_time DESC,a.id",
            _ => return Err(invalid("invalid sort")),
        };
        let mut filter = "a.library_id=? AND a.trashed=?".to_string();
        let mut values: Vec<rusqlite::types::Value> = vec![
            lib.to_string().into(),
            (q.trashed.unwrap_or(false) as i64).into(),
        ];
        if let Some(s) = q.q.as_ref().filter(|v| !v.is_empty()) {
            filter.push_str(" AND (a.filename LIKE ? ESCAPE '\\' OR json_extract(a.snapshot,'$.cameraModel') LIKE ? ESCAPE '\\')");
            let pattern = format!("%{}%", escape_like(s));
            values.push(pattern.clone().into());
            values.push(pattern.into());
        }
        if let Some(rating) = q.min_rating {
            if !(0..=5).contains(&rating) {
                return Err(invalid("minRating must be 0...5"));
            }
            filter.push_str(" AND a.rating>=?");
            values.push(rating.into());
        }
        for (value, column) in [(&q.flag_state, "a.flag"), (&q.color_label, "a.color")] {
            if let Some(value) = value {
                filter.push_str(&format!(" AND {column}=?"));
                values.push(value.clone().into());
            }
        }
        if let Some(tag) = &q.tag {
            filter.push_str(
                " AND EXISTS(SELECT 1 FROM json_each(a.snapshot,'$.tags') WHERE value=?)",
            );
            values.push(tag.clone().into());
        }
        if let Some(path) = &q.directory {
            // Materialize the directory asset set once; a correlated EXISTS can repeat
            // the entire path range scan for every asset in a large library.
            filter.push_str(DIRECTORY_ASSET_SET);
            values.push(lib.to_string().into());
            let root = path.trim_end_matches('/');
            filter.push_str("f.path>=? AND f.path<?");
            values.push(format!("{root}/").into());
            values.push(format!("{root}0").into());
            if !q.recursive.unwrap_or(true) {
                filter.push_str(" AND substr(f.path,?) NOT LIKE '%/%'");
                values.push(((root.chars().count() + 2) as i64).into());
            }
            filter.push(')');
        }
        if !q.show_hidden.unwrap_or(false) {
            hidden_filter(&mut filter, &mut values, lib, q.directory.as_deref());
        }
        let db = self.lock()?;
        let total: i64 = db.query_row(
            &format!("SELECT count(*) FROM catalog_assets a WHERE {filter}"),
            rusqlite::params_from_iter(&values),
            |r| r.get(0),
        )?;
        let mut statement=db.prepare(&format!("SELECT a.snapshot FROM catalog_assets a WHERE {filter} ORDER BY {order} LIMIT ? OFFSET ?"))?;
        values.push((limit as i64).into());
        values.push(offset.into());
        let rows = statement.query_map(rusqlite::params_from_iter(&values), |r| {
            r.get::<_, String>(0)
        })?;
        let mut items = Vec::new();
        for row in rows {
            items.push(decorate(&db, lib, serde_json::from_str(&row?)?)?);
        }
        let next = offset + items.len() as i64;
        Ok(
            json!({"items":items,"total":total,"nextCursor":if next<total{Some(next.to_string())}else{None}}),
        )
    }
    pub fn asset(&self, lib: &str, id: &str) -> Result<Value> {
        let db = self.lock()?;
        decorate(&db, lib, existing(&db, lib, id)?.ok_or_else(not_found)?)
    }
    pub fn counts(&self, lib: &str, show_hidden: bool) -> Result<Value> {
        let db = self.lock()?;
        let mut filter = "a.library_id=?".to_string();
        let mut values: Vec<rusqlite::types::Value> = vec![lib.to_string().into()];
        if !show_hidden {
            hidden_filter(&mut filter, &mut values, lib, None);
        }
        let result=db.query_row(&format!("SELECT count(*) FILTER(WHERE trashed=0),count(*) FILTER(WHERE trashed=1),count(*) FILTER(WHERE trashed=0 AND flag='picked') FROM catalog_assets a WHERE {filter}"),rusqlite::params_from_iter(&values),|r|Ok(json!({"all":r.get::<_,i64>(0)?,"trashed":r.get::<_,i64>(1)?,"picked":r.get::<_,i64>(2)?})))?;
        Ok(result)
    }
    /// Indexed descendant counts; each asset counts once even with multiple paths.
    pub fn directory_photo_counts(&self, lib: &str, paths: &[String]) -> Result<Vec<i64>> {
        let db = self.lock()?;
        let mut statement = db.prepare("SELECT count(DISTINCT p.asset_id) FROM catalog_paths p JOIN catalog_assets a ON a.library_id=p.library_id AND a.id=p.asset_id WHERE p.library_id=?1 AND p.path>=?2 AND p.path<?3 AND a.trashed=0")?;
        paths
            .iter()
            .map(|path| {
                let root = path.trim_end_matches('/');
                Ok(statement.query_row(
                    params![lib, format!("{root}/"), format!("{root}0")],
                    |row| row.get(0),
                )?)
            })
            .collect()
    }
    pub fn directories(&self, lib: &str) -> Result<Value> {
        let db = self.lock()?;
        let mut statement =
            db.prepare("SELECT path,asset_id FROM catalog_paths WHERE library_id=?")?;
        let paths = statement.query_map([lib], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut counts = std::collections::BTreeMap::<String, BTreeSet<String>>::new();
        for row in paths {
            let (path, id) = row?;
            if let Some(parent) = std::path::Path::new(&path).parent() {
                counts
                    .entry(parent.to_string_lossy().into_owned())
                    .or_default()
                    .insert(id);
            }
        }
        Ok(
            json!({"directories":counts.into_iter().map(|(path,count)|json!({"path":path,"count":count.len()})).collect::<Vec<_>>()}),
        )
    }
    pub fn patch_asset(&self, lib: &str, id: &str, patch: &Value) -> Result<Value> {
        let fields = patch
            .as_object()
            .ok_or_else(|| invalid("metadata patch must be an object"))?;
        if fields.is_empty()
            || fields
                .keys()
                .any(|k| !["rating", "flagState", "colorLabel", "tags"].contains(&k.as_str()))
        {
            return Err(invalid("unsupported metadata patch"));
        }
        if fields
            .get("rating")
            .is_some_and(|v| !v.as_i64().is_some_and(|n| (0..=5).contains(&n)))
        {
            return Err(invalid("rating must be 0...5"));
        }
        if fields.get("flagState").is_some_and(|v| {
            !v.as_str()
                .is_some_and(|s| ["unflagged", "picked", "rejected"].contains(&s))
        }) {
            return Err(invalid("invalid flagState"));
        }
        if fields.get("colorLabel").is_some_and(|v| {
            !v.is_null()
                && !v
                    .as_str()
                    .is_some_and(|s| ["red", "yellow", "green", "blue", "purple"].contains(&s))
        }) {
            return Err(invalid("invalid colorLabel"));
        }
        if fields.get("tags").is_some_and(|v| {
            !v.as_array().is_some_and(|a| {
                a.len() <= 100
                    && a.iter()
                        .all(|v| v.as_str().is_some_and(|s| !s.is_empty() && s.len() <= 128))
            })
        }) {
            return Err(invalid(
                "tags must be an array of at most 100 nonempty strings",
            ));
        }
        let mut db = self.lock()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old = existing(&tx, lib, id)?.ok_or_else(not_found)?;
        for (key, value) in fields {
            if old[key] == *value {
                continue;
            }
            if key == "tags" {
                let before: BTreeSet<&str> = old["tags"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .collect();
                let after: BTreeSet<&str> = value
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(Value::as_str)
                    .collect();
                server_event(
                    &tx,
                    lib,
                    "asset",
                    id,
                    "tags_updated",
                    json!({"tagsUpdated":{"assetID":id,"add":after.difference(&before).collect::<Vec<_>>(),"remove":before.difference(&after).collect::<Vec<_>>()}}),
                )?;
            } else {
                let field = match key.as_str() {
                    "rating" => "rating",
                    "flagState" => "flag_state",
                    _ => "color_label",
                };
                let value = if value.is_null() {
                    json!({"null":{}})
                } else if key == "rating" {
                    json!({"int":{"_0":value}})
                } else {
                    json!({"string":{"_0":value}})
                };
                server_event(
                    &tx,
                    lib,
                    "asset",
                    id,
                    "metadata_set",
                    json!({"metadataSet":{"assetID":id,"field":field,"value":value}}),
                )?;
            }
        }
        let a = decorate(&tx, lib, existing(&tx, lib, id)?.ok_or_else(not_found)?)?;
        tx.commit()?;
        Ok(a)
    }
    pub fn set_trashed(&self, lib: &str, id: &str, trashed: bool) -> Result<Value> {
        let mut db = self.lock()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old = existing(&tx, lib, id)?.ok_or_else(not_found)?;
        if old["trashed"] != trashed {
            let (kind, payload) = if trashed {
                (
                    "move_to_trash",
                    json!({"moveToTrash":{"assetID":id,"reason":"user"}}),
                )
            } else {
                (
                    "restore_from_trash",
                    json!({"restoreFromTrash":{"assetID":id}}),
                )
            };
            server_event(&tx, lib, "asset", id, kind, payload)?;
        }
        let a = decorate(&tx, lib, existing(&tx, lib, id)?.ok_or_else(not_found)?)?;
        tx.commit()?;
        Ok(a)
    }
}
fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub(crate) fn server_event(
    db: &Connection,
    lib: &str,
    entity: &str,
    id: &str,
    kind: &str,
    payload: Value,
) -> Result<()> {
    let last:Option<(i64,String)>=db.query_row("SELECT device_seq,hybrid_logical_time FROM ledger_events WHERE library_id=? AND device_id='keeps-nas' ORDER BY device_seq DESC LIMIT 1",[lib],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let seq = last.as_ref().map_or(1, |v| v.0 + 1);
    let mut wall = Utc::now().timestamp_millis();
    let mut counter = 0;
    if let Some((_, clock)) = last {
        let clock: Value = serde_json::from_str(&clock)?;
        if clock["wallTimeMilliseconds"].as_i64().unwrap_or(0) >= wall {
            wall = clock["wallTimeMilliseconds"].as_i64().unwrap();
            counter = clock["counter"].as_i64().unwrap_or(0) + 1;
        }
    }
    let time = Utc::now().to_rfc3339();
    let mut op = json!({"opID":Uuid::new_v4(),"libraryID":lib,"deviceID":"keeps-nas","deviceSequence":seq,"hybridLogicalTime":{"wallTimeMilliseconds":wall,"counter":counter,"nodeID":"keeps-nas"},"actorID":"server","entityType":entity,"entityID":id,"opType":kind,"payload":payload,"createdAt":time});
    crate::protocol::validate_operation(&mut op).map_err(anyhow::Error::msg)?;
    let result = append_one(db, lib, &op)?;
    ensure!(
        result["status"] == "committed",
        "server command conflicted: {result}"
    );
    Ok(())
}

impl Store {
    pub fn original_is_online(&self, lib: &str, path: &str, hash: &str) -> Result<bool> {
        Ok(self.lock()?.query_row("SELECT EXISTS(SELECT 1 FROM catalog_paths p JOIN catalog_files f ON f.library_id=p.library_id AND f.asset_id=p.asset_id AND f.content_hash=p.content_hash AND f.role=p.role WHERE p.library_id=? AND p.path=? AND p.content_hash=? AND f.holder='keeps-nas' AND f.availability='online')",params![lib,path,hash],|r|r.get(0))?)
    }
    pub fn ingest_original(
        &self,
        lib: &str,
        path: &str,
        file: &Value,
        snapshot: &Value,
    ) -> Result<String> {
        let mut db = self.lock()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let hash = text(file, "contentHash")?;
        let fingerprint = text(snapshot, "metadataFingerprint")?;
        let by_hash: Option<String> = tx
            .query_row(
                "SELECT asset_id FROM catalog_files WHERE library_id=? AND content_hash=? LIMIT 1",
                params![lib, hash],
                |r| r.get(0),
            )
            .optional()?;
        let matched = if by_hash.is_some() {
            by_hash
        } else {
            tx.query_row("SELECT id FROM catalog_assets WHERE library_id=? AND (content_hash=? OR fingerprint=?) ORDER BY CASE WHEN content_hash=? THEN 0 ELSE 1 END,id LIMIT 1",params![lib,hash,fingerprint,hash],|r|r.get(0)).optional()?
        };
        let id = matched
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        if existing(&tx, lib, &id)?.is_none() {
            let mut snapshot = snapshot.clone();
            snapshot["assetID"] = json!(id);
            server_event(
                &tx,
                lib,
                "asset",
                &id,
                "asset_snapshot_declared",
                json!({"assetSnapshotDeclared":{"snapshot":snapshot}}),
            )?;
        }
        let present:Option<String>=tx.query_row("SELECT availability FROM catalog_files WHERE library_id=? AND asset_id=? AND content_hash=? AND role=? AND holder='keeps-nas'",params![lib,id,hash,text(file,"role")?],|r|r.get(0)).optional()?;
        if present.as_deref() != Some("online") {
            let placement = json!({"fileObjectID":file,"holderID":"keeps-nas","storageKind":"nas","authorityRole":"canonical","availability":"online"});
            server_event(
                &tx,
                lib,
                "file_placement",
                &id,
                "file_placement_snapshot_declared",
                json!({"filePlacementSnapshotDeclared":{"assetID":id,"fileObject":file,"placement":placement}}),
            )?;
            if file["role"] != "sidecar" {
                server_event(
                    &tx,
                    lib,
                    "file_placement",
                    &id,
                    "original_archive_receipt_recorded",
                    json!({"originalArchiveReceiptRecorded":{"assetID":id,"fileObject":file,"serverPlacement":placement}}),
                )?;
            }
        }
        tx.execute("INSERT INTO catalog_paths VALUES(?,?,?,?,?) ON CONFLICT(library_id,path) DO UPDATE SET asset_id=excluded.asset_id,content_hash=excluded.content_hash,role=excluded.role",params![lib,path,id,hash,text(file,"role")?])?;
        tx.commit()?;
        Ok(id)
    }
    pub fn declare_generated_preview(&self, lib: &str, id: &str, derivative: &Value) -> Result<()> {
        let mut db = self.lock()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current:Option<String>=tx.query_row("SELECT object_key FROM derivative_objects WHERE library_id=? AND asset_id=? AND role='preview'",params![lib,id],|r|r.get(0)).optional()?;
        if current.as_deref() != derivative["objectRef"]["key"].as_str() {
            let entity = format!(
                "{id}:preview:{}",
                text(&derivative["fileObject"], "contentHash")?
            );
            server_event(
                &tx,
                lib,
                "derivative_object",
                &entity,
                "derivative_declared",
                json!({"derivativeDeclared":{"assetID":id,"derivative":derivative}}),
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn mark_missing_under(&self, lib: &str, directory: &str) -> Result<usize> {
        let mut db = self.lock()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let files = {
            let mut statement=tx.prepare("SELECT p.asset_id,p.path,p.content_hash,f.size,p.role FROM catalog_paths p JOIN catalog_files f ON f.library_id=p.library_id AND f.asset_id=p.asset_id AND f.content_hash=p.content_hash AND f.role=p.role WHERE p.library_id=? AND f.holder='keeps-nas' AND p.path LIKE ? ESCAPE '\\'")?;
            statement
                .query_map(
                    params![
                        lib,
                        format!("{}/%", escape_like(directory.trim_end_matches('/')))
                    ],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, i64>(3)?,
                            r.get::<_, String>(4)?,
                        ))
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut missing = 0;
        for (id, path, hash, size, role) in files {
            match std::fs::symlink_metadata(&path) {
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).with_context(|| format!("check original {path}")),
            }
            tx.execute(
                "DELETE FROM catalog_paths WHERE library_id=? AND path=?",
                params![lib, path],
            )?;
            let other:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM catalog_paths WHERE library_id=? AND asset_id=? AND content_hash=? AND role=?)",params![lib,id,hash,role],|r|r.get(0))?;
            if other {
                continue;
            }
            let file = json!({"contentHash":hash,"sizeBytes":size,"role":role});
            let placement = json!({"fileObjectID":file,"holderID":"keeps-nas","storageKind":"nas","authorityRole":"canonical","availability":"missing"});
            server_event(
                &tx,
                lib,
                "file_placement",
                &id,
                "file_placement_snapshot_declared",
                json!({"filePlacementSnapshotDeclared":{"assetID":id,"fileObject":file,"placement":placement}}),
            )?;
            missing += 1;
        }
        tx.commit()?;
        Ok(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot() -> Value {
        json!({"assetID":Uuid::new_v4(),"captureTime":"2024-01-01T00:00:00Z","cameraMake":"Canon","cameraModel":"R3","lensModel":"50mm","originalFilename":"photo.jpg","contentFingerprint":"photo-hash","metadataFingerprint":"2024|Canon|R3|photo","rating":1,"flagState":"unflagged","tags":[],"createdAt":"2024-01-01T00:00:00Z","updatedAt":"2024-01-01T00:00:00Z"})
    }
    #[test]
    fn mixed_paths_remain_hidden_and_v1_migration_preserves_assets() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let store = Store::open(&path, true, Default::default())?;
        let file = json!({"contentHash":"photo-hash","sizeBytes":100,"role":"jpeg_original"});
        let asset = snapshot();
        let id = store.ingest_original("lib", "/private/photo.jpg", &file, &asset)?;
        assert_eq!(
            store.ingest_original("lib", "/public/photo.jpg", &file, &asset)?,
            id
        );
        store.set_hidden_directory("lib", "/private", true)?;
        assert_eq!(
            store.query_assets(
                "lib",
                &AssetQuery {
                    directory: Some("/public".into()),
                    ..Default::default()
                }
            )?["total"],
            0
        );
        assert_eq!(
            store.query_assets(
                "lib",
                &AssetQuery {
                    directory: Some("/private".into()),
                    ..Default::default()
                }
            )?["total"],
            1
        );
        store.set_hidden_directory("lib", "/", true)?;
        assert_eq!(
            store.query_assets("lib", &AssetQuery::default())?["total"],
            0
        );
        assert_eq!(
            store.query_assets(
                "lib",
                &AssetQuery {
                    directory: Some("/private".into()),
                    ..Default::default()
                }
            )?["total"],
            1
        );
        drop(store);
        let db = Connection::open(&path)?;
        db.execute_batch("DROP TABLE catalog_hidden_directories; PRAGMA user_version=1;")?;
        drop(db);
        assert!(Store::open(&path, false, Default::default()).is_err());
        let store = Store::open(&path, true, Default::default())?;
        assert_eq!(store.counts("lib", false)?["all"], 1);
        assert_eq!(store.hidden_directories("lib")?, json!({"paths":[]}));
        drop(store);
        Store::open(&path, false, Default::default())?;
        Ok(())
    }
    #[test]
    fn nas_commands_projection_and_idempotency() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("db"), true, Default::default()).unwrap();
        let file = json!({"contentHash":"photo-hash","sizeBytes":100,"role":"jpeg_original"});
        let id = store
            .ingest_original("lib", "/photos/photo.jpg", &file, &snapshot())
            .unwrap();
        assert_eq!(
            store
                .ingest_original("lib", "/photos/photo.jpg", &file, &snapshot())
                .unwrap(),
            id
        );
        assert_eq!(store.counts("lib", false).unwrap()["all"], 1);
        let result = store
            .patch_asset(
                "lib",
                &id,
                &json!({"rating":5,"flagState":"picked","tags":["旅行","北京"],"colorLabel":"red"}),
            )
            .unwrap();
        assert_eq!(result["rating"], 5);
        let before = store.fetch_operations("lib", 0, 100).unwrap();
        store.patch_asset("lib", &id, &json!({"rating":5})).unwrap();
        assert_eq!(store.fetch_operations("lib", 0, 100).unwrap(), before);
        let q = AssetQuery {
            tag: Some("北京".into()),
            min_rating: Some(4),
            ..Default::default()
        };
        assert_eq!(store.query_assets("lib", &q).unwrap()["total"], 1);
        store.set_trashed("lib", &id, true).unwrap();
        assert_eq!(store.counts("lib", false).unwrap()["trashed"], 1);
        assert_eq!(
            store.query_assets("lib", &AssetQuery::default()).unwrap()["total"],
            0
        );
        store.set_trashed("lib", &id, false).unwrap();
        assert!(store.patch_asset("lib", &id, &json!({"rating":6})).is_err());
        assert_eq!(store.asset("lib", &id).unwrap()["rating"], 5);
    }
    #[test]
    fn lifecycle_before_snapshot_and_invalid_legacy_metadata() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("db"), true, Default::default())?;
        let mut a = snapshot();
        let id = a["assetID"].as_str().unwrap().to_owned();
        let mut db = store.lock()?;
        let tx = db.transaction()?;
        server_event(
            &tx,
            "lib",
            "asset",
            &id,
            "move_to_trash",
            json!({"moveToTrash":{"assetID":id,"reason":"user"}}),
        )?;
        server_event(
            &tx,
            "lib",
            "asset",
            &id,
            "asset_snapshot_declared",
            json!({"assetSnapshotDeclared":{"snapshot":a}}),
        )?;
        server_event(
            &tx,
            "lib",
            "asset",
            &id,
            "metadata_set",
            json!({"metadataSet":{"assetID":id,"field":"rating","value":{"null":{}}}}),
        )?;
        assert_eq!(existing(&tx, "lib", &id)?.unwrap()["rating"], 1);
        assert_eq!(existing(&tx, "lib", &id)?.unwrap()["trashed"], true);
        server_event(
            &tx,
            "lib",
            "asset",
            &id,
            "restore_from_trash",
            json!({"restoreFromTrash":{"assetID":id}}),
        )?;
        a["rating"] = json!(3);
        server_event(
            &tx,
            "lib",
            "asset",
            &id,
            "asset_snapshot_declared",
            json!({"assetSnapshotDeclared":{"snapshot":a}}),
        )?;
        assert_eq!(existing(&tx, "lib", &id)?.unwrap()["rating"], 3);
        assert_eq!(existing(&tx, "lib", &id)?.unwrap()["trashed"], false);
        tx.commit()?;
        Ok(())
    }
    #[test]
    fn duplicate_paths_and_missing_restore_keep_one_asset() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let photos = dir.path().join("photos");
        std::fs::create_dir(&photos)?;
        let a = photos.join("one.jpg");
        let b = photos.join("two.jpg");
        std::fs::write(&a, b"same")?;
        std::fs::write(&b, b"same")?;
        let store = Store::open(&dir.path().join("db"), true, Default::default())?;
        let file = json!({"contentHash":"same","sizeBytes":4,"role":"jpeg_original"});
        let id = store.ingest_original("lib", a.to_str().unwrap(), &file, &snapshot())?;
        assert_eq!(
            store.ingest_original("lib", b.to_str().unwrap(), &file, &snapshot())?,
            id
        );
        assert!(store.original_is_online("lib", a.to_str().unwrap(), "same")?);
        std::fs::remove_file(&a)?;
        assert_eq!(
            store.mark_missing_under("lib", photos.to_str().unwrap())?,
            0
        );
        assert!(store.original_is_online("lib", b.to_str().unwrap(), "same")?);
        std::fs::remove_file(&b)?;
        assert_eq!(
            store.mark_missing_under("lib", photos.to_str().unwrap())?,
            1
        );
        std::fs::write(&a, b"same")?;
        assert_eq!(
            store.ingest_original("lib", a.to_str().unwrap(), &file, &snapshot())?,
            id
        );
        assert!(store.original_is_online("lib", a.to_str().unwrap(), "same")?);
        assert_eq!(store.counts("lib", false)?["all"], 1);
        Ok(())
    }
    #[test]
    fn explicit_migration_gate_and_legacy_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(include_str!("../tests/fixtures/python_ledger.sql"))
            .unwrap();
        drop(db);
        assert!(Store::open(&path, false, Default::default()).is_err());
        let store = Store::open(&path, true, Default::default()).unwrap();
        assert_eq!(
            store.fetch_operations("library-a", 0, 100).unwrap()["operations"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        drop(store);
        Store::open(&path, false, Default::default()).unwrap();
    }
}

#[cfg(test)]
mod directory_plan_tests {
    use super::*;

    #[test]
    fn directory_asset_set_is_not_correlated_per_catalog_asset() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE catalog_assets(library_id TEXT,id TEXT,trashed INTEGER,PRIMARY KEY(library_id,id)); CREATE INDEX catalog_sort ON catalog_assets(library_id,trashed); CREATE TABLE catalog_paths(library_id TEXT,path TEXT,asset_id TEXT,PRIMARY KEY(library_id,path)); CREATE INDEX catalog_paths_asset ON catalog_paths(library_id,asset_id);")?;
        for suffix in [
            "f.path>=? AND f.path<?)",
            "f.path>=? AND f.path<? AND substr(f.path,7) NOT LIKE '%/%')",
        ] {
            let mut query = db.prepare(&format!("EXPLAIN QUERY PLAN SELECT count(*) FROM catalog_assets a WHERE a.library_id=? AND a.trashed=?{DIRECTORY_ASSET_SET}{suffix}"))?;
            let plan = query
                .query_map(
                    params!["photos", 0, "photos", "/photo/", "/photo0"],
                    |row| row.get::<_, String>(3),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert!(
                plan.iter().all(|step| !step.contains("CORRELATED")),
                "{plan:?}"
            );
            assert!(
                plan.iter().any(|step| step.contains("LIST SUBQUERY")),
                "{plan:?}"
            );
            assert!(
                plan.iter()
                    .any(|step| step.contains("path>?") && step.contains("path<?")),
                "{plan:?}"
            );
        }
        Ok(())
    }
}
