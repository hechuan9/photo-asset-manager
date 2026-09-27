use crate::protocol::{canonical_payload, validate_operation};
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    path::Path,
    sync::{Mutex, MutexGuard},
    time::Duration,
};
use uuid::Uuid;

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
fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v[key]
        .as_str()
        .ok_or_else(|| invalid(422, "invalid_request", &format!("{key} must be a string")))
}
fn validation_error(message: String) -> anyhow::Error {
    let code = match message.as_str() {
        "payload_case_mismatch"
        | "entity_type_mismatch"
        | "entity_id_mismatch"
        | "derivative_asset_id_mismatch" => message.as_str(),
        _ => "invalid_operation",
    };
    invalid(422, code, &message)
}
fn now() -> String {
    Utc::now().to_rfc3339()
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS ledger_sequence_counters(library_id VARCHAR PRIMARY KEY NOT NULL,next_global_seq BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS ledger_events(library_id VARCHAR NOT NULL,global_seq BIGINT NOT NULL,op_id VARCHAR(36) NOT NULL UNIQUE,device_id VARCHAR NOT NULL,device_seq BIGINT NOT NULL,hybrid_logical_time JSON NOT NULL,actor_id VARCHAR NOT NULL,entity_type VARCHAR NOT NULL,entity_id VARCHAR NOT NULL,op_type VARCHAR NOT NULL,payload_json JSON NOT NULL,payload_hash VARCHAR(64) NOT NULL,base_version VARCHAR,committed_at DATETIME NOT NULL,PRIMARY KEY(library_id,global_seq),UNIQUE(library_id,device_id,device_seq));
CREATE INDEX IF NOT EXISTS ix_ledger_events_entity ON ledger_events(library_id,entity_type,entity_id,global_seq);
CREATE INDEX IF NOT EXISTS ix_ledger_events_op_type ON ledger_events(library_id,op_type,global_seq);
CREATE TABLE IF NOT EXISTS device_states(library_id VARCHAR NOT NULL,device_id VARCHAR NOT NULL,actor_id VARCHAR NOT NULL,last_seen_at DATETIME NOT NULL,last_uploaded_device_seq BIGINT NOT NULL,last_pull_cursor BIGINT NOT NULL,capabilities JSON NOT NULL,PRIMARY KEY(library_id,device_id));
CREATE TABLE IF NOT EXISTS derivative_objects(library_id VARCHAR NOT NULL,asset_id VARCHAR(36) NOT NULL,role VARCHAR NOT NULL,file_object JSON NOT NULL,object_bucket VARCHAR NOT NULL,object_key VARCHAR NOT NULL,object_etag VARCHAR,pixel_width BIGINT NOT NULL,pixel_height BIGINT NOT NULL,declared_event_seq BIGINT NOT NULL,updated_at DATETIME NOT NULL,PRIMARY KEY(library_id,asset_id,role));
CREATE TABLE IF NOT EXISTS archive_receipts(library_id VARCHAR NOT NULL,asset_id VARCHAR(36) NOT NULL,receipt_event_seq BIGINT NOT NULL,file_object JSON NOT NULL,server_placement JSON NOT NULL,committed_at DATETIME NOT NULL,PRIMARY KEY(library_id,asset_id,receipt_event_seq));
CREATE TABLE IF NOT EXISTS sync_conflicts(id VARCHAR(36) PRIMARY KEY NOT NULL,library_id VARCHAR NOT NULL,entity_type VARCHAR NOT NULL,entity_id VARCHAR NOT NULL,conflict_type VARCHAR NOT NULL,left_op_id VARCHAR(36),right_op_id VARCHAR(36),detail JSON NOT NULL,created_at DATETIME NOT NULL);
"#;
const EVENT_COLUMNS: &str = "library_id,global_seq,op_id,device_id,device_seq,hybrid_logical_time,actor_id,entity_type,entity_id,op_type,payload_json,payload_hash,base_version,committed_at";
fn parse_json(row: &Row<'_>, index: usize) -> rusqlite::Result<Value> {
    let s: String = row.get(index)?;
    serde_json::from_str(&s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(e))
    })
}
fn timestamp(row: &Row<'_>, index: usize) -> rusqlite::Result<String> {
    let raw: String = row.get(index)?;
    if let Ok(value) = chrono::DateTime::parse_from_rfc3339(&raw) {
        return Ok(value.to_rfc3339());
    }
    chrono::NaiveDateTime::parse_from_str(&raw, "%Y-%m-%d %H:%M:%S%.f")
        .map(|value| value.and_utc().to_rfc3339())
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
}
fn event(row: &Row<'_>) -> rusqlite::Result<Value> {
    Ok(
        json!({"libraryID":row.get::<_,String>(0)?,"globalSeq":row.get::<_,i64>(1)?,"opID":row.get::<_,String>(2)?,"deviceID":row.get::<_,String>(3)?,"deviceSequence":row.get::<_,i64>(4)?,"hybridLogicalTime":parse_json(row,5)?,"actorID":row.get::<_,String>(6)?,"entityType":row.get::<_,String>(7)?,"entityID":row.get::<_,String>(8)?,"opType":row.get::<_,String>(9)?,"payload":parse_json(row,10)?,"payloadHash":row.get::<_,String>(11)?,"baseVersion":row.get::<_,Option<String>>(12)?,"createdAt":timestamp(row,13)?,"committedAt":timestamp(row,13)?}),
    )
}
fn accepted(op: &Value) -> Value {
    json!({"status":"committed","opID":op["opID"],"globalSeq":op["globalSeq"],"committedAt":op["committedAt"],"payloadHash":op["payloadHash"]})
}

pub struct Store {
    connection: Mutex<Connection>,
    trusted: HashSet<String>,
}
impl Store {
    pub fn open(path: &Path, auto_create: bool, trusted: HashSet<String>) -> Result<Self> {
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
        if auto_create {
            connection.execute_batch(SCHEMA)?;
        }
        for (table, columns) in [
            ("ledger_events", EVENT_COLUMNS),
            ("ledger_sequence_counters", "library_id,next_global_seq"),
            (
                "device_states",
                "library_id,device_id,actor_id,last_seen_at,last_uploaded_device_seq,last_pull_cursor,capabilities",
            ),
            (
                "derivative_objects",
                "library_id,asset_id,role,file_object,object_bucket,object_key,object_etag,pixel_width,pixel_height,declared_event_seq,updated_at",
            ),
            (
                "archive_receipts",
                "library_id,asset_id,receipt_event_seq,file_object,server_placement,committed_at",
            ),
            (
                "sync_conflicts",
                "id,library_id,entity_type,entity_id,conflict_type,left_op_id,right_op_id,detail,created_at",
            ),
        ] {
            connection
                .prepare(&format!("SELECT {columns} FROM {table} LIMIT 0"))
                .with_context(|| {
                    format!(
                        "invalid database schema: required table {table} or columns are missing"
                    )
                })?;
        }
        crate::catalog::ensure_schema(&mut connection, auto_create)?;
        Ok(Self {
            connection: Mutex::new(connection),
            trusted,
        })
    }
    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("database mutex poisoned"))
    }
    pub fn append_operations(&self, library: &str, request: &Value) -> Result<Value> {
        let mut operations = request["operations"]
            .as_array()
            .ok_or_else(|| invalid(422, "invalid_request", "operations must be an array"))?
            .clone();
        for op in &mut operations {
            validate_operation(op).map_err(validation_error)?;
            if op["opType"] == "original_archive_receipt_recorded"
                && op["actorID"] != "server"
                && !self.trusted.contains(string(op, "deviceID")?)
            {
                return Err(invalid(
                    403,
                    "forbidden",
                    "archive receipts require actorID=server or a trusted deviceID",
                ));
            }
        }
        let mut db = self.lock()?;
        let mut accepted_ops = Vec::new();
        let mut conflicts = Vec::new();
        for op in operations {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let outcome = append_one(&tx, library, &op)?;
            if outcome.get("conflictType").is_some() {
                conflicts.push(outcome);
            } else {
                accepted_ops.push(outcome);
            }
            tx.commit()?;
        }
        let cursor: i64 = db.query_row(
            "SELECT COALESCE(MAX(global_seq),0) FROM ledger_events WHERE library_id=?",
            [library],
            |r| r.get(0),
        )?;
        Ok(json!({"accepted":accepted_ops,"conflicts":conflicts,"cursor":cursor.to_string()}))
    }
    pub fn fetch_operations(&self, library: &str, after: i64, limit: usize) -> Result<Value> {
        let db = self.lock()?;
        let mut stmt=db.prepare(&format!("SELECT {EVENT_COLUMNS} FROM ledger_events WHERE library_id=? AND global_seq>? ORDER BY global_seq LIMIT ?"))?;
        let mut ops = stmt
            .query_map(params![library, after, (limit + 1) as i64], event)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let more = ops.len() > limit;
        ops.truncate(limit);
        let cursor = ops
            .last()
            .and_then(|o| o["globalSeq"].as_i64())
            .unwrap_or(after);
        Ok(json!({"operations":ops,"cursor":cursor.to_string(),"hasMore":more}))
    }
    pub fn upsert_heartbeat(&self, r: &Value) -> Result<Value> {
        let library = string(r, "libraryID")?;
        let device = string(r, "deviceID")?;
        let seen = r["sentAt"].as_str().map(str::to_owned).unwrap_or_else(now);
        let mut caps = json!({"appVersion":r["appVersion"],"localPendingCount":r["localPendingCount"],"placementSummary":r.get("placementSummary").filter(|v|!v.is_null()).cloned().unwrap_or(json!([])),"placements":r.get("placements").cloned().unwrap_or(json!([]))});
        for k in ["lastUploadedDeviceSeq", "lastPullCursor"] {
            if !r[k].is_null() {
                caps[k] = r[k].clone();
            }
        }
        let db = self.lock()?;
        db.execute("INSERT INTO device_states VALUES (?1,?2,COALESCE(?3,'unknown'),?4,COALESCE(?5,0),COALESCE(?6,0),?7) ON CONFLICT(library_id,device_id) DO UPDATE SET actor_id=COALESCE(?3,actor_id),last_seen_at=?4,last_uploaded_device_seq=COALESCE(?5,last_uploaded_device_seq),last_pull_cursor=COALESCE(?6,last_pull_cursor),capabilities=?7",params![library,device,r["actorID"].as_str(),seen,r["lastUploadedDeviceSeq"].as_i64(),r["lastPullCursor"].as_i64(),caps.to_string()])?;
        Ok(db.query_row("SELECT actor_id,last_seen_at,last_uploaded_device_seq,last_pull_cursor,capabilities FROM device_states WHERE library_id=? AND device_id=?",params![library,device],|row|Ok(json!({"libraryID":library,"deviceID":device,"actorID":row.get::<_,String>(0)?,"lastSeenAt":timestamp(row,1)?,"lastUploadedDeviceSeq":row.get::<_,i64>(2)?,"lastPullCursor":row.get::<_,i64>(3)?,"capabilities":parse_json(row,4)?})))?)
    }
    pub fn record_archive_receipt(&self, r: &Value) -> Result<Value> {
        let mut op = r["operation"].clone();
        validate_operation(&mut op).map_err(validation_error)?;
        if op["opType"] != "original_archive_receipt_recorded" {
            return Err(invalid(
                422,
                "invalid_operation",
                "archive receipt requires originalArchiveReceiptRecorded",
            ));
        }
        if op["actorID"] != "server" && !self.trusted.contains(string(&op, "deviceID")?) {
            return Err(invalid(
                403,
                "forbidden",
                "archive receipts require actorID=server or a trusted deviceID",
            ));
        }
        let result =
            self.append_operations(string(&op, "libraryID")?, &json!({"operations":[op]}))?;
        if !result["conflicts"].as_array().unwrap().is_empty() {
            return Err(invalid(
                409,
                "archive_conflict",
                &result["conflicts"].to_string(),
            ));
        }
        Ok(
            json!({"status":"committed","globalSeq":result["accepted"][0]["globalSeq"],"assetID":op["payload"]["originalArchiveReceiptRecorded"]["assetID"]}),
        )
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
        tx.commit()?;
        Ok(object)
    }
}
pub(crate) fn append_one(db: &Connection, library: &str, op: &Value) -> Result<Value> {
    if op["libraryID"] != library {
        return Ok(
            json!({"opID":op["opID"],"conflictType":"library_mismatch","detail":{"expectedLibraryID":library,"actualLibraryID":op["libraryID"]}}),
        );
    }
    let payload = canonical_payload(&op["payload"]);
    let hash = format!("{:x}", Sha256::digest(payload.as_bytes()));
    let existing = db
        .query_row(
            &format!("SELECT {EVENT_COLUMNS} FROM ledger_events WHERE op_id=?"),
            [string(op, "opID")?],
            event,
        )
        .optional()?;
    if let Some(existing) = existing {
        let same_identity = [
            "libraryID",
            "deviceID",
            "deviceSequence",
            "actorID",
            "entityType",
            "entityID",
            "opType",
            "baseVersion",
            "hybridLogicalTime",
        ]
        .iter()
        .all(|key| existing[key] == op[key]);
        if existing["payloadHash"] == hash && same_identity {
            return Ok(accepted(&existing));
        }
        let (kind, detail) = if existing["payloadHash"] != hash {
            (
                "duplicate_op_id_payload_mismatch",
                json!({"payloadHash":hash,"existingPayloadHash":existing["payloadHash"],"globalSeq":existing["globalSeq"]}),
            )
        } else {
            (
                "duplicate_op_id_identity_mismatch",
                json!({"existingLibraryID":existing["libraryID"],"libraryID":library,"existingDeviceID":existing["deviceID"],"deviceID":op["deviceID"],"existingDeviceSequence":existing["deviceSequence"],"deviceSequence":op["deviceSequence"],"existingEntityType":existing["entityType"],"entityType":op["entityType"],"existingEntityID":existing["entityID"],"entityID":op["entityID"],"existingOpType":existing["opType"],"opType":op["opType"],"globalSeq":existing["globalSeq"]}),
            )
        };
        return conflict(db, library, op, kind, existing["opID"].as_str(), detail);
    }
    let seq_existing=db.query_row("SELECT op_id,global_seq FROM ledger_events WHERE library_id=? AND device_id=? AND device_seq=?",params![library,string(op,"deviceID")?,op["deviceSequence"].as_i64()],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))).optional()?;
    if let Some((left, seq)) = seq_existing {
        return conflict(
            db,
            library,
            op,
            "duplicate_device_sequence",
            Some(&left),
            json!({"deviceID":op["deviceID"],"deviceSequence":op["deviceSequence"],"existingGlobalSeq":seq}),
        );
    }
    db.execute("INSERT INTO ledger_sequence_counters(library_id,next_global_seq) VALUES (?1,(SELECT COALESCE(MAX(global_seq),0)+1 FROM ledger_events WHERE library_id=?1)) ON CONFLICT(library_id) DO NOTHING",[library])?;
    let seq:i64=db.query_row("UPDATE ledger_sequence_counters SET next_global_seq=next_global_seq+1 WHERE library_id=? RETURNING next_global_seq-1",[library],|r|r.get(0))?;
    let time = now();
    db.execute(
        "INSERT INTO ledger_events VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            library,
            seq,
            string(op, "opID")?,
            string(op, "deviceID")?,
            op["deviceSequence"].as_i64(),
            op["hybridLogicalTime"].to_string(),
            string(op, "actorID")?,
            string(op, "entityType")?,
            string(op, "entityID")?,
            string(op, "opType")?,
            payload,
            hash,
            op["baseVersion"].as_str(),
            time
        ],
    )?;
    project(db, library, seq, &time, op)?;
    Ok(
        json!({"status":"committed","opID":op["opID"],"globalSeq":seq,"committedAt":time,"payloadHash":hash}),
    )
}
fn conflict(
    db: &Connection,
    library: &str,
    op: &Value,
    kind: &str,
    left: Option<&str>,
    detail: Value,
) -> Result<Value> {
    db.execute(
        "INSERT INTO sync_conflicts VALUES (?,?,?,?,?,?,?,?,?)",
        params![
            Uuid::new_v4().to_string(),
            library,
            string(op, "entityType")?,
            string(op, "entityID")?,
            kind,
            left,
            string(op, "opID")?,
            detail.to_string(),
            now()
        ],
    )?;
    Ok(json!({"opID":op["opID"],"conflictType":kind,"detail":detail}))
}
fn project(db: &Connection, library: &str, seq: i64, time: &str, op: &Value) -> Result<()> {
    crate::catalog::project(db, library, seq, op)?;
    match op["opType"].as_str() {
        Some("derivative_declared") => {
            let p = &op["payload"]["derivativeDeclared"];
            let d = &p["derivative"];
            db.execute("INSERT INTO derivative_objects VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(library_id,asset_id,role) DO UPDATE SET file_object=excluded.file_object,object_bucket=excluded.object_bucket,object_key=excluded.object_key,object_etag=excluded.object_etag,pixel_width=excluded.pixel_width,pixel_height=excluded.pixel_height,declared_event_seq=excluded.declared_event_seq,updated_at=excluded.updated_at",params![library,string(p,"assetID")?,string(d,"role")?,d["fileObject"].to_string(),string(&d["objectRef"],"bucket")?,string(&d["objectRef"],"key")?,d["objectRef"]["eTag"].as_str(),d["pixelSize"]["width"].as_i64(),d["pixelSize"]["height"].as_i64(),seq,time])?;
        }
        Some("original_archive_receipt_recorded") => {
            let p = &op["payload"]["originalArchiveReceiptRecorded"];
            db.execute(
                "INSERT INTO archive_receipts VALUES (?,?,?,?,?,?)",
                params![
                    library,
                    string(p, "assetID")?,
                    seq,
                    p["fileObject"].to_string(),
                    p["serverPlacement"].to_string(),
                    time
                ],
            )?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn operation(seq: i64) -> Value {
        json!({"opID":Uuid::new_v4().to_string(),"libraryID":"library-a","deviceID":"mac","deviceSequence":seq,"hybridLogicalTime":{"wallTimeMilliseconds":1700000000000_i64+seq,"counter":0,"nodeID":"mac"},"actorID":"user","entityType":"asset","entityID":"00000000-0000-0000-0000-00000000a001","opType":"metadata_set","payload":{"metadataSet":{"assetID":"00000000-0000-0000-0000-00000000a001","field":"rating","value":{"intValue":4}}},"createdAt":"2024-01-01T00:00:00Z"})
    }
    #[test]
    fn ledger_retries_conflicts_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ledger.sqlite");
        let store = Store::open(&path, true, HashSet::new()).unwrap();
        let op = operation(1);
        let request = json!({"operations":[op]});
        let first = store.append_operations("library-a", &request).unwrap();
        assert_eq!(first["accepted"][0]["globalSeq"], 1);
        assert_eq!(
            store.append_operations("library-a", &request).unwrap(),
            first
        );
        let mut changed = op.clone();
        changed["payload"]["metadataSet"]["value"]["intValue"] = json!(5);
        assert_eq!(
            store
                .append_operations("library-a", &json!({"operations":[changed]}))
                .unwrap()["conflicts"][0]["conflictType"],
            "duplicate_op_id_payload_mismatch"
        );
        assert_eq!(
            store
                .append_operations("library-a", &json!({"operations":[operation(1)]}))
                .unwrap()["conflicts"][0]["conflictType"],
            "duplicate_device_sequence"
        );
        drop(store);
        let store = Store::open(&path, false, HashSet::new()).unwrap();
        assert_eq!(
            store.append_operations("library-a", &request).unwrap(),
            first
        );
        assert_eq!(
            store
                .append_operations("library-a", &json!({"operations":[operation(2)]}))
                .unwrap()["accepted"][0]["globalSeq"],
            2
        );
        let page = store.fetch_operations("library-a", 0, 1).unwrap();
        assert_eq!(page["hasMore"], true);
        assert_eq!(page["cursor"], "1");
    }
    #[test]
    fn existing_python_schema_and_data_are_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(include_str!("../tests/fixtures/python_ledger.sql"))
            .unwrap();
        drop(db);
        let mut op = operation(1);
        op["opID"] = json!("00000000-0000-0000-0000-000000000001");
        let store = Store::open(&path, true, HashSet::new()).unwrap();
        let replay = store
            .append_operations("library-a", &json!({"operations":[op]}))
            .unwrap();
        assert_eq!(replay["accepted"][0]["globalSeq"], 1);
        assert_eq!(
            store
                .append_operations("library-a", &json!({"operations":[operation(2)]}))
                .unwrap()["accepted"][0]["globalSeq"],
            2
        );
        assert_eq!(
            store.fetch_operations("library-a", 0, 500).unwrap()["operations"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }
    #[test]
    fn batch_validation_happens_before_any_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("db"), true, HashSet::new()).unwrap();
        let mut invalid_op = operation(2);
        invalid_op["entityID"] = json!("wrong");
        assert!(
            store
                .append_operations(
                    "library-a",
                    &json!({"operations":[operation(1),invalid_op]})
                )
                .is_err()
        );
        assert_eq!(
            store.fetch_operations("library-a", 0, 500).unwrap()["operations"],
            json!([])
        );
    }
    #[test]
    fn same_payload_with_changed_identity_is_a_persisted_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("db"), true, HashSet::new()).unwrap();
        let mut op = operation(1);
        store
            .append_operations("library-a", &json!({"operations":[op]}))
            .unwrap();
        op["actorID"] = json!("another-user");
        let result = store
            .append_operations("library-a", &json!({"operations":[op]}))
            .unwrap();
        assert_eq!(
            result["conflicts"][0]["conflictType"],
            "duplicate_op_id_identity_mismatch"
        );
        assert_eq!(
            store
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM sync_conflicts", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    #[test]
    fn archive_authority_is_checked_on_generic_upload_and_receipts_persist() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("db"), true, HashSet::new()).unwrap();
        let mut op = operation(1);
        op["opType"] = json!("original_archive_receipt_recorded");
        op["entityType"] = json!("file_placement");
        let file = json!({"contentHash":"hash","sizeBytes":1,"role":"raw_original"});
        op["payload"] = json!({"originalArchiveReceiptRecorded":{"assetID":op["entityID"],"fileObject":file,"serverPlacement":{"fileObjectID":file,"holderID":"server","storageKind":"nas","authorityRole":"canonical","availability":"online"}}});
        let error = store
            .append_operations("library-a", &json!({"operations":[operation(2),op]}))
            .unwrap_err();
        assert_eq!(error.downcast_ref::<StoreError>().unwrap().status, 403);
        assert_eq!(
            store.fetch_operations("library-a", 0, 500).unwrap()["operations"],
            json!([])
        );
        op["actorID"] = json!("server");
        let receipt = store
            .record_archive_receipt(&json!({"operation":op}))
            .unwrap();
        assert_eq!(receipt["status"], "committed");
        assert_eq!(
            store
                .lock()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM archive_receipts", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    #[test]
    fn startup_rejects_each_missing_required_table() {
        for table in [
            "ledger_events",
            "ledger_sequence_counters",
            "device_states",
            "derivative_objects",
            "archive_receipts",
            "sync_conflicts",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let db = Connection::open(&path).unwrap();
            db.execute_batch(SCHEMA).unwrap();
            db.execute_batch(&format!("DROP TABLE {table}")).unwrap();
            drop(db);
            let error = Store::open(&path, false, HashSet::new())
                .err()
                .expect("missing table must prevent startup");
            assert!(error.to_string().contains(table));
            assert!(format!("{error:#}").contains("no such table"));
        }
    }
    #[test]
    fn startup_rejects_missing_required_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(SCHEMA).unwrap();
        db.execute_batch("ALTER TABLE device_states DROP COLUMN capabilities")
            .unwrap();
        drop(db);
        let error = Store::open(&path, false, HashSet::new())
            .err()
            .expect("missing column must prevent startup");
        assert!(error.to_string().contains("device_states"));
        assert!(format!("{error:#}").contains("capabilities"));
    }
    #[test]
    fn heartbeat_omitted_cursors_are_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("db"), true, HashSet::new()).unwrap();
        store.upsert_heartbeat(&json!({"libraryID":"a","deviceID":"mac","lastUploadedDeviceSeq":9,"lastPullCursor":4})).unwrap();
        let result = store
            .upsert_heartbeat(&json!({"libraryID":"a","deviceID":"mac"}))
            .unwrap();
        assert_eq!(result["lastUploadedDeviceSeq"], 9);
        assert_eq!(result["lastPullCursor"], 4);
    }
}
