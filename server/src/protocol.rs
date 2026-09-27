use chrono::{DateTime, NaiveDateTime, SecondsFormat};
use serde_json::{Value, json};
use uuid::Uuid;

type Result<T> = std::result::Result<T, String>;

fn object(value: &mut Value, fields: &[&str]) -> Result<()> {
    let map = value.as_object_mut().ok_or("expected object")?;
    map.retain(|key, value| fields.contains(&key.as_str()) && !value.is_null());
    Ok(())
}
fn string(value: &Value, key: &str) -> Result<String> {
    value[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("{key} must be a string"))
}
fn strings(value: &Value, keys: &[&str]) -> Result<()> {
    for key in keys {
        string(value, key)?;
    }
    Ok(())
}
fn ints(value: &Value, keys: &[&str]) -> Result<()> {
    for key in keys {
        if value[key].as_i64().is_none() {
            return Err(format!("{key} must be an integer"));
        }
    }
    Ok(())
}
fn enumeration(value: &Value, key: &str, options: &[&str]) -> Result<()> {
    if !options.contains(&string(value, key)?.as_str()) {
        return Err(format!("invalid {key}"));
    }
    Ok(())
}
fn uuid(value: &mut Value, key: &str) -> Result<()> {
    value[key] = json!(
        Uuid::parse_str(&string(value, key)?)
            .map_err(|e| format!("invalid {key}: {e}"))?
            .to_string()
    );
    Ok(())
}
fn date(value: &mut Value, key: &str) -> Result<()> {
    let raw = string(value, key)?;
    let normalized = if let Ok(dt) = DateTime::parse_from_rfc3339(&raw) {
        let precision = if dt.timestamp_subsec_micros() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        };
        dt.to_rfc3339_opts(precision, true)
    } else {
        let dt = NaiveDateTime::parse_from_str(&raw, "%Y-%m-%dT%H:%M:%S%.f")
            .map_err(|e| format!("invalid {key}: {e}"))?;
        dt.format(if dt.and_utc().timestamp_subsec_micros() != 0 {
            "%Y-%m-%dT%H:%M:%S%.6f"
        } else {
            "%Y-%m-%dT%H:%M:%S"
        })
        .to_string()
    };
    value[key] = json!(normalized);
    Ok(())
}
fn file(value: &mut Value) -> Result<()> {
    object(value, &["contentHash", "sizeBytes", "role"])?;
    strings(value, &["contentHash"])?;
    ints(value, &["sizeBytes"])?;
    enumeration(
        value,
        "role",
        &[
            "raw_original",
            "jpeg_original",
            "sidecar",
            "preview",
            "thumbnail",
            "export",
        ],
    )
}
fn placement(value: &mut Value) -> Result<()> {
    object(
        value,
        &[
            "fileObjectID",
            "holderID",
            "storageKind",
            "authorityRole",
            "availability",
        ],
    )?;
    file(&mut value["fileObjectID"])?;
    strings(value, &["holderID"])?;
    enumeration(
        value,
        "storageKind",
        &["local", "nas", "external_drive", "cloud_preview"],
    )?;
    enumeration(
        value,
        "authorityRole",
        &["canonical", "working_copy", "source_copy", "cache"],
    )?;
    enumeration(value, "availability", &["online", "offline", "missing"])
}
fn tags(value: &mut Value, key: &str, set: bool) -> Result<()> {
    if value.get(key).is_none() && set {
        value[key] = json!([]);
    }
    let array = value[key]
        .as_array_mut()
        .ok_or_else(|| format!("{key} must be an array"))?;
    if array.iter().any(|v| !v.is_string()) {
        return Err(format!("{key} must contain strings"));
    }
    if set {
        array.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
        array.dedup();
    }
    Ok(())
}
fn ledger_value(value: &mut Value) -> Result<()> {
    let map = value.as_object().ok_or("LedgerValue must be an object")?;
    let active: Vec<_> = ["int", "string", "null", "intValue", "stringValue"]
        .into_iter()
        .filter(|k| map.get(*k).is_some_and(|v| !v.is_null()))
        .collect();
    if active.len() != 1 {
        return Err("LedgerValue must contain exactly one case".into());
    }
    let case = active[0];
    let inner = value[case].get("_0").unwrap_or(&value[case]);
    *value = match case {
        "int" | "intValue" => {
            json!({"int": {"_0": inner.as_i64().ok_or("LedgerValue int must be an integer")?}})
        }
        "string" | "stringValue" => {
            json!({"string": {"_0": inner.as_str().ok_or("LedgerValue string must be a string")?}})
        }
        _ => json!({"null": {}}),
    };
    Ok(())
}
const CASES: &[(&str, &str, &str)] = &[
    ("asset_snapshot_declared", "assetSnapshotDeclared", "asset"),
    (
        "file_placement_snapshot_declared",
        "filePlacementSnapshotDeclared",
        "file_placement",
    ),
    ("metadata_set", "metadataSet", "asset"),
    ("tags_updated", "tagsUpdated", "asset"),
    ("move_to_trash", "moveToTrash", "asset"),
    ("restore_from_trash", "restoreFromTrash", "asset"),
    (
        "imported_original_declared",
        "importedOriginalDeclared",
        "file_object",
    ),
    ("archive_requested", "archiveRequested", "asset"),
    (
        "original_archive_receipt_recorded",
        "originalArchiveReceiptRecorded",
        "file_placement",
    ),
    (
        "derivative_declared",
        "derivativeDeclared",
        "derivative_object",
    ),
];

pub fn validate_operation(operation: &mut Value) -> Result<()> {
    object(
        operation,
        &[
            "opID",
            "libraryID",
            "deviceID",
            "deviceSequence",
            "hybridLogicalTime",
            "actorID",
            "entityType",
            "entityID",
            "opType",
            "payload",
            "baseVersion",
            "createdAt",
            "globalSeq",
            "payloadHash",
            "committedAt",
        ],
    )?;
    uuid(operation, "opID")?;
    strings(
        operation,
        &[
            "libraryID",
            "deviceID",
            "actorID",
            "entityType",
            "entityID",
            "opType",
        ],
    )?;
    ints(operation, &["deviceSequence"])?;
    date(operation, "createdAt")?;
    for key in ["baseVersion", "payloadHash"] {
        if operation.get(key).is_some() {
            string(operation, key)?;
        }
    }
    if operation.get("globalSeq").is_some() {
        ints(operation, &["globalSeq"])?;
    }
    if operation.get("committedAt").is_some() {
        date(operation, "committedAt")?;
    }
    let time = &mut operation["hybridLogicalTime"];
    object(time, &["wallTimeMilliseconds", "counter", "nodeID"])?;
    ints(time, &["wallTimeMilliseconds", "counter"])?;
    strings(time, &["nodeID"])?;
    let op_type = string(operation, "opType")?;
    let (_, case, entity) = CASES
        .iter()
        .find(|(op, _, _)| *op == op_type)
        .ok_or("invalid opType")?;
    let actual_entity = string(operation, "entityType")?;
    let payload = &mut operation["payload"];
    object(
        payload,
        &CASES.iter().map(|(_, case, _)| *case).collect::<Vec<_>>(),
    )?;
    if payload.as_object().unwrap().len() != 1 {
        return Err("OperationPayload must contain exactly one case".into());
    }
    if payload.get(*case).is_none() {
        return Err("payload_case_mismatch".into());
    }
    if actual_entity != *entity {
        return Err("entity_type_mismatch".into());
    }
    let body = &mut payload[*case];
    let fields: &[&str] = match *case {
        "assetSnapshotDeclared" => &["snapshot"],
        "metadataSet" => &["assetID", "field", "value"],
        "tagsUpdated" => &["assetID", "add", "remove"],
        "moveToTrash" => &["assetID", "reason"],
        "filePlacementSnapshotDeclared" | "importedOriginalDeclared" => {
            &["assetID", "fileObject", "placement"]
        }
        "originalArchiveReceiptRecorded" => &["assetID", "fileObject", "serverPlacement"],
        "derivativeDeclared" => &["assetID", "derivative"],
        _ => &["assetID"],
    };
    object(body, fields)?;
    if *case != "assetSnapshotDeclared" {
        uuid(body, "assetID")?;
    }
    match *case {
        "assetSnapshotDeclared" => {
            let snapshot = &mut body["snapshot"];
            object(
                snapshot,
                &[
                    "assetID",
                    "captureTime",
                    "cameraMake",
                    "cameraModel",
                    "lensModel",
                    "originalFilename",
                    "contentFingerprint",
                    "metadataFingerprint",
                    "rating",
                    "flagState",
                    "colorLabel",
                    "tags",
                    "createdAt",
                    "updatedAt",
                ],
            )?;
            uuid(snapshot, "assetID")?;
            strings(
                snapshot,
                &[
                    "cameraMake",
                    "cameraModel",
                    "lensModel",
                    "originalFilename",
                    "contentFingerprint",
                    "metadataFingerprint",
                ],
            )?;
            ints(snapshot, &["rating"])?;
            enumeration(snapshot, "flagState", &["unflagged", "picked", "rejected"])?;
            if snapshot.get("colorLabel").is_some() {
                enumeration(
                    snapshot,
                    "colorLabel",
                    &["red", "yellow", "green", "blue", "purple"],
                )?;
            }
            for key in ["createdAt", "updatedAt"] {
                date(snapshot, key)?;
            }
            if snapshot.get("captureTime").is_some() {
                date(snapshot, "captureTime")?;
            }
            tags(snapshot, "tags", false)?;
        }
        "metadataSet" => {
            enumeration(
                body,
                "field",
                &["rating", "flag_state", "color_label", "caption"],
            )?;
            ledger_value(&mut body["value"])?;
        }
        "tagsUpdated" => {
            tags(body, "add", true)?;
            tags(body, "remove", true)?;
        }
        "moveToTrash" => {
            string(body, "reason")?;
        }
        "filePlacementSnapshotDeclared"
        | "importedOriginalDeclared"
        | "originalArchiveReceiptRecorded" => {
            file(&mut body["fileObject"])?;
            let key = if *case == "originalArchiveReceiptRecorded" {
                "serverPlacement"
            } else {
                "placement"
            };
            placement(&mut body[key])?;
        }
        "derivativeDeclared" => {
            let asset_id = body["assetID"].clone();
            let derivative = &mut body["derivative"];
            object(
                derivative,
                &["assetID", "role", "fileObject", "objectRef", "pixelSize"],
            )?;
            uuid(derivative, "assetID")?;
            if derivative["assetID"] != asset_id {
                return Err("derivative_asset_id_mismatch".into());
            }
            enumeration(derivative, "role", &["thumbnail", "preview"])?;
            file(&mut derivative["fileObject"])?;
            object(&mut derivative["objectRef"], &["bucket", "key", "eTag"])?;
            strings(&derivative["objectRef"], &["bucket", "key"])?;
            if derivative["objectRef"].get("eTag").is_some() {
                string(&derivative["objectRef"], "eTag")?;
            }
            object(&mut derivative["pixelSize"], &["width", "height"])?;
            ints(&derivative["pixelSize"], &["width", "height"])?;
        }
        _ => {}
    }
    let expected = match *case {
        "assetSnapshotDeclared" => string(&body["snapshot"], "assetID")?,
        "importedOriginalDeclared" => format!(
            "{}:{}:{}",
            string(&body["fileObject"], "role")?,
            body["fileObject"]["sizeBytes"],
            string(&body["fileObject"], "contentHash")?
        ),
        "derivativeDeclared" => format!(
            "{}:{}:{}",
            string(body, "assetID")?,
            string(&body["derivative"], "role")?,
            string(&body["derivative"]["fileObject"], "contentHash")?
        ),
        _ => string(body, "assetID")?,
    };
    let actual = string(operation, "entityID")?;
    let matches = if *case == "importedOriginalDeclared" {
        actual == expected
    } else {
        actual.eq_ignore_ascii_case(&expected)
    };
    if !matches {
        return Err("entity_id_mismatch".into());
    }
    Ok(())
}

/// Match Python json.dumps(sort_keys=True, separators=(",", ":")), including ensure_ascii.
pub fn canonical_payload(payload: &Value) -> String {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let mut keys: Vec<_> = map.keys().collect();
                keys.sort();
                Value::Object(
                    keys.into_iter()
                        .map(|k| (k.clone(), sorted(&map[k])))
                        .collect(),
                )
            }
            Value::Array(values) => Value::Array(values.iter().map(sorted).collect()),
            _ => value.clone(),
        }
    }
    let serialized = serde_json::to_string(&sorted(payload)).expect("JSON Value serialization");
    let mut result = String::new();
    for c in serialized.chars() {
        if c.is_ascii() && c != '\u{7f}' {
            result.push(c);
        } else {
            for unit in c.encode_utf16(&mut [0; 2]) {
                result.push_str(&format!("\\u{unit:04x}"));
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    fn operation() -> Value {
        json!({"opID":"AA000000-0000-0000-0000-000000000001","libraryID":"library","deviceID":"mac","deviceSequence":1,"hybridLogicalTime":{"wallTimeMilliseconds":1,"counter":0,"nodeID":"mac"},"actorID":"owner","entityType":"asset","entityID":"BB000000-0000-0000-0000-000000000001","opType":"tags_updated","payload":{"tagsUpdated":{"assetID":"BB000000-0000-0000-0000-000000000001","add":["中文","a","a"]}},"createdAt":"2026-01-01T00:00:00Z"})
    }
    #[test]
    fn normalizes_uuid_and_sets() {
        let mut op = operation();
        validate_operation(&mut op).unwrap();
        assert_eq!(op["payload"]["tagsUpdated"]["add"], json!(["a", "中文"]));
        assert_eq!(op["payload"]["tagsUpdated"]["remove"], json!([]));
        assert_eq!(op["opID"], "aa000000-0000-0000-0000-000000000001");
    }
    #[test]
    fn rejects_mismatch_and_malformed_time() {
        let mut op = operation();
        op["entityID"] = json!("other");
        assert!(validate_operation(&mut op).is_err());
        let mut op = operation();
        op["createdAt"] = json!("yesterday");
        assert!(validate_operation(&mut op).is_err());
    }
    #[test]
    fn payload_mismatch_takes_precedence_over_entity_mismatch() {
        let mut op = operation();
        op["opType"] = json!("metadata_set");
        op["entityType"] = json!("file_placement");
        assert_eq!(
            validate_operation(&mut op).unwrap_err(),
            "payload_case_mismatch"
        );
    }
    #[test]
    fn datetime_matches_pydantic_precision() {
        for (input, expected) in [
            ("2026-01-01T00:00:00.000", "2026-01-01T00:00:00"),
            ("2026-01-01T00:00:00.123Z", "2026-01-01T00:00:00.123000Z"),
            (
                "2026-01-01T00:00:00.123456789Z",
                "2026-01-01T00:00:00.123456Z",
            ),
        ] {
            let mut v = json!({"date":input});
            date(&mut v, "date").unwrap();
            assert_eq!(v["date"], expected);
        }
    }
    #[test]
    fn canonical_matches_python_unicode() {
        assert_eq!(
            canonical_payload(&json!({"z":"中😀\u{7f}","a":1})),
            "{\"a\":1,\"z\":\"\\u4e2d\\ud83d\\ude00\\u007f\"}"
        );
    }
    #[test]
    fn ledger_value_matches_swift() {
        let mut value = json!({"int":3});
        ledger_value(&mut value).unwrap();
        assert_eq!(value, json!({"int":{"_0":3}}));
        let mut value = json!({"string":{"_0":"中文"}});
        ledger_value(&mut value).unwrap();
        assert_eq!(value, json!({"string":{"_0":"中文"}}));
        assert!(ledger_value(&mut json!({"int":1,"string":"bad"})).is_err());
    }
}
