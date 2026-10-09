use crate::{
    api::{ApiError, AppState},
    store::{Store, StoreError},
};
use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::sync::Arc;

pub(crate) fn migrate(db: &Connection) -> Result<()> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS ai_editing_state(library_id TEXT NOT NULL,kind TEXT NOT NULL,revision INTEGER NOT NULL,content TEXT,PRIMARY KEY(library_id,kind)); CREATE TABLE IF NOT EXISTS photo_edit_decisions(library_id TEXT NOT NULL,asset_id TEXT NOT NULL,metadata TEXT NOT NULL,PRIMARY KEY(library_id,asset_id));")?;
    Ok(())
}
fn invalid(status: u16, message: &str) -> anyhow::Error {
    StoreError {
        status,
        code: if status == 409 {
            "ai_editing_conflict"
        } else {
            "invalid_ai_editing"
        }
        .into(),
        message: message.into(),
    }
    .into()
}
fn read(db: &Connection, library: &str, kind: &str) -> Result<Value> {
    let (revision, content) = db
        .query_row(
            "SELECT revision,content FROM ai_editing_state WHERE library_id=? AND kind=?",
            params![library, kind],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?)),
        )
        .optional()?
        .unwrap_or((0, None));
    Ok(if kind == "preferences" {
        json!({"revision":revision,"text":content.unwrap_or_default()})
    } else {
        json!({"revision":revision,"document":content})
    })
}
impl Store {
    pub fn ai_editing_state(&self, library: &str, kind: &str) -> Result<Value> {
        read(&*self.lock()?, library, kind)
    }
    pub fn save_ai_editing_state(&self, library: &str, kind: &str, value: &Value) -> Result<Value> {
        let expected = value["expectedRevision"]
            .as_i64()
            .filter(|v| *v >= 0)
            .ok_or_else(|| invalid(422, "expectedRevision required"))?;
        let content = match kind {
            "preferences" => Some(
                value["text"]
                    .as_str()
                    .filter(|s| s.len() <= 64 * 1024)
                    .ok_or_else(|| invalid(422, "text required (maximum 64 KiB)"))?,
            ),
            "workspace" => {
                if value.get("document").is_none() {
                    return Err(invalid(422, "document required"));
                }
                if value["document"].is_null() {
                    None
                } else {
                    let document = value["document"]
                        .as_str()
                        .filter(|s| s.len() <= 64 * 1024 * 1024)
                        .ok_or_else(|| {
                            invalid(422, "document must be JSON text (maximum 64 MiB)")
                        })?;
                    if !serde_json::from_str::<Value>(document).is_ok_and(|v| v.is_object()) {
                        return Err(invalid(422, "document must encode an object"));
                    }
                    Some(document)
                }
            }
            _ => return Err(invalid(404, "unknown AI editing resource")),
        };
        let mut db = self.lock()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = read(&tx, library, kind)?;
        if current["revision"] != expected {
            let key = if kind == "preferences" {
                "text"
            } else {
                "document"
            };
            if current[key].as_str() == content {
                return Ok(current);
            }
            return Err(invalid(409, "调色工作台已变化，请刷新后重试"));
        }
        tx.execute("INSERT INTO ai_editing_state(library_id,kind,revision,content) VALUES(?,?,?,?) ON CONFLICT(library_id,kind) DO UPDATE SET revision=excluded.revision,content=excluded.content",params![library,kind,expected+1,content])?;
        let result = read(&tx, library, kind)?;
        tx.commit()?;
        Ok(result)
    }
}
async fn load(
    State(s): State<Arc<AppState>>,
    Path((library, kind)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    if !["preferences", "workspace"].contains(&kind.as_str()) {
        return Err(invalid(404, "unknown AI editing resource").into());
    }
    Ok(Json(
        tokio::task::spawn_blocking(move || s.store.ai_editing_state(&library, &kind))
            .await
            .map_err(anyhow::Error::from)??,
    ))
}
async fn save(
    State(s): State<Arc<AppState>>,
    Path((library, kind)): Path<(String, String)>,
    Json(value): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        tokio::task::spawn_blocking(move || s.store.save_ai_editing_state(&library, &kind, &value))
            .await
            .map_err(anyhow::Error::from)??,
    ))
}
pub fn router() -> Router<Arc<AppState>> {
    Router::new().route(
        "/libraries/{library}/ai-editing/{kind}",
        get(load)
            .put(save)
            .layer(axum::extract::DefaultBodyLimit::max(
                128 * 1024 * 1024 + 64 * 1024,
            )),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn workspace_route_overrides_global_body_limit_for_batch_payloads() -> Result<()> {
        use tower::ServiceExt;
        let dir = tempfile::tempdir()?;
        let root = dir.path().canonicalize()?;
        let photos = root.join("photos");
        std::fs::create_dir(&photos)?;
        let state = Arc::new(AppState {
            store: Arc::new(Store::open(&root.join("catalog.sqlite"), true)?),
            jobs: Arc::new(crate::jobs::Jobs::open(&root.join("jobs.sqlite"), &photos)?),
            previews: Arc::new(crate::previews::PreviewStorage::new(
                &root.join("keeps"),
                Some(&photos),
                "http://localhost",
                "test",
            )?),
            access_token: "test".into(),
            library_id: "lib".into(),
            original_root_names: Default::default(),
        });
        let document = json!({"xmp":"x".repeat(17*1024*1024)}).to_string();
        let response = crate::api::router(state)
            .oneshot(
                axum::http::Request::builder()
                    .method("PUT")
                    .uri("/libraries/lib/ai-editing/workspace")
                    .header("authorization", "Bearer test")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({"expectedRevision":0,"document":document}).to_string(),
                    ))?,
            )
            .await?;
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        Ok(())
    }
    #[test]
    fn lost_response_retries_return_the_saved_revision() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("catalog.sqlite"), true)?;
        for (kind, key, content) in [
            ("preferences", "text", json!("自然肤色")),
            ("workspace", "document", json!("{\"photos\":[]}")),
        ] {
            let input = json!({"expectedRevision":0,key:content});
            let saved = store.save_ai_editing_state("lib", kind, &input)?;
            assert_eq!(store.save_ai_editing_state("lib", kind, &input)?, saved);
            assert_eq!(saved["revision"], 1);
            assert_eq!(store.ai_editing_state("lib", kind)?["revision"], 1);
            let changed =
                json!({"expectedRevision":0,key:if kind == "preferences" {"不同偏好"} else {"{}"}});
            assert_eq!(
                store
                    .save_ai_editing_state("lib", kind, &changed)
                    .unwrap_err()
                    .downcast_ref::<StoreError>()
                    .unwrap()
                    .status,
                409
            );
        }
        let clear = json!({"expectedRevision":1,"document":null});
        let cleared = store.save_ai_editing_state("lib", "workspace", &clear)?;
        assert_eq!(
            store.save_ai_editing_state("lib", "workspace", &clear)?,
            cleared
        );
        Ok(())
    }
    #[test]
    fn workspace_accepts_large_batches_and_rejects_oversized_documents() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(&dir.path().join("catalog.sqlite"), true)?;
        let document = json!({"xmp":"x".repeat(17*1024*1024)}).to_string();
        store.save_ai_editing_state(
            "lib",
            "workspace",
            &json!({"expectedRevision":0,"document":document}),
        )?;
        let oversized = json!({"xmp":"x".repeat(64*1024*1024)}).to_string();
        assert_eq!(
            store
                .save_ai_editing_state(
                    "lib",
                    "workspace",
                    &json!({"expectedRevision":1,"document":oversized})
                )
                .unwrap_err()
                .downcast_ref::<StoreError>()
                .unwrap()
                .status,
            422
        );
        Ok(())
    }
    #[test]
    fn schema_twelve_requires_explicit_migration() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("catalog.sqlite");
        let store = Store::open(&path, true)?;
        store
            .lock()?
            .execute_batch("DROP TABLE ai_editing_state; PRAGMA user_version=12;")?;
        drop(store);
        assert!(Store::open(&path, false).is_err());
        let store = Store::open(&path, true)?;
        assert_eq!(
            store.ai_editing_state("lib", "workspace")?,
            json!({"revision":0,"document":null})
        );
        Ok(())
    }
    #[test]
    fn simultaneous_writers_cannot_overwrite_a_new_revision() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Arc::new(Store::open(&dir.path().join("catalog.sqlite"), true)?);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let threads = (0..2)
            .map(|index| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store.save_ai_editing_state(
                        "lib",
                        "preferences",
                        &json!({"expectedRevision":0,"text":index.to_string()}),
                    )
                })
            })
            .collect::<Vec<_>>();
        let results = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| r
                    .as_ref()
                    .err()
                    .and_then(|e| e.downcast_ref::<StoreError>())
                    .is_some_and(|e| e.status == 409))
                .count(),
            1
        );
        Ok(())
    }
    #[test]
    fn persistent_independent_revisions_and_conflicts() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("catalog.sqlite");
        let store = Store::open(&path, true)?;
        let before = store.library_revision("lib")?;
        assert_eq!(
            store.ai_editing_state("lib", "preferences")?,
            json!({"revision":0,"text":""})
        );
        store.save_ai_editing_state(
            "lib",
            "preferences",
            &json!({"expectedRevision":0,"text":"自然肤色"}),
        )?;
        store.save_ai_editing_state(
            "lib",
            "workspace",
            &json!({"expectedRevision":0,"document":"{\"photos\":[]}"}),
        )?;
        let err = store
            .save_ai_editing_state(
                "lib",
                "workspace",
                &json!({"expectedRevision":0,"document":null}),
            )
            .unwrap_err();
        assert_eq!(err.downcast_ref::<StoreError>().unwrap().status, 409);
        assert_eq!(store.library_revision("lib")?, before);
        assert_eq!(store.ai_editing_state("other", "workspace")?["revision"], 0);
        drop(store);
        let store = Store::open(&path, false)?;
        assert_eq!(
            store.ai_editing_state("lib", "preferences")?["text"],
            "自然肤色"
        );
        assert_eq!(
            store.ai_editing_state("lib", "workspace")?["document"],
            "{\"photos\":[]}"
        );
        assert_eq!(
            store.save_ai_editing_state(
                "lib",
                "workspace",
                &json!({"expectedRevision":1,"document":null})
            )?,
            json!({"revision":2,"document":null})
        );
        assert!(
            store
                .save_ai_editing_state(
                    "lib",
                    "workspace",
                    &json!({"expectedRevision":2,"document":"[]"})
                )
                .is_err()
        );
        Ok(())
    }
}
