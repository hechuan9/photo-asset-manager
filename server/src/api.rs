use std::{collections::HashMap, sync::Arc};

use crate::{previews::PreviewStorage, store::Store};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tower_http::trace::TraceLayer;

pub struct AppState {
    pub store: Arc<Store>,
    pub previews: Arc<PreviewStorage>,
    pub jobs: Arc<crate::jobs::Jobs>,
    pub access_token: String,
    pub library_id: String,
    pub original_root_names: crate::navigation::RootNames,
}

type ApiResult = Result<Response, ApiError>;
pub struct ApiError(StatusCode, String, String);
impl ApiError {
    fn bad(message: impl Into<String>) -> Self {
        Self(
            StatusCode::BAD_REQUEST,
            "invalid_request".into(),
            message.into(),
        )
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        tracing::error!(error = ?error, "request failed");
        if let Some(e) = error.downcast_ref::<crate::store::StoreError>() {
            return Self(
                StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                e.code.clone(),
                e.message.clone(),
            );
        }
        if let Some(e) = error.downcast_ref::<crate::previews::PreviewError>() {
            return Self(
                StatusCode::from_u16(e.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                e.code.clone(),
                e.message.clone(),
            );
        }
        Self(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error".into(),
            "server operation failed; see server logs".into(),
        )
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(json!({"detail":{"code":self.1,"message":self.2}})),
        )
            .into_response()
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    let protected = Router::new()
        .route("/libraries/{library}/assets", get(assets))
        .route(
            "/libraries/{library}/assets/{asset}",
            get(asset).patch(patch_asset),
        )
        .route(
            "/libraries/{library}/assets/{asset}/trash",
            post(trash_asset),
        )
        .route(
            "/libraries/{library}/assets/{asset}/restore",
            post(restore_asset),
        )
        .route(
            "/libraries/{library}/assets/{asset}/versions",
            get(versions),
        )
        .route(
            "/libraries/{library}/assets/{asset}/version-candidates",
            get(version_candidates),
        )
        .route(
            "/libraries/{library}/assets/{asset}/default-version",
            axum::routing::put(default_version),
        )
        .route("/libraries/{library}/revision", get(revision))
        .route("/libraries/{library}/counts", get(counts))
        .route(
            "/libraries/{library}/hidden-directories",
            get(hidden_directories).put(set_hidden_directory),
        )
        .route("/libraries/{library}/directories", get(directories))
        .route("/libraries/{library}/navigation", get(navigation))
        .route(
            "/libraries/{library}/folders",
            get(folders).post(add_folder),
        )
        .route(
            "/libraries/{library}/folders/{folder}",
            delete(remove_folder),
        )
        .route(
            "/libraries/{library}/folders/{folder}/scan",
            post(scan_folder),
        )
        .route("/libraries/{library}/jobs", get(jobs))
        .route("/libraries/{library}/jobs/{job}/retry", post(retry_job))
        .route("/derivatives/{asset}", get(derivative_metadata))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate));
    Router::new()
        .merge(protected)
        .route("/healthz", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/derivatives/local-download/{token}", get(local_download))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn authenticate(
    State(state): State<Arc<AppState>>,
    Path(params): Path<HashMap<String, String>>,
    request: Request,
    next: Next,
) -> Response {
    let expected = format!("Bearer {}", state.access_token);
    if request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        != Some(expected.as_str())
    {
        return ApiError(
            StatusCode::UNAUTHORIZED,
            "unauthorized".into(),
            "valid Bearer token required".into(),
        )
        .into_response();
    }
    if let Some(library) = params.get("library")
        && let Err(error) = require_library(&state, library)
    {
        return error.into_response();
    }
    next.run(request).await
}

fn require_library(state: &AppState, library: &str) -> Result<(), ApiError> {
    if library != state.library_id {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "library_not_found".into(),
            "图库 ID 不存在，请检查连接设置中的图库 ID；此字段不是 NAS 用户名。".into(),
        ));
    }
    Ok(())
}

async fn blocking<F>(operation: F) -> Result<Value, ApiError>
where
    F: FnOnce() -> anyhow::Result<Value> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(anyhow::Error::from)?
        .map_err(ApiError::from)
}
fn signed_asset(state: &AppState, mut asset: Value) -> anyhow::Result<Value> {
    let preview = asset
        .as_object_mut()
        .and_then(|a| a.remove("_preview"))
        .unwrap_or(Value::Null);
    asset["preview"] = if preview.is_null() {
        Value::Null
    } else {
        json!({"downloadURL":state.previews.download_url(&preview["objectRef"])?,"width":preview["width"],"height":preview["height"],"version":preview["version"]})
    };
    Ok(asset)
}
fn asset_id(id: &str) -> Result<String, ApiError> {
    uuid::Uuid::parse_str(id)
        .map(|id| id.to_string())
        .map_err(|_| ApiError::bad("invalid asset UUID"))
}
async fn assets(
    State(state): State<Arc<AppState>>,
    Path(library): Path<String>,
    Query(mut query): Query<crate::catalog::AssetQuery>,
) -> ApiResult {
    Ok(Json(
        blocking(move || {
            if let Some(id) = &query.folder_id {
                query.directory = Some(folder_for(&state, &library, id)?.path);
            }
            let mut page = state.store.query_assets(&library, &query)?;
            for asset in page["items"].as_array_mut().expect("catalog page") {
                *asset = signed_asset(&state, asset.take())?;
            }
            Ok(page)
        })
        .await?,
    )
    .into_response())
}
async fn asset(
    State(state): State<Arc<AppState>>,
    Path((library, id)): Path<(String, String)>,
) -> ApiResult {
    let id = asset_id(&id)?;
    Ok(
        Json(blocking(move || signed_asset(&state, state.store.asset(&library, &id)?)).await?)
            .into_response(),
    )
}
async fn patch_asset(
    State(state): State<Arc<AppState>>,
    Path((library, id)): Path<(String, String)>,
    Json(patch): Json<Value>,
) -> ApiResult {
    let id = asset_id(&id)?;
    Ok(Json(
        blocking(move || signed_asset(&state, state.store.patch_asset(&library, &id, &patch)?))
            .await?,
    )
    .into_response())
}
async fn set_trashed(
    state: Arc<AppState>,
    library: String,
    id: String,
    trashed: bool,
) -> ApiResult {
    let id = asset_id(&id)?;
    Ok(Json(
        blocking(move || signed_asset(&state, state.store.set_trashed(&library, &id, trashed)?))
            .await?,
    )
    .into_response())
}
async fn trash_asset(
    State(state): State<Arc<AppState>>,
    Path((library, id)): Path<(String, String)>,
) -> ApiResult {
    set_trashed(state, library, id, true).await
}
async fn restore_asset(
    State(state): State<Arc<AppState>>,
    Path((library, id)): Path<(String, String)>,
) -> ApiResult {
    set_trashed(state, library, id, false).await
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CountsQuery {
    #[serde(default)]
    show_hidden: bool,
}
async fn counts(
    State(state): State<Arc<AppState>>,
    Path(library): Path<String>,
    Query(query): Query<CountsQuery>,
) -> ApiResult {
    Ok(
        Json(blocking(move || state.store.counts(&library, query.show_hidden)).await?)
            .into_response(),
    )
}
async fn hidden_directories(
    State(state): State<Arc<AppState>>,
    Path(library): Path<String>,
) -> ApiResult {
    Ok(Json(blocking(move || state.store.hidden_directories(&library)).await?).into_response())
}
#[derive(Deserialize)]
struct HiddenDirectoryChange {
    path: String,
    hidden: bool,
}
async fn set_hidden_directory(
    State(state): State<Arc<AppState>>,
    Path(library): Path<String>,
    Json(change): Json<HiddenDirectoryChange>,
) -> ApiResult {
    Ok(Json(
        blocking(move || {
            state
                .store
                .set_hidden_directory(&library, &change.path, change.hidden)
        })
        .await?,
    )
    .into_response())
}
async fn directories(State(state): State<Arc<AppState>>, Path(library): Path<String>) -> ApiResult {
    Ok(Json(blocking(move || state.store.directories(&library)).await?).into_response())
}
#[derive(Deserialize)]
struct NavigationQuery {
    path: Option<String>,
}
async fn navigation(
    State(state): State<Arc<AppState>>,
    Path(library): Path<String>,
    Query(query): Query<NavigationQuery>,
) -> ApiResult {
    Ok(Json(
        blocking(move || {
            let mut result = crate::navigation::navigation(
                &state.jobs,
                &library,
                query.path.as_deref(),
                &state.original_root_names,
            )?;
            let directories = result["directories"]
                .as_array_mut()
                .expect("navigation directories");
            let paths: Vec<String> = directories
                .iter()
                .map(|directory| {
                    directory["path"]
                        .as_str()
                        .expect("navigation path")
                        .to_owned()
                })
                .collect();
            let counts = state.store.directory_photo_counts(&library, &paths)?;
            for (directory, count) in directories.iter_mut().zip(counts) {
                directory["photoCount"] = json!(count);
            }
            Ok(result)
        })
        .await?,
    )
    .into_response())
}
fn resource_error(status: u16, message: impl Into<String>) -> anyhow::Error {
    crate::store::StoreError {
        status,
        code: "invalid_request".into(),
        message: message.into(),
    }
    .into()
}
fn folder_for(state: &AppState, library: &str, id: &str) -> anyhow::Result<crate::jobs::Folder> {
    state
        .jobs
        .folders()?
        .into_iter()
        .find(|f| f.id == id && f.library_id == library)
        .ok_or_else(|| resource_error(404, "folder not found"))
}
async fn folders(State(state): State<Arc<AppState>>, Path(library): Path<String>) -> ApiResult {
    Ok(Json(blocking(move || Ok(json!({"rootPath":state.jobs.root(),"folders":state.jobs.folders()?.into_iter().filter(|f|f.library_id==library&&f.active).collect::<Vec<_>>()}))).await?).into_response())
}
#[derive(Deserialize)]
struct FolderRequest {
    path: String,
}
async fn add_folder(
    State(state): State<Arc<AppState>>,
    Path(library): Path<String>,
    Json(body): Json<FolderRequest>,
) -> ApiResult {
    Ok((
        StatusCode::CREATED,
        Json(
            blocking(move || {
                state
                    .jobs
                    .validate_path(std::path::Path::new(&body.path))
                    .map_err(|e| resource_error(422, format!("{e:#}")))?;
                let folder = state.jobs.add_folder(&library, &body.path)?;
                state.jobs.enqueue_scan(&folder.id)?;
                Ok(serde_json::to_value(folder)?)
            })
            .await?,
        ),
    )
        .into_response())
}
async fn remove_folder(
    State(state): State<Arc<AppState>>,
    Path((library, id)): Path<(String, String)>,
) -> ApiResult {
    blocking(move || {
        folder_for(&state, &library, &id)?;
        state.jobs.remove_folder(&id)?;
        Ok(Value::Null)
    })
    .await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
async fn scan_folder(
    State(state): State<Arc<AppState>>,
    Path((library, id)): Path<(String, String)>,
) -> ApiResult {
    Ok((
        StatusCode::ACCEPTED,
        Json(
            blocking(move || {
                let folder = folder_for(&state, &library, &id)?;
                if !folder.active {
                    return Err(resource_error(409, "folder is inactive"));
                }
                Ok(serde_json::to_value(state.jobs.enqueue_scan(&id)?)?)
            })
            .await?,
        ),
    )
        .into_response())
}
async fn jobs(State(state): State<Arc<AppState>>, Path(library): Path<String>) -> ApiResult {
    Ok(Json(
        blocking(move || Ok(json!({"jobs":state.jobs.jobs_for_library(&library,100)?}))).await?,
    )
    .into_response())
}
async fn retry_job(
    State(state): State<Arc<AppState>>,
    Path((library, id)): Path<(String, String)>,
) -> ApiResult {
    Ok(Json(
        blocking(move || {
            let job = state
                .jobs
                .job(&id)?
                .filter(|j| j.library_id == library)
                .ok_or_else(|| resource_error(404, "job not found"))?;
            if job.status != "failed" {
                return Err(resource_error(409, "only failed jobs can be retried"));
            }
            Ok(serde_json::to_value(
                state
                    .jobs
                    .retry(&id)
                    .map_err(|e| resource_error(409, format!("{e:#}")))?,
            )?)
        })
        .await?,
    )
    .into_response())
}
#[derive(Deserialize)]
struct DerivativeQuery {
    role: String,
    #[serde(rename = "libraryID")]
    library: Option<String>,
}
fn validate_derivative(asset: &str, role: &str) -> Result<String, ApiError> {
    let asset = uuid::Uuid::parse_str(asset).map_err(|_| {
        ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request".into(),
            "invalid asset UUID".into(),
        )
    })?;
    if !["preview", "thumbnail"].contains(&role) {
        return Err(ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request".into(),
            "invalid derivative role".into(),
        ));
    }
    Ok(asset.to_string())
}
async fn derivative_metadata(
    State(state): State<Arc<AppState>>,
    Path(asset): Path<String>,
    Query(query): Query<DerivativeQuery>,
) -> ApiResult {
    if let Some(library) = &query.library {
        require_library(&state, library)?;
    }
    let asset = validate_derivative(&asset, &query.role)?;
    let result = blocking(move || {
        let Some(derivative) =
            state
                .store
                .derivative_metadata(query.library.as_deref(), &asset, &query.role)?
        else {
            return Ok(Value::Null);
        };
        let url = state.previews.download_url(&derivative["objectRef"])?;
        Ok(json!({
            "downloadURL": url,
            "width": derivative["pixelSize"]["width"],
            "height": derivative["pixelSize"]["height"],
            "version": derivative["fileObject"]["contentHash"],
            "derivative": derivative
        }))
    })
    .await?;
    if result.is_null() {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "derivative_not_found".into(),
            "derivative metadata not declared".into(),
        ));
    }
    Ok(Json(result).into_response())
}
async fn local_download(
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
) -> ApiResult {
    let content = tokio::task::spawn_blocking(move || state.previews.read(&token))
        .await
        .map_err(anyhow::Error::from)??;
    Ok((
        [(header::CONTENT_TYPE, "application/octet-stream")],
        content,
    )
        .into_response())
}

async fn revision(State(state): State<Arc<AppState>>, Path(library): Path<String>) -> ApiResult {
    Ok(Json(blocking(move || state.store.library_revision(&library)).await?).into_response())
}
async fn versions(
    State(state): State<Arc<AppState>>,
    Path((library, id)): Path<(String, String)>,
) -> ApiResult {
    let id = asset_id(&id)?;
    Ok(Json(blocking(move || state.store.versions(&library, &id)).await?).into_response())
}
async fn version_candidates(
    State(state): State<Arc<AppState>>,
    Path((library, id)): Path<(String, String)>,
) -> ApiResult {
    let id = asset_id(&id)?;
    Ok(
        Json(blocking(move || state.store.version_candidates(&library, &id)).await?)
            .into_response(),
    )
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DefaultVersionChange {
    content_hash: String,
}
async fn default_version(
    State(state): State<Arc<AppState>>,
    Path((library, id)): Path<(String, String)>,
    Json(change): Json<DefaultVersionChange>,
) -> ApiResult {
    let id = asset_id(&id)?;
    Ok(Json(
        blocking(move || {
            let versions = state.store.versions(&library, &id)?;
            let version = versions["items"]
                .as_array()
                .and_then(|items| {
                    items.iter().find(|version| {
                        version["contentHash"].as_str() == Some(&change.content_hash)
                            && version["available"] == true
                    })
                })
                .ok_or_else(|| resource_error(422, "version is not available for this asset"))?;
            let folders = state.jobs.folders()?;
            let target = version["paths"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|location| location["available"] == true)
                .filter_map(|location| location["path"].as_str())
                .find_map(|path| {
                    folders
                        .iter()
                        .find(|folder| {
                            folder.active
                                && folder.library_id == library
                                && std::path::Path::new(path).starts_with(&folder.path)
                        })
                        .map(|folder| (folder.id.clone(), path))
                })
                .ok_or_else(|| {
                    resource_error(
                        409,
                        "version folder is not tracked; enable tracking before selecting a default",
                    )
                })?;
            let scope = std::path::Path::new(target.1)
                .parent()
                .ok_or_else(|| resource_error(422, "version path has no parent"))?;
            state
                .jobs
                .validate_path(scope)
                .map_err(|error| resource_error(409, format!("{error:#}")))?;
            let result = state
                .store
                .set_default_version(&library, &id, &change.content_hash)?;
            state.jobs.enqueue_reconcile_scope(&target.0, scope)?;
            Ok(result)
        })
        .await?,
    )
    .into_response())
}
