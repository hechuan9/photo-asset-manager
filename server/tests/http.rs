use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use keeps_server::{
    api::{AppState, router},
    jobs::Jobs,
    previews::PreviewStorage,
    store::Store,
};
use serde_json::{Value, json};
use std::{collections::HashSet, path::Path, sync::Arc};
use tower::ServiceExt;

const SIGNING_KEY: &str = "test-preview-key-01234567890123456789";
fn state(root: &Path) -> Arc<AppState> {
    let originals = root.join("originals");
    std::fs::create_dir_all(&originals).unwrap();
    let keeps = root.join("keeps");
    let previews =
        PreviewStorage::new(&keeps, Some(&originals), "http://localhost", SIGNING_KEY).unwrap();
    let store = Store::open(&keeps.join("db/control_plane.sqlite"), true, HashSet::new()).unwrap();
    let jobs = Arc::new(Jobs::open(&keeps.join("db/jobs.sqlite"), &originals).unwrap());
    Arc::new(AppState {
        store: Arc::new(store),
        previews: Arc::new(previews),
        jobs,
        access_token: SIGNING_KEY.into(),
        library_id: "photos".into(),
        original_root_names: Default::default(),
    })
}
async fn call(app: axum::Router, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {SIGNING_KEY}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
fn seed(state: &AppState, root: &Path, name: &str) -> String {
    let path = root.join("originals").join(name);
    std::fs::write(&path, b"original bytes").unwrap();
    let snapshot = json!({"captureTime":"2024-01-01T00:00:00.000Z","cameraMake":"Canon","cameraModel":"R3","lensModel":"50mm",
        "originalFilename":name,"contentFingerprint":name,"metadataFingerprint":format!("metadata-{name}"),"rating":1,"flagState":"unflagged","tags":[],
        "createdAt":"2024-01-01T00:00:00.000Z","updatedAt":"2024-01-01T00:00:00.000Z"});
    state
        .store
        .ingest_original(
            "photos",
            path.to_str().unwrap(),
            &json!({"contentHash":name,"sizeBytes":14,"role":"jpeg_original"}),
            &snapshot,
        )
        .unwrap()
}

#[tokio::test]
async fn nas_catalog_commands_queries_authentication_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let id = seed(&state, dir.path(), "photo.jpg");
    seed(&state, dir.path(), "second.jpg");
    let app = router(state.clone());
    let unauthenticated = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/libraries/photos/assets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        call(app.clone(), "GET", "/healthz", Value::Null).await.0,
        StatusCode::OK
    );
    let (status, page) = call(
        app.clone(),
        "GET",
        "/libraries/photos/assets?limit=1",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["total"], 2);
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["nextCursor"], "1");
    let path = format!("/libraries/photos/assets/{}", id.to_uppercase());
    let patch =
        json!({"rating":5,"flagState":"picked","colorLabel":"green","tags":["旅行","family"]});
    let (status, updated) = call(app.clone(), "PATCH", &path, patch.clone()).await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["rating"], 5);
    assert_eq!(updated["flagState"], "picked");
    let (_, filtered) = call(
        app.clone(),
        "GET",
        "/libraries/photos/assets?minRating=5&tag=family",
        Value::Null,
    )
    .await;
    assert_eq!(filtered["total"], 1);
    assert_eq!(filtered["items"][0]["id"], id);
    let (_, counts) = call(app.clone(), "GET", "/libraries/photos/counts", Value::Null).await;
    assert_eq!(counts, json!({"all":2,"trashed":0,"picked":1}));
    assert_eq!(
        call(app.clone(), "PATCH", &path, json!({"rating":6}))
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        call(
            app.clone(),
            "PATCH",
            &path,
            json!({"originalFilename":"moved.jpg"})
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        call(app.clone(), "POST", &format!("{path}/trash"), Value::Null)
            .await
            .1["trashed"],
        true
    );
    assert_eq!(
        call(
            app.clone(),
            "GET",
            "/libraries/photos/assets?trashed=true",
            Value::Null
        )
        .await
        .1["total"],
        1
    );
    assert_eq!(
        call(app.clone(), "POST", &format!("{path}/restore"), Value::Null)
            .await
            .1["trashed"],
        false
    );
    assert_eq!(
        call(app.clone(), "PATCH", &path, json!({"colorLabel":null}))
            .await
            .1["colorLabel"],
        Value::Null
    );
    assert_eq!(
        call(
            app.clone(),
            "GET",
            "/libraries/photos/assets?cursor=bad",
            Value::Null
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        std::fs::read(dir.path().join("originals/photo.jpg")).unwrap(),
        b"original bytes"
    );
    drop(app);
    drop(state);
    let reopened = router(self::state(dir.path()));
    let (status, asset) = call(reopened, "GET", &path, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(asset["rating"], 5);
    assert_eq!(asset["tags"], json!(["family", "旅行"]));
}

#[tokio::test]
async fn client_cannot_upload_ledger_originals_or_derivatives() {
    let dir = tempfile::tempdir().unwrap();
    let app = router(state(dir.path()));
    for path in [
        "/libraries/photos/ops",
        "/devices/mac/heartbeat",
        "/derivatives/uploads",
        "/archive/receipts",
    ] {
        let status = call(app.clone(), "POST", path, json!({})).await.0;
        assert!(
            matches!(
                status,
                StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
            ),
            "{path}: {status}"
        );
    }
    assert_eq!(
        call(app, "PUT", "/derivatives/local-upload/anything", json!({}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn folders_jobs_are_library_scoped_and_removal_keeps_originals() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    seed(&state, dir.path(), "photo.jpg");
    let app = router(state.clone());
    let (status, folder) = call(
        app.clone(),
        "POST",
        "/libraries/photos/folders",
        json!({"path":"."}),
    )
    .await;
    assert!(status.is_success(), "{folder}");
    assert_eq!(folder["libraryID"], "photos");
    let id = folder["id"].as_str().unwrap();
    let (_, folders) = call(app.clone(), "GET", "/libraries/photos/folders", Value::Null).await;
    assert_eq!(folders["folders"][0]["id"], id);
    assert!(folders["rootPath"].as_str().unwrap().ends_with("originals"));
    let (status, scan) = call(
        app.clone(),
        "POST",
        &format!("/libraries/photos/folders/{id}/scan"),
        Value::Null,
    )
    .await;
    assert!(status.is_success(), "{scan}");
    assert_eq!(scan["folderID"], id);
    assert_eq!(scan["libraryID"], "photos");
    let (_, jobs) = call(app.clone(), "GET", "/libraries/photos/jobs", Value::Null).await;
    assert_eq!(jobs["jobs"][0]["id"], scan["id"]);
    assert_eq!(
        call(app.clone(), "GET", "/libraries/other/jobs", Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            app.clone(),
            "DELETE",
            &format!("/libraries/other/folders/{id}"),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (status, _) = call(
        app.clone(),
        "DELETE",
        &format!("/libraries/photos/folders/{id}"),
        Value::Null,
    )
    .await;
    assert!(status.is_success());
    assert_eq!(state.jobs.jobs(1).unwrap()[0].status, "cancelled");
    assert_eq!(
        std::fs::read(dir.path().join("originals/photo.jpg")).unwrap(),
        b"original bytes"
    );
}

#[tokio::test]
async fn nas_generated_preview_is_downloaded_by_signed_url() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let id = seed(&state, dir.path(), "photo.jpg");
    let generated = dir.path().join("generated.heic");
    std::fs::write(&generated, b"abc").unwrap();
    let object = state
        .previews
        .put_generated("photos", &id, "preview-hash", &generated)
        .unwrap();
    state.store.declare_generated_preview("photos",&id,&json!({"assetID":id,"role":"preview","fileObject":{"contentHash":"preview-hash","sizeBytes":3,"role":"preview"},"objectRef":object,"pixelSize":{"width":1200,"height":800}})).unwrap();
    let app = router(state);
    let (status, asset) = call(
        app.clone(),
        "GET",
        &format!("/libraries/photos/assets/{id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{asset}");
    assert_eq!(asset["preview"]["width"], 1200);
    assert_eq!(asset["preview"]["version"], "preview-hash");
    let (status, metadata) = call(
        app.clone(),
        "GET",
        &format!(
            "/derivatives/{}?role=preview&libraryID=photos",
            id.to_uppercase()
        ),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{metadata}");
    assert_eq!(metadata["width"], asset["preview"]["width"]);
    assert_eq!(metadata["height"], asset["preview"]["height"]);
    assert_eq!(metadata["version"], asset["preview"]["version"]);
    let path = metadata["downloadURL"]
        .as_str()
        .unwrap()
        .strip_prefix("http://localhost")
        .unwrap();
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "abc"
    );
    let bad = format!("{path}tampered");
    assert_eq!(
        call(app, "GET", &bad, Value::Null).await.0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn navigation_lists_empty_and_intermediate_directories_without_indexed_assets() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let root = state.jobs.root();
    for name in ["empty", "middle/leaf", ".hidden", "@eaDir", "#recycle"] {
        std::fs::create_dir_all(root.join(name)).unwrap();
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(root.join("middle"), root.join("linked")).unwrap();
    state
        .jobs
        .add_folder("photos", root.to_str().unwrap())
        .unwrap();
    let app = router(state.clone());
    let (status, page) = call(
        app.clone(),
        "GET",
        "/libraries/photos/navigation",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert!(page.get("location").is_none());
    assert!(page.get("local").is_none());
    assert!(page["path"].is_null());
    assert_eq!(page["directories"].as_array().unwrap().len(), 2);
    assert_eq!(page["directories"][0]["name"], "empty");
    assert_eq!(page["directories"][0]["hasChildren"], false);
    assert_eq!(page["directories"][1]["name"], "middle");
    assert_eq!(page["directories"][1]["hasChildren"], true);
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("path", root.join("middle").to_str().unwrap())
        .finish();
    let (status, page) = call(
        app,
        "GET",
        &format!("/libraries/photos/navigation?{query}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["directories"][0]["name"], "leaf");
}

#[tokio::test]
async fn navigation_enforces_active_library_roots_and_rejects_escape_and_symlinks() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let root = state.jobs.root();
    for name in ["a/nested", "b", "inactive"] {
        std::fs::create_dir_all(root.join(name)).unwrap();
    }
    state
        .jobs
        .add_folder("photos", root.join("a").to_str().unwrap())
        .unwrap();
    state
        .jobs
        .add_folder("photos", root.join("a/nested").to_str().unwrap())
        .unwrap();
    state
        .jobs
        .add_folder("other", root.join("b").to_str().unwrap())
        .unwrap();
    let inactive = state
        .jobs
        .add_folder("photos", root.join("inactive").to_str().unwrap())
        .unwrap();
    state.jobs.remove_folder(&inactive.id).unwrap();
    let app = router(state.clone());
    let (_, page) = call(
        app.clone(),
        "GET",
        "/libraries/photos/navigation",
        Value::Null,
    )
    .await;
    assert_eq!(page["directories"].as_array().unwrap().len(), 1);
    assert_eq!(page["directories"][0]["name"], "a");
    let (status, page) = call(
        app.clone(),
        "GET",
        "/libraries/unknown/navigation",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(page["detail"]["code"], "library_not_found");
    let mut denied = vec![
        (root.to_path_buf(), StatusCode::NOT_FOUND),
        (root.join("b"), StatusCode::NOT_FOUND),
        (root.join("inactive"), StatusCode::NOT_FOUND),
        (root.join("a/../b"), StatusCode::UNPROCESSABLE_ENTITY),
        (dir.path().to_path_buf(), StatusCode::UNPROCESSABLE_ENTITY),
    ];
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(root.join("b"), root.join("a/link")).unwrap();
        denied.push((root.join("a/link"), StatusCode::UNPROCESSABLE_ENTITY));
    }
    for (path, expected) in denied {
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("path", path.to_str().unwrap())
            .finish();
        let (status, body) = call(
            app.clone(),
            "GET",
            &format!("/libraries/photos/navigation?{query}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, expected, "{}: {body}", path.display());
    }
}

#[tokio::test]
async fn navigation_photo_counts_are_distinct_recursive_live_catalog_counts() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let root = state.jobs.root();
    for name in ["foo/年份", "Foo", "foobar", "empty", "旅行_%"] {
        std::fs::create_dir_all(root.join(name)).unwrap();
    }
    state
        .jobs
        .add_folder("photos", root.to_str().unwrap())
        .unwrap();
    let first = seed(&state, dir.path(), "first.jpg");
    let second = seed(&state, dir.path(), "second.jpg");
    let trashed = seed(&state, dir.path(), "trashed.jpg");
    state.store.set_trashed("photos", &trashed, true).unwrap();
    let db = rusqlite::Connection::open(dir.path().join("keeps/db/control_plane.sqlite")).unwrap();
    for (relative, asset) in [
        ("foo/first.jpg", &first),
        ("foo/年份/duplicate.jpg", &first),
        ("foo/年份/second.jpg", &second),
        ("foo/trashed.jpg", &trashed),
        ("foobar/first.jpg", &first),
        ("Foo/second.jpg", &second),
        ("旅行_%/second.jpg", &second),
    ] {
        db.execute("INSERT INTO catalog_paths(library_id,path,asset_id,content_hash,role) VALUES('photos',?1,?2,'fixture','jpeg_original')", rusqlite::params![root.join(relative).to_str().unwrap(), asset]).unwrap();
    }
    let app = router(state.clone());
    let (status, page) = call(
        app.clone(),
        "GET",
        "/libraries/photos/navigation",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    for directory in page["directories"].as_array().unwrap() {
        let expected = match directory["name"].as_str().unwrap() {
            "foo" => 2,
            "Foo" | "foobar" | "旅行_%" => 1,
            "empty" => 0,
            name => panic!("unexpected {name}"),
        };
        assert_eq!(directory["photoCount"], expected, "{directory}");
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("directory", directory["path"].as_str().unwrap())
            .finish();
        let (status, gallery) = call(
            app.clone(),
            "GET",
            &format!("/libraries/photos/assets?{query}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{gallery}");
        assert_eq!(directory["photoCount"], gallery["total"]);
    }
    let direct_query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("directory", root.join("foo").to_str().unwrap())
        .append_pair("recursive", "false")
        .finish();
    let (status, direct) = call(
        app,
        "GET",
        &format!("/libraries/photos/assets?{direct_query}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(direct["total"], 1);
    let other = state
        .store
        .directory_photo_counts("other", &[root.join("foo").to_string_lossy().into_owned()])
        .unwrap();
    assert_eq!(other, vec![0]);
}

#[tokio::test]
async fn hidden_directories_filter_before_paging_and_persist() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    for folder in ["private/nested", "private-other", "public"] {
        std::fs::create_dir_all(dir.path().join("originals").join(folder)).unwrap();
    }
    let root = dir.path().join("originals");
    let hidden = root.join("private").to_str().unwrap().to_string();
    let nested = format!("{hidden}/nested");
    let id = seed(&state, dir.path(), "private/a.jpg");
    seed(&state, dir.path(), "private/nested/b.jpg");
    seed(&state, dir.path(), "private-other/c.jpg");
    seed(&state, dir.path(), "public/d.jpg");
    state
        .store
        .patch_asset("photos", &id, &json!({"flagState":"picked"}))
        .unwrap();
    let app = router(state.clone());
    let endpoint = "/libraries/photos/hidden-directories";
    for path in [&hidden, &nested] {
        let (status, response) = call(
            app.clone(),
            "PUT",
            endpoint,
            json!({"path":path,"hidden":true}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(response["paths"].as_array().unwrap().contains(&json!(path)));
    }
    let (_, page) = call(
        app.clone(),
        "GET",
        "/libraries/photos/assets?limit=1",
        Value::Null,
    )
    .await;
    assert_eq!(page["total"], 2);
    assert_eq!(page["nextCursor"], "1");
    let (_, counts) = call(app.clone(), "GET", "/libraries/photos/counts", Value::Null).await;
    assert_eq!(counts, json!({"all":2,"picked":0,"trashed":0}));
    let (_, counts) = call(
        app.clone(),
        "GET",
        "/libraries/photos/counts?showHidden=true",
        Value::Null,
    )
    .await;
    assert_eq!(counts["all"], 4);
    let (_, all) = call(
        app.clone(),
        "GET",
        "/libraries/photos/assets?showHidden=true",
        Value::Null,
    )
    .await;
    assert_eq!(all["total"], 4);
    let query = |path: &str| {
        format!(
            "/libraries/photos/assets?directory={}",
            url::form_urlencoded::byte_serialize(path.as_bytes()).collect::<String>()
        )
    };
    for path in [&hidden, &nested] {
        let (_, page) = call(app.clone(), "GET", &query(path), Value::Null).await;
        assert_eq!(page["total"], 1);
    }
    let (_, page) = call(
        app.clone(),
        "GET",
        "/libraries/photos/assets?q=a.jpg",
        Value::Null,
    )
    .await;
    assert_eq!(page["total"], 0);
    state.store.set_trashed("photos", &id, true).unwrap();
    let (_, page) = call(
        app.clone(),
        "GET",
        "/libraries/photos/assets?trashed=true",
        Value::Null,
    )
    .await;
    assert_eq!(page["total"], 0);
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            endpoint,
            json!({"path":"relative","hidden":true})
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let (_, response) = call(
        app.clone(),
        "PUT",
        endpoint,
        json!({"path":hidden,"hidden":false}),
    )
    .await;
    assert_eq!(response, json!({"paths":[nested]}));
    drop(app);
    drop(state);
    let store = Store::open(
        &dir.path().join("keeps/db/control_plane.sqlite"),
        false,
        HashSet::new(),
    )
    .unwrap();
    assert_eq!(
        store.hidden_directories("photos").unwrap(),
        json!({"paths":[nested]})
    );
    assert_eq!(
        store.hidden_directories("another").unwrap(),
        json!({"paths":[]})
    );
}

#[tokio::test]
async fn version_api_requires_asset_membership_and_exposes_revision() {
    let dir = tempfile::tempdir().unwrap();
    let canonical = dir.path().canonicalize().unwrap();
    let state = state(&canonical);
    let id = seed(&state, &canonical, "one.jpg");
    let other = seed(&state, &canonical, "two.jpg");
    let folder = state.jobs.add_folder("photos", ".").unwrap();
    let app = router(state.clone());
    let route = format!("/libraries/photos/assets/{id}");
    let before = call(
        app.clone(),
        "GET",
        "/libraries/photos/revision",
        Value::Null,
    )
    .await
    .1["revision"]
        .as_i64()
        .unwrap();
    let (status, versions) = call(
        app.clone(),
        "GET",
        &format!("{route}/versions"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(versions["items"][0]["contentHash"], "one.jpg");
    assert_eq!(versions["items"][0]["isDefault"], true);
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            &format!("{route}/default-version"),
            json!({"contentHash":"two.jpg"})
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let (status, versions) = call(
        app.clone(),
        "PUT",
        &format!("{route}/default-version"),
        json!({"contentHash":"one.jpg"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(versions["items"][0]["userSelected"], true);
    assert!(
        call(
            app.clone(),
            "GET",
            "/libraries/photos/revision",
            Value::Null
        )
        .await
        .1["revision"]
            .as_i64()
            .unwrap()
            > before
    );
    state.jobs.remove_folder(&folder.id).unwrap();
    let revision_before = state.store.library_revision("photos").unwrap();
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            &format!("{route}/default-version"),
            json!({"contentHash":"one.jpg"})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        state.store.library_revision("photos").unwrap(),
        revision_before
    );
    let candidates = call(
        app,
        "GET",
        &format!("{route}/version-candidates"),
        Value::Null,
    )
    .await
    .1;
    assert_eq!(candidates["items"][0]["assetID"], other);
    assert_eq!(
        std::fs::read(dir.path().join("originals/one.jpg")).unwrap(),
        b"original bytes"
    );
}

#[tokio::test]
async fn rejects_unknown_library_but_accepts_configured_empty_library() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let app = router(state.clone());
    for endpoint in [
        "counts",
        "assets",
        "revision",
        "directories",
        "navigation",
        "folders",
        "jobs",
        "hidden-directories",
    ] {
        let (status, body) = call(
            app.clone(),
            "GET",
            &format!("/libraries/hechuan/{endpoint}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{endpoint}: {body}");
        assert_eq!(body["detail"]["code"], "library_not_found");
        let (status, body) = call(
            app.clone(),
            "GET",
            &format!("/libraries/photos/{endpoint}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{endpoint}: {body}");
    }
    let (status, body) = call(
        app.clone(),
        "POST",
        "/libraries/hechuan/folders",
        json!({"path":"."}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(state.jobs.folders().unwrap().is_empty());
}

#[tokio::test]
async fn unknown_library_preview_metadata_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let app = router(state(dir.path()));
    let (status, body) = call(
        app,
        "GET",
        "/derivatives/00000000-0000-0000-0000-000000000001?role=preview&libraryID=hechuan",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["detail"]["code"], "library_not_found");
}
