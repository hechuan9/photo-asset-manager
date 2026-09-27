use anyhow::{Context, Result, bail};
use keeps_server::{
    api::{AppState, router},
    jobs::Jobs,
    media::MediaProcessor,
    previews::PreviewStorage,
    store::Store,
};
use std::{
    collections::HashSet,
    env,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "keeps_server=info,tower_http=info".into()),
        )
        .init();
    let root = PathBuf::from(env::var("KEEPS_ROOT").context("KEEPS_ROOT is required")?);
    let migrate = match env::args().nth(1).as_deref() {
        None => false,
        Some("migrate") => true,
        Some(other) => bail!("unknown command {other}; use keeps-server [migrate]"),
    };
    let database = root.join("db/control_plane.sqlite");
    if migrate {
        Store::open(&database, true, HashSet::new())?;
        tracing::info!("database migration complete");
        return Ok(());
    }
    let original = PathBuf::from(env::var("ORIGINAL_ROOT").context("ORIGINAL_ROOT is required")?);
    let access_token = env::var("KEEPS_ACCESS_TOKEN").context("KEEPS_ACCESS_TOKEN is required")?;
    if access_token.len() < 32 {
        bail!("KEEPS_ACCESS_TOKEN must be at least 32 bytes");
    }
    let base_url = env::var("CONTROL_PLANE_PUBLIC_BASE_URL")
        .context("CONTROL_PLANE_PUBLIC_BASE_URL is required")?;
    let previews = Arc::new(PreviewStorage::new(
        &root,
        Some(&original),
        &base_url,
        &access_token,
    )?);
    let auto_create = match env::var("CONTROL_PLANE_AUTO_CREATE_SCHEMA").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(_) => false,
        Ok(_) => bail!("CONTROL_PLANE_AUTO_CREATE_SCHEMA must be 0 or 1"),
    };
    let trusted: HashSet<String> = env::var("CONTROL_PLANE_TRUSTED_DEVICE_IDS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .collect();
    let store = Arc::new(Store::open(&database, auto_create, trusted)?);
    let jobs = Arc::new(Jobs::open(&root.join("db/jobs.sqlite"), &original)?);
    let original_root_names = keeps_server::navigation::parse_root_names(
        jobs.root(),
        env::var("KEEPS_ORIGINAL_ROOT_SOURCES").ok().as_deref(),
    )?;
    if let Ok(library) = env::var("KEEPS_LIBRARY_ID") {
        if library.trim().is_empty() {
            bail!("KEEPS_LIBRARY_ID cannot be empty");
        }
        if jobs.folders()?.iter().all(|f| f.library_id != library) {
            jobs.add_folder(&library, ".")?;
        }
    }
    MediaProcessor::new().probe().context("NAS media runtime")?;
    let interval = env::var("KEEPS_SCAN_INTERVAL_SECONDS")
        .unwrap_or_else(|_| "300".into())
        .parse::<u64>()
        .context("invalid KEEPS_SCAN_INTERVAL_SECONDS")?;
    if interval == 0 {
        bail!("KEEPS_SCAN_INTERVAL_SECONDS must be positive");
    }
    let address = env::var("KEEPS_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:2283".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let mut worker = tokio::task::spawn_blocking({
        let (store, jobs, previews, stop) =
            (store.clone(), jobs.clone(), previews.clone(), stop.clone());
        move || {
            keeps_server::worker::run(store, jobs, previews, stop, Duration::from_secs(interval))
        }
    });
    tracing::info!(%address, "Keeps Rust server started");
    let http = axum::serve(
        listener,
        router(Arc::new(AppState {
            store,
            previews,
            jobs,
            access_token,
            original_root_names,
        })),
    )
    .with_graceful_shutdown({
        let stop = stop.clone();
        async move {
            shutdown().await;
            stop.store(true, Ordering::Relaxed);
        }
    });
    tokio::select! {
        result=http=>{stop.store(true,Ordering::Relaxed);result?;worker.await??;}
        result=&mut worker=>{result??;if !stop.load(Ordering::Relaxed) {bail!("NAS worker exited unexpectedly");}}
    }
    Ok(())
}
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .expect("install interrupt handler");
}
