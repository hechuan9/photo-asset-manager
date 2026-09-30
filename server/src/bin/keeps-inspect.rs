use anyhow::{Context, Result, ensure};
use keeps_server::media::{self, MediaProcessor};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Write},
    os::unix::fs::MetadataExt,
    path::{Component, Path},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    path: String,
}

fn stamp(path: &Path) -> Result<Value> {
    ensure!(path.is_absolute(), "path must be absolute");
    ensure!(
        !path
            .components()
            .any(|part| matches!(part, Component::ParentDir)),
        "parent traversal is not supported"
    );
    for ancestor in path.ancestors() {
        let info = std::fs::symlink_metadata(ancestor)
            .with_context(|| format!("stat {}", ancestor.display()))?;
        ensure!(
            !info.file_type().is_symlink(),
            "symlink is not supported: {}",
            ancestor.display()
        );
    }
    let info =
        std::fs::symlink_metadata(path).with_context(|| format!("stat {}", path.display()))?;
    ensure!(info.is_file(), "not a regular file: {}", path.display());
    Ok(
        json!({"size":info.len(), "mtime_ns":info.mtime() * 1_000_000_000 + info.mtime_nsec(),
        "ctime_ns":info.ctime() * 1_000_000_000 + info.ctime_nsec(), "dev":info.dev(), "ino":info.ino()}),
    )
}

fn inspect(path: &Path) -> Result<Value> {
    let before = stamp(path)?;
    ensure!(
        media::is_photo(path),
        "unsupported photo extension: {}",
        path.display()
    );
    let sidecar_before = media::sidecar_stamp(path)?;
    let metadata = MediaProcessor::new().extract(path)?;
    let sha256 = media::sha256_file(path)?;
    let visual_hash = media::jpeg_visual_hash(path, &metadata)?;
    let after = stamp(path)?;
    let sidecar_after = media::sidecar_stamp(path)?;
    ensure!(
        before == after,
        "source changed during inspection: {}",
        path.display()
    );
    ensure!(
        sidecar_before == sidecar_after,
        "sidecar changed during inspection: {}",
        path.display()
    );
    let raw = media::is_raw(path);
    Ok(
        json!({"path":path, "sha256":sha256, "visual_hash":visual_hash,
        "role":if raw {"raw_original"} else {"jpeg_original"},
        "width":metadata.width, "height":metadata.height,
        "priority":if raw {1} else if metadata.edited {3} else {2},
        "capture":{"captureTime":metadata.capture_time, "cameraMake":metadata.camera_make,
            "cameraModel":metadata.camera_model, "lensModel":metadata.lens_model,
            "cameraSerial":metadata.camera_serial, "captureOriginal":metadata.capture_original},
        "stamp":after, "sidecar_stamp":sidecar_after}),
    )
}

fn response(line: &str) -> Value {
    let request = serde_json::from_str::<Request>(line).context("parsing inspection request");
    match request {
        Ok(request) => inspect(Path::new(&request.path))
            .unwrap_or_else(|error| json!({"path":request.path,"error":format!("{error:#}")})),
        Err(error) => json!({"path":null,"error":format!("{error:#}")}),
    }
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!(
            "keeps-inspect: read JSON {{\"path\":\"/absolute/photo.jpg\"}} lines from stdin; write one read-only inspection result per line"
        );
        return Ok(());
    }
    ensure!(args.is_empty(), "expected no arguments (or --help)");
    let stdout = io::stdout();
    let mut output = stdout.lock();
    for line in io::stdin().lock().lines() {
        let line = line.context("reading inspection request")?;
        serde_json::to_writer(&mut output, &response(&line))
            .context("writing inspection result")?;
        writeln!(output)?;
        output.flush()?;
    }
    Ok(())
}
