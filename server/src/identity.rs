use anyhow::{Context, Result, ensure};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};
use uuid::Uuid;

#[derive(Debug)]
pub struct IdentityWrite {
    pub id: String,
    pub changed: bool,
    pub path: PathBuf,
}

pub fn normalize(value: &str) -> Option<String> {
    let value = value.trim();
    let value = ["urn:uuid:", "xmp.did:", "uuid:"]
        .into_iter()
        .find_map(|prefix| {
            value
                .get(..prefix.len())
                .filter(|head| head.eq_ignore_ascii_case(prefix))
                .map(|_| &value[prefix.len()..])
        })
        .unwrap_or(value);
    Uuid::parse_str(value)
        .ok()
        .filter(|id| !id.is_nil())
        .map(|id| id.to_string())
}

fn run(command: &mut Command) -> Result<Vec<u8>> {
    crate::media::run(command)
}

fn embedded(path: &Path) -> Result<Option<String>> {
    let output = run(Command::new("exiftool")
        .args(["-s3", "-XMP-xmpMM:OriginalDocumentID", "--"])
        .arg(path))?;
    Ok(String::from_utf8_lossy(&output).lines().find_map(normalize))
}

pub fn sidecar_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".xmp");
    PathBuf::from(name)
}

pub fn read(path: &Path) -> Result<Option<String>> {
    if let Some(id) = embedded(path)? {
        return Ok(Some(id));
    }
    for sidecar in crate::media::sidecars(path)? {
        if sidecar != path
            && let Some(id) = embedded(&sidecar)?
        {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

fn can_embed(path: &Path) -> Result<bool> {
    static WRITABLE: OnceLock<Vec<String>> = OnceLock::new();
    if WRITABLE.get().is_none() {
        let output = run(Command::new("exiftool").arg("-listwf"))?;
        let extensions = String::from_utf8(output)
            .context("ExifTool writable extensions")?
            .split_whitespace()
            .map(str::to_ascii_lowercase)
            .collect();
        let _ = WRITABLE.set(extensions);
    }
    Ok(path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| {
            WRITABLE
                .get()
                .unwrap()
                .iter()
                .any(|item| item.eq_ignore_ascii_case(ext))
        }))
}

fn write(path: &Path, id: &str) -> Result<()> {
    if let Ok(info) = std::fs::symlink_metadata(path) {
        ensure!(
            info.file_type().is_file(),
            "metadata target must be a regular file"
        );
    }
    run(Command::new("exiftool")
        .args(["-overwrite_original", "-P"])
        .arg(format!("-XMP-xmpMM:OriginalDocumentID=xmp.did:{id}"))
        .arg("--")
        .arg(path))?;
    Ok(())
}

pub fn ensure(path: &Path, proposed_id: &str) -> Result<IdentityWrite> {
    if let Some(id) = read(path)? {
        return Ok(IdentityWrite {
            id,
            changed: false,
            path: path.to_owned(),
        });
    }
    let id = normalize(proposed_id).context("invalid proposed photo identity UUID")?;
    let destination = if can_embed(path)? {
        path.to_owned()
    } else {
        sidecar_path(path)
    };
    // Full filenames keep RAW and JPEG sidecars independent; existing XMP fields remain intact.
    write(&destination, &id)?;
    Ok(IdentityWrite {
        id,
        changed: true,
        path: destination,
    })
}

pub fn generated(path: &Path, root_id: &str) -> Result<()> {
    let id = normalize(root_id).context("invalid generated photo identity UUID")?;
    ensure!(
        path.is_file(),
        "generated photo does not exist: {}",
        path.display()
    );
    write(path, &id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_standard_uuid_forms() {
        let id = "c0421f3c-1637-467a-94b7-77be3450f4aa";
        for value in [
            id.to_owned(),
            format!("xmp.did:{id}"),
            format!("uuid:{id}"),
            format!("urn:uuid:{id}"),
            format!(" XMP.DID:{} ", id.to_uppercase()),
        ] {
            assert_eq!(normalize(&value).as_deref(), Some(id));
        }
        assert_eq!(normalize("not-a-uuid"), None);
        assert_eq!(normalize("00000000-0000-0000-0000-000000000000"), None);
    }

    #[test]
    fn sidecars_preserve_source_extension() {
        assert_eq!(
            sidecar_path(Path::new("/photos/one.3FR")),
            Path::new("/photos/one.3FR.xmp")
        );
        assert_ne!(
            sidecar_path(Path::new("one.3FR")),
            sidecar_path(Path::new("one.jpg"))
        );
    }

    #[test]
    #[ignore = "requires ExifTool and ImageMagick"]
    fn metadata_roundtrip_preserves_identity_and_other_tags() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let photo = directory.path().join("photo.jpg");
        run(Command::new("convert")
            .args(["-size", "20x20", "xc:red"])
            .arg(&photo))?;
        run(Command::new("exiftool")
            .args(["-overwrite_original", "-Rating=4"])
            .arg(&photo))?;
        let original_pixels = run(Command::new("convert").arg(&photo).arg("rgb:-"))?;
        let id = Uuid::new_v4().to_string();
        assert!(ensure(&photo, &id)?.changed);
        let second = ensure(&photo, &Uuid::new_v4().to_string())?;
        assert!(!second.changed);
        assert_eq!(second.id, id);
        assert_eq!(read(&photo)?.as_deref(), Some(id.as_str()));
        assert_eq!(
            original_pixels,
            run(Command::new("convert").arg(&photo).arg("rgb:-"))?
        );
        assert_eq!(
            run(Command::new("exiftool")
                .args(["-s3", "-Rating"])
                .arg(&photo))?,
            b"4\n"
        );
        let sidecar = sidecar_path(Path::new("photo.3FR"));
        let sidecar = directory.path().join(sidecar);
        write(&sidecar, &id)?;
        assert_eq!(embedded(&sidecar)?.as_deref(), Some(id.as_str()));
        Ok(())
    }
}
