use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Local, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
};

const RAW_EXTENSIONS: &[&str] = &[
    "3fr", "ari", "arw", "bay", "cr2", "cr3", "crw", "dcr", "dng", "erf", "fff", "iiq", "k25",
    "kdc", "mef", "mos", "mrw", "nef", "nrw", "orf", "pef", "raf", "raw", "rw2", "rwl", "sr2",
    "srf", "srw",
];

pub fn is_raw(path: &Path) -> bool {
    RAW_EXTENSIONS.contains(&extension(path).as_str())
}

pub fn is_photo(path: &Path) -> bool {
    is_raw(path)
        || ["jpg", "jpeg", "heic", "heif", "hif", "png", "tif", "tiff"]
            .contains(&extension(path).as_str())
}

fn extension(path: &Path) -> String {
    path.extension()
        .and_then(|v| v.to_str())
        .unwrap_or_default()
        .to_lowercase()
}

#[derive(Debug, Default, Clone)]
pub struct Metadata {
    pub capture_time: Option<String>,
    pub camera_make: String,
    pub camera_model: String,
    pub lens_model: String,
    pub rating: i64,
    pub camera_serial: String,
    pub capture_original: String,
    pub width: i64,
    pub height: i64,
    pub edited: bool,
    pub orientation: i64,
    pub color_space: String,
}

impl Metadata {
    pub fn fingerprint(&self, path: &Path) -> String {
        let name = path.file_stem().unwrap_or_default().to_string_lossy();
        [
            self.capture_time.as_deref().unwrap_or_default(),
            &self.camera_make,
            &self.camera_model,
            &self.lens_model,
            normalized_basename(&name),
        ]
        .join("|")
    }
}

// Match the two ordered suffix removals in the macOS ImageMetadata fingerprint.
fn normalized_basename(name: &str) -> &str {
    let mut name = name;
    if let Some(stem) = name.strip_suffix(')')
        && let Some((prefix, digits)) = stem.rsplit_once('(')
        && !digits.is_empty()
        && digits.chars().all(|c| c.is_ascii_digit())
    {
        name = prefix.trim_end();
    }
    let lower = name.to_lowercase();
    for suffix in ["edited", "export", "copy", "edit", "副本", "已编辑"] {
        if lower.ends_with(suffix) {
            name = &name[..name.len() - suffix.len()];
            if name.ends_with(['-', '_', ' ']) {
                name = &name[..name.len() - 1];
            }
            break;
        }
    }
    name
}

#[derive(Debug)]
pub struct Preview {
    pub width: u32,
    pub height: u32,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Default, Clone)]
pub struct MediaProcessor;

impl MediaProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn probe(&self) -> Result<()> {
        run(Command::new("exiftool").arg("-ver"))?;
        run(Command::new("heif-convert").arg("--version"))?;
        let formats = run(Command::new("convert").args(["-list", "format"]))?;
        ensure!(
            String::from_utf8_lossy(&formats).lines().any(|line| {
                let cols: Vec<_> = line.split_whitespace().collect();
                cols.first().is_some_and(|v| v.starts_with("HEIC"))
                    && cols.get(2).is_some_and(|v| v.contains('w'))
            }),
            "ImageMagick has no HEIC encoder"
        );
        // dcraw_emu prints its command-line contract and exits nonzero without input.
        let help = Command::new("dcraw_emu")
            .output()
            .context("starting LibRaw dcraw_emu")?;
        let help = format!(
            "{}{}",
            String::from_utf8_lossy(&help.stdout),
            String::from_utf8_lossy(&help.stderr)
        );
        ensure!(
            help.contains("-Z"),
            "LibRaw dcraw_emu lacks explicit output path support: {help}"
        );
        Ok(())
    }

    pub fn extract(&self, path: &Path) -> Result<Metadata> {
        let path = path
            .canonicalize()
            .with_context(|| format!("resolving {}", path.display()))?;
        let tags = read_tags(&path)?;
        let text = |key: &str| tags[key].as_str().unwrap_or_default().to_owned();
        let capture_original = tags["Composite:SubSecDateTimeOriginal"]
            .as_str()
            .or_else(|| tags["EXIF:DateTimeOriginal"].as_str())
            .unwrap_or_default();
        let capture_time = tags["Composite:SubSecDateTimeOriginal"]
            .as_str()
            .or_else(|| tags["EXIF:DateTimeOriginal"].as_str())
            .map(parse_capture_time)
            .transpose()?;
        let mut rating = rating(&tags);
        if rating.is_none() {
            for candidate in sidecars(&path)? {
                rating = rating_from_sidecar(&candidate)?;
                if rating.is_some() {
                    break;
                }
            }
        }
        Ok(Metadata {
            capture_time,
            camera_make: text("EXIF:Make"),
            camera_model: text("EXIF:Model"),
            lens_model: text("EXIF:LensModel"),
            rating: rating.unwrap_or(0),
            camera_serial: text("EXIF:BodySerialNumber"),
            capture_original: capture_original.to_owned(),
            width: image_dimension(&tags, "ImageWidth"),
            height: image_dimension(&tags, "ImageHeight"),
            edited: tags["XMP:HasSettings"].as_bool().unwrap_or(false)
                || tags["XMP:HasSettings"]
                    .as_str()
                    .is_some_and(|v| v.eq_ignore_ascii_case("true")),
            orientation: tags["EXIF:Orientation"].as_i64().unwrap_or(1),
            color_space: tags["EXIF:ColorSpace"].to_string(),
        })
    }

    pub fn generate_preview(&self, source: &Path, target: &Path) -> Result<Preview> {
        ensure!(
            is_photo(source),
            "unsupported photo extension: {}",
            source.display()
        );
        ensure!(
            !target.exists(),
            "preview target already exists: {}",
            target.display()
        );
        let source = source
            .canonicalize()
            .with_context(|| format!("resolving {}", source.display()))?;
        let scratch = tempfile::tempdir().context("creating media scratch directory")?;
        let decoded = scratch.path().join(if is_raw(&source) {
            "decoded.ppm"
        } else {
            "decoded.tiff"
        });
        let input = if is_raw(&source) {
            run(Command::new("dcraw_emu")
                .args(["-w", "-h", "-o", "1", "-Z"])
                .arg(&decoded)
                .arg(&source))
            .with_context(|| format!("decoding RAW {}", source.display()))?;
            ensure!(
                decoded.is_file(),
                "LibRaw produced no image for {}",
                source.display()
            );
            decoded.as_path()
        } else if ["heic", "heif", "hif"].contains(&extension(&source).as_str()) {
            // 100 MP 10-bit Hasselblad images exceed libheif's default 512 MiB block limit.
            // The subprocess instead has an OS address-space limit and a wall-clock deadline.
            run(Command::new("heif-convert")
                .args(["--disable-limits", "--quiet"])
                .arg(&source)
                .arg(&decoded))
            .with_context(|| format!("decoding HEIC {}", source.display()))?;
            decoded.as_path()
        } else {
            source.as_path()
        };
        // The source stays read-only; only the scratch directory contains intermediate images.
        let output = scratch.path().join("preview.heic");
        let input_frame = format!("{}[0]", input.display());
        run(Command::new("convert")
            .args([
                "-limit", "thread", "2", "-limit", "memory", "256MiB", "-limit", "map", "512MiB",
                "-limit", "disk", "1GiB", "-limit", "time", "120",
            ])
            .arg(input_frame)
            .args([
                "-auto-orient",
                "-thumbnail",
                "1200x1200>",
                "-strip",
                "-quality",
                "80",
            ])
            .arg(&output))
        .with_context(|| format!("generating preview for {}", source.display()))?;
        let dimensions = run(Command::new("identify")
            .args(["-format", "%w %h"])
            .arg(&output))?;
        let dimensions = String::from_utf8(dimensions).context("reading preview dimensions")?;
        let values = dimensions
            .split_whitespace()
            .map(str::parse::<u32>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            values.len() == 2
                && values[0] > 0
                && values[1] > 0
                && values[0] <= 1200
                && values[1] <= 1200,
            "invalid preview dimensions: {dimensions}"
        );
        let sha256 = sha256_file(&output)?;
        let size_bytes = output.metadata()?.len();
        // create_new prevents replacing an existing file, including an accidental original target.
        let mut destination = File::options()
            .write(true)
            .create_new(true)
            .open(target)
            .with_context(|| format!("creating preview {}", target.display()))?;
        std::io::copy(&mut File::open(&output)?, &mut destination)?;
        destination.sync_all()?;
        Ok(Preview {
            width: values[0],
            height: values[1],
            sha256,
            size_bytes,
        })
    }
}

fn run(command: &mut Command) -> Result<Vec<u8>> {
    let output = Command::new("timeout")
        .args(["--kill-after=5s", "180s", "prlimit", "--as=4294967296"])
        .arg(command.get_program())
        .args(command.get_args())
        .output()
        .with_context(|| format!("starting {command:?}"))?;
    if !output.status.success() {
        bail!(
            "{command:?} failed with {}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output.stdout)
}

fn read_tags(path: &Path) -> Result<Value> {
    let output = run(Command::new("exiftool")
        .args([
            "-j",
            "-G0",
            "-n",
            "-DateTimeOriginal",
            "-SubSecDateTimeOriginal",
            "-BodySerialNumber",
            "-ImageWidth",
            "-ImageHeight",
            "-Orientation",
            "-ColorSpace",
            "-HasSettings",
            "-Make",
            "-Model",
            "-LensModel",
            "-Rating",
            "-StarRating",
            "-Urgency",
            "--",
        ])
        .arg(path))?;
    let mut tags: Vec<Value> = serde_json::from_slice(&output).context("parsing ExifTool JSON")?;
    ensure!(
        tags.len() == 1,
        "ExifTool returned {} records for {}",
        tags.len(),
        path.display()
    );
    Ok(tags.remove(0))
}

fn rating(tags: &Value) -> Option<i64> {
    [
        "XMP:Rating",
        "XMP:Urgency",
        "EXIF:Rating",
        "EXIF:StarRating",
        "IPTC:Rating",
        "IPTC:Urgency",
    ]
    .iter()
    .find_map(|key| {
        tags[key]
            .as_i64()
            .or_else(|| tags[key].as_str()?.trim().parse().ok())
    })
    .map(|value: i64| value.clamp(0, 5))
}

fn sidecars(path: &Path) -> Result<Vec<PathBuf>> {
    let mut result = Vec::new();
    for extension in ["xmp", "XMP"] {
        let mut appended = path.as_os_str().to_os_string();
        appended.push(format!(".{extension}"));
        for candidate in [path.with_extension(extension), PathBuf::from(appended)] {
            match std::fs::symlink_metadata(&candidate) {
                Ok(info) if info.file_type().is_file() => result.push(candidate),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("stat sidecar {}", candidate.display()));
                }
            }
        }
    }
    Ok(result)
}

pub fn sidecar_stamp(path: &Path) -> Result<String> {
    let mut entries = Vec::new();
    for candidate in sidecars(path)? {
        let info = std::fs::symlink_metadata(&candidate)
            .with_context(|| format!("stat sidecar {}", candidate.display()))?;
        entries.push(format!(
            "{}:{}:{}",
            candidate.display(),
            info.len(),
            info.modified()?
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
    }
    Ok(entries.join("|"))
}

fn rating_from_sidecar(path: &Path) -> Result<Option<i64>> {
    Ok(rating(&read_tags(path)?))
}

fn image_dimension(tags: &Value, name: &str) -> i64 {
    ["File", "PNG", "QuickTime", "EXIF"]
        .iter()
        .find_map(|group| tags[format!("{group}:{name}")].as_i64())
        .unwrap_or(0)
}

fn parse_capture_time(value: &str) -> Result<String> {
    // Composite timestamps retain the camera's fractional seconds and explicit offset.
    if let Ok(date) = DateTime::parse_from_str(value, "%Y:%m:%d %H:%M:%S%.f%:z") {
        return Ok(date
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::AutoSi, true));
    }
    let naive = NaiveDateTime::parse_from_str(value, "%Y:%m:%d %H:%M:%S%.f")
        .with_context(|| format!("invalid EXIF capture time {value:?}"))?;
    let date = Local
        .from_local_datetime(&naive)
        .single()
        .with_context(|| {
            format!("EXIF capture time is ambiguous or falls in a local timezone gap: {value}")
        })?;
    Ok(date
        .with_timezone(&Utc)
        .to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

/// Exact JPEG compressed image identity, excluding EXIF/XMP/IPTC text metadata.
/// Rendering orientation, color space, ICC and Adobe transform segments remain evidence.
pub fn jpeg_visual_hash(path: &Path, metadata: &Metadata) -> Result<Option<String>> {
    if !["jpg", "jpeg"].contains(&extension(path).as_str()) {
        return Ok(None);
    }
    let mut input = File::open(path).with_context(|| format!("open JPEG {}", path.display()))?;
    jpeg_payload_hash(&mut input, metadata)
        .map(Some)
        .with_context(|| format!("read JPEG image identity {}", path.display()))
}

fn jpeg_payload_hash(input: &mut impl Read, metadata: &Metadata) -> Result<String> {
    let mut marker = [0u8; 2];
    input.read_exact(&mut marker)?;
    ensure!(marker == [0xff, 0xd8], "missing JPEG SOI");
    let mut digest = Sha256::new();
    digest.update(format!(
        "jpeg-image-v1|{}|{}|",
        metadata.orientation, metadata.color_space
    ));
    loop {
        input.read_exact(&mut marker)?;
        while marker == [0xff, 0xff] {
            input.read_exact(&mut marker[1..])?;
        }
        ensure!(marker[0] == 0xff, "invalid JPEG marker");
        if marker[1] == 0xda {
            digest.update(marker);
            let mut buffer = [0u8; 128 * 1024];
            let mut tail = [0u8; 2];
            let mut length = 0usize;
            let mut end_seen = false;
            loop {
                let count = input.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                digest.update(&buffer[..count]);
                for &byte in &buffer[..count] {
                    tail = [tail[1], byte];
                    end_seen |= tail == [0xff, 0xd9];
                }
                length += count;
            }
            ensure!(length >= 4 && end_seen, "incomplete JPEG scan");
            return Ok(format!("jpeg-v1:{:x}", digest.finalize()));
        }
        ensure!(
            !matches!(marker[1], 0x00 | 0xd8 | 0xd9 | 0xff),
            "unexpected JPEG marker"
        );
        let mut size = [0u8; 2];
        input.read_exact(&mut size)?;
        let length = u16::from_be_bytes(size) as usize;
        ensure!(length >= 2, "invalid JPEG segment length");
        let mut segment = vec![0u8; length - 2];
        input.read_exact(&mut segment)?;
        if !matches!(marker[1], 0xe1 | 0xed | 0xfe) {
            digest.update(marker);
            digest.update(size);
            digest.update(segment);
        }
    }
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("opening {} for hashing", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 128 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .with_context(|| format!("hashing {}", path.display()))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swift_fingerprint_suffix_and_rating_contract() {
        let metadata = Metadata {
            capture_time: Some("2024-01-02T12:00:00.000Z".into()),
            camera_make: "Canon".into(),
            camera_model: "R5".into(),
            lens_model: "50mm".into(),
            rating: 5,
            ..Default::default()
        };
        assert_eq!(
            metadata.fingerprint(Path::new("/photos/IMG_123-Edited (2).CR3")),
            "2024-01-02T12:00:00.000Z|Canon|R5|50mm|IMG_123"
        );
        assert_eq!(normalized_basename("照片副本"), "照片");
        assert_eq!(normalized_basename("IMG (word)"), "IMG (word)");
        assert_eq!(
            rating(&serde_json::json!({"XMP:Rating":-1, "IPTC:Urgency":5})),
            Some(0)
        );
    }

    #[test]
    fn sidecar_stamp_detects_changes_and_ignores_symlinks() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let photo = dir.path().join("a.jpg");
        assert_eq!(sidecar_stamp(&photo)?, "");
        std::fs::write(dir.path().join("a.xmp"), b"first")?;
        let first = sidecar_stamp(&photo)?;
        std::fs::write(dir.path().join("a.xmp"), b"changed and larger")?;
        assert_ne!(first, sidecar_stamp(&photo)?);
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(dir.path().join("a.xmp"), dir.path().join("b.xmp"))?;
            assert_eq!(sidecar_stamp(&dir.path().join("b.jpg"))?, "");
        }
        Ok(())
    }

    #[test]
    fn capture_time_keeps_subseconds_and_offset() -> Result<()> {
        assert_eq!(
            parse_capture_time("2025:11:24 20:02:35.123456-05:00")?,
            "2025-11-25T01:02:35.123456Z"
        );
        assert_ne!(
            parse_capture_time("2025:11:24 20:02:35.100-05:00")?,
            parse_capture_time("2025:11:24 20:02:35.200-05:00")?
        );
        assert!(parse_capture_time("not-a-date").is_err());
        Ok(())
    }

    #[test]
    fn jpeg_identity_ignores_text_metadata_but_keeps_image_and_rendering() -> Result<()> {
        fn fixture(text: &[u8], pixels: u8, profile: u8) -> Vec<u8> {
            let mut bytes = vec![0xff, 0xd8, 0xff, 0xe1];
            bytes.extend_from_slice(&((text.len() + 2) as u16).to_be_bytes());
            bytes.extend_from_slice(text);
            bytes.extend_from_slice(&[
                0xff, 0xe2, 0, 3, profile, 0xff, 0xda, 0, 2, pixels, 0xff, 0xd9,
            ]);
            bytes
        }
        let metadata = Metadata {
            orientation: 1,
            ..Default::default()
        };
        let hash = |bytes: Vec<u8>, m: &Metadata| jpeg_payload_hash(&mut bytes.as_slice(), m);
        let original = hash(fixture(b"original", 42, 1), &metadata)?;
        assert_eq!(
            original,
            hash(fixture(b"different rating and name", 42, 1), &metadata)?
        );
        assert_ne!(original, hash(fixture(b"original", 43, 1), &metadata)?);
        assert_ne!(original, hash(fixture(b"original", 42, 2), &metadata)?);
        assert_ne!(
            original,
            hash(
                fixture(b"original", 42, 1),
                &Metadata {
                    orientation: 6,
                    ..metadata
                }
            )?
        );
        assert!(hash(vec![0xff, 0xd8, 0xff, 0xda, 0, 2], &Metadata::default()).is_err());
        Ok(())
    }

    #[test]
    #[ignore = "requires the Linux media runtime (ExifTool, ImageMagick HEIC, LibRaw)"]
    fn real_media_roundtrip() -> Result<()> {
        let processor = MediaProcessor::new();
        processor.probe()?;
        let directory = tempfile::tempdir()?;
        let original = directory.path().join("source.jpg");
        run(Command::new("convert")
            .args(["-size", "1600x800", "gradient:red-blue"])
            .arg(&original))?;
        run(Command::new("exiftool")
            .args([
                "-overwrite_original",
                "-Make=Test",
                "-Model=Camera",
                "-LensModel=50mm",
                "-Rating=4",
                "-DateTimeOriginal=2024:01:02 12:00:00",
            ])
            .arg(&original))?;
        let before = sha256_file(&original)?;
        let metadata = processor.extract(&original)?;
        assert_eq!(metadata.camera_make, "Test");
        assert_eq!(metadata.rating, 4);
        assert!(metadata.capture_time.is_some());
        let preview_path = directory.path().join("preview.heic");
        let preview = processor.generate_preview(&original, &preview_path)?;
        assert_eq!((preview.width, preview.height), (1200, 600));
        assert_eq!(before, sha256_file(&original)?);
        assert_eq!(preview.sha256, sha256_file(&preview_path)?);
        assert!(processor.generate_preview(&original, &original).is_err());
        for extension in ["png", "tiff", "heic"] {
            let source = directory.path().join(format!("source.{extension}"));
            run(Command::new("convert").arg(&original).arg(&source))?;
            let target = directory.path().join(format!("from-{extension}.heic"));
            let preview = processor.generate_preview(&source, &target)?;
            assert_eq!((preview.width, preview.height), (1200, 600));
        }
        Ok(())
    }
}
