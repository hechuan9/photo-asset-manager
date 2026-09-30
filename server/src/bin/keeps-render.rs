use anyhow::{Context, Result, ensure};
use keeps_server::media::MediaProcessor;
use std::{env, path::Path};

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    ensure!(
        args.len() == 6,
        "usage: keeps-render SOURCE OUTPUT_DIR GENERATE_STANDARD EDGE QUALITY"
    );
    let source = Path::new(&args[1]);
    let output = Path::new(&args[2]);
    let generate_standard: bool = args[3].parse()?;
    let edge: u32 = args[4].parse()?;
    let quality: u8 = args[5].parse()?;
    ensure!(edge > 0 && quality <= 100, "invalid thumbnail settings");
    let media = MediaProcessor::new();
    let standard = output.join("standard.heic");
    if generate_standard {
        let metadata = media.extract(source)?;
        ensure!(
            metadata.width > 0 && metadata.height > 0,
            "source dimensions missing"
        );
        media.generate_standard(
            source,
            &standard,
            metadata
                .width
                .max(metadata.height)
                .try_into()
                .context("invalid dimensions")?,
        )?;
        media.preserve_standard_metadata(source, &standard)?;
    }
    media.generate_image_quality(
        if generate_standard { &standard } else { source },
        &output.join("thumbnail.heic"),
        edge,
        quality,
    )?;
    Ok(())
}
