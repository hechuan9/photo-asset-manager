//! Read-only, lazy navigation over each library's active tracking roots.
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
};

use crate::{jobs::Jobs, store::StoreError};

pub type RootNames = BTreeMap<PathBuf, String>;

pub fn parse_root_names(root: &Path, raw: Option<&str>) -> Result<RootNames> {
    let Some(raw) = raw else {
        return Ok(RootNames::new());
    };
    let sources: BTreeMap<PathBuf, PathBuf> = serde_json::from_str(raw).context(
        "KEEPS_ORIGINAL_ROOT_SOURCES must be a JSON object of container paths to source paths",
    )?;
    let mut names = RootNames::new();
    for (path, source) in sources {
        if !path.is_absolute()
            || !path.starts_with(root)
            || path
                .components()
                .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
            || !source.is_absolute()
        {
            bail!(
                "invalid KEEPS_ORIGINAL_ROOT_SOURCES entry for {}",
                path.display()
            );
        }
        let name = source
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.trim().is_empty() && !name.contains('\0'))
            .context("source path must have a UTF-8 basename")?;
        names.insert(path, name.to_owned());
    }
    Ok(names)
}

fn visible_directory(entry: std::io::Result<std::fs::DirEntry>) -> Result<Option<PathBuf>> {
    let entry = entry?;
    let name = entry.file_name();
    let name = name.to_str().context("directory name must be UTF-8")?;
    if name.starts_with('.') || name == "@eaDir" || name == "#recycle" {
        return Ok(None);
    }
    Ok(entry.file_type()?.is_dir().then(|| entry.path()))
}

fn directory_entries(path: &Path) -> Result<std::fs::ReadDir> {
    std::fs::read_dir(path).with_context(|| format!("read directory {}", path.display()))
}

fn children(path: &Path) -> Result<Vec<PathBuf>> {
    let mut children = Vec::new();
    for entry in directory_entries(path)? {
        if let Some(path) = visible_directory(entry)? {
            children.push(path);
        }
    }
    children.sort();
    Ok(children)
}

fn has_visible_children(path: &Path) -> Result<bool> {
    for entry in directory_entries(path)? {
        if visible_directory(entry)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn navigation(
    jobs: &Jobs,
    library: &str,
    path: Option<&str>,
    names: &RootNames,
) -> Result<Value> {
    let roots: Vec<PathBuf> = jobs
        .folders()?
        .into_iter()
        .filter(|folder| folder.active && folder.library_id == library)
        .map(|folder| PathBuf::from(folder.path))
        .collect();
    let selected = path
        .map(|path| {
            jobs.validate_path(Path::new(path)).map_err(|error| {
                anyhow::Error::from(StoreError {
                    status: 422,
                    code: "invalid_directory".into(),
                    message: format!("{error:#}"),
                })
            })
        })
        .transpose()?;
    let paths = if let Some(selected) = &selected {
        if !roots.iter().any(|root| selected.starts_with(root)) {
            return Err(StoreError {
                status: 404,
                code: "directory_not_tracked".into(),
                message: "directory is not tracked in this library".into(),
            }
            .into());
        }
        children(selected)?
    } else if roots.iter().any(|root| root == jobs.root()) {
        children(jobs.root())?
    } else {
        // Overlapping tracking roots appear once, at their highest configured ancestor.
        roots
            .iter()
            .filter(|root| {
                !roots
                    .iter()
                    .any(|ancestor| ancestor != *root && root.starts_with(ancestor))
            })
            .cloned()
            .collect()
    };
    let directories = paths
        .into_iter()
        .map(|path| {
            let path = jobs.validate_path(&path)?;
            let name = names
                .get(&path)
                .map(String::as_str)
                .or_else(|| path.file_name().and_then(|name| name.to_str()))
                .context("directory name must be UTF-8")?;
            let has_children = has_visible_children(&path)?;
            Ok(json!({"path":path,"name":name,"hasChildren":has_children}))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({"path":selected,"directories":directories}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_probe_ignores_files_hidden_system_and_symlink_directories() -> Result<()> {
        let fixture = tempfile::tempdir()?;
        let root = fixture.path();
        std::fs::write(root.join("photo.jpg"), b"fixture")?;
        for excluded in [".hidden", "@eaDir", "#recycle"] {
            std::fs::create_dir_all(root.join(excluded).join("nested"))?;
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(root.join(".hidden"), root.join("linked"))?;
        assert!(!has_visible_children(root)?);
        std::fs::create_dir(root.join("visible"))?;
        assert!(has_visible_children(root)?);
        assert!(!has_visible_children(&root.join("visible"))?);
        assert!(has_visible_children(&root.join("missing")).is_err());
        Ok(())
    }

    #[test]
    fn navigation_returns_boolean_expandability_for_each_child() -> Result<()> {
        let fixture = tempfile::tempdir()?;
        let root = fixture.path().canonicalize()?;
        std::fs::create_dir_all(root.join("branch/child"))?;
        std::fs::create_dir_all(root.join("leaf/.hidden"))?;
        let jobs = Jobs::open(&fixture.path().join("jobs.sqlite"), &root)?;
        jobs.add_folder("photos", root.to_str().unwrap())?;
        let page = navigation(&jobs, "photos", None, &RootNames::new())?;
        assert_eq!(page["directories"][0]["hasChildren"], true);
        assert_eq!(page["directories"][1]["hasChildren"], false);
        let page = navigation(
            &jobs,
            "photos",
            root.join("branch").to_str(),
            &RootNames::new(),
        )?;
        assert_eq!(page["directories"][0]["hasChildren"], false);
        Ok(())
    }

    #[test]
    fn source_names_only_override_exact_container_paths() -> Result<()> {
        let fixture = tempfile::tempdir()?;
        let root = fixture.path().canonicalize()?;
        std::fs::create_dir_all(root.join("library/year"))?;
        let jobs = Jobs::open(&fixture.path().join("jobs.sqlite"), &root)?;
        jobs.add_folder("photos", root.to_str().unwrap())?;
        let sources = json!({root.join("library").to_str().unwrap(): "/volume2/photo"});
        let names = parse_root_names(&root, Some(&sources.to_string()))?;
        let page = navigation(&jobs, "photos", None, &names)?;
        assert_eq!(page["directories"][0]["name"], "photo");
        assert_eq!(
            page["directories"][0]["path"],
            root.join("library").to_str().unwrap()
        );
        assert!(!page.to_string().contains("/volume2"));
        let page = navigation(&jobs, "photos", root.join("library").to_str(), &names)?;
        assert_eq!(page["directories"][0]["name"], "year");
        assert!(parse_root_names(&root, Some("[]")).is_err());
        assert!(parse_root_names(&root, Some(r#"{"/outside":"/volume2/photo"}"#)).is_err());
        assert!(parse_root_names(&root, None)?.is_empty());
        Ok(())
    }
}
