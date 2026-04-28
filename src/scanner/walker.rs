use std::collections::HashSet;
use std::io;
use std::path::Path;

use sqlx::SqlitePool;
use tokio::fs;

use super::ssh;
use crate::db::queries;

#[allow(dead_code)]
#[derive(Debug)]
pub struct ScanResult {
    pub added: u32,
    pub deleted: u32,
}

/// Scan all configured directories (local and remote).
pub async fn scan_all_dirs(
    pool: &SqlitePool,
    movie_dirs: &[String],
    ssh_key_path: Option<&str>,
) -> Result<ScanResult, io::Error> {
    let mut total_added = 0u32;
    let mut total_deleted = 0u32;

    for dir in movie_dirs {
        let result = if dir.starts_with("ssh://") {
            scan_remote_dir(pool, dir, ssh_key_path).await
        } else {
            scan_local_dir(pool, Path::new(dir)).await
        };

        match result {
            Ok(r) => {
                total_added += r.added;
                total_deleted += r.deleted;
            }
            Err(e) => {
                tracing::error!(dir = dir.as_str(), error = %e, "scan failed for directory");
            }
        }
    }

    tracing::info!(added = total_added, deleted = total_deleted, dirs = movie_dirs.len(), "all dirs scan complete");
    Ok(ScanResult { added: total_added, deleted: total_deleted })
}

/// Scan a local directory for first-level subdirectories.
async fn scan_local_dir(pool: &SqlitePool, movie_dir: &Path) -> Result<ScanResult, io::Error> {
    if !movie_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("movie_dir not found: {}", movie_dir.display()),
        ));
    }

    let mut disk_dirs: HashSet<String> = HashSet::new();
    let mut entries = fs::read_dir(movie_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            if let Some(path_str) = entry.path().to_str() {
                disk_dirs.insert(path_str.to_string());
            }
        }
    }

    reconcile(pool, &disk_dirs, movie_dir.to_str().unwrap_or("")).await
}

/// Scan a remote directory via SSH+SFTP.
async fn scan_remote_dir(
    pool: &SqlitePool,
    url: &str,
    ssh_key_path: Option<&str>,
) -> Result<ScanResult, io::Error> {
    let parsed = ssh::parse_ssh_url(url)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    let remote_dirs = ssh::list_remote_dirs(&parsed, ssh_key_path).await?;

    // For remote dirs, we store the ssh:// full path as dir_path and the basename as dir_name.
    // reconcile handles add/delete against DB.
    reconcile_with_names(pool, &remote_dirs, url).await
}

/// Reconcile local dirs (derive dir_name from path).
async fn reconcile(pool: &SqlitePool, disk_dirs: &HashSet<String>, source_prefix: &str) -> Result<ScanResult, io::Error> {
    let db_dirs: HashSet<String> = queries::get_all_dir_paths(pool)
        .await
        .map_err(to_io_error)?
        .into_iter()
        .filter(|p| p.starts_with(source_prefix))
        .collect();

    let mut added = 0u32;
    for dir_path in disk_dirs.difference(&db_dirs) {
        let dir_name = Path::new(dir_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        if dir_name.is_empty() {
            continue;
        }

        queries::insert_media_dir(pool, dir_path, &dir_name)
            .await
            .map_err(to_io_error)?;
        added += 1;
    }

    let mut deleted = 0u32;
    for dir_path in db_dirs.difference(disk_dirs) {
        queries::mark_dir_deleted(pool, dir_path)
            .await
            .map_err(to_io_error)?;
        deleted += 1;
    }

    Ok(ScanResult { added, deleted })
}

/// Reconcile remote dirs (dir_name already extracted by ssh::list_remote_dirs).
async fn reconcile_with_names(
    pool: &SqlitePool,
    remote_dirs: &[(String, String)],
    source_prefix: &str,
) -> Result<ScanResult, io::Error> {
    let disk_dirs: HashSet<String> = remote_dirs.iter().map(|(p, _)| p.clone()).collect();

    let db_dirs: HashSet<String> = queries::get_all_dir_paths(pool)
        .await
        .map_err(to_io_error)?
        .into_iter()
        .filter(|p| p.starts_with("ssh://"))
        .filter(|p| p.starts_with(source_prefix) || {
            // Match by same host+path prefix
            ssh::parse_ssh_url(source_prefix)
                .ok()
                .map(|u| p.contains(&u.host) && p.contains(&u.path))
                .unwrap_or(false)
        })
        .collect();

    let mut added = 0u32;
    for (dir_path, dir_name) in remote_dirs {
        if db_dirs.contains(dir_path) {
            continue;
        }
        if dir_name.is_empty() {
            continue;
        }

        queries::insert_media_dir(pool, dir_path, dir_name)
            .await
            .map_err(to_io_error)?;
        added += 1;
    }

    let mut deleted = 0u32;
    for dir_path in &db_dirs {
        if !disk_dirs.contains(dir_path) {
            queries::mark_dir_deleted(pool, dir_path)
                .await
                .map_err(to_io_error)?;
            deleted += 1;
        }
    }

    Ok(ScanResult { added, deleted })
}

fn to_io_error(err: sqlx::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err)
}
