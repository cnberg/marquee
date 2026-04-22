use std::collections::HashSet;
use std::io;
use std::path::Path;

use sqlx::SqlitePool;
use tokio::fs;

use crate::db::queries;

#[derive(Debug)]
pub struct ScanResult {
    pub added: u32,
    pub deleted: u32,
}

pub async fn scan_movie_dir(pool: &SqlitePool, movie_dir: &Path) -> Result<ScanResult, io::Error> {
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

    let db_dirs: HashSet<String> = queries::get_all_dir_paths(pool)
        .await
        .map_err(to_io_error)?
        .into_iter()
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
    for dir_path in db_dirs.difference(&disk_dirs) {
        queries::mark_dir_deleted(pool, dir_path)
            .await
            .map_err(to_io_error)?;
        deleted += 1;
    }

    tracing::info!(added, deleted, "movie_dir scan complete");
    Ok(ScanResult { added, deleted })
}

fn to_io_error(err: sqlx::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err)
}
