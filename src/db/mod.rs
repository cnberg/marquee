pub mod models;
pub mod queries;

pub use models::*;
pub use queries::*;
pub use sqlx::SqlitePool;

use sqlx::sqlite::SqlitePoolOptions;
use std::path::Path;

pub async fn init_pool(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    if let Some(parent) = Path::new(database_url).parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        // rwc mode: read/write and create if missing.
        .connect(&format!("sqlite:{}?mode=rwc", database_url))
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    Ok(pool)
}
