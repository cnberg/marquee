use axum::{routing::get, Router};

use crate::config::Config;
use crate::db::SqlitePool;
use crate::embedding::{EmbeddingModel, EmbeddingStore};
use crate::llm::LlmClient;
use crate::static_files;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::db::Movie;
use serde::Serialize;
use std::collections::HashSet;
use std::time::Instant;
use tokio::sync::Mutex;

pub mod auth;
pub mod benchmark;
pub mod common;
pub mod corrections;
pub mod douban;
pub mod history;
pub mod marks;
pub mod movies;
pub mod persons;
pub mod recommend;
pub mod tasks;

#[derive(Debug, Clone, Serialize)]
pub struct CachedMostRelatedItem {
    pub movie: Movie,
    pub ref_count: i64,
    pub downloading: bool,
}

pub struct MostRelatedCache {
    pub items: Vec<CachedMostRelatedItem>,
    pub updated_at: Instant,
    /// Snapshot of `MAX(dir_movie_mappings.updated_at)` at cache creation. On
    /// cache hit, we re-query the same MAX and invalidate if it advanced —
    /// any bind/unbind/refetch bumps that row's `updated_at`, so this gives
    /// us automatic invalidation across all mutation paths without each
    /// handler having to remember to clear the cache.
    pub mappings_snapshot: Option<String>,
}

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub config: Arc<RwLock<Config>>,
    pub llm: LlmClient,
    /// Embedding dependencies are optional so integration tests can build an
    /// AppState without triggering the fastembed model download or spinning
    /// up a LanceDB connection. Production (`main.rs`) always wraps these in
    /// `Some(..)`; only the `recommend` routes dereference them, and those
    /// routes are not expected to run under tests that pass `None`.
    pub embedding_model: Option<Arc<EmbeddingModel>>,
    pub embedding_store: Option<Arc<EmbeddingStore>>,
    pub most_related_cache: Arc<RwLock<Option<MostRelatedCache>>>,
    /// In-flight `(user_id, today)` cache_keys for the most-related-tip LLM
    /// generator. The handler returns immediately when this DB cache misses,
    /// spawning a background task to populate it; this set deduplicates
    /// concurrent spawns so only one LLM call per key is in flight.
    pub most_related_reasons_pending: Arc<Mutex<HashSet<Option<i64>>>>,
}

pub fn api_routes() -> Router<AppState> {
    Router::new()
        .merge(auth::routes())
        .merge(movies::routes())
        .merge(history::routes())
        .merge(marks::routes())
        .merge(tasks::routes())
        .merge(corrections::routes())
        .merge(douban::routes())
        .merge(recommend::routes())
        .merge(persons::routes())
        .merge(benchmark::routes())
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .nest("/api", api_routes())
        .fallback(get(static_files::static_handler))
        .with_state(state)
}
