use axum::{routing::get, Router};

use crate::config::Config;
use crate::db::SqlitePool;
use crate::embedding::{EmbeddingModel, EmbeddingStore};
use crate::llm::LlmClient;
use crate::static_files;
use std::sync::Arc;

pub mod auth;
pub mod common;
pub mod corrections;
pub mod history;
pub mod marks;
pub mod movies;
pub mod persons;
pub mod recommend;
pub mod tasks;

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub config: Config,
    pub llm: LlmClient,
    /// Embedding dependencies are optional so integration tests can build an
    /// AppState without triggering the fastembed model download or spinning
    /// up a LanceDB connection. Production (`main.rs`) always wraps these in
    /// `Some(..)`; only the `recommend` routes dereference them, and those
    /// routes are not expected to run under tests that pass `None`.
    pub embedding_model: Option<Arc<EmbeddingModel>>,
    pub embedding_store: Option<Arc<EmbeddingStore>>,
}

pub fn api_routes() -> Router<AppState> {
    Router::new()
        .merge(auth::routes())
        .merge(movies::routes())
        .merge(history::routes())
        .merge(marks::routes())
        .merge(tasks::routes())
        .merge(corrections::routes())
        .merge(recommend::routes())
        .merge(persons::routes())
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .nest("/api", api_routes())
        .fallback(get(static_files::static_handler))
        .with_state(state)
}
