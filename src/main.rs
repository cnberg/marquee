mod api;
mod auth;
mod config;
mod db;
mod embedding;
mod llm;
mod search;
mod scanner;
mod static_files;
#[cfg(test)]
mod test_support;
mod tmdb;
mod worker;

use config::Config;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = Config::load().expect("failed to load config");
    tracing::info!("config loaded, movie_dir={}", config.scan.movie_dir);

    let pool = db::init_pool(&config.database.path)
        .await
        .expect("failed to init database");
    tracing::info!("database initialized");

    worker::scheduler::start_workers(pool.clone(), config.clone()).await;
    tracing::info!("workers started");

    let llm_client = llm::LlmClient::new(&config.llm);

    // Initialize embedding model (downloads model on first run)
    tracing::info!("initializing embedding model...");
    let embedding_model = embedding::EmbeddingModel::new()
        .expect("failed to initialize embedding model");
    let embedding_model = Arc::new(embedding_model);
    tracing::info!("embedding model ready (dim={})", embedding_model.dimension());

    // Initialize LanceDB
    let lancedb_path = "data/lancedb";
    let embedding_store = embedding::EmbeddingStore::new(lancedb_path, embedding_model.dimension())
        .await
        .expect("failed to initialize LanceDB");
    embedding_store
        .ensure_table()
        .await
        .expect("failed to ensure LanceDB table");
    let embedding_store = Arc::new(embedding_store);
    tracing::info!("LanceDB initialized at {}", lancedb_path);

    // Background task: index movies that don't have embeddings yet
    {
        let pool = pool.clone();
        let model = embedding_model.clone();
        let store = embedding_store.clone();
        tokio::spawn(async move {
            if let Err(e) = backfill_embeddings(&pool, &model, &store).await {
                tracing::error!("embedding backfill failed: {}", e);
            }
        });
    }

    let state = api::AppState {
        pool,
        config: config.clone(),
        llm: llm_client,
        embedding_model: Some(embedding_model),
        embedding_store: Some(embedding_store),
    };

    let router = api::create_router(state);

    let addr = format!("{}:{}", config.server.host, config.server.port);
    tracing::info!("starting server on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind address");

    axum::serve(listener, router)
        .await
        .expect("server error");
}

async fn backfill_embeddings(
    pool: &db::SqlitePool,
    model: &embedding::EmbeddingModel,
    store: &embedding::EmbeddingStore,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let all_movies = db::get_all_movies_for_embedding(pool).await?;
    let indexed_ids = store.get_indexed_movie_ids().await?;

    let to_index: Vec<_> = all_movies
        .into_iter()
        .filter(|m| !indexed_ids.contains(&m.id))
        .collect();

    if to_index.is_empty() {
        tracing::info!("all movies already have embeddings");
        return Ok(());
    }

    tracing::info!("backfilling embeddings for {} movies...", to_index.len());

    // Process in batches of 64
    for chunk in to_index.chunks(64) {
        let texts: Vec<String> = chunk
            .iter()
            .map(|m| {
                build_embedding_text(
                    &m.title,
                    m.overview.as_deref(),
                    m.genres.as_deref(),
                    m.keywords.as_deref(),
                    m.director.as_deref(),
                )
            })
            .collect();

        let embeddings = model.embed(texts.clone())?;

        let data: Vec<(i64, String, Vec<f32>)> = chunk
            .iter()
            .zip(texts.into_iter())
            .zip(embeddings.into_iter())
            .map(|((m, text), emb)| (m.id, text, emb))
            .collect();

        store.upsert_movies(data).await?;
        tracing::info!("indexed {} movies", chunk.len());
    }

    tracing::info!("embedding backfill complete");
    Ok(())
}

fn build_embedding_text(
    title: &str,
    overview: Option<&str>,
    genres: Option<&str>,
    keywords: Option<&str>,
    director: Option<&str>,
) -> String {
    let mut parts = vec![title.to_string()];

    if let Some(overview) = overview {
        parts.push(overview.to_string());
    }

    if let Some(genres) = genres {
        if let Ok(arr) = serde_json::from_str::<Vec<String>>(genres) {
            parts.push(arr.join(" "));
        }
    }

    if let Some(keywords) = keywords {
        if let Ok(arr) = serde_json::from_str::<Vec<String>>(keywords) {
            parts.push(arr.join(" "));
        }
    }

    if let Some(director) = director {
        parts.push(format!("导演: {}", director));
    }

    parts.join(". ")
}
