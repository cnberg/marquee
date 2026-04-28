mod api;
mod auth;
mod config;
mod db;
mod douban;
mod embedding;
mod llm;
mod qbittorrent;
mod search;
mod scanner;
mod static_files;
#[cfg(test)]
mod test_support;
mod tmdb;
mod worker;

use config::Config;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = Config::load().expect("failed to load config");
    tracing::info!("config loaded, movie_dirs={:?}", config.scan.movie_dirs);

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

    // Load translated keywords into in-memory dictionary so build_embedding_text
    // can substitute Chinese phrases. Empty on first boot — translation worker
    // populates it incrementally.
    let keyword_dict = embedding::keyword_dict::load(&pool)
        .await
        .expect("failed to load keyword dictionary");

    // Background task: index movies that don't have embeddings yet
    {
        let pool = pool.clone();
        let model = embedding_model.clone();
        let store = embedding_store.clone();
        let dict = keyword_dict.clone();
        tokio::spawn(async move {
            if let Err(e) = backfill_embeddings(&pool, &model, &store, &dict).await {
                tracing::error!("embedding backfill failed: {}", e);
            }
        });
    }

    // LLM-driven workers: keyword translation, overview translation, embedding
    // rebuild on text drift. These need the LLM client + embedding model that
    // weren't available when start_workers() ran.
    worker::scheduler::start_translation_workers(
        pool.clone(),
        llm_client.clone(),
        embedding_model.clone(),
        embedding_store.clone(),
        keyword_dict.clone(),
    );
    tracing::info!("translation + embedding-rebuild workers started");

    let state = api::AppState {
        pool,
        config: Arc::new(RwLock::new(config.clone())),
        llm: llm_client,
        embedding_model: Some(embedding_model),
        embedding_store: Some(embedding_store),
        most_related_cache: Arc::new(RwLock::new(None)),
        most_related_reasons_pending: Arc::new(tokio::sync::Mutex::new(
            std::collections::HashSet::new(),
        )),
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
    keyword_dict: &embedding::KeywordDict,
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

    // Snapshot the dict once per run so all batches see a consistent state.
    let dict_snapshot = keyword_dict.read().await.clone();

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
                    &dict_snapshot,
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

/// Build the text fed to BGE-Small-ZH for a movie.
///
/// Keywords come from TMDB in English. Since BGE is Chinese-only, we look up
/// each keyword in `keyword_dict` and use the Chinese translation; untranslated
/// keywords are **dropped** (not falling back to English) — leaving an English
/// token in an otherwise-Chinese segment pollutes the embedding more than the
/// missing signal hurts. The translation worker fills the dict over time and
/// the embedding-rebuild worker re-encodes affected movies.
pub(crate) fn build_embedding_text(
    title: &str,
    overview: Option<&str>,
    genres: Option<&str>,
    keywords: Option<&str>,
    director: Option<&str>,
    keyword_dict: &std::collections::HashMap<String, String>,
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
            let zh: Vec<&str> = arr
                .iter()
                .filter_map(|en| keyword_dict.get(en).map(String::as_str))
                .collect();
            if !zh.is_empty() {
                parts.push(zh.join(" "));
            }
        }
    }

    if let Some(director) = director {
        parts.push(format!("导演: {}", director));
    }

    parts.join(". ")
}

#[cfg(test)]
mod tests {
    use super::build_embedding_text;
    use std::collections::HashMap;

    fn dict(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn build_uses_chinese_keyword_when_translation_present() {
        let d = dict(&[("motorcycle", "摩托车"), ("road movie", "公路片")]);
        let txt = build_embedding_text(
            "摩托日记",
            Some("切·格瓦拉的南美旅程"),
            Some(r#"["剧情","冒险"]"#),
            Some(r#"["motorcycle","road movie"]"#),
            Some("沃尔特·塞勒斯"),
            &d,
        );
        assert!(txt.contains("摩托车"), "got: {}", txt);
        assert!(txt.contains("公路片"));
        assert!(!txt.contains("motorcycle"));
        assert!(!txt.contains("road movie"));
    }

    #[test]
    fn build_drops_untranslated_keywords_silently() {
        let d = dict(&[("motorcycle", "摩托车")]);
        let txt = build_embedding_text(
            "Some Movie",
            None,
            None,
            Some(r#"["motorcycle","unknown_keyword"]"#),
            None,
            &d,
        );
        assert!(txt.contains("摩托车"));
        // English fallback is forbidden — leaving "unknown_keyword" in the
        // string would pollute BGE-ZH embedding.
        assert!(!txt.contains("unknown_keyword"));
    }

    #[test]
    fn build_omits_keyword_segment_when_all_untranslated() {
        let d = dict(&[]);
        let txt = build_embedding_text(
            "T",
            None,
            None,
            Some(r#"["unknown_a","unknown_b"]"#),
            None,
            &d,
        );
        assert_eq!(txt, "T");
    }

    #[test]
    fn build_handles_no_keywords() {
        let d = dict(&[]);
        let txt = build_embedding_text("T", Some("ov"), None, None, None, &d);
        assert_eq!(txt, "T. ov");
    }
}
