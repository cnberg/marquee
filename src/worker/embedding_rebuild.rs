//! Background worker that re-encodes a movie's BGE embedding when its
//! source text has drifted away from what's stored in LanceDB.
//!
//! The worker samples a random batch of movies, computes the *current*
//! `build_embedding_text` output (which now factors in the keyword dictionary
//! and any LLM-translated overview), compares against the `text` payload
//! stored in LanceDB, and re-encodes any rows where they differ.
//!
//! This pattern is deliberately decoupled from the writers (translation
//! workers, TMDB fetch). They can land their changes whenever; this worker
//! reconciles eventually. Batches that hit zero drift are essentially free.

use sqlx::SqlitePool;
use std::collections::HashMap;

use crate::db::queries;
use crate::embedding::{EmbeddingModel, EmbeddingStore, KeywordDict};

/// Per-tick batch size. Each item costs one `build_embedding_text` call (a
/// HashMap lookup chain) plus one fastembed forward pass when drift is
/// detected. 64 mirrors `backfill_embeddings`.
const BATCH_SIZE: i64 = 64;

/// Run one reconciliation cycle. Returns immediately when no movies need
/// re-embedding. Errors are logged and swallowed so the worker keeps ticking.
pub async fn process_rebuild_batch(
    pool: &SqlitePool,
    model: &EmbeddingModel,
    store: &EmbeddingStore,
    dict: &KeywordDict,
) {
    let movies = match queries::sample_movies_for_embedding_check(pool, BATCH_SIZE).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "sample_movies_for_embedding_check failed");
            return;
        }
    };
    if movies.is_empty() {
        return;
    }

    // Snapshot the dict once for this batch so all entries see consistent state.
    let dict_snapshot: HashMap<String, String> = dict.read().await.clone();

    // Compute the desired embedding text for each candidate.
    let desired: Vec<(i64, String)> = movies
        .iter()
        .map(|m| {
            let text = crate::build_embedding_text(
                &m.title,
                m.overview.as_deref(),
                m.genres.as_deref(),
                m.keywords.as_deref(),
                m.director.as_deref(),
                &dict_snapshot,
            );
            (m.id, text)
        })
        .collect();

    let ids: Vec<i64> = desired.iter().map(|(id, _)| *id).collect();
    let stored = match store.get_texts_by_ids(&ids).await {
        Ok(map) => map,
        Err(e) => {
            tracing::error!(error = %e, "get_texts_by_ids failed");
            return;
        }
    };

    // Filter to rows whose desired text differs from what's stored. Rows
    // missing from `stored` (not yet indexed) are skipped — backfill_embeddings
    // owns initial population to avoid a write conflict here.
    let needs_rebuild: Vec<(i64, String)> = desired
        .into_iter()
        .filter(|(id, want)| match stored.get(id) {
            Some(have) => have != want,
            None => false,
        })
        .collect();

    if needs_rebuild.is_empty() {
        tracing::debug!(scanned = movies.len(), "embedding rebuild: no drift");
        return;
    }

    let texts: Vec<String> = needs_rebuild.iter().map(|(_, t)| t.clone()).collect();
    let embeddings = match model.embed(texts.clone()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "embedding model rebuild call failed");
            return;
        }
    };

    let data: Vec<(i64, String, Vec<f32>)> = needs_rebuild
        .iter()
        .zip(embeddings.into_iter())
        .map(|((id, text), emb)| (*id, text.clone(), emb))
        .collect();

    if let Err(e) = store.upsert_movies(data).await {
        tracing::error!(error = %e, "rebuild upsert failed");
        return;
    }

    tracing::info!(
        rebuilt = needs_rebuild.len(),
        scanned = movies.len(),
        "embedding rebuild batch committed"
    );
}
