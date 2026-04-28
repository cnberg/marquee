//! In-memory cache of TMDB English-keyword → Chinese translations.
//!
//! BGE-Small-ZH is a Chinese-only embedding model; English keyword tokens
//! contribute weak signal when concatenated into the embedding text. The
//! daemon translates each unique keyword via LLM (worker `translation`),
//! persists the result in `keyword_translations`, and keeps a hot copy here
//! so `build_embedding_text` can substitute Chinese phrases at index time.
//!
//! The cache is loaded once at startup from rows where `zh IS NOT NULL`,
//! and the translation worker calls [`merge`] after each successful batch
//! so subsequent embedding rebuilds see the new translations.

use sqlx::SqlitePool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::db::queries;

pub type KeywordDict = Arc<RwLock<HashMap<String, String>>>;

/// Load every translated entry from `keyword_translations` into a fresh
/// dictionary. Failed/pending rows are skipped — `build_embedding_text`
/// drops untranslated keywords rather than fall back to English.
pub async fn load(pool: &SqlitePool) -> Result<KeywordDict, sqlx::Error> {
    let pairs = queries::load_all_keyword_translations(pool).await?;
    let map: HashMap<String, String> = pairs.into_iter().collect();
    tracing::info!(loaded = map.len(), "keyword_dict loaded");
    Ok(Arc::new(RwLock::new(map)))
}

/// Merge newly-translated pairs into the live cache. Used by the translation
/// worker right after committing a batch to SQLite.
pub async fn merge(dict: &KeywordDict, pairs: &[(String, String)]) {
    if pairs.is_empty() {
        return;
    }
    let mut w = dict.write().await;
    for (en, zh) in pairs {
        w.insert(en.clone(), zh.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn merge_inserts_new_pairs() {
        let dict: KeywordDict = Arc::new(RwLock::new(HashMap::new()));
        merge(&dict, &[("motorcycle".into(), "摩托车".into())]).await;
        let g = dict.read().await;
        assert_eq!(g.get("motorcycle").map(String::as_str), Some("摩托车"));
    }

    #[tokio::test]
    async fn merge_overwrites_existing() {
        let dict: KeywordDict = Arc::new(RwLock::new(HashMap::new()));
        merge(&dict, &[("biker".into(), "骑手".into())]).await;
        merge(&dict, &[("biker".into(), "机车骑士".into())]).await;
        let g = dict.read().await;
        assert_eq!(g.get("biker").map(String::as_str), Some("机车骑士"));
    }

    #[tokio::test]
    async fn merge_empty_is_noop() {
        let dict: KeywordDict = Arc::new(RwLock::new(HashMap::new()));
        merge(&dict, &[]).await;
        assert!(dict.read().await.is_empty());
    }
}
