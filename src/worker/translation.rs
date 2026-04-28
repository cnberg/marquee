//! Background worker that fills `keyword_translations.zh` via LLM.
//!
//! Pulls a batch of pending English keywords, asks the LLM for a JSON dict of
//! Chinese translations, persists results, and merges them into the live
//! `KeywordDict` cache so the embedding-rebuild worker sees them.

use sqlx::SqlitePool;
use std::collections::HashMap;

use crate::db::queries;
use crate::embedding::{keyword_dict, KeywordDict};
use crate::llm::LlmClient;

/// How many pending keywords to translate per worker tick.
const BATCH_SIZE: i64 = 50;

/// Run one translation cycle. Idempotent: if no rows are pending, returns
/// quickly. LLM hard errors mark the batch as `failed` so retries don't
/// hammer a broken provider.
pub async fn process_translation_batch(
    pool: &SqlitePool,
    llm: &LlmClient,
    dict: &KeywordDict,
) {
    let pending = match queries::claim_pending_keyword_translations(pool, BATCH_SIZE).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "claim_pending_keyword_translations failed");
            return;
        }
    };

    if pending.is_empty() {
        tracing::debug!("translation_worker: no pending keywords");
        return;
    }

    tracing::info!(count = pending.len(), "translating keywords batch");

    // Build prompt by JSON-encoding the input list. The LLM is told to return
    // a flat dict keyed by these exact strings.
    let keywords_json = serde_json::to_string(&pending).unwrap_or_else(|_| "[]".into());
    let user_prompt = SYSTEM_PROMPT.replace("{{keywords}}", &keywords_json);

    let llm_resp = match llm.chat(SYSTEM_PROMPT_BRIEF, &user_prompt).await {
        Ok(text) => text,
        Err(e) => {
            tracing::warn!(error = %e, "translation LLM call failed; marking batch failed");
            queries::mark_keyword_translations_failed(pool, &pending).await.ok();
            return;
        }
    };

    let translations = parse_translations(&llm_resp);

    if translations.is_empty() {
        tracing::warn!("translation parse yielded zero entries; marking batch failed");
        queries::mark_keyword_translations_failed(pool, &pending).await.ok();
        return;
    }

    let mut new_pairs: Vec<(String, String)> = Vec::new();
    for en in &pending {
        if let Some(zh) = translations.get(en) {
            let zh = zh.trim();
            if zh.is_empty() {
                continue;
            }
            if let Err(e) = queries::save_keyword_translation(pool, en, zh).await {
                tracing::warn!(error = %e, en = %en, "save_keyword_translation failed");
                continue;
            }
            new_pairs.push((en.clone(), zh.to_string()));
        }
        // Keys the LLM didn't return stay pending — picked up next tick.
    }

    keyword_dict::merge(dict, &new_pairs).await;
    tracing::info!(translated = new_pairs.len(), "translation batch committed");
}

/// Brief role hint for backends that take a short system prompt.
const SYSTEM_PROMPT_BRIEF: &str = "You translate TMDB keywords to short Chinese phrases. Output JSON only.";

/// Full instructions + placeholder for the input keyword list.
///
/// We keep this as a string constant rather than going through prompt_overrides
/// because the worker's correctness depends on a strict JSON output contract
/// that we don't want admins to silently break. The pattern can be revisited
/// if/when ops needs locale-specific tweaks.
const SYSTEM_PROMPT: &str = include_str!("../../prompts/keyword-translate.md");

/// Parse the LLM response into an `en → zh` map. Accepts:
/// - bare `{"k": "v"}` JSON
/// - Markdown-fenced `\`\`\`json ... \`\`\`` payloads
/// - leading/trailing whitespace
///
/// Skips non-string values defensively. Returns empty map on parse failure;
/// caller decides whether to mark the batch failed.
pub(crate) fn parse_translations(raw: &str) -> HashMap<String, String> {
    let json_str = strip_markdown_fence(raw.trim());
    let parsed: serde_json::Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(_) => {
            tracing::warn!(raw = %raw.chars().take(200).collect::<String>(), "translation JSON parse failed");
            return HashMap::new();
        }
    };
    let obj = match parsed.as_object() {
        Some(o) => o,
        None => {
            tracing::warn!("translation response was not a JSON object");
            return HashMap::new();
        }
    };
    obj.iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect()
}

/// Strip ``` ```json ... ``` `` fences if present.
fn strip_markdown_fence(text: &str) -> String {
    let trimmed = text.trim();
    let body = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .unwrap_or(trimmed);
    let body = body.trim();
    let body = body.strip_suffix("```").unwrap_or(body);
    // Find the outermost JSON object by bracket scanning to handle stray text.
    if let (Some(start), Some(end)) = (body.find('{'), body.rfind('}')) {
        if start <= end {
            return body[start..=end].to_string();
        }
    }
    body.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bare_json_dict() {
        let raw = r#"{"motorcycle": "摩托车", "biker": "机车骑士"}"#;
        let m = parse_translations(raw);
        assert_eq!(m.get("motorcycle").map(String::as_str), Some("摩托车"));
        assert_eq!(m.get("biker").map(String::as_str), Some("机车骑士"));
    }

    #[test]
    fn parse_strips_markdown_fence() {
        let raw = "```json\n{\"motorcycle\": \"摩托车\"}\n```";
        let m = parse_translations(raw);
        assert_eq!(m.get("motorcycle").map(String::as_str), Some("摩托车"));
    }

    #[test]
    fn parse_handles_leading_chatter() {
        // Some models prefix "Here is the translation:". Bracket scan recovers.
        let raw = "Here you go:\n{\"a\": \"甲\"}";
        let m = parse_translations(raw);
        assert_eq!(m.get("a").map(String::as_str), Some("甲"));
    }

    #[test]
    fn parse_skips_non_string_values() {
        let raw = r#"{"motorcycle": "摩托车", "broken": 42, "biker": "骑手"}"#;
        let m = parse_translations(raw);
        assert_eq!(m.len(), 2);
        assert!(!m.contains_key("broken"));
    }

    #[test]
    fn parse_empty_on_invalid_json() {
        assert!(parse_translations("not JSON").is_empty());
        assert!(parse_translations(r#"{broken: "v"}"#).is_empty());
    }

    #[test]
    fn parse_empty_on_array_root() {
        // Spec requires object root; array is a contract violation.
        assert!(parse_translations(r#"["a", "b"]"#).is_empty());
    }
}
