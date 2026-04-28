//! Background worker that LLM-translates `overview_en` into `movies.overview`
//! for movies whose Chinese overview is missing or trivially short.
//!
//! Operates in batches of 10 (smaller than the keyword worker because each
//! overview carries far more text per row, and per-batch JSON output reliably
//! degrades past a few thousand characters).
//!
//! Outcomes are written directly to `movies.overview` and tracked via the
//! `overview_zh_source` column ('llm' on success, 'failed' on hard error).
//! The TMDB refetch path consults the same column to avoid clobbering a good
//! LLM translation with a shorter TMDB stub later on.

use sqlx::SqlitePool;
use std::collections::HashMap;

use crate::db::queries;
use crate::llm::LlmClient;

/// Per-tick batch size. Overview text is ~150-300 characters per row, so 10
/// inputs map to ~3K characters of LLM output — comfortable for JSON
/// reliability across both the OpenAI and claude-cli backends.
const BATCH_SIZE: i64 = 10;

/// Run one translation cycle. Returns quietly when no rows are pending. LLM
/// hard errors and parse failures mark the whole batch as `'failed'` so the
/// worker doesn't loop on poison input.
pub async fn process_overview_batch(pool: &SqlitePool, llm: &LlmClient) {
    let pending = match queries::claim_pending_overviews(pool, BATCH_SIZE).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "claim_pending_overviews failed");
            return;
        }
    };

    if pending.is_empty() {
        tracing::debug!("overview_translation_worker: no pending rows");
        return;
    }

    tracing::info!(count = pending.len(), "translating overview batch");

    let payload = build_payload(&pending);
    let user_prompt = SYSTEM_PROMPT.replace("{{movies}}", &payload);

    let resp = match llm.chat(SYSTEM_PROMPT_BRIEF, &user_prompt).await {
        Ok(text) => text,
        Err(e) => {
            tracing::warn!(error = %e, "overview LLM call failed; marking batch failed");
            let ids: Vec<i64> = pending.iter().map(|p| p.id).collect();
            queries::mark_overview_translations_failed(pool, &ids).await.ok();
            return;
        }
    };

    let translations = parse_overview_translations(&resp);
    if translations.is_empty() {
        tracing::warn!("overview translation parse yielded zero entries; marking batch failed");
        let ids: Vec<i64> = pending.iter().map(|p| p.id).collect();
        queries::mark_overview_translations_failed(pool, &ids).await.ok();
        return;
    }

    let mut saved = 0u32;
    for row in &pending {
        let key = row.id.to_string();
        if let Some(zh) = translations.get(&key) {
            let zh = zh.trim();
            if zh.is_empty() {
                continue;
            }
            if let Err(e) = queries::save_overview_translation(pool, row.id, zh).await {
                tracing::warn!(error = %e, movie_id = row.id, "save_overview_translation failed");
                continue;
            }
            saved += 1;
        }
        // Rows the LLM didn't return stay NULL — picked up next tick.
    }

    tracing::info!(saved, "overview translation batch committed");
}

const SYSTEM_PROMPT_BRIEF: &str =
    "You translate movie overviews from English to Chinese. Output JSON only.";

const SYSTEM_PROMPT: &str = include_str!("../../prompts/overview-translate.md");

/// Render the input rows as a JSON-array payload the prompt can interpolate.
fn build_payload(rows: &[queries::PendingOverview]) -> String {
    let entries: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| serde_json::json!({ "id": r.id, "en": r.overview_en }))
        .collect();
    serde_json::to_string(&entries).unwrap_or_else(|_| "[]".into())
}

/// Parse the LLM response into a `movie_id_str → zh_overview` map. Returns
/// an empty map on parse failure; caller decides whether to mark the batch
/// failed.
pub(crate) fn parse_overview_translations(raw: &str) -> HashMap<String, String> {
    let json_str = strip_markdown_fence(raw.trim());
    let parsed: serde_json::Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(_) => {
            tracing::warn!(
                raw = %raw.chars().take(200).collect::<String>(),
                "overview translation JSON parse failed"
            );
            return HashMap::new();
        }
    };
    let obj = match parsed.as_object() {
        Some(o) => o,
        None => {
            tracing::warn!("overview translation response was not a JSON object");
            return HashMap::new();
        }
    };
    obj.iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect()
}

fn strip_markdown_fence(text: &str) -> String {
    let trimmed = text.trim();
    let body = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .unwrap_or(trimmed);
    let body = body.trim();
    let body = body.strip_suffix("```").unwrap_or(body);
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
        let raw = r#"{"123": "未来世界的故事", "456": "二战时期的战争片"}"#;
        let m = parse_overview_translations(raw);
        assert_eq!(m.get("123").map(String::as_str), Some("未来世界的故事"));
        assert_eq!(m.get("456").map(String::as_str), Some("二战时期的战争片"));
    }

    #[test]
    fn parse_strips_markdown_fence() {
        let raw = "```json\n{\"123\": \"测试中文\"}\n```";
        let m = parse_overview_translations(raw);
        assert_eq!(m.get("123").map(String::as_str), Some("测试中文"));
    }

    #[test]
    fn parse_handles_leading_chatter() {
        let raw = "Here you go:\n{\"77\": \"乱世佳人\"}";
        let m = parse_overview_translations(raw);
        assert_eq!(m.get("77").map(String::as_str), Some("乱世佳人"));
    }

    #[test]
    fn parse_skips_non_string_values() {
        let raw = r#"{"1": "正常", "2": 42, "3": null, "4": "也正常"}"#;
        let m = parse_overview_translations(raw);
        assert_eq!(m.len(), 2);
        assert!(!m.contains_key("2"));
        assert!(!m.contains_key("3"));
    }

    #[test]
    fn parse_empty_on_invalid_json() {
        assert!(parse_overview_translations("not JSON").is_empty());
    }

    #[test]
    fn parse_empty_on_array_root() {
        // Spec requires object root; array is a contract violation.
        assert!(parse_overview_translations(r#"["a", "b"]"#).is_empty());
    }
}
