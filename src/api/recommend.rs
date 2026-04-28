use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
    Json, Router,
};
use futures::stream::Stream;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;

mod sse_event {
    pub const STATUS: &str = "status";
    pub const THINKING: &str = "thinking";
    pub const DONE: &str = "done";
    pub const ERROR: &str = "error";
    pub const RESULT: &str = "result";
}

use crate::api::AppState;
use crate::auth::OptionalUser;
use crate::db;
use crate::embedding::{EmbeddingModel, EmbeddingStore};
use crate::llm::LlmClient;
use crate::search::intent::{
    semantic_recall_per_intent, structured_recall_limit, system_sort_rules, validate_intent,
    ConstraintSaturation, QueryIntent, SortRule,
};
use crate::search::ranking::{coarse_rank, RankedCandidate};

#[derive(Clone)]
pub struct EventSink {
    tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
    recorder: Option<Arc<Mutex<Vec<serde_json::Value>>>>,
}

impl EventSink {
    pub fn new(tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>) -> Self {
        Self { tx, recorder: None }
    }

    pub fn with_recorder(
        tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
        recorder: Arc<Mutex<Vec<serde_json::Value>>>,
    ) -> Self {
        Self {
            tx,
            recorder: Some(recorder),
        }
    }

    pub async fn emit<T: serde::Serialize>(&self, event_name: &str, data: &T) {
        let data_value = serde_json::to_value(data).unwrap_or(serde_json::Value::Null);

        if let Some(ref rec) = self.recorder {
            let mut guard = rec.lock().await;
            guard.push(serde_json::json!({
                "event": event_name,
                "data": data_value.clone(),
            }));
        }

        let data_str = serde_json::to_string(&data_value).unwrap_or_else(|_| "{}".to_string());
        let event = Event::default().event(event_name).data(data_str);
        let _ = self.tx.try_send(Ok(event));
    }

    pub fn emit_raw(&self, event_name: &str, data: &str) {
        let event = Event::default().event(event_name).data(data);
        let _ = self.tx.try_send(Ok(event));
    }
}

async fn emit_status(sink_opt: Option<&EventSink>, stage: &str, message: &str) {
    if let Some(sink) = sink_opt {
        sink.emit(
            sse_event::STATUS,
            &StatusEvent {
                stage: stage.to_string(),
                message: message.to_string(),
            },
        )
        .await;
    }
}

async fn emit_thinking(
    sink_opt: Option<&EventSink>,
    stage: &str,
    label: &str,
    detail: serde_json::Value,
) {
    if let Some(sink) = sink_opt {
        sink.emit(
            sse_event::THINKING,
            &ThinkingEvent {
                stage: stage.to_string(),
                label: label.to_string(),
                detail,
            },
        )
        .await;
    }
}

// --- Shared types ---

#[derive(Deserialize)]
pub struct RecommendRequest {
    pub prompt: String,
}

#[derive(Deserialize)]
struct LlmRecommendations {
    recommendations: Vec<LlmRecommendation>,
}

#[derive(Deserialize)]
struct LlmRecommendation {
    tmdb_id: Option<i64>,
    #[serde(default)]
    reason_zh: Option<String>,
    #[serde(default)]
    reason_en: Option<String>,
    // Legacy single-language fallback
    #[serde(default)]
    reason: Option<String>,
}

impl LlmRecommendation {
    fn pick_reason(&self, locale: &str) -> Option<String> {
        if locale == "en" {
            self.reason_en.clone().or_else(|| self.reason.clone()).or_else(|| self.reason_zh.clone())
        } else {
            self.reason_zh.clone().or_else(|| self.reason.clone()).or_else(|| self.reason_en.clone())
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RecommendResult {
    pub recommendations: Vec<RecommendItem>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RecommendItem {
    pub movie: db::Movie,
    pub reason: Option<String>,
    #[serde(default)]
    pub in_library: bool,
    #[serde(default)]
    pub downloading: bool,
}

#[derive(Serialize)]
struct StatusEvent {
    stage: String,
    message: String,
}

#[derive(Serialize)]
struct ThinkingEvent {
    stage: String,
    label: String,
    detail: serde_json::Value,
}

// --- Prompt helpers ---

pub(super) fn render_prompt_public(template: &str, vars: &[(&str, &str)]) -> String {
    render_prompt(template, vars)
}

fn render_prompt(template: &str, vars: &[(&str, &str)]) -> String {
    let mut result = template.to_string();
    for (key, value) in vars {
        result = result.replace(&format!("{{{{{}}}}}", key), value);
    }
    result
}

pub(super) fn default_prompt(name: &str, locale: &str) -> &'static str {
    match (name, locale) {
        ("recommend-filter", "en") => include_str!("../../prompts/recommend-filter.en.md"),
        ("recommend-pick", "en") => include_str!("../../prompts/recommend-pick.en.md"),
        ("inspire", "en") => include_str!("../../prompts/inspire.en.md"),
        ("query-understand", "en") => include_str!("../../prompts/query-understand.en.md"),
        // smart-rank: single bilingual prompt, no en variant
        ("query-classify", "en") => include_str!("../../prompts/query-classify.en.md"),
        ("most-related-tip", "en") => include_str!("../../prompts/most-related-tip.en.md"),
        ("person-pick", "en") => include_str!("../../prompts/person-pick.en.md"),
        ("recommend-filter", _) => include_str!("../../prompts/recommend-filter.md"),
        ("recommend-pick", _) => include_str!("../../prompts/recommend-pick.md"),
        ("inspire", _) => include_str!("../../prompts/inspire.md"),
        ("query-understand", _) => include_str!("../../prompts/query-understand.md"),
        ("smart-rank", _) => include_str!("../../prompts/smart-rank.md"),
        ("query-classify", _) => include_str!("../../prompts/query-classify.md"),
        ("most-related-tip", _) => include_str!("../../prompts/most-related-tip.md"),
        ("movie-insight", _) => include_str!("../../prompts/movie-insight.md"),
        ("person-pick", _) => include_str!("../../prompts/person-pick.md"),
        _ => panic!("Unknown prompt: {}", name),
    }
}

pub(super) async fn load_prompt_public(pool: &db::SqlitePool, name: &str, locale: &str) -> String {
    load_prompt(pool, name, locale).await
}

async fn load_prompt(pool: &db::SqlitePool, name: &str, locale: &str) -> String {
    if let Ok(Some(content)) = db::get_prompt_override(pool, name, locale).await {
        return content;
    }
    default_prompt(name, locale).to_string()
}

pub(super) async fn get_locale(pool: &db::SqlitePool) -> String {
    db::get_setting(pool, "locale")
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "en".to_string())
}

pub(super) struct LibrarySummary {
    pub total: String,
    pub genres: String,
    pub countries: String,
    pub decades: String,
    directors: String,
    cast: String,
    keywords: String,
    ratings: String,
    budgets: String,
}

pub(super) fn build_library_summary_public(stats: &db::LibraryStats, locale: &str) -> LibrarySummary {
    build_library_summary(stats, locale)
}

fn build_library_summary(stats: &db::LibraryStats, locale: &str) -> LibrarySummary {
    // Localize the count suffix and list separator. The injected strings end
    // up inside the LLM prompt, so they must be coherent with the prompt's
    // language — an English template with "、" separators reads as noise.
    let (count_fmt, sep, decade_suffix): (fn(i64) -> String, &str, &str) = if locale == "en" {
        (
            |c| if c == 1 { "1 movie".to_string() } else { format!("{} movies", c) },
            ", ",
            "s",
        )
    } else {
        (|c| format!("{} 部", c), "、", "年代")
    };

    let fmt_entry = |label: &str, c: i64| format!("{} ({})", label, count_fmt(c));

    LibrarySummary {
        total: stats.total.to_string(),
        genres: stats
            .genres
            .iter()
            .map(|(g, c)| fmt_entry(g, *c))
            .collect::<Vec<_>>()
            .join(sep),
        countries: stats
            .countries
            .iter()
            .map(|(c, n)| fmt_entry(c, *n))
            .collect::<Vec<_>>()
            .join(sep),
        decades: stats
            .decades
            .iter()
            .map(|(d, c)| fmt_entry(&format!("{}{}", d, decade_suffix), *c))
            .collect::<Vec<_>>()
            .join(sep),
        directors: stats
            .directors
            .iter()
            .map(|(d, c)| fmt_entry(d, *c))
            .collect::<Vec<_>>()
            .join(sep),
        cast: stats
            .cast
            .iter()
            .map(|(a, c)| fmt_entry(a, *c))
            .collect::<Vec<_>>()
            .join(sep),
        keywords: stats
            .keywords
            .iter()
            .map(|(k, c)| fmt_entry(k, *c))
            .collect::<Vec<_>>()
            .join(sep),
        ratings: stats
            .rating_tiers
            .iter()
            .map(|(t, c)| fmt_entry(t, *c))
            .collect::<Vec<_>>()
            .join(sep),
        budgets: stats
            .budget_tiers
            .iter()
            .map(|(t, c)| fmt_entry(t, *c))
            .collect::<Vec<_>>()
            .join(sep),
    }
}

// --- Core reusable logic ---

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IdeaItem {
    pub display_zh: String,
    pub display_en: String,
    pub query: String,
}

/// Generate inspiration ideas from LLM
async fn generate_ideas(llm: &LlmClient, pool: &db::SqlitePool) -> Result<Vec<IdeaItem>, String> {
    let stats = db::get_library_stats(pool).await.map_err(|e| e.to_string())?;
    let locale = get_locale(pool).await;
    let summary = build_library_summary(&stats, &locale);
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M (%A)").to_string();

    // 把分层采样的库内代表性电影注入 prompt，给 LLM 一个 ground truth——
    // 库小时 LLM 不再凭聚合分布幻想出"90 年代北欧科幻"这种库里没有的 query。
    // 空库时给明确提示，inspire 端点仍可生成通用灵感（用户点进搜索会兜底）。
    let movie_samples = if stats.sample_movies.is_empty() {
        if locale == "en" { "(library is empty)".to_string() } else { "（影片库为空）".to_string() }
    } else {
        stats.sample_movies.join("\n")
    };

    let template = load_prompt(pool, "inspire", &locale).await;
    let system_prompt = render_prompt(&template, &[
        ("total", &summary.total),
        ("movie_samples", &movie_samples),
        ("genres", &summary.genres),
        ("countries", &summary.countries),
        ("decades", &summary.decades),
        ("directors", &summary.directors),
        ("cast", &summary.cast),
        ("keywords", &summary.keywords),
        ("ratings", &summary.ratings),
        ("budgets", &summary.budgets),
        ("now", &now),
    ]);

    let user_msg = if locale == "en" {
        "Give me some movie inspiration"
    } else {
        "给我一些观影灵感"
    };
    let llm_response = llm.chat(&system_prompt, user_msg).await
        .map_err(|e| format!("error_llm_call: {}", e))?;

    let json_str = extract_json(&llm_response);
    serde_json::from_str(&json_str).map_err(|e| {
        tracing::warn!("Failed to parse inspire response: {}. Raw: {}", e, llm_response);
        "LLM 返回格式异常".to_string()
    })
}

/// Benchmark 等复用路径用来回捞中间产物。普通 `recommend` 调用传 `None`。
#[derive(Default, Debug)]
pub(crate) struct SearchCapture {
    /// query-understand 阶段解析后的 intent，序列化为 JSON 字符串。
    pub intent_json: Option<String>,
    /// Stage 0 分类结果，序列化为 JSON 字符串。
    pub classification_json: Option<String>,
}

/// Stage 0 分类 + 按 kind 路由到专用 handler。未命中或 handler 降级时回到
/// `run_descriptive_pipeline`（原四阶段管线）。详见 docs/specs/query-router.md。
pub(crate) async fn run_smart_search(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    embedding_model: &EmbeddingModel,
    embedding_store: &EmbeddingStore,
    prompt: &str,
    max_results: usize,
    sink_opt: Option<&EventSink>,
    user_marks: &[db::UserMarkedMovie],
    mut capture: Option<&mut SearchCapture>,
) -> Result<Vec<RecommendItem>, String> {
    let locale = get_locale(pool).await;

    // ========== Stage 0: Classification ==========
    emit_status(sink_opt, "classifying", "正在判断查询类型…").await;
    let classification = crate::search::classify_query(llm, pool, prompt, &locale).await;
    tracing::info!(
        "query classified as {} (confidence {:.2}): {:?}",
        classification.kind.as_str(),
        classification.confidence,
        classification.reasoning
    );
    emit_thinking(
        sink_opt,
        "classification",
        "查询分类结果",
        serde_json::to_value(&classification).unwrap_or(serde_json::json!({})),
    )
    .await;
    if let Some(cap) = capture.as_deref_mut() {
        cap.classification_json = serde_json::to_string(&classification).ok();
    }

    // ========== Route ==========
    match classification.kind {
        crate::search::QueryKind::ExactTitle => {
            if let Some(subject) = classification.subject.as_ref() {
                match handle_exact_title(pool, &subject.name, max_results, sink_opt).await {
                    Ok(items) if !items.is_empty() => return Ok(items),
                    Ok(_) => {
                        tracing::info!(
                            "exact_title handler miss for '{}', fall through to descriptive",
                            subject.name
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "exact_title handler error for '{}': {}; fall through",
                            subject.name, e
                        );
                    }
                }
            }
        }
        crate::search::QueryKind::SimilarTo => {
            if let Some(subject) = classification.subject.as_ref() {
                match handle_similar_to(
                    llm, pool, subject, prompt, max_results, sink_opt, &locale,
                )
                .await
                {
                    Ok(items) if !items.is_empty() => return Ok(items),
                    Ok(_) => {
                        tracing::info!(
                            "similar_to handler empty for '{}' (kind={:?}), fall through to descriptive",
                            subject.name, subject.kind
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "similar_to handler error for '{}' (kind={:?}): {}; fall through",
                            subject.name, subject.kind, e
                        );
                    }
                }
            }
        }
        crate::search::QueryKind::Person => {
            if let Some(subject) = classification.subject.as_ref() {
                match handle_person(llm, pool, &subject.name, prompt, max_results, sink_opt, &locale).await {
                    Ok(items) if !items.is_empty() => return Ok(items),
                    Ok(_) => {
                        tracing::info!(
                            "person handler empty for '{}', fall through to descriptive",
                            subject.name
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "person handler error for '{}': {}; fall through",
                            subject.name, e
                        );
                    }
                }
            }
        }
        crate::search::QueryKind::Attribute => {
            match handle_attribute(
                llm,
                pool,
                prompt,
                max_results,
                sink_opt,
                &locale,
                user_marks,
                capture.as_deref_mut(),
            )
            .await
            {
                Ok(items) if !items.is_empty() => return Ok(items),
                Ok(_) => {
                    tracing::info!("attribute handler empty, fall through to descriptive");
                }
                Err(e) => {
                    tracing::warn!("attribute handler error: {}; fall through", e);
                }
            }
        }
        _ => {}
    }
    // descriptive 走原四阶段管线（router 未命中也落到这里）

    run_descriptive_pipeline(
        llm,
        pool,
        embedding_model,
        embedding_store,
        prompt,
        max_results,
        sink_opt,
        user_marks,
        capture,
    )
    .await
}

/// Stage 1 (query-understand)：descriptive 和 attribute handler 共用。
/// 调 LLM 解析 intent，做校验，发 thinking 事件，写 capture。
async fn do_query_understand(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    prompt: &str,
    locale: &str,
    user_marks: &[db::UserMarkedMovie],
    sink_opt: Option<&EventSink>,
    mut capture: Option<&mut SearchCapture>,
) -> Result<QueryIntent, String> {
    emit_status(sink_opt, "understanding", "正在理解你的查询…").await;

    let stats = db::get_library_stats(pool).await.map_err(|e| e.to_string())?;
    let summary = build_library_summary(&stats, locale);
    let template = load_prompt(pool, "query-understand", locale).await;

    // Build user history section for prompt
    let interested: Vec<&db::UserMarkedMovie> = user_marks
        .iter()
        .filter(|m| m.mark_type == "want" || m.mark_type == "favorite")
        .collect();
    let watched: Vec<&db::UserMarkedMovie> = user_marks
        .iter()
        .filter(|m| m.mark_type == "watched")
        .collect();
    let interested_count = interested.len();
    let watched_count = watched.len();

    let format_movie = |m: &db::UserMarkedMovie| -> String {
        format!(
            "- {} ({}) | {} | {}",
            m.title,
            m.year.map(|y| y.to_string()).unwrap_or_else(|| "?".into()),
            m.genres.as_deref().unwrap_or(""),
            m.director.as_deref().unwrap_or(""),
        )
    };

    let user_history = if user_marks.is_empty() {
        String::new()
    } else {
        let (header, interested_label, watched_label, footer) = if locale == "en" {
            (
                "\n## Current user's viewing history\n\n",
                "Films the user is interested in (want-to-watch / favorite):\n",
                "Films the user has watched:\n",
                "Use this history as soft context when analyzing the query. The user's current query intent remains the most important signal.\n",
            )
        } else {
            (
                "\n## 当前用户的观影历史\n\n",
                "感兴趣的电影（想看/收藏）：\n",
                "看过的电影：\n",
                "请在分析查询时参考用户的历史偏好。注意：用户的当前查询意图仍然是最重要的。\n",
            )
        };

        let mut section = String::from(header);

        if !interested.is_empty() {
            section.push_str(interested_label);
            for m in interested.iter().take(50) {
                section.push_str(&format_movie(m));
                section.push('\n');
            }
            section.push('\n');
        }

        if !watched.is_empty() {
            section.push_str(watched_label);
            for m in watched.iter().take(50) {
                section.push_str(&format_movie(m));
                section.push('\n');
            }
            section.push('\n');
        }

        section.push_str(footer);
        section
    };
    let system_prompt = render_prompt(&template, &[
        ("total", &summary.total),
        ("genres", &summary.genres),
        ("countries", &summary.countries),
        ("decades", &summary.decades),
        ("directors", &summary.directors),
        ("cast", &summary.cast),
        ("keywords", &summary.keywords),
        ("ratings", &summary.ratings),
        ("budgets", &summary.budgets),
        ("user_history", &user_history),
    ]);

    let llm_response = llm
        .chat(&system_prompt, prompt)
        .await
        .map_err(|e| format!("Query 理解失败: {}", e))?;

    let json_str = extract_json(&llm_response);
    let mut intent: QueryIntent = match serde_json::from_str(&json_str) {
        Ok(i) => i,
        Err(e) => {
            tracing::warn!("Failed to parse QueryIntent: {}. Raw: {}", e, llm_response);
            write_parse_error_log("query_understand_error", &llm_response, &e.to_string());
            QueryIntent {
                constraints: Default::default(),
                exclusions: Default::default(),
                preferences: Default::default(),
                search_intents: vec![prompt.to_string()],
                sort_rules: vec![SortRule {
                    field: "relevance".to_string(),
                    weight: 1.0,
                    order: "desc".to_string(),
                }],
                query_type: "semantic".to_string(),
                watched_policy: "neutral".to_string(),
            }
        }
    };

    let available_genres: Vec<String> = stats.genres.iter().map(|(g, _)| g.clone()).collect();
    validate_intent(&mut intent, prompt, &available_genres);

    if let Some(cap) = capture.as_deref_mut() {
        cap.intent_json = serde_json::to_string(&intent).ok();
    }

    tracing::info!(
        "QueryIntent parsed: query_type={}, search_intents={:?}, constraints_genres={:?}",
        intent.query_type,
        intent.search_intents,
        intent.constraints.genres
    );

    emit_thinking(sink_opt, "understanding", "查询理解结果", serde_json::json!({
        "query_type": intent.query_type,
        "search_intents": intent.search_intents,
        "constraints": {
            "genres": intent.constraints.genres,
            "countries": intent.constraints.countries,
            "languages": intent.constraints.languages,
            "decades": intent.constraints.decades,
            "directors": intent.constraints.directors,
            "cast": intent.constraints.cast,
            "keywords": intent.constraints.keywords,
            "min_rating": intent.constraints.min_rating,
            "max_rating": intent.constraints.max_rating,
            "year_range": { "min": intent.constraints.year_range.min, "max": intent.constraints.year_range.max },
            "runtime_range": { "min": intent.constraints.runtime_range.min, "max": intent.constraints.runtime_range.max },
            "budget_tier": intent.constraints.budget_tier,
            "popularity_tier": intent.constraints.popularity_tier,
        },
        "watched_policy": intent.watched_policy,
        "user_marks": {
            "interested_count": interested_count,
            "watched_count": watched_count,
        },
        "preferences": {
            "decades": intent.preferences.decades,
            "genres": intent.preferences.genres,
            "countries": intent.preferences.countries,
            "languages": intent.preferences.languages,
            "directors": intent.preferences.directors,
            "keywords": intent.preferences.keywords,
            "budget_tier": intent.preferences.budget_tier,
            "popularity_tier": intent.preferences.popularity_tier,
        },
        "exclusions": {
            "genres": intent.exclusions.genres,
            "keywords": intent.exclusions.keywords,
        },
        "sort_rules": intent.sort_rules.iter().map(|r| serde_json::json!({
            "field": r.field, "weight": r.weight, "order": r.order,
        })).collect::<Vec<_>>(),
    })).await;

    Ok(intent)
}

/// Decide whether the user's raw prompt should join the LLM-generated
/// `search_intents` as a 4th embedding query in stage 2 recall.
///
/// Skipped on `Strong` saturation because `passes_constraints` (in coarse
/// rank) would reject anything the prompt-similarity surfaces that doesn't
/// already match the cast/director/keyword filters — paying for the embed +
/// lancedb call buys nothing. Also skipped for blank prompts (defensive;
/// the calling site shouldn't hand us an empty string but guard anyway).
fn should_embed_user_prompt(saturation: ConstraintSaturation, prompt: &str) -> bool {
    saturation != ConstraintSaturation::Strong && !prompt.trim().is_empty()
}

/// 把 LanceDB 的距离转成绝对的 cosine 相似度（[0, 1]）。
///
/// 关键事实链（每个都已验源码）：
/// 1. fastembed BGE Small ZH 输出**单位归一化**向量
///    （`fastembed-5.13.3/src/common.rs::normalize` 显式 `v / ||v||`）
/// 2. LanceDB 默认 L2 metric 返回的是**平方 L2**，不是线性 L2
///    （`lance-linalg-4.0.0/src/distance/l2.rs:482` 注释原文：
///    "Note that we skip the final square root step for performance reasons."）
/// 3. 对单位向量：squared L2 = ||a−b||² = 2(1−cos)
/// 4. 所以 **cos = 1 − distance/2**（distance 已经是平方过的）
///
/// 替代了原来的 per-batch 归一化（`1 − distance/max_distance`）——后者把每批的
/// 最差距离强行钉到 0.0、最好钉到 1.0，丢失了绝对距离信息：批次全是烂候选时，
/// 最强烂候选仍以 1.0 进 top 50。
///
/// 实测验证：share `TEbG_qgV44FM` 摩托日记 squared L2=0.8143 →
/// cos = 1−0.8143/2 ≈ 0.593；老 per-batch 公式在 max≈1.0 时把它压到 0.19，
/// 最终分 ~0.51，挤不进 top 50；改完之后分 ~0.70，进 top 5。
fn semantic_similarity_from_l2(distance: f32) -> f64 {
    let cos = 1.0 - (distance as f64) / 2.0;
    cos.clamp(0.0, 1.0)
}

/// Floor for semantic similarity——低于这个的候选直接丢弃，不送进 candidate_map。
///
/// 0.3 对应 squared L2 距离 1.4。BGE Small ZH 实测中文 query 跟完全不相关
/// 文档的 cos 一般 0.3~0.5（squared L2 1.0~1.4），所以这个 floor 偏宽松，
/// 只切"几乎正交"的最弱候选。如果发现误切真候选，下调到 0.2；如果发现
/// 假候选漏过太多，上调到 0.4。需要部署后实测校准。
const SEMANTIC_SIMILARITY_FLOOR: f64 = 0.3;

/// 四阶段管线本身（原 `run_smart_search` 的主体）。Stage 0 路由未命中时由
/// `run_smart_search` 调用到这里。
async fn run_descriptive_pipeline(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    embedding_model: &EmbeddingModel,
    embedding_store: &EmbeddingStore,
    prompt: &str,
    max_results: usize,
    sink_opt: Option<&EventSink>,
    user_marks: &[db::UserMarkedMovie],
    capture: Option<&mut SearchCapture>,
) -> Result<Vec<RecommendItem>, String> {
    let locale = get_locale(pool).await;

    // ========== Stage 1: Query Understanding ==========
    let mut intent = do_query_understand(llm, pool, prompt, &locale, user_marks, sink_opt, capture).await?;

    // System overrides LLM's sort_rules. The LLM at understand-stage hedges
    // with patterns like rating 0.5 / relevance 0.5 — fine for vibes queries
    // but disastrous when hard constraints (a TMDB keyword like "based on
    // novel or book", a cast member, a director) already locked candidates
    // to a precise on-target subset. In those cases the relevance dimension
    // becomes a filter on the LLM's *imagined* sub-theme rather than the
    // user's intent — pushing real on-target films out of top 50.
    //
    // See `ConstraintSaturation` doc + share TZySV4da0_F9 case in backlog.
    let saturation = intent.constraints.saturation();
    let llm_sort_rules = intent.sort_rules.clone();
    let new_sort_rules = system_sort_rules(saturation);
    tracing::info!(
        saturation = ?saturation,
        llm_gave = ?llm_sort_rules,
        system_uses = ?new_sort_rules,
        "sort_rules: system override applied"
    );
    intent.sort_rules = new_sort_rules;

    // ========== Stage 2: Multi-path Recall ==========
    emit_status(sink_opt, "recall", "正在多路召回候选…").await;

    // Structured recall — pool size shrinks when constraints are empty. With
    // no SQL filter the call degenerates to "library Bayesian-top N"——pure
    // noise relative to a descriptive query. See `structured_recall_limit`
    // doc + share YGWuFuCjCFkX motorcycle case.
    let structured_limit = structured_recall_limit(saturation);
    let structured_future =
        db::structured_recall(pool, &intent.constraints, &intent.exclusions, structured_limit);

    // Semantic recall — top-K per intent shrinks with saturation. The LLM's
    // imagined search_intents are most useful when there's no hard signal
    // to lean on; once constraints lock the pool down, the same imagination
    // is more likely to drag in sub-theme bias than to add useful candidates.
    //
    // When saturation < Strong we also embed the user's raw prompt as a 4th
    // intent. Rationale: query-understand sometimes generates flowery intents
    // that drift from the literal keyword in the prompt (e.g. "跟摩托车相关
    // 的电影" → "机车骑士追逐自由的旅程"); the prompt itself is a cleaner
    // direct signal. Skip on saturation=Strong because passes_constraints
    // would reject the extra hits anyway (cast/director/keyword filters).
    let semantic_per_intent = semantic_recall_per_intent(saturation);
    let mut intents_for_embedding: Vec<String> = intent.search_intents.clone();
    if should_embed_user_prompt(saturation, prompt) {
        intents_for_embedding.push(prompt.to_string());
    }
    let semantic_results = {
        let mut all_hits: Vec<(i64, f32)> = Vec::new();
        for search_intent in &intents_for_embedding {
            match embedding_model.embed_one(search_intent) {
                Ok(query_vec) => match embedding_store.search(&query_vec, semantic_per_intent).await {
                    Ok(hits) => all_hits.extend(hits),
                    Err(e) => tracing::warn!("semantic search failed: {}", e),
                },
                Err(e) => tracing::warn!("embedding failed for intent: {}", e),
            }
        }
        all_hits
    };

    let structured = structured_future.await.map_err(|e| e.to_string())?;

    let structured_ids: Vec<i64> = structured.iter().map(|m| m.id).collect();
    let structured_membership = db::library_membership_for_movie_ids(pool, &structured_ids)
        .await
        .unwrap_or_default();

    let mut candidate_map: HashMap<i64, RankedCandidate> = HashMap::new();

    for m in &structured {
        let in_library = structured_membership.get(&m.id).copied().unwrap_or(false);
        candidate_map.insert(
            m.id,
            RankedCandidate {
                movie_id: m.id,
                tmdb_id: m.tmdb_id,
                title: m.title.clone(),
                year: m.year,
                genres: m.genres.clone(),
                director: m.director.clone(),
                language: m.language.clone(),
                country: m.country.clone(),
                overview: m.overview.clone(),
                tmdb_rating: m.tmdb_rating,
                tmdb_votes: m.tmdb_votes,
                runtime: m.runtime,
                popularity: m.popularity,
                budget: m.budget,
                keywords: m.keywords.clone(),
                cast: m.cast_json.clone(),
                source: "structured".to_string(),
                in_library,
                // 直接命中过滤条件 → baseline 0.5；间接关联（collab）0.3；
                // 无召回 0。原相对框架见 238d537 引入的实施指南。
                semantic_score: 0.5,
            },
        );
    }

    // Diagnostic: surface per-movie semantic distances so we can verify
    // whether expected films (e.g. share `m2YECoRR_9fq` 摩托日记) actually
    // made it into the embedding-side recall pool. Cap at 100 closest hits
    // to keep the SSE payload tractable. Sorted by distance ascending so
    // the strongest matches come first.
    {
        let mut min_dist: HashMap<i64, f32> = HashMap::new();
        for (id, d) in &semantic_results {
            let entry = min_dist.entry(*id).or_insert(f32::MAX);
            if *d < *entry {
                *entry = *d;
            }
        }
        let mut top: Vec<(i64, f32)> = min_dist.into_iter().collect();
        top.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        top.truncate(100);
        emit_thinking(
            sink_opt,
            "recall",
            "语义召回 top 100 距离",
            serde_json::json!({
                "semantic_top": top.iter().map(|(id, d)| serde_json::json!({
                    "movie_id": id,
                    "distance": format!("{:.4}", d),
                })).collect::<Vec<_>>(),
            }),
        )
        .await;
    }

    for (movie_id, distance) in &semantic_results {
        let similarity = semantic_similarity_from_l2(*distance);
        if similarity < SEMANTIC_SIMILARITY_FLOOR {
            continue;
        }

        if let Some(existing) = candidate_map.get_mut(movie_id) {
            // Only upgrade structured → both. If existing source is already
            // "semantic" (same movie appeared in a previous intent's hits),
            // leave it as "semantic"——the old code unconditionally set
            // "both", which inflated the source_breakdown.both count to
            // include "movie hit by ≥2 intents" rather than "in both
            // structured AND semantic" (its label-implied meaning).
            if existing.source == "structured" {
                existing.source = "both".to_string();
            }
            existing.semantic_score = existing.semantic_score.max(similarity);
        } else {
            candidate_map.insert(
                *movie_id,
                RankedCandidate {
                    movie_id: *movie_id,
                    tmdb_id: 0,
                    title: String::new(),
                    year: None,
                    genres: None,
                    director: None,
                    language: None,
                    country: None,
                    overview: None,
                    tmdb_rating: None,
                    tmdb_votes: None,
                    runtime: None,
                    popularity: None,
                    budget: None,
                    keywords: None,
                    cast: None,
                    source: "semantic".to_string(),
                    in_library: true,
                    semantic_score: similarity,
                },
            );
        }
    }

    let seed_ids: Vec<i64> = structured
        .iter()
        .filter(|m| structured_membership.get(&m.id).copied().unwrap_or(false))
        .map(|m| m.id)
        .collect();
    let collaborative_related =
        db::get_related_movies_for_seeds(pool, &seed_ids).await.map_err(|e| e.to_string())?;
    let collaborative_count = collaborative_related.len();
    for m in collaborative_related {
        candidate_map.entry(m.id).or_insert_with(|| RankedCandidate {
            movie_id: m.id,
            tmdb_id: m.tmdb_id,
            title: m.title.clone(),
            year: m.year,
            genres: m.genres.clone(),
            director: m.director.clone(),
            language: m.language.clone(),
            country: m.country.clone(),
            overview: m.overview.clone(),
            tmdb_rating: m.tmdb_rating,
            tmdb_votes: m.tmdb_votes,
            runtime: m.runtime,
            popularity: m.popularity,
            budget: m.budget,
            keywords: m.keywords.clone(),
            cast: m.cast_json.clone(),
            source: "collaborative".to_string(),
            in_library: false,
            semantic_score: 0.3,
        });
    }

    let need_detail: Vec<i64> = candidate_map
        .iter()
        .filter(|(_, c)| c.tmdb_id == 0)
        .map(|(id, _)| *id)
        .collect();

    if !need_detail.is_empty() {
        for movie_id in &need_detail {
            if let Ok(Some(movie)) = db::get_movie_by_id(pool, *movie_id).await {
                if let Some(c) = candidate_map.get_mut(movie_id) {
                    c.tmdb_id = movie.tmdb_id;
                    c.title = movie.title;
                    c.year = movie.year;
                    c.genres = movie.genres;
                    c.director = movie.director;
                    c.language = movie.language;
                    c.country = movie.country;
                    c.overview = movie.overview;
                    c.tmdb_rating = movie.tmdb_rating;
                    c.tmdb_votes = movie.tmdb_votes;
                    c.runtime = movie.runtime;
                    c.popularity = movie.popularity;
                    c.budget = movie.budget;
                    c.keywords = movie.keywords;
                    c.cast = movie.cast;
                    // in_library 在下面统一从 dir_movie_mappings ground truth 设置
                }
            } else {
                candidate_map.remove(movie_id);
            }
        }
    }

    candidate_map.retain(|_, c| c.tmdb_id != 0);

    // 统一刷写 in_library：覆盖 structured/semantic-only/collaborative 三路的占位值，
    // 全部以 dir_movie_mappings 为真。movies.source 不可信（first-touch 标记，
    // 绑定后不刷新——见 docs/tech.md "movies.source 字段语义"）
    let final_ids: Vec<i64> = candidate_map.values().map(|c| c.movie_id).collect();
    let final_membership = db::library_membership_for_movie_ids(pool, &final_ids)
        .await
        .unwrap_or_default();
    for c in candidate_map.values_mut() {
        c.in_library = final_membership.get(&c.movie_id).copied().unwrap_or(false);
    }

    let mut candidates: Vec<RankedCandidate> = candidate_map.into_values().collect();

    tracing::info!(
        "recall complete: {} candidates (structured={}, semantic hits={}, collaborative={})",
        candidates.len(),
        structured.len(),
        semantic_results.len(),
        collaborative_count
    );

    // Thinking: Stage 2 — recall statistics
    emit_thinking(sink_opt, "recall", "多路召回统计", serde_json::json!({
        "structured_count": structured.len(),
        "semantic_count": semantic_results.len(),
        "collaborative_count": collaborative_count,
        "merged_total": candidates.len(),
        "source_breakdown": {
            "structured_only": candidates.iter().filter(|c| c.source == "structured").count(),
            "semantic_only": candidates.iter().filter(|c| c.source == "semantic").count(),
            "both": candidates.iter().filter(|c| c.source == "both").count(),
            "collaborative": candidates.iter().filter(|c| c.source == "collaborative").count(),
        }
    })).await;

    if candidates.is_empty() {
        return Err("error_no_matching_movies".to_string());
    }

    // ========== Stage 3: Coarse Ranking ==========
    emit_status(sink_opt, "ranking", "正在筛选排序…").await;
    coarse_rank(&mut candidates, &intent, 50, user_marks);
    tracing::info!("coarse ranking complete: {} candidates remain", candidates.len());

    // Thinking: Stage 3 — coarse ranking top candidates
    {
        let top_preview: Vec<serde_json::Value> = candidates.iter().take(15).map(|c| {
            // cast 列是对象数组 [{"name":"...","tmdb_person_id":...,...},...]
            // 解析后只取前 5 个 name，避免把完整对象塞进思考面板
            let cast_names: Option<Vec<String>> = c.cast.as_deref()
                .and_then(|s| serde_json::from_str::<Vec<serde_json::Value>>(s).ok())
                .map(|vals| {
                    vals.iter().take(5)
                        .filter_map(|v| v.get("name").and_then(|n| n.as_str()).map(String::from))
                        .collect()
                });
            serde_json::json!({
                "title": c.title,
                "year": c.year,
                "genres": c.genres,
                "director": c.director,
                "cast": cast_names,
                "rating": c.tmdb_rating,
                "votes": c.tmdb_votes,
                "source": c.source,
                "semantic_score": format!("{:.2}", c.semantic_score),
            })
        }).collect();
        emit_thinking(sink_opt, "ranking", "粗排后候选列表", serde_json::json!({
            "candidates_remaining": candidates.len(),
            "top_candidates": top_preview,
        })).await;
    }

    // ========== Stage 4: LLM Fine Ranking ==========
    emit_status(sink_opt, "selecting", "正在精选最终结果…").await;

    smart_rank_candidates(llm, pool, &locale, &candidates, prompt, max_results, sink_opt).await
}

/// 复用 smart-rank 精排：调用 LLM 从候选池里选 top-N 并给出理由。
/// 被 run_descriptive_pipeline Stage 4 和 handle_similar_to 共用。
async fn smart_rank_candidates(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    locale: &str,
    candidates: &[RankedCandidate],
    user_query: &str,
    max_results: usize,
    sink_opt: Option<&EventSink>,
) -> Result<Vec<RecommendItem>, String> {
    let in_library_map: HashMap<i64, bool> = candidates
        .iter()
        .map(|c| (c.tmdb_id, c.in_library))
        .collect();

    let candidates_text: Vec<String> = candidates
        .iter()
        .map(|c| {
            let source_tag = if c.in_library { "[库内]" } else { "[库外]" };
            format!(
                "- [tmdb_id={}] {} ({}) | {} | genres: {} | director: {} | language: {} | rating: {} | overview: {}",
                c.tmdb_id,
                c.title,
                c.year.map(|y| y.to_string()).unwrap_or_else(|| "unknown".into()),
                source_tag,
                c.genres.as_deref().unwrap_or("unknown"),
                c.director.as_deref().unwrap_or("unknown"),
                c.language.as_deref().unwrap_or("unknown"),
                c.tmdb_rating
                    .map(|r| format!("{:.1}", r))
                    .unwrap_or_else(|| "N/A".into()),
                c.overview
                    .as_deref()
                    .unwrap_or("")
                    .chars()
                    .take(100)
                    .collect::<String>(),
            )
        })
        .collect();

    let template = load_prompt(pool, "smart-rank", locale).await;
    let candidate_count = candidates.len().to_string();
    let candidates_joined = candidates_text.join("\n");
    let system_prompt = render_prompt(&template, &[
        ("candidate_count", &candidate_count),
        ("candidates", &candidates_joined),
        ("user_query", user_query),
    ]);

    // Thinking: candidates sent to LLM for fine ranking
    {
        let preview: Vec<serde_json::Value> = candidates.iter().take(15).map(|c| {
            serde_json::json!({
                "title": c.title,
                "year": c.year,
                "genres": c.genres,
                "director": c.director,
                "rating": c.tmdb_rating,
                "votes": c.tmdb_votes,
                "source": c.source,
                "in_library": c.in_library,
            })
        }).collect();
        emit_thinking(sink_opt, "selecting", "送入 LLM 精排的候选", serde_json::json!({
            "candidate_count": candidates.len(),
            "top_candidates": preview,
        })).await;
    }

    let llm_response = llm
        .chat(&system_prompt, user_query)
        .await
        .map_err(|e| format!("LLM 精排失败: {}", e))?;

    let json_str = extract_json(&llm_response);
    let llm_recs: Vec<LlmRecommendation> =
        match serde_json::from_str::<LlmRecommendations>(&json_str) {
            Ok(r) => r.recommendations,
            Err(_) => match parse_recommendations_lenient(&json_str) {
                Some(recs) => recs,
                None => {
                    write_parse_error_log("smart_rank_error", &llm_response, "parse failed");
                    return Err("LLM 精排结果格式异常".to_string());
                }
            },
        };

    // Thinking: LLM fine ranking result.
    // 双语字段都摊出来，admin 面板能完整看到 LLM 给的中英文理由
    // （之前只读 r.reason，bilingual prompt 下永远 null）。
    emit_thinking(sink_opt, "selecting", "LLM 精排结果", serde_json::json!({
        "picked_count": llm_recs.len(),
        "picks": llm_recs.iter().map(|r| serde_json::json!({
            "tmdb_id": r.tmdb_id,
            "reason_zh": r.reason_zh,
            "reason_en": r.reason_en,
            "reason": r.reason,
        })).collect::<Vec<_>>(),
    })).await;

    let mut result_items = Vec::new();
    let mut seen_ids = HashSet::new();
    for rec in &llm_recs {
        if result_items.len() >= max_results {
            break;
        }
        let tmdb_id = match rec.tmdb_id {
            Some(id) => id,
            None => continue,
        };
        if !seen_ids.insert(tmdb_id) {
            continue;
        }
        if let Ok(Some(movie)) = db::get_movie_by_tmdb_id(pool, tmdb_id).await {
            result_items.push(RecommendItem {
                movie,
                reason: rec.pick_reason(locale),
                in_library: *in_library_map.get(&tmdb_id).unwrap_or(&true),
                downloading: false,
            });
        }
    }

    Ok(result_items)
}

// ===== Handler: exact_title =====
//
// 用户输入的是一部电影名（如 "海底总动员"、"教父"）。
// 流程：库内 fuzzy 标题匹配 → 命中的电影放在结果前排 → 用第一个命中片作为
// 种子，从 `related_movies` 表（TMDB similar / recommendations 缓存）拉
// 相关电影填到 `max_results` 部。不经过 LLM 精排。
// 详见 docs/specs/query-router.md § "Handler: exact_title"。
async fn handle_exact_title(
    pool: &db::SqlitePool,
    reference: &str,
    max_results: usize,
    sink_opt: Option<&EventSink>,
) -> Result<Vec<RecommendItem>, String> {
    emit_status(sink_opt, "matching", "在库内匹配标题…").await;

    // 1) 库内 fuzzy 匹配：最多取 5 部，用于"教父系列"这类多部同名命中场景
    let hits = db::search_movies_by_title_fuzzy(pool, reference, 5)
        .await
        .map_err(|e| e.to_string())?;

    emit_thinking(
        sink_opt,
        "matching",
        "标题匹配结果",
        serde_json::json!({
            "reference": reference,
            "hit_count": hits.len(),
            "hits": hits.iter().map(|m| serde_json::json!({
                "tmdb_id": m.tmdb_id, "title": m.title, "year": m.year,
            })).collect::<Vec<_>>(),
        }),
    )
    .await;

    // 2) 未命中 → 向调用方报告空，由 router 降级到 descriptive 管线
    if hits.is_empty() {
        return Ok(Vec::new());
    }

    let hit_tmdb_ids: std::collections::HashSet<i64> =
        hits.iter().map(|m| m.tmdb_id).collect();

    let hit_ids: Vec<i64> = hits.iter().map(|m| m.id).collect();
    let hit_membership = db::library_membership_for_movie_ids(pool, &hit_ids)
        .await
        .unwrap_or_default();

    // 3) 命中片直接进结果前排，不走 LLM
    let mut items: Vec<RecommendItem> = hits
        .iter()
        .map(|m| RecommendItem {
            movie: m.clone(),
            reason: None,
            in_library: hit_membership.get(&m.id).copied().unwrap_or(false),
            downloading: false,
        })
        .collect();

    // 4) 以第一个命中片为种子，拉相关电影填满剩余位置
    let remaining = max_results.saturating_sub(items.len());
    if remaining > 0 {
        emit_status(sink_opt, "expanding", "正在为你找相似电影…").await;
        // 取 max_results 倍数，反正 Rust 侧过滤后截断
        let seed = &hits[0];
        let related = db::get_related_movies_all_sources(pool, &[seed.id], (remaining * 3).max(20))
            .await
            .map_err(|e| e.to_string())?;

        let related_ids: Vec<i64> = related.iter().map(|m| m.id).collect();
        let related_membership = db::library_membership_for_movie_ids(pool, &related_ids)
            .await
            .unwrap_or_default();

        emit_thinking(
            sink_opt,
            "expanding",
            "相关电影扩展",
            serde_json::json!({
                "seed_title": seed.title,
                "seed_tmdb_id": seed.tmdb_id,
                "related_count": related.len(),
                "in_library": related.iter().filter(|m| related_membership.get(&m.id).copied().unwrap_or(false)).count(),
            }),
        )
        .await;

        for m in related.into_iter() {
            if items.len() >= max_results {
                break;
            }
            if hit_tmdb_ids.contains(&m.tmdb_id) {
                continue;
            }
            let in_library = related_membership.get(&m.id).copied().unwrap_or(false);
            items.push(RecommendItem {
                movie: m,
                reason: None,
                in_library,
                downloading: false,
            });
        }
    }

    Ok(items)
}

// ===== Handler: similar_to =====
//
// 用户想找类似某部电影的片子（"类似海底总动员的电影"）。
// 流程：库内 fuzzy 匹配 seed → 拉 seed 的 related_movies（TMDB similar/recommendations）
// 作为候选池 → 走 smart-rank 让 LLM 按用户的原始表述挑 top-10。
// 和 exact_title 的区别：种子片本身**不**返回；走 LLM 精排而不是直接取 related。
// 详见 docs/specs/query-router.md § "Handler: similar_to"。
async fn handle_similar_to(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    subject: &crate::search::Subject,
    prompt: &str,
    max_results: usize,
    sink_opt: Option<&EventSink>,
    locale: &str,
) -> Result<Vec<RecommendItem>, String> {
    use crate::search::SubjectKind;
    match subject.kind {
        SubjectKind::Movie => {
            handle_similar_to_movie(llm, pool, &subject.name, prompt, max_results, sink_opt, locale)
                .await
        }
        SubjectKind::Person => {
            handle_similar_to_person(
                llm,
                pool,
                &subject.name,
                prompt,
                max_results,
                sink_opt,
                locale,
            )
            .await
        }
        SubjectKind::Movement | SubjectKind::Studio | SubjectKind::Franchise => {
            // Schema 接受这些 subject kind，但本次未实现专用候选池构造。
            // 立刻返回 empty 让 router 降级 descriptive，让兜底管线尝试。
            emit_thinking(
                sink_opt,
                "matching",
                "subject 类型暂未实现",
                serde_json::json!({
                    "subject_kind": format!("{:?}", subject.kind),
                    "subject_name": subject.name,
                    "note": "降级到 descriptive 管线",
                }),
            )
            .await;
            Ok(Vec::new())
        }
    }
}

async fn handle_similar_to_movie(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    reference: &str,
    prompt: &str,
    max_results: usize,
    sink_opt: Option<&EventSink>,
    locale: &str,
) -> Result<Vec<RecommendItem>, String> {
    emit_status(sink_opt, "matching", "在库内查找你提到的电影…").await;

    // 1) fuzzy 匹配 seed 电影
    let hits = db::search_movies_by_title_fuzzy(pool, reference, 3)
        .await
        .map_err(|e| e.to_string())?;

    emit_thinking(
        sink_opt,
        "matching",
        "种子电影匹配结果",
        serde_json::json!({
            "reference": reference,
            "hit_count": hits.len(),
            "top_hit": hits.first().map(|m| serde_json::json!({
                "tmdb_id": m.tmdb_id, "title": m.title, "year": m.year,
            })),
        }),
    )
    .await;

    if hits.is_empty() {
        return Ok(Vec::new()); // router 降级 descriptive
    }

    let seed = &hits[0];
    let seed_tmdb_id = seed.tmdb_id;

    // 2) 取 seed 的相关电影（库内 + 库外）。给 LLM 足够的候选池，超过 max 也没关系
    emit_status(sink_opt, "expanding", "找相关电影候选…").await;
    let related = db::get_related_movies_all_sources(pool, &[seed.id], 60)
        .await
        .map_err(|e| e.to_string())?;

    // 候选池过小不值得走 LLM，降级
    if related.len() < 3 {
        tracing::info!(
            "similar_to: only {} related movies for seed '{}', downgrade",
            related.len(),
            seed.title
        );
        return Ok(Vec::new());
    }

    let movie_ids: Vec<i64> = related.iter().map(|m| m.id).collect();
    let in_library_map = db::library_membership_for_movie_ids(pool, &movie_ids)
        .await
        .unwrap_or_default();

    emit_thinking(
        sink_opt,
        "expanding",
        "相关电影候选统计",
        serde_json::json!({
            "seed_title": seed.title,
            "seed_tmdb_id": seed_tmdb_id,
            "candidate_count": related.len(),
            "in_library": related.iter().filter(|m| in_library_map.get(&m.id).copied().unwrap_or(false)).count(),
        }),
    )
    .await;

    // 3) 构造 RankedCandidate，过滤掉 seed 本身
    let candidates: Vec<RankedCandidate> = related
        .into_iter()
        .filter(|m| m.tmdb_id != seed_tmdb_id)
        .map(|m| {
            let in_library = in_library_map.get(&m.id).copied().unwrap_or(false);
            RankedCandidate {
                movie_id: m.id,
                tmdb_id: m.tmdb_id,
                title: m.title,
                year: m.year,
                genres: m.genres,
                director: m.director,
                language: m.language,
                country: m.country,
                overview: m.overview,
                tmdb_rating: m.tmdb_rating,
                tmdb_votes: m.tmdb_votes,
                runtime: m.runtime,
                popularity: m.popularity,
                budget: m.budget,
                keywords: m.keywords,
                cast: m.cast,
                source: "related".to_string(),
                in_library,
                semantic_score: 0.5,
            }
        })
        .collect();

    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    // 4) 走 smart-rank 精排。把种子信息塞进 user_query 前缀，让 LLM 知道"类似什么"
    emit_status(sink_opt, "selecting", "精选最终结果…").await;
    let framed_query = format!(
        "用户想找类似《{}》（{}年）的电影。用户的原始查询是：「{}」。请从候选池里挑最贴合这种调性的电影，不要重复推荐这部种子片本身。",
        seed.title,
        seed.year.map(|y| y.to_string()).unwrap_or_else(|| "年份未知".into()),
        prompt,
    );

    smart_rank_candidates(llm, pool, locale, &candidates, &framed_query, max_results, sink_opt).await
}

/// 以人为种子的 similar_to——找该人风格相近的**其他**电影（排除此人自己作品）。
/// 触发场景：用户提到导演/演员 + 表达"已看过/要其他相关风格"扩散意图。
/// 流程：人名 fuzzy → 拉此人所有库内作品 → 用作品当 seeds 拉 related 候选池 →
///       从候选池过滤掉此人所有作品 → smart-rank 精排
async fn handle_similar_to_person(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    reference_person: &str,
    prompt: &str,
    max_results: usize,
    sink_opt: Option<&EventSink>,
    locale: &str,
) -> Result<Vec<RecommendItem>, String> {
    emit_status(sink_opt, "matching", "在库内查找这位导演或演员…").await;

    // 1) fuzzy 人名匹配
    let persons = db::search_persons_by_name_fuzzy(pool, reference_person, 3)
        .await
        .map_err(|e| e.to_string())?;

    emit_thinking(
        sink_opt,
        "matching",
        "种子人物匹配结果",
        serde_json::json!({
            "reference_person": reference_person,
            "hit_count": persons.len(),
            "top_matches": persons.iter().take(3).map(|p| serde_json::json!({
                "tmdb_person_id": p.tmdb_person_id,
                "name": p.name,
                "movie_count": p.movie_count,
            })).collect::<Vec<_>>(),
        }),
    )
    .await;

    if persons.is_empty() {
        return Ok(Vec::new()); // router 降级 descriptive
    }
    let person = &persons[0];

    // 2) 拉此人在库内全部作品，多取一些保证种子充分
    emit_status(sink_opt, "expanding", "收集此人作品当种子…").await;
    let works = db::get_movies_by_person(pool, person.tmdb_person_id, 60)
        .await
        .map_err(|e| e.to_string())?;

    if works.len() < 2 {
        tracing::info!(
            "similar_to_person: '{}' (tmdb_person_id={}) has only {} in-library works, downgrade",
            reference_person, person.tmdb_person_id, works.len()
        );
        return Ok(Vec::new());
    }

    let work_ids: Vec<i64> = works.iter().map(|w| w.movie.id).collect();
    let work_id_set: std::collections::HashSet<i64> = work_ids.iter().copied().collect();

    // 3) 用所有作品当 seeds 拉相关电影（库内 + related）
    let related = db::get_related_movies_all_sources(pool, &work_ids, 80)
        .await
        .map_err(|e| e.to_string())?;

    // 4) 关键：从候选池过滤掉此人自己的所有作品
    let filtered: Vec<_> = related
        .into_iter()
        .filter(|m| !work_id_set.contains(&m.id))
        .collect();

    let movie_ids: Vec<i64> = filtered.iter().map(|m| m.id).collect();
    let in_library_map = db::library_membership_for_movie_ids(pool, &movie_ids)
        .await
        .unwrap_or_default();

    emit_thinking(
        sink_opt,
        "expanding",
        "排除此人作品后的候选池",
        serde_json::json!({
            "person_name": person.name,
            "tmdb_person_id": person.tmdb_person_id,
            "seed_works_count": works.len(),
            "candidate_count": filtered.len(),
            "in_library": filtered.iter().filter(|m| in_library_map.get(&m.id).copied().unwrap_or(false)).count(),
        }),
    )
    .await;

    if filtered.len() < 3 {
        tracing::info!(
            "similar_to_person: only {} candidates after excluding {} own works for '{}', downgrade",
            filtered.len(), works.len(), person.name
        );
        return Ok(Vec::new());
    }

    // 5) 构造 RankedCandidate
    let candidates: Vec<RankedCandidate> = filtered
        .into_iter()
        .map(|m| {
            let in_library = in_library_map.get(&m.id).copied().unwrap_or(false);
            RankedCandidate {
                movie_id: m.id,
                tmdb_id: m.tmdb_id,
                title: m.title,
                year: m.year,
                genres: m.genres,
                director: m.director,
                language: m.language,
                country: m.country,
                overview: m.overview,
                tmdb_rating: m.tmdb_rating,
                tmdb_votes: m.tmdb_votes,
                runtime: m.runtime,
                popularity: m.popularity,
                budget: m.budget,
                keywords: m.keywords,
                cast: m.cast,
                source: "related".to_string(),
                in_library,
                semantic_score: 0.5,
            }
        })
        .collect();

    // 6) 走 smart-rank，framed_query 提示 LLM 用户已看过此人作品
    emit_status(sink_opt, "selecting", "精选最终结果…").await;
    let framed_query = format!(
        "用户喜欢「{}」的风格，但已经看过此人的电影，要风格相近的**其他**电影。\
         用户的原始查询是：「{}」。请从候选池里挑风格、调性、主题最贴近{}\
         的电影，**不要**推荐{}本人的任何作品（候选池已排除）。",
        person.name, prompt, person.name, person.name,
    );

    smart_rank_candidates(llm, pool, locale, &candidates, &framed_query, max_results, sink_opt).await
}

// ===== Handler: person =====
//
// 用户想看某个导演 / 演员的作品（"诺兰的电影" / "基努里维斯演的动作片"）。
// 流程：从 movie_credits 做人名 fuzzy 匹配 → 取最佳匹配 person → 拉其库内作品
// （按"身份重要度"分级排序，uncredited/Stunt Double 过滤）→ 按 role_kind 拼推荐语 →
// 截取前 max_results 部。不走 LLM——作品列表已经排好，推荐语由模板生成。
// 详见 docs/specs/query-router.md § "Handler: person"。
async fn handle_person(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    reference_person: &str,
    user_prompt: &str,
    max_results: usize,
    sink_opt: Option<&EventSink>,
    locale: &str,
) -> Result<Vec<RecommendItem>, String> {
    emit_status(sink_opt, "matching", "在库内查找这位导演或演员…").await;

    // 1) fuzzy 人名匹配
    let persons = db::search_persons_by_name_fuzzy(pool, reference_person, 3)
        .await
        .map_err(|e| e.to_string())?;

    emit_thinking(
        sink_opt,
        "matching",
        "人名匹配结果",
        serde_json::json!({
            "reference_person": reference_person,
            "hit_count": persons.len(),
            "top_matches": persons.iter().take(3).map(|p| serde_json::json!({
                "tmdb_person_id": p.tmdb_person_id,
                "name": p.name,
                "credit_count": p.credit_count,
                "movie_count": p.movie_count,
                "has_director_credit": p.has_director_credit,
            })).collect::<Vec<_>>(),
        }),
    )
    .await;

    if persons.is_empty() {
        return Ok(Vec::new()); // router 降级 descriptive
    }

    let person = &persons[0];

    // 2) 拉该人的库内作品，按身份重要度分级排序
    emit_status(sink_opt, "expanding", "正在收集作品…").await;
    // 多取一点，防止某些作品数据不完整被过滤后凑不够 max_results
    let works = db::get_movies_by_person(pool, person.tmdb_person_id, max_results * 2)
        .await
        .map_err(|e| e.to_string())?;

    if works.is_empty() {
        tracing::info!(
            "person handler: '{}' matched tmdb_person_id={} but no in-library works",
            reference_person, person.tmdb_person_id
        );
        return Ok(Vec::new());
    }

    emit_thinking(
        sink_opt,
        "expanding",
        "作品列表",
        serde_json::json!({
            "person_name": person.name,
            "tmdb_person_id": person.tmdb_person_id,
            "works_in_library": works.len(),
            "role_breakdown": {
                "director": works.iter().filter(|w| w.role_kind == db::PersonRoleKind::Director).count(),
                "lead_actor": works.iter().filter(|w| w.role_kind == db::PersonRoleKind::LeadActor).count(),
                "supporting_actor": works.iter().filter(|w| w.role_kind == db::PersonRoleKind::SupportingActor).count(),
                "voice": works.iter().filter(|w| w.role_kind == db::PersonRoleKind::Voice).count(),
                "crew": works.iter().filter(|w| w.role_kind == db::PersonRoleKind::Crew).count(),
            }
        }),
    )
    .await;

    // 3) 把全部候选 + 用户原 prompt 一起喂给 person-pick LLM，让它按用户意图
    //    挑出 max_results 部并写差异化 reason。这样"早期" / "喜剧" 等修饰词
    //    会被 LLM 真正用来过滤，而不是被代码层 take(N) 忽略掉。
    //
    //    LLM 失败 vs LLM 明确返空是**两种不同的语义**，分别处理：
    //    - LLM 失败（chat 出错 / JSON 解析挂）→ 我们不知道用户意图能否满足，
    //      给前 max_results 部代表作（按身份重要度，reason=None）当兜底
    //    - LLM 成功但返空数组 → LLM 看了候选已经判断"没有符合用户意图的"，
    //      handle_person 返空，让 router 的 fall-through 接管降级到 descriptive
    //      路径。**不要**在这里塞代表作——会让用户搜"成龙科幻片"看到一堆动作
    //      片但没任何提示。
    let chosen: Vec<(db::PersonWork, Option<String>)> = match generate_person_picks_with_reasons(
        llm, pool, locale, &person.name, user_prompt, &works, max_results,
    )
    .await
    {
        Err(e) => {
            tracing::warn!(person = person.name.as_str(), error = e.as_str(),
                "person-pick LLM failed; falling back to top-N by role importance");
            works
                .into_iter()
                .take(max_results)
                .map(|w| (w, None))
                .collect()
        }
        Ok(picks) if picks.is_empty() => {
            tracing::info!(
                person = person.name.as_str(),
                user_prompt = user_prompt,
                "person-pick LLM returned no matches; deferring to descriptive fallback"
            );
            Vec::new()
        }
        Ok(picks) => {
            let mut work_by_tmdb: HashMap<i64, db::PersonWork> = works
                .into_iter()
                .map(|w| (w.movie.tmdb_id, w))
                .collect();
            picks
                .into_iter()
                .filter_map(|(tmdb_id, reason)| {
                    work_by_tmdb.remove(&tmdb_id).map(|w| (w, Some(reason)))
                })
                .collect()
        }
    };

    let movie_ids: Vec<i64> = chosen.iter().map(|(w, _)| w.movie.id).collect();
    let in_library_map = db::library_membership_for_movie_ids(pool, &movie_ids)
        .await
        .unwrap_or_default();

    let items: Vec<RecommendItem> = chosen
        .into_iter()
        .map(|(w, reason)| {
            let in_library = in_library_map.get(&w.movie.id).copied().unwrap_or(false);
            RecommendItem {
                movie: w.movie,
                reason,
                in_library,
                downloading: false,
            }
        })
        .collect();

    Ok(items)
}

#[derive(Debug, Deserialize)]
struct PersonPickReason {
    tmdb_id: i64,
    #[serde(default)]
    reason_zh: Option<String>,
    #[serde(default)]
    reason_en: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PersonPickResponse {
    reasons: Vec<PersonPickReason>,
}

/// Pick the locale-appropriate reason from a parsed PersonPickReason, falling
/// back to the other language if the preferred one is missing/blank.
fn pick_locale_reason(r: &PersonPickReason, locale: &str) -> Option<String> {
    let prefer_en = locale == "en";
    let (primary, secondary) = if prefer_en {
        (r.reason_en.as_deref(), r.reason_zh.as_deref())
    } else {
        (r.reason_zh.as_deref(), r.reason_en.as_deref())
    };
    primary
        .filter(|s| !s.trim().is_empty())
        .or(secondary.filter(|s| !s.trim().is_empty()))
        .map(|s| s.to_string())
}

/// Parse the LLM person-pick response into a Vec<(tmdb_id, reason)>, preserving
/// LLM order (which encodes its picking ranking). Returns an empty Vec on parse
/// failure or when no entry has a usable reason — caller treats that as "LLM
/// returned nothing usable" and falls back to top-N by role importance.
fn parse_person_pick_picks(json_str: &str, locale: &str) -> Vec<(i64, String)> {
    let Ok(parsed) = serde_json::from_str::<PersonPickResponse>(json_str) else {
        return Vec::new();
    };
    parsed
        .reasons
        .iter()
        .filter_map(|r| pick_locale_reason(r, locale).map(|s| (r.tmdb_id, s)))
        .collect()
}

async fn generate_person_picks_with_reasons(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    locale: &str,
    person_name: &str,
    user_prompt: &str,
    works: &[db::PersonWork],
    max_results: usize,
) -> Result<Vec<(i64, String)>, String> {
    if works.is_empty() {
        return Ok(Vec::new());
    }

    // Each line is one candidate — title + year + role + genres + rating + brief overview.
    // Overview truncated to 60 chars to keep prompt compact (the prompt body is the
    // bulk of the LLM call's token cost; we don't need full plot).
    let movies_list: String = works
        .iter()
        .map(|w| {
            let role_label = match w.role_kind {
                db::PersonRoleKind::Director => "导演".to_string(),
                db::PersonRoleKind::LeadActor => match w.role_detail.as_deref().filter(|s| !s.trim().is_empty()) {
                    Some(c) => format!("主演（饰 {}）", c.trim_end_matches(" (voice)").trim()),
                    None => "主演".to_string(),
                },
                db::PersonRoleKind::SupportingActor => match w.role_detail.as_deref().filter(|s| !s.trim().is_empty()) {
                    Some(c) => format!("配角（饰 {}）", c.trim_end_matches(" (voice)").trim()),
                    None => "配角".to_string(),
                },
                db::PersonRoleKind::Voice => match w.role_detail.as_deref().filter(|s| !s.trim().is_empty()) {
                    Some(c) => format!("配音（{}）", c.trim_end_matches(" (voice)").trim()),
                    None => "配音".to_string(),
                },
                db::PersonRoleKind::Crew => match w.role_detail.as_deref().filter(|s| !s.trim().is_empty()) {
                    Some(d) => format!("剧组（{}）", d),
                    None => "剧组".to_string(),
                },
            };
            format!(
                "- [tmdb_id={}] {} ({}) | 角色: {} | genres: {} | rating: {} | overview: {}",
                w.movie.tmdb_id,
                w.movie.title,
                w.movie.year.map(|y| y.to_string()).unwrap_or_else(|| "?".into()),
                role_label,
                w.movie.genres.as_deref().unwrap_or("[]"),
                w.movie.tmdb_rating.map(|r| format!("{:.1}", r)).unwrap_or_else(|| "N/A".into()),
                w.movie.overview.as_deref().map(|s| s.chars().take(60).collect::<String>()).unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let template = load_prompt(pool, "person-pick", locale).await;
    let candidate_count = works.len().to_string();
    let n = max_results.to_string();
    let system_prompt = render_prompt(&template, &[
        ("person_name", person_name),
        ("user_prompt", user_prompt),
        ("candidate_count", &candidate_count),
        ("n", &n),
        ("movies_list", &movies_list),
    ]);

    let llm_response = llm
        .chat(&system_prompt, user_prompt)
        .await
        .map_err(|e| format!("person-pick chat failed: {}", e))?;

    let json_str = extract_json(&llm_response);
    Ok(parse_person_pick_picks(&json_str, locale))
}

// ===== Handler: attribute =====
//
// 用户用明确的结构化属性（年代/类型/评分/国家…）描述想看什么，没提到具体
// 电影名或人物（"2020 年后的高分悬疑片"）。
// 和 descriptive 的区别：只做 structured_recall，不做 semantic / collaborative，
// 也不走 coarse_rank。这样结果是"真正满足所有硬约束"的纯净候选池，不会被
// embedding 噪音稀释。最后还是走 smart-rank 让 LLM 挑选 top-N 并给理由。
// 详见 docs/specs/query-router.md § "Handler: attribute"。
#[allow(clippy::too_many_arguments)]
async fn handle_attribute(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    prompt: &str,
    max_results: usize,
    sink_opt: Option<&EventSink>,
    locale: &str,
    user_marks: &[db::UserMarkedMovie],
    capture: Option<&mut SearchCapture>,
) -> Result<Vec<RecommendItem>, String> {
    // 1) query-understand 拿约束
    let intent = do_query_understand(llm, pool, prompt, locale, user_marks, sink_opt, capture).await?;

    // 2) 只做 structured_recall
    emit_status(sink_opt, "recall", "按约束筛选…").await;
    let structured = db::structured_recall(pool, &intent.constraints, &intent.exclusions, 200)
        .await
        .map_err(|e| e.to_string())?;

    let movie_ids: Vec<i64> = structured.iter().map(|m| m.id).collect();
    let in_library_map = db::library_membership_for_movie_ids(pool, &movie_ids)
        .await
        .unwrap_or_default();

    emit_thinking(
        sink_opt,
        "recall",
        "结构化筛选结果",
        serde_json::json!({
            "total": structured.len(),
            "in_library": structured.iter().filter(|m| in_library_map.get(&m.id).copied().unwrap_or(false)).count(),
        }),
    )
    .await;

    // 候选池为空 → 用户的硬约束太窄（可能没库存），降级 descriptive 用语义兜底
    if structured.is_empty() {
        tracing::info!("attribute handler: structured_recall empty, fall through");
        return Ok(Vec::new());
    }

    // 3) 构造 RankedCandidate，按 rating desc, popularity desc 排序，截前 50 给 LLM
    let mut candidates: Vec<RankedCandidate> = structured
        .into_iter()
        .map(|m| {
            let in_library = in_library_map.get(&m.id).copied().unwrap_or(false);
            RankedCandidate {
                movie_id: m.id,
                tmdb_id: m.tmdb_id,
                title: m.title,
                year: m.year,
                genres: m.genres,
                director: m.director,
                language: m.language,
                country: m.country,
                overview: m.overview,
                tmdb_rating: m.tmdb_rating,
                tmdb_votes: m.tmdb_votes,
                runtime: m.runtime,
                popularity: m.popularity,
                budget: m.budget,
                keywords: m.keywords,
                cast: m.cast_json,
                source: "structured".to_string(),
                in_library,
                semantic_score: 0.0,
            }
        })
        .collect();

    // SQL 已按 Bayesian 加权评分排序，这里直接截前 50。
    // 不再按裸 tmdb_rating 二次排序——会把 1-票 10.0 的噪音重新顶到前面。
    candidates.truncate(50);

    emit_thinking(
        sink_opt,
        "ranking",
        "按评分/热度排序后的 top",
        serde_json::json!({
            "candidates_remaining": candidates.len(),
            "top_candidates": candidates.iter().take(15).map(|c| serde_json::json!({
                "title": c.title,
                "year": c.year,
                "genres": c.genres,
                "rating": c.tmdb_rating,
                "votes": c.tmdb_votes,
                "popularity": c.popularity,
            })).collect::<Vec<_>>(),
        }),
    )
    .await;

    // 4) smart-rank 精排
    emit_status(sink_opt, "selecting", "精选最终结果…").await;
    smart_rank_candidates(llm, pool, locale, &candidates, prompt, max_results, sink_opt).await
}

// --- Lenient parsing helpers ---

fn parse_recommendations_lenient(json_str: &str) -> Option<Vec<LlmRecommendation>> {
    // 1. Strict parse via Value tree.
    if let Some(recs) = try_parse_value_tree(json_str) {
        return Some(recs);
    }

    // 2. Recover from common LLM truncation: outer `}` (and sometimes `]`)
    //    missing. claude-cli has been observed to ship `exit 0` with
    //    `{"recommendations": [...]` (no outer `}`) — see commit 86651a9
    //    follow-up case in 2026-04-26 21:34 logs. Try appending closures.
    let trimmed = json_str.trim();
    for suffix in ["}", "]}", "\"}]}"] {
        let patched = format!("{}{}", trimmed, suffix);
        if let Some(recs) = try_parse_value_tree(&patched) {
            return Some(recs);
        }
    }

    // 3. Last resort: scan individual `{...}` items inside an
    //    `"recommendations": [` array. This recovers from mid-record
    //    truncation as long as at least one full record landed on the wire.
    //    Iterate balanced braces inside the array (handles strings + escapes).
    if let Some(items) = scan_recommendation_items(json_str) {
        if !items.is_empty() {
            return Some(items);
        }
    }

    // 4. Repair unescaped inner double quotes in string values, then retry.
    //    Real case (2026-04-28 06:31): LLM emitted ASCII `"` around a quoted
    //    word inside `reason_zh` ("...真正以"外卖"为主题..."), which closes the
    //    JSON string prematurely. We can't ask the LLM to be perfect, so
    //    walk the bytes and escape any `"` that's clearly inside a string
    //    body (i.e. not followed by `,]}:`-style structural punctuation).
    let repaired = repair_unescaped_inner_quotes(json_str);
    if repaired != json_str {
        if let Some(recs) = try_parse_value_tree(&repaired) {
            return Some(recs);
        }
        if let Some(items) = scan_recommendation_items(&repaired) {
            if !items.is_empty() {
                return Some(items);
            }
        }
    }

    // 5. Legacy regex for the historic single-`reason` shape (old prompt
    //    versions). Kept for backwards compat with archived runs.
    let mut results = Vec::new();
    for cap in Regex::new(
        r#"\{\s*"tmdb_id"\s*:\s*(\d+)\s*,\s*"reason"\s*:\s*"([^"]*(?:\\.[^"]*)*)"\s*\}"#,
    )
    .ok()?
    .captures_iter(json_str)
    {
        if let Ok(id) = cap[1].parse::<i64>() {
            let reason = cap[2].replace("\\\"", "\"").replace("\\n", "\n");
            results.push(LlmRecommendation {
                tmdb_id: Some(id),
                reason_zh: Some(reason.clone()),
                reason_en: None,
                reason: Some(reason),
            });
        }
    }
    if results.is_empty() {
        None
    } else {
        Some(results)
    }
}

fn try_parse_value_tree(json_str: &str) -> Option<Vec<LlmRecommendation>> {
    let val = serde_json::from_str::<serde_json::Value>(json_str).ok()?;
    let arr = val.get("recommendations").and_then(|v| v.as_array())?;
    let mut results = Vec::new();
    for item in arr {
        let tmdb_id = item.get("tmdb_id").and_then(|v| v.as_i64());
        if tmdb_id.is_none() {
            continue;
        }
        let reason_zh = item.get("reason_zh").and_then(|v| v.as_str()).map(|s| s.to_string());
        let reason_en = item.get("reason_en").and_then(|v| v.as_str()).map(|s| s.to_string());
        let reason = item.get("reason").and_then(|v| v.as_str()).map(|s| s.to_string());
        results.push(LlmRecommendation { tmdb_id, reason_zh, reason_en, reason });
    }
    if results.is_empty() {
        None
    } else {
        Some(results)
    }
}

/// Walk through the input looking for `{...}` blocks inside the
/// `"recommendations": [...]` array, balance-matching `{` `}` while
/// respecting string literals + escapes. Each balanced object is fed back
/// through `serde_json` for field extraction. Survives both
/// `{"recommendations": [{...}]` (missing outer `}`) and
/// `{"recommendations": [{...}, {...}` (missing both `]` and `}`).
fn scan_recommendation_items(json_str: &str) -> Option<Vec<LlmRecommendation>> {
    let array_start = json_str.find("\"recommendations\"")?;
    let after_key = &json_str[array_start..];
    let bracket_off = after_key.find('[')?;
    let body = &after_key[bracket_off + 1..];

    let bytes = body.as_bytes();
    let mut results = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        // Skip whitespace + commas between items.
        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b',') {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] == b']' {
            break;
        }
        if bytes[i] != b'{' {
            // Unexpected token, abandon — caller falls through to legacy regex.
            break;
        }
        // Walk until matching `}`, tracking string state.
        let start = i;
        let mut depth = 0i32;
        let mut in_str = false;
        let mut escape = false;
        while i < bytes.len() {
            let c = bytes[i];
            if in_str {
                if escape {
                    escape = false;
                } else if c == b'\\' {
                    escape = true;
                } else if c == b'"' {
                    in_str = false;
                }
            } else {
                match c {
                    b'"' => in_str = true,
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            i += 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        if depth != 0 {
            // Object incomplete (truncation hit mid-record). Give up.
            break;
        }
        // body[start..i] is a complete `{...}` literal.
        let item_str = &body[start..i];
        if let Ok(item) = serde_json::from_str::<serde_json::Value>(item_str) {
            let tmdb_id = item.get("tmdb_id").and_then(|v| v.as_i64());
            if tmdb_id.is_some() {
                let reason_zh = item.get("reason_zh").and_then(|v| v.as_str()).map(String::from);
                let reason_en = item.get("reason_en").and_then(|v| v.as_str()).map(String::from);
                let reason = item.get("reason").and_then(|v| v.as_str()).map(String::from);
                results.push(LlmRecommendation { tmdb_id, reason_zh, reason_en, reason });
            }
        }
    }
    if results.is_empty() {
        None
    } else {
        Some(results)
    }
}

/// Walk JSON byte-by-byte and escape ASCII `"` chars that are clearly inside
/// a string body (i.e. NOT followed by structural punctuation `,]}:`).
///
/// LLMs occasionally emit `reason_zh` like `"真正以"外卖"为主题..."` — the
/// inner ASCII quotes close the JSON string prematurely. Heuristic: a real
/// closing `"` is always followed (after optional whitespace) by `,` `}` `]`
/// or `:` (the last for keys). Any other follower means we're still inside.
///
/// UTF-8 safe: only operates on single ASCII bytes (`"` `\` and structural
/// punctuation). Multi-byte Chinese characters never have a 0x22 byte.
fn repair_unescaped_inner_quotes(json_str: &str) -> String {
    let bytes = json_str.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len() + 16);
    let mut i = 0;
    let mut in_string = false;
    while i < bytes.len() {
        let c = bytes[i];
        if !in_string {
            out.push(c);
            if c == b'"' {
                in_string = true;
            }
            i += 1;
            continue;
        }
        // Inside a string. Handle escape sequences atomically.
        if c == b'\\' {
            out.push(c);
            i += 1;
            if i < bytes.len() {
                out.push(bytes[i]);
                i += 1;
            }
            continue;
        }
        if c == b'"' {
            // Decide: real close, or stray inner quote?
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            let next = if j < bytes.len() { bytes[j] } else { b',' };
            if matches!(next, b',' | b'}' | b']' | b':') {
                // Real string close.
                out.push(c);
                in_string = false;
                i += 1;
            } else {
                // Stray inner `"` — escape it.
                out.push(b'\\');
                out.push(b'"');
                i += 1;
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    // Bytes are valid UTF-8 by construction (we only inserted ASCII `\` `"`).
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(&e.into_bytes()).into_owned())
}

fn write_parse_error_log(tag: &str, raw: &str, err: &str) {
    let dir = std::path::Path::new(crate::llm::client::LLM_LOGS_DIR);
    std::fs::create_dir_all(dir).ok();
    let ts = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let path = dir.join(format!("{}_{}.log", ts, tag));
    let content = format!(
        "=== PARSE ERROR ===\nError: {}\n\n=== RAW LLM CONTENT ===\n{}\n",
        err, raw
    );
    std::fs::write(&path, &content).ok();
}

// --- Daily picks ---
//
// Wire / API shape (returned to frontend, also used internally during
// generation): DailyPicksData → DailyPickSection → RecommendItem (full movie).
//
// On-disk shape (cached in `daily_picks` table): DailyPicksStored stores only
// `tmdb_id` + `reason` per item. The handler hydrates by JOINing movies on
// read so that any change to a movie row (poster_url repick, new metadata
// fields, etc.) is reflected immediately without invalidating the cache.
// See docs/specs/2026-04-26-sidecar-evidence-design.md for the surrounding
// poster-selection refactor that motivated this.

#[derive(Serialize, Deserialize, Clone)]
pub struct DailyPicksData {
    pub sections: Vec<DailyPickSection>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DailyPickSection {
    pub inspiration_zh: String,
    pub inspiration_en: String,
    pub movies: Vec<RecommendItem>,
}

#[derive(Serialize, Deserialize, Clone)]
struct DailyPicksStored {
    sections: Vec<DailyPickSectionStored>,
}

#[derive(Serialize, Deserialize, Clone)]
struct DailyPickSectionStored {
    // alias 容忍旧缓存（双语化前的单语 `inspiration` 字段）
    #[serde(default, alias = "inspiration")]
    inspiration_zh: String,
    #[serde(default)]
    inspiration_en: String,
    items: Vec<DailyPickItemStored>,
}

#[derive(Serialize, Deserialize, Clone)]
struct DailyPickItemStored {
    tmdb_id: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl From<&DailyPicksData> for DailyPicksStored {
    fn from(data: &DailyPicksData) -> Self {
        DailyPicksStored {
            sections: data
                .sections
                .iter()
                .map(|s| DailyPickSectionStored {
                    inspiration_zh: s.inspiration_zh.clone(),
                    inspiration_en: s.inspiration_en.clone(),
                    items: s
                        .movies
                        .iter()
                        .map(|m| DailyPickItemStored {
                            tmdb_id: m.movie.tmdb_id,
                            reason: m.reason.clone(),
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

/// Hydrate the compact on-disk form into the full DailyPicksData by joining
/// movies on tmdb_id. Items whose movies have since been deleted are dropped
/// silently. `in_library` and `downloading` are recomputed from the live
/// movies table — never frozen.
async fn hydrate_daily_picks(
    stored: DailyPicksStored,
    pool: &db::SqlitePool,
) -> DailyPicksData {
    // Collect all tmdb_ids across sections, then batch-fetch.
    let tmdb_ids: Vec<i64> = stored
        .sections
        .iter()
        .flat_map(|s| s.items.iter().map(|i| i.tmdb_id))
        .collect();

    let mut by_tmdb: std::collections::HashMap<i64, db::Movie> =
        std::collections::HashMap::new();
    if !tmdb_ids.is_empty() {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT * FROM movies WHERE tmdb_id IN (");
        let mut sep = qb.separated(", ");
        for id in &tmdb_ids {
            sep.push_bind(*id);
        }
        qb.push(")");
        if let Ok(movies) = qb.build_query_as::<db::Movie>().fetch_all(pool).await {
            for m in movies {
                by_tmdb.insert(m.tmdb_id, m);
            }
        }
    }

    let movie_ids: Vec<i64> = by_tmdb.values().map(|m| m.id).collect();
    let in_library_map = db::library_membership_for_movie_ids(pool, &movie_ids)
        .await
        .unwrap_or_default();

    let mut sections = Vec::with_capacity(stored.sections.len());
    for section in stored.sections {
        let mut movies = Vec::with_capacity(section.items.len());
        for item in section.items {
            let movie = match by_tmdb.get(&item.tmdb_id) {
                Some(m) => m.clone(),
                None => continue, // movie deleted since cache, drop the item
            };
            let in_library = in_library_map.get(&movie.id).copied().unwrap_or(false);
            let downloading = if in_library {
                db::is_movie_downloading(pool, movie.id).await.unwrap_or(false)
            } else {
                false
            };
            movies.push(RecommendItem {
                movie,
                reason: item.reason,
                in_library,
                downloading,
            });
        }
        if !movies.is_empty() {
            // 旧缓存只有 zh，en 缺省为空——hydration 时回退用 zh 兜底
            let inspiration_en = if section.inspiration_en.is_empty() {
                section.inspiration_zh.clone()
            } else {
                section.inspiration_en
            };
            sections.push(DailyPickSection {
                inspiration_zh: section.inspiration_zh,
                inspiration_en,
                movies,
            });
        }
    }
    DailyPicksData { sections }
}

async fn generate_daily_picks(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    embedding_model: &EmbeddingModel,
    embedding_store: &EmbeddingStore,
) -> Result<DailyPicksData, String> {
    const TARGET_SECTIONS: usize = 3;

    // 空库短路：没有库内电影就不调 LLM。LLM 没有 ground truth 也生成不出
    // 能搜到东西的灵感；让 daily-picks 直接返回空，前端显示"暂无推荐"。
    let library_total = db::get_library_total(pool).await.map_err(|e| e.to_string())?;
    if library_total == 0 {
        tracing::info!("daily-picks skipped: library is empty");
        return Ok(DailyPicksData { sections: Vec::new() });
    }

    let ideas = generate_ideas(llm, pool).await?;
    let shuffled: Vec<IdeaItem> = {
        use rand::seq::SliceRandom;
        let mut rng = rand::rng();
        let mut v = ideas.clone();
        v.shuffle(&mut rng);
        v
    };

    let mut sections = Vec::new();
    for idea in &shuffled {
        if sections.len() >= TARGET_SECTIONS {
            break;
        }
        tracing::info!(
            display_zh = idea.display_zh.as_str(),
            query = idea.query.as_str(),
            "generating daily pick section ({}/{})",
            sections.len() + 1,
            TARGET_SECTIONS
        );
        // run_smart_search 内部各路径（含 person）已经各自负责 reason 生成——
        // person 路径走 person-pick LLM（不再用模板，所以不会再有 "5 条 X 执导"
        // 这种 uniform 输出）；descriptive/similar_to/attribute 走 smart-rank
        // 已经天然给差异化 reason。daily-picks 这一层不再需要二次 rerank 兜底。
        match run_smart_search(llm, pool, embedding_model, embedding_store, &idea.query, 5, None, &[], None).await {
            Ok(movies) if !movies.is_empty() => {
                sections.push(DailyPickSection {
                    inspiration_zh: idea.display_zh.clone(),
                    inspiration_en: idea.display_en.clone(),
                    movies,
                });
            }
            Ok(_) => {
                tracing::warn!(display_zh = idea.display_zh.as_str(), "daily pick returned no movies, trying next");
            }
            Err(e) => {
                tracing::warn!(display_zh = idea.display_zh.as_str(), error = e.as_str(), "daily pick failed, trying next");
            }
        }
    }

    Ok(DailyPicksData { sections })
}

// --- Routes ---

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/recommend", post(recommend))
        .route("/inspire", post(inspire))
        .route("/daily-picks", get(daily_picks))
        .route("/admin/regenerate-daily-picks", post(regenerate_daily_picks))
}

static GENERATING: AtomicBool = AtomicBool::new(false);

async fn daily_picks(State(state): State<AppState>) -> Json<DailyPicksData> {
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();

    if let Ok(Some(cached)) =
        sqlx::query_scalar::<_, String>("SELECT data FROM daily_picks WHERE date = ?")
            .bind(&today)
            .fetch_optional(&state.pool)
            .await
    {
        // New compact format: stores only tmdb_id + reason; hydrate by
        // joining live movies. Old rows that pre-date this refactor will fail
        // to parse here — fall through to background regen.
        if let Ok(stored) = serde_json::from_str::<DailyPicksStored>(&cached) {
            return Json(hydrate_daily_picks(stored, &state.pool).await);
        }
    }

    let pool = state.pool.clone();
    let llm = state.llm.clone();
    let embedding_model = state
        .embedding_model
        .clone()
        .expect("embedding model required for daily picks");
    let embedding_store = state
        .embedding_store
        .clone()
        .expect("embedding store required for daily picks");

    tokio::spawn(async move {
        if GENERATING
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            tracing::info!("daily picks generation already in progress, skipping");
            return;
        }

        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        let already_exists = sqlx::query_scalar::<_, String>(
            "SELECT data FROM daily_picks WHERE date = ?",
        )
        .bind(&today)
        .fetch_optional(&pool)
        .await
        .ok()
        .flatten()
        .is_some();

        if already_exists {
            GENERATING.store(false, Ordering::SeqCst);
            return;
        }

        tracing::info!("starting daily picks generation for {}", today);

        match generate_daily_picks(&llm, &pool, &embedding_model, &embedding_store).await {
            Ok(data) => {
                let stored = DailyPicksStored::from(&data);
                let json = serde_json::to_string(&stored).unwrap_or_default();
                let _ = sqlx::query(
                    "INSERT OR REPLACE INTO daily_picks (date, data) VALUES (?, ?)",
                )
                .bind(&today)
                .bind(&json)
                .execute(&pool)
                .await;
                tracing::info!("daily picks generated: {} sections", data.sections.len());
            }
            Err(e) => {
                tracing::error!("daily picks generation failed: {}", e);
            }
        }

        GENERATING.store(false, Ordering::SeqCst);
    });

    Json(DailyPicksData { sections: vec![] })
}

async fn regenerate_daily_picks(
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();

    let _ = sqlx::query("DELETE FROM daily_picks WHERE date = ?")
        .bind(&today)
        .execute(&state.pool)
        .await;

    tracing::info!("daily picks cache cleared for {}, triggering regeneration", today);

    let pool = state.pool.clone();
    let llm = state.llm.clone();
    let embedding_model = state
        .embedding_model
        .clone()
        .expect("embedding model required for daily picks regen");
    let embedding_store = state
        .embedding_store
        .clone()
        .expect("embedding store required for daily picks regen");

    tokio::spawn(async move {
        if GENERATING
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            tracing::info!("daily picks generation already in progress");
            return;
        }

        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        tracing::info!("starting daily picks regeneration for {}", today);

        match generate_daily_picks(&llm, &pool, &embedding_model, &embedding_store).await {
            Ok(data) => {
                let stored = DailyPicksStored::from(&data);
                let json = serde_json::to_string(&stored).unwrap_or_default();
                let _ = sqlx::query(
                    "INSERT OR REPLACE INTO daily_picks (date, data) VALUES (?, ?)",
                )
                .bind(&today)
                .bind(&json)
                .execute(&pool)
                .await;
                tracing::info!("daily picks regenerated: {} sections", data.sections.len());
            }
            Err(e) => {
                tracing::error!("daily picks regeneration failed: {}", e);
            }
        }

        GENERATING.store(false, Ordering::SeqCst);
    });

    Json(serde_json::json!({ "message": "regeneration started" }))
}

// --- Existing handlers ---

async fn recommend(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
    Json(req): Json<RecommendRequest>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(32);

    let user_marks = if let Some(ref u) = user {
        db::get_user_marked_movies(&state.pool, u.id)
            .await
            .unwrap_or_default()
    } else {
        vec![]
    };

    let pool_for_history = state.pool.clone();
    let user_id_for_history = user.as_ref().map(|u| u.id);
    let prompt_for_history = req.prompt.clone();

    tokio::spawn(async move {
        let user_marks = user_marks;
        let recorder = if user_id_for_history.is_some() {
            Some(Arc::new(Mutex::new(Vec::<serde_json::Value>::new())))
        } else {
            None
        };

        let sink = match recorder.clone() {
            Some(rec) => EventSink::with_recorder(tx.clone(), rec),
            None => EventSink::new(tx.clone()),
        };

        let embedding_model = state
            .embedding_model
            .as_ref()
            .expect("embedding model required for recommend");
        let embedding_store = state
            .embedding_store
            .as_ref()
            .expect("embedding store required for recommend");
        let result = run_smart_search(
            &state.llm,
            &state.pool,
            embedding_model,
            embedding_store,
            &req.prompt,
            10,
            Some(&sink),
            &user_marks,
            None,
        )
        .await;

        let mut saved_result_count: i64 = 0;
        // Record history whenever the pipeline ran end-to-end, including the
        // empty-result case (user typed a valid query, we just didn't find
        // anything). Hard pipeline errors (LLM timeout, DB fault) stay out of
        // history to avoid polluting it with transient failures.
        let mut pipeline_completed = false;

        match result {
            Ok(mut items) if !items.is_empty() => {
                // Enrich with downloading status
                for item in &mut items {
                    if item.in_library {
                        if let Ok(true) = db::is_movie_downloading(&state.pool, item.movie.id).await {
                            item.downloading = true;
                        }
                    }
                }

                saved_result_count = items.len() as i64;
                pipeline_completed = true;

                sink.emit(
                    sse_event::STATUS,
                    &StatusEvent {
                        stage: "done".to_string(),
                        message: format!("找到 {} 部推荐电影", items.len()),
                    },
                )
                .await;

                let result = RecommendResult {
                    recommendations: items,
                };
                sink.emit(sse_event::RESULT, &result).await;
            }
            Ok(_) => {
                pipeline_completed = true;
                sink.emit(sse_event::ERROR, &serde_json::json!({ "message": "error_no_recommendations" }))
                    .await;
            }
            Err(e) => {
                sink.emit(sse_event::ERROR, &serde_json::json!({ "message": e })).await;
            }
        }

        if pipeline_completed {
            if let (Some(uid), Some(rec)) = (user_id_for_history, recorder) {
                let events_vec = rec.lock().await.clone();
                let events_json =
                    serde_json::to_string(&events_vec).unwrap_or_else(|_| "[]".to_string());
                let _ = db::insert_search_history(
                    &pool_for_history,
                    uid,
                    &prompt_for_history,
                    &events_json,
                    saved_result_count,
                )
                .await;
            }
        }

        sink.emit_raw(sse_event::DONE, "{}");
    });

    // LLM smart-rank 可能静默等待 20-60 秒，期间不发任何事件。
    // 加 SSE keep-alive 注释帧（`:\n\n`），每 15 秒发一次，避免中间代理 / 浏览器
    // 把连接当成空闲断开，也让前端知道"还在干活，没卡死"。
    Sse::new(ReceiverStream::new(rx))
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(15)))
}

#[derive(Serialize)]
struct InspireResponse {
    ideas: Vec<IdeaItem>,
}

async fn inspire(
    State(state): State<AppState>,
) -> Result<Json<InspireResponse>, (axum::http::StatusCode, String)> {
    let ideas = generate_ideas(&state.llm, &state.pool)
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(InspireResponse { ideas }))
}

// --- Utils ---

fn extract_json(text: &str) -> String {
    // Find the outermost JSON value (object or array) by bracket scanning.
    // This handles both bare JSON and markdown-fenced (```json ... ```) responses,
    // because the brackets we look for live strictly inside the JSON payload.
    let trimmed = text.trim();

    let obj_start = trimmed.find('{');
    let arr_start = trimmed.find('[');
    let pick_array = match (obj_start, arr_start) {
        (Some(o), Some(a)) => a < o,
        (None, Some(_)) => true,
        _ => false,
    };
    if pick_array {
        if let (Some(start), Some(end)) = (trimmed.find('['), trimmed.rfind(']')) {
            return trimmed[start..=end].to_string();
        }
    }
    if let Some(start) = obj_start {
        if let Some(end) = trimmed.rfind('}') {
            return trimmed[start..=end].to_string();
        }
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_prompt_embedded_when_saturation_none() {
        assert!(should_embed_user_prompt(ConstraintSaturation::None, "跟摩托车相关的电影"));
    }

    #[test]
    fn user_prompt_embedded_when_saturation_weak() {
        assert!(should_embed_user_prompt(ConstraintSaturation::Weak, "日本动画"));
    }

    #[test]
    fn user_prompt_embedded_when_saturation_medium() {
        assert!(should_embed_user_prompt(ConstraintSaturation::Medium, "90 年代日本动画"));
    }

    #[test]
    fn user_prompt_skipped_when_saturation_strong() {
        // Strong = passes_constraints will filter out anything new the prompt
        // surfaces; pre-skipping saves the embed + lancedb round-trip.
        assert!(!should_embed_user_prompt(ConstraintSaturation::Strong, "周星驰的电影"));
    }

    #[test]
    fn user_prompt_skipped_when_blank() {
        assert!(!should_embed_user_prompt(ConstraintSaturation::None, ""));
        assert!(!should_embed_user_prompt(ConstraintSaturation::None, "   "));
    }

    #[test]
    fn semantic_similarity_identical_vectors_is_one() {
        // squared L2 = 0 means a==b. cos = 1.
        assert!((semantic_similarity_from_l2(0.0) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn semantic_similarity_orthogonal_vectors_is_zero() {
        // For unit vectors, orthogonal (cos=0) → squared L2 = 2(1-0) = 2.
        let s = semantic_similarity_from_l2(2.0);
        assert!(s.abs() < 1e-5, "expected ~0, got {}", s);
    }

    #[test]
    fn semantic_similarity_opposite_vectors_clamps_to_zero() {
        // For unit vectors, opposite (cos=-1) → squared L2 = 2(1-(-1)) = 4.
        // Raw cos would be -1, clamp to 0.
        assert_eq!(semantic_similarity_from_l2(4.0), 0.0);
    }

    #[test]
    fn semantic_similarity_motorcycle_case_strong_match() {
        // Empirical: share TEbG_qgV44FM 摩托日记 squared L2 = 0.8143
        // → cos = 1 - 0.8143/2 ≈ 0.593. Above floor 0.3 → enters pool.
        let s = semantic_similarity_from_l2(0.8143);
        assert!(
            (s - 0.5929).abs() < 0.01,
            "expected ~0.59, got {}",
            s
        );
        assert!(s > SEMANTIC_SIMILARITY_FLOOR);
    }

    #[test]
    fn semantic_similarity_floor_drops_weak_pairs() {
        // squared L2 = 1.5 → cos = 1 - 1.5/2 = 0.25, below 0.3 floor.
        let s = semantic_similarity_from_l2(1.5);
        assert!(s < SEMANTIC_SIMILARITY_FLOOR);
    }

    #[test]
    fn extract_json_strips_markdown_fenced_array() {
        // Regression: Gemini sometimes wraps inspire response in ```json ... ```
        // Old splitn-based stripping returned "json\n[...]\n```" (still invalid).
        let raw = "```json\n[{\"display\": \"d1\", \"query\": \"q1\"}]\n```";
        let extracted = extract_json(raw);
        let parsed: serde_json::Value = serde_json::from_str(&extracted)
            .expect("extracted text must be valid JSON");
        assert!(parsed.is_array());
        assert_eq!(parsed[0]["display"], "d1");
    }

    #[test]
    fn extract_json_strips_markdown_fenced_object() {
        let raw = "```json\n{\"a\": 1}\n```";
        let extracted = extract_json(raw);
        let parsed: serde_json::Value = serde_json::from_str(&extracted).unwrap();
        assert_eq!(parsed["a"], 1);
    }

    #[test]
    fn extract_json_handles_bare_json() {
        let raw = "  [1, 2, 3]  ";
        assert_eq!(extract_json(raw), "[1, 2, 3]");
    }

    // Regression: 2026-04-26 21:34 production case. claude-cli returned a
    // smart-rank response missing the outer object's closing `}` —
    //   {"recommendations": [{...}, ..., {...}]
    // exit 0, no stderr, but `serde_json::from_str` rejects it. The lenient
    // parser must recover the inner array so the user gets recommendations
    // instead of "LLM 精排结果格式异常".
    //
    // Triggered downstream of commit 86651a9 (saturation=Strong → sort_rules
    // rating 1.0): candidate pool composition shifted toward long-tail
    // foreign films, LLM output rhythm changed, truncation rate spiked.
    #[test]
    fn lenient_recovers_truncated_outer_object_close() {
        // Real raw content captured from
        // /Users/berg/marquee-runtime/data/llm-logs/20260426_213418_smart_rank_error.log
        // (10 picks, missing trailing `}`)
        let truncated = r#"{"recommendations": [{"tmdb_id": 122, "reason_zh": "改编自托尔金奇幻文学巅峰之作", "reason_en": "Tolkien classic"}, {"tmdb_id": 4348, "reason_zh": "简·奥斯汀经典", "reason_en": "Austen"}, {"tmdb_id": 1084736, "reason_zh": "大仲马复仇", "reason_en": "Dumas"}, {"tmdb_id": 595, "reason_zh": "哈珀·李", "reason_en": "Lee"}, {"tmdb_id": 3175, "reason_zh": "萨克雷", "reason_en": "Thackeray"}, {"tmdb_id": 37257, "reason_zh": "阿加莎·克里斯蒂", "reason_en": "Christie"}, {"tmdb_id": 555604, "reason_zh": "科洛迪", "reason_en": "Collodi"}, {"tmdb_id": 14696, "reason_zh": "上田秋成", "reason_en": "Akinari"}, {"tmdb_id": 17663, "reason_zh": "蒙哥马利", "reason_en": "Montgomery"}, {"tmdb_id": 536338, "reason_zh": "谷崎润一郎", "reason_en": "Tanizaki"}]"#;
        let recs = parse_recommendations_lenient(truncated)
            .expect("lenient parser should recover truncated outer-object close");
        assert_eq!(recs.len(), 10, "all 10 picks should survive recovery");
        assert_eq!(recs[0].tmdb_id, Some(122));
        assert_eq!(recs[9].tmdb_id, Some(536338));
        // Bilingual fields preserved (the original regex fallback dropped these)
        assert_eq!(recs[0].reason_zh.as_deref(), Some("改编自托尔金奇幻文学巅峰之作"));
        assert_eq!(recs[0].reason_en.as_deref(), Some("Tolkien classic"));
    }

    #[test]
    fn lenient_recovers_truncated_array_close_and_object_close() {
        // Even more aggressive truncation: missing both `]` and `}`. Last
        // recommendation entry is fully written, just no closing punctuation.
        let truncated = r#"{"recommendations": [{"tmdb_id": 1, "reason_zh": "a", "reason_en": "A"}, {"tmdb_id": 2, "reason_zh": "b", "reason_en": "B"}"#;
        let recs = parse_recommendations_lenient(truncated)
            .expect("lenient parser should recover even more aggressive truncation");
        assert_eq!(recs.len(), 2);
    }

    #[test]
    fn lenient_recovers_unescaped_inner_quotes() {
        // Regression: 2026-04-28 06:31 production case
        // /Users/berg/marquee-runtime/data/llm-logs/20260428_063141_smart_rank_error.log
        // LLM emitted ASCII `"` around a quoted word inside reason_zh,
        // breaking strict JSON syntax. Repair tier should escape them.
        let bad = r#"{"recommendations": [{"tmdb_id": 496243, "reason_zh": "候选库中没有真正以"外卖"为主题的电影。最接近的是《寄生虫》开篇——金家一家靠折披萨外卖盒勉强糊口，这条线索铺开了整个阶级寓言。", "reason_en": "No film in the pool truly centers on food delivery. The closest is Parasite's opening, where the Kim family folds pizza delivery boxes to survive—setting up the entire class allegory."}]}"#;
        let recs = parse_recommendations_lenient(bad)
            .expect("lenient parser should repair unescaped inner quotes");
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].tmdb_id, Some(496243));
        let reason_zh = recs[0].reason_zh.as_deref().unwrap();
        assert!(reason_zh.contains("外卖"), "Chinese text content preserved");
        assert!(reason_zh.contains("寄生虫"), "trailing content preserved");
        assert_eq!(
            recs[0].reason_en.as_deref().unwrap().chars().take(7).collect::<String>(),
            "No film",
        );
    }

    #[test]
    fn repair_quotes_noop_on_valid_json() {
        // Sanity: well-formed JSON should pass through unchanged.
        let good = r#"{"recommendations": [{"tmdb_id": 1, "reason_zh": "abc", "reason_en": "xyz"}]}"#;
        assert_eq!(repair_unescaped_inner_quotes(good), good);
    }

    #[test]
    fn repair_quotes_preserves_escaped_quotes() {
        // Already-escaped `\"` should not be touched, real close still detected.
        let s = r#"{"recommendations": [{"tmdb_id": 1, "reason_zh": "say \"hi\" again", "reason_en": "x"}]}"#;
        // After repair, parsing must still produce reason_zh containing actual quotes.
        let repaired = repair_unescaped_inner_quotes(s);
        let recs = try_parse_value_tree(&repaired).expect("repaired JSON parses");
        assert_eq!(recs[0].reason_zh.as_deref(), Some("say \"hi\" again"));
    }

    #[test]
    fn lenient_intact_json_passes_through() {
        // Sanity: well-formed JSON should not regress.
        let intact = r#"{"recommendations": [{"tmdb_id": 1, "reason_zh": "a", "reason_en": "A"}]}"#;
        let recs = parse_recommendations_lenient(intact).expect("intact JSON parses");
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].tmdb_id, Some(1));
    }

    #[test]
    fn lenient_garbage_returns_none() {
        // Non-JSON / no recoverable items → None (caller writes parse_error log).
        assert!(parse_recommendations_lenient("totally not json").is_none());
        assert!(parse_recommendations_lenient("").is_none());
        assert!(parse_recommendations_lenient("{\"recommendations\": []}").is_none());
    }

    // ---------- person-pick LLM reason 解析 ----------
    //
    // 替代了之前的 daily-picks reasons_too_uniform / merge_llm_reasons 兜底路径——
    // 现在 handle_person 直接走 person-pick LLM 给每部生成差异化 reason；
    // 解析失败 / 缺漏 → reason = None，前端 graceful 隐藏。
    //
    // 这组测试覆盖 parse_person_pick_reasons 解析行为：完整 / 缺失 / 双语
    // 兜底 / 格式错误 → 空 map。

    fn pick_with_reason(tmdb_id: i64, reason: Option<&str>) -> RecommendItem {
        // 最小 RecommendItem fixture——只为测 reasons_too_uniform 这个纯函数，
        // 它只读 .reason 字段，其它 movie 字段填啥都行。
        RecommendItem {
            movie: db::Movie {
                id: tmdb_id,
                tmdb_id,
                title: format!("M{}", tmdb_id),
                original_title: None,
                year: None,
                overview: None,
                poster_url: None,
                genres: None,
                country: None,
                language: None,
                runtime: None,
                director: None,
                director_info: None,
                cast: None,
                tmdb_rating: None,
                tmdb_votes: None,
                keywords: None,
                llm_tags: None,
                budget: None,
                revenue: None,
                popularity: None,
                title_zh: None,
                title_en: None,
                overview_zh: None,
                overview_en: None,
                tagline_zh: None,
                tagline_en: None,
                genres_zh: None,
                genres_en: None,
                director_info_en: None,
                cast_en: None,
                keywords_en: None,
                collection_en: None,
                production_companies_en: None,
                imdb_id: None,
                backdrop_path: None,
                homepage: None,
                status: None,
                collection: None,
                production_companies: None,
                spoken_languages: None,
                origin_country: None,
                source: Some("library".into()),
                created_at: String::new(),
                updated_at: String::new(),
            },
            reason: reason.map(|s| s.to_string()),
            in_library: true,
            downloading: false,
        }
    }

    // 静默 unused warning (pick_with_reason 仅给 person-pick 测试 fixture 用，
    // 但 person-pick 测试本身不需要构造完整 RecommendItem——只测 JSON 解析层。
    // 保留 helper 是为了未来加针对 RecommendItem-级别的 person-pick 测试时不
    // 重复造轮子。)
    #[allow(dead_code)]
    fn _ensure_pick_with_reason_used(_t: i64, _r: Option<&str>) -> RecommendItem {
        pick_with_reason(_t, _r)
    }

    fn find_reason<'a>(picks: &'a [(i64, String)], tmdb_id: i64) -> Option<&'a str> {
        picks.iter().find(|(id, _)| *id == tmdb_id).map(|(_, s)| s.as_str())
    }

    #[test]
    fn parse_person_pick_full_zh_locale() {
        let json = r#"{"reasons": [
            {"tmdb_id": 1, "reason_zh": "夜色弥漫的悬念", "reason_en": "Suspense in the dark"},
            {"tmdb_id": 2, "reason_zh": "黑白光影中的窒息", "reason_en": "Black and white claustrophobia"}
        ]}"#;
        let picks = parse_person_pick_picks(json, "zh");
        assert_eq!(picks.len(), 2);
        assert_eq!(find_reason(&picks, 1), Some("夜色弥漫的悬念"));
        assert_eq!(find_reason(&picks, 2), Some("黑白光影中的窒息"));
    }

    #[test]
    fn parse_person_pick_full_en_locale() {
        let json = r#"{"reasons": [
            {"tmdb_id": 1, "reason_zh": "夜色弥漫的悬念", "reason_en": "Suspense in the dark"}
        ]}"#;
        let picks = parse_person_pick_picks(json, "en");
        assert_eq!(find_reason(&picks, 1), Some("Suspense in the dark"));
    }

    #[test]
    fn parse_person_pick_falls_back_to_other_locale() {
        // zh 语境但 reason_zh 缺失 → 用 reason_en 兜底；不留 None
        let json = r#"{"reasons": [
            {"tmdb_id": 1, "reason_zh": null, "reason_en": "fallback en reason"},
            {"tmdb_id": 2, "reason_zh": "", "reason_en": "fallback again"}
        ]}"#;
        let picks = parse_person_pick_picks(json, "zh");
        assert_eq!(find_reason(&picks, 1), Some("fallback en reason"));
        assert_eq!(find_reason(&picks, 2), Some("fallback again"));
    }

    #[test]
    fn parse_person_pick_skips_entries_with_no_reason() {
        // 两个语言字段都缺失 → 这条不进 picks（caller 看到该 tmdb_id 缺失 → reason=None）
        let json = r#"{"reasons": [
            {"tmdb_id": 1, "reason_zh": "有", "reason_en": "have"},
            {"tmdb_id": 2}
        ]}"#;
        let picks = parse_person_pick_picks(json, "zh");
        assert_eq!(picks.len(), 1);
        assert_eq!(find_reason(&picks, 1), Some("有"));
        assert!(find_reason(&picks, 2).is_none());
    }

    #[test]
    fn parse_person_pick_blank_reasons_treated_as_missing() {
        // 全空白也算缺失，避免把无意义内容当 reason
        let json = r#"{"reasons": [
            {"tmdb_id": 1, "reason_zh": "   ", "reason_en": "\t"}
        ]}"#;
        let picks = parse_person_pick_picks(json, "zh");
        assert!(picks.is_empty());
    }

    #[test]
    fn parse_person_pick_returns_empty_on_garbage() {
        // 解析失败 / 显式空数组 → 空 Vec
        // 这是"LLM 明确返空"的合法路径——caller 看到空 → handle_person 返空 →
        // router fall through 到 descriptive，不是兜底前 N 部代表作。
        assert!(parse_person_pick_picks("totally not json", "zh").is_empty());
        assert!(parse_person_pick_picks("", "zh").is_empty());
        assert!(parse_person_pick_picks("{\"reasons\": []}", "zh").is_empty());
    }

    #[test]
    fn parse_person_pick_preserves_llm_order() {
        // Vec 而不是 HashMap：LLM 给的顺序是它的"挑选排序"，要按这个顺序展示给用户。
        // 这是改动 person-pick 后的新合同——之前 HashMap 丢了顺序信息，碰巧因为
        // 当时是"全给"才没出问题；现在 LLM 在挑选了，顺序变成产品语义。
        let json = r#"{"reasons": [
            {"tmdb_id": 30, "reason_zh": "三", "reason_en": "three"},
            {"tmdb_id": 10, "reason_zh": "一", "reason_en": "one"},
            {"tmdb_id": 20, "reason_zh": "二", "reason_en": "two"}
        ]}"#;
        let picks = parse_person_pick_picks(json, "zh");
        assert_eq!(picks.len(), 3);
        assert_eq!(picks[0].0, 30);
        assert_eq!(picks[1].0, 10);
        assert_eq!(picks[2].0, 20);
    }

    #[test]
    fn parse_person_pick_tolerates_extra_fields() {
        // LLM 偶尔给额外字段（confidence / order 等）—— 不该让解析失败
        let json = r#"{
            "reasons": [
                {"tmdb_id": 1, "reason_zh": "正经 reason", "reason_en": "real", "confidence": 0.95}
            ],
            "model_note": "some debug info"
        }"#;
        let picks = parse_person_pick_picks(json, "zh");
        assert_eq!(find_reason(&picks, 1), Some("正经 reason"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn hydrate_daily_picks_uses_live_movie_rows(pool: db::SqlitePool) {
        // Stored format references a movie by tmdb_id; hydrate must pull
        // poster_url et al. from the live row, not from any frozen snapshot.
        let movie_id = db::insert_movie(
            &pool, 49047, "Léon", None, Some(1994), None,
            Some("https://image.tmdb.org/t/p/w500/NEW.jpg"),
            "[]", None, Some("fr"), None, None, "[]",
            None, None, "[]", None, None, None, "library",
        )
        .await
        .unwrap();
        // 绑给一个目录——in_library 真值看 dir_movie_mappings，不看 movies.source
        let dir_id = db::insert_media_dir(&pool, "/m/leon", "Leon (1994)").await.unwrap();
        db::insert_mapping(&pool, dir_id, Some(movie_id), "auto", Some(0.95), None)
            .await
            .unwrap();

        let stored = DailyPicksStored {
            sections: vec![DailyPickSectionStored {
                inspiration_zh: "夜色温柔".into(),
                inspiration_en: "Tender Night".into(),
                items: vec![DailyPickItemStored {
                    tmdb_id: 49047,
                    reason: Some("有点温柔有点狠".into()),
                }],
            }],
        };

        let hydrated = hydrate_daily_picks(stored, &pool).await;
        assert_eq!(hydrated.sections.len(), 1);
        assert_eq!(hydrated.sections[0].movies.len(), 1);
        let item = &hydrated.sections[0].movies[0];
        assert_eq!(item.movie.id, movie_id);
        assert_eq!(
            item.movie.poster_url.as_deref(),
            Some("https://image.tmdb.org/t/p/w500/NEW.jpg"),
            "poster_url must be the live movies-row value, not a snapshot"
        );
        assert_eq!(item.reason.as_deref(), Some("有点温柔有点狠"));
        assert!(item.in_library, "bound to dir → in_library=true");
    }

    /// 回归 bug：用户用 locate-movie 把 source='related' 的电影绑给本地目录后，
    /// daily-picks 仍标 in_library=false，前端显示"库外发现"。修复后 in_library
    /// 必须从 dir_movie_mappings 读，跟 source 无关。
    #[sqlx::test(migrations = "./migrations")]
    async fn hydrate_daily_picks_uses_dir_mappings_not_source(pool: db::SqlitePool) {
        // 模拟：电影最初作为 related 抓入（source='related'），后被 locate-movie 绑定
        let movie_id = db::insert_movie(
            &pool, 129, "千与千寻", None, Some(2001), None, Some("/p.jpg"),
            "[]", None, Some("ja"), None, None, "[]",
            None, None, "[]", None, None, None, "related",
        )
        .await
        .unwrap();
        let dir_id = db::insert_media_dir(&pool, "/m/spirited", "Spirited Away").await.unwrap();
        // 'manual' = locate-movie 绑定路径
        db::insert_mapping(&pool, dir_id, Some(movie_id), "manual", Some(1.0), None)
            .await
            .unwrap();

        let stored = DailyPicksStored {
            sections: vec![DailyPickSectionStored {
                inspiration_zh: "经典".into(),
                inspiration_en: "Classic".into(),
                items: vec![DailyPickItemStored { tmdb_id: 129, reason: None }],
            }],
        };

        let hydrated = hydrate_daily_picks(stored, &pool).await;
        let item = &hydrated.sections[0].movies[0];
        assert_eq!(item.movie.source.as_deref(), Some("related"), "source 不变");
        assert!(item.in_library, "尽管 source='related'，dir_movie_mappings 已 manual → 应当 in_library=true");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn hydrate_daily_picks_drops_deleted_movies(pool: db::SqlitePool) {
        // tmdb_id 999 doesn't exist → that item is silently dropped.
        let _kept_id = db::insert_movie(
            &pool, 100, "Kept", None, None, None, Some("/k.jpg"),
            "[]", None, Some("en"), None, None, "[]",
            None, None, "[]", None, None, None, "library",
        )
        .await
        .unwrap();

        let stored = DailyPicksStored {
            sections: vec![DailyPickSectionStored {
                inspiration_zh: "test".into(),
                inspiration_en: "test".into(),
                items: vec![
                    DailyPickItemStored { tmdb_id: 100, reason: None },
                    DailyPickItemStored { tmdb_id: 999, reason: None },
                ],
            }],
        };
        let hydrated = hydrate_daily_picks(stored, &pool).await;
        assert_eq!(hydrated.sections[0].movies.len(), 1);
        assert_eq!(hydrated.sections[0].movies[0].movie.tmdb_id, 100);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn hydrate_daily_picks_drops_empty_sections(pool: db::SqlitePool) {
        // A section whose movies are all deleted should not appear in output.
        let stored = DailyPicksStored {
            sections: vec![DailyPickSectionStored {
                inspiration_zh: "lost".into(),
                inspiration_en: "lost".into(),
                items: vec![DailyPickItemStored { tmdb_id: 999, reason: None }],
            }],
        };
        let hydrated = hydrate_daily_picks(stored, &pool).await;
        assert!(hydrated.sections.is_empty());
    }

    #[test]
    fn daily_picks_stored_round_trip_excludes_movie_payload() {
        // Sanity: stored form must NOT include the full Movie struct on the
        // wire (or the size and snapshot-ness regress).
        let data = DailyPicksData {
            sections: vec![DailyPickSection {
                inspiration_zh: "x".into(),
                inspiration_en: "x".into(),
                movies: vec![RecommendItem {
                    movie: db::Movie {
                        id: 1, tmdb_id: 42, title: "x".into(),
                        original_title: None, year: None, overview: None,
                        poster_url: Some("/SHOULD-NOT-PERSIST.jpg".into()),
                        genres: None, country: None, language: None,
                        runtime: None, director: None, director_info: None,
                        cast: None, tmdb_rating: None, tmdb_votes: None,
                        keywords: None, llm_tags: None, budget: None,
                        revenue: None, popularity: None,
                        title_zh: None, title_en: None,
                        overview_zh: None, overview_en: None,
                        tagline_zh: None, tagline_en: None,
                        genres_zh: None, genres_en: None,
                        director_info_en: None, cast_en: None,
                        keywords_en: None, collection_en: None,
                        production_companies_en: None,
                        imdb_id: None, backdrop_path: None, homepage: None,
                        status: None, collection: None,
                        production_companies: None, spoken_languages: None,
                        origin_country: None, source: Some("library".into()),
                        created_at: "".into(), updated_at: "".into(),
                    },
                    reason: Some("good vibes".into()),
                    in_library: true,
                    downloading: false,
                }],
            }],
        };
        let stored: DailyPicksStored = (&data).into();
        let json = serde_json::to_string(&stored).unwrap();
        assert!(!json.contains("SHOULD-NOT-PERSIST"));
        assert!(json.contains("\"tmdb_id\":42"));
        assert!(json.contains("good vibes"));
    }
}
