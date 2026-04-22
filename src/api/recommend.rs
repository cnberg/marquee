use axum::{
    extract::State,
    response::sse::{Event, Sse},
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

use crate::api::AppState;
use crate::auth::OptionalUser;
use crate::db;
use crate::embedding::{EmbeddingModel, EmbeddingStore};
use crate::llm::LlmClient;
use crate::search::intent::{validate_intent, QueryIntent, SortRule};
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
            "status",
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
            "thinking",
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
    reason: Option<String>,
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
        ("smart-rank", "en") => include_str!("../../prompts/smart-rank.en.md"),
        ("recommend-filter", _) => include_str!("../../prompts/recommend-filter.md"),
        ("recommend-pick", _) => include_str!("../../prompts/recommend-pick.md"),
        ("inspire", _) => include_str!("../../prompts/inspire.md"),
        ("query-understand", _) => include_str!("../../prompts/query-understand.md"),
        ("smart-rank", _) => include_str!("../../prompts/smart-rank.md"),
        _ => panic!("Unknown prompt: {}", name),
    }
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

struct LibrarySummary {
    total: String,
    genres: String,
    countries: String,
    decades: String,
    directors: String,
    cast: String,
    ratings: String,
    budgets: String,
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
    pub display: String,
    pub query: String,
}

/// Generate inspiration ideas from LLM
async fn generate_ideas(llm: &LlmClient, pool: &db::SqlitePool) -> Result<Vec<IdeaItem>, String> {
    let stats = db::get_library_stats(pool).await.map_err(|e| e.to_string())?;
    let locale = get_locale(pool).await;
    let summary = build_library_summary(&stats, &locale);
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M (%A)").to_string();

    let template = load_prompt(pool, "inspire", &locale).await;
    let system_prompt = render_prompt(&template, &[
        ("total", &summary.total),
        ("genres", &summary.genres),
        ("countries", &summary.countries),
        ("decades", &summary.decades),
        ("directors", &summary.directors),
        ("cast", &summary.cast),
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

/// 新的四阶段智能搜索管线。
async fn run_smart_search(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    embedding_model: &EmbeddingModel,
    embedding_store: &EmbeddingStore,
    prompt: &str,
    max_results: usize,
    sink_opt: Option<&EventSink>,
    user_marks: &[db::UserMarkedMovie],
) -> Result<Vec<RecommendItem>, String> {
    // ========== Stage 1: Query Understanding ==========
    emit_status(sink_opt, "understanding", "正在理解你的查询…").await;

    let stats = db::get_library_stats(pool).await.map_err(|e| e.to_string())?;
    let locale = get_locale(pool).await;
    let summary = build_library_summary(&stats, &locale);

    // Stage 1: Query Understanding
    let template = load_prompt(pool, "query-understand", &locale).await;

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

    tracing::info!(
        "QueryIntent parsed: query_type={}, search_intents={:?}, constraints_genres={:?}",
        intent.query_type,
        intent.search_intents,
        intent.constraints.genres
    );

    // Thinking: Stage 1 — query understanding result
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

    // ========== Stage 2: Multi-path Recall ==========
    emit_status(sink_opt, "recall", "正在多路召回候选…").await;

    // Structured recall
    let structured_future = db::structured_recall(pool, &intent.constraints, &intent.exclusions, 200);

    // Semantic recall
    let semantic_results = {
        let mut all_hits: Vec<(i64, f32)> = Vec::new();
        for search_intent in &intent.search_intents {
            match embedding_model.embed_one(search_intent) {
                Ok(query_vec) => match embedding_store.search(&query_vec, 100).await {
                    Ok(hits) => all_hits.extend(hits),
                    Err(e) => tracing::warn!("semantic search failed: {}", e),
                },
                Err(e) => tracing::warn!("embedding failed for intent: {}", e),
            }
        }
        all_hits
    };

    let structured = structured_future.await.map_err(|e| e.to_string())?;

    let mut candidate_map: HashMap<i64, RankedCandidate> = HashMap::new();

    for m in &structured {
        let in_library = m.source.as_deref() != Some("related");
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
                runtime: m.runtime,
                popularity: m.popularity,
                budget: m.budget,
                keywords: m.keywords.clone(),
                cast: m.cast_json.clone(),
                source: "structured".to_string(),
                in_library,
                semantic_score: 0.5,
            },
        );
    }

    let max_distance = semantic_results
        .iter()
        .map(|(_, d)| *d)
        .fold(0.0f32, f32::max);

    for (movie_id, distance) in &semantic_results {
        let similarity = if max_distance > 0.0 {
            1.0 - (distance / max_distance) as f64
        } else {
            1.0
        };

        if let Some(existing) = candidate_map.get_mut(movie_id) {
            existing.source = "both".to_string();
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
        .filter(|m| m.source.as_deref() != Some("related"))
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
                    c.runtime = movie.runtime;
                    c.popularity = movie.popularity;
                    c.budget = movie.budget;
                    c.keywords = movie.keywords;
                    c.cast = movie.cast;
                    c.in_library = movie.source.as_deref() != Some("related");
                }
            } else {
                candidate_map.remove(movie_id);
            }
        }
    }

    candidate_map.retain(|_, c| c.tmdb_id != 0);

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
            serde_json::json!({
                "title": c.title,
                "year": c.year,
                "genres": c.genres,
                "director": c.director,
                "rating": c.tmdb_rating,
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

    let template = load_prompt(pool, "smart-rank", &locale).await;
    let candidate_count = candidates.len().to_string();
    let candidates_joined = candidates_text.join("\n");
    let system_prompt = render_prompt(&template, &[
        ("candidate_count", &candidate_count),
        ("candidates", &candidates_joined),
        ("user_query", prompt),
    ]);

    let llm_response = llm
        .chat(&system_prompt, prompt)
        .await
        .map_err(|e| format!("LLM 精排失败: {}", e))?;

    // Thinking: Stage 4 — LLM fine ranking raw response
    emit_thinking(sink_opt, "selecting", "LLM 精排原始返回", serde_json::json!({
        "candidates_sent": candidates.len(),
        "llm_raw_response": llm_response,
    })).await;

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
                reason: rec.reason.clone(),
                in_library: *in_library_map.get(&tmdb_id).unwrap_or(&true),
            });
        }
    }

    Ok(result_items)
}

// --- Lenient parsing helpers ---

fn parse_recommendations_lenient(json_str: &str) -> Option<Vec<LlmRecommendation>> {
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str) {
        if let Some(arr) = val.get("recommendations").and_then(|v| v.as_array()) {
            let mut results = Vec::new();
            for item in arr {
                let tmdb_id = item.get("tmdb_id").and_then(|v| v.as_i64());
                if tmdb_id.is_none() {
                    continue;
                }
                let reason = item
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                results.push(LlmRecommendation { tmdb_id, reason });
            }
            if !results.is_empty() {
                return Some(results);
            }
        }
    }

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

fn write_parse_error_log(tag: &str, raw: &str, err: &str) {
    let dir = std::path::Path::new("data/llm-logs");
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

#[derive(Serialize, Deserialize, Clone)]
pub struct DailyPicksData {
    pub sections: Vec<DailyPickSection>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DailyPickSection {
    pub inspiration: String,
    pub movies: Vec<RecommendItem>,
}

async fn generate_daily_picks(
    llm: &LlmClient,
    pool: &db::SqlitePool,
    embedding_model: &EmbeddingModel,
    embedding_store: &EmbeddingStore,
) -> Result<DailyPicksData, String> {
    const TARGET_SECTIONS: usize = 3;

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
            display = idea.display.as_str(),
            query = idea.query.as_str(),
            "generating daily pick section ({}/{})",
            sections.len() + 1,
            TARGET_SECTIONS
        );
        match run_smart_search(llm, pool, embedding_model, embedding_store, &idea.query, 5, None, &[]).await {
            Ok(movies) if !movies.is_empty() => {
                sections.push(DailyPickSection {
                    inspiration: idea.display.clone(),
                    movies,
                });
            }
            Ok(_) => {
                tracing::warn!(display = idea.display.as_str(), "daily pick returned no movies, trying next");
            }
            Err(e) => {
                tracing::warn!(display = idea.display.as_str(), error = e.as_str(), "daily pick failed, trying next");
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
        if let Ok(data) = serde_json::from_str::<DailyPicksData>(&cached) {
            return Json(data);
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
                let json = serde_json::to_string(&data).unwrap_or_default();
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
                let json = serde_json::to_string(&data).unwrap_or_default();
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
        )
        .await;

        let mut saved_result_count: i64 = 0;
        let mut had_result = false;

        match result {
            Ok(items) if !items.is_empty() => {
                saved_result_count = items.len() as i64;
                had_result = true;

                sink.emit(
                    "status",
                    &StatusEvent {
                        stage: "done".to_string(),
                        message: format!("找到 {} 部推荐电影", items.len()),
                    },
                )
                .await;

                let result = RecommendResult {
                    recommendations: items,
                };
                sink.emit("result", &result).await;
            }
            Ok(_) => {
                sink.emit("error", &serde_json::json!({ "message": "error_no_recommendations" }))
                    .await;
            }
            Err(e) => {
                sink.emit("error", &serde_json::json!({ "message": e })).await;
            }
        }

        if had_result {
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

        sink.emit_raw("done", "{}");
    });

    Sse::new(ReceiverStream::new(rx))
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
}
