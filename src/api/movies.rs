use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

use super::common::{default_page, default_per_page, ListResponse};
use crate::{
    api::{AppState, CachedMostRelatedItem, MostRelatedCache},
    auth::OptionalUser,
    db::{self, Movie},
};

const MOST_RELATED_CACHE_TTL: Duration = Duration::from_secs(3600);

#[derive(Debug, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_per_page")]
    pub per_page: i64,
    #[serde(default)]
    pub decade: Option<String>,
    #[serde(default)]
    pub genre: Option<String>,
    #[serde(default)]
    pub country: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub rating: Option<String>,
    #[serde(default)]
    pub runtime: Option<String>,
    #[serde(default)]
    pub director: Option<String>,
    #[serde(default)]
    pub keyword: Option<String>,
    #[serde(default)]
    pub cast: Option<String>,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/movies", get(list_movies))
        .route("/movies/status-counts", get(status_counts))
        .route("/movies/stats", get(library_stats))
        .route("/movies/filters", get(filters))
        .route("/movies/most-related", get(most_related_out_of_library))
        .route("/movies/recent-library", get(recent_library_movies))
        .route("/movies/{id}", get(get_movie))
        // 子资源端点
        .route("/movies/{id}/credits", get(movie_credits))
        .route("/movies/{id}/images", get(movie_images))
        .route("/movies/{id}/videos", get(movie_videos))
        .route("/movies/{id}/reviews", get(movie_reviews))
        .route("/movies/{id}/similar", get(movie_similar))
        .route("/movies/{id}/recommendations", get(movie_recommendations))
        .route("/movies/{id}/watch-providers", get(movie_watch_providers))
        .route("/movies/{id}/release-dates", get(movie_release_dates))
        .route("/movies/{id}/ai-insight", get(movie_ai_insight))
        .route("/movies/{id}/locate", post(locate_movie))
}

#[derive(Debug, Serialize)]
struct LocateCandidate {
    dir_id: i64,
    dir_name: String,
    dir_path: String,
    status: Option<String>,
    score: f64,
    parsed_title: String,
    parsed_year: Option<u16>,
}

#[derive(Debug, Serialize)]
struct LocateResponse {
    candidates: Vec<LocateCandidate>,
}

/// 反向定位：在所有未绑定的 media_dirs 里找跟当前电影标题/年份匹配的目录。
/// 不扫文件系统、不调 TMDB——纯 DB + parser + 标题年份打分。
/// 详见 docs/specs/2026-04-28-locate-movie-design.md。
async fn locate_movie(
    _user: crate::auth::RequireUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<LocateResponse>, (StatusCode, String)> {
    let movie = db::get_movie_by_id(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "movie not found".into()))?;

    let dirs = db::list_unbound_media_dirs(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let query_year: Option<u16> = movie.year.and_then(|y| u16::try_from(y).ok());

    let mut scored: Vec<LocateCandidate> = dirs
        .into_iter()
        .filter_map(|d| {
            let parsed = crate::scanner::parser::parse_directory_name(&d.dir_name);
            // 空标题（极端 noise 目录名）跳过——打分没意义
            if parsed.title.is_empty() {
                return None;
            }
            let score = crate::tmdb::matcher::score_title_year(
                &movie.title,
                movie.original_title.as_deref(),
                query_year,
                &parsed.title,
                parsed.alt_title.as_deref(),
                parsed.year,
                None,
            );
            Some(LocateCandidate {
                dir_id: d.dir_id,
                dir_name: d.dir_name,
                dir_path: d.dir_path,
                status: d.status,
                score,
                parsed_title: parsed.title,
                parsed_year: parsed.year,
            })
        })
        .filter(|c| c.score >= 0.5)
        .collect();

    scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(10);

    Ok(Json(LocateResponse { candidates: scored }))
}

async fn status_counts(
    State(state): State<AppState>,
) -> Json<std::collections::HashMap<String, i64>> {
    let counts = db::get_match_status_counts(&state.pool).await.unwrap_or_default();
    let mut map = std::collections::HashMap::new();
    let mut total: i64 = 0;
    for (status, count) in counts {
        total += count;
        // merge "manual" into "auto" for display
        if status == "manual" {
            *map.entry("auto".to_string()).or_insert(0) += count;
        } else {
            *map.entry(status).or_insert(0) += count;
        }
    }
    map.insert("all".to_string(), total);
    let library_total = db::get_library_total(&state.pool).await.unwrap_or(0);
    map.insert("library_total".to_string(), library_total);
    Json(map)
}

async fn list_movies(
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<ListResponse<Vec<Movie>>>, (StatusCode, String)> {
    let filters = db::MovieFilters {
        decade: params.decade,
        genre: params.genre,
        country: params.country,
        language: params.language,
        rating: params.rating,
        runtime: params.runtime,
        director: params.director,
        keyword: params.keyword,
        cast: params.cast,
        ..Default::default()
    };
    let (movies, total) = db::list_movies(
        &state.pool,
        params.search.as_deref(),
        params.status.as_deref(),
        &filters,
        params.page,
        params.per_page,
    )
    .await
    .map_err(internal_error)?;

    Ok(Json(ListResponse {
        data: movies,
        page: params.page.max(1),
        per_page: params.per_page.max(1),
        total,
    }))
}

#[derive(Debug, Serialize)]
pub struct MovieDetail {
    #[serde(flatten)]
    pub movie: Movie,
    pub dir_paths: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credits: Option<Vec<db::MovieCredit>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub images: Option<Vec<db::MovieImage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub videos: Option<Vec<db::MovieVideo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reviews: Option<Vec<db::MovieReview>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub similar: Option<Vec<db::Movie>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommendations: Option<Vec<db::Movie>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watch_providers: Option<Vec<db::MovieWatchProvider>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_dates: Option<Vec<db::MovieReleaseDate>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_ids: Option<db::MovieExternalId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alternative_titles: Option<Vec<db::MovieAlternativeTitle>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub translations: Option<Vec<db::MovieTranslation>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lists: Option<Vec<db::MovieList>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_status: Option<db::DownloadStatus>,
}

#[derive(Debug, Deserialize)]
pub struct MovieDetailParams {
    #[serde(default)]
    pub include: Option<String>, // comma separated
}

async fn get_movie(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<MovieDetailParams>,
) -> Result<Json<MovieDetail>, StatusCode> {
    match db::get_movie_by_id(&state.pool, id).await {
        Ok(Some(movie)) => {
            let includes: Vec<String> = params
                .include
                .as_deref()
                .map(|s| {
                    s.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            let has_include = |key: &str| includes.iter().any(|k| k == key);

            let dir_paths = db::get_dir_paths_for_movie(&state.pool, id)
                .await
                .unwrap_or_default();

            let credits = if has_include("credits") {
                Some(db::get_movie_credits(&state.pool, id).await.unwrap_or_default())
            } else {
                None
            };

            let images = if has_include("images") {
                Some(db::get_movie_images(&state.pool, id).await.unwrap_or_default())
            } else {
                None
            };

            let videos = if has_include("videos") {
                Some(db::get_movie_videos(&state.pool, id).await.unwrap_or_default())
            } else {
                None
            };

            let reviews = if has_include("reviews") {
                Some(db::get_movie_reviews(&state.pool, id).await.unwrap_or_default())
            } else {
                None
            };

            let similar = if has_include("similar") {
                Some(
                    db::get_enriched_related_movies(&state.pool, id, "similar")
                        .await
                        .unwrap_or_default(),
                )
            } else {
                None
            };

            let recommendations = if has_include("recommendations") {
                Some(
                    db::get_enriched_related_movies(&state.pool, id, "recommendation")
                        .await
                        .unwrap_or_default(),
                )
            } else {
                None
            };

            let watch_providers = if has_include("watch_providers") || has_include("watch-providers") {
                Some(
                    db::get_movie_watch_providers(&state.pool, id)
                        .await
                        .unwrap_or_default(),
                )
            } else {
                None
            };

            let release_dates = if has_include("release_dates") || has_include("release-dates") {
                Some(
                    db::get_movie_release_dates(&state.pool, id)
                        .await
                        .unwrap_or_default(),
                )
            } else {
                None
            };

            let external_ids = if has_include("external_ids") || has_include("external-ids") {
                db::get_movie_external_ids(&state.pool, id).await.unwrap_or(None)
            } else {
                None
            };

            let alternative_titles = if has_include("alternative_titles")
                || has_include("alternative-titles")
            {
                Some(
                    db::get_movie_alternative_titles(&state.pool, id)
                        .await
                        .unwrap_or_default(),
                )
            } else {
                None
            };

            let translations = if has_include("translations") {
                Some(
                    db::get_movie_translations(&state.pool, id)
                        .await
                        .unwrap_or_default(),
                )
            } else {
                None
            };

            let lists = if has_include("lists") {
                Some(db::get_movie_lists(&state.pool, id).await.unwrap_or_default())
            } else {
                None
            };

            let download_status = db::get_download_status_for_movie(&state.pool, id)
                .await
                .unwrap_or(None);

            Ok(Json(MovieDetail {
                movie,
                dir_paths,
                credits,
                images,
                videos,
                reviews,
                similar,
                recommendations,
                watch_providers,
                release_dates,
                external_ids,
                alternative_titles,
                translations,
                lists,
                download_status,
            }))
        }
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

macro_rules! sub_resource_handler {
    ($fn_name:ident, $query_fn:path, $ret_type:ty) => {
        async fn $fn_name(
            State(state): State<AppState>,
            Path(id): Path<i64>,
        ) -> Result<Json<Vec<$ret_type>>, StatusCode> {
            $query_fn(&state.pool, id).await.map(Json).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        }
    };
    ($fn_name:ident, $query_fn:path, $ret_type:ty, $($extra:expr),+) => {
        async fn $fn_name(
            State(state): State<AppState>,
            Path(id): Path<i64>,
        ) -> Result<Json<Vec<$ret_type>>, StatusCode> {
            $query_fn(&state.pool, id, $($extra),+).await.map(Json).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        }
    };
}

sub_resource_handler!(movie_credits, db::get_movie_credits, db::MovieCredit);
sub_resource_handler!(movie_images, db::get_movie_images, db::MovieImage);
sub_resource_handler!(movie_videos, db::get_movie_videos, db::MovieVideo);
sub_resource_handler!(movie_reviews, db::get_movie_reviews, db::MovieReview);
sub_resource_handler!(movie_similar, db::get_enriched_related_movies, db::Movie, "similar");
sub_resource_handler!(movie_recommendations, db::get_enriched_related_movies, db::Movie, "recommendation");
sub_resource_handler!(movie_watch_providers, db::get_movie_watch_providers, db::MovieWatchProvider);
sub_resource_handler!(movie_release_dates, db::get_movie_release_dates, db::MovieReleaseDate);

#[derive(Debug, Serialize)]
struct RecentLibraryItem {
    movie: Movie,
    downloading: bool,
}

#[derive(Debug, Serialize)]
struct ItemsResponse<T> {
    items: Vec<T>,
}

#[derive(Debug, Serialize)]
struct MostRelatedResponseItem {
    movie: Movie,
    ref_count: i64,
    downloading: bool,
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct MostRelatedResponse {
    items: Vec<MostRelatedResponseItem>,
}

async fn get_or_refresh_most_related_cache(state: &AppState) -> Vec<CachedMostRelatedItem> {
    // Cheap DB sniff — any bind/unbind/refetch advances this. If unchanged
    // since cache creation AND TTL not elapsed, the cache is still authoritative.
    let current_snapshot = db::dir_movie_mappings_max_updated_at(&state.pool)
        .await
        .ok()
        .flatten();
    {
        let cache = state.most_related_cache.read().await;
        if let Some(ref c) = *cache {
            if c.updated_at.elapsed() < MOST_RELATED_CACHE_TTL
                && c.mappings_snapshot == current_snapshot
            {
                return c.items.clone();
            }
        }
    }
    // Cache miss / TTL expired / mappings changed — refresh
    let rows = db::most_related_out_of_library(&state.pool, 10)
        .await
        .unwrap_or_default();
    let mut items = Vec::with_capacity(rows.len());
    for r in rows {
        let downloading = db::is_movie_downloading(&state.pool, r.movie.id)
            .await
            .unwrap_or(false);
        items.push(CachedMostRelatedItem {
            movie: r.movie,
            ref_count: r.ref_count,
            downloading,
        });
    }
    // Write cache
    {
        let mut cache = state.most_related_cache.write().await;
        *cache = Some(MostRelatedCache {
            items: items.clone(),
            updated_at: Instant::now(),
            mappings_snapshot: current_snapshot,
        });
    }
    items
}

#[derive(Debug, serde::Deserialize)]
struct ReasonEntry {
    tmdb_id: i64,
    reason: String,
}

/// Decide which DB cache row a given user reads/writes. Logged-out users and
/// logged-in users without any 'watched' marks share the anonymous (NULL) row
/// — their prompt has no personalized watch list, so the same tip is valid.
async fn most_related_reasons_cache_key(state: &AppState, user_id: Option<i64>) -> Option<i64> {
    let uid = user_id?;
    let has_watched = db::get_user_marked_movies(&state.pool, uid)
        .await
        .map(|marks| marks.iter().any(|m| m.mark_type == "watched"))
        .unwrap_or(false);
    if has_watched {
        Some(uid)
    } else {
        None
    }
}

/// Read today's reasons from DB cache. Returns the parsed map plus the
/// cache_key used (so the caller can spawn a refresh on the same key without
/// re-deriving it). `Ok(None)` = cache miss or stored JSON unparseable.
async fn lookup_cached_reasons(
    state: &AppState,
    user_id: Option<i64>,
) -> (Option<std::collections::HashMap<i64, String>>, Option<i64>) {
    let cache_key = most_related_reasons_cache_key(state, user_id).await;
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let cached = match db::get_most_related_tip(&state.pool, cache_key, &today).await {
        Ok(Some(s)) => s,
        _ => return (None, cache_key),
    };
    match serde_json::from_str::<Vec<ReasonEntry>>(&cached) {
        Ok(entries) => (
            Some(entries.into_iter().map(|e| (e.tmdb_id, e.reason)).collect()),
            cache_key,
        ),
        Err(_) => (None, cache_key),
    }
}

/// Spawn a background task that calls the LLM and writes the DB cache. The
/// HTTP response does NOT wait for this — the next request after completion
/// will hit the cache. `most_related_reasons_pending` deduplicates concurrent
/// spawns so only one LLM call per cache_key is in flight at a time.
fn spawn_reasons_refresh(
    state: AppState,
    items: Vec<CachedMostRelatedItem>,
    user_id: Option<i64>,
    cache_key: Option<i64>,
) {
    if items.is_empty() {
        return;
    }
    tokio::spawn(async move {
        {
            let mut pending = state.most_related_reasons_pending.lock().await;
            if !pending.insert(cache_key) {
                return;
            }
        }
        if let Err(e) = run_reasons_llm(&state, &items, user_id, cache_key).await {
            tracing::error!("most-related-tip refresh failed: {}", e);
        }
        state.most_related_reasons_pending.lock().await.remove(&cache_key);
    });
}

/// Run the LLM and persist the result. Errors propagate so the caller can log
/// once; nothing here mutates HTTP state.
async fn run_reasons_llm(
    state: &AppState,
    items: &[CachedMostRelatedItem],
    user_id: Option<i64>,
    cache_key: Option<i64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();

    let watched_movies = if let Some(uid) = user_id {
        db::get_user_marked_movies(&state.pool, uid)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|m| m.mark_type == "watched")
            .collect::<Vec<_>>()
    } else {
        vec![]
    };

    let locale = crate::api::recommend::get_locale(&state.pool).await;
    let stats = db::get_library_stats(&state.pool).await?;
    let summary = crate::api::recommend::build_library_summary_public(&stats, &locale);

    let related_list = items
        .iter()
        .map(|item| {
            let title = &item.movie.title;
            let tmdb_id = item.movie.tmdb_id;
            let year = item.movie.year.map(|y| format!(" ({})", y)).unwrap_or_default();
            let genres = item.movie.genres.as_deref().unwrap_or("[]");
            format!(
                "- [tmdb_id={}] {}{} [关联 {} 部库内电影] genres={}",
                tmdb_id, title, year, item.ref_count, genres
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let watched_section = if watched_movies.is_empty() {
        if locale == "en" {
            "User is not logged in or has no watch history.".to_string()
        } else {
            "用户未登录或没有观影记录。".to_string()
        }
    } else {
        let watched_list = watched_movies
            .iter()
            .take(20)
            .map(|m| {
                let year = m.year.map(|y| format!(" ({})", y)).unwrap_or_default();
                format!("- {}{}", m.title, year)
            })
            .collect::<Vec<_>>()
            .join("\n");
        if locale == "en" {
            format!("Movies the user has watched:\n{}", watched_list)
        } else {
            format!("用户看过的电影：\n{}", watched_list)
        }
    };

    let template = crate::api::recommend::load_prompt_public(&state.pool, "most-related-tip", &locale).await;
    let prompt = crate::api::recommend::render_prompt_public(&template, &[
        ("total", &stats.total.to_string()),
        ("genres", &summary.genres),
        ("countries", &summary.countries),
        ("decades", &summary.decades),
        ("related_movies", &related_list),
        ("watched_section", &watched_section),
    ]);

    let user_msg = if locale == "en" {
        "Generate recommendations."
    } else {
        "请生成推荐语。"
    };

    let result = state.llm.chat(&prompt, user_msg).await?;
    let text = result.trim();
    let json_str = text
        .strip_prefix("```json").or_else(|| text.strip_prefix("```"))
        .and_then(|s| s.strip_suffix("```"))
        .map(|s| s.trim())
        .unwrap_or(text);

    let _: Vec<ReasonEntry> = serde_json::from_str(json_str).map_err(|e| {
        tracing::error!("most-related-tip JSON parse failed: {} | raw: {}", e, text);
        e
    })?;

    db::save_most_related_tip(&state.pool, cache_key, &today, json_str).await?;
    Ok(())
}

async fn most_related_out_of_library(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
) -> Json<MostRelatedResponse> {
    let items = get_or_refresh_most_related_cache(&state).await;
    let user_id = user.map(|u| u.id);
    let (reasons_opt, cache_key) = lookup_cached_reasons(&state, user_id).await;
    let reasons = match reasons_opt {
        Some(r) => r,
        None => {
            // Cache miss: spawn LLM in the background and answer NOW with
            // empty reasons. The next request after the spawned task finishes
            // will hit the DB cache. ~13s LLM latency no longer blocks
            // the homepage's 库外热门 section from rendering.
            spawn_reasons_refresh(state.clone(), items.clone(), user_id, cache_key);
            std::collections::HashMap::new()
        }
    };
    let response_items = items
        .into_iter()
        .map(|item| {
            let reason = reasons.get(&item.movie.tmdb_id).cloned();
            MostRelatedResponseItem {
                movie: item.movie,
                ref_count: item.ref_count,
                downloading: item.downloading,
                reason,
            }
        })
        .collect();
    Json(MostRelatedResponse { items: response_items })
}

async fn recent_library_movies(
    State(state): State<AppState>,
) -> Json<ItemsResponse<RecentLibraryItem>> {
    let movies = db::recent_library_movies(&state.pool, 5).await.unwrap_or_default();
    let mut items = Vec::with_capacity(movies.len());
    for movie in movies {
        let downloading = db::is_movie_downloading(&state.pool, movie.id)
            .await
            .unwrap_or(false);
        items.push(RecentLibraryItem { movie, downloading });
    }
    Json(ItemsResponse { items })
}

async fn library_stats(
    State(state): State<AppState>,
) -> Json<db::queries::LibraryStats> {
    let stats = match db::get_library_stats(&state.pool).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("library_stats error: {e}");
            db::queries::LibraryStats {
                total: 0, library_total: 0, sample_movies: vec![],
                decades: vec![], genres: vec![], countries: vec![], directors: vec![],
                cast: vec![], keywords: vec![], rating_tiers: vec![], budget_tiers: vec![],
            }
        }
    };
    Json(stats)
}

async fn filters(
    State(state): State<AppState>,
) -> Json<db::queries::FilterOptions> {
    let opts = db::get_filter_options(&state.pool).await.unwrap_or(db::queries::FilterOptions {
        decades: vec![],
        genres: vec![],
        countries: vec![],
        languages: vec![],
        ratings: vec![],
        runtimes: vec![],
    });
    Json(opts)
}

fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

// ===== AI Insight for movie detail page =====

/// Cache invalidates when watched count grows by >= max(current * 10%, 10).
fn watched_refresh_threshold(current_watched: i64) -> i64 {
    (current_watched / 10).max(10)
}

#[derive(Debug, Serialize)]
struct AiInsightResponse {
    verdict: Option<String>,
    picks: Vec<AiInsightPick>,
}

#[derive(Debug, Serialize)]
struct AiInsightPick {
    movie: Movie,
    reason: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct InsightLlmOutput {
    verdict_zh: Option<String>,
    verdict_en: Option<String>,
    #[serde(default)]
    picks: Vec<InsightPickEntry>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct InsightPickEntry {
    tmdb_id: i64,
    reason_zh: String,
    reason_en: String,
}

async fn movie_ai_insight(
    State(state): State<AppState>,
    Path(movie_id): Path<i64>,
    OptionalUser(user): OptionalUser,
) -> Result<Json<AiInsightResponse>, StatusCode> {
    let movie = db::get_movie_by_id(&state.pool, movie_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let user_id = user.map(|u| u.id);
    let locale = crate::api::recommend::get_locale(&state.pool).await;

    // Get current watched count for cache invalidation
    let current_watched = if let Some(uid) = user_id {
        db::count_user_watched(&state.pool, uid).await.unwrap_or(0)
    } else {
        0
    };

    // Check cache (stores bilingual JSON, locale selects at read time)
    let cache_key = user_id; // None for anonymous
    if let Ok(Some((cached_json, cached_watched))) =
        db::get_movie_ai_insight(&state.pool, cache_key, movie_id).await
    {
        if (current_watched - cached_watched).abs() < watched_refresh_threshold(current_watched) {
            if let Ok(parsed) = serde_json::from_str::<InsightLlmOutput>(&cached_json) {
                let picks = resolve_picks(&state.pool, &parsed.picks, &locale).await;
                return Ok(Json(AiInsightResponse {
                    verdict: pick_locale_str(&parsed.verdict_zh, &parsed.verdict_en, &locale),
                    picks,
                }));
            }
        }
    }

    // Fetch candidates: merge similar + recommendations, deduplicate
    let similar = db::get_enriched_related_movies(&state.pool, movie_id, "similar")
        .await
        .unwrap_or_default();
    let recs = db::get_enriched_related_movies(&state.pool, movie_id, "recommendation")
        .await
        .unwrap_or_default();

    let mut seen = std::collections::HashSet::new();
    let mut candidates: Vec<Movie> = Vec::new();
    for m in similar.into_iter().chain(recs.into_iter()) {
        if seen.insert(m.tmdb_id) {
            candidates.push(m);
        }
    }

    if candidates.is_empty() {
        return Ok(Json(AiInsightResponse {
            verdict: None,
            picks: vec![],
        }));
    }

    // Get user's watched movies (if logged in)
    let watched_movies = if let Some(uid) = user_id {
        db::get_user_marked_movies(&state.pool, uid)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|m| m.mark_type == "watched")
            .take(30)
            .collect::<Vec<_>>()
    } else {
        vec![]
    };

    // Build prompt

    let current_movie_desc = format!(
        "{} ({}) | {} | {} | {}",
        movie.title,
        movie.year.map(|y| y.to_string()).unwrap_or_default(),
        movie.genres.as_deref().unwrap_or(""),
        movie.director.as_deref().unwrap_or(""),
        movie.overview.as_deref().unwrap_or("").chars().take(200).collect::<String>(),
    );

    let candidates_desc = candidates
        .iter()
        .map(|m| {
            format!(
                "- [tmdb_id={}] {} ({}) | {} | {}",
                m.tmdb_id,
                m.title,
                m.year.map(|y| y.to_string()).unwrap_or_default(),
                m.genres.as_deref().unwrap_or(""),
                m.director.as_deref().unwrap_or(""),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let watched_section = if watched_movies.is_empty() {
        if locale == "en" {
            "No user watch history available.".to_string()
        } else {
            "没有用户观影记录。".to_string()
        }
    } else {
        let list = watched_movies
            .iter()
            .map(|m| {
                format!(
                    "- {} ({}) | {} | {}",
                    m.title,
                    m.year.map(|y| y.to_string()).unwrap_or_default(),
                    m.genres.as_deref().unwrap_or(""),
                    m.director.as_deref().unwrap_or(""),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        if locale == "en" {
            format!("## User's Watch History (recent 30)\n{}", list)
        } else {
            format!("## 用户观影记录（最近 30 部）\n{}", list)
        }
    };

    let template =
        crate::api::recommend::load_prompt_public(&state.pool, "movie-insight", &locale).await;
    let prompt = crate::api::recommend::render_prompt_public(&template, &[
        ("current_movie", &current_movie_desc),
        ("candidates", &candidates_desc),
        ("watched_section", &watched_section),
    ]);

    let user_msg = "请生成推荐，中英双语输出。";

    match state.llm.chat(&prompt, user_msg).await {
        Ok(result) => {
            let text = result.trim();
            let json_str = text
                .strip_prefix("```json")
                .or_else(|| text.strip_prefix("```"))
                .and_then(|s| s.strip_suffix("```"))
                .map(|s| s.trim())
                .unwrap_or(text);

            match serde_json::from_str::<InsightLlmOutput>(json_str) {
                Ok(parsed) => {
                    // Cache the bilingual JSON
                    let _ = db::save_movie_ai_insight(
                        &state.pool,
                        cache_key,
                        movie_id,
                        json_str,
                        current_watched,
                    )
                    .await;

                    let picks = resolve_picks(&state.pool, &parsed.picks, &locale).await;
                    Ok(Json(AiInsightResponse {
                        verdict: pick_locale_str(&parsed.verdict_zh, &parsed.verdict_en, &locale),
                        picks,
                    }))
                }
                Err(e) => {
                    tracing::error!("movie-insight JSON parse failed: {} | raw: {}", e, text);
                    Ok(Json(AiInsightResponse {
                        verdict: None,
                        picks: vec![],
                    }))
                }
            }
        }
        Err(e) => {
            tracing::error!("movie-insight LLM call failed: {}", e);
            Ok(Json(AiInsightResponse {
                verdict: None,
                picks: vec![],
            }))
        }
    }
}

fn pick_locale_str(zh: &Option<String>, en: &Option<String>, locale: &str) -> Option<String> {
    if locale == "en" {
        en.clone().or_else(|| zh.clone())
    } else {
        zh.clone().or_else(|| en.clone())
    }
}

/// Resolve tmdb_id pick entries to full Movie objects, skipping any not found.
async fn resolve_picks(pool: &db::SqlitePool, entries: &[InsightPickEntry], locale: &str) -> Vec<AiInsightPick> {
    let mut picks = Vec::new();
    for entry in entries {
        if let Ok(Some(movie)) = db::get_movie_by_tmdb_id(pool, entry.tmdb_id).await {
            let reason = if locale == "en" {
                entry.reason_en.clone()
            } else {
                entry.reason_zh.clone()
            };
            picks.push(AiInsightPick { movie, reason });
        }
    }
    picks
}

#[cfg(test)]
mod tests {
    use super::get_or_refresh_most_related_cache;
    use crate::test_support::{get_json, test_app};
    use axum::http::StatusCode;
    use sqlx::SqlitePool;

    /// Insert a movie with a matched dir so it appears in list_movies.
    async fn seed_visible_movie(
        pool: &SqlitePool,
        tmdb_id: i64,
        title: &str,
        year: Option<i64>,
        genres: &str,
    ) -> i64 {
        let movie_id = sqlx::query(
            "INSERT INTO movies (tmdb_id, title, year, genres) VALUES (?, ?, ?, ?)",
        )
        .bind(tmdb_id)
        .bind(title)
        .bind(year)
        .bind(genres)
        .execute(pool)
        .await
        .unwrap()
        .last_insert_rowid();

        let dir_path = format!("/movies/{}", title);
        let dir_id = crate::db::insert_media_dir(pool, &dir_path, title)
            .await
            .unwrap();
        crate::db::update_dir_status(pool, dir_id, "matched")
            .await
            .unwrap();
        crate::db::insert_mapping(pool, dir_id, Some(movie_id), "auto", Some(0.95), None)
            .await
            .unwrap();

        movie_id
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn list_movies_returns_paginated(pool: SqlitePool) {
        seed_visible_movie(&pool, 1, "Alpha", Some(2020), "[]").await;
        seed_visible_movie(&pool, 2, "Beta", Some(2021), "[]").await;
        seed_visible_movie(&pool, 3, "Gamma", Some(2022), "[]").await;

        let (status, body) = get_json(
            test_app(pool),
            "/api/movies?page=1&per_page=2",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"].as_i64().unwrap(), 3);
        assert_eq!(body["data"].as_array().unwrap().len(), 2);
        assert_eq!(body["per_page"], 2);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn list_movies_search_filter(pool: SqlitePool) {
        seed_visible_movie(&pool, 1, "Inception", Some(2010), "[]").await;
        seed_visible_movie(&pool, 2, "Interstellar", Some(2014), "[]").await;
        seed_visible_movie(&pool, 3, "Parasite", Some(2019), "[]").await;

        let (status, body) = get_json(
            test_app(pool),
            "/api/movies?search=Inter",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"].as_i64().unwrap(), 1);
        assert_eq!(body["data"][0]["title"], "Interstellar");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_movie_detail_and_404(pool: SqlitePool) {
        let movie_id = seed_visible_movie(&pool, 1, "Inception", Some(2010), "[]").await;

        let (status, body) = get_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}", movie_id),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["title"], "Inception");
        assert_eq!(body["year"], 2010);
        assert!(body["dir_paths"].is_array());

        // 404 for non-existent.
        let (status, _) = get_json(test_app(pool), "/api/movies/99999", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn library_stats_and_filters_return_ok(pool: SqlitePool) {
        seed_visible_movie(&pool, 1, "A", Some(2020), "[\"Drama\"]").await;

        let (s1, b1) = get_json(test_app(pool.clone()), "/api/movies/stats", None).await;
        assert_eq!(s1, StatusCode::OK);
        assert!(b1["total"].as_i64().unwrap() >= 1);

        let (s2, _) = get_json(test_app(pool.clone()), "/api/movies/filters", None).await;
        assert_eq!(s2, StatusCode::OK);

        let (s3, b3) = get_json(test_app(pool), "/api/movies/status-counts", None).await;
        assert_eq!(s3, StatusCode::OK);
        assert!(b3["auto"].as_i64().unwrap() >= 1);
        assert!(b3["library_total"].as_i64().unwrap() >= 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn empty_library_returns_zero(pool: SqlitePool) {
        let (status, body) = get_json(test_app(pool), "/api/movies", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"], 0);
        assert_eq!(body["data"].as_array().unwrap().len(), 0);
    }

    // ---- locate (反向定位) ----

    use crate::test_support::post_json;
    use serde_json::json;

    async fn register_admin(pool: &SqlitePool) -> String {
        let (_, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": "admin", "password": "pw" }),
            None,
        )
        .await;
        body["token"].as_str().unwrap().to_string()
    }

    /// 插一个 movie（不绑任何 dir），返回 movie_id。
    async fn seed_movie_no_dir(
        pool: &SqlitePool,
        tmdb_id: i64,
        title: &str,
        year: Option<i64>,
    ) -> i64 {
        sqlx::query("INSERT INTO movies (tmdb_id, title, year) VALUES (?, ?, ?)")
            .bind(tmdb_id)
            .bind(title)
            .bind(year)
            .execute(pool)
            .await
            .unwrap()
            .last_insert_rowid()
    }

    /// 插一个 media_dirs 行 + 可选 mapping。`mapping_status = None` → 不插 mapping
    async fn seed_dir(
        pool: &SqlitePool,
        path: &str,
        name: &str,
        mapping_status: Option<&str>,
    ) -> i64 {
        let dir_id = crate::db::insert_media_dir(pool, path, name).await.unwrap();
        crate::db::update_dir_status(pool, dir_id, "parsed").await.unwrap();
        if let Some(status) = mapping_status {
            crate::db::insert_mapping(pool, dir_id, None, status, Some(0.6), None)
                .await
                .unwrap();
        }
        dir_id
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn locate_finds_unbound_dir_by_title_year(pool: SqlitePool) {
        let token = register_admin(&pool).await;
        let movie_id = seed_movie_no_dir(&pool, 27205, "Inception", Some(2010)).await;

        // 三个目录：一个名字接近且 pending、一个名字不接近、一个已 auto-bind
        seed_dir(&pool, "/m/Inception.2010.1080p", "Inception.2010.1080p.BluRay.x264-CtrlHD", Some("pending")).await;
        seed_dir(&pool, "/m/Frozen.2013", "Frozen.2013.1080p", Some("failed")).await;

        // 已绑定的目录——应该不出现在结果里
        let other_movie_id = seed_movie_no_dir(&pool, 99999, "OtherMovie", Some(2000)).await;
        let bound_dir = crate::db::insert_media_dir(&pool, "/m/Other.Inception.2010", "Inception.2010.Bound").await.unwrap();
        crate::db::insert_mapping(&pool, bound_dir, Some(other_movie_id), "auto", Some(0.95), None).await.unwrap();

        let (status, body) = post_json(
            test_app(pool),
            &format!("/api/movies/{}/locate", movie_id),
            &json!({}),
            Some(&token),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let candidates = body["candidates"].as_array().unwrap();
        // 标题接近的应该命中，Frozen 那个 score < 0.5 被滤掉
        assert!(!candidates.is_empty());
        assert_eq!(candidates[0]["dir_path"], "/m/Inception.2010.1080p");
        assert_eq!(candidates[0]["status"], "pending");
        assert_eq!(candidates[0]["parsed_year"], 2010);
        let score = candidates[0]["score"].as_f64().unwrap();
        assert!(score >= 0.5, "score should pass threshold, got {}", score);
        // 已绑定的不出现
        for c in candidates {
            assert_ne!(c["dir_path"], "/m/Other.Inception.2010");
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn locate_returns_empty_when_no_match(pool: SqlitePool) {
        let token = register_admin(&pool).await;
        let movie_id = seed_movie_no_dir(&pool, 1, "An Unrelated Movie", Some(2010)).await;
        seed_dir(&pool, "/m/Frozen.2013", "Frozen.2013.1080p", Some("pending")).await;

        let (status, body) = post_json(
            test_app(pool),
            &format!("/api/movies/{}/locate", movie_id),
            &json!({}),
            Some(&token),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["candidates"].as_array().unwrap().len(), 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn locate_404_for_unknown_movie(pool: SqlitePool) {
        let token = register_admin(&pool).await;
        let (status, _) = post_json(
            test_app(pool),
            "/api/movies/99999/locate",
            &json!({}),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn locate_requires_auth(pool: SqlitePool) {
        let movie_id = seed_movie_no_dir(&pool, 1, "Inception", Some(2010)).await;
        let (status, _) = post_json(
            test_app(pool),
            &format!("/api/movies/{}/locate", movie_id),
            &json!({}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    // ---- most-related cache 自动失效 ----

    /// 给两个 movie + 一条 seed → external 的 related 关系，让 external 出现在
    /// "库外发现"。返回 (seed_movie_id, external_movie_id, external_tmdb_id)。
    async fn seed_external_with_relation(pool: &SqlitePool) -> (i64, i64, i64) {
        let seed_id = sqlx::query(
            "INSERT INTO movies (tmdb_id, title, year, source) VALUES (?, ?, ?, 'library')",
        )
        .bind(1_i64)
        .bind("seed")
        .bind(2000_i64)
        .execute(pool)
        .await
        .unwrap()
        .last_insert_rowid();
        let ext_tmdb = 42_i64;
        let ext_id = sqlx::query(
            "INSERT INTO movies (tmdb_id, title, year, source) VALUES (?, ?, ?, 'related')",
        )
        .bind(ext_tmdb)
        .bind("Spirited Away")
        .bind(2001_i64)
        .execute(pool)
        .await
        .unwrap()
        .last_insert_rowid();
        sqlx::query(
            "INSERT INTO related_movies (movie_id, related_tmdb_id, relation_type) \
             VALUES (?, ?, 'similar')",
        )
        .bind(seed_id)
        .bind(ext_tmdb)
        .execute(pool)
        .await
        .unwrap();
        (seed_id, ext_id, ext_tmdb)
    }

    /// 回归 bug：通过 locate-movie 绑定的电影，首页"库外发现"仍展示。
    /// 根因是 most_related_cache TTL 1h 且无任何 mutation 路径触发失效。
    /// 修复：cache 命中前查 dir_movie_mappings.MAX(updated_at) 与 cache
    /// 创建时的快照比对——任何 bind/unbind/refetch 都会推进该值。
    #[sqlx::test(migrations = "./migrations")]
    async fn most_related_cache_invalidates_on_bind(pool: SqlitePool) {
        let (_seed_id, ext_id, ext_tmdb) = seed_external_with_relation(&pool).await;
        let state = crate::test_support::test_state(pool.clone());

        // 第一次：cache miss → 应包含 ext_tmdb
        let items1 = get_or_refresh_most_related_cache(&state).await;
        assert!(items1.iter().any(|i| i.movie.tmdb_id == ext_tmdb));

        // 用户用 locate-movie + bind 把这部绑给一个目录
        let dir_id = crate::db::insert_media_dir(&pool, "/m/spirited", "Spirited Away (2001)")
            .await
            .unwrap();
        crate::db::insert_mapping(&pool, dir_id, Some(ext_id), "manual", Some(1.0), None)
            .await
            .unwrap();
        // SQLite datetime('now') 是秒级精度——测试可能在 cache 创建同一秒内完成 bind，
        // 导致 snapshot 字符串相等。把 updated_at 推到 +5s 确保差异稳定可见
        sqlx::query("UPDATE dir_movie_mappings SET updated_at = datetime('now', '+5 seconds')")
            .execute(&pool)
            .await
            .unwrap();

        // 第二次：DB snapshot 已变 → cache 应自动失效，结果不再含 ext_tmdb
        let items2 = get_or_refresh_most_related_cache(&state).await;
        assert!(
            !items2.iter().any(|i| i.movie.tmdb_id == ext_tmdb),
            "cache should auto-invalidate after bind; got {:?}",
            items2.iter().map(|i| i.movie.tmdb_id).collect::<Vec<_>>()
        );
    }

    /// 反向场景：mappings 没变化 → cache 命中应返回原数据，不重算。
    #[sqlx::test(migrations = "./migrations")]
    async fn most_related_cache_hits_when_mappings_unchanged(pool: SqlitePool) {
        let (_seed_id, _ext_id, ext_tmdb) = seed_external_with_relation(&pool).await;
        let state = crate::test_support::test_state(pool.clone());

        let items1 = get_or_refresh_most_related_cache(&state).await;
        assert!(items1.iter().any(|i| i.movie.tmdb_id == ext_tmdb));
        let cached_at = state
            .most_related_cache
            .read()
            .await
            .as_ref()
            .map(|c| c.updated_at);

        // 第二次：DB 没动过 → 应返回 cache（updated_at 不变）
        let items2 = get_or_refresh_most_related_cache(&state).await;
        assert!(items2.iter().any(|i| i.movie.tmdb_id == ext_tmdb));
        let cached_at_2 = state
            .most_related_cache
            .read()
            .await
            .as_ref()
            .map(|c| c.updated_at);
        assert_eq!(cached_at, cached_at_2, "cache should not have been refreshed");
    }

    // ---- most-related reasons: async cache (no LLM blocking the response) ----

    /// DB cache hit returns the parsed reasons; HTTP path consumes them inline.
    #[sqlx::test(migrations = "./migrations")]
    async fn most_related_reasons_returns_cached_when_present(pool: SqlitePool) {
        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        let tip_json = r#"[{"tmdb_id":42,"reason":"经典"},{"tmdb_id":7,"reason":"必看"}]"#;
        crate::db::save_most_related_tip(&pool, None, &today, tip_json)
            .await
            .unwrap();

        let state = crate::test_support::test_state(pool.clone());
        let (reasons_opt, cache_key) = super::lookup_cached_reasons(&state, None).await;
        assert_eq!(cache_key, None);
        let reasons = reasons_opt.expect("cache hit must yield Some(map)");
        assert_eq!(reasons.get(&42).map(String::as_str), Some("经典"));
        assert_eq!(reasons.get(&7).map(String::as_str), Some("必看"));
    }

    /// DB cache miss → returns None; the HTTP handler will spawn the refresh.
    #[sqlx::test(migrations = "./migrations")]
    async fn most_related_reasons_misses_when_cache_absent(pool: SqlitePool) {
        let state = crate::test_support::test_state(pool);
        let (reasons_opt, cache_key) = super::lookup_cached_reasons(&state, None).await;
        assert!(reasons_opt.is_none(), "empty DB → cache miss");
        assert_eq!(cache_key, None);
    }

    /// Regression: handler responds immediately on DB cache miss. Pre-fix the
    /// LLM call ran inline (~13s blocking). Now miss returns reason=None and
    /// the LLM call is spawned. We assert (a) the response arrives well under
    /// the previous LLM latency, and (b) every item's reason is None.
    #[sqlx::test(migrations = "./migrations")]
    async fn most_related_handler_does_not_block_on_cache_miss(pool: SqlitePool) {
        let (_seed_id, _ext_id, ext_tmdb) = seed_external_with_relation(&pool).await;
        let started = std::time::Instant::now();
        let (status, body) = get_json(test_app(pool), "/api/movies/most-related", None).await;
        let elapsed = started.elapsed();
        assert_eq!(status, StatusCode::OK);
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "handler must not wait for LLM; took {:?}",
            elapsed
        );
        let items = body["items"].as_array().expect("items array");
        assert!(items.iter().any(|i| i["movie"]["tmdb_id"] == ext_tmdb));
        for item in items {
            assert!(
                item["reason"].is_null(),
                "cache miss must yield reason=null, got {:?}",
                item["reason"]
            );
        }
    }

    /// Cache-hit path of the same handler: when the DB has today's tip, the
    /// response carries reasons inline and no spawn is needed.
    #[sqlx::test(migrations = "./migrations")]
    async fn most_related_handler_returns_reasons_when_cache_hits(pool: SqlitePool) {
        let (_seed_id, _ext_id, ext_tmdb) = seed_external_with_relation(&pool).await;
        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        let tip_json = format!(r#"[{{"tmdb_id":{ext_tmdb},"reason":"测试推荐语"}}]"#);
        crate::db::save_most_related_tip(&pool, None, &today, &tip_json)
            .await
            .unwrap();

        let (status, body) = get_json(test_app(pool), "/api/movies/most-related", None).await;
        assert_eq!(status, StatusCode::OK);
        let items = body["items"].as_array().expect("items array");
        let target = items
            .iter()
            .find(|i| i["movie"]["tmdb_id"] == ext_tmdb)
            .expect("seeded external movie");
        assert_eq!(target["reason"].as_str(), Some("测试推荐语"));
    }
}
