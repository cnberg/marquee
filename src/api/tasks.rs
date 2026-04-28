use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post},
    routing::put,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::api::common::PageParams;
use crate::api::AppState;
use crate::auth::RequireUser;
use crate::config::Config;
use crate::db;
use crate::worker::scheduler::run_scan_cycle;

#[derive(Debug, Serialize)]
pub struct ScanResponse {
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub tasks: HashMap<String, HashMap<String, i64>>,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/admin/scan", post(trigger_scan))
        .route("/admin/refetch-all", post(refetch_all))
        .route("/admin/rematch-pending", post(rematch_pending))
        .route("/admin/status", get(admin_status))
        .route("/admin/overview", get(admin_overview))
        .route("/admin/settings", get(get_settings))
        .route("/admin/settings", put(update_settings))
        .route("/admin/llm-logs", get(list_llm_logs))
        .route("/admin/llm-logs/{filename}", get(get_llm_log))
        .route("/admin/failed-tasks", get(list_failed_tasks_handler))
        .route("/admin/prompts", get(list_prompts))
        .route("/admin/prompts/{name}", put(update_prompt))
        .route("/admin/prompts/{name}", delete(reset_prompt))
        .route("/admin/config", get(get_config))
        .route("/admin/config", put(update_config))
        .route("/admin/multi-version", get(list_multi_version))
}

async fn trigger_scan(
    _user: RequireUser,
    State(state): State<AppState>,
) -> Result<Json<ScanResponse>, (StatusCode, String)> {
    let pool = state.pool.clone();
    let config = state.config.read().await.clone();

    tokio::spawn(async move {
        if let Err(err) = run_scan_cycle(&pool, &config).await {
            tracing::error!("manual scan cycle failed: {}", err);
        }
    });

    Ok(Json(ScanResponse {
        message: "scan started".to_string(),
    }))
}

async fn refetch_all(
    _user: RequireUser,
    State(state): State<AppState>,
) -> Result<Json<ScanResponse>, (StatusCode, String)> {
    let pool = state.pool.clone();

    tokio::spawn(async move {
        let movies = sqlx::query_as::<_, (i64, i64)>(
            "SELECT id, tmdb_id FROM movies WHERE imdb_id IS NULL AND tmdb_id > 0",
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        let mut created = 0;
        for (movie_id, tmdb_id) in movies {
            let existing = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM tasks WHERE task_type = 'tmdb_fetch' AND status IN ('pending', 'running') AND payload LIKE ?",
            )
            .bind(format!("%\"tmdb_id\":{}%", tmdb_id))
            .fetch_one(&pool)
            .await
            .unwrap_or(0);

            if existing == 0 {
                let source = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT source FROM movies WHERE id = ?",
                )
                .bind(movie_id)
                .fetch_one(&pool)
                .await
                .unwrap_or(Some("library".to_string()));

                let fetch_related = source.as_deref() != Some("related");
                let payload = serde_json::json!({
                    "tmdb_id": tmdb_id,
                    "movie_id": movie_id,
                    "fetch_related": fetch_related,
                });
                let _ = db::insert_task(&pool, "tmdb_fetch", &payload.to_string()).await;
                created += 1;
            }
        }

        tracing::info!(count = created, "created refetch tasks for existing movies");
    });

    Ok(Json(ScanResponse {
        message: "refetch-all started".to_string(),
    }))
}

#[derive(Debug, Serialize)]
struct RematchResponse {
    rematched: i64,
}

/// Re-run TMDB matching for every dir currently sitting in `failed` or
/// `pending`. The scanner won't re-enqueue `tmdb_search` for already-mapped
/// dirs (by design, to avoid re-hammering TMDB on every scan), so when
/// downstream logic improves — matcher scoring, TMDB search fallback, parser
/// — existing stale mappings stay stale unless something explicitly resets
/// them. This is that reset.
///
/// For each failed/pending dir: re-parse the dir name with the current
/// parser, delete the old mapping, reset `media_dirs.scan_status` to 'parsed',
/// and enqueue a fresh `tmdb_search` task. The standing worker picks them up
/// at the regular TMDB rate limit (~4 req/s × 3-6 calls per dir = ~10 minutes
/// for 750 records).
async fn rematch_pending(
    _user: RequireUser,
    State(state): State<AppState>,
) -> Result<Json<RematchResponse>, (StatusCode, String)> {
    let pool = state.pool.clone();

    let dirs: Vec<(i64, String)> = sqlx::query_as(
        "SELECT md.id, md.dir_name
         FROM media_dirs md
         JOIN dir_movie_mappings dm ON dm.dir_id = md.id
         WHERE dm.match_status IN ('failed', 'pending')",
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let count = dirs.len() as i64;

    for (dir_id, dir_name) in dirs {
        let parsed = crate::scanner::parser::parse_directory_name(&dir_name);

        if let Err(err) = sqlx::query("DELETE FROM dir_movie_mappings WHERE dir_id = ?")
            .bind(dir_id)
            .execute(&pool)
            .await
        {
            tracing::warn!(dir_id, error = %err, "rematch: delete mapping failed, skipping");
            continue;
        }

        if let Err(err) = db::update_dir_status(&pool, dir_id, "parsed").await {
            tracing::warn!(dir_id, error = %err, "rematch: update_dir_status failed");
        }

        let payload = serde_json::json!({
            "dir_id": dir_id,
            "title": parsed.title,
            "alt_title": parsed.alt_title,
            "year": parsed.year,
        });
        if let Err(err) = db::insert_task(&pool, "tmdb_search", &payload.to_string()).await {
            tracing::warn!(dir_id, error = %err, "rematch: insert_task failed");
        }
    }

    Ok(Json(RematchResponse { rematched: count }))
}

async fn admin_status(
    _user: RequireUser,
    State(state): State<AppState>,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    let counts = db::get_task_counts(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut tasks: HashMap<String, HashMap<String, i64>> = HashMap::new();
    for (task_type, status, count) in counts {
        tasks
            .entry(task_type)
            .or_default()
            .insert(status, count);
    }

    Ok(Json(StatusResponse { tasks }))
}

#[derive(Debug, Serialize)]
struct OverviewResponse {
    dir_total: i64,
    dir_status: Vec<(String, i64)>,
    match_status: Vec<(String, i64)>,
    movies_by_source: Vec<(String, i64)>,
    tasks: HashMap<String, HashMap<String, i64>>,
    year_buckets: Vec<(String, i64)>,
    country_top: Vec<(String, i64)>,
    genre_top: Vec<(String, i64)>,
    rating_histogram: Vec<(String, i64)>,
    mark_counts: HashMap<String, i64>,
}

async fn admin_overview(
    _user: RequireUser,
    State(state): State<AppState>,
) -> Result<Json<OverviewResponse>, (StatusCode, String)> {
    let dir_total = db::get_dir_total(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let dir_status = db::get_dir_status_counts(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let match_status = db::get_match_status_counts(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let movies_by_source = db::get_movies_source_counts(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let task_counts = db::get_task_counts(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let year_buckets = db::get_library_year_buckets(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let country_top = db::get_library_country_top(&state.pool, 10)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let genre_top = db::get_library_genre_top(&state.pool, 10)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let rating_histogram = db::get_library_rating_histogram(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let mark_rows = db::get_mark_counts(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut tasks: HashMap<String, HashMap<String, i64>> = HashMap::new();
    for (task_type, status, count) in task_counts {
        tasks.entry(task_type).or_default().insert(status, count);
    }

    let mark_counts: HashMap<String, i64> = mark_rows.into_iter().collect();

    Ok(Json(OverviewResponse {
        dir_total,
        dir_status,
        match_status,
        movies_by_source,
        tasks,
        year_buckets,
        country_top,
        genre_top,
        rating_histogram,
        mark_counts,
    }))
}

#[derive(Debug, Deserialize)]
struct UpdateSettingsBody {
    locale: Option<String>,
}

async fn get_settings(
    _user: RequireUser,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let locale = db::get_setting(&state.pool, "locale")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .unwrap_or_else(|| "en".to_string());
    Ok(Json(serde_json::json!({ "locale": locale })))
}

async fn update_settings(
    _user: RequireUser,
    State(state): State<AppState>,
    Json(body): Json<UpdateSettingsBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if let Some(locale) = &body.locale {
        if locale != "en" && locale != "zh" {
            return Err((StatusCode::BAD_REQUEST, "locale must be 'en' or 'zh'".to_string()));
        }
        db::set_setting(&state.pool, "locale", locale)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    Ok(Json(serde_json::json!({ "message": "updated" })))
}

async fn get_config(
    _user: RequireUser,
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    let masked = state.config.read().await.masked();
    Json(serde_json::to_value(&masked).unwrap_or_default())
}

async fn update_config(
    _user: RequireUser,
    State(state): State<AppState>,
    Json(mut incoming): Json<Config>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    {
        let current = state.config.read().await;
        current.merge_sensitive(&mut incoming);
    }
    incoming.save().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    *state.config.write().await = incoming;
    Ok(Json(serde_json::json!({ "message": "config_save_restart_hint" })))
}

#[derive(Debug, Serialize)]
struct LlmLogEntry {
    filename: String,
    size: u64,
    modified: String,
}

async fn list_llm_logs(
    _user: RequireUser,
) -> Result<Json<Vec<LlmLogEntry>>, (StatusCode, String)> {
    let log_dir = std::path::Path::new(crate::llm::client::LLM_LOGS_DIR);
    if !log_dir.exists() {
        return Ok(Json(vec![]));
    }

    let mut entries = Vec::new();
    let read_dir = std::fs::read_dir(log_dir)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    for entry in read_dir {
        let entry = entry.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let metadata = entry.metadata().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        if metadata.is_file() {
            let filename = entry.file_name().to_string_lossy().to_string();
            let modified = metadata.modified()
                .ok()
                .and_then(|t| {
                    let dt: chrono::DateTime<chrono::Local> = t.into();
                    Some(dt.format("%Y-%m-%d %H:%M:%S").to_string())
                })
                .unwrap_or_default();
            entries.push(LlmLogEntry {
                filename,
                size: metadata.len(),
                modified,
            });
        }
    }

    entries.sort_by(|a, b| b.filename.cmp(&a.filename));
    entries.truncate(100);

    Ok(Json(entries))
}

async fn get_llm_log(
    _user: RequireUser,
    Path(filename): Path<String>,
) -> Result<String, (StatusCode, String)> {
    if filename.contains("..") || filename.contains('/') || filename.contains('\\') {
        return Err((StatusCode::BAD_REQUEST, "invalid filename".to_string()));
    }
    let path = std::path::Path::new(crate::llm::client::LLM_LOGS_DIR).join(&filename);
    std::fs::read_to_string(&path)
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))
}


async fn list_failed_tasks_handler(
    _user: RequireUser,
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let (tasks, total) = db::list_failed_tasks(&state.pool, params.page, params.per_page)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "data": tasks,
        "total": total,
        "page": params.page.max(1),
        "per_page": params.per_page.max(1),
    })))
}

#[derive(Debug, Serialize)]
struct PromptInfo {
    name: String,
    content: String,
    default_content: String,
    locale: String,  // The locale used to select the md file (zh or en)
    overridden: bool,
}

const PROMPT_NAMES: &[&str] = &[
    "recommend-filter",
    "recommend-pick",
    "inspire",
    "query-understand",
    "smart-rank",
    "query-classify",
    "most-related-tip",
    "movie-insight",
    "person-pick",
];

fn is_valid_prompt_name(name: &str) -> bool {
    PROMPT_NAMES.contains(&name)
}

async fn list_prompts(
    _user: RequireUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<PromptInfo>>, (StatusCode, String)> {
    let locale = crate::api::recommend::get_locale(&state.pool).await;
    let mut prompts = Vec::with_capacity(PROMPT_NAMES.len());
    for &name in PROMPT_NAMES {
        let default_content =
            crate::api::recommend::default_prompt(name, &locale).to_string();
        let override_content = db::get_prompt_override(&state.pool, name, &locale)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let overridden = override_content.is_some();
        let content = override_content.unwrap_or_else(|| default_content.clone());
        prompts.push(PromptInfo {
            name: name.to_string(),
            content,
            default_content,
            locale: locale.clone(),
            overridden,
        });
    }
    Ok(Json(prompts))
}

#[derive(Debug, Deserialize)]
struct UpdatePromptBody {
    content: String,
}

async fn update_prompt(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<UpdatePromptBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !is_valid_prompt_name(&name) {
        return Err((StatusCode::BAD_REQUEST, "unknown prompt".to_string()));
    }
    if body.content.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "content is empty".to_string()));
    }
    let locale = crate::api::recommend::get_locale(&state.pool).await;
    db::upsert_prompt_override(&state.pool, &name, &locale, &body.content)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "message": "updated", "locale": locale })))
}

async fn reset_prompt(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !is_valid_prompt_name(&name) {
        return Err((StatusCode::BAD_REQUEST, "unknown prompt".to_string()));
    }
    let locale = crate::api::recommend::get_locale(&state.pool).await;
    db::delete_prompt_override(&state.pool, &name, &locale)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "message": "reset", "locale": locale })))
}

#[derive(Debug, Deserialize)]
struct MultiVersionParams {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Debug, Serialize)]
struct MultiVersionResponse {
    items: Vec<db::MultiVersionMovie>,
    total: i64,
    limit: i64,
    offset: i64,
}

async fn list_multi_version(
    _user: RequireUser,
    State(state): State<AppState>,
    Query(params): Query<MultiVersionParams>,
) -> Result<Json<MultiVersionResponse>, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(50).clamp(1, 200);
    let offset = params.offset.unwrap_or(0).max(0);
    let (items, total) = db::list_multi_version_movies(&state.pool, limit, offset)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(MultiVersionResponse {
        items,
        total,
        limit,
        offset,
    }))
}

#[cfg(test)]
mod tests {
    use crate::test_support::{get_json, post_json, test_app};
    use axum::http::StatusCode;
    use serde_json::json;
    use sqlx::SqlitePool;

    #[sqlx::test(migrations = "./migrations")]
    async fn admin_routes_reject_anonymous(pool: SqlitePool) {
        let cases = vec![
            ("GET", "/api/admin/overview"),
            ("GET", "/api/admin/status"),
            ("GET", "/api/admin/settings"),
            ("GET", "/api/admin/failed-tasks"),
            ("GET", "/api/admin/prompts"),
            ("GET", "/api/admin/llm-logs"),
            ("GET", "/api/admin/config"),
            ("GET", "/api/admin/multi-version"),
        ];
        for (_, path) in &cases {
            let (status, _) = get_json(test_app(pool.clone()), path, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "expected 401 for {}", path);
        }

        let post_cases = vec![
            "/api/admin/scan",
            "/api/admin/refetch-all",
            "/api/admin/rematch-pending",
        ];
        for path in &post_cases {
            let (status, _) =
                post_json(test_app(pool.clone()), path, &json!({}), None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "expected 401 for {}", path);
        }
    }

    /// Seed a dir + a 'failed' (or 'pending') mapping. Returns dir_id.
    async fn seed_unmatched_dir(
        pool: &SqlitePool,
        dir_name: &str,
        match_status: &str,
    ) -> i64 {
        let dir_id = crate::db::insert_media_dir(pool, &format!("/x/{}", dir_name), dir_name)
            .await
            .unwrap();
        crate::db::update_dir_status(pool, dir_id, "failed")
            .await
            .unwrap();
        crate::db::insert_mapping(pool, dir_id, None, match_status, Some(0.0), Some("[]"))
            .await
            .unwrap();
        dir_id
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn rematch_pending_resets_mappings_and_reenqueues(pool: SqlitePool) {
        // Two unmatched dirs (one failed, one pending) plus one auto-confirmed
        // dir that should be left alone.
        let failed_id =
            seed_unmatched_dir(&pool, "Inception.2010.BluRay.1080p", "failed").await;
        let pending_id =
            seed_unmatched_dir(&pool, "Beau.Travail.1999.Criterion", "pending").await;
        let auto_dir_id = crate::db::insert_media_dir(&pool, "/x/leave-alone", "Leave Alone")
            .await
            .unwrap();
        crate::db::update_dir_status(&pool, auto_dir_id, "matched")
            .await
            .unwrap();
        // Insert a placeholder movie row so the mapping FK is satisfied.
        sqlx::query("INSERT INTO movies (tmdb_id, title) VALUES (?, ?)")
            .bind(123_i64)
            .bind("Some Movie")
            .execute(&pool)
            .await
            .unwrap();
        let movie_id: i64 =
            sqlx::query_scalar("SELECT id FROM movies WHERE tmdb_id = 123")
                .fetch_one(&pool)
                .await
                .unwrap();
        crate::db::insert_mapping(&pool, auto_dir_id, Some(movie_id), "auto", Some(0.95), Some("[]"))
            .await
            .unwrap();

        // Auth + call
        let (_, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": "admin", "password": "pw" }),
            None,
        )
        .await;
        let token = body["token"].as_str().unwrap().to_string();

        let (status, body) = post_json(
            test_app(pool.clone()),
            "/api/admin/rematch-pending",
            &json!({}),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["rematched"], 2, "should rematch only failed+pending dirs");

        // Old mappings deleted for the two unmatched dirs.
        let remaining_failed = crate::db::get_mapping_by_dir_id(&pool, failed_id)
            .await
            .unwrap();
        let remaining_pending = crate::db::get_mapping_by_dir_id(&pool, pending_id)
            .await
            .unwrap();
        assert!(remaining_failed.is_none(), "failed mapping should be deleted");
        assert!(remaining_pending.is_none(), "pending mapping should be deleted");

        // Auto mapping untouched.
        let auto_mapping = crate::db::get_mapping_by_dir_id(&pool, auto_dir_id)
            .await
            .unwrap()
            .expect("auto mapping should still exist");
        assert_eq!(auto_mapping.match_status, "auto");

        // Both unmatched dirs: scan_status reset to 'parsed'.
        let scan_failed: String =
            sqlx::query_scalar("SELECT scan_status FROM media_dirs WHERE id = ?")
                .bind(failed_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(scan_failed, "parsed");

        // tmdb_search tasks enqueued (one per unmatched dir).
        let task_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE task_type = 'tmdb_search' AND status = 'pending'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(task_count, 2);

        // Task payload has the parsed title (proving we re-parsed, not used the
        // raw dir_name as title).
        let payloads: Vec<String> = sqlx::query_scalar(
            "SELECT payload FROM tasks WHERE task_type = 'tmdb_search' ORDER BY id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(payloads[0].contains("\"title\":\"Inception\""), "payload was {}", payloads[0]);
        assert!(payloads[1].contains("\"title\":\"Beau Travail\""), "payload was {}", payloads[1]);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn admin_overview_works_with_auth(pool: SqlitePool) {
        let (_, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": "admin", "password": "pw" }),
            None,
        )
        .await;
        let token = body["token"].as_str().unwrap();

        let (status, body) = get_json(
            test_app(pool),
            "/api/admin/overview",
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["dir_total"].is_number());
        assert!(body["dir_status"].is_array());
        assert!(body["match_status"].is_array());
        assert!(body["movies_by_source"].is_array());
        assert!(body["tasks"].is_object());
    }

    /// Helper: insert a movie and return its id.
    async fn seed_movie(pool: &SqlitePool, tmdb_id: i64, title: &str) -> i64 {
        sqlx::query("INSERT INTO movies (tmdb_id, title) VALUES (?, ?)")
            .bind(tmdb_id)
            .bind(title)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query_scalar::<_, i64>("SELECT id FROM movies WHERE tmdb_id = ?")
            .bind(tmdb_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Helper: insert media_dir + matching mapping. Returns dir_id.
    async fn seed_dir_with_mapping(
        pool: &SqlitePool,
        dir_path: &str,
        dir_name: &str,
        source: &str,
        movie_id: i64,
        match_status: &str,
        confidence: f64,
    ) -> i64 {
        let dir_id =
            crate::db::insert_media_dir_with_source(pool, dir_path, dir_name, source)
                .await
                .unwrap();
        crate::db::insert_mapping(
            pool,
            dir_id,
            Some(movie_id),
            match_status,
            Some(confidence),
            Some("[]"),
        )
        .await
        .unwrap();
        dir_id
    }

    async fn admin_token(pool: &SqlitePool) -> String {
        let (_, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": "admin", "password": "pw" }),
            None,
        )
        .await;
        body["token"].as_str().unwrap().to_string()
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn multi_version_empty(pool: SqlitePool) {
        let token = admin_token(&pool).await;
        let (status, body) = get_json(
            test_app(pool),
            "/api/admin/multi-version",
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"], 0);
        assert_eq!(body["items"].as_array().unwrap().len(), 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn multi_version_filters_pending_and_groups_by_movie(pool: SqlitePool) {
        // Movie A: 2 auto mappings → should appear with version_count=2
        let movie_a = seed_movie(&pool, 1001, "Inception").await;
        seed_dir_with_mapping(
            &pool,
            "/local/Inception",
            "Inception (2010)",
            "local",
            movie_a,
            "auto",
            0.92,
        )
        .await;
        let qbt_dir_a = seed_dir_with_mapping(
            &pool,
            "/qbt/Inception.2010.BluRay",
            "Inception.2010.1080p.BluRay-RARBG",
            "qbittorrent",
            movie_a,
            "manual",
            0.88,
        )
        .await;
        // Attach a torrent_info row to the qBT dir so size/media_type comes through.
        crate::db::upsert_torrent_info(
            &pool,
            qbt_dir_a,
            "hashA",
            "completed",
            1.0,
            Some(8_000_000_000),
            None,
            None,
            None,
            None,
            None,
            "Blu-ray",
            "Inception.2010.1080p.BluRay-RARBG",
        )
        .await
        .unwrap();

        // Movie B: 1 auto + 1 pending → should NOT appear (only 1 valid mapping)
        let movie_b = seed_movie(&pool, 1002, "Tenet").await;
        seed_dir_with_mapping(&pool, "/local/Tenet", "Tenet", "local", movie_b, "auto", 0.9)
            .await;
        seed_dir_with_mapping(
            &pool,
            "/local/Tenet.alt",
            "Tenet.alt",
            "local",
            movie_b,
            "pending",
            0.4,
        )
        .await;

        // Movie C: 3 auto mappings → highest version_count, should be first
        let movie_c = seed_movie(&pool, 1003, "Dune").await;
        for i in 0..3 {
            seed_dir_with_mapping(
                &pool,
                &format!("/local/Dune.v{}", i),
                &format!("Dune.v{}", i),
                "local",
                movie_c,
                "auto",
                0.85,
            )
            .await;
        }

        let token = admin_token(&pool).await;
        let (status, body) = get_json(
            test_app(pool.clone()),
            "/api/admin/multi-version",
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"], 2, "Movie B should be filtered out");

        let items = body["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        // Movie C first (3 versions), then movie A (2 versions).
        assert_eq!(items[0]["movie"]["id"], movie_c);
        assert_eq!(items[0]["version_count"], 3);
        assert_eq!(items[0]["dirs"].as_array().unwrap().len(), 3);
        assert_eq!(items[1]["movie"]["id"], movie_a);
        assert_eq!(items[1]["version_count"], 2);

        // Movie A's qBT dir should have torrent_info JOINed.
        let dirs_a = items[1]["dirs"].as_array().unwrap();
        let qbt_entry = dirs_a
            .iter()
            .find(|d| d["source"] == "qbittorrent")
            .expect("qbt dir should be present");
        assert_eq!(qbt_entry["media_type"], "Blu-ray");
        assert_eq!(qbt_entry["size_bytes"], 8_000_000_000_i64);
        assert_eq!(qbt_entry["torrent_progress"], 1.0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn multi_version_pagination(pool: SqlitePool) {
        // Two qualifying movies, both with 2 versions.
        for tmdb in [2001_i64, 2002] {
            let mid = seed_movie(&pool, tmdb, &format!("Movie{}", tmdb)).await;
            for i in 0..2 {
                seed_dir_with_mapping(
                    &pool,
                    &format!("/x/{}/{}", tmdb, i),
                    &format!("M{}-{}", tmdb, i),
                    "local",
                    mid,
                    "auto",
                    0.9,
                )
                .await;
            }
        }

        let token = admin_token(&pool).await;
        let (status, body) = get_json(
            test_app(pool.clone()),
            "/api/admin/multi-version?limit=1&offset=1",
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"], 2);
        assert_eq!(body["items"].as_array().unwrap().len(), 1);
        assert_eq!(body["limit"], 1);
        assert_eq!(body["offset"], 1);
    }
}
