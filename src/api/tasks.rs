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
}

async fn trigger_scan(
    _user: RequireUser,
    State(state): State<AppState>,
) -> Result<Json<ScanResponse>, (StatusCode, String)> {
    let pool = state.pool.clone();
    let config = state.config.clone();

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
    tasks: HashMap<String, HashMap<String, i64>>,
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
    let task_counts = db::get_task_counts(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut tasks: HashMap<String, HashMap<String, i64>> = HashMap::new();
    for (task_type, status, count) in task_counts {
        tasks.entry(task_type).or_default().insert(status, count);
    }

    Ok(Json(OverviewResponse {
        dir_total,
        dir_status,
        match_status,
        tasks,
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

#[derive(Debug, Serialize)]
struct LlmLogEntry {
    filename: String,
    size: u64,
    modified: String,
}

async fn list_llm_logs(
    _user: RequireUser,
) -> Result<Json<Vec<LlmLogEntry>>, (StatusCode, String)> {
    let log_dir = std::path::Path::new("data/llm-logs");
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
    let path = std::path::Path::new("data/llm-logs").join(&filename);
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
        ];
        for (_, path) in &cases {
            let (status, _) = get_json(test_app(pool.clone()), path, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "expected 401 for {}", path);
        }

        let post_cases = vec!["/api/admin/scan", "/api/admin/refetch-all"];
        for path in &post_cases {
            let (status, _) =
                post_json(test_app(pool.clone()), path, &json!({}), None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "expected 401 for {}", path);
        }
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
        assert!(body["tasks"].is_object());
    }
}
