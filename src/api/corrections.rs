use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::{
    api::common::{internal_error, ListResponse, PageParams},
    api::AppState,
    auth::RequireUser,
    db,
    tmdb::client::{TmdbClient, TmdbSearchResult},
};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/dirs/pending", get(list_pending_dirs))
        .route("/dirs/{dir_id}/candidates", get(get_candidates))
        .route("/dirs/{dir_id}/bind", post(bind_dir))
        .route("/dirs/{dir_id}/unbind", post(unbind_dir))
        .route("/tmdb/search", get(tmdb_search))
}

#[derive(Debug, Serialize)]
struct CandidatesResponse {
    dir_id: i64,
    match_status: String,
    confidence: Option<f64>,
    candidates: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct BindRequest {
    tmdb_id: i64,
}

#[derive(Debug, Serialize)]
struct BindResponse {
    dir_id: i64,
    movie_id: i64,
    message: String,
}

#[derive(Debug, Serialize)]
struct UnbindResponse {
    dir_id: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: String,
    #[serde(default)]
    year: Option<u32>,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    results: Vec<TmdbSearchResult>,
}

async fn list_pending_dirs(
    _user: RequireUser,
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> Result<Json<ListResponse<Vec<db::PendingDirRow>>>, (StatusCode, String)> {
    let (rows, total) = db::list_pending_dirs(&state.pool, params.page, params.per_page)
        .await
        .map_err(internal_error)?;

    Ok(Json(ListResponse {
        data: rows,
        page: params.page.max(1),
        per_page: params.per_page.max(1),
        total,
    }))
}

async fn get_candidates(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(dir_id): Path<i64>,
) -> Result<Json<CandidatesResponse>, StatusCode> {
    let mapping = db::get_mapping_by_dir_id(&state.pool, dir_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let candidates = mapping
        .candidates
        .as_deref()
        .and_then(|c| serde_json::from_str(c).ok())
        .unwrap_or_else(|| serde_json::json!([]));

    Ok(Json(CandidatesResponse {
        dir_id,
        match_status: mapping.match_status,
        confidence: mapping.confidence,
        candidates,
    }))
}

async fn bind_dir(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(dir_id): Path<i64>,
    Json(body): Json<BindRequest>,
) -> Result<Json<BindResponse>, (StatusCode, String)> {
    let existing = db::get_movie_by_tmdb_id(&state.pool, body.tmdb_id)
        .await
        .map_err(internal_error)?;

    let tmdb_client = tmdb_client(&state);

    let movie_id = if let Some(movie) = existing {
        movie.id
    } else {
        let detail = tmdb_client.get_movie_detail(body.tmdb_id).await.ok();

        let title = detail.as_ref().map(|d| d.title.as_str()).unwrap_or("Unknown");
        let original_title = detail.as_ref().and_then(|d| d.original_title.as_deref());
        let year = detail
            .as_ref()
            .and_then(|d| d.release_date.as_ref())
            .and_then(|d| d.get(..4))
            .and_then(|y| y.parse::<i64>().ok());
        let overview = detail.as_ref().and_then(|d| d.overview.as_deref());
        let poster_url = detail.as_ref().and_then(|d| d.poster_path.as_deref());
        let country = detail
            .as_ref()
            .and_then(|d| d.production_countries.as_ref())
            .and_then(|c| c.first())
            .map(|c| c.iso_3166_1.as_str());
        let language = detail.as_ref().and_then(|d| d.original_language.as_deref());
        let runtime = detail.as_ref().and_then(|d| d.runtime);
        let rating = detail.as_ref().and_then(|d| d.vote_average);
        let votes = detail.as_ref().and_then(|d| d.vote_count);

        db::insert_movie(
            &state.pool,
            body.tmdb_id,
            title,
            original_title,
            year,
            overview,
            poster_url,
            "[]",
            country,
            language,
            runtime,
            None,
            "[]",
            rating,
            votes,
            "[]",
            None,
            None,
            None,
            "library",
        )
        .await
        .map_err(internal_error)?;

        db::get_movie_by_tmdb_id(&state.pool, body.tmdb_id)
            .await
            .map_err(internal_error)?
            .map(|m| m.id)
            .ok_or((
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create movie".to_string(),
            ))?
    };

    db::bind_dir_to_movie(&state.pool, dir_id, movie_id)
        .await
        .map_err(map_sqlx_error)?;

    // trigger detailed fetch in background
    let payload = serde_json::json!({ "tmdb_id": body.tmdb_id, "movie_id": movie_id });
    if let Err(err) = db::insert_task(&state.pool, "tmdb_fetch", &payload.to_string()).await {
        tracing::error!(dir_id, movie_id, %err, "failed to enqueue tmdb_fetch");
    }

    Ok(Json(BindResponse {
        dir_id,
        movie_id,
        message: "dir bound to movie".to_string(),
    }))
}

async fn unbind_dir(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(dir_id): Path<i64>,
) -> Result<Json<UnbindResponse>, (StatusCode, String)> {
    db::unbind_dir(&state.pool, dir_id)
        .await
        .map_err(map_sqlx_error)?;

    Ok(Json(UnbindResponse {
        dir_id,
        message: "dir unbound".to_string(),
    }))
}

async fn tmdb_search(
    _user: RequireUser,
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    if params.q.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "missing query".to_string()));
    }

    let tmdb = tmdb_client(&state);
    let results = tmdb
        .search_movie(&params.q, params.year)
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;

    Ok(Json(SearchResponse { results }))
}

fn tmdb_client(state: &AppState) -> TmdbClient {
    TmdbClient::new(&state.config.tmdb.api_key, &state.config.tmdb.language, state.config.tmdb.proxy.as_deref())
}

fn map_sqlx_error(err: sqlx::Error) -> (StatusCode, String) {
    match err {
        sqlx::Error::RowNotFound => (StatusCode::NOT_FOUND, "record not found".to_string()),
        other => internal_error(other),
    }
}
#[cfg(test)]
mod tests {
    use crate::test_support::{get_json, post_json, test_app};
    use axum::http::StatusCode;
    use serde_json::json;
    use sqlx::SqlitePool;

    async fn register(pool: &SqlitePool) -> String {
        let (_, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": "admin", "password": "pw" }),
            None,
        )
        .await;
        body["token"].as_str().unwrap().to_string()
    }

    /// Insert a dir in "parsed" status with a pending mapping and return (dir_id, mapping_id).
    async fn seed_pending_dir(
        pool: &SqlitePool,
        path: &str,
        candidates_json: &str,
    ) -> (i64, i64) {
        let dir_id = crate::db::insert_media_dir(pool, path, path)
            .await
            .unwrap();
        crate::db::update_dir_status(pool, dir_id, "parsed")
            .await
            .unwrap();
        let mapping_id = crate::db::insert_mapping(
            pool,
            dir_id,
            None,
            "pending",
            Some(0.6),
            Some(candidates_json),
        )
        .await
        .unwrap();
        (dir_id, mapping_id)
    }

    async fn seed_movie(pool: &SqlitePool, tmdb_id: i64, title: &str) -> i64 {
        sqlx::query("INSERT INTO movies (tmdb_id, title) VALUES (?, ?)")
            .bind(tmdb_id)
            .bind(title)
            .execute(pool)
            .await
            .unwrap()
            .last_insert_rowid()
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymous_requests_are_rejected(pool: SqlitePool) {
        let (s1, _) = get_json(test_app(pool.clone()), "/api/dirs/pending", None).await;
        assert_eq!(s1, StatusCode::UNAUTHORIZED);

        let (s2, _) = get_json(test_app(pool.clone()), "/api/dirs/1/candidates", None).await;
        assert_eq!(s2, StatusCode::UNAUTHORIZED);

        let (s3, _) = post_json(
            test_app(pool.clone()),
            "/api/dirs/1/bind",
            &json!({ "tmdb_id": 1 }),
            None,
        )
        .await;
        assert_eq!(s3, StatusCode::UNAUTHORIZED);

        let (s4, _) = post_json(test_app(pool), "/api/dirs/1/unbind", &json!({}), None).await;
        assert_eq!(s4, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn list_pending_dirs_returns_paginated(pool: SqlitePool) {
        let token = register(&pool).await;
        seed_pending_dir(&pool, "/a", "[]").await;
        seed_pending_dir(&pool, "/b", "[]").await;

        let (status, body) = get_json(
            test_app(pool),
            "/api/dirs/pending?page=1&per_page=10",
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"].as_i64().unwrap(), 2);
        assert_eq!(body["data"].as_array().unwrap().len(), 2);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_candidates_returns_stored_json(pool: SqlitePool) {
        let token = register(&pool).await;
        let cands = r#"[{"id":123,"title":"Test","popularity":50}]"#;
        let (dir_id, _) = seed_pending_dir(&pool, "/m", cands).await;

        let (status, body) = get_json(
            test_app(pool),
            &format!("/api/dirs/{}/candidates", dir_id),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["dir_id"], dir_id);
        assert_eq!(body["match_status"], "pending");
        assert_eq!(body["candidates"][0]["id"], 123);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_candidates_404_for_unknown_dir(pool: SqlitePool) {
        let token = register(&pool).await;
        let (status, _) = get_json(
            test_app(pool),
            "/api/dirs/999/candidates",
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn bind_existing_movie_updates_status(pool: SqlitePool) {
        let token = register(&pool).await;
        let (dir_id, _) = seed_pending_dir(&pool, "/m", "[]").await;
        let movie_id = seed_movie(&pool, 555, "Inception").await;

        let (status, body) = post_json(
            test_app(pool.clone()),
            &format!("/api/dirs/{}/bind", dir_id),
            &json!({ "tmdb_id": 555 }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["dir_id"], dir_id);
        assert_eq!(body["movie_id"], movie_id);

        // Verify the DB state matches — mapping is 'manual' with the movie.
        let mapping = crate::db::get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mapping.match_status, "manual");
        assert_eq!(mapping.movie_id, Some(movie_id));

        let scan_status: String =
            sqlx::query_scalar("SELECT scan_status FROM media_dirs WHERE id = ?")
                .bind(dir_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(scan_status, "matched");

        // A tmdb_fetch task should be queued.
        let fetch_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE task_type = 'tmdb_fetch' AND status = 'pending'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(fetch_count, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn unbind_reverts_to_pending_and_parsed(pool: SqlitePool) {
        let token = register(&pool).await;
        let (dir_id, _) = seed_pending_dir(&pool, "/m", "[]").await;
        let _movie_id = seed_movie(&pool, 555, "Inception").await;

        // Bind first.
        post_json(
            test_app(pool.clone()),
            &format!("/api/dirs/{}/bind", dir_id),
            &json!({ "tmdb_id": 555 }),
            Some(&token),
        )
        .await;

        // Unbind.
        let (status, body) = post_json(
            test_app(pool.clone()),
            &format!("/api/dirs/{}/unbind", dir_id),
            &json!({}),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["dir_id"], dir_id);

        let mapping = crate::db::get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mapping.match_status, "pending");
        assert!(mapping.movie_id.is_none());

        let scan_status: String =
            sqlx::query_scalar("SELECT scan_status FROM media_dirs WHERE id = ?")
                .bind(dir_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(scan_status, "parsed");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn unbind_unknown_dir_returns_404(pool: SqlitePool) {
        let token = register(&pool).await;
        let (status, _) = post_json(
            test_app(pool),
            "/api/dirs/999/unbind",
            &json!({}),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
