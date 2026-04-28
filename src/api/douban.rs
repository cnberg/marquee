use std::collections::HashMap;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::api::common::internal_error;
use crate::api::AppState;
use crate::auth::RequireUser;
use crate::config::Config;
use crate::db;
use crate::douban::matcher::{match_douban_record, DoubanMatch};
use crate::douban::parser::{parse_csv, DoubanRecord};
use crate::tmdb::client::TmdbClient;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/admin/douban/import", post(import_csv))
        .route("/admin/douban/status", get(status))
        .route("/admin/douban/pending", get(list_pending))
        .route("/admin/douban/pending/{id}/bind", post(bind_pending))
        .route("/admin/douban/pending/{id}/skip", post(skip_pending))
}

#[derive(Debug, Serialize)]
struct ImportResponse {
    total_received: usize,
    newly_queued: usize,
    already_existed: usize,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    counts: HashMap<String, i64>,
}

#[derive(Debug, Serialize)]
struct PendingItem {
    id: i64,
    raw_title: String,
    parsed_title_zh: Option<String>,
    parsed_title_en: Option<String>,
    year: Option<i64>,
    country: Option<String>,
    douban_url: String,
    error_msg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BindRequest {
    tmdb_id: i64,
}

#[derive(Debug, Serialize)]
struct BindResponse {
    movie_id: i64,
}

async fn import_csv(
    RequireUser(user): RequireUser,
    State(state): State<AppState>,
    body: String,
) -> Result<Json<ImportResponse>, (StatusCode, String)> {
    let records = parse_csv(&body).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let total_received = records.len();

    let mut newly_queued = 0;
    let mut already_existed = 0;
    for r in &records {
        match db::upsert_douban_import_pending(
            &state.pool,
            user.id,
            &r.douban_subject_id,
            &r.raw_title,
            r.parsed_title_zh.as_deref(),
            r.parsed_title_en.as_deref(),
            r.year,
            r.country.as_deref(),
            &r.douban_url,
        )
        .await
        .map_err(internal_error)?
        {
            (_, true) => newly_queued += 1,
            (_, false) => already_existed += 1,
        }
    }

    // Kick off async processing for everything currently 'pending' for this user.
    // Returning immediately is the right call: the matching loop talks to TMDB
    // at 4 req/s, so 1000 entries take ~8 minutes; the user polls /status for
    // progress.
    let pool = state.pool.clone();
    let config = state.config.read().await.clone();
    let user_id = user.id;
    tokio::spawn(async move {
        if let Err(err) = process_pending_for_user(&pool, &config, user_id).await {
            tracing::error!(user_id, %err, "douban import processing loop failed");
        }
    });

    Ok(Json(ImportResponse {
        total_received,
        newly_queued,
        already_existed,
    }))
}

async fn status(
    RequireUser(user): RequireUser,
    State(state): State<AppState>,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    let rows = db::count_douban_imports_by_status(&state.pool, user.id)
        .await
        .map_err(internal_error)?;
    let mut counts = HashMap::new();
    for (status, n) in rows {
        counts.insert(status, n);
    }
    Ok(Json(StatusResponse { counts }))
}

async fn list_pending(
    RequireUser(user): RequireUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<PendingItem>>, (StatusCode, String)> {
    let rows = db::list_douban_imports_by_status(&state.pool, user.id, "pending")
        .await
        .map_err(internal_error)?;
    let out = rows
        .into_iter()
        .map(|r| PendingItem {
            id: r.id,
            raw_title: r.raw_title,
            parsed_title_zh: r.parsed_title_zh,
            parsed_title_en: r.parsed_title_en,
            year: r.year,
            country: r.country,
            douban_url: r.douban_url,
            error_msg: r.error_msg,
        })
        .collect();
    Ok(Json(out))
}

async fn bind_pending(
    RequireUser(user): RequireUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<BindRequest>,
) -> Result<Json<BindResponse>, (StatusCode, String)> {
    let row = db::get_douban_import(&state.pool, id)
        .await
        .map_err(internal_error)?
        .ok_or((StatusCode::NOT_FOUND, "import row not found".to_string()))?;
    if row.user_id != user.id {
        return Err((StatusCode::FORBIDDEN, "not your import row".to_string()));
    }

    let config = state.config.read().await.clone();
    let tmdb = TmdbClient::new(&config.tmdb.api_key, &config.tmdb.language, config.tmdb.proxy.as_deref());

    let (movie_id, was_created) = ensure_movie_for_tmdb_id(&state.pool, &tmdb, body.tmdb_id)
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e))?;

    db::add_user_mark(&state.pool, user.id, movie_id, "watched")
        .await
        .map_err(internal_error)?;

    let new_status = if was_created { "created" } else { "matched" };
    db::update_douban_import_matched(&state.pool, id, movie_id, new_status)
        .await
        .map_err(internal_error)?;

    Ok(Json(BindResponse { movie_id }))
}

async fn skip_pending(
    RequireUser(user): RequireUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, (StatusCode, String)> {
    let row = db::get_douban_import(&state.pool, id)
        .await
        .map_err(internal_error)?
        .ok_or((StatusCode::NOT_FOUND, "import row not found".to_string()))?;
    if row.user_id != user.id {
        return Err((StatusCode::FORBIDDEN, "not your import row".to_string()));
    }

    db::update_douban_import_status(&state.pool, id, "skipped", None)
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Process every 'pending' row for a user via TMDB matcher. Idempotent —
/// rows that auto-confirm transition to matched/created, rows that don't get
/// left as pending (the user surfaces them in the待绑定 page).
async fn process_pending_for_user(
    pool: &db::SqlitePool,
    config: &Config,
    user_id: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tmdb = TmdbClient::new(&config.tmdb.api_key, &config.tmdb.language, config.tmdb.proxy.as_deref());
    let pending = db::list_douban_imports_by_status(pool, user_id, "pending").await?;
    let threshold = config.tmdb.auto_confirm_threshold;

    for row in pending {
        // Re-hydrate a DoubanRecord from the stored fields. The matcher only
        // reads zh/en/raw + year, so we don't need the full original CSV here.
        let record = DoubanRecord {
            douban_subject_id: row.douban_subject_id.clone(),
            raw_title: row.raw_title.clone(),
            parsed_title_zh: row.parsed_title_zh.clone(),
            parsed_title_en: row.parsed_title_en.clone(),
            year: row.year,
            country: row.country.clone(),
            douban_url: row.douban_url.clone(),
        };

        let outcome = match_douban_record(&tmdb, &record, threshold).await;

        match outcome {
            DoubanMatch::Found {
                tmdb_id,
                tmdb_title,
                tmdb_original_title,
                tmdb_year,
                ..
            } => {
                let (movie_id, was_created) = ensure_movie_minimal(
                    pool,
                    tmdb_id,
                    &tmdb_title,
                    tmdb_original_title.as_deref(),
                    tmdb_year,
                )
                .await?;
                db::add_user_mark(pool, user_id, movie_id, "watched").await?;
                let new_status = if was_created { "created" } else { "matched" };
                db::update_douban_import_matched(pool, row.id, movie_id, new_status).await?;
            }
            DoubanMatch::Pending => {
                // Leave status='pending' so the user can intervene.
                db::update_douban_import_status(pool, row.id, "pending", Some("auto-confirm 阈值未达到，请手工绑定")).await?;
            }
            DoubanMatch::Failed => {
                db::update_douban_import_status(pool, row.id, "pending", Some("TMDB 未返回候选（可能是剧集，请手工绑定或跳过）")).await?;
            }
        }
    }

    Ok(())
}

/// Look up a movie by tmdb_id. If it doesn't exist, insert a stub row with
/// `source='related'` and enqueue a `tmdb_fetch` task to fill in everything
/// else (overview, cast, posters, etc.). Returns (movie_id, was_created).
///
/// We reuse `'related'` rather than introducing a new `'douban'` source value
/// because every existing query (library_total / 库外热门 / 协同召回 etc.) is
/// already wired to handle library-vs-`'related'`. Provenance is recoverable
/// from `douban_imports.movie_id`.
async fn ensure_movie_minimal(
    pool: &db::SqlitePool,
    tmdb_id: i64,
    title: &str,
    original_title: Option<&str>,
    year: Option<i64>,
) -> Result<(i64, bool), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(m) = db::get_movie_by_tmdb_id(pool, tmdb_id).await? {
        return Ok((m.id, false));
    }
    db::insert_movie(
        pool,
        tmdb_id,
        title,
        original_title,
        year,
        None, // overview
        None, // poster
        "[]", // genres
        None, // country
        None, // language
        None, // runtime
        None, // director
        "[]", // cast
        None, // tmdb_rating
        None, // tmdb_votes
        "[]", // keywords
        None, // budget
        None, // revenue
        None, // popularity
        "related",
    )
    .await?;
    let payload = serde_json::json!({ "tmdb_id": tmdb_id });
    db::insert_task(pool, "tmdb_fetch", &payload.to_string()).await?;
    let row = db::get_movie_by_tmdb_id(pool, tmdb_id)
        .await?
        .ok_or("inserted movie disappeared")?;
    Ok((row.id, true))
}

/// Like ensure_movie_minimal but uses TMDB to fetch the title/year before the
/// initial insert (used by manual bind, where we don't have the matcher's
/// shortcut data).
async fn ensure_movie_for_tmdb_id(
    pool: &db::SqlitePool,
    tmdb: &TmdbClient,
    tmdb_id: i64,
) -> Result<(i64, bool), String> {
    if let Some(m) = db::get_movie_by_tmdb_id(pool, tmdb_id).await.map_err(|e| e.to_string())? {
        return Ok((m.id, false));
    }
    let detail = tmdb.get_movie_detail(tmdb_id).await.map_err(|e| e.to_string())?;
    let year = detail
        .release_date
        .as_ref()
        .and_then(|d| d.get(0..4))
        .and_then(|y| y.parse::<i64>().ok());
    ensure_movie_minimal(
        pool,
        tmdb_id,
        &detail.title,
        detail.original_title.as_deref(),
        year,
    )
    .await
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use crate::test_support::{get_json, post_json, test_app};
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use serde_json::{json, Value};
    use sqlx::SqlitePool;
    use tower::ServiceExt;

    const CSV_TWO_ROWS: &str = "\u{feff}封面,标题,个人评分,打分日期,我的短评,上映日期,制片国家,条目链接\n\
\"https://img/p1.jpg\",\"通天塔/Babel\",\"\",\"2007/03/01\",\"\",\"2006/11/10\",\"美国\",\"https://movie.douban.com/subject/1498818/\",\n\
\"https://img/p2.jpg\",\"心慌方/Cube\",\"5\",\"2007/03/01\",\"\",\"1997/09/09\",\"加拿大\",\"https://movie.douban.com/subject/1305903/\",\n";

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

    /// POST plain-text body; the importer accepts CSV as the request body.
    async fn post_csv(
        router: axum::Router,
        path: &str,
        csv: &str,
        bearer: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut req = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "text/csv");
        if let Some(token) = bearer {
            req = req.header("authorization", format!("Bearer {}", token));
        }
        let req = req.body(Body::from(csv.to_string())).unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let json: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, json)
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn anonymous_requests_are_rejected(pool: SqlitePool) {
        let (s, _) = post_csv(test_app(pool.clone()), "/api/admin/douban/import", CSV_TWO_ROWS, None).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);

        let (s, _) = get_json(test_app(pool.clone()), "/api/admin/douban/pending", None).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);

        let (s, _) = get_json(test_app(pool.clone()), "/api/admin/douban/status", None).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn import_persists_records_as_pending(pool: SqlitePool) {
        let token = register(&pool).await;

        let (status, body) = post_csv(
            test_app(pool.clone()),
            "/api/admin/douban/import",
            CSV_TWO_ROWS,
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total_received"], 2);
        assert_eq!(body["newly_queued"], 2);
        assert_eq!(body["already_existed"], 0);

        // Rows should land in DB as 'pending'. The async TMDB matching loop
        // runs in a tokio::spawn we don't await, but new records start at
        // 'pending' before that loop touches them.
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM douban_imports WHERE user_id = 1 AND status = 'pending'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(n, 2);

        // Subject IDs come through unchanged.
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT douban_subject_id FROM douban_imports ORDER BY id")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(ids, vec!["1498818", "1305903"]);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn import_is_idempotent_on_re_upload(pool: SqlitePool) {
        let token = register(&pool).await;

        post_csv(
            test_app(pool.clone()),
            "/api/admin/douban/import",
            CSV_TWO_ROWS,
            Some(&token),
        )
        .await;
        let (_, body) = post_csv(
            test_app(pool.clone()),
            "/api/admin/douban/import",
            CSV_TWO_ROWS,
            Some(&token),
        )
        .await;

        assert_eq!(body["total_received"], 2);
        assert_eq!(body["newly_queued"], 0);
        assert_eq!(body["already_existed"], 2);

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM douban_imports")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 2, "re-upload must not create new rows");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn skip_pending_record(pool: SqlitePool) {
        let token = register(&pool).await;
        post_csv(
            test_app(pool.clone()),
            "/api/admin/douban/import",
            CSV_TWO_ROWS,
            Some(&token),
        )
        .await;

        let id: i64 = sqlx::query_scalar(
            "SELECT id FROM douban_imports WHERE douban_subject_id = '1498818'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let (status, _) = post_json(
            test_app(pool.clone()),
            &format!("/api/admin/douban/pending/{}/skip", id),
            &json!({}),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let row_status: String =
            sqlx::query_scalar("SELECT status FROM douban_imports WHERE id = ?")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(row_status, "skipped");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn skip_other_users_row_forbidden(pool: SqlitePool) {
        // user A imports
        let token_a = register(&pool).await;
        post_csv(
            test_app(pool.clone()),
            "/api/admin/douban/import",
            CSV_TWO_ROWS,
            Some(&token_a),
        )
        .await;
        let id: i64 = sqlx::query_scalar("SELECT id FROM douban_imports LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();

        // user B tries to skip A's row
        let (_, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": "other", "password": "pw" }),
            None,
        )
        .await;
        let token_b = body["token"].as_str().unwrap().to_string();

        let (status, _) = post_json(
            test_app(pool.clone()),
            &format!("/api/admin/douban/pending/{}/skip", id),
            &json!({}),
            Some(&token_b),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
}
