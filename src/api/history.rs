use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::api::AppState;
use crate::auth::RequireUser;
use crate::db;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/history", get(list_history).delete(clear_history))
        .route("/history/{id}", get(get_history).delete(delete_history))
}

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

fn default_limit() -> i64 {
    20
}

async fn list_history(
    State(state): State<AppState>,
    RequireUser(user): RequireUser,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<db::SearchHistoryItem>>, StatusCode> {
    let limit = q.limit.clamp(1, 100);
    let offset = q.offset.max(0);
    db::list_search_history(&state.pool, user.id, limit, offset)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn get_history(
    State(state): State<AppState>,
    RequireUser(user): RequireUser,
    Path(id): Path<i64>,
) -> Result<Json<db::SearchHistoryDetail>, StatusCode> {
    match db::get_search_history(&state.pool, user.id, id).await {
        Ok(Some(item)) => Ok(Json(item)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn delete_history(
    State(state): State<AppState>,
    RequireUser(user): RequireUser,
    Path(id): Path<i64>,
) -> Result<StatusCode, StatusCode> {
    match db::delete_search_history(&state.pool, user.id, id).await {
        Ok(true) => Ok(StatusCode::NO_CONTENT),
        Ok(false) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn clear_history(
    State(state): State<AppState>,
    RequireUser(user): RequireUser,
) -> Result<StatusCode, StatusCode> {
    db::clear_search_history(&state.pool, user.id)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[cfg(test)]
mod tests {
    use crate::test_support::{delete_json, get_json, post_json, test_app};
    use axum::http::StatusCode;
    use serde_json::json;
    use sqlx::SqlitePool;

    async fn register(pool: &SqlitePool, username: &str) -> String {
        let (_, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": username, "password": "pw" }),
            None,
        )
        .await;
        body["token"].as_str().unwrap().to_string()
    }

    async fn seed_history(pool: &SqlitePool, user_id: i64, prompt: &str) -> i64 {
        crate::db::insert_search_history(pool, user_id, prompt, "[]", 0)
            .await
            .unwrap()
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn history_requires_auth(pool: SqlitePool) {
        let (s1, _) = get_json(test_app(pool.clone()), "/api/history", None).await;
        assert_eq!(s1, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn list_and_get_history(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        // user_id = 1 (first registered user).
        let id = seed_history(&pool, 1, "cozy movies").await;

        let (status, body) = get_json(
            test_app(pool.clone()),
            "/api/history",
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_array().unwrap().len(), 1);
        assert_eq!(body[0]["prompt"], "cozy movies");

        let (status, body) = get_json(
            test_app(pool),
            &format!("/api/history/{}", id),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["prompt"], "cozy movies");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn cross_user_isolation(pool: SqlitePool) {
        let token_a = register(&pool, "alice").await;
        let token_b = register(&pool, "bob").await;
        let id = seed_history(&pool, 1, "alice only").await;

        // Bob cannot see alice's history.
        let (status, _) = get_json(
            test_app(pool.clone()),
            &format!("/api/history/{}", id),
            Some(&token_b),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Bob cannot delete alice's history.
        let (status, _) = delete_json(
            test_app(pool.clone()),
            &format!("/api/history/{}", id),
            Some(&token_b),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Alice can.
        let (status, _) = delete_json(
            test_app(pool),
            &format!("/api/history/{}", id),
            Some(&token_a),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn clear_history_removes_only_own(pool: SqlitePool) {
        let token_a = register(&pool, "alice").await;
        let _token_b = register(&pool, "bob").await;
        seed_history(&pool, 1, "alice1").await;
        seed_history(&pool, 1, "alice2").await;
        seed_history(&pool, 2, "bob1").await;

        let (status, _) = delete_json(
            test_app(pool.clone()),
            "/api/history",
            Some(&token_a),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Alice's history gone.
        let (_, body) = get_json(test_app(pool.clone()), "/api/history", Some(&token_a)).await;
        assert_eq!(body.as_array().unwrap().len(), 0);

        // Bob's intact.
        let (_, body) = get_json(test_app(pool), "/api/history", Some(&_token_b)).await;
        assert_eq!(body.as_array().unwrap().len(), 1);
    }
}
