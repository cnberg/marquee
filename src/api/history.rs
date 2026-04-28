use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

use crate::api::AppState;
use crate::auth::RequireUser;
use crate::db;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/history", get(list_history).delete(clear_history))
        .route("/history/{id}", get(get_history).delete(delete_history))
        .route(
            "/history/{id}/share",
            post(create_share).delete(revoke_share),
        )
        .route("/shared/{token}", get(get_shared))
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

async fn create_share(
    State(state): State<AppState>,
    RequireUser(user): RequireUser,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let candidate = generate_share_token();
    match db::get_or_set_share_token(&state.pool, user.id, id, &candidate).await {
        Ok(Some(token)) => Ok(Json(json!({ "token": token }))),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn revoke_share(
    State(state): State<AppState>,
    RequireUser(user): RequireUser,
    Path(id): Path<i64>,
) -> Result<StatusCode, StatusCode> {
    match db::clear_share_token(&state.pool, user.id, id).await {
        Ok(true) => Ok(StatusCode::NO_CONTENT),
        Ok(false) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn get_shared(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<Json<db::SearchHistoryDetail>, StatusCode> {
    match db::get_search_history_by_share_token(&state.pool, &token).await {
        Ok(Some(item)) => Ok(Json(item)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

const SHARE_TOKEN_ALPHABET: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";
const SHARE_TOKEN_LEN: usize = 12;

fn generate_share_token() -> String {
    use rand::RngExt;
    let mut rng = rand::rng();
    (0..SHARE_TOKEN_LEN)
        .map(|_| {
            let i = rng.random_range(0..SHARE_TOKEN_ALPHABET.len());
            SHARE_TOKEN_ALPHABET[i] as char
        })
        .collect()
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

    #[sqlx::test(migrations = "./migrations")]
    async fn share_creates_token(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let id = seed_history(&pool, 1, "anything").await;

        let (status, body) = post_json(
            test_app(pool.clone()),
            &format!("/api/history/{}/share", id),
            &json!({}),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let share_token = body["token"].as_str().unwrap();
        assert_eq!(share_token.len(), 12);

        // Token is queryable from public endpoint.
        let (status, body) = get_json(
            test_app(pool),
            &format!("/api/shared/{}", share_token),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["prompt"], "anything");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn share_is_idempotent(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let id = seed_history(&pool, 1, "anything").await;

        let (_, body1) = post_json(
            test_app(pool.clone()),
            &format!("/api/history/{}/share", id),
            &json!({}),
            Some(&token),
        )
        .await;
        let t1 = body1["token"].as_str().unwrap().to_string();

        let (_, body2) = post_json(
            test_app(pool),
            &format!("/api/history/{}/share", id),
            &json!({}),
            Some(&token),
        )
        .await;
        let t2 = body2["token"].as_str().unwrap().to_string();

        assert_eq!(t1, t2, "second share should reuse the same token");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn share_owner_only(pool: SqlitePool) {
        let token_a = register(&pool, "alice").await;
        let token_b = register(&pool, "bob").await;
        let id = seed_history(&pool, 1, "alice's").await;

        // Bob cannot share alice's history.
        let (status, _) = post_json(
            test_app(pool.clone()),
            &format!("/api/history/{}/share", id),
            &json!({}),
            Some(&token_b),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Bob cannot revoke alice's share either (after alice shares).
        let (_, _) = post_json(
            test_app(pool.clone()),
            &format!("/api/history/{}/share", id),
            &json!({}),
            Some(&token_a),
        )
        .await;
        let (status, _) = delete_json(
            test_app(pool),
            &format!("/api/history/{}/share", id),
            Some(&token_b),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn share_unauthenticated_blocked(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let id = seed_history(&pool, 1, "anything").await;
        let _ = token;

        let (status, _) = post_json(
            test_app(pool.clone()),
            &format!("/api/history/{}/share", id),
            &json!({}),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let (status, _) = delete_json(
            test_app(pool),
            &format!("/api/history/{}/share", id),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn unshare_invalidates_link(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let id = seed_history(&pool, 1, "anything").await;

        let (_, body) = post_json(
            test_app(pool.clone()),
            &format!("/api/history/{}/share", id),
            &json!({}),
            Some(&token),
        )
        .await;
        let share_token = body["token"].as_str().unwrap().to_string();

        // Revoke.
        let (status, _) = delete_json(
            test_app(pool.clone()),
            &format!("/api/history/{}/share", id),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Public endpoint now 404s.
        let (status, _) = get_json(
            test_app(pool.clone()),
            &format!("/api/shared/{}", share_token),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Re-sharing yields a fresh token (old one stays dead).
        let (_, body) = post_json(
            test_app(pool),
            &format!("/api/history/{}/share", id),
            &json!({}),
            Some(&token),
        )
        .await;
        let new_token = body["token"].as_str().unwrap();
        assert_ne!(new_token, share_token);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn deleting_history_invalidates_share(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let id = seed_history(&pool, 1, "anything").await;

        let (_, body) = post_json(
            test_app(pool.clone()),
            &format!("/api/history/{}/share", id),
            &json!({}),
            Some(&token),
        )
        .await;
        let share_token = body["token"].as_str().unwrap().to_string();

        // Delete the underlying history row.
        let (status, _) = delete_json(
            test_app(pool.clone()),
            &format!("/api/history/{}", id),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        // Public endpoint now 404s.
        let (status, _) = get_json(
            test_app(pool),
            &format!("/api/shared/{}", share_token),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn shared_endpoint_404s_for_unknown_token(pool: SqlitePool) {
        let (status, _) = get_json(
            test_app(pool),
            "/api/shared/this_does_not_exist",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
