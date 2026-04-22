use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::api::AppState;
use crate::auth::{jwt, password, RequireUser};

#[derive(Deserialize)]
pub struct AuthRequest {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    pub token: String,
    pub user: UserInfo,
}

#[derive(Serialize)]
pub struct UserInfo {
    pub id: i64,
    pub username: String,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        .route("/auth/me", get(me))
}

async fn register(
    State(state): State<AppState>,
    Json(body): Json<AuthRequest>,
) -> Result<(StatusCode, Json<AuthResponse>), StatusCode> {
    if body.username.trim().is_empty() || body.password.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let password_hash = password::hash_password(&body.password)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result = sqlx::query("INSERT INTO users (username, password_hash) VALUES (?, ?)")
        .bind(&body.username)
        .bind(password_hash)
        .execute(&state.pool)
        .await;

    let query_result = match result {
        Ok(res) => res,
        Err(err) => {
            if let sqlx::Error::Database(db_err) = &err {
                if db_err.message().contains("UNIQUE") {
                    return Err(StatusCode::CONFLICT);
                }
            }
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let user_id = query_result.last_insert_rowid();
    let token = jwt::create_token(
        user_id,
        &body.username,
        &state.config.auth.jwt_secret,
        state.config.auth.jwt_expiry_days,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let resp = AuthResponse {
        token,
        user: UserInfo {
            id: user_id,
            username: body.username,
        },
    };

    Ok((StatusCode::CREATED, Json(resp)))
}

async fn login(
    State(state): State<AppState>,
    Json(body): Json<AuthRequest>,
) -> Result<Json<AuthResponse>, StatusCode> {
    let row = sqlx::query_as::<_, (i64, String, String)>(
        "SELECT id, username, password_hash FROM users WHERE username = ?",
    )
    .bind(&body.username)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let Some((user_id, username, password_hash)) = row else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    let verified = password::verify_password(&body.password, &password_hash)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if !verified {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let token = jwt::create_token(
        user_id,
        &username,
        &state.config.auth.jwt_secret,
        state.config.auth.jwt_expiry_days,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(AuthResponse {
        token,
        user: UserInfo { id: user_id, username },
    }))
}

async fn me(RequireUser(user): RequireUser) -> Json<UserInfo> {
    Json(UserInfo {
        id: user.id,
        username: user.username,
    })
}

#[cfg(test)]
mod tests {
    use crate::test_support::{get_json, post_json, test_app};
    use axum::http::StatusCode;
    use serde_json::json;
    use sqlx::SqlitePool;

    #[sqlx::test(migrations = "./migrations")]
    async fn register_creates_user_and_returns_token(pool: SqlitePool) {
        let app = test_app(pool.clone());
        let (status, body) = post_json(
            app,
            "/api/auth/register",
            &json!({ "username": "alice", "password": "s3cret!" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert!(body["token"].as_str().unwrap().len() > 20);
        assert_eq!(body["user"]["username"], "alice");
        assert!(body["user"]["id"].as_i64().unwrap() > 0);

        // A row exists in the users table with a hashed password.
        let (username, hash): (String, String) =
            sqlx::query_as("SELECT username, password_hash FROM users WHERE username = ?")
                .bind("alice")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(username, "alice");
        assert!(hash.starts_with("$argon2"));
        assert!(!hash.contains("s3cret"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn register_rejects_empty_fields(pool: SqlitePool) {
        let (status, _) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": "", "password": "pw" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _) = post_json(
            test_app(pool),
            "/api/auth/register",
            &json!({ "username": "bob", "password": "   " }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn register_duplicate_username_returns_conflict(pool: SqlitePool) {
        let (status, _) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": "alice", "password": "pw1" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, _) = post_json(
            test_app(pool),
            "/api/auth/register",
            &json!({ "username": "alice", "password": "pw2" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn login_roundtrip_and_wrong_password(pool: SqlitePool) {
        let (_, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": "carol", "password": "correct" }),
            None,
        )
        .await;
        assert_eq!(body["user"]["username"], "carol");

        // Correct password → 200 + new token.
        let (status, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/login",
            &json!({ "username": "carol", "password": "correct" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["token"].as_str().is_some());

        // Wrong password → 401.
        let (status, _) = post_json(
            test_app(pool.clone()),
            "/api/auth/login",
            &json!({ "username": "carol", "password": "wrong" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Unknown user → 401.
        let (status, _) = post_json(
            test_app(pool),
            "/api/auth/login",
            &json!({ "username": "nobody", "password": "any" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn me_requires_bearer_token(pool: SqlitePool) {
        // No header → 401.
        let (status, _) = get_json(test_app(pool.clone()), "/api/auth/me", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        // Register → use token → 200.
        let (_, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": "dave", "password": "pw" }),
            None,
        )
        .await;
        let token = body["token"].as_str().unwrap().to_string();

        let (status, body) = get_json(test_app(pool.clone()), "/api/auth/me", Some(&token)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["username"], "dave");

        // Garbage token → 401.
        let (status, _) = get_json(test_app(pool), "/api/auth/me", Some("not.a.jwt")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
}
