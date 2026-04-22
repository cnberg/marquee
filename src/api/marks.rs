use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post, put},
    Json, Router,
};
use sqlx::QueryBuilder;
use serde::{Deserialize, Serialize};

use crate::api::AppState;
use crate::auth::{OptionalUser, RequireUser};
use crate::db::SqlitePool;

#[derive(Serialize)]
pub struct MarksResponse {
    pub want: bool,
    pub watched: bool,
    pub favorite: bool,
}

#[derive(Deserialize)]
pub struct BatchRequest {
    pub movie_ids: Vec<i64>,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/movies/{id}/marks", get(get_marks))
        .route("/movies/{id}/marks/{mark_type}", put(set_mark).delete(remove_mark))
        .route("/marks/batch", post(batch_marks))
        .route("/marks/movies", get(list_my_marked_movies))
}

async fn get_marks(
    State(state): State<AppState>,
    Path(movie_id): Path<i64>,
    OptionalUser(user): OptionalUser,
) -> Json<MarksResponse> {
    match user {
        Some(u) => {
            let marks = fetch_marks(&state.pool, u.id, movie_id)
                .await
                .unwrap_or_else(|_| empty_marks());
            Json(marks)
        }
        None => Json(empty_marks()),
    }
}

#[derive(Deserialize)]
pub struct MarkedMoviesQuery {
    #[serde(rename = "type")]
    pub mark_type: String,
}

async fn list_my_marked_movies(
    State(state): State<AppState>,
    RequireUser(user): RequireUser,
    Query(q): Query<MarkedMoviesQuery>,
) -> Result<Json<Vec<crate::db::Movie>>, StatusCode> {
    let normalized = normalize_mark_type(&q.mark_type).ok_or(StatusCode::BAD_REQUEST)?;
    crate::db::list_marked_movies(&state.pool, user.id, normalized)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn set_mark(
    State(state): State<AppState>,
    Path((movie_id, mark_type)): Path<(i64, String)>,
    RequireUser(user): RequireUser,
) -> Result<Json<MarksResponse>, StatusCode> {
    let mark_type = normalize_mark_type(&mark_type).ok_or(StatusCode::BAD_REQUEST)?;

    if mark_type == "want" {
        sqlx::query(
            "DELETE FROM user_movie_marks WHERE user_id = ? AND movie_id = ? AND mark_type = 'watched'",
        )
        .bind(user.id)
        .bind(movie_id)
        .execute(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    } else if mark_type == "watched" {
        sqlx::query(
            "DELETE FROM user_movie_marks WHERE user_id = ? AND movie_id = ? AND mark_type = 'want'",
        )
        .bind(user.id)
        .bind(movie_id)
        .execute(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    sqlx::query(
        "INSERT OR IGNORE INTO user_movie_marks (user_id, movie_id, mark_type) VALUES (?, ?, ?)",
    )
    .bind(user.id)
    .bind(movie_id)
    .bind(mark_type)
    .execute(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let marks = fetch_marks(&state.pool, user.id, movie_id).await?;
    Ok(Json(marks))
}

async fn remove_mark(
    State(state): State<AppState>,
    Path((movie_id, mark_type)): Path<(i64, String)>,
    RequireUser(user): RequireUser,
) -> Result<Json<MarksResponse>, StatusCode> {
    let mark_type = normalize_mark_type(&mark_type).ok_or(StatusCode::BAD_REQUEST)?;

    sqlx::query("DELETE FROM user_movie_marks WHERE user_id = ? AND movie_id = ? AND mark_type = ?")
        .bind(user.id)
        .bind(movie_id)
        .bind(mark_type)
        .execute(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let marks = fetch_marks(&state.pool, user.id, movie_id).await?;
    Ok(Json(marks))
}

async fn batch_marks(
    State(state): State<AppState>,
    RequireUser(user): RequireUser,
    Json(body): Json<BatchRequest>,
) -> Json<HashMap<i64, MarksResponse>> {
    let mut result: HashMap<i64, MarksResponse> = HashMap::new();
    for id in &body.movie_ids {
        result.insert(*id, empty_marks());
    }

    if body.movie_ids.is_empty() {
        return Json(result);
    }

    let mut builder = QueryBuilder::<sqlx::Sqlite>::new(
        "SELECT movie_id, mark_type FROM user_movie_marks WHERE user_id = ",
    );
    builder.push_bind(user.id);
    builder.push(" AND movie_id IN (");
    let mut separated = builder.separated(", ");
    for id in &body.movie_ids {
        separated.push_bind(id);
    }
    builder.push(")");

    let rows = builder
        .build_query_as::<(i64, String)>()
        .fetch_all(&state.pool)
        .await
        .unwrap_or_default();

    for (movie_id, mark_type) in rows {
        if let Some(entry) = result.get_mut(&movie_id) {
            match mark_type.as_str() {
                "want" => entry.want = true,
                "watched" => entry.watched = true,
                "favorite" => entry.favorite = true,
                _ => {}
            }
        }
    }

    Json(result)
}

fn empty_marks() -> MarksResponse {
    MarksResponse {
        want: false,
        watched: false,
        favorite: false,
    }
}

fn normalize_mark_type(mark_type: &str) -> Option<&'static str> {
    match mark_type {
        "want" => Some("want"),
        "watched" => Some("watched"),
        "favorite" => Some("favorite"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::{delete_json, get_json, post_json, put_json, test_app};
    use axum::http::StatusCode;
    use serde_json::json;
    use sqlx::SqlitePool;

    /// Register a user and return the JWT token.
    async fn register(pool: &SqlitePool, username: &str) -> String {
        let (status, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": username, "password": "pw" }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        body["token"].as_str().unwrap().to_string()
    }

    /// Insert a minimal movie row and return its id.
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
    async fn set_mark_requires_auth(pool: SqlitePool) {
        let movie_id = seed_movie(&pool, 1, "A").await;
        let (status, _) = put_json(
            test_app(pool),
            &format!("/api/movies/{}/marks/want", movie_id),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn set_mark_invalid_type_returns_400(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let movie_id = seed_movie(&pool, 1, "A").await;
        let (status, _) = put_json(
            test_app(pool),
            &format!("/api/movies/{}/marks/love", movie_id),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn want_and_watched_are_mutually_exclusive(pool: SqlitePool) {
        let token = register(&pool, "bob").await;
        let mid = seed_movie(&pool, 1, "Film").await;

        // Set 'want'.
        let (status, body) = put_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/want", mid),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["want"].as_bool().unwrap());
        assert!(!body["watched"].as_bool().unwrap());

        // Set 'watched' → should auto-remove 'want'.
        let (status, body) = put_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/watched", mid),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["watched"].as_bool().unwrap());
        assert!(!body["want"].as_bool().unwrap(), "want should be cleared");

        // Set 'want' again → should auto-remove 'watched'.
        let (_, body) = put_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/want", mid),
            Some(&token),
        )
        .await;
        assert!(body["want"].as_bool().unwrap());
        assert!(!body["watched"].as_bool().unwrap());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn favorite_is_independent_of_want_watched(pool: SqlitePool) {
        let token = register(&pool, "carol").await;
        let mid = seed_movie(&pool, 1, "Film").await;

        // Set want + favorite.
        put_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/want", mid),
            Some(&token),
        )
        .await;
        let (_, body) = put_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/favorite", mid),
            Some(&token),
        )
        .await;
        assert!(body["want"].as_bool().unwrap());
        assert!(body["favorite"].as_bool().unwrap());

        // Switch to watched → favorite should remain.
        let (_, body) = put_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/watched", mid),
            Some(&token),
        )
        .await;
        assert!(body["watched"].as_bool().unwrap());
        assert!(body["favorite"].as_bool().unwrap());
        assert!(!body["want"].as_bool().unwrap());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn remove_mark_clears_only_that_type(pool: SqlitePool) {
        let token = register(&pool, "dave").await;
        let mid = seed_movie(&pool, 1, "Film").await;

        put_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/watched", mid),
            Some(&token),
        )
        .await;
        put_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/favorite", mid),
            Some(&token),
        )
        .await;

        let (status, body) = delete_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/watched", mid),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(!body["watched"].as_bool().unwrap());
        assert!(body["favorite"].as_bool().unwrap(), "favorite untouched");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_marks_anonymous_returns_all_false(pool: SqlitePool) {
        let mid = seed_movie(&pool, 1, "Film").await;
        let (status, body) = get_json(
            test_app(pool),
            &format!("/api/movies/{}/marks", mid),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(!body["want"].as_bool().unwrap());
        assert!(!body["watched"].as_bool().unwrap());
        assert!(!body["favorite"].as_bool().unwrap());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn batch_marks_returns_per_movie_status(pool: SqlitePool) {
        let token = register(&pool, "eve").await;
        let m1 = seed_movie(&pool, 1, "A").await;
        let m2 = seed_movie(&pool, 2, "B").await;
        let m3 = seed_movie(&pool, 3, "C").await;

        put_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/want", m1),
            Some(&token),
        )
        .await;
        put_json(
            test_app(pool.clone()),
            &format!("/api/movies/{}/marks/watched", m2),
            Some(&token),
        )
        .await;

        let (status, body) = post_json(
            test_app(pool.clone()),
            "/api/marks/batch",
            &json!({ "movie_ids": [m1, m2, m3] }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let m1s = &body[m1.to_string()];
        assert!(m1s["want"].as_bool().unwrap());
        assert!(!m1s["watched"].as_bool().unwrap());

        let m2s = &body[m2.to_string()];
        assert!(!m2s["want"].as_bool().unwrap());
        assert!(m2s["watched"].as_bool().unwrap());

        let m3s = &body[m3.to_string()];
        assert!(!m3s["want"].as_bool().unwrap());
        assert!(!m3s["watched"].as_bool().unwrap());
        assert!(!m3s["favorite"].as_bool().unwrap());
    }
}

async fn fetch_marks(
    pool: &SqlitePool,
    user_id: i64,
    movie_id: i64,
) -> Result<MarksResponse, StatusCode> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT mark_type FROM user_movie_marks WHERE user_id = ? AND movie_id = ?",
    )
    .bind(user_id)
    .bind(movie_id)
    .fetch_all(pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut marks = empty_marks();
    for mark in rows {
        match mark.as_str() {
            "want" => marks.want = true,
            "watched" => marks.watched = true,
            "favorite" => marks.favorite = true,
            _ => {}
        }
    }

    Ok(marks)
}
