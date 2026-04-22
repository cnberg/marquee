use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::{api::AppState, db};

#[derive(Debug, Serialize)]
pub struct PersonResponse {
    pub tmdb_person_id: i64,
    pub name: String,
    pub also_known_as: Vec<String>,
    pub biography: Option<String>,
    pub profile_url: Option<String>,
    pub birthday: Option<String>,
    pub deathday: Option<String>,
    pub place_of_birth: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PersonMoviesParams {
    #[serde(default)]
    pub role: Option<String>, // "cast" or "director", default cast
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_per_page")]
    pub per_page: i64,
}

fn default_page() -> i64 {
    1
}

fn default_per_page() -> i64 {
    40
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/persons/{tmdb_person_id}", get(get_person))
        .route("/persons/{tmdb_person_id}/movies", get(person_movies))
}

async fn get_person(
    State(state): State<AppState>,
    Path(tmdb_person_id): Path<i64>,
) -> Result<Json<PersonResponse>, StatusCode> {
    if let Ok(Some(cached)) = db::get_person_by_tmdb_id(&state.pool, tmdb_person_id).await {
        if cached.biography.is_some() {
            return Ok(Json(person_to_response(cached)));
        }
    }

    let tmdb_client = crate::tmdb::client::TmdbClient::new(
        &state.config.tmdb.api_key,
        &state.config.tmdb.language,
        state.config.tmdb.proxy.as_deref(),
    );

    let detail = tmdb_client
        .get_person_detail(tmdb_person_id)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    let also_known_as_json = detail
        .also_known_as
        .as_ref()
        .map(|v| serde_json::to_string(v).unwrap_or_default());

    let _ = db::upsert_person(
        &state.pool,
        tmdb_person_id,
        &detail.name,
        also_known_as_json.as_deref(),
        detail.biography.as_deref(),
        detail.profile_path.as_deref(),
        detail.birthday.as_deref(),
        detail.deathday.as_deref(),
        detail.place_of_birth.as_deref(),
    )
    .await;

    let person = db::get_person_by_tmdb_id(&state.pool, tmdb_person_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(person_to_response(person)))
}

fn person_to_response(p: db::Person) -> PersonResponse {
    let also_known_as: Vec<String> = p
        .also_known_as
        .as_ref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    let profile_url = p
        .profile_path
        .as_ref()
        .map(|path| format!("https://image.tmdb.org/t/p/w500{}", path));

    PersonResponse {
        tmdb_person_id: p.tmdb_person_id,
        name: p.name,
        also_known_as,
        biography: p.biography,
        profile_url,
        birthday: p.birthday,
        deathday: p.deathday,
        place_of_birth: p.place_of_birth,
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::{get_json, test_app};
    use axum::http::StatusCode;
    use sqlx::SqlitePool;

    async fn seed_person_and_movie(pool: &SqlitePool) -> (i64, i64) {
        // Person
        crate::db::upsert_person(
            pool,
            500,
            "Nolan",
            Some("[\"Christopher Nolan\"]"),
            Some("Director bio"),
            Some("/nolan.jpg"),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // Movie with matched dir
        let movie_id = sqlx::query(
            "INSERT INTO movies (tmdb_id, title, year, director) VALUES (27205, 'Inception', 2010, 'Nolan')",
        )
        .execute(pool)
        .await
        .unwrap()
        .last_insert_rowid();

        let dir_id = crate::db::insert_media_dir(pool, "/m/Inception", "Inception")
            .await
            .unwrap();
        crate::db::update_dir_status(pool, dir_id, "matched")
            .await
            .unwrap();
        crate::db::insert_mapping(pool, dir_id, Some(movie_id), "auto", Some(0.9), None)
            .await
            .unwrap();

        // Credit row
        crate::db::replace_movie_credits(
            pool,
            movie_id,
            &[crate::db::queries::CreditRow {
                tmdb_person_id: 500,
                person_name: "Nolan".into(),
                credit_type: "director".into(),
                role: Some("Director".into()),
                department: Some("Directing".into()),
                order: Some(0),
                profile_path: None,
            }],
        )
        .await
        .unwrap();

        (500, movie_id)
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_person_returns_cached(pool: SqlitePool) {
        seed_person_and_movie(&pool).await;

        let (status, body) = get_json(
            test_app(pool),
            "/api/persons/500",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["name"], "Nolan");
        assert_eq!(body["tmdb_person_id"], 500);
        assert!(body["biography"].as_str().unwrap().contains("bio"));
        assert!(body["profile_url"].as_str().unwrap().contains("nolan.jpg"));
        assert_eq!(body["also_known_as"][0], "Christopher Nolan");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn person_movies_lists_directed_films(pool: SqlitePool) {
        seed_person_and_movie(&pool).await;

        let (status, body) = get_json(
            test_app(pool),
            "/api/persons/500/movies?role=director",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"].as_i64().unwrap(), 1);
        assert_eq!(body["data"][0]["title"], "Inception");
    }
}

async fn person_movies(
    State(state): State<AppState>,
    Path(tmdb_person_id): Path<i64>,
    Query(params): Query<PersonMoviesParams>,
) -> Result<Json<crate::api::common::ListResponse<Vec<db::Movie>>>, StatusCode> {
    let role = params.role.as_deref().unwrap_or("cast");

    let person_name = if let Ok(Some(person)) = db::get_person_by_tmdb_id(&state.pool, tmdb_person_id).await {
        person.name
    } else {
        let tmdb_client = crate::tmdb::client::TmdbClient::new(
            &state.config.tmdb.api_key,
            &state.config.tmdb.language,
            state.config.tmdb.proxy.as_deref(),
        );

        match tmdb_client.get_person_detail(tmdb_person_id).await {
            Ok(detail) => {
                let _ = db::upsert_person(
                    &state.pool,
                    tmdb_person_id,
                    &detail.name,
                    None,
                    detail.biography.as_deref(),
                    detail.profile_path.as_deref(),
                    detail.birthday.as_deref(),
                    detail.deathday.as_deref(),
                    detail.place_of_birth.as_deref(),
                )
                .await;
                detail.name
            }
            Err(_) => return Err(StatusCode::NOT_FOUND),
        }
    };

    let (movies, total) = db::list_movies_by_person_name(
        &state.pool,
        &person_name,
        role,
        params.page,
        params.per_page,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(crate::api::common::ListResponse {
        data: movies,
        page: params.page.max(1),
        per_page: params.per_page.max(1),
        total,
    }))
}
