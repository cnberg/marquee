use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use super::common::{default_page, default_per_page, ListResponse};
use crate::{
    api::AppState,
    db::{self, Movie},
};

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
    pub similar: Option<Vec<db::RelatedMovie>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommendations: Option<Vec<db::RelatedMovie>>,
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
                    db::get_related_movies(&state.pool, id, "similar")
                        .await
                        .unwrap_or_default(),
                )
            } else {
                None
            };

            let recommendations = if has_include("recommendations") {
                Some(
                    db::get_related_movies(&state.pool, id, "recommendation")
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
sub_resource_handler!(movie_similar, db::get_related_movies, db::RelatedMovie, "similar");
sub_resource_handler!(movie_recommendations, db::get_related_movies, db::RelatedMovie, "recommendation");
sub_resource_handler!(movie_watch_providers, db::get_movie_watch_providers, db::MovieWatchProvider);
sub_resource_handler!(movie_release_dates, db::get_movie_release_dates, db::MovieReleaseDate);

async fn library_stats(
    State(state): State<AppState>,
) -> Json<db::queries::LibraryStats> {
    let stats = match db::get_library_stats(&state.pool).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("library_stats error: {e}");
            db::queries::LibraryStats {
                total: 0, decades: vec![], genres: vec![], countries: vec![], directors: vec![],
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

#[cfg(test)]
mod tests {
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
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn empty_library_returns_zero(pool: SqlitePool) {
        let (status, body) = get_json(test_app(pool), "/api/movies", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"], 0);
        assert_eq!(body["data"].as_array().unwrap().len(), 0);
    }
}
