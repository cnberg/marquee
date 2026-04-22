use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MediaDir {
    pub id: i64,
    pub dir_path: String,
    pub dir_name: String,
    pub scan_status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Movie {
    pub id: i64,
    pub tmdb_id: i64,
    pub title: String,
    pub original_title: Option<String>,
    pub year: Option<i64>,
    pub overview: Option<String>,
    pub poster_url: Option<String>,
    pub genres: Option<String>,
    pub country: Option<String>,
    pub language: Option<String>,
    pub runtime: Option<i64>,
    pub director: Option<String>,
    pub director_info: Option<String>,
    pub cast: Option<String>,
    pub tmdb_rating: Option<f64>,
    pub tmdb_votes: Option<i64>,
    pub keywords: Option<String>,
    pub llm_tags: Option<String>,
    pub budget: Option<i64>,
    pub revenue: Option<i64>,
    pub popularity: Option<f64>,
    // 双语字段
    pub title_zh: Option<String>,
    pub title_en: Option<String>,
    pub overview_zh: Option<String>,
    pub overview_en: Option<String>,
    pub tagline_zh: Option<String>,
    pub tagline_en: Option<String>,
    pub genres_zh: Option<String>,
    pub genres_en: Option<String>,
    // 新标量字段
    pub imdb_id: Option<String>,
    pub backdrop_path: Option<String>,
    pub homepage: Option<String>,
    pub status: Option<String>,
    pub collection: Option<String>,
    pub production_companies: Option<String>,
    pub spoken_languages: Option<String>,
    pub origin_country: Option<String>,
    // 来源
    pub source: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieCredit {
    pub id: i64,
    pub movie_id: i64,
    pub tmdb_person_id: i64,
    pub person_name: String,
    pub credit_type: String,
    pub role: Option<String>,
    pub department: Option<String>,
    pub order: Option<i64>,
    pub profile_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieImage {
    pub id: i64,
    pub movie_id: i64,
    pub image_type: String,
    pub file_path: String,
    pub iso_639_1: Option<String>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub vote_average: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieVideo {
    pub id: i64,
    pub movie_id: i64,
    pub video_key: String,
    pub site: Option<String>,
    pub video_type: Option<String>,
    pub name: Option<String>,
    pub iso_639_1: Option<String>,
    pub official: Option<i64>,
    pub published_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieReview {
    pub id: i64,
    pub movie_id: i64,
    pub tmdb_review_id: String,
    pub author: Option<String>,
    pub author_username: Option<String>,
    pub content: Option<String>,
    pub rating: Option<f64>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieReleaseDate {
    pub id: i64,
    pub movie_id: i64,
    pub iso_3166_1: String,
    pub release_date: Option<String>,
    pub certification: Option<String>,
    pub release_type: Option<i64>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieWatchProvider {
    pub id: i64,
    pub movie_id: i64,
    pub iso_3166_1: String,
    pub provider_id: i64,
    pub provider_name: Option<String>,
    pub logo_path: Option<String>,
    pub provider_type: String,
    pub display_priority: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieExternalId {
    pub id: i64,
    pub movie_id: i64,
    pub imdb_id: Option<String>,
    pub facebook_id: Option<String>,
    pub instagram_id: Option<String>,
    pub twitter_id: Option<String>,
    pub wikidata_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieAlternativeTitle {
    pub id: i64,
    pub movie_id: i64,
    pub iso_3166_1: Option<String>,
    pub title: String,
    pub title_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieTranslation {
    pub id: i64,
    pub movie_id: i64,
    pub iso_639_1: String,
    pub iso_3166_1: Option<String>,
    pub language_name: Option<String>,
    pub title: Option<String>,
    pub overview: Option<String>,
    pub tagline: Option<String>,
    pub homepage: Option<String>,
    pub runtime: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct RelatedMovie {
    pub id: i64,
    pub movie_id: i64,
    pub related_tmdb_id: i64,
    pub relation_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieList {
    pub id: i64,
    pub movie_id: i64,
    pub tmdb_list_id: i64,
    pub list_name: Option<String>,
    pub description: Option<String>,
    pub item_count: Option<i64>,
    pub iso_639_1: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Person {
    pub id: i64,
    pub tmdb_person_id: i64,
    pub name: String,
    pub also_known_as: Option<String>,
    pub biography: Option<String>,
    pub profile_path: Option<String>,
    pub birthday: Option<String>,
    pub deathday: Option<String>,
    pub place_of_birth: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DirMovieMapping {
    pub id: i64,
    pub dir_id: i64,
    pub movie_id: Option<i64>,
    pub match_status: String,
    pub confidence: Option<f64>,
    pub candidates: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Task {
    pub id: i64,
    pub task_type: String,
    pub payload: Option<String>,
    pub status: String,
    pub retries: i64,
    pub max_retries: i64,
    pub error_msg: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct SearchHistoryItem {
    pub id: i64,
    pub prompt: String,
    pub result_count: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct SearchHistoryDetail {
    pub id: i64,
    pub prompt: String,
    pub sse_events: String,
    pub result_count: i64,
    pub created_at: String,
}
