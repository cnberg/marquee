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
    pub director_info_en: Option<String>,
    pub cast_en: Option<String>,
    pub keywords_en: Option<String>,
    pub collection_en: Option<String>,
    pub production_companies_en: Option<String>,
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
    pub person_name_en: Option<String>,
    pub role_en: Option<String>,
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
    /// Present only when the row has been opted into sharing by its owner.
    /// Public `/api/shared/:token` lookups also include this (the caller
    /// already holds the token, so echoing it leaks nothing).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct BenchmarkQuery {
    pub id: i64,
    pub query: String,
    pub note: Option<String>,
    pub expected_ids: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub source_history_id: Option<i64>,
    /// 「不应包含」标准答案——picks 中出现任何此 id 即 hit=false（硬否决）。
    pub not_expected_ids: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct BenchmarkRun {
    pub id: i64,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub status: String,
    pub total: i64,
    pub passed: i64,
    pub failed: i64,
    pub note: Option<String>,
    pub is_baseline: i64,
    pub cancel_requested: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct BenchmarkResult {
    pub id: i64,
    pub run_id: i64,
    pub query_id: i64,
    pub query_snapshot: String,
    pub expected_ids: Option<String>,
    pub top_movie_ids: String,
    pub intent_json: Option<String>,
    pub hit: Option<i64>,
    pub elapsed_ms: Option<i64>,
    pub error: Option<String>,
    pub not_expected_ids: Option<String>,
    /// Recall@K（分母 min(expected.len(), 10)）。expected 空时 NULL。
    pub coverage_ratio: Option<f64>,
}

/// 人物在一部电影里的"最重要身份"。用于 person handler 的排序加权和推荐语生成。
/// `uncredited` / `stunt double` 被过滤掉，不会出现在这里。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersonRoleKind {
    /// 导演（crew + role=Director 或 department=Directing）
    Director,
    /// 主演（cast + order 小 + 非配音非临演）
    LeadActor,
    /// 配角（cast + order 较大 + 非配音非临演）
    SupportingActor,
    /// 配音（cast + role 含 voice）
    Voice,
    /// 制片 / 编剧 / 其他 crew（不含 Director）
    Crew,
}

/// person handler 返回的一条记录：电影本体 + 该人在片中的"代表身份"。
/// `role_detail` 是原始的 role 文本（比如 "Keung" / "Executive Producer" / "Monkey (voice)"），
/// 前端展示推荐语时用来组装出 "{name} 饰 Keung" / "{name} 为 Monkey 配音" 这类短语。
#[derive(Debug, Clone)]
pub struct PersonWork {
    pub movie: Movie,
    pub role_kind: PersonRoleKind,
    pub role_detail: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TorrentInfo {
    pub id: i64,
    pub media_dir_id: i64,
    pub torrent_hash: String,
    pub state: String,
    pub progress: f64,
    pub size: Option<i64>,
    pub dlspeed: Option<i64>,
    pub upspeed: Option<i64>,
    pub ratio: Option<f64>,
    pub seeds: Option<i64>,
    pub added_on: Option<i64>,
    pub updated_at: String,
    pub media_type: String,
    pub torrent_name: String,
}

/// Download status for a movie, aggregated from its associated torrent_info records.
#[derive(Debug, Clone, Serialize)]
pub struct DownloadStatus {
    pub state: String,
    pub progress: f64,
    pub dlspeed: i64,
    pub size: Option<i64>,
    pub media_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DoubanImport {
    pub id: i64,
    pub user_id: i64,
    pub douban_subject_id: String,
    pub raw_title: String,
    pub parsed_title_zh: Option<String>,
    pub parsed_title_en: Option<String>,
    pub year: Option<i64>,
    pub country: Option<String>,
    pub douban_url: String,
    pub status: String,
    pub movie_id: Option<i64>,
    pub error_msg: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}
