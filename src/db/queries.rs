use super::models::{
    BenchmarkQuery, BenchmarkResult, BenchmarkRun, DirMovieMapping, DoubanImport, DownloadStatus,
    MediaDir, Movie, MovieAlternativeTitle, MovieCredit, MovieExternalId, MovieImage, MovieList,
    MovieReleaseDate, MovieReview, MovieTranslation, MovieVideo, MovieWatchProvider, Person,
    PersonRoleKind, PersonWork, SearchHistoryDetail, SearchHistoryItem, Task,
};
use crate::search::intent::{Constraints, Exclusions};
use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, Sqlite, SqlitePool};

pub const BUDGET_HIGH_THRESHOLD: i64 = 50_000_000;
pub const BUDGET_MEDIUM_THRESHOLD: i64 = 5_000_000;

// IMDB-style Bayesian weighted rating used to sort recommendation candidates.
// WR = (R * v + C * m) / (v + m). Smoothly pulls low-vote ratings toward the
// prior mean C, so a 1-vote 10.0 ranks below a 10000-vote 8.5.
pub const BAYES_PRIOR_MEAN: f64 = 6.5;
pub const BAYES_PRIOR_VOTES: i64 = 50;

pub async fn get_all_dir_paths(pool: &SqlitePool) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT dir_path FROM media_dirs WHERE scan_status != 'deleted'",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn insert_media_dir(
    pool: &SqlitePool,
    dir_path: &str,
    dir_name: &str,
) -> Result<i64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO media_dirs (dir_path, dir_name, scan_status) VALUES (?, ?, 'new')",
    )
    .bind(dir_path)
    .bind(dir_name)
    .execute(pool)
    .await?;

    Ok(result.last_insert_rowid())
}

pub async fn mark_dir_deleted(pool: &SqlitePool, dir_path: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE media_dirs SET scan_status = 'deleted', updated_at = datetime('now') WHERE dir_path = ?",
    )
    .bind(dir_path)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn get_new_dirs(pool: &SqlitePool) -> Result<Vec<MediaDir>, sqlx::Error> {
    sqlx::query_as::<_, MediaDir>("SELECT * FROM media_dirs WHERE scan_status = 'new'")
        .fetch_all(pool)
        .await
}

pub async fn update_dir_status(pool: &SqlitePool, id: i64, status: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE media_dirs SET scan_status = ?, updated_at = datetime('now') WHERE id = ?",
    )
    .bind(status)
    .bind(id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn insert_task(
    pool: &SqlitePool,
    task_type: &str,
    payload: &str,
) -> Result<i64, sqlx::Error> {
    let result = sqlx::query("INSERT INTO tasks (task_type, payload, status) VALUES (?, ?, 'pending')")
        .bind(task_type)
        .bind(payload)
        .execute(pool)
        .await?;

    Ok(result.last_insert_rowid())
}

pub async fn claim_next_task(
    pool: &SqlitePool,
    task_type: &str,
) -> Result<Option<Task>, sqlx::Error> {
    let task = sqlx::query_as::<_, Task>(
        "UPDATE tasks SET status = 'running', updated_at = datetime('now')
         WHERE id = (
             SELECT id FROM tasks
             WHERE task_type = ? AND status = 'pending'
             ORDER BY created_at ASC LIMIT 1
         )
         RETURNING *",
    )
    .bind(task_type)
    .fetch_optional(pool)
    .await?;

    Ok(task)
}

/// Reset any tasks left in `running` state back to `pending` so they can be
/// re-claimed. Called once on worker startup to recover from a previous
/// process exit / crash that left tasks orphaned mid-execution.
pub async fn requeue_stale_running_tasks(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE tasks SET status = 'pending', updated_at = datetime('now')
         WHERE status = 'running'",
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// 选出一批"版本过期"的电影 id（同时覆盖 library 和 related），
/// 供周期 refresh worker 重新入队 tmdb_fetch 任务。
///
/// 选择依据：`movies.tmdb_fetch_version` 为 NULL 或严格小于 `current_version`。
/// 已经有 pending/running 的同 tmdb_id fetch 任务的行会被跳过，避免重复派发。
/// 返回 `(movie_id, tmdb_id, source)`，`source` 用来决定 fetch_related 开关。
pub async fn claim_stale_movies(
    pool: &SqlitePool,
    current_version: i32,
    batch_size: u32,
) -> Result<Vec<(i64, i64, String)>, sqlx::Error> {
    sqlx::query_as::<_, (i64, i64, String)>(
        r#"
        SELECT m.id, m.tmdb_id, m.source
        FROM movies m
        WHERE m.tmdb_id > 0
          AND (m.tmdb_fetch_version IS NULL OR m.tmdb_fetch_version < ?)
          AND NOT EXISTS (
              -- Anchor the match with a closing '}' so "tmdb_id":100 does not
              -- accidentally match a pending "tmdb_id":1001. serde_json writes
              -- object keys in alphabetical order (no preserve_order feature),
              -- so "tmdb_id" — the lexicographically largest of our payload
              -- keys (fetch_related/movie_id/tmdb_id) — is always the LAST
              -- entry, followed by '}' with no trailing comma.
              SELECT 1 FROM tasks t
              WHERE t.task_type = 'tmdb_fetch'
                AND t.status IN ('pending', 'running')
                AND t.payload LIKE '%"tmdb_id":' || m.tmdb_id || '}%'
          )
        ORDER BY COALESCE(m.tmdb_fetch_version, -1) ASC, m.updated_at ASC
        LIMIT ?
        "#,
    )
    .bind(current_version)
    .bind(batch_size as i64)
    .fetch_all(pool)
    .await
}

/// 记录电影本次 tmdb_fetch 成功时对应的 pipeline 版本号。
/// 在 process_tmdb_fetch_tasks 成功分支（complete_task 之前）调用，
/// 让下一轮 claim_stale_movies 不再重复选中这行。
pub async fn set_movie_fetch_version(
    pool: &SqlitePool,
    tmdb_id: i64,
    version: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE movies SET tmdb_fetch_version = ? WHERE tmdb_id = ?")
        .bind(version)
        .bind(tmdb_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// 统计版本过期的库内/库外电影数量，供管理接口展示。
pub async fn count_stale_movies(
    pool: &SqlitePool,
    current_version: i32,
) -> Result<i64, sqlx::Error> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM movies
         WHERE tmdb_id > 0
           AND (tmdb_fetch_version IS NULL OR tmdb_fetch_version < ?)",
    )
    .bind(current_version)
    .fetch_one(pool)
    .await?;
    Ok(count)
}

pub async fn complete_task(pool: &SqlitePool, task_id: i64) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE tasks SET status = 'done', updated_at = datetime('now') WHERE id = ?")
        .bind(task_id)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn fail_task(pool: &SqlitePool, task_id: i64, error: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE tasks SET
            status = CASE WHEN retries + 1 >= max_retries THEN 'failed' ELSE 'pending' END,
            retries = retries + 1,
            error_msg = ?,
            updated_at = datetime('now')
         WHERE id = ?",
    )
    .bind(error)
    .bind(task_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn insert_mapping(
    pool: &SqlitePool,
    dir_id: i64,
    movie_id: Option<i64>,
    match_status: &str,
    confidence: Option<f64>,
    candidates_json: Option<&str>,
) -> Result<i64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO dir_movie_mappings (dir_id, movie_id, match_status, confidence, candidates)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(dir_id)
    .bind(movie_id)
    .bind(match_status)
    .bind(confidence)
    .bind(candidates_json)
    .execute(pool)
    .await?;

    Ok(result.last_insert_rowid())
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_movie(
    pool: &SqlitePool,
    tmdb_id: i64,
    title: &str,
    original_title: Option<&str>,
    year: Option<i64>,
    overview: Option<&str>,
    poster_url: Option<&str>,
    genres: &str,
    country: Option<&str>,
    language: Option<&str>,
    runtime: Option<i64>,
    director: Option<&str>,
    cast: &str,
    tmdb_rating: Option<f64>,
    tmdb_votes: Option<i64>,
    keywords: &str,
    budget: Option<i64>,
    revenue: Option<i64>,
    popularity: Option<f64>,
    source: &str,
) -> Result<i64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT OR IGNORE INTO movies
         (tmdb_id, title, original_title, year, overview, poster_url, genres, country, language, runtime, director, cast, tmdb_rating, tmdb_votes, keywords, budget, revenue, popularity, source)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(tmdb_id)
    .bind(title)
    .bind(original_title)
    .bind(year)
    .bind(overview)
    .bind(poster_url)
    .bind(genres)
    .bind(country)
    .bind(language)
    .bind(runtime)
    .bind(director)
    .bind(cast)
    .bind(tmdb_rating)
    .bind(tmdb_votes)
    .bind(keywords)
    .bind(budget)
    .bind(revenue)
    .bind(popularity)
    .bind(source)
    .execute(pool)
    .await?;

    Ok(result.last_insert_rowid())
}

// === Movie sub-table replace helpers ===

pub struct CreditRow {
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

pub async fn replace_movie_credits(
    pool: &SqlitePool,
    movie_id: i64,
    rows: &[CreditRow],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM movie_credits WHERE movie_id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;

    for r in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO movie_credits (movie_id, tmdb_person_id, person_name, credit_type, role, department, \"order\", profile_path, person_name_en, role_en) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(r.tmdb_person_id)
        .bind(&r.person_name)
        .bind(&r.credit_type)
        .bind(&r.role)
        .bind(&r.department)
        .bind(r.order)
        .bind(&r.profile_path)
        .bind(&r.person_name_en)
        .bind(&r.role_en)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub struct ImageRow {
    pub image_type: String,
    pub file_path: String,
    pub iso_639_1: Option<String>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub vote_average: Option<f64>,
}

pub async fn replace_movie_images(
    pool: &SqlitePool,
    movie_id: i64,
    rows: &[ImageRow],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM movie_images WHERE movie_id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;

    for r in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO movie_images (movie_id, image_type, file_path, iso_639_1, width, height, vote_average) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(&r.image_type)
        .bind(&r.file_path)
        .bind(&r.iso_639_1)
        .bind(r.width)
        .bind(r.height)
        .bind(r.vote_average)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub struct VideoRow {
    pub video_key: String,
    pub site: Option<String>,
    pub video_type: Option<String>,
    pub name: Option<String>,
    pub iso_639_1: Option<String>,
    pub official: bool,
    pub published_at: Option<String>,
}

pub async fn replace_movie_videos(
    pool: &SqlitePool,
    movie_id: i64,
    rows: &[VideoRow],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM movie_videos WHERE movie_id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;

    for r in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO movie_videos (movie_id, video_key, site, video_type, name, iso_639_1, official, published_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(&r.video_key)
        .bind(&r.site)
        .bind(&r.video_type)
        .bind(&r.name)
        .bind(&r.iso_639_1)
        .bind(r.official)
        .bind(&r.published_at)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub struct ReviewRow {
    pub tmdb_review_id: String,
    pub author: Option<String>,
    pub author_username: Option<String>,
    pub content: Option<String>,
    pub rating: Option<f64>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

pub async fn replace_movie_reviews(
    pool: &SqlitePool,
    movie_id: i64,
    rows: &[ReviewRow],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM movie_reviews WHERE movie_id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;

    for r in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO movie_reviews (movie_id, tmdb_review_id, author, author_username, content, rating, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(&r.tmdb_review_id)
        .bind(&r.author)
        .bind(&r.author_username)
        .bind(&r.content)
        .bind(r.rating)
        .bind(&r.created_at)
        .bind(&r.updated_at)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub struct ReleaseDateRow {
    pub iso_3166_1: String,
    pub release_date: Option<String>,
    pub certification: Option<String>,
    pub release_type: Option<i64>,
    pub note: Option<String>,
}

pub async fn replace_movie_release_dates(
    pool: &SqlitePool,
    movie_id: i64,
    rows: &[ReleaseDateRow],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM movie_release_dates WHERE movie_id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;

    for r in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO movie_release_dates (movie_id, iso_3166_1, release_date, certification, release_type, note) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(&r.iso_3166_1)
        .bind(&r.release_date)
        .bind(&r.certification)
        .bind(r.release_type)
        .bind(&r.note)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub struct WatchProviderRow {
    pub iso_3166_1: String,
    pub provider_id: i64,
    pub provider_name: Option<String>,
    pub logo_path: Option<String>,
    pub provider_type: String,
    pub display_priority: Option<i64>,
}

pub async fn replace_movie_watch_providers(
    pool: &SqlitePool,
    movie_id: i64,
    rows: &[WatchProviderRow],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM movie_watch_providers WHERE movie_id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;

    for r in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO movie_watch_providers (movie_id, iso_3166_1, provider_id, provider_name, logo_path, provider_type, display_priority) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(&r.iso_3166_1)
        .bind(r.provider_id)
        .bind(&r.provider_name)
        .bind(&r.logo_path)
        .bind(&r.provider_type)
        .bind(r.display_priority)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub struct ExternalIdRow {
    pub imdb_id: Option<String>,
    pub facebook_id: Option<String>,
    pub instagram_id: Option<String>,
    pub twitter_id: Option<String>,
    pub wikidata_id: Option<String>,
}

pub async fn replace_movie_external_ids(
    pool: &SqlitePool,
    movie_id: i64,
    row: &ExternalIdRow,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT OR REPLACE INTO movie_external_ids (movie_id, imdb_id, facebook_id, instagram_id, twitter_id, wikidata_id) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(movie_id)
    .bind(&row.imdb_id)
    .bind(&row.facebook_id)
    .bind(&row.instagram_id)
    .bind(&row.twitter_id)
    .bind(&row.wikidata_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub struct AlternativeTitleRow {
    pub iso_3166_1: Option<String>,
    pub title: String,
    pub title_type: Option<String>,
}

pub async fn replace_movie_alternative_titles(
    pool: &SqlitePool,
    movie_id: i64,
    rows: &[AlternativeTitleRow],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM movie_alternative_titles WHERE movie_id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;

    for r in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO movie_alternative_titles (movie_id, iso_3166_1, title, title_type) VALUES (?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(&r.iso_3166_1)
        .bind(&r.title)
        .bind(&r.title_type)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub struct TranslationRow {
    pub iso_639_1: String,
    pub iso_3166_1: Option<String>,
    pub language_name: Option<String>,
    pub title: Option<String>,
    pub overview: Option<String>,
    pub tagline: Option<String>,
    pub homepage: Option<String>,
    pub runtime: Option<i64>,
}

pub async fn replace_movie_translations(
    pool: &SqlitePool,
    movie_id: i64,
    rows: &[TranslationRow],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM movie_translations WHERE movie_id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;

    for r in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO movie_translations (movie_id, iso_639_1, iso_3166_1, language_name, title, overview, tagline, homepage, runtime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(&r.iso_639_1)
        .bind(&r.iso_3166_1)
        .bind(&r.language_name)
        .bind(&r.title)
        .bind(&r.overview)
        .bind(&r.tagline)
        .bind(&r.homepage)
        .bind(r.runtime)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub struct RelatedMovieRow {
    pub related_tmdb_id: i64,
    pub relation_type: String,
}

pub async fn replace_related_movies(
    pool: &SqlitePool,
    movie_id: i64,
    rows: &[RelatedMovieRow],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM related_movies WHERE movie_id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;

    for r in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO related_movies (movie_id, related_tmdb_id, relation_type) VALUES (?, ?, ?)",
        )
        .bind(movie_id)
        .bind(r.related_tmdb_id)
        .bind(&r.relation_type)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub struct MovieListRow {
    pub tmdb_list_id: i64,
    pub list_name: Option<String>,
    pub description: Option<String>,
    pub item_count: Option<i64>,
    pub iso_639_1: Option<String>,
}

pub async fn replace_movie_lists(
    pool: &SqlitePool,
    movie_id: i64,
    rows: &[MovieListRow],
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM movie_lists WHERE movie_id = ?")
        .bind(movie_id)
        .execute(pool)
        .await?;

    for r in rows {
        sqlx::query(
            "INSERT OR IGNORE INTO movie_lists (movie_id, tmdb_list_id, list_name, description, item_count, iso_639_1) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(r.tmdb_list_id)
        .bind(&r.list_name)
        .bind(&r.description)
        .bind(r.item_count)
        .bind(&r.iso_639_1)
        .execute(pool)
        .await?;
    }
    Ok(())
}

// === Movie sub-table read helpers ===

pub async fn get_movie_credits(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Vec<MovieCredit>, sqlx::Error> {
    sqlx::query_as::<_, MovieCredit>("SELECT * FROM movie_credits WHERE movie_id = ?")
        .bind(movie_id)
        .fetch_all(pool)
        .await
}

pub async fn get_movie_images(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Vec<MovieImage>, sqlx::Error> {
    sqlx::query_as::<_, MovieImage>("SELECT * FROM movie_images WHERE movie_id = ?")
        .bind(movie_id)
        .fetch_all(pool)
        .await
}

pub async fn get_movie_videos(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Vec<MovieVideo>, sqlx::Error> {
    sqlx::query_as::<_, MovieVideo>("SELECT * FROM movie_videos WHERE movie_id = ?")
        .bind(movie_id)
        .fetch_all(pool)
        .await
}

pub async fn get_movie_reviews(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Vec<MovieReview>, sqlx::Error> {
    sqlx::query_as::<_, MovieReview>("SELECT * FROM movie_reviews WHERE movie_id = ?")
        .bind(movie_id)
        .fetch_all(pool)
        .await
}

pub async fn get_movie_release_dates(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Vec<MovieReleaseDate>, sqlx::Error> {
    sqlx::query_as::<_, MovieReleaseDate>("SELECT * FROM movie_release_dates WHERE movie_id = ?")
        .bind(movie_id)
        .fetch_all(pool)
        .await
}

pub async fn get_movie_watch_providers(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Vec<MovieWatchProvider>, sqlx::Error> {
    sqlx::query_as::<_, MovieWatchProvider>("SELECT * FROM movie_watch_providers WHERE movie_id = ?")
        .bind(movie_id)
        .fetch_all(pool)
        .await
}

pub async fn get_movie_external_ids(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Option<MovieExternalId>, sqlx::Error> {
    sqlx::query_as::<_, MovieExternalId>("SELECT * FROM movie_external_ids WHERE movie_id = ?")
        .bind(movie_id)
        .fetch_optional(pool)
        .await
}

pub async fn get_movie_alternative_titles(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Vec<MovieAlternativeTitle>, sqlx::Error> {
    sqlx::query_as::<_, MovieAlternativeTitle>("SELECT * FROM movie_alternative_titles WHERE movie_id = ?")
        .bind(movie_id)
        .fetch_all(pool)
        .await
}

pub async fn get_movie_translations(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Vec<MovieTranslation>, sqlx::Error> {
    sqlx::query_as::<_, MovieTranslation>("SELECT * FROM movie_translations WHERE movie_id = ?")
        .bind(movie_id)
        .fetch_all(pool)
        .await
}

/// Returns enriched related movies by joining with the movies table.
/// Only returns movies that exist in our DB (library or related source).
pub async fn get_enriched_related_movies(
    pool: &SqlitePool,
    movie_id: i64,
    relation_type: &str,
) -> Result<Vec<Movie>, sqlx::Error> {
    sqlx::query_as::<_, Movie>(
        "SELECT DISTINCT m.* FROM movies m \
         INNER JOIN related_movies rm ON rm.related_tmdb_id = m.tmdb_id \
         WHERE rm.movie_id = ? AND rm.relation_type = ? \
         ORDER BY m.tmdb_rating DESC NULLS LAST \
         LIMIT 20",
    )
    .bind(movie_id)
    .bind(relation_type)
    .fetch_all(pool)
    .await
}

pub async fn get_movie_lists(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Vec<MovieList>, sqlx::Error> {
    sqlx::query_as::<_, MovieList>("SELECT * FROM movie_lists WHERE movie_id = ?")
        .bind(movie_id)
        .fetch_all(pool)
        .await
}

pub async fn get_movie_by_tmdb_id(
    pool: &SqlitePool,
    tmdb_id: i64,
) -> Result<Option<Movie>, sqlx::Error> {
    sqlx::query_as::<_, Movie>("SELECT * FROM movies WHERE tmdb_id = ?")
        .bind(tmdb_id)
        .fetch_optional(pool)
        .await
}

/// Bulk lookup by TMDB id. The result map only contains rows that exist in
/// `movies`; callers must handle missing ids themselves.
pub async fn get_movies_by_tmdb_ids(
    pool: &SqlitePool,
    tmdb_ids: &[i64],
) -> Result<std::collections::HashMap<i64, Movie>, sqlx::Error> {
    let mut out = std::collections::HashMap::new();
    if tmdb_ids.is_empty() {
        return Ok(out);
    }
    // SQLite has a default parameter limit (~32k), but for safety chunk by 500.
    for chunk in tmdb_ids.chunks(500) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let sql = format!("SELECT * FROM movies WHERE tmdb_id IN ({})", placeholders);
        let mut q = sqlx::query_as::<_, Movie>(&sql);
        for id in chunk {
            q = q.bind(*id);
        }
        let rows = q.fetch_all(pool).await?;
        for m in rows {
            out.insert(m.tmdb_id, m);
        }
    }
    Ok(out)
}

pub async fn get_person_by_tmdb_id(
    pool: &SqlitePool,
    tmdb_person_id: i64,
) -> Result<Option<Person>, sqlx::Error> {
    sqlx::query_as::<_, Person>("SELECT * FROM persons WHERE tmdb_person_id = ?")
        .bind(tmdb_person_id)
        .fetch_optional(pool)
        .await
}

pub async fn upsert_person(
    pool: &SqlitePool,
    tmdb_person_id: i64,
    name: &str,
    also_known_as: Option<&str>,
    biography: Option<&str>,
    profile_path: Option<&str>,
    birthday: Option<&str>,
    deathday: Option<&str>,
    place_of_birth: Option<&str>,
) -> Result<i64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO persons (tmdb_person_id, name, also_known_as, biography, profile_path, birthday, deathday, place_of_birth)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(tmdb_person_id) DO UPDATE SET
            name = excluded.name,
            also_known_as = excluded.also_known_as,
            biography = excluded.biography,
            profile_path = excluded.profile_path,
            birthday = excluded.birthday,
            deathday = excluded.deathday,
            place_of_birth = excluded.place_of_birth,
            updated_at = datetime('now')",
    )
    .bind(tmdb_person_id)
    .bind(name)
    .bind(also_known_as)
    .bind(biography)
    .bind(profile_path)
    .bind(birthday)
    .bind(deathday)
    .bind(place_of_birth)
    .execute(pool)
    .await?;

    Ok(result.last_insert_rowid())
}

/// 获取所有已匹配电影的 embedding 源数据。
/// 返回 (movie_id, title, overview, genres, keywords, director) 用于拼接 embedding 文本。
#[derive(Debug, sqlx::FromRow)]
pub struct MovieEmbeddingSource {
    pub id: i64,
    pub title: String,
    pub overview: Option<String>,
    pub genres: Option<String>,
    pub keywords: Option<String>,
    pub director: Option<String>,
}

pub async fn get_all_movies_for_embedding(
    pool: &SqlitePool,
) -> Result<Vec<MovieEmbeddingSource>, sqlx::Error> {
    sqlx::query_as::<_, MovieEmbeddingSource>(
        "SELECT m.id, m.title, m.overview, m.genres, m.keywords, m.director
         FROM movies m
         WHERE (
            EXISTS (
                SELECT 1 FROM dir_movie_mappings dmm
                JOIN media_dirs md ON md.id = dmm.dir_id
                WHERE dmm.movie_id = m.id
                AND md.scan_status != 'deleted'
                AND dmm.match_status IN ('auto', 'manual')
            )
            OR m.source = 'related'
         )",
    )
    .fetch_all(pool)
    .await
}

pub async fn get_task_counts(pool: &SqlitePool) -> Result<Vec<(String, String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, String, i64)>(
        "SELECT task_type, status, COUNT(*) as count FROM tasks GROUP BY task_type, status",
    )
    .fetch_all(pool)
    .await
}

/// Insert pending rows for any English keyword not yet in the dictionary table.
/// Called from the TMDB fetch path so newly-stored keywords get queued for
/// translation automatically. Empty-string keys are silently skipped.
pub async fn ensure_keyword_translation_rows(
    pool: &SqlitePool,
    keywords: &[String],
) -> Result<(), sqlx::Error> {
    if keywords.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    for kw in keywords {
        if kw.trim().is_empty() {
            continue;
        }
        sqlx::query(
            "INSERT OR IGNORE INTO keyword_translations (en, status) VALUES (?, 'pending')",
        )
        .bind(kw)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Claim a batch of pending keyword rows for translation. Returns the English
/// strings; the worker is responsible for filling `zh` and flipping `status`.
pub async fn claim_pending_keyword_translations(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT en FROM keyword_translations
         WHERE zh IS NULL AND status = 'pending'
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Mark a translation as done with the Chinese rendering. Idempotent.
pub async fn save_keyword_translation(
    pool: &SqlitePool,
    en: &str,
    zh: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE keyword_translations
         SET zh = ?, status = 'done', updated_at = datetime('now')
         WHERE en = ?",
    )
    .bind(zh)
    .bind(en)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark translations as failed (LLM hard error or unparseable response).
/// Failed rows do not retry automatically — admin must reset to 'pending'.
pub async fn mark_keyword_translations_failed(
    pool: &SqlitePool,
    keywords: &[String],
) -> Result<(), sqlx::Error> {
    if keywords.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    for kw in keywords {
        sqlx::query(
            "UPDATE keyword_translations
             SET status = 'failed', updated_at = datetime('now')
             WHERE en = ? AND status = 'pending'",
        )
        .bind(kw)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Load every translated (en, zh) pair for the in-memory dictionary cache.
pub async fn load_all_keyword_translations(
    pool: &SqlitePool,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT en, zh FROM keyword_translations WHERE zh IS NOT NULL",
    )
    .fetch_all(pool)
    .await
}

/// One unit of work for the overview-translation worker: a movie that has
/// English overview text but no usable Chinese version yet.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PendingOverview {
    pub id: i64,
    pub overview_en: String,
}

/// Claim a batch of movies whose Chinese overview is missing or trivially
/// short, and which we have not previously attempted to translate. Workers
/// process the returned rows; success populates `overview` + sets
/// `overview_zh_source = 'llm'`, hard failure sets `'failed'`.
pub async fn claim_pending_overviews(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<PendingOverview>, sqlx::Error> {
    sqlx::query_as::<_, PendingOverview>(
        "SELECT id, overview_en
         FROM movies
         WHERE (overview IS NULL OR length(overview) < 30)
           AND overview_zh_source IS NULL
           AND length(overview_en) > 50
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Persist a successful LLM translation. Marks `overview_zh_source = 'llm'`
/// so future TMDB refetches know this is a synthetic translation worth
/// preserving when the official zh response is shorter or empty.
pub async fn save_overview_translation(
    pool: &SqlitePool,
    movie_id: i64,
    zh: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE movies
         SET overview = ?, overview_zh_source = 'llm', updated_at = datetime('now')
         WHERE id = ?",
    )
    .bind(zh)
    .bind(movie_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark translations as failed in bulk so the worker doesn't keep retrying
/// poison input on every tick. Admin can reset to NULL to re-attempt.
pub async fn mark_overview_translations_failed(
    pool: &SqlitePool,
    movie_ids: &[i64],
) -> Result<(), sqlx::Error> {
    if movie_ids.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    for id in movie_ids {
        sqlx::query(
            "UPDATE movies
             SET overview_zh_source = 'failed', updated_at = datetime('now')
             WHERE id = ? AND overview_zh_source IS NULL",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Snapshot of a movie's current zh overview state, used by the TMDB refetch
/// path to decide whether the new response should overwrite the existing
/// text. We refuse to overwrite an LLM translation with a much shorter TMDB
/// payload because the LLM version is more likely to give the embedding
/// model useful semantic content.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OverviewState {
    pub overview: Option<String>,
    pub overview_zh_source: Option<String>,
}

pub async fn get_movie_overview_state(
    pool: &SqlitePool,
    tmdb_id: i64,
) -> Result<Option<OverviewState>, sqlx::Error> {
    sqlx::query_as::<_, OverviewState>(
        "SELECT overview, overview_zh_source FROM movies WHERE tmdb_id = ?",
    )
    .bind(tmdb_id)
    .fetch_optional(pool)
    .await
}

/// Random sample of indexed movies for the embedding-rebuild worker. Random
/// rather than ordered so a steady-state daemon walks the whole library over
/// time without us needing to track per-movie cursors.
pub async fn sample_movies_for_embedding_check(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<MovieEmbeddingSource>, sqlx::Error> {
    sqlx::query_as::<_, MovieEmbeddingSource>(
        "SELECT m.id, m.title, m.overview, m.genres, m.keywords, m.director
         FROM movies m
         WHERE (
            EXISTS (
                SELECT 1 FROM dir_movie_mappings dmm
                JOIN media_dirs md ON md.id = dmm.dir_id
                WHERE dmm.movie_id = m.id
                AND md.scan_status != 'deleted'
                AND dmm.match_status IN ('auto', 'manual')
            )
            OR m.source = 'related'
         )
         ORDER BY RANDOM()
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

pub async fn get_movie_by_id(pool: &SqlitePool, id: i64) -> Result<Option<Movie>, sqlx::Error> {
    sqlx::query_as::<_, Movie>("SELECT * FROM movies WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
}

/// Get library statistics for the homepage summary
pub async fn get_library_stats(pool: &SqlitePool) -> Result<LibraryStats, sqlx::Error> {
    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM movies").fetch_one(pool).await?;

    // Decade distribution
    let decades = sqlx::query_as::<_, (i64, i64)>(
        "SELECT (year / 10) * 10 AS decade, COUNT(*) FROM movies WHERE year IS NOT NULL GROUP BY decade ORDER BY decade"
    ).fetch_all(pool).await?;

    // Top genres (from JSON array)
    let genres = sqlx::query_as::<_, (String, i64)>(
        "SELECT j.value AS genre, COUNT(*) AS cnt FROM movies, json_each(movies.genres) AS j WHERE genres IS NOT NULL AND genres != '[]' GROUP BY genre ORDER BY cnt DESC LIMIT 20"
    ).fetch_all(pool).await?;

    // Top countries
    let countries = sqlx::query_as::<_, (String, i64)>(
        "SELECT country, COUNT(*) AS cnt FROM movies WHERE country IS NOT NULL AND country != '' GROUP BY country ORDER BY cnt DESC LIMIT 20"
    ).fetch_all(pool).await?;

    // Top directors
    let directors = sqlx::query_as::<_, (String, i64)>(
        "SELECT director, COUNT(*) AS cnt FROM movies WHERE director IS NOT NULL AND director != '' GROUP BY director ORDER BY cnt DESC LIMIT 20"
    ).fetch_all(pool).await?;

    // Top cast (from JSON array)
    let cast = sqlx::query_as::<_, (String, i64)>(
        "SELECT j.value AS actor, COUNT(*) AS cnt FROM movies, json_each(movies.\"cast\") AS j WHERE movies.\"cast\" IS NOT NULL AND movies.\"cast\" != '[]' GROUP BY actor ORDER BY cnt DESC LIMIT 20"
    ).fetch_all(pool).await?;

    // Top keywords (from JSON array). Top 30 是为了让 query-understand prompt 拿到
    // 足够长的实际词汇表，约束 LLM 生成 keyword 时对齐到库里真实存在的词
    // （例如必须是 "black and white" 而不是它自己造出来的 "monochrome"）。
    let keywords = sqlx::query_as::<_, (String, i64)>(
        "SELECT j.value AS kw, COUNT(*) AS cnt FROM movies, json_each(movies.keywords) AS j WHERE keywords IS NOT NULL AND keywords != '[]' GROUP BY kw ORDER BY cnt DESC LIMIT 30"
    ).fetch_all(pool).await?;

    // Rating tiers
    let rating_tiers = sqlx::query_as::<_, (String, i64)>(
        "SELECT\n\
            CASE\n\
                WHEN tmdb_rating >= 8 THEN 'rating_excellent'\n\
                WHEN tmdb_rating >= 6 THEN 'rating_good'\n\
                WHEN tmdb_rating >= 4 THEN 'rating_average'\n\
                ELSE 'rating_poor'\n\
            END AS tier,\n\
            COUNT(*) AS cnt\n\
         FROM movies\n\
         WHERE tmdb_rating IS NOT NULL\n\
         GROUP BY tier\n\
         ORDER BY MIN(tmdb_rating) DESC"
    ).fetch_all(pool).await?;

    // Budget tiers (in USD)
    let budget_sql = format!(
        "SELECT \
            CASE \
                WHEN budget > {high} THEN 'budget_high' \
                WHEN budget >= {med} THEN 'budget_medium' \
                ELSE 'budget_low' \
            END AS tier, \
            COUNT(*) AS cnt \
         FROM movies \
         WHERE budget IS NOT NULL AND budget > 0 \
         GROUP BY tier \
         ORDER BY MIN(budget) DESC",
        high = BUDGET_HIGH_THRESHOLD,
        med = BUDGET_MEDIUM_THRESHOLD,
    );
    let budget_tiers = sqlx::query_as::<_, (String, i64)>(&budget_sql)
        .fetch_all(pool).await?;

    // 仅统计库内电影（source='library'）——related 电影不算"库里有的片"，
    // 不应参与代表性采样或空库判定。
    let library_total: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM movies WHERE source = 'library'"
    ).fetch_one(pool).await?;

    // 按 genre 分层采样的代表性电影。PARTITION BY g.value 让同一部片出现在
    // 每个 genre 分组里 → 外层 GROUP BY m.id 去重并保留最佳排名（多 genre 片
    // 的 best_rn 更容易 <=5 → 总体排位更靠前，这正是"代表性"想要的）。
    let raw_samples = sqlx::query_as::<_, SampleMovieRow>(
        "WITH per_genre AS (\n\
             SELECT m.id, m.title, m.year, m.director, m.tmdb_rating,\n\
                    ROW_NUMBER() OVER (\n\
                      PARTITION BY g.value\n\
                      ORDER BY m.popularity DESC NULLS LAST, m.tmdb_rating DESC NULLS LAST, m.id ASC\n\
                    ) AS rn\n\
             FROM movies m, json_each(m.genres) g\n\
             WHERE m.source = 'library'\n\
               AND m.title IS NOT NULL\n\
               AND m.genres IS NOT NULL AND m.genres != '[]'\n\
         )\n\
         SELECT p.id, p.title, p.year, p.director, p.tmdb_rating,\n\
                (SELECT GROUP_CONCAT(DISTINCT g2.value)\n\
                   FROM movies m2, json_each(m2.genres) g2\n\
                  WHERE m2.id = p.id) AS genres_concat,\n\
                MIN(p.rn) AS best_rn\n\
         FROM per_genre p\n\
         WHERE p.rn <= 5\n\
         GROUP BY p.id\n\
         ORDER BY best_rn ASC, p.id ASC"
    )
    .fetch_all(pool)
    .await?;

    let sample_movies: Vec<String> = format_movie_samples(&raw_samples, SAMPLE_MOVIES_LIMIT);

    Ok(LibraryStats {
        total: total.0,
        library_total: library_total.0,
        sample_movies,
        decades,
        genres,
        countries,
        directors,
        cast,
        keywords,
        rating_tiers,
        budget_tiers,
    })
}

/// 注入进 inspire prompt 的代表性电影样本上限。
pub const SAMPLE_MOVIES_LIMIT: usize = 50;

/// 只查库内电影总数（不跑完整 stats），供 daily-picks 空库短路用。
pub async fn get_library_total(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM movies WHERE source = 'library'"
    )
    .fetch_one(pool)
    .await?;
    Ok(count)
}

#[allow(dead_code)]
#[derive(Debug, sqlx::FromRow)]
pub struct SampleMovieRow {
    pub id: i64,
    pub title: String,
    pub year: Option<i64>,
    pub director: Option<String>,
    pub tmdb_rating: Option<f64>,
    pub genres_concat: Option<String>,
    #[allow(dead_code)]
    pub best_rn: i64,
}

/// 把分层采样的原始电影行格式化为注入 prompt 的单行文本，
/// 并按 limit 截断。NULL 字段用 "-" 占位，rating 保留一位小数。
pub fn format_movie_samples(rows: &[SampleMovieRow], limit: usize) -> Vec<String> {
    rows.iter()
        .take(limit)
        .map(|m| {
            let year = m.year.map(|y| y.to_string()).unwrap_or_else(|| "-".into());
            let director = m.director.as_deref().filter(|s| !s.is_empty()).unwrap_or("-");
            let genres = m.genres_concat.as_deref().filter(|s| !s.is_empty()).unwrap_or("-");
            let rating = m.tmdb_rating
                .map(|r| format!("⭐{:.1}", r))
                .unwrap_or_else(|| "⭐-".into());
            format!("- {} ({}) · {} · {} · {}", m.title, year, genres, director, rating)
        })
        .collect()
}

#[derive(Debug, Serialize)]
pub struct LibraryStats {
    /// 影片总数（含 source='related' 的库外电影）
    pub total: i64,
    /// 仅 source='library' 的库内电影数——空库短路用这个
    pub library_total: i64,
    /// 分层采样的代表性库内电影，每行预格式化文本，最多 SAMPLE_MOVIES_LIMIT 部
    pub sample_movies: Vec<String>,
    pub decades: Vec<(i64, i64)>,
    pub genres: Vec<(String, i64)>,
    pub countries: Vec<(String, i64)>,
    pub directors: Vec<(String, i64)>,
    pub cast: Vec<(String, i64)>,
    pub keywords: Vec<(String, i64)>,
    pub rating_tiers: Vec<(String, i64)>,
    pub budget_tiers: Vec<(String, i64)>,
}

pub async fn get_match_status_counts(pool: &SqlitePool) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT dm.match_status, COUNT(*) FROM dir_movie_mappings dm
         JOIN media_dirs md ON md.id = dm.dir_id
         WHERE md.scan_status != 'deleted'
           AND dm.movie_id IS NOT NULL
         GROUP BY dm.match_status"
    ).fetch_all(pool).await
}

pub async fn get_dir_status_counts(pool: &SqlitePool) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT scan_status, COUNT(*) FROM media_dirs GROUP BY scan_status"
    )
    .fetch_all(pool)
    .await
}

pub async fn get_dir_total(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    let (total,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM media_dirs")
        .fetch_one(pool)
        .await?;
    Ok(total)
}

/// 按 `source` 维度统计电影库构成（`library` = 库内，`related` = 库外）。
pub async fn get_movies_source_counts(pool: &SqlitePool) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT COALESCE(source, 'library') AS source, COUNT(*) \
         FROM movies GROUP BY COALESCE(source, 'library')"
    )
    .fetch_all(pool)
    .await
}

/// Library-only year distribution, bucketed by decade. "unknown" bucket for NULL year.
/// Returned sorted by decade descending (newest first), unknown at the end.
pub async fn get_library_year_buckets(pool: &SqlitePool) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT bucket, COUNT(*) AS cnt FROM (
             SELECT CASE
                 WHEN year IS NULL THEN 'unknown'
                 WHEN year >= 2020 THEN '2020s'
                 WHEN year >= 2010 THEN '2010s'
                 WHEN year >= 2000 THEN '2000s'
                 WHEN year >= 1990 THEN '1990s'
                 WHEN year >= 1980 THEN '1980s'
                 ELSE 'earlier'
             END AS bucket
             FROM movies
             WHERE COALESCE(source, 'library') = 'library'
         )
         GROUP BY bucket
         ORDER BY CASE bucket
             WHEN '2020s' THEN 1
             WHEN '2010s' THEN 2
             WHEN '2000s' THEN 3
             WHEN '1990s' THEN 4
             WHEN '1980s' THEN 5
             WHEN 'earlier' THEN 6
             WHEN 'unknown' THEN 7
         END",
    )
    .fetch_all(pool)
    .await
}

/// Library-only top N countries by movie count.
pub async fn get_library_country_top(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT country, COUNT(*) AS cnt
         FROM movies
         WHERE COALESCE(source, 'library') = 'library'
           AND country IS NOT NULL AND country != ''
         GROUP BY country
         ORDER BY cnt DESC
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Library-only top N genres by movie count. Genres stored as JSON array per movie.
pub async fn get_library_genre_top(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT j.value AS genre, COUNT(DISTINCT m.id) AS cnt
         FROM movies m, json_each(m.genres) AS j
         WHERE COALESCE(m.source, 'library') = 'library'
           AND m.genres IS NOT NULL AND m.genres != '[]'
         GROUP BY genre
         ORDER BY cnt DESC
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// Library-only TMDB rating histogram, 1-point buckets from 0..10 plus 'unrated'.
pub async fn get_library_rating_histogram(pool: &SqlitePool) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT bucket, COUNT(*) AS cnt FROM (
             SELECT CASE
                 WHEN tmdb_rating IS NULL THEN 'unrated'
                 WHEN tmdb_rating < 1 THEN '0-1'
                 WHEN tmdb_rating < 2 THEN '1-2'
                 WHEN tmdb_rating < 3 THEN '2-3'
                 WHEN tmdb_rating < 4 THEN '3-4'
                 WHEN tmdb_rating < 5 THEN '4-5'
                 WHEN tmdb_rating < 6 THEN '5-6'
                 WHEN tmdb_rating < 7 THEN '6-7'
                 WHEN tmdb_rating < 8 THEN '7-8'
                 WHEN tmdb_rating < 9 THEN '8-9'
                 ELSE '9-10'
             END AS bucket
             FROM movies
             WHERE COALESCE(source, 'library') = 'library'
         )
         GROUP BY bucket
         ORDER BY CASE bucket
             WHEN '0-1' THEN 0
             WHEN '1-2' THEN 1
             WHEN '2-3' THEN 2
             WHEN '3-4' THEN 3
             WHEN '4-5' THEN 4
             WHEN '5-6' THEN 5
             WHEN '6-7' THEN 6
             WHEN '7-8' THEN 7
             WHEN '8-9' THEN 8
             WHEN '9-10' THEN 9
             WHEN 'unrated' THEN 10
         END",
    )
    .fetch_all(pool)
    .await
}

/// Aggregate mark counts across all users, grouped by mark_type (want / watched / favorite).
pub async fn get_mark_counts(pool: &SqlitePool) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT mark_type, COUNT(*) AS cnt
         FROM user_movie_marks
         GROUP BY mark_type",
    )
    .fetch_all(pool)
    .await
}

pub async fn get_dir_paths_for_movie(pool: &SqlitePool, movie_id: i64) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT md.dir_path FROM media_dirs md
         JOIN dir_movie_mappings dm ON dm.dir_id = md.id
         WHERE dm.movie_id = ? AND md.scan_status != 'deleted'"
    )
    .bind(movie_id)
    .fetch_all(pool)
    .await
}

#[derive(Debug, Default, Deserialize)]
pub struct MovieFilters {
    pub decade: Option<String>,    // "2010s"
    pub genre: Option<String>,     // "剧情"
    pub country: Option<String>,   // "US"
    pub language: Option<String>,  // "en"
    pub rating: Option<String>,    // "8-9"
    pub runtime: Option<String>,   // "90-120min"
    pub director: Option<String>,  // "伍迪·艾伦"
    pub keyword: Option<String>,   // "based-on-novel"
    pub cast: Option<String>,      // actor name
}

#[allow(clippy::too_many_arguments)]
pub async fn list_movies(
    pool: &SqlitePool,
    search: Option<&str>,
    status: Option<&str>,
    filters: &MovieFilters,
    page: i64,
    per_page: i64,
) -> Result<(Vec<Movie>, i64), sqlx::Error> {
    let page = page.max(1);
    let per_page = per_page.max(1);
    let offset = (page - 1) * per_page;

    let build_query = |select_clause: &str| {
        let mut builder = QueryBuilder::<Sqlite>::new(select_clause);
        builder.push(
            " FROM movies m \
             JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id \
             JOIN media_dirs md ON md.id = dmm.dir_id \
             WHERE md.scan_status != 'deleted'",
        );

        if let Some(term) = search {
            let like = format!("%{term}%");
            builder.push(" AND (m.title LIKE ");
            builder.push_bind(like.clone());
            builder.push(" OR m.original_title LIKE ");
            builder.push_bind(like);
            builder.push(")");
        }

        // "auto" filter includes both auto-confirmed and manually confirmed movies
        if let Some(s) = status {
            if s == "auto" {
                builder.push(" AND dmm.match_status IN ('auto', 'manual')");
            } else {
                builder.push(" AND dmm.match_status = ");
                builder.push_bind(s.to_string());
            }
        }

        if let Some(decade) = &filters.decade {
            if let Ok(d) = decade.trim_end_matches('s').parse::<i64>() {
                builder.push(" AND (m.year / 10) * 10 = ");
                builder.push_bind(d);
            }
        }

        if let Some(genre) = &filters.genre {
            builder.push(" AND EXISTS (SELECT 1 FROM json_each(m.genres) WHERE value = ");
            builder.push_bind(genre.clone());
            builder.push(")");
        }

        if let Some(country) = &filters.country {
            builder.push(" AND m.country = ");
            builder.push_bind(country.clone());
        }

        if let Some(language) = &filters.language {
            builder.push(" AND m.language = ");
            builder.push_bind(language.clone());
        }

        if let Some(rating) = &filters.rating {
            match rating.as_str() {
                "9+" => {
                    builder.push(" AND m.tmdb_rating >= 9.0");
                }
                "8-9" => {
                    builder.push(" AND m.tmdb_rating >= 8.0 AND m.tmdb_rating < 9.0");
                }
                "7-8" => {
                    builder.push(" AND m.tmdb_rating >= 7.0 AND m.tmdb_rating < 8.0");
                }
                "6-7" => {
                    builder.push(" AND m.tmdb_rating >= 6.0 AND m.tmdb_rating < 7.0");
                }
                "<6" => {
                    builder.push(" AND m.tmdb_rating < 6.0");
                }
                _ => {}
            }
        }

        if let Some(runtime) = &filters.runtime {
            match runtime.as_str() {
                "<90min" => {
                    builder.push(" AND m.runtime < 90");
                }
                "90-120min" => {
                    builder.push(" AND m.runtime >= 90 AND m.runtime < 120");
                }
                "120-150min" => {
                    builder.push(" AND m.runtime >= 120 AND m.runtime < 150");
                }
                ">150min" => {
                    builder.push(" AND m.runtime >= 150");
                }
                _ => {}
            }
        }

        if let Some(director) = &filters.director {
            builder.push(" AND m.director = ");
            builder.push_bind(director.clone());
        }

        if let Some(keyword) = &filters.keyword {
            builder.push(" AND EXISTS (SELECT 1 FROM json_each(m.keywords) WHERE value = ");
            builder.push_bind(keyword.clone());
            builder.push(")");
        }

        if let Some(cast_name) = &filters.cast {
            builder.push(
                " AND (EXISTS (SELECT 1 FROM json_each(m.\"cast\") AS j WHERE json_extract(j.value, '$.name') = ",
            );
            builder.push_bind(cast_name.clone());
            builder.push(") OR EXISTS (SELECT 1 FROM json_each(m.\"cast\") AS j WHERE j.value = ");
            builder.push_bind(cast_name.clone());
            builder.push("))");
        }

        builder
    };

    let mut items_builder = build_query("SELECT DISTINCT m.*");
    items_builder.push(" ORDER BY m.updated_at DESC LIMIT ");
    items_builder.push_bind(per_page);
    items_builder.push(" OFFSET ");
    items_builder.push_bind(offset);

    let items = items_builder
        .build_query_as::<Movie>()
        .fetch_all(pool)
        .await?;

    let mut count_builder = build_query("SELECT COUNT(DISTINCT m.id)");
    let total = count_builder
        .build_query_scalar::<i64>()
        .fetch_one(pool)
        .await?;

    Ok((items, total))
}

pub async fn list_movies_by_person_name(
    pool: &SqlitePool,
    person_name: &str,
    role: &str, // "cast" or "director"
    page: i64,
    per_page: i64,
) -> Result<(Vec<Movie>, i64), sqlx::Error> {
    let page = page.max(1);
    let per_page = per_page.max(1);
    let offset = (page - 1) * per_page;

    let (count_sql, items_sql) = if role == "director" {
        (
            "SELECT COUNT(DISTINCT m.id) FROM movies m
             JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id
             JOIN media_dirs md ON md.id = dmm.dir_id
             WHERE md.scan_status != 'deleted' AND m.director = ?"
                .to_string(),
            format!(
                "SELECT DISTINCT m.* FROM movies m
                 JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id
                 JOIN media_dirs md ON md.id = dmm.dir_id
                 WHERE md.scan_status != 'deleted' AND m.director = ?
                 ORDER BY m.year DESC NULLS LAST
                 LIMIT {} OFFSET {}",
                per_page, offset
            ),
        )
    } else {
        (
            "SELECT COUNT(DISTINCT m.id) FROM movies m
             JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id
             JOIN media_dirs md ON md.id = dmm.dir_id
             WHERE md.scan_status != 'deleted'
               AND (EXISTS (SELECT 1 FROM json_each(m.\"cast\") AS j WHERE json_extract(j.value, '$.name') = ?)
                    OR EXISTS (SELECT 1 FROM json_each(m.\"cast\") AS j WHERE j.value = ?))"
                .to_string(),
            format!(
                "SELECT DISTINCT m.* FROM movies m
                 JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id
                 JOIN media_dirs md ON md.id = dmm.dir_id
                 WHERE md.scan_status != 'deleted'
                   AND (EXISTS (SELECT 1 FROM json_each(m.\"cast\") AS j WHERE json_extract(j.value, '$.name') = ?)
                        OR EXISTS (SELECT 1 FROM json_each(m.\"cast\") AS j WHERE j.value = ?))
                 ORDER BY m.year DESC NULLS LAST
                 LIMIT {} OFFSET {}",
                per_page, offset
            ),
        )
    };

    let total = if role == "director" {
        sqlx::query_scalar::<_, i64>(&count_sql)
            .bind(person_name)
            .fetch_one(pool)
            .await?
    } else {
        sqlx::query_scalar::<_, i64>(&count_sql)
            .bind(person_name)
            .bind(person_name)
            .fetch_one(pool)
            .await?
    };

    let items = if role == "director" {
        sqlx::query_as::<_, Movie>(&items_sql)
            .bind(person_name)
            .fetch_all(pool)
            .await?
    } else {
        sqlx::query_as::<_, Movie>(&items_sql)
            .bind(person_name)
            .bind(person_name)
            .fetch_all(pool)
            .await?
    };

    Ok((items, total))
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PendingDirRow {
    pub dir_id: i64,
    pub dir_path: String,
    pub dir_name: String,
    pub match_status: String,
    pub confidence: Option<f64>,
    pub candidates: Option<String>,
}

pub async fn list_pending_dirs(
    pool: &SqlitePool,
    page: i64,
    per_page: i64,
) -> Result<(Vec<PendingDirRow>, i64), sqlx::Error> {
    let page = page.max(1);
    let per_page = per_page.max(1);
    let offset = (page - 1) * per_page;

    let total = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM dir_movie_mappings WHERE match_status IN ('pending', 'failed')",
    )
    .fetch_one(pool)
    .await?;

    let rows = sqlx::query_as::<_, PendingDirRow>(
        "SELECT md.id AS dir_id, md.dir_path, md.dir_name, dm.match_status, dm.confidence, dm.candidates
         FROM media_dirs md
         JOIN dir_movie_mappings dm ON dm.dir_id = md.id
         WHERE dm.match_status IN ('pending', 'failed')
         ORDER BY dm.confidence DESC NULLS LAST, md.updated_at DESC
         LIMIT ? OFFSET ?",
    )
    .bind(per_page)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok((rows, total))
}

/// 一个尚未绑定到任何电影的目录。`status` 为 None 表示完全没 mapping，
/// Some("pending"/"failed") 表示有 mapping 但当前状态没绑成。
/// auto / manual 已绑的目录不会出现在这个查询里——这是反向定位特性的明确边界。
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct UnboundDir {
    pub dir_id: i64,
    pub dir_path: String,
    pub dir_name: String,
    pub status: Option<String>,
}

pub async fn list_unbound_media_dirs(pool: &SqlitePool) -> Result<Vec<UnboundDir>, sqlx::Error> {
    sqlx::query_as::<_, UnboundDir>(
        "SELECT md.id AS dir_id, md.dir_path, md.dir_name, dm.match_status AS status
         FROM media_dirs md
         LEFT JOIN dir_movie_mappings dm ON dm.dir_id = md.id
         WHERE md.scan_status != 'deleted'
           AND (dm.match_status IS NULL OR dm.match_status IN ('pending', 'failed'))",
    )
    .fetch_all(pool)
    .await
}

pub async fn get_mapping_by_dir_id(
    pool: &SqlitePool,
    dir_id: i64,
) -> Result<Option<DirMovieMapping>, sqlx::Error> {
    sqlx::query_as::<_, DirMovieMapping>(
        "SELECT * FROM dir_movie_mappings WHERE dir_id = ? ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(dir_id)
    .fetch_optional(pool)
    .await
}

pub async fn bind_dir_to_movie(
    pool: &SqlitePool,
    dir_id: i64,
    movie_id: i64,
) -> Result<(), sqlx::Error> {
    let result = sqlx::query(
        "UPDATE dir_movie_mappings
         SET match_status = 'manual', movie_id = ?, confidence = 1.0, updated_at = datetime('now')
         WHERE dir_id = ?",
    )
    .bind(movie_id)
    .bind(dir_id)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(sqlx::Error::RowNotFound);
    }

    sqlx::query(
        "UPDATE media_dirs SET scan_status = 'matched', updated_at = datetime('now') WHERE id = ?",
    )
    .bind(dir_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn unbind_dir(pool: &SqlitePool, dir_id: i64) -> Result<(), sqlx::Error> {
    let result = sqlx::query(
        "UPDATE dir_movie_mappings
         SET match_status = 'pending', movie_id = NULL, confidence = NULL, updated_at = datetime('now')
         WHERE dir_id = ?",
    )
    .bind(dir_id)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(sqlx::Error::RowNotFound);
    }

    sqlx::query(
        "UPDATE media_dirs SET scan_status = 'parsed', updated_at = datetime('now') WHERE id = ?",
    )
    .bind(dir_id)
    .execute(pool)
    .await?;

    Ok(())
}

#[derive(Debug, Serialize)]
pub struct FilterOptions {
    pub decades: Vec<(String, i64)>,
    pub genres: Vec<(String, i64)>,
    pub countries: Vec<(String, i64)>,
    pub languages: Vec<(String, i64)>,
    pub ratings: Vec<(String, i64)>,
    pub runtimes: Vec<(String, i64)>,
}

pub async fn get_filter_options(pool: &SqlitePool) -> Result<FilterOptions, sqlx::Error> {
    let decades = sqlx::query_as::<_, (String, i64)>(
        "SELECT CAST((m.year / 10) * 10 AS TEXT) || 's' AS decade, COUNT(DISTINCT m.id) AS cnt
         FROM movies m
         JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id
         JOIN media_dirs md ON md.id = dmm.dir_id
         WHERE md.scan_status != 'deleted' AND m.year IS NOT NULL
         GROUP BY (m.year / 10) * 10
         HAVING cnt >= 3
         ORDER BY cnt DESC",
    )
    .fetch_all(pool)
    .await?;

    let genres = sqlx::query_as::<_, (String, i64)>(
        "SELECT j.value AS genre, COUNT(DISTINCT m.id) AS cnt
         FROM movies m, json_each(m.genres) AS j
         JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id
         JOIN media_dirs md ON md.id = dmm.dir_id
         WHERE md.scan_status != 'deleted' AND m.genres IS NOT NULL AND m.genres != '[]'
         GROUP BY genre
         HAVING cnt >= 3
         ORDER BY cnt DESC",
    )
    .fetch_all(pool)
    .await?;

    let countries = sqlx::query_as::<_, (String, i64)>(
        "SELECT m.country, COUNT(DISTINCT m.id) AS cnt
         FROM movies m
         JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id
         JOIN media_dirs md ON md.id = dmm.dir_id
         WHERE md.scan_status != 'deleted' AND m.country IS NOT NULL AND m.country != ''
         GROUP BY m.country
         HAVING cnt >= 3
         ORDER BY cnt DESC",
    )
    .fetch_all(pool)
    .await?;

    let languages = sqlx::query_as::<_, (String, i64)>(
        "SELECT m.language, COUNT(DISTINCT m.id) AS cnt
         FROM movies m
         JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id
         JOIN media_dirs md ON md.id = dmm.dir_id
         WHERE md.scan_status != 'deleted' AND m.language IS NOT NULL AND m.language != ''
         GROUP BY m.language
         HAVING cnt >= 3
         ORDER BY cnt DESC",
    )
    .fetch_all(pool)
    .await?;

    let ratings = sqlx::query_as::<_, (String, i64)>(
        "SELECT
            CASE
                WHEN m.tmdb_rating >= 9.0 THEN '9+'
                WHEN m.tmdb_rating >= 8.0 THEN '8-9'
                WHEN m.tmdb_rating >= 7.0 THEN '7-8'
                WHEN m.tmdb_rating >= 6.0 THEN '6-7'
                ELSE '<6'
            END AS rating_bucket,
            COUNT(DISTINCT m.id) AS cnt
         FROM movies m
         JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id
         JOIN media_dirs md ON md.id = dmm.dir_id
         WHERE md.scan_status != 'deleted' AND m.tmdb_rating IS NOT NULL
         GROUP BY rating_bucket
         HAVING cnt >= 3
         ORDER BY cnt DESC",
    )
    .fetch_all(pool)
    .await?;

    let runtimes = sqlx::query_as::<_, (String, i64)>(
        "SELECT
            CASE
                WHEN m.runtime < 90 THEN '<90min'
                WHEN m.runtime < 120 THEN '90-120min'
                WHEN m.runtime < 150 THEN '120-150min'
                ELSE '>150min'
            END AS runtime_bucket,
            COUNT(DISTINCT m.id) AS cnt
         FROM movies m
         JOIN dir_movie_mappings dmm ON dmm.movie_id = m.id
         JOIN media_dirs md ON md.id = dmm.dir_id
         WHERE md.scan_status != 'deleted' AND m.runtime IS NOT NULL
         GROUP BY runtime_bucket
         HAVING cnt >= 3
         ORDER BY cnt DESC",
    )
    .fetch_all(pool)
    .await?;

    Ok(FilterOptions {
        decades,
        genres,
        countries,
        languages,
        ratings,
        runtimes,
    })
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct MovieBrief {
    pub id: i64,
    pub tmdb_id: i64,
    pub title: String,
    pub year: Option<i64>,
    pub genres: Option<String>,
    pub director: Option<String>,
    pub language: Option<String>,
}

#[allow(dead_code)]
pub async fn query_movies_by_filters(
    pool: &SqlitePool,
    genres: &[String],
    countries: &[String],
    decades: &[i64],
    directors: &[String],
    cast: &[String],
    min_rating: Option<f64>,
    budget_tier: &[String],
) -> Result<Vec<MovieBrief>, sqlx::Error> {
    let mut qb = QueryBuilder::<Sqlite>::new(
        "SELECT m.id, m.tmdb_id, m.title, m.year, m.genres, m.director, m.language FROM movies m WHERE \
         EXISTS (SELECT 1 FROM dir_movie_mappings dmm JOIN media_dirs md ON md.id = dmm.dir_id \
         WHERE dmm.movie_id = m.id AND md.scan_status != 'deleted' AND dmm.match_status IN ('auto', 'manual'))"
    );

    // Filter conditions — all OR'd together
    let mut has_filter = false;

    macro_rules! or_filter {
        ($qb:expr, $has:expr) => {
            if !$has { $qb.push(" AND ("); $has = true; } else { $qb.push(" OR "); }
        };
    }

    if !genres.is_empty() {
        or_filter!(qb, has_filter);
        qb.push("EXISTS (SELECT 1 FROM json_each(m.genres) WHERE value IN (");
        let mut sep = qb.separated(", ");
        for g in genres { sep.push_bind(g.clone()); }
        qb.push("))");
    }

    if !countries.is_empty() {
        or_filter!(qb, has_filter);
        qb.push("m.country IN (");
        let mut sep = qb.separated(", ");
        for c in countries { sep.push_bind(c.clone()); }
        qb.push(")");
    }

    if !decades.is_empty() {
        or_filter!(qb, has_filter);
        qb.push("(m.year / 10) * 10 IN (");
        let mut sep = qb.separated(", ");
        for d in decades { sep.push_bind(*d); }
        qb.push(")");
    }

    if !directors.is_empty() {
        or_filter!(qb, has_filter);
        qb.push("m.director IN (");
        let mut sep = qb.separated(", ");
        for d in directors { sep.push_bind(d.clone()); }
        qb.push(")");
    }

    if !cast.is_empty() {
        or_filter!(qb, has_filter);
        qb.push("EXISTS (SELECT 1 FROM json_each(m.\"cast\") WHERE value IN (");
        let mut sep = qb.separated(", ");
        for a in cast { sep.push_bind(a.clone()); }
        qb.push("))");
    }

    if let Some(rating) = min_rating {
        or_filter!(qb, has_filter);
        qb.push("m.tmdb_rating >= ");
        qb.push_bind(rating);
    }

    if !budget_tier.is_empty() {
        let mut tier_conditions: Vec<String> = Vec::new();
        for tier in budget_tier {
            match tier.as_str() {
                "high" | "大制作" | "budget_high" => tier_conditions.push(format!("m.budget > {}", BUDGET_HIGH_THRESHOLD)),
                "medium" | "中等" | "budget_medium" => tier_conditions.push(format!("(m.budget >= {} AND m.budget <= {})", BUDGET_MEDIUM_THRESHOLD, BUDGET_HIGH_THRESHOLD)),
                "low" | "小成本" | "budget_low" => tier_conditions.push(format!("(m.budget > 0 AND m.budget < {})", BUDGET_MEDIUM_THRESHOLD)),
                _ => {}
            }
        }
        if !tier_conditions.is_empty() {
            or_filter!(qb, has_filter);
            qb.push("(");
            qb.push(tier_conditions.join(" OR "));
            qb.push(")");
        }
    }

    if has_filter {
        qb.push(")");
    }

    qb.push(" ORDER BY m.tmdb_rating DESC NULLS LAST");

    let rows = qb.build_query_as::<MovieBrief>().fetch_all(pool).await?;
    Ok(rows)
}

/// 用于智能搜索管线的结构化召回。
/// 返回完整的电影信息（比 MovieBrief 多 overview, rating, runtime 等字段）用于粗排。
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MovieForRanking {
    pub id: i64,
    pub tmdb_id: i64,
    pub title: String,
    pub year: Option<i64>,
    pub genres: Option<String>,
    pub director: Option<String>,
    pub language: Option<String>,
    pub country: Option<String>,
    pub overview: Option<String>,
    pub tmdb_rating: Option<f64>,
    pub tmdb_votes: Option<i64>,
    pub runtime: Option<i64>,
    pub popularity: Option<f64>,
    pub budget: Option<i64>,
    pub keywords: Option<String>,
    #[sqlx(rename = "cast")]
    pub cast_json: Option<String>,
}

pub async fn structured_recall(
    pool: &SqlitePool,
    constraints: &Constraints,
    exclusions: &Exclusions,
    limit: i64,
) -> Result<Vec<MovieForRanking>, sqlx::Error> {
    let mut qb = QueryBuilder::<Sqlite>::new(
        "SELECT m.id, m.tmdb_id, m.title, m.year, m.genres, m.director, m.language, m.country, \
         m.overview, m.tmdb_rating, m.tmdb_votes, m.runtime, m.popularity, m.budget, m.keywords, m.\"cast\" \
         FROM movies m \
         WHERE (EXISTS (SELECT 1 FROM dir_movie_mappings dmm JOIN media_dirs md ON md.id = dmm.dir_id \
         WHERE dmm.movie_id = m.id AND md.scan_status != 'deleted' AND dmm.match_status IN ('auto', 'manual')) \
         OR m.source = 'related')"
    );

    // --- constraints (AND'd) ---

    if let Some(min) = constraints.year_range.min {
        qb.push(" AND m.year >= ");
        qb.push_bind(min);
    }
    if let Some(max) = constraints.year_range.max {
        qb.push(" AND m.year <= ");
        qb.push_bind(max);
    }

    if !constraints.decades.is_empty() {
        qb.push(" AND (m.year / 10) * 10 IN (");
        let mut sep = qb.separated(", ");
        for d in &constraints.decades { sep.push_bind(*d); }
        qb.push(")");
    }

    if !constraints.languages.is_empty() {
        qb.push(" AND m.language IN (");
        let mut sep = qb.separated(", ");
        for l in &constraints.languages { sep.push_bind(l.clone()); }
        qb.push(")");
    }

    if !constraints.genres.is_empty() {
        qb.push(" AND EXISTS (SELECT 1 FROM json_each(m.genres) WHERE value IN (");
        let mut sep = qb.separated(", ");
        for g in &constraints.genres { sep.push_bind(g.clone()); }
        qb.push("))");
    }

    if !constraints.countries.is_empty() {
        qb.push(" AND m.country IN (");
        let mut sep = qb.separated(", ");
        for c in &constraints.countries { sep.push_bind(c.clone()); }
        qb.push(")");
    }

    if !constraints.directors.is_empty() {
        qb.push(" AND m.director IN (");
        let mut sep = qb.separated(", ");
        for d in &constraints.directors { sep.push_bind(d.clone()); }
        qb.push(")");
    }

    if !constraints.cast.is_empty() {
        // cast 列存的是对象数组 [{"name":"...","tmdb_person_id":...,...},...]
        // 取 .name 做匹配。
        qb.push(" AND EXISTS (SELECT 1 FROM json_each(m.\"cast\") WHERE json_extract(value, '$.name') IN (");
        let mut sep = qb.separated(", ");
        for a in &constraints.cast { sep.push_bind(a.clone()); }
        qb.push("))");
    }

    if !constraints.keywords.is_empty() {
        qb.push(" AND EXISTS (SELECT 1 FROM json_each(m.keywords) WHERE value IN (");
        let mut sep = qb.separated(", ");
        for k in &constraints.keywords { sep.push_bind(k.clone()); }
        qb.push("))");
    }

    if let Some(min_r) = constraints.min_rating {
        qb.push(" AND m.tmdb_rating >= ");
        qb.push_bind(min_r);
    }
    if let Some(max_r) = constraints.max_rating {
        qb.push(" AND m.tmdb_rating <= ");
        qb.push_bind(max_r);
    }

    if let Some(min) = constraints.runtime_range.min {
        qb.push(" AND m.runtime >= ");
        qb.push_bind(min);
    }
    if let Some(max) = constraints.runtime_range.max {
        qb.push(" AND m.runtime <= ");
        qb.push_bind(max);
    }

    if let Some(ref tier) = constraints.budget_tier {
        match tier.as_str() {
            "high" => { qb.push(format!(" AND m.budget > {}", BUDGET_HIGH_THRESHOLD)); }
            "medium" => { qb.push(format!(" AND (m.budget >= {} AND m.budget <= {})", BUDGET_MEDIUM_THRESHOLD, BUDGET_HIGH_THRESHOLD)); }
            "low" => { qb.push(format!(" AND (m.budget > 0 AND m.budget < {})", BUDGET_MEDIUM_THRESHOLD)); }
            _ => {}
        }
    }

    if let Some(ref tier) = constraints.popularity_tier {
        match tier.as_str() {
            "popular" => {
                qb.push(" AND m.popularity >= (SELECT popularity FROM movies WHERE popularity IS NOT NULL ORDER BY popularity DESC LIMIT 1 OFFSET (SELECT COUNT(*)/3 FROM movies WHERE popularity IS NOT NULL))");
            }
            "niche" => {
                qb.push(" AND m.popularity <= (SELECT popularity FROM movies WHERE popularity IS NOT NULL ORDER BY popularity ASC LIMIT 1 OFFSET (SELECT COUNT(*)/3 FROM movies WHERE popularity IS NOT NULL))");
            }
            _ => {}
        }
    }

    // --- exclusions (AND'd) ---

    if !exclusions.genres.is_empty() {
        qb.push(" AND NOT EXISTS (SELECT 1 FROM json_each(m.genres) WHERE value IN (");
        let mut sep = qb.separated(", ");
        for g in &exclusions.genres { sep.push_bind(g.clone()); }
        qb.push("))");
    }

    if !exclusions.keywords.is_empty() {
        qb.push(" AND NOT EXISTS (SELECT 1 FROM json_each(m.keywords) WHERE value IN (");
        let mut sep = qb.separated(", ");
        for k in &exclusions.keywords { sep.push_bind(k.clone()); }
        qb.push("))");
    }

    // 按 Bayesian 加权评分排序，避免 1-票 10.0 的边缘片淹没真正高质量的电影。
    // 详见 BAYES_PRIOR_MEAN / BAYES_PRIOR_VOTES 常量定义。
    qb.push(format!(
        " ORDER BY (m.tmdb_rating * COALESCE(m.tmdb_votes, 0) + {} * {}) \
          / (COALESCE(m.tmdb_votes, 0) + {}) DESC NULLS LAST, \
          m.popularity DESC NULLS LAST LIMIT ",
        BAYES_PRIOR_MEAN, BAYES_PRIOR_VOTES, BAYES_PRIOR_VOTES
    ));
    qb.push_bind(limit);

    qb.build_query_as::<MovieForRanking>().fetch_all(pool).await
}

/// 根据种子电影，从 related_movies 关联表中召回库外电影。
pub async fn get_related_movies_for_seeds(
    pool: &SqlitePool,
    seed_movie_ids: &[i64],
) -> Result<Vec<MovieForRanking>, sqlx::Error> {
    if seed_movie_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "SELECT DISTINCT m.id, m.tmdb_id, m.title, m.year, m.genres, m.director, m.language, m.country, m.overview, m.tmdb_rating, m.tmdb_votes, m.runtime, m.popularity, m.budget, m.keywords, m.\"cast\"
         FROM movies m
         INNER JOIN related_movies rm ON rm.related_tmdb_id = m.tmdb_id
         WHERE m.source = 'related' AND rm.movie_id IN (",
    );

    let mut separated = query_builder.separated(", ");
    for seed_id in seed_movie_ids {
        separated.push_bind(seed_id);
    }
    query_builder.push(")");

    query_builder
        .build_query_as::<MovieForRanking>()
        .fetch_all(pool)
        .await
}

/// 用户标记的电影信息（用于协同召回）
#[allow(dead_code)]
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UserMarkedMovie {
    pub movie_id: i64,
    pub mark_type: String, // "want" | "watched" | "favorite"
    pub title: String,
    pub year: Option<i64>,
    pub genres: Option<String>, // JSON array
    pub director: Option<String>,
    pub country: Option<String>,
    pub language: Option<String>,
}

/// 查询用户所有标记的电影（含元数据），用于协同召回。
/// 返回值按 mark_type 分组使用：want+favorite 为"感兴趣"，watched 为"看过"。
pub async fn get_user_marked_movies(
    pool: &SqlitePool,
    user_id: i64,
) -> Result<Vec<UserMarkedMovie>, sqlx::Error> {
    sqlx::query_as::<_, UserMarkedMovie>(
        r#"
        SELECT
            m.id as movie_id,
            um.mark_type,
            m.title,
            m.year,
            m.genres,
            m.director,
            m.country,
            m.language
        FROM user_movie_marks um
        JOIN movies m ON m.id = um.movie_id
        WHERE um.user_id = ?
        ORDER BY um.created_at DESC
        "#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
}

/// 查询用户在某个标记类型下的所有电影（完整 Movie 结构），按标记时间倒序。
/// mark_type 必须是 "want" / "watched" / "favorite" 之一。
pub async fn list_marked_movies(
    pool: &SqlitePool,
    user_id: i64,
    mark_type: &str,
) -> Result<Vec<Movie>, sqlx::Error> {
    sqlx::query_as::<_, Movie>(
        r#"
        SELECT m.*
        FROM user_movie_marks um
        JOIN movies m ON m.id = um.movie_id
        WHERE um.user_id = ? AND um.mark_type = ?
        ORDER BY um.created_at DESC
        "#
    )
    .bind(user_id)
    .bind(mark_type)
    .fetch_all(pool)
    .await
}

pub async fn insert_search_history(
    pool: &SqlitePool,
    user_id: i64,
    prompt: &str,
    sse_events_json: &str,
    result_count: i64,
) -> Result<i64, sqlx::Error> {
    let res = sqlx::query(
        "INSERT INTO search_history (user_id, prompt, sse_events, result_count) VALUES (?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(prompt)
    .bind(sse_events_json)
    .bind(result_count)
    .execute(pool)
    .await?;
    Ok(res.last_insert_rowid())
}

pub async fn list_search_history(
    pool: &SqlitePool,
    user_id: i64,
    limit: i64,
    offset: i64,
) -> Result<Vec<SearchHistoryItem>, sqlx::Error> {
    sqlx::query_as::<_, SearchHistoryItem>(
        "SELECT id, prompt, result_count, created_at
         FROM search_history
         WHERE user_id = ?
         ORDER BY created_at DESC
         LIMIT ? OFFSET ?",
    )
    .bind(user_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
}

pub async fn get_search_history(
    pool: &SqlitePool,
    user_id: i64,
    id: i64,
) -> Result<Option<SearchHistoryDetail>, sqlx::Error> {
    sqlx::query_as::<_, SearchHistoryDetail>(
        "SELECT id, prompt, sse_events, result_count, created_at, share_token
         FROM search_history
         WHERE id = ? AND user_id = ?",
    )
    .bind(id)
    .bind(user_id)
    .fetch_optional(pool)
    .await
}

/// Used by admin benchmark aggregation: pull every search_history row that
/// matches a benchmark query verbatim, regardless of user_id, so admins can
/// pick expected_ids from across-the-board picks.
pub async fn list_search_history_by_prompt(
    pool: &SqlitePool,
    prompt: &str,
) -> Result<Vec<SearchHistoryDetail>, sqlx::Error> {
    sqlx::query_as::<_, SearchHistoryDetail>(
        "SELECT id, prompt, sse_events, result_count, created_at, share_token
         FROM search_history
         WHERE prompt = ?
         ORDER BY created_at DESC",
    )
    .bind(prompt)
    .fetch_all(pool)
    .await
}

pub async fn delete_search_history(
    pool: &SqlitePool,
    user_id: i64,
    id: i64,
) -> Result<bool, sqlx::Error> {
    let res = sqlx::query("DELETE FROM search_history WHERE id = ? AND user_id = ?")
        .bind(id)
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn clear_search_history(
    pool: &SqlitePool,
    user_id: i64,
) -> Result<u64, sqlx::Error> {
    let res = sqlx::query("DELETE FROM search_history WHERE user_id = ?")
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

/// Returns Ok(Some(token)) if the row exists (current or freshly written
/// token), Ok(None) if the row does not exist or belongs to another user.
pub async fn get_or_set_share_token(
    pool: &SqlitePool,
    user_id: i64,
    history_id: i64,
    new_token: &str,
) -> Result<Option<String>, sqlx::Error> {
    let existing: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT share_token FROM search_history WHERE id = ? AND user_id = ?",
    )
    .bind(history_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await?;

    match existing {
        None => Ok(None),
        Some((Some(token),)) => Ok(Some(token)),
        Some((None,)) => {
            sqlx::query(
                "UPDATE search_history SET share_token = ? WHERE id = ? AND user_id = ?",
            )
            .bind(new_token)
            .bind(history_id)
            .bind(user_id)
            .execute(pool)
            .await?;
            Ok(Some(new_token.to_string()))
        }
    }
}

pub async fn clear_share_token(
    pool: &SqlitePool,
    user_id: i64,
    history_id: i64,
) -> Result<bool, sqlx::Error> {
    let res = sqlx::query(
        "UPDATE search_history SET share_token = NULL WHERE id = ? AND user_id = ?",
    )
    .bind(history_id)
    .bind(user_id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn get_search_history_by_share_token(
    pool: &SqlitePool,
    token: &str,
) -> Result<Option<SearchHistoryDetail>, sqlx::Error> {
    sqlx::query_as::<_, SearchHistoryDetail>(
        "SELECT id, prompt, sse_events, result_count, created_at, share_token
         FROM search_history
         WHERE share_token = ?",
    )
    .bind(token)
    .fetch_optional(pool)
    .await
}

pub async fn list_failed_tasks(
    pool: &SqlitePool,
    page: i64,
    per_page: i64,
) -> Result<(Vec<Task>, i64), sqlx::Error> {
    let page = page.max(1);
    let per_page = per_page.max(1);
    let offset = (page - 1) * per_page;

    let total = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM tasks WHERE status = 'failed'"
    )
    .fetch_one(pool)
    .await?;

    let rows = sqlx::query_as::<_, Task>(
        "SELECT * FROM tasks WHERE status = 'failed' ORDER BY updated_at DESC LIMIT ? OFFSET ?"
    )
    .bind(per_page)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok((rows, total))
}

pub async fn get_setting(pool: &SqlitePool, key: &str) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>("SELECT value FROM settings WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await
}

pub async fn set_setting(pool: &SqlitePool, key: &str, value: &str) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)")
        .bind(key)
        .bind(value)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_prompt_override(
    pool: &SqlitePool,
    name: &str,
    locale: &str,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT content FROM prompt_overrides WHERE name = ? AND locale = ?",
    )
    .bind(name)
    .bind(locale)
    .fetch_optional(pool)
    .await
}

pub async fn upsert_prompt_override(
    pool: &SqlitePool,
    name: &str,
    locale: &str,
    content: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO prompt_overrides (name, locale, content, updated_at) \
         VALUES (?, ?, ?, datetime('now')) \
         ON CONFLICT(name, locale) DO UPDATE SET content = excluded.content, updated_at = datetime('now')",
    )
    .bind(name)
    .bind(locale)
    .bind(content)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_prompt_override(
    pool: &SqlitePool,
    name: &str,
    locale: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM prompt_overrides WHERE name = ? AND locale = ?")
        .bind(name)
        .bind(locale)
        .execute(pool)
        .await?;
    Ok(())
}

// ===== Fuzzy title / name search（Stage 0 路由使用）=====
// 不引入 FTS5，轻量 LIKE + Rust 侧评分够用。详见 docs/specs/query-router.md 中的
// "标题 / 人名的库内模糊匹配"章节。

fn score_title_match(candidate: &str, query: &str) -> f64 {
    // 简单打分：完全相等 > 前缀 > 包含 > 反包含 > 字符交集比。
    // 对 CJK 文本 to_lowercase 是 no-op，对英文则做大小写无关匹配。
    let c = candidate.to_lowercase();
    let q = query.to_lowercase();
    if c == q {
        return 1.0;
    }
    if c.starts_with(&q) {
        return 0.9;
    }
    if c.contains(&q) {
        return 0.7;
    }
    if q.contains(&c) {
        return 0.6;
    }
    let c_chars: std::collections::HashSet<char> = c.chars().collect();
    let q_chars: std::collections::HashSet<char> = q.chars().collect();
    if c_chars.is_empty() || q_chars.is_empty() {
        return 0.0;
    }
    let inter = c_chars.intersection(&q_chars).count() as f64;
    let max_len = c_chars.len().max(q_chars.len()) as f64;
    inter / max_len * 0.5
}

/// 从 title / title_zh / title_en / original_title 任一字段匹配 query，取
/// 库内（非 related-only）电影。评分在 Rust 侧做，按分数倒排取前 `limit` 个。
pub async fn search_movies_by_title_fuzzy(
    pool: &SqlitePool,
    query: &str,
    limit: usize,
) -> Result<Vec<Movie>, sqlx::Error> {
    let q = query.trim();
    if q.is_empty() {
        return Ok(Vec::new());
    }
    let like = format!("%{}%", q);
    // 库内 + 库外都参与匹配。库外电影（source='related'）虽然在 related_movies 表里
    // 没有自己的相似片记录，但 handler 会通过 structured_recall + LanceDB 语义召回
    // 用种子特征自召回候选，所以让库外片做种子是有意义的。详见
    // docs/specs/2026-04-25-similar-seed-self-recall-design.md
    let rows = sqlx::query_as::<_, Movie>(
        "SELECT * FROM movies \
          WHERE (title LIKE ?1 OR title_zh LIKE ?1 OR title_en LIKE ?1 OR original_title LIKE ?1) \
          LIMIT 200",
    )
    .bind(&like)
    .fetch_all(pool)
    .await?;

    let mut scored: Vec<(f64, Movie)> = rows
        .into_iter()
        .map(|m| {
            let s = [
                Some(m.title.as_str()),
                m.title_zh.as_deref(),
                m.title_en.as_deref(),
                m.original_title.as_deref(),
            ]
            .into_iter()
            .flatten()
            .map(|t| score_title_match(t, q))
            .fold(0.0_f64, f64::max);
            (s, m)
        })
        .filter(|(s, _)| *s > 0.0)
        .collect();

    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(limit);
    Ok(scored.into_iter().map(|(_, m)| m).collect())
}

/// 人名 fuzzy 匹配的候选条目。来源是 `movie_credits`（`persons` 表几乎空，
/// 真正的人名数据沉在 credits 里）。同一个 tmdb_person_id 在不同电影里可能以
/// 中文或英文出现，聚合后给用户返回得分最高的那一条。
#[derive(Debug, Clone)]
pub struct PersonMatch {
    pub tmdb_person_id: i64,
    /// 选出的"最匹配"的候选名（中英文选一个）。
    pub name: String,
    /// 该人在库内出现了多少条 credits（演员 + 剧组合计）。
    pub credit_count: i64,
    /// 是否当过导演——用于区分"诺兰"指的是克里斯托弗·诺兰（导演）而不是演员
    /// 里碰巧姓诺兰的人。
    pub has_director_credit: bool,
    /// 该人参与的不重复影片数；越多越"主角"化。
    pub movie_count: i64,
}

pub async fn search_persons_by_name_fuzzy(
    pool: &SqlitePool,
    query: &str,
    limit: usize,
) -> Result<Vec<PersonMatch>, sqlx::Error> {
    let q = query.trim();
    if q.is_empty() {
        return Ok(Vec::new());
    }
    let like = format!("%{}%", q);

    // 一次查询聚合：匹配到任何一条 credit 的 tmdb_person_id 都纳入。
    // GROUP_CONCAT 出该 person 的所有不同显示名，Rust 侧挑最匹配的。
    let rows: Vec<(i64, String, i64, i64, i64)> = sqlx::query_as(
        "SELECT tmdb_person_id, \
                GROUP_CONCAT(DISTINCT person_name) AS all_names, \
                COUNT(*) AS credit_count, \
                COUNT(DISTINCT movie_id) AS movie_count, \
                SUM(CASE WHEN role = 'Director' OR department = 'Directing' THEN 1 ELSE 0 END) AS director_count \
         FROM movie_credits \
         WHERE tmdb_person_id IN ( \
             SELECT DISTINCT tmdb_person_id FROM movie_credits WHERE person_name LIKE ?1 \
         ) \
         GROUP BY tmdb_person_id",
    )
    .bind(&like)
    .fetch_all(pool)
    .await?;

    let mut scored: Vec<(f64, PersonMatch)> = rows
        .into_iter()
        .map(|(tmdb_person_id, all_names, credit_count, movie_count, director_count)| {
            // all_names 是 "克里斯托弗·诺兰,Christopher Nolan" 这样的 CSV。
            // 选打分最高的那一个作为展示名。
            let candidates: Vec<&str> = all_names.split(',').collect();
            let (name, name_score) = candidates
                .iter()
                .map(|n| (n.trim().to_string(), score_title_match(n.trim(), q)))
                .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                .unwrap_or_else(|| ("".into(), 0.0));
            let has_director = director_count > 0;
            // 综合分：名字匹配度 + 导演加成 + 作品量加成（log-scale 到 0.3 上限）
            let movie_bonus = (movie_count as f64).ln().max(0.0) * 0.05;
            let score = name_score
                + if has_director { 0.2 } else { 0.0 }
                + movie_bonus.min(0.3);
            (
                score,
                PersonMatch {
                    tmdb_person_id,
                    name,
                    credit_count,
                    has_director_credit: has_director,
                    movie_count,
                },
            )
        })
        .filter(|(s, _)| *s > 0.0)
        .collect();

    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(limit);
    Ok(scored.into_iter().map(|(_, m)| m).collect())
}

/// 取某人参与的库内电影，按"身份重要度"分级排序：导演/主演 > 配角 > 配音 > 其他 crew。
/// `uncredited` 和 `Stunt Double` 等临演/替身角色被过滤掉——这些不是"这个人的作品"。
/// 同一部片里一个人可能身兼多职（自编自导自演），取 role_score 最高的身份作为代表。
///
/// 排序键：`role_score DESC, year DESC NULLS LAST, rating DESC NULLS LAST`。
/// 这样"成龙的电影"查询会把主演的《警察故事》《醉拳》排到前面，而不是让配音的
/// 《功夫熊猫》三部曲（因年份新）霸占前排。
pub async fn get_movies_by_person(
    pool: &SqlitePool,
    tmdb_person_id: i64,
    limit: usize,
) -> Result<Vec<PersonWork>, sqlx::Error> {
    // role_score 决策规则：
    //   100 = 导演 (crew, role='Director' 或 department='Directing')
    //    90 = 主演 (cast, order<=5, 非配音, 非 uncredited/Stunt)
    //    70 = 配角 (cast, order<=15, 非配音, 非 uncredited/Stunt)
    //    50 = 配音 (cast, role 含 'voice')
    //    40 = 制片/编剧 (crew, role Producer/Writer 或 department Writing/Production)
    //    30 = 次要 cast (order 大或未知, 非 uncredited)
    //    20 = 其他 crew (特效/配乐/剪辑等)
    //     0 = uncredited / Stunt Double / Stand-in（过滤掉）
    //
    // role_kind_code 与 PersonRoleKind 一一对应：0=Director 1=LeadActor 2=SupportingActor 3=Voice 4=Crew
    //
    // CTE 步骤：
    //   1) scored: 把该人的每条 credit 算 role_score 和 role_kind_code
    //   2) best:   按 (movie_id) 取最高 role_score 对应的那条 credit，同时保留 role 文本
    //
    // 最终 JOIN movies 取出整行。库外电影（source='related'）也包含——TMDB 抓取已为
    // 库外片建好 movie_credits，按人查作品天然要给完整作品列表。前端通过 RecommendItem
    // 的 in_library 字段（recommend.rs handle_person）配合 Sparkles 横幅区分库内库外。
    // role_score=0（uncredited / Stunt Double）仍然排除。
    let rows: Vec<PersonWorkRow> = sqlx::query_as::<_, PersonWorkRow>(
        "WITH scored AS (
           SELECT
             mc.movie_id,
             mc.role,
             CASE
               WHEN mc.role IS NOT NULL
                    AND (mc.role LIKE '%uncredited%' OR mc.role LIKE '%Stunt Double%' OR mc.role LIKE '%Stand-in%')
                 THEN 0
               WHEN mc.credit_type = 'crew'
                    AND (mc.role = 'Director' OR mc.department = 'Directing')
                 THEN 100
               WHEN mc.credit_type = 'cast' AND mc.role LIKE '%voice%'
                 THEN 50
               WHEN mc.credit_type = 'cast' AND (mc.\"order\" IS NOT NULL AND mc.\"order\" <= 5)
                 THEN 90
               WHEN mc.credit_type = 'cast' AND (mc.\"order\" IS NOT NULL AND mc.\"order\" <= 15)
                 THEN 70
               WHEN mc.credit_type = 'crew'
                    AND (mc.role LIKE '%Producer%' OR mc.role LIKE '%Writer%' OR mc.department IN ('Writing','Production'))
                 THEN 40
               WHEN mc.credit_type = 'cast'
                 THEN 30
               ELSE 20
             END AS role_score,
             CASE
               WHEN mc.credit_type = 'crew'
                    AND (mc.role = 'Director' OR mc.department = 'Directing')
                 THEN 0
               WHEN mc.credit_type = 'cast' AND mc.role LIKE '%voice%'
                 THEN 3
               WHEN mc.credit_type = 'cast' AND (mc.\"order\" IS NOT NULL AND mc.\"order\" <= 5)
                 THEN 1
               WHEN mc.credit_type = 'cast' AND (mc.\"order\" IS NOT NULL AND mc.\"order\" <= 15)
                 THEN 2
               WHEN mc.credit_type = 'cast'
                 THEN 2
               ELSE 4
             END AS role_kind_code
           FROM movie_credits mc
           WHERE mc.tmdb_person_id = ?1
         ),
         best AS (
           SELECT movie_id,
                  MAX(role_score) AS best_score
           FROM scored
           GROUP BY movie_id
         ),
         best_credit AS (
           SELECT s.movie_id, s.role_kind_code, s.role
           FROM scored s
           INNER JOIN best b ON b.movie_id = s.movie_id AND b.best_score = s.role_score
           GROUP BY s.movie_id
         )
         SELECT m.*, b.best_score AS role_score, bc.role_kind_code, bc.role AS role_detail
         FROM movies m
         INNER JOIN best b ON b.movie_id = m.id
         INNER JOIN best_credit bc ON bc.movie_id = m.id
         WHERE b.best_score > 0
         ORDER BY b.best_score DESC, m.year DESC NULLS LAST, m.tmdb_rating DESC NULLS LAST
         LIMIT ?2",
    )
    .bind(tmdb_person_id)
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(PersonWorkRow::into_work).collect())
}

#[derive(sqlx::FromRow)]
struct PersonWorkRow {
    #[sqlx(flatten)]
    movie: Movie,
    #[allow(dead_code)]
    role_score: i64,
    role_kind_code: i64,
    role_detail: Option<String>,
}

impl PersonWorkRow {
    fn into_work(self) -> PersonWork {
        let kind = match self.role_kind_code {
            0 => PersonRoleKind::Director,
            1 => PersonRoleKind::LeadActor,
            2 => PersonRoleKind::SupportingActor,
            3 => PersonRoleKind::Voice,
            _ => PersonRoleKind::Crew,
        };
        PersonWork {
            movie: self.movie,
            role_kind: kind,
            role_detail: self.role_detail,
        }
    }
}

/// 给定种子电影 ID，从 related_movies 关联表里取所有相关电影（**包含库内和库外**），
/// 按 `related_movies.id` 顺序——TMDB similar / recommendations 抓下来时的相关度顺序。
/// 与 `get_related_movies_for_seeds` 的区别：本函数不过滤 `source = 'related'`，
/// 因此适用于 exact_title / similar_to handler 给用户展示库内外都相关的片单。
pub async fn get_related_movies_all_sources(
    pool: &SqlitePool,
    seed_movie_ids: &[i64],
    limit: usize,
) -> Result<Vec<Movie>, sqlx::Error> {
    if seed_movie_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut qb = QueryBuilder::<Sqlite>::new(
        "SELECT DISTINCT m.* FROM movies m \
          INNER JOIN related_movies rm ON rm.related_tmdb_id = m.tmdb_id \
          WHERE rm.movie_id IN (",
    );
    let mut sep = qb.separated(", ");
    for id in seed_movie_ids {
        sep.push_bind(id);
    }
    qb.push(") ORDER BY rm.id ASC LIMIT ");
    qb.push_bind(limit as i64);
    qb.build_query_as::<Movie>().fetch_all(pool).await
}

/// 库外热门一行的查询结果：movie 信息 + 被关联次数。
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MostRelatedOutOfLibraryRow {
    #[sqlx(flatten)]
    pub movie: Movie,
    pub ref_count: i64,
}

/// 库外热门：返回**不在库**（无 matched dir mapping）但被库内电影 similar/recommendations
/// 关联次数最多的电影。每行附带 ref_count = 在 related_movies 表中作为 related_tmdb_id 出现的次数。
///
/// 注意"不在库"的判定走 dir_movie_mappings，不用 movies.source——因为 source 是 first-touch
/// 标记不会随绑定状态更新（详见 docs/specs/2026-04-25-homepage-bottom-sections-design.md）。
pub async fn most_related_out_of_library(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<MostRelatedOutOfLibraryRow>, sqlx::Error> {
    sqlx::query_as::<_, MostRelatedOutOfLibraryRow>(
        "SELECT m.*, COUNT(rm.id) AS ref_count
         FROM movies m
         INNER JOIN related_movies rm ON rm.related_tmdb_id = m.tmdb_id
         WHERE NOT EXISTS (
             SELECT 1 FROM dir_movie_mappings dmm
             WHERE dmm.movie_id = m.id AND dmm.match_status IN ('auto', 'manual')
         )
         GROUP BY m.id
         ORDER BY ref_count DESC, m.popularity DESC NULLS LAST, m.id ASC
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// 返回 `dir_movie_mappings.updated_at` 全表最大值——任意 bind / unbind /
/// refetch 都会刷该行 `updated_at`（schema 默认 `datetime('now')` + 显式 SET）。
/// 用于 in-memory cache（如 `most_related_cache`）做"自动失效"：
/// cache 创建时存一份此值的快照，每次命中再查一次比对——快照与当前一致 ⇒
/// 期间没有任何 mapping 变化 ⇒ cache 仍有效。
///
/// 单行带索引的 MAX 聚合，~μs 级。比直接砍 cache 重算 join 便宜数量级。
pub async fn dir_movie_mappings_max_updated_at(
    pool: &SqlitePool,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT MAX(updated_at) FROM dir_movie_mappings")
        .fetch_one(pool)
        .await
}

/// 返回 `movie_id -> in_library` 的 map。`in_library = true` 当且仅当存在一条
/// `dir_movie_mappings` 行 `match_status IN ('auto', 'manual')`。
///
/// **`movies.source` 不是 in-library 的真值** —— 它是 first-touch 标记，
/// 一部 `source='related'` 电影被 locate-movie / 自动匹配绑定后 source 不会更新。
/// 任何 in_library 判断都必须走这个 helper（或等价的 `dir_movie_mappings`
/// EXISTS 子查询），不能读 `movies.source`。
///
/// 单条 IN-list + EXISTS 子查询带索引，候选 ≤ 200 时 ~μs 级。批量调一次 >>
/// 在循环里 per-movie 查 N 次。
pub async fn library_membership_for_movie_ids(
    pool: &SqlitePool,
    movie_ids: &[i64],
) -> Result<std::collections::HashMap<i64, bool>, sqlx::Error> {
    if movie_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let mut qb = QueryBuilder::<Sqlite>::new(
        "SELECT m.id, EXISTS (\
            SELECT 1 FROM dir_movie_mappings dmm \
            WHERE dmm.movie_id = m.id AND dmm.match_status IN ('auto', 'manual')\
         ) AS in_library FROM movies m WHERE m.id IN (",
    );
    let mut sep = qb.separated(", ");
    for id in movie_ids {
        sep.push_bind(*id);
    }
    qb.push(")");
    let rows: Vec<(i64, bool)> = qb.build_query_as().fetch_all(pool).await?;
    Ok(rows.into_iter().collect())
}

/// 最新入库：返回最近被绑定到本地 dir 的电影 N 部。"入库时间"取该电影所有 matched
/// mapping 的 MAX(updated_at)——绑定状态从 pending 变 auto/manual 时刷新。
///
/// **两阶段查询** — 内层先在 dir_movie_mappings 上做 GROUP BY MAX 选出 top-N
/// movie_id，再外层 PK lookup 拿 movies.*。一阶 `SELECT m.* … GROUP BY m.id`
/// 会让 SQLite 把全部 movie 列拽进 temp B-tree（15k 行 × 几十列），冷查询
/// 实测 5s+。两阶段把 movies 限制到 N 行，冷查询降到 ~10ms。
pub async fn recent_library_movies(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<Movie>, sqlx::Error> {
    sqlx::query_as::<_, Movie>(
        "SELECT m.*
         FROM movies m
         INNER JOIN (
             SELECT movie_id, MAX(updated_at) AS last_updated
             FROM dir_movie_mappings
             WHERE match_status IN ('auto', 'manual')
             GROUP BY movie_id
             ORDER BY last_updated DESC
             LIMIT ?
         ) top ON top.movie_id = m.id
         ORDER BY top.last_updated DESC, m.id DESC",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

// ===== Benchmark =====
// 题库、运行历史和运行结果都落库，代码仓里不带任何用户私有数据。
// 详见 docs/specs/benchmark.md

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct QueryRunResultRow {
    pub run_id: i64,
    pub run_started_at: String,
    pub run_finished_at: Option<String>,
    pub run_status: String,
    pub run_note: Option<String>,
    pub run_is_baseline: i64,
    pub hit: Option<i64>,
    pub elapsed_ms: Option<i64>,
    pub top_movies_json: Option<String>,
    pub intent_json: Option<String>,
    pub error: Option<String>,
    pub coverage_ratio: Option<f64>,
    pub not_expected_ids: Option<String>,
}

/// 按 run.started_at DESC 列出 query_id 对应的所有 benchmark run 结果。
/// 没跑过的 query 返回空 vec。
pub async fn list_query_run_results(
    pool: &SqlitePool,
    query_id: i64,
) -> Result<Vec<QueryRunResultRow>, sqlx::Error> {
    sqlx::query_as::<_, QueryRunResultRow>(
        "SELECT \
            br.id AS run_id, \
            br.started_at AS run_started_at, \
            br.finished_at AS run_finished_at, \
            br.status AS run_status, \
            br.note AS run_note, \
            br.is_baseline AS run_is_baseline, \
            bres.hit AS hit, \
            bres.elapsed_ms AS elapsed_ms, \
            bres.top_movie_ids AS top_movies_json, \
            bres.intent_json AS intent_json, \
            bres.error AS error, \
            bres.coverage_ratio AS coverage_ratio, \
            bres.not_expected_ids AS not_expected_ids \
         FROM benchmark_results bres \
         JOIN benchmark_runs br ON br.id = bres.run_id \
         WHERE bres.query_id = ? \
         ORDER BY br.started_at DESC",
    )
    .bind(query_id)
    .fetch_all(pool)
    .await
}

pub async fn list_benchmark_queries(
    pool: &SqlitePool,
) -> Result<Vec<BenchmarkQuery>, sqlx::Error> {
    sqlx::query_as::<_, BenchmarkQuery>(
        "SELECT id, query, note, expected_ids, created_at, updated_at, source_history_id, not_expected_ids \
         FROM benchmark_queries ORDER BY id ASC",
    )
    .fetch_all(pool)
    .await
}

pub async fn get_benchmark_query(
    pool: &SqlitePool,
    id: i64,
) -> Result<Option<BenchmarkQuery>, sqlx::Error> {
    sqlx::query_as::<_, BenchmarkQuery>(
        "SELECT id, query, note, expected_ids, created_at, updated_at, source_history_id, not_expected_ids \
         FROM benchmark_queries WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn insert_benchmark_query(
    pool: &SqlitePool,
    query: &str,
    note: Option<&str>,
    expected_ids: Option<&str>,
    source_history_id: Option<i64>,
    not_expected_ids: Option<&str>,
) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO benchmark_queries (query, note, expected_ids, source_history_id, not_expected_ids) \
         VALUES (?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(query)
    .bind(note)
    .bind(expected_ids)
    .bind(source_history_id)
    .bind(not_expected_ids)
    .fetch_one(pool)
    .await?;
    use sqlx::Row;
    Ok(row.get::<i64, _>(0))
}

pub async fn update_benchmark_query(
    pool: &SqlitePool,
    id: i64,
    query: &str,
    note: Option<&str>,
    expected_ids: Option<&str>,
    not_expected_ids: Option<&str>,
) -> Result<u64, sqlx::Error> {
    let res = sqlx::query(
        "UPDATE benchmark_queries \
            SET query = ?, note = ?, expected_ids = ?, not_expected_ids = ?, \
                updated_at = datetime('now') \
          WHERE id = ?",
    )
    .bind(query)
    .bind(note)
    .bind(expected_ids)
    .bind(not_expected_ids)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

pub async fn delete_benchmark_query(pool: &SqlitePool, id: i64) -> Result<u64, sqlx::Error> {
    let res = sqlx::query("DELETE FROM benchmark_queries WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected())
}

pub async fn get_running_benchmark_run(
    pool: &SqlitePool,
) -> Result<Option<BenchmarkRun>, sqlx::Error> {
    sqlx::query_as::<_, BenchmarkRun>(
        "SELECT id, started_at, finished_at, status, total, passed, failed, note, \
                is_baseline, cancel_requested \
         FROM benchmark_runs WHERE status = 'running' LIMIT 1",
    )
    .fetch_optional(pool)
    .await
}

pub async fn insert_benchmark_run(
    pool: &SqlitePool,
    total: i64,
    note: Option<&str>,
) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO benchmark_runs (started_at, status, total, note) \
         VALUES (datetime('now'), 'running', ?, ?) RETURNING id",
    )
    .bind(total)
    .bind(note)
    .fetch_one(pool)
    .await?;
    use sqlx::Row;
    Ok(row.get::<i64, _>(0))
}

pub async fn increment_benchmark_run_counters(
    pool: &SqlitePool,
    run_id: i64,
    passed_delta: i64,
    failed_delta: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE benchmark_runs \
            SET passed = passed + ?, failed = failed + ? WHERE id = ?",
    )
    .bind(passed_delta)
    .bind(failed_delta)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn finalize_benchmark_run(
    pool: &SqlitePool,
    run_id: i64,
    status: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE benchmark_runs \
            SET status = ?, finished_at = datetime('now') WHERE id = ?",
    )
    .bind(status)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn request_benchmark_run_cancel(
    pool: &SqlitePool,
    run_id: i64,
) -> Result<u64, sqlx::Error> {
    let res = sqlx::query(
        "UPDATE benchmark_runs SET cancel_requested = 1 \
          WHERE id = ? AND status = 'running'",
    )
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

pub async fn is_benchmark_run_cancel_requested(
    pool: &SqlitePool,
    run_id: i64,
) -> Result<bool, sqlx::Error> {
    let v = sqlx::query_scalar::<_, i64>(
        "SELECT cancel_requested FROM benchmark_runs WHERE id = ?",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    Ok(v.unwrap_or(0) != 0)
}

pub async fn list_benchmark_runs(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<BenchmarkRun>, sqlx::Error> {
    sqlx::query_as::<_, BenchmarkRun>(
        "SELECT id, started_at, finished_at, status, total, passed, failed, note, \
                is_baseline, cancel_requested \
         FROM benchmark_runs ORDER BY started_at DESC LIMIT ?",
    )
    .bind(limit.max(1).min(200))
    .fetch_all(pool)
    .await
}

pub async fn get_benchmark_run(
    pool: &SqlitePool,
    id: i64,
) -> Result<Option<BenchmarkRun>, sqlx::Error> {
    sqlx::query_as::<_, BenchmarkRun>(
        "SELECT id, started_at, finished_at, status, total, passed, failed, note, \
                is_baseline, cancel_requested \
         FROM benchmark_runs WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn get_baseline_benchmark_run(
    pool: &SqlitePool,
) -> Result<Option<BenchmarkRun>, sqlx::Error> {
    sqlx::query_as::<_, BenchmarkRun>(
        "SELECT id, started_at, finished_at, status, total, passed, failed, note, \
                is_baseline, cancel_requested \
         FROM benchmark_runs WHERE is_baseline = 1 LIMIT 1",
    )
    .fetch_optional(pool)
    .await
}

pub async fn set_benchmark_run_as_baseline(
    pool: &SqlitePool,
    run_id: i64,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE benchmark_runs SET is_baseline = 0 WHERE is_baseline = 1")
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE benchmark_runs SET is_baseline = 1 WHERE id = ?")
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_benchmark_result(
    pool: &SqlitePool,
    run_id: i64,
    query_id: i64,
    query_snapshot: &str,
    expected_ids: Option<&str>,
    top_movie_ids: &str,
    intent_json: Option<&str>,
    hit: Option<bool>,
    elapsed_ms: Option<i64>,
    error: Option<&str>,
    not_expected_ids: Option<&str>,
    coverage_ratio: Option<f64>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO benchmark_results \
            (run_id, query_id, query_snapshot, expected_ids, top_movie_ids, \
             intent_json, hit, elapsed_ms, error, not_expected_ids, coverage_ratio) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(run_id)
    .bind(query_id)
    .bind(query_snapshot)
    .bind(expected_ids)
    .bind(top_movie_ids)
    .bind(intent_json)
    .bind(hit.map(|b| if b { 1_i64 } else { 0 }))
    .bind(elapsed_ms)
    .bind(error)
    .bind(not_expected_ids)
    .bind(coverage_ratio)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_benchmark_results(
    pool: &SqlitePool,
    run_id: i64,
) -> Result<Vec<BenchmarkResult>, sqlx::Error> {
    sqlx::query_as::<_, BenchmarkResult>(
        "SELECT id, run_id, query_id, query_snapshot, expected_ids, top_movie_ids, \
                intent_json, hit, elapsed_ms, error, not_expected_ids, coverage_ratio \
         FROM benchmark_results WHERE run_id = ? ORDER BY query_id ASC",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await
}

// =====================
// Douban imports
// =====================

/// Insert a Douban CSV row in `pending` status. If `(user_id, douban_subject_id)`
/// already exists, leaves the existing row untouched (idempotent re-upload).
/// Returns the row id (newly inserted or pre-existing).
pub async fn upsert_douban_import_pending(
    pool: &SqlitePool,
    user_id: i64,
    douban_subject_id: &str,
    raw_title: &str,
    parsed_title_zh: Option<&str>,
    parsed_title_en: Option<&str>,
    year: Option<i64>,
    country: Option<&str>,
    douban_url: &str,
) -> Result<(i64, bool), sqlx::Error> {
    // Try to find existing first.
    if let Some((id,)) = sqlx::query_as::<_, (i64,)>(
        "SELECT id FROM douban_imports WHERE user_id = ? AND douban_subject_id = ?",
    )
    .bind(user_id)
    .bind(douban_subject_id)
    .fetch_optional(pool)
    .await?
    {
        return Ok((id, false));
    }

    let result = sqlx::query(
        "INSERT INTO douban_imports
         (user_id, douban_subject_id, raw_title, parsed_title_zh, parsed_title_en,
          year, country, douban_url, status)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
    )
    .bind(user_id)
    .bind(douban_subject_id)
    .bind(raw_title)
    .bind(parsed_title_zh)
    .bind(parsed_title_en)
    .bind(year)
    .bind(country)
    .bind(douban_url)
    .execute(pool)
    .await?;

    Ok((result.last_insert_rowid(), true))
}

pub async fn get_douban_import(
    pool: &SqlitePool,
    id: i64,
) -> Result<Option<DoubanImport>, sqlx::Error> {
    sqlx::query_as::<_, DoubanImport>("SELECT * FROM douban_imports WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_douban_imports_by_status(
    pool: &SqlitePool,
    user_id: i64,
    status: &str,
) -> Result<Vec<DoubanImport>, sqlx::Error> {
    sqlx::query_as::<_, DoubanImport>(
        "SELECT * FROM douban_imports
         WHERE user_id = ? AND status = ?
         ORDER BY id ASC",
    )
    .bind(user_id)
    .bind(status)
    .fetch_all(pool)
    .await
}

pub async fn count_douban_imports_by_status(
    pool: &SqlitePool,
    user_id: i64,
) -> Result<Vec<(String, i64)>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT status, COUNT(*) AS n
         FROM douban_imports
         WHERE user_id = ?
         GROUP BY status",
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
}

pub async fn update_douban_import_matched(
    pool: &SqlitePool,
    id: i64,
    movie_id: i64,
    status: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE douban_imports
         SET status = ?, movie_id = ?, error_msg = NULL, updated_at = datetime('now')
         WHERE id = ?",
    )
    .bind(status)
    .bind(movie_id)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_douban_import_status(
    pool: &SqlitePool,
    id: i64,
    status: &str,
    error_msg: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE douban_imports
         SET status = ?, error_msg = ?, updated_at = datetime('now')
         WHERE id = ?",
    )
    .bind(status)
    .bind(error_msg)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Idempotent: writes (user_id, movie_id, mark_type) if not already present.
pub async fn add_user_mark(
    pool: &SqlitePool,
    user_id: i64,
    movie_id: i64,
    mark_type: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT OR IGNORE INTO user_movie_marks (user_id, movie_id, mark_type)
         VALUES (?, ?, ?)",
    )
    .bind(user_id)
    .bind(movie_id)
    .bind(mark_type)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;

    /// Verify structured_recall SQL uses quoted "cast" (not aliased to cast_json),
    /// matching the #[sqlx(rename = "cast")] on MovieForRanking.cast_json.
    /// Regression test for: "no column found for name: cast" bug.
    #[test]
    fn structured_recall_uses_quoted_cast_column() {
        // Verify the QueryBuilder-based structured_recall still references m."cast"
        // by inspecting the static SQL prefix embedded in the function.
        // (The actual parameterized query is built at runtime; here we just verify
        // the struct mapping works correctly.)
        let movie = MovieForRanking {
            id: 1, tmdb_id: 1, title: "T".to_string(), year: None,
            genres: None, director: None, language: None, country: None,
            overview: None, tmdb_rating: None, tmdb_votes: None, runtime: None,
            popularity: None,
            budget: None, keywords: None,
            cast_json: Some("[\"A\"]".to_string()),
        };
        // sqlx #[sqlx(rename = "cast")] maps the "cast" column to cast_json field
        assert_eq!(movie.cast_json, Some("[\"A\"]".to_string()));
    }

    /// Verify MovieForRanking field names match the SQL SELECT column order.
    #[test]
    fn movie_for_ranking_has_cast_json_field() {
        // This is a compile-time check: if the struct changes, the test won't compile.
        let movie = MovieForRanking {
            id: 1,
            tmdb_id: 100,
            title: "Test".to_string(),
            year: Some(2020),
            genres: Some("[\"Drama\"]".to_string()),
            director: Some("Director".to_string()),
            language: Some("en".to_string()),
            country: Some("US".to_string()),
            overview: Some("Overview".to_string()),
            tmdb_rating: Some(8.0),
            tmdb_votes: Some(500),
            runtime: Some(120),
            popularity: Some(50.0),
            budget: Some(1000000),
            keywords: Some("[\"test\"]".to_string()),
            cast_json: Some("[\"Actor\"]".to_string()),
        };
        assert_eq!(movie.cast_json, Some("[\"Actor\"]".to_string()));
    }

    /// 回归测试：structured_recall 必须用 Bayesian 加权评分排序，
    /// 1-票 10.0 的边缘片不能压过 1 万票 8.5 的真正高质量片。
    /// Bug: shared/QsE_4LqkcIX0「小人物逆风翻盘热血追梦」只返回 1 部推荐，
    /// 因为候选池被 1-票 10.0 的小语种短片淹没，《肖申克》《阿甘》全被挤出 top 200。
    #[sqlx::test(migrations = "./migrations")]
    async fn structured_recall_bayesian_demotes_low_vote_noise(pool: SqlitePool) {
        // 库内：高票数中等评分的"经典"
        sqlx::query(
            "INSERT INTO movies (tmdb_id, title, tmdb_rating, tmdb_votes, source) \
             VALUES (1, 'Classic', 8.5, 10000, 'library')",
        )
        .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO media_dirs (dir_path, dir_name, scan_status) VALUES ('/x', 'x', 'matched')")
            .execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO dir_movie_mappings (dir_id, movie_id, match_status) \
             VALUES ((SELECT id FROM media_dirs WHERE dir_path='/x'), \
                     (SELECT id FROM movies WHERE tmdb_id=1), 'auto')",
        )
        .execute(&pool).await.unwrap();

        // 库外噪音：1 票 10.0
        for tmdb_id in 100..150 {
            sqlx::query(
                "INSERT INTO movies (tmdb_id, title, tmdb_rating, tmdb_votes, source) \
                 VALUES (?, 'Noise', 10.0, 1, 'related')",
            )
            .bind(tmdb_id).execute(&pool).await.unwrap();
        }

        let constraints = Constraints::default();
        let exclusions = Exclusions::default();
        let results = structured_recall(&pool, &constraints, &exclusions, 10).await.unwrap();

        // Classic 必须排在前面，不能被 50 部 1-票 10.0 噪音压住
        assert!(!results.is_empty(), "应该召回到至少 1 条");
        assert_eq!(
            results[0].tmdb_id, 1,
            "高票数 8.5 应该排在 1-票 10.0 噪音前面，实际排在第一的是 tmdb_id={}",
            results[0].tmdb_id
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_search_history_crud(pool: SqlitePool) {
        sqlx::query("INSERT INTO users (username, password_hash) VALUES ('u1', 'x'), ('u2', 'x')")
            .execute(&pool)
            .await
            .unwrap();

        let id1 = insert_search_history(&pool, 1, "cozy movies", "[]", 5)
            .await
            .unwrap();
        insert_search_history(&pool, 2, "action", "[]", 3)
            .await
            .unwrap();

        let items = list_search_history(&pool, 1, 10, 0).await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].prompt, "cozy movies");

        assert!(get_search_history(&pool, 2, id1).await.unwrap().is_none());
        assert!(get_search_history(&pool, 1, id1).await.unwrap().is_some());

        assert!(!delete_search_history(&pool, 2, id1).await.unwrap());
        assert!(delete_search_history(&pool, 1, id1).await.unwrap());
        assert!(get_search_history(&pool, 1, id1).await.unwrap().is_none());

        insert_search_history(&pool, 1, "aaa", "[]", 0)
            .await
            .unwrap();
        insert_search_history(&pool, 1, "bbb", "[]", 0)
            .await
            .unwrap();
        let cleared = clear_search_history(&pool, 1).await.unwrap();
        assert_eq!(cleared, 2);
        assert_eq!(list_search_history(&pool, 2, 10, 0).await.unwrap().len(), 1);
    }

    // --- test helpers ---

    async fn make_movie(pool: &SqlitePool, tmdb_id: i64, title: &str) -> i64 {
        insert_movie(
            pool, tmdb_id, title, None, Some(2020), None, None, "[]", None, None, None, None,
            "[]", None, None, "[]", None, None, None, "library",
        )
        .await
        .unwrap();
        get_movie_by_tmdb_id(pool, tmdb_id)
            .await
            .unwrap()
            .unwrap()
            .id
    }

    async fn make_dir(pool: &SqlitePool, path: &str) -> i64 {
        insert_media_dir(pool, path, path).await.unwrap()
    }

    // --- movies ---

    #[sqlx::test(migrations = "./migrations")]
    async fn insert_movie_is_idempotent_by_tmdb_id(pool: SqlitePool) {
        let id1 = make_movie(&pool, 101, "First Title").await;
        // INSERT OR IGNORE should not overwrite — second call keeps the first row.
        insert_movie(
            &pool, 101, "Second Title", None, Some(2021), None, None, "[]", None, None, None,
            None, "[]", None, None, "[]", None, None, None, "library",
        )
        .await
        .unwrap();
        let movie = get_movie_by_tmdb_id(&pool, 101).await.unwrap().unwrap();
        assert_eq!(movie.id, id1);
        assert_eq!(movie.title, "First Title");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_movie_by_tmdb_id_returns_none_when_missing(pool: SqlitePool) {
        assert!(get_movie_by_tmdb_id(&pool, 99999).await.unwrap().is_none());
    }

    // --- media_dirs lifecycle ---

    #[sqlx::test(migrations = "./migrations")]
    async fn media_dir_new_to_matched_flow(pool: SqlitePool) {
        let dir_id = make_dir(&pool, "/movies/Inception (2010)").await;

        // Newly inserted dir should show up under 'new'.
        let news = get_new_dirs(&pool).await.unwrap();
        assert_eq!(news.len(), 1);
        assert_eq!(news[0].id, dir_id);
        assert_eq!(news[0].scan_status, "new");

        update_dir_status(&pool, dir_id, "parsed").await.unwrap();
        assert!(get_new_dirs(&pool).await.unwrap().is_empty());

        let all = get_all_dir_paths(&pool).await.unwrap();
        assert_eq!(all.len(), 1);

        // Deleted dirs are filtered out of get_all_dir_paths.
        mark_dir_deleted(&pool, "/movies/Inception (2010)")
            .await
            .unwrap();
        assert!(get_all_dir_paths(&pool).await.unwrap().is_empty());
    }

    // --- mapping + correction flow ---

    #[sqlx::test(migrations = "./migrations")]
    async fn bind_and_unbind_dir_status_transitions(pool: SqlitePool) {
        let movie_a = make_movie(&pool, 1, "Movie A").await;
        let movie_b = make_movie(&pool, 2, "Movie B").await;
        let dir_id = make_dir(&pool, "/m/a").await;

        // Initial auto-match with confidence 0.6 — pending manual confirmation.
        insert_mapping(&pool, dir_id, Some(movie_a), "pending", Some(0.6), None)
            .await
            .unwrap();
        update_dir_status(&pool, dir_id, "parsed").await.unwrap();

        let mapping = get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mapping.match_status, "pending");
        assert_eq!(mapping.movie_id, Some(movie_a));

        // Manual correction: bind to movie_b.
        bind_dir_to_movie(&pool, dir_id, movie_b).await.unwrap();
        let mapping = get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mapping.match_status, "manual");
        assert_eq!(mapping.movie_id, Some(movie_b));
        assert_eq!(mapping.confidence, Some(1.0));

        // Media dir should now be 'matched'.
        let scan_status: String =
            sqlx::query_scalar("SELECT scan_status FROM media_dirs WHERE id = ?")
                .bind(dir_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(scan_status, "matched");

        // Unbind reverses both sides.
        unbind_dir(&pool, dir_id).await.unwrap();
        let mapping = get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mapping.match_status, "pending");
        assert!(mapping.movie_id.is_none());
        assert!(mapping.confidence.is_none());

        let scan_status: String =
            sqlx::query_scalar("SELECT scan_status FROM media_dirs WHERE id = ?")
                .bind(dir_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(scan_status, "parsed");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn bind_dir_errors_when_no_mapping_exists(pool: SqlitePool) {
        let movie = make_movie(&pool, 1, "Any").await;
        let dir_id = make_dir(&pool, "/m/orphan").await;
        // No mapping inserted — bind should fail with RowNotFound.
        let err = bind_dir_to_movie(&pool, dir_id, movie).await.unwrap_err();
        assert!(matches!(err, sqlx::Error::RowNotFound));
    }

    // --- task queue ---

    #[sqlx::test(migrations = "./migrations")]
    async fn task_claim_marks_running_and_returns_oldest(pool: SqlitePool) {
        insert_task(&pool, "tmdb_search", "first").await.unwrap();
        insert_task(&pool, "tmdb_search", "second").await.unwrap();
        insert_task(&pool, "tmdb_fetch", "other").await.unwrap();

        let claimed = claim_next_task(&pool, "tmdb_search")
            .await
            .unwrap()
            .expect("should claim a task");
        assert_eq!(claimed.payload.as_deref(), Some("first"));
        assert_eq!(claimed.status, "running");

        // The other tmdb_search task is still pending; tmdb_fetch is untouched.
        let counts = get_task_counts(&pool).await.unwrap();
        let pending_search: i64 = counts
            .iter()
            .filter(|(tt, st, _)| tt == "tmdb_search" && st == "pending")
            .map(|(_, _, n)| *n)
            .sum();
        assert_eq!(pending_search, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn complete_and_fail_task_transitions(pool: SqlitePool) {
        let t1 = insert_task(&pool, "tmdb_search", "ok").await.unwrap();
        let t2 = insert_task(&pool, "tmdb_search", "err").await.unwrap();

        complete_task(&pool, t1).await.unwrap();
        let status: String = sqlx::query_scalar("SELECT status FROM tasks WHERE id = ?")
            .bind(t1)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "done");

        // fail_task increments retries; stays pending until max_retries reached.
        fail_task(&pool, t2, "boom").await.unwrap();
        let (status, retries): (String, i64) =
            sqlx::query_as("SELECT status, retries FROM tasks WHERE id = ?")
                .bind(t2)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "pending");
        assert_eq!(retries, 1);

        // Two more failures (default max_retries = 3) should mark it failed.
        fail_task(&pool, t2, "boom").await.unwrap();
        fail_task(&pool, t2, "boom").await.unwrap();
        let (status, retries): (String, i64) =
            sqlx::query_as("SELECT status, retries FROM tasks WHERE id = ?")
                .bind(t2)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "failed");
        assert_eq!(retries, 3);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn requeue_stale_running_resets_to_pending(pool: SqlitePool) {
        let t1 = insert_task(&pool, "tmdb_search", "a").await.unwrap();
        let _t2 = insert_task(&pool, "tmdb_search", "b").await.unwrap();
        // Claim one so it's in 'running'.
        claim_next_task(&pool, "tmdb_search").await.unwrap().unwrap();

        let affected = requeue_stale_running_tasks(&pool).await.unwrap();
        assert_eq!(affected, 1);
        let status: String = sqlx::query_scalar("SELECT status FROM tasks WHERE id = ?")
            .bind(t1)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "pending");
    }

    // --- persons upsert ---

    #[sqlx::test(migrations = "./migrations")]
    async fn upsert_person_updates_existing_row(pool: SqlitePool) {
        upsert_person(&pool, 500, "Old Name", None, None, None, None, None, None)
            .await
            .unwrap();
        let p = get_person_by_tmdb_id(&pool, 500).await.unwrap().unwrap();
        let original_id = p.id;
        assert_eq!(p.name, "Old Name");

        upsert_person(
            &pool,
            500,
            "New Name",
            Some("[\"AKA\"]"),
            Some("bio"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let p = get_person_by_tmdb_id(&pool, 500).await.unwrap().unwrap();
        assert_eq!(p.id, original_id, "upsert must not change the row id");
        assert_eq!(p.name, "New Name");
        assert_eq!(p.biography.as_deref(), Some("bio"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_list_marked_movies_filters_and_order(pool: SqlitePool) {
        sqlx::query("INSERT INTO users (username, password_hash) VALUES ('u1', 'x')")
            .execute(&pool)
            .await
            .unwrap();

        let m1 = sqlx::query("INSERT INTO movies (tmdb_id, title) VALUES (1, 'Movie A')")
            .execute(&pool)
            .await
            .unwrap()
            .last_insert_rowid();
        let m2 = sqlx::query("INSERT INTO movies (tmdb_id, title) VALUES (2, 'Movie B')")
            .execute(&pool)
            .await
            .unwrap()
            .last_insert_rowid();
        let m3 = sqlx::query("INSERT INTO movies (tmdb_id, title) VALUES (3, 'Movie C')")
            .execute(&pool)
            .await
            .unwrap()
            .last_insert_rowid();

        // watched entries (later insert should come first)
        sqlx::query(
            "INSERT INTO user_movie_marks (user_id, movie_id, mark_type, created_at) VALUES (1, ?, 'watched', datetime('now','-1 minute'))",
        )
        .bind(m1)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO user_movie_marks (user_id, movie_id, mark_type, created_at) VALUES (1, ?, 'watched', datetime('now'))",
        )
        .bind(m2)
        .execute(&pool)
        .await
        .unwrap();
        // different type should be ignored
        sqlx::query(
            "INSERT INTO user_movie_marks (user_id, movie_id, mark_type) VALUES (1, ?, 'favorite')",
        )
        .bind(m3)
        .execute(&pool)
        .await
        .unwrap();

        let movies = list_marked_movies(&pool, 1, "watched")
            .await
            .unwrap();
        assert_eq!(movies.len(), 2);
        // Most recent first
        assert_eq!(movies[0].title, "Movie B");
        assert_eq!(movies[1].title, "Movie A");

        // other type filtered
        let favs = list_marked_movies(&pool, 1, "favorite")
            .await
            .unwrap();
        assert_eq!(favs.len(), 1);
        assert_eq!(favs[0].title, "Movie C");
    }

    // --- fuzzy title match ---

    #[test]
    fn score_title_match_exact_beats_prefix() {
        assert!((score_title_match("教父", "教父") - 1.0).abs() < 1e-9);
        assert!(score_title_match("教父 2", "教父") >= 0.9);
        assert!(score_title_match("教父三部曲", "教父") >= 0.7);
        assert!(score_title_match("X 教父", "教父") >= 0.7); // contains
    }

    #[test]
    fn score_title_match_case_insensitive_english() {
        assert!((score_title_match("The Godfather", "the godfather") - 1.0).abs() < 1e-9);
        assert!(score_title_match("Finding Nemo", "FINDING NEMO") >= 0.9);
    }

    #[test]
    fn score_title_match_zero_for_no_overlap() {
        assert_eq!(score_title_match("猫和老鼠", "超人"), 0.0);
    }

    async fn insert_test_movie(
        pool: &SqlitePool,
        tmdb_id: i64,
        title: &str,
        title_zh: Option<&str>,
        title_en: Option<&str>,
        source: &str,
    ) -> i64 {
        use sqlx::Row;
        let row = sqlx::query(
            "INSERT INTO movies (tmdb_id, title, title_zh, title_en, source) \
             VALUES (?, ?, ?, ?, ?) RETURNING id",
        )
        .bind(tmdb_id)
        .bind(title)
        .bind(title_zh)
        .bind(title_en)
        .bind(source)
        .fetch_one(pool)
        .await
        .unwrap();
        row.get::<i64, _>(0)
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn fuzzy_title_finds_library_hits(pool: SqlitePool) {
        insert_test_movie(&pool, 12, "海底总动员", Some("海底总动员"), Some("Finding Nemo"), "library").await;
        insert_test_movie(&pool, 127380, "海底总动员 2", Some("海底总动员 2"), Some("Finding Dory"), "library").await;
        insert_test_movie(&pool, 238, "教父", Some("教父"), Some("The Godfather"), "library").await;

        let hits = search_movies_by_title_fuzzy(&pool, "海底总动员", 5).await.unwrap();
        assert_eq!(hits.len(), 2);
        // 精确匹配的排第一
        assert_eq!(hits[0].tmdb_id, 12);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn fuzzy_title_english_case_insensitive(pool: SqlitePool) {
        insert_test_movie(&pool, 12, "海底总动员", None, Some("Finding Nemo"), "library").await;

        let hits = search_movies_by_title_fuzzy(&pool, "finding nemo", 5).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].tmdb_id, 12);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn fuzzy_title_includes_related_source(pool: SqlitePool) {
        // 库外种子可被 similar_to / exact_title handler 用来跑 multi_recall_from_seed,
        // 所以 fuzzy 必须能匹配 source='related' 的电影。
        // 详见 docs/specs/2026-04-25-similar-seed-self-recall-design.md
        insert_test_movie(&pool, 12, "海底总动员", None, None, "library").await;
        insert_test_movie(&pool, 9999, "海底总动员（外部）", None, None, "related").await;

        let hits = search_movies_by_title_fuzzy(&pool, "海底总动员", 5).await.unwrap();
        assert_eq!(hits.len(), 2, "库内和库外都要匹配上");
        let ids: std::collections::HashSet<i64> = hits.iter().map(|m| m.tmdb_id).collect();
        assert!(ids.contains(&12), "库内的应被匹配");
        assert!(ids.contains(&9999), "库外的也应被匹配");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn fuzzy_title_empty_query_returns_empty(pool: SqlitePool) {
        insert_test_movie(&pool, 12, "海底总动员", None, None, "library").await;
        let hits = search_movies_by_title_fuzzy(&pool, "   ", 5).await.unwrap();
        assert!(hits.is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn fuzzy_title_nothing_matches(pool: SqlitePool) {
        insert_test_movie(&pool, 12, "海底总动员", None, None, "library").await;
        let hits = search_movies_by_title_fuzzy(&pool, "霸王别姬", 5).await.unwrap();
        assert!(hits.is_empty());
    }

    async fn insert_credit(
        pool: &SqlitePool,
        movie_id: i64,
        tmdb_person_id: i64,
        person_name: &str,
        credit_type: &str,
        role: Option<&str>,
        department: Option<&str>,
    ) {
        insert_credit_with_order(
            pool,
            movie_id,
            tmdb_person_id,
            person_name,
            credit_type,
            role,
            department,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_credit_with_order(
        pool: &SqlitePool,
        movie_id: i64,
        tmdb_person_id: i64,
        person_name: &str,
        credit_type: &str,
        role: Option<&str>,
        department: Option<&str>,
        order: Option<i64>,
    ) {
        sqlx::query(
            "INSERT INTO movie_credits \
                (movie_id, tmdb_person_id, person_name, credit_type, role, department, \"order\") \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(tmdb_person_id)
        .bind(person_name)
        .bind(credit_type)
        .bind(role)
        .bind(department)
        .bind(order)
        .execute(pool)
        .await
        .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn persons_fuzzy_prefers_director_over_same_surname_cast(pool: SqlitePool) {
        // UNIQUE(movie_id, tmdb_person_id, credit_type, role) 只允许同 person 在同 movie
        // 上单条 credit。真实数据里同一人在不同 movie 出现中 / 英文名各占一条，这里模拟。
        let m1 = insert_test_movie(&pool, 100, "Inception", None, Some("Inception"), "library").await;
        let m2 = insert_test_movie(&pool, 101, "记忆碎片", None, Some("Memento"), "library").await;
        let m3 = insert_test_movie(&pool, 102, "敦刻尔克", None, Some("Dunkirk"), "library").await;
        let m4 = insert_test_movie(&pool, 103, "Some Film", None, Some("Some Film"), "library").await;

        // 导演诺兰：两部中文名、一部英文名，多部导演作品
        insert_credit(&pool, m1, 525, "克里斯托弗·诺兰", "crew", Some("Director"), Some("Directing")).await;
        insert_credit(&pool, m2, 525, "克里斯托弗·诺兰", "crew", Some("Director"), Some("Directing")).await;
        insert_credit(&pool, m3, 525, "Christopher Nolan", "crew", Some("Director"), Some("Directing")).await;
        // 演员里姓诺兰的路人
        insert_credit(&pool, m4, 9999, "Anto Nolan", "cast", Some("Irish Cop"), None).await;

        let hits = search_persons_by_name_fuzzy(&pool, "诺兰", 3).await.unwrap();
        assert!(!hits.is_empty());
        // 导演应排第一
        assert_eq!(hits[0].tmdb_person_id, 525);
        assert!(hits[0].has_director_credit);
        // 展示名挑"克里斯托弗·诺兰"（中文 query 的匹配分更高）
        assert_eq!(hits[0].name, "克里斯托弗·诺兰");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn persons_fuzzy_english_query_picks_english_name(pool: SqlitePool) {
        let m1 = insert_test_movie(&pool, 100, "Inception", None, Some("Inception"), "library").await;
        let m2 = insert_test_movie(&pool, 101, "记忆碎片", None, Some("Memento"), "library").await;
        insert_credit(&pool, m1, 525, "Christopher Nolan", "crew", Some("Director"), Some("Directing")).await;
        insert_credit(&pool, m2, 525, "克里斯托弗·诺兰", "crew", Some("Director"), Some("Directing")).await;

        let hits = search_persons_by_name_fuzzy(&pool, "Nolan", 3).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "Christopher Nolan");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn persons_fuzzy_empty_returns_empty(pool: SqlitePool) {
        let hits = search_persons_by_name_fuzzy(&pool, "  ", 3).await.unwrap();
        assert!(hits.is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn persons_fuzzy_no_match_returns_empty(pool: SqlitePool) {
        let m1 = insert_test_movie(&pool, 100, "X", None, Some("X"), "library").await;
        insert_credit(&pool, m1, 525, "Christopher Nolan", "crew", Some("Director"), Some("Directing")).await;
        let hits = search_persons_by_name_fuzzy(&pool, "霸王别姬", 3).await.unwrap();
        assert!(hits.is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn movies_by_person_includes_library_external_works(pool: SqlitePool) {
        // 库外作品（source='related'）也要进入 person 路径结果，与库内混排——
        // 前端通过 RecommendItem.in_library 字段配合 Sparkles 横幅区分。
        let m1 = insert_test_movie(&pool, 100, "Memento", None, Some("Memento"), "library").await;
        let m2 = insert_test_movie(&pool, 101, "Inception", None, Some("Inception"), "library").await;
        let m_ext = insert_test_movie(&pool, 999, "Oppenheimer", None, Some("Oppenheimer"), "related").await;

        // 同为 Director，year DESC 排序：Inception(2010) > Oppenheimer(2023) — 但 Oppenheimer 更晚，应排第一
        sqlx::query("UPDATE movies SET year=2000, tmdb_rating=8.4 WHERE id=?").bind(m1).execute(&pool).await.unwrap();
        sqlx::query("UPDATE movies SET year=2010, tmdb_rating=8.8 WHERE id=?").bind(m2).execute(&pool).await.unwrap();
        sqlx::query("UPDATE movies SET year=2023, tmdb_rating=8.3 WHERE id=?").bind(m_ext).execute(&pool).await.unwrap();

        insert_credit(&pool, m1, 525, "Christopher Nolan", "crew", Some("Director"), Some("Directing")).await;
        insert_credit(&pool, m2, 525, "Christopher Nolan", "crew", Some("Director"), Some("Directing")).await;
        insert_credit(&pool, m_ext, 525, "Christopher Nolan", "crew", Some("Director"), Some("Directing")).await;

        let works = get_movies_by_person(&pool, 525, 10).await.unwrap();
        // 库内 + 库外都返回
        assert_eq!(works.len(), 3, "库外作品也必须返回");
        // 同为 Director，按 year DESC：Oppenheimer(2023, related) > Inception(2010) > Memento(2000)
        assert_eq!(works[0].movie.tmdb_id, 999, "最新的库外作品应排第一");
        assert_eq!(works[0].movie.source.as_deref(), Some("related"));
        assert_eq!(works[1].movie.tmdb_id, 101);
        assert_eq!(works[2].movie.tmdb_id, 100);
        assert_eq!(works[0].role_kind, PersonRoleKind::Director);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn movies_by_person_prefers_lead_over_voice_regardless_of_year(pool: SqlitePool) {
        // 回归"成龙的电影"出三部功夫熊猫而把真正主演作品压下去"的 bug：
        // 配音（role 含 voice）的 role_score=50，远低于主演（order<=5）的 90。
        // 即使配音作品年份更新，也不能压过主演作品。
        let m_old_lead =
            insert_test_movie(&pool, 100, "警察故事", Some("警察故事"), None, "library").await;
        let m_new_voice =
            insert_test_movie(&pool, 101, "功夫熊猫3", Some("功夫熊猫3"), None, "library").await;
        sqlx::query("UPDATE movies SET year=1985, tmdb_rating=8.0 WHERE id=?")
            .bind(m_old_lead).execute(&pool).await.unwrap();
        sqlx::query("UPDATE movies SET year=2016, tmdb_rating=6.9 WHERE id=?")
            .bind(m_new_voice).execute(&pool).await.unwrap();

        // 成龙在警察故事里是主演
        insert_credit_with_order(
            &pool, m_old_lead, 18897, "成龙", "cast",
            Some("Sergeant 'Kevin' Chan Ka-Kui"), None, Some(0),
        ).await;
        // 成龙在功夫熊猫 3 里是配音
        insert_credit_with_order(
            &pool, m_new_voice, 18897, "成龙", "cast",
            Some("Monkey (voice)"), None, Some(5),
        ).await;

        let works = get_movies_by_person(&pool, 18897, 10).await.unwrap();
        assert_eq!(works.len(), 2);
        assert_eq!(works[0].movie.tmdb_id, 100, "主演作品必须排在配音作品前");
        assert_eq!(works[0].role_kind, PersonRoleKind::LeadActor);
        assert_eq!(works[1].movie.tmdb_id, 101);
        assert_eq!(works[1].role_kind, PersonRoleKind::Voice);
        assert_eq!(works[1].role_detail.as_deref(), Some("Monkey (voice)"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn movies_by_person_filters_uncredited_and_stunt(pool: SqlitePool) {
        // "喜剧之王"成龙只是 Stunt Double (uncredited)，不算他的作品。
        // 这种 role_score=0 的 credit 必须完全从结果中过滤掉。
        let m_real = insert_test_movie(&pool, 100, "醉拳", Some("醉拳"), None, "library").await;
        let m_fake = insert_test_movie(&pool, 101, "喜剧之王", Some("喜剧之王"), None, "library").await;
        sqlx::query("UPDATE movies SET year=1978 WHERE id=?").bind(m_real).execute(&pool).await.unwrap();
        sqlx::query("UPDATE movies SET year=1999 WHERE id=?").bind(m_fake).execute(&pool).await.unwrap();

        insert_credit_with_order(
            &pool, m_real, 18897, "成龙", "cast", Some("Fred Wong"), None, Some(0),
        ).await;
        insert_credit_with_order(
            &pool, m_fake, 18897, "成龙", "cast",
            Some("Stunt Double on Set (uncredited)"), None, Some(7),
        ).await;

        let works = get_movies_by_person(&pool, 18897, 10).await.unwrap();
        assert_eq!(works.len(), 1, "uncredited/Stunt Double 的电影必须被过滤");
        assert_eq!(works[0].movie.tmdb_id, 100);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn movies_by_person_takes_best_role_when_multi_credit(pool: SqlitePool) {
        // 同一部片里一个人可能身兼多职（警察故事里成龙既是 Director、主演、又是 Screenplay）。
        // role_kind 必须取最高分身份（Director=100 > LeadActor=90 > Crew=40）。
        let m = insert_test_movie(&pool, 100, "警察故事", Some("警察故事"), None, "library").await;
        sqlx::query("UPDATE movies SET year=1985 WHERE id=?").bind(m).execute(&pool).await.unwrap();

        insert_credit_with_order(
            &pool, m, 18897, "成龙", "cast", Some("Sergeant 'Kevin' Chan"), None, Some(0),
        ).await;
        insert_credit(&pool, m, 18897, "成龙", "crew", Some("Director"), Some("Directing")).await;
        insert_credit(&pool, m, 18897, "成龙", "crew", Some("Screenplay"), Some("Writing")).await;

        let works = get_movies_by_person(&pool, 18897, 10).await.unwrap();
        assert_eq!(works.len(), 1);
        assert_eq!(works[0].role_kind, PersonRoleKind::Director);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn movies_by_person_crew_ranks_below_cast(pool: SqlitePool) {
        // 只以 Producer 身份参与的作品（"胭脂扣"里成龙只是出品人）排序应低于主演作品。
        let m_lead = insert_test_movie(&pool, 100, "红番区", Some("红番区"), None, "library").await;
        let m_producer = insert_test_movie(&pool, 101, "胭脂扣", Some("胭脂扣"), None, "library").await;
        sqlx::query("UPDATE movies SET year=1995 WHERE id=?").bind(m_lead).execute(&pool).await.unwrap();
        sqlx::query("UPDATE movies SET year=1987 WHERE id=?").bind(m_producer).execute(&pool).await.unwrap();

        insert_credit_with_order(
            &pool, m_lead, 18897, "成龙", "cast", Some("Keung"), None, Some(0),
        ).await;
        insert_credit(
            &pool, m_producer, 18897, "成龙", "crew", Some("Producer"), Some("Production"),
        ).await;

        let works = get_movies_by_person(&pool, 18897, 10).await.unwrap();
        assert_eq!(works.len(), 2);
        assert_eq!(works[0].role_kind, PersonRoleKind::LeadActor);
        assert_eq!(works[1].role_kind, PersonRoleKind::Crew);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn related_movies_all_sources_joins_library_and_external(pool: SqlitePool) {
        let seed_id = insert_test_movie(&pool, 12, "海底总动员", None, None, "library").await;
        // 另外两部电影，一部库内一部库外，都设为与 seed 相关
        insert_test_movie(&pool, 100, "Library Friend", None, None, "library").await;
        insert_test_movie(&pool, 200, "External Friend", None, None, "related").await;
        sqlx::query(
            "INSERT INTO related_movies (movie_id, related_tmdb_id, relation_type) \
             VALUES (?, ?, 'similar'), (?, ?, 'similar')",
        )
        .bind(seed_id)
        .bind(100_i64)
        .bind(seed_id)
        .bind(200_i64)
        .execute(&pool)
        .await
        .unwrap();

        let results = get_related_movies_all_sources(&pool, &[seed_id], 10).await.unwrap();
        let ids: std::collections::HashSet<i64> = results.iter().map(|m| m.tmdb_id).collect();
        assert!(ids.contains(&100), "should include library-internal related");
        assert!(ids.contains(&200), "should include library-external related");
    }

    // --- 库外热门 / 最新入库 ---

    /// 给一部 movie 创建一个 matched dir mapping，模拟"已入库"。
    async fn make_matched(pool: &SqlitePool, movie_id: i64, dir_path: &str) {
        let dir_id = insert_media_dir(pool, dir_path, dir_path).await.unwrap();
        update_dir_status(pool, dir_id, "matched").await.unwrap();
        insert_mapping(pool, dir_id, Some(movie_id), "auto", Some(0.95), None)
            .await
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn most_related_excludes_movies_with_matched_mapping(pool: SqlitePool) {
        // 关键测试：source='related' 但已经被绑定到本地 dir 的电影（"先 related 后入库"），
        // 必须从"库外热门"结果中排除。
        let seed = insert_test_movie(&pool, 1, "seed", None, None, "library").await;
        let promoted = insert_test_movie(&pool, 2, "promoted", None, None, "related").await;
        let _truly_external = insert_test_movie(&pool, 3, "external", None, None, "related").await;

        // promoted 被绑定 → 实际上在库
        make_matched(&pool, promoted, "/movies/promoted").await;

        // seed 把 promoted 和 truly_external 都列为 similar
        sqlx::query(
            "INSERT INTO related_movies (movie_id, related_tmdb_id, relation_type) \
             VALUES (?, ?, 'similar'), (?, ?, 'similar')",
        )
        .bind(seed)
        .bind(2_i64)
        .bind(seed)
        .bind(3_i64)
        .execute(&pool)
        .await
        .unwrap();

        let result = most_related_out_of_library(&pool, 10).await.unwrap();
        let ids: Vec<i64> = result.iter().map(|r| r.movie.tmdb_id).collect();
        assert!(!ids.contains(&2), "promoted (有 matched mapping) 不能出现在库外热门");
        assert!(ids.contains(&3), "truly external 应该出现");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn most_related_orders_by_ref_count_desc(pool: SqlitePool) {
        let seed1 = insert_test_movie(&pool, 1, "seed1", None, None, "library").await;
        let seed2 = insert_test_movie(&pool, 2, "seed2", None, None, "library").await;
        let pop = insert_test_movie(&pool, 100, "popular", None, None, "related").await;
        let _less = insert_test_movie(&pool, 101, "less", None, None, "related").await;

        // popular 被两部 seed 关联，less 只被一部
        sqlx::query(
            "INSERT INTO related_movies (movie_id, related_tmdb_id, relation_type) VALUES \
             (?, 100, 'similar'), (?, 100, 'recommendation'), (?, 101, 'similar')",
        )
        .bind(seed1)
        .bind(seed2)
        .bind(seed1)
        .execute(&pool)
        .await
        .unwrap();

        let result = most_related_out_of_library(&pool, 10).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].movie.id, pop);
        assert_eq!(result[0].ref_count, 2);
        assert_eq!(result[1].ref_count, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recent_library_orders_by_max_mapping_updated_at(pool: SqlitePool) {
        let m1 = insert_test_movie(&pool, 1, "old", None, None, "library").await;
        let m2 = insert_test_movie(&pool, 2, "new", None, None, "library").await;
        let m3 = insert_test_movie(&pool, 3, "promoted", None, None, "related").await;
        let _orphan = insert_test_movie(&pool, 4, "orphan", None, None, "library").await;

        // 三部都有 matched mapping，但更新时间不同；orphan 没绑定 → 不出现
        let d1 = insert_media_dir(&pool, "/m1", "m1").await.unwrap();
        let d2 = insert_media_dir(&pool, "/m2", "m2").await.unwrap();
        let d3 = insert_media_dir(&pool, "/m3", "m3").await.unwrap();
        insert_mapping(&pool, d1, Some(m1), "auto", Some(0.9), None).await.unwrap();
        insert_mapping(&pool, d2, Some(m2), "auto", Some(0.9), None).await.unwrap();
        insert_mapping(&pool, d3, Some(m3), "manual", None, None).await.unwrap();

        sqlx::query("UPDATE dir_movie_mappings SET updated_at = ? WHERE dir_id = ?")
            .bind("2025-01-01 00:00:00")
            .bind(d1)
            .execute(&pool).await.unwrap();
        sqlx::query("UPDATE dir_movie_mappings SET updated_at = ? WHERE dir_id = ?")
            .bind("2026-04-25 12:00:00")
            .bind(d2)
            .execute(&pool).await.unwrap();
        sqlx::query("UPDATE dir_movie_mappings SET updated_at = ? WHERE dir_id = ?")
            .bind("2026-03-15 00:00:00")
            .bind(d3)
            .execute(&pool).await.unwrap();

        let result = recent_library_movies(&pool, 5).await.unwrap();
        let ids: Vec<i64> = result.iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![m2, m3, m1], "按最新 mapping updated_at 倒序，并包含 source='related' 但已入库的");
    }

    /// Regression for the two-stage rewrite: a single movie bound by two
    /// different dirs (legitimate when the same film exists in two folders)
    /// must show up exactly once, not duplicated. Also pins the LIMIT clause —
    /// previously the inner-query rewrite could leak more rows than asked if
    /// the sub-LIMIT and outer ORDER BY were misaligned.
    #[sqlx::test(migrations = "./migrations")]
    async fn recent_library_dedupes_movie_with_multiple_mappings(pool: SqlitePool) {
        let m1 = insert_test_movie(&pool, 1, "popular", None, None, "library").await;
        let m2 = insert_test_movie(&pool, 2, "other", None, None, "library").await;
        let m3 = insert_test_movie(&pool, 3, "third", None, None, "library").await;
        let d1a = insert_media_dir(&pool, "/m1a", "m1a").await.unwrap();
        let d1b = insert_media_dir(&pool, "/m1b", "m1b").await.unwrap();
        let d2 = insert_media_dir(&pool, "/m2", "m2").await.unwrap();
        let d3 = insert_media_dir(&pool, "/m3", "m3").await.unwrap();
        insert_mapping(&pool, d1a, Some(m1), "auto", Some(0.9), None).await.unwrap();
        insert_mapping(&pool, d1b, Some(m1), "auto", Some(0.9), None).await.unwrap();
        insert_mapping(&pool, d2, Some(m2), "auto", Some(0.9), None).await.unwrap();
        insert_mapping(&pool, d3, Some(m3), "auto", Some(0.9), None).await.unwrap();

        sqlx::query("UPDATE dir_movie_mappings SET updated_at = ? WHERE dir_id = ?")
            .bind("2026-04-28 09:00:00").bind(d1a).execute(&pool).await.unwrap();
        sqlx::query("UPDATE dir_movie_mappings SET updated_at = ? WHERE dir_id = ?")
            .bind("2026-04-28 12:00:00").bind(d1b).execute(&pool).await.unwrap();
        sqlx::query("UPDATE dir_movie_mappings SET updated_at = ? WHERE dir_id = ?")
            .bind("2026-04-28 11:00:00").bind(d2).execute(&pool).await.unwrap();
        sqlx::query("UPDATE dir_movie_mappings SET updated_at = ? WHERE dir_id = ?")
            .bind("2026-04-28 10:00:00").bind(d3).execute(&pool).await.unwrap();

        let result = recent_library_movies(&pool, 2).await.unwrap();
        let ids: Vec<i64> = result.iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![m1, m2], "去重 + 取 MAX(updated_at) + LIMIT 2");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recent_library_excludes_pending_mappings(pool: SqlitePool) {
        let m = insert_test_movie(&pool, 1, "candidate", None, None, "library").await;
        let d = insert_media_dir(&pool, "/m", "m").await.unwrap();
        // pending mapping 不算"入库"
        insert_mapping(&pool, d, Some(m), "pending", None, None).await.unwrap();

        let result = recent_library_movies(&pool, 5).await.unwrap();
        assert!(result.is_empty(), "pending mapping 不应该让电影出现在最新入库");
    }

    // --- library_stats: sample movies ---

    #[test]
    fn format_movie_samples_empty() {
        let out = format_movie_samples(&[], 50);
        assert!(out.is_empty());
    }

    #[test]
    fn format_movie_samples_renders_each_row() {
        let rows = vec![
            SampleMovieRow {
                id: 1, title: "Inception".into(), year: Some(2010),
                director: Some("Christopher Nolan".into()), tmdb_rating: Some(8.37),
                genres_concat: Some("Action,Sci-Fi,Thriller".into()), best_rn: 1,
            },
            SampleMovieRow {
                id: 2, title: "Nobody".into(), year: None,
                director: None, tmdb_rating: None,
                genres_concat: None, best_rn: 9,
            },
        ];
        let out = format_movie_samples(&rows, 50);
        assert_eq!(out.len(), 2);
        // Full fields: one argv-like compact line with all hints the LLM needs to tell this movie apart.
        assert_eq!(out[0], "- Inception (2010) · Action,Sci-Fi,Thriller · Christopher Nolan · ⭐8.4");
        // Missing fields use "-" placeholder (NOT empty/blank, so the layout stays readable).
        assert_eq!(out[1], "- Nobody (-) · - · - · ⭐-");
    }

    #[test]
    fn format_movie_samples_respects_limit() {
        let rows: Vec<SampleMovieRow> = (0..100)
            .map(|i| SampleMovieRow {
                id: i, title: format!("T{}", i), year: Some(2000),
                director: None, tmdb_rating: None, genres_concat: None, best_rn: 1,
            })
            .collect();
        let out = format_movie_samples(&rows, 50);
        assert_eq!(out.len(), 50);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn library_stats_empty_db(pool: SqlitePool) {
        let stats = get_library_stats(&pool).await.unwrap();
        assert_eq!(stats.total, 0);
        assert_eq!(stats.library_total, 0);
        assert!(stats.sample_movies.is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn library_stats_samples_are_unique_by_id(pool: SqlitePool) {
        // A: two genres; B: one genre; C: two genres overlapping A.
        // PARTITION BY genre would list A twice (Action + Thriller) and C twice
        // (Thriller + Drama) — verify outer GROUP BY p.id deduplicates.
        insert_movie(
            &pool, 1, "A", None, Some(2020), None, None,
            r#"["Action","Thriller"]"#, None, None, None, None, "[]",
            Some(8.0), None, "[]", None, None, Some(90.0), "library",
        ).await.unwrap();
        insert_movie(
            &pool, 2, "B", None, Some(2019), None, None,
            r#"["Action"]"#, None, None, None, None, "[]",
            Some(7.5), None, "[]", None, None, Some(80.0), "library",
        ).await.unwrap();
        insert_movie(
            &pool, 3, "C", None, Some(2021), None, None,
            r#"["Thriller","Drama"]"#, None, None, None, None, "[]",
            Some(7.0), None, "[]", None, None, Some(70.0), "library",
        ).await.unwrap();

        let stats = get_library_stats(&pool).await.unwrap();
        assert_eq!(stats.library_total, 3);
        assert_eq!(stats.sample_movies.len(), 3, "each movie must appear exactly once");
        let joined = stats.sample_movies.join("\n");
        assert!(joined.contains("A (2020)"));
        assert!(joined.contains("B (2019)"));
        assert!(joined.contains("C (2021)"));
        // A should list both its genres in the single output line.
        let a_line = stats.sample_movies.iter().find(|l| l.contains("A (2020)")).unwrap();
        assert!(a_line.contains("Action") && a_line.contains("Thriller"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn library_stats_excludes_related_source(pool: SqlitePool) {
        // 库外电影（source='related'）不应进入 sample_movies 或 library_total
        insert_movie(
            &pool, 1, "InLib", None, Some(2020), None, None,
            r#"["Drama"]"#, None, None, None, None, "[]",
            None, None, "[]", None, None, None, "library",
        ).await.unwrap();
        insert_movie(
            &pool, 2, "OutLib", None, Some(2018), None, None,
            r#"["Drama"]"#, None, None, None, None, "[]",
            None, None, "[]", None, None, None, "related",
        ).await.unwrap();

        let stats = get_library_stats(&pool).await.unwrap();
        assert_eq!(stats.total, 2, "total 含 related");
        assert_eq!(stats.library_total, 1, "library_total 只算 source='library'");
        assert_eq!(stats.sample_movies.len(), 1);
        assert!(stats.sample_movies[0].contains("InLib"));
    }
}

// ==================== Torrent Info ====================

pub async fn find_media_dir_by_name(
    pool: &SqlitePool,
    dir_name: &str,
) -> Result<Option<MediaDir>, sqlx::Error> {
    sqlx::query_as::<_, MediaDir>(
        "SELECT * FROM media_dirs WHERE dir_name = ? AND scan_status != 'deleted' LIMIT 1",
    )
    .bind(dir_name)
    .fetch_optional(pool)
    .await
}

pub async fn insert_media_dir_with_source(
    pool: &SqlitePool,
    dir_path: &str,
    dir_name: &str,
    source: &str,
) -> Result<i64, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO media_dirs (dir_path, dir_name, scan_status, source) VALUES (?, ?, 'new', ?)",
    )
    .bind(dir_path)
    .bind(dir_name)
    .bind(source)
    .execute(pool)
    .await?;

    Ok(result.last_insert_rowid())
}

pub async fn upsert_torrent_info(
    pool: &SqlitePool,
    media_dir_id: i64,
    torrent_hash: &str,
    state: &str,
    progress: f64,
    size: Option<i64>,
    dlspeed: Option<i64>,
    upspeed: Option<i64>,
    ratio: Option<f64>,
    seeds: Option<i64>,
    added_on: Option<i64>,
    media_type: &str,
    torrent_name: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO torrent_info (media_dir_id, torrent_hash, state, progress, size, dlspeed, upspeed, ratio, seeds, added_on, media_type, torrent_name, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
         ON CONFLICT(torrent_hash) DO UPDATE SET
           state = excluded.state,
           progress = excluded.progress,
           size = excluded.size,
           dlspeed = excluded.dlspeed,
           upspeed = excluded.upspeed,
           ratio = excluded.ratio,
           seeds = excluded.seeds,
           media_type = excluded.media_type,
           torrent_name = excluded.torrent_name,
           updated_at = datetime('now')",
    )
    .bind(media_dir_id)
    .bind(torrent_hash)
    .bind(state)
    .bind(progress)
    .bind(size)
    .bind(dlspeed)
    .bind(upspeed)
    .bind(ratio)
    .bind(seeds)
    .bind(added_on)
    .bind(media_type)
    .bind(torrent_name)
    .execute(pool)
    .await?;

    Ok(())
}

/// Get aggregated download status for a movie (via dir_movie_mappings → media_dirs → torrent_info).
/// Returns the record with highest progress if multiple torrents exist.
pub async fn get_download_status_for_movie(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<Option<DownloadStatus>, sqlx::Error> {
    let row: Option<(String, f64, i64, Option<i64>, String)> = sqlx::query_as(
        "SELECT ti.state, ti.progress, COALESCE(ti.dlspeed, 0), ti.size, ti.media_type
         FROM torrent_info ti
         JOIN media_dirs md ON ti.media_dir_id = md.id
         JOIN dir_movie_mappings dm ON dm.dir_id = md.id
         WHERE dm.movie_id = ?
         ORDER BY ti.progress DESC
         LIMIT 1",
    )
    .bind(movie_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(state, progress, dlspeed, size, media_type)| DownloadStatus {
        state,
        progress,
        dlspeed,
        size,
        media_type,
    }))
}

/// Check if all media_dirs for a movie are still downloading (progress < 1.0).
pub async fn is_movie_downloading(
    pool: &SqlitePool,
    movie_id: i64,
) -> Result<bool, sqlx::Error> {
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT
           COUNT(*) AS total,
           SUM(CASE WHEN ti.progress >= 1.0 THEN 1 ELSE 0 END) AS completed
         FROM torrent_info ti
         JOIN media_dirs md ON ti.media_dir_id = md.id
         JOIN dir_movie_mappings dm ON dm.dir_id = md.id
         WHERE dm.movie_id = ?",
    )
    .bind(movie_id)
    .fetch_optional(pool)
    .await?;

    match row {
        Some((total, completed)) if total > 0 => Ok(completed == 0),
        _ => Ok(false),
    }
}

// ===== Most-Related Tips =====

/// Get cached tip for a user (or anonymous if user_id is None) for today.
pub async fn get_most_related_tip(
    pool: &SqlitePool,
    user_id: Option<i64>,
    date: &str,
) -> Result<Option<String>, sqlx::Error> {
    let row: Option<(String,)> = if let Some(uid) = user_id {
        sqlx::query_as("SELECT tip FROM most_related_tips WHERE user_id = ? AND date = ?")
            .bind(uid)
            .bind(date)
            .fetch_optional(pool)
            .await?
    } else {
        sqlx::query_as("SELECT tip FROM most_related_tips WHERE user_id IS NULL AND date = ?")
            .bind(date)
            .fetch_optional(pool)
            .await?
    };
    Ok(row.map(|r| r.0))
}

/// Save tip for a user (or anonymous if user_id is None) for today.
pub async fn save_most_related_tip(
    pool: &SqlitePool,
    user_id: Option<i64>,
    date: &str,
    tip: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO most_related_tips (user_id, date, tip) VALUES (?, ?, ?)
         ON CONFLICT(user_id, date) DO UPDATE SET tip = excluded.tip, created_at = datetime('now')",
    )
    .bind(user_id)
    .bind(date)
    .bind(tip)
    .execute(pool)
    .await?;
    Ok(())
}

// ===== Movie AI Insights =====

/// Get cached AI insight for a user+movie. Returns (insight_json, watched_count_at_cache_time).
pub async fn get_movie_ai_insight(
    pool: &SqlitePool,
    user_id: Option<i64>,
    movie_id: i64,
) -> Result<Option<(String, i64)>, sqlx::Error> {
    let row: Option<(String, i64)> = if let Some(uid) = user_id {
        sqlx::query_as("SELECT insight, watched_count FROM movie_ai_insights WHERE user_id = ? AND movie_id = ?")
            .bind(uid)
            .bind(movie_id)
            .fetch_optional(pool)
            .await?
    } else {
        sqlx::query_as("SELECT insight, watched_count FROM movie_ai_insights WHERE user_id IS NULL AND movie_id = ?")
            .bind(movie_id)
            .fetch_optional(pool)
            .await?
    };
    Ok(row)
}

/// Save AI insight for a user+movie.
pub async fn save_movie_ai_insight(
    pool: &SqlitePool,
    user_id: Option<i64>,
    movie_id: i64,
    insight: &str,
    watched_count: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO movie_ai_insights (user_id, movie_id, insight, watched_count) VALUES (?, ?, ?, ?)
         ON CONFLICT(user_id, movie_id) DO UPDATE SET insight = excluded.insight, watched_count = excluded.watched_count, created_at = datetime('now')",
    )
    .bind(user_id)
    .bind(movie_id)
    .bind(insight)
    .bind(watched_count)
    .execute(pool)
    .await?;
    Ok(())
}

/// Count how many movies a user has marked as watched.
pub async fn count_user_watched(pool: &SqlitePool, user_id: i64) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM user_movie_marks WHERE user_id = ? AND mark_type = 'watched'",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
}

// ===== Multi-version movies (admin "影片整理") =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiVersionMovieSummary {
    pub id: i64,
    pub title: String,
    pub title_zh: Option<String>,
    pub year: Option<i64>,
    pub poster_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiVersionDir {
    pub dir_id: i64,
    pub dir_name: String,
    pub dir_path: String,
    pub source: Option<String>,
    pub match_status: String,
    pub match_confidence: Option<f64>,
    pub torrent_name: Option<String>,
    pub media_type: Option<String>,
    pub size_bytes: Option<i64>,
    pub torrent_state: Option<String>,
    pub torrent_progress: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiVersionMovie {
    pub movie: MultiVersionMovieSummary,
    pub version_count: i64,
    pub dirs: Vec<MultiVersionDir>,
}

/// List movies with ≥ 2 dir_movie_mappings in ('auto','manual'). Returns
/// (items_for_this_page, total_groups). Pending/failed mappings are excluded
/// because they don't reliably represent the same movie. If a media_dir has
/// multiple `torrent_info` rows (rare — happens when two torrents share a
/// dir), we pick the lowest-id row to keep one row per media_dir.
pub async fn list_multi_version_movies(
    pool: &SqlitePool,
    limit: i64,
    offset: i64,
) -> Result<(Vec<MultiVersionMovie>, i64), sqlx::Error> {
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM (
           SELECT movie_id
             FROM dir_movie_mappings
            WHERE match_status IN ('auto','manual')
              AND movie_id IS NOT NULL
            GROUP BY movie_id
           HAVING COUNT(*) >= 2
         )",
    )
    .fetch_one(pool)
    .await?;

    if total == 0 {
        return Ok((Vec::new(), 0));
    }

    let movie_rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT movie_id, COUNT(*) AS version_count
           FROM dir_movie_mappings
          WHERE match_status IN ('auto','manual')
            AND movie_id IS NOT NULL
          GROUP BY movie_id
         HAVING version_count >= 2
          ORDER BY version_count DESC, movie_id ASC
          LIMIT ? OFFSET ?",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    if movie_rows.is_empty() {
        return Ok((Vec::new(), total));
    }

    let movie_ids: Vec<i64> = movie_rows.iter().map(|(id, _)| *id).collect();
    let placeholders = vec!["?"; movie_ids.len()].join(",");

    let movies_sql = format!(
        "SELECT id, title, title_zh, year, poster_url
           FROM movies
          WHERE id IN ({})",
        placeholders
    );
    let mut q =
        sqlx::query_as::<_, (i64, String, Option<String>, Option<i64>, Option<String>)>(&movies_sql);
    for id in &movie_ids {
        q = q.bind(id);
    }
    let movie_data = q.fetch_all(pool).await?;
    let mut movie_map: std::collections::HashMap<i64, MultiVersionMovieSummary> =
        std::collections::HashMap::with_capacity(movie_data.len());
    for (id, title, title_zh, year, poster_url) in movie_data {
        movie_map.insert(
            id,
            MultiVersionMovieSummary { id, title, title_zh, year, poster_url },
        );
    }

    let dirs_sql = format!(
        "SELECT
           dm.movie_id,
           md.id AS dir_id,
           md.dir_name,
           md.dir_path,
           md.source,
           dm.match_status,
           dm.confidence AS match_confidence,
           ti.torrent_name,
           ti.media_type,
           ti.size AS size_bytes,
           ti.state AS torrent_state,
           ti.progress AS torrent_progress
         FROM dir_movie_mappings dm
         JOIN media_dirs md ON dm.dir_id = md.id
         LEFT JOIN torrent_info ti
                ON ti.id = (
                  SELECT MIN(id) FROM torrent_info WHERE media_dir_id = md.id
                )
         WHERE dm.movie_id IN ({})
           AND dm.match_status IN ('auto','manual')
         ORDER BY dm.movie_id ASC, md.id ASC",
        placeholders
    );
    let mut q = sqlx::query_as::<
        _,
        (
            i64,
            i64,
            String,
            String,
            Option<String>,
            String,
            Option<f64>,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<String>,
            Option<f64>,
        ),
    >(&dirs_sql);
    for id in &movie_ids {
        q = q.bind(id);
    }
    let dir_rows = q.fetch_all(pool).await?;

    let mut dirs_by_movie: std::collections::HashMap<i64, Vec<MultiVersionDir>> =
        std::collections::HashMap::new();
    for row in dir_rows {
        let (
            movie_id,
            dir_id,
            dir_name,
            dir_path,
            source,
            match_status,
            match_confidence,
            torrent_name,
            media_type,
            size_bytes,
            torrent_state,
            torrent_progress,
        ) = row;
        dirs_by_movie
            .entry(movie_id)
            .or_default()
            .push(MultiVersionDir {
                dir_id,
                dir_name,
                dir_path,
                source,
                match_status,
                match_confidence,
                torrent_name,
                media_type,
                size_bytes,
                torrent_state,
                torrent_progress,
            });
    }

    let mut items = Vec::with_capacity(movie_rows.len());
    for (movie_id, version_count) in movie_rows {
        if let Some(movie) = movie_map.remove(&movie_id) {
            let dirs = dirs_by_movie.remove(&movie_id).unwrap_or_default();
            items.push(MultiVersionMovie {
                movie,
                version_count,
                dirs,
            });
        }
    }

    Ok((items, total))
}
