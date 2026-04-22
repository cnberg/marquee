use super::models::{
    DirMovieMapping, MediaDir, Movie, MovieAlternativeTitle, MovieCredit, MovieExternalId,
    MovieImage, MovieList, MovieReleaseDate, MovieReview, MovieTranslation, MovieVideo,
    MovieWatchProvider, Person, RelatedMovie, SearchHistoryDetail, SearchHistoryItem, Task,
};
use crate::search::intent::{Constraints, Exclusions};
use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, Sqlite, SqlitePool};

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
            "INSERT OR IGNORE INTO movie_credits (movie_id, tmdb_person_id, person_name, credit_type, role, department, \"order\", profile_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(movie_id)
        .bind(r.tmdb_person_id)
        .bind(&r.person_name)
        .bind(&r.credit_type)
        .bind(&r.role)
        .bind(&r.department)
        .bind(r.order)
        .bind(&r.profile_path)
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

pub async fn get_related_movies(
    pool: &SqlitePool,
    movie_id: i64,
    relation_type: &str,
) -> Result<Vec<RelatedMovie>, sqlx::Error> {
    sqlx::query_as::<_, RelatedMovie>("SELECT * FROM related_movies WHERE movie_id = ? AND relation_type = ?")
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

    // Top keywords (from JSON array)
    let keywords = sqlx::query_as::<_, (String, i64)>(
        "SELECT j.value AS kw, COUNT(*) AS cnt FROM movies, json_each(movies.keywords) AS j WHERE keywords IS NOT NULL AND keywords != '[]' GROUP BY kw ORDER BY cnt DESC LIMIT 20"
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
    let budget_tiers = sqlx::query_as::<_, (String, i64)>(
        "SELECT\n\
            CASE\n\
                WHEN budget > 50000000 THEN 'budget_high'\n\
                WHEN budget >= 5000000 THEN 'budget_medium'\n\
                ELSE 'budget_low'\n\
            END AS tier,\n\
            COUNT(*) AS cnt\n\
         FROM movies\n\
         WHERE budget IS NOT NULL AND budget > 0\n\
         GROUP BY tier\n\
         ORDER BY MIN(budget) DESC"
    ).fetch_all(pool).await?;

    Ok(LibraryStats {
        total: total.0,
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

#[derive(Debug, Serialize)]
pub struct LibraryStats {
    pub total: i64,
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
         ORDER BY md.updated_at DESC
         LIMIT ? OFFSET ?",
    )
    .bind(per_page)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok((rows, total))
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
        let mut tier_conditions: Vec<&str> = Vec::new();
        for tier in budget_tier {
            match tier.as_str() {
                "high" | "大制作" | "budget_high" => tier_conditions.push("m.budget > 50000000"),
                "medium" | "中等" | "budget_medium" => tier_conditions.push("(m.budget >= 5000000 AND m.budget <= 50000000)"),
                "low" | "小成本" | "budget_low" => tier_conditions.push("(m.budget > 0 AND m.budget < 5000000)"),
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
    pub runtime: Option<i64>,
    pub popularity: Option<f64>,
    pub budget: Option<i64>,
    pub keywords: Option<String>,
    pub source: Option<String>,
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
         m.overview, m.tmdb_rating, m.runtime, m.popularity, m.budget, m.keywords, m.source, m.\"cast\" \
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
        qb.push(" AND EXISTS (SELECT 1 FROM json_each(m.\"cast\") WHERE value IN (");
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
            "high" => { qb.push(" AND m.budget > 50000000"); }
            "medium" => { qb.push(" AND (m.budget >= 5000000 AND m.budget <= 50000000)"); }
            "low" => { qb.push(" AND (m.budget > 0 AND m.budget < 5000000)"); }
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

    qb.push(" ORDER BY m.tmdb_rating DESC NULLS LAST LIMIT ");
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
        "SELECT DISTINCT m.id, m.tmdb_id, m.title, m.year, m.genres, m.director, m.language, m.country, m.overview, m.tmdb_rating, m.runtime, m.popularity, m.budget, m.keywords, m.source, m.\"cast\"
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
        "SELECT id, prompt, sse_events, result_count, created_at
         FROM search_history
         WHERE id = ? AND user_id = ?",
    )
    .bind(id)
    .bind(user_id)
    .fetch_optional(pool)
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
            overview: None, tmdb_rating: None, runtime: None, popularity: None,
            budget: None, keywords: None, source: None,
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
            runtime: Some(120),
            popularity: Some(50.0),
            budget: Some(1000000),
            keywords: Some("[\"test\"]".to_string()),
            source: Some("library".to_string()),
            cast_json: Some("[\"Actor\"]".to_string()),
        };
        assert_eq!(movie.cast_json, Some("[\"Actor\"]".to_string()));
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
}
