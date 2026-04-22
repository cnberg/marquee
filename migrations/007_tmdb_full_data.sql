-- ===========================================
-- movies 表扩展列
-- ===========================================

-- 双语字段
ALTER TABLE movies ADD COLUMN title_zh TEXT;
ALTER TABLE movies ADD COLUMN title_en TEXT;
ALTER TABLE movies ADD COLUMN overview_zh TEXT;
ALTER TABLE movies ADD COLUMN overview_en TEXT;
ALTER TABLE movies ADD COLUMN tagline_zh TEXT;
ALTER TABLE movies ADD COLUMN tagline_en TEXT;
ALTER TABLE movies ADD COLUMN genres_zh TEXT;
ALTER TABLE movies ADD COLUMN genres_en TEXT;

-- 新标量字段
ALTER TABLE movies ADD COLUMN imdb_id TEXT;
ALTER TABLE movies ADD COLUMN backdrop_path TEXT;
ALTER TABLE movies ADD COLUMN homepage TEXT;
ALTER TABLE movies ADD COLUMN status TEXT;
ALTER TABLE movies ADD COLUMN collection TEXT;
ALTER TABLE movies ADD COLUMN production_companies TEXT;
ALTER TABLE movies ADD COLUMN spoken_languages TEXT;
ALTER TABLE movies ADD COLUMN origin_country TEXT;

-- 电影来源标记
ALTER TABLE movies ADD COLUMN source TEXT NOT NULL DEFAULT 'library';

-- ===========================================
-- 子表
-- ===========================================

CREATE TABLE IF NOT EXISTS movie_credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    tmdb_person_id INTEGER NOT NULL,
    person_name TEXT NOT NULL,
    credit_type TEXT NOT NULL,
    role TEXT,
    department TEXT,
    "order" INTEGER,
    profile_path TEXT,
    UNIQUE(movie_id, tmdb_person_id, credit_type, role)
);

CREATE TABLE IF NOT EXISTS movie_images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    image_type TEXT NOT NULL,
    file_path TEXT NOT NULL,
    iso_639_1 TEXT,
    width INTEGER,
    height INTEGER,
    vote_average REAL,
    UNIQUE(movie_id, image_type, file_path)
);

CREATE TABLE IF NOT EXISTS movie_videos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    video_key TEXT NOT NULL,
    site TEXT,
    video_type TEXT,
    name TEXT,
    iso_639_1 TEXT,
    official INTEGER DEFAULT 0,
    published_at TEXT,
    UNIQUE(movie_id, video_key)
);

CREATE TABLE IF NOT EXISTS movie_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    tmdb_review_id TEXT UNIQUE NOT NULL,
    author TEXT,
    author_username TEXT,
    content TEXT,
    rating REAL,
    created_at TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS movie_release_dates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    iso_3166_1 TEXT NOT NULL,
    release_date TEXT,
    certification TEXT,
    release_type INTEGER,
    note TEXT,
    UNIQUE(movie_id, iso_3166_1, release_type)
);

CREATE TABLE IF NOT EXISTS movie_watch_providers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    iso_3166_1 TEXT NOT NULL,
    provider_id INTEGER NOT NULL,
    provider_name TEXT,
    logo_path TEXT,
    provider_type TEXT NOT NULL,
    display_priority INTEGER,
    UNIQUE(movie_id, iso_3166_1, provider_id, provider_type)
);

CREATE TABLE IF NOT EXISTS movie_external_ids (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL UNIQUE REFERENCES movies(id) ON DELETE CASCADE,
    imdb_id TEXT,
    facebook_id TEXT,
    instagram_id TEXT,
    twitter_id TEXT,
    wikidata_id TEXT
);

CREATE TABLE IF NOT EXISTS movie_alternative_titles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    iso_3166_1 TEXT,
    title TEXT NOT NULL,
    title_type TEXT,
    UNIQUE(movie_id, iso_3166_1, title)
);

CREATE TABLE IF NOT EXISTS movie_translations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    iso_639_1 TEXT NOT NULL,
    iso_3166_1 TEXT,
    language_name TEXT,
    title TEXT,
    overview TEXT,
    tagline TEXT,
    homepage TEXT,
    runtime INTEGER,
    UNIQUE(movie_id, iso_639_1, iso_3166_1)
);

CREATE TABLE IF NOT EXISTS related_movies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    related_tmdb_id INTEGER NOT NULL,
    relation_type TEXT NOT NULL,
    UNIQUE(movie_id, related_tmdb_id, relation_type)
);

CREATE TABLE IF NOT EXISTS movie_lists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movie_id INTEGER NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    tmdb_list_id INTEGER NOT NULL,
    list_name TEXT,
    description TEXT,
    item_count INTEGER,
    iso_639_1 TEXT,
    UNIQUE(movie_id, tmdb_list_id)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_movie_credits_movie ON movie_credits(movie_id);
CREATE INDEX IF NOT EXISTS idx_movie_credits_person ON movie_credits(tmdb_person_id);
CREATE INDEX IF NOT EXISTS idx_movie_images_movie ON movie_images(movie_id);
CREATE INDEX IF NOT EXISTS idx_movie_videos_movie ON movie_videos(movie_id);
CREATE INDEX IF NOT EXISTS idx_movie_reviews_movie ON movie_reviews(movie_id);
CREATE INDEX IF NOT EXISTS idx_movie_release_dates_movie ON movie_release_dates(movie_id);
CREATE INDEX IF NOT EXISTS idx_movie_watch_providers_movie ON movie_watch_providers(movie_id);
CREATE INDEX IF NOT EXISTS idx_movie_alternative_titles_movie ON movie_alternative_titles(movie_id);
CREATE INDEX IF NOT EXISTS idx_movie_translations_movie ON movie_translations(movie_id);
CREATE INDEX IF NOT EXISTS idx_related_movies_movie ON related_movies(movie_id);
CREATE INDEX IF NOT EXISTS idx_related_movies_related ON related_movies(related_tmdb_id);
CREATE INDEX IF NOT EXISTS idx_movie_lists_movie ON movie_lists(movie_id);
CREATE INDEX IF NOT EXISTS idx_movies_source ON movies(source);
CREATE INDEX IF NOT EXISTS idx_movies_imdb ON movies(imdb_id);
