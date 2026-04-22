CREATE TABLE IF NOT EXISTS media_dirs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    dir_path    TEXT UNIQUE NOT NULL,
    dir_name    TEXT NOT NULL,
    scan_status TEXT NOT NULL DEFAULT 'new',
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS movies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tmdb_id         INTEGER UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    original_title  TEXT,
    year            INTEGER,
    overview        TEXT,
    poster_url      TEXT,
    genres          TEXT DEFAULT '[]',
    country         TEXT,
    language        TEXT,
    runtime         INTEGER,
    director        TEXT,
    cast            TEXT DEFAULT '[]',
    tmdb_rating     REAL,
    tmdb_votes      INTEGER,
    keywords        TEXT DEFAULT '[]',
    llm_tags        TEXT DEFAULT '[]',
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dir_movie_mappings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dir_id        INTEGER NOT NULL REFERENCES media_dirs(id),
    movie_id      INTEGER REFERENCES movies(id),
    match_status  TEXT NOT NULL DEFAULT 'pending',
    confidence    REAL,
    candidates    TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tasks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_type   TEXT NOT NULL,
    payload     TEXT,
    status      TEXT NOT NULL DEFAULT 'pending',
    retries     INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    error_msg   TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_media_dirs_scan_status ON media_dirs(scan_status);
CREATE INDEX idx_dir_movie_mappings_status ON dir_movie_mappings(match_status);
CREATE INDEX idx_tasks_status ON tasks(status, task_type);
