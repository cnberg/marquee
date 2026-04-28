CREATE TABLE IF NOT EXISTS torrent_info (
    id INTEGER PRIMARY KEY,
    media_dir_id INTEGER NOT NULL REFERENCES media_dirs(id),
    torrent_hash TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'unknown',
    progress REAL NOT NULL DEFAULT 0.0,
    size INTEGER,
    dlspeed INTEGER DEFAULT 0,
    upspeed INTEGER DEFAULT 0,
    ratio REAL,
    seeds INTEGER,
    added_on INTEGER,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_torrent_info_media_dir ON torrent_info(media_dir_id);
