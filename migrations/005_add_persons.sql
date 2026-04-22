CREATE TABLE IF NOT EXISTS persons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tmdb_person_id INTEGER UNIQUE NOT NULL,
    name TEXT NOT NULL,
    also_known_as TEXT,
    biography TEXT,
    profile_path TEXT,
    birthday TEXT,
    deathday TEXT,
    place_of_birth TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

ALTER TABLE movies ADD COLUMN director_info TEXT;
