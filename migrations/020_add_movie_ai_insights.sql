CREATE TABLE IF NOT EXISTS movie_ai_insights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    movie_id INTEGER NOT NULL,
    insight TEXT NOT NULL,
    watched_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(user_id, movie_id)
);
