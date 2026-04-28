CREATE TABLE IF NOT EXISTS most_related_tips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    tip TEXT NOT NULL,
    date TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(user_id, date)
);
