CREATE TABLE IF NOT EXISTS daily_picks (
    date TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
