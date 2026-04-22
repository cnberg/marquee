CREATE TABLE IF NOT EXISTS prompt_overrides (
    name TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
