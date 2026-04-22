CREATE TABLE IF NOT EXISTS prompt_overrides (
    name TEXT NOT NULL,
    locale TEXT NOT NULL,
    content TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (name, locale)
);
