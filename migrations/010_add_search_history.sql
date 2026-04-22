CREATE TABLE search_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    prompt       TEXT NOT NULL,
    sse_events   TEXT NOT NULL,
    result_count INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_search_history_user_created
    ON search_history(user_id, created_at DESC);
