CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE user_movie_marks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id),
    movie_id INTEGER NOT NULL REFERENCES movies(id),
    mark_type TEXT NOT NULL CHECK(mark_type IN ('want', 'watched', 'favorite')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(user_id, movie_id, mark_type)
);

CREATE INDEX idx_user_movie_marks_user ON user_movie_marks(user_id);
CREATE INDEX idx_user_movie_marks_movie ON user_movie_marks(movie_id);
