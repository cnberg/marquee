-- Version marker for tmdb_fetch pipeline output. NULL means "never fetched
-- by the current pipeline"; a background worker re-enqueues tmdb_fetch tasks
-- whenever movies.tmdb_fetch_version < CURRENT_TMDB_FETCH_VERSION in code.
ALTER TABLE movies ADD COLUMN tmdb_fetch_version INTEGER;

CREATE INDEX idx_movies_fetch_version
    ON movies(source, tmdb_fetch_version);
