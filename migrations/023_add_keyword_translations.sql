-- Translation dictionary for TMDB keywords.
--
-- TMDB only returns English keywords; previously stored as-is in `movies.keywords`.
-- Embedded into BGE-Small-ZH (Chinese-only) embedding text, English tokens
-- contributed weak signal. This table maps each unique English keyword to a
-- Chinese translation, populated incrementally by translation_worker.
--
-- Status lifecycle: pending → done (LLM translated) | failed (LLM rejected).
-- failed rows are retried only on explicit reset.
CREATE TABLE keyword_translations (
    en          TEXT PRIMARY KEY NOT NULL,
    zh          TEXT,
    status      TEXT NOT NULL DEFAULT 'pending',
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_keyword_translations_status_pending
    ON keyword_translations(status)
    WHERE status = 'pending';

-- Seed dictionary with every distinct keyword currently in the library so the
-- worker has something to chew on at first start. SQLite's json_each unrolls
-- the JSON arrays in movies.keywords into individual rows.
INSERT OR IGNORE INTO keyword_translations (en)
SELECT DISTINCT json_each.value
FROM movies, json_each(movies.keywords)
WHERE movies.keywords IS NOT NULL
  AND movies.keywords != '[]'
  AND length(json_each.value) > 0;
