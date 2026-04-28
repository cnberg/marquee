-- Track the origin of each movie's Chinese overview so we can distinguish
-- TMDB-provided text from LLM-translated text. The translation worker scans
-- rows where overview is missing/short AND overview_zh_source IS NULL (i.e.
-- never tried). Failed LLM attempts get 'failed' to suppress retries.
-- TMDB refetch path compares against this column to avoid overwriting a good
-- LLM translation with a shorter TMDB stub.
--
-- Values:
--   NULL     — never attempted
--   'tmdb'   — sourced from TMDB zh-CN response
--   'llm'    — translated by overview_translation_worker
--   'failed' — LLM tried and failed; not retried automatically
ALTER TABLE movies ADD COLUMN overview_zh_source TEXT;

-- Partial index optimised for the worker's claim query: only rows that
-- the worker would consider candidates ever live in this index.
CREATE INDEX idx_movies_overview_zh_pending
    ON movies(id)
    WHERE overview_zh_source IS NULL
      AND (overview IS NULL OR length(overview) < 30)
      AND length(overview_en) > 50;
