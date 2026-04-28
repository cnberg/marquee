ALTER TABLE search_history ADD COLUMN share_token TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_search_history_share_token
    ON search_history(share_token) WHERE share_token IS NOT NULL;
