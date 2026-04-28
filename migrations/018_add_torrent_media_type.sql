ALTER TABLE torrent_info ADD COLUMN media_type TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE torrent_info ADD COLUMN torrent_name TEXT NOT NULL DEFAULT '';
