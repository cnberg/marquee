-- Add English bilingual storage for fields that were only stored in zh-CN.
-- Existing rows keep NULL in these columns; the frontend falls back to the zh
-- version, so there is no visible regression. Re-scanning a movie repopulates
-- the _en columns from TMDB en-US responses.

ALTER TABLE movies ADD COLUMN director_info_en TEXT;
ALTER TABLE movies ADD COLUMN cast_en TEXT;
ALTER TABLE movies ADD COLUMN keywords_en TEXT;
ALTER TABLE movies ADD COLUMN collection_en TEXT;
ALTER TABLE movies ADD COLUMN production_companies_en TEXT;

ALTER TABLE movie_credits ADD COLUMN person_name_en TEXT;
ALTER TABLE movie_credits ADD COLUMN role_en TEXT;
