-- dir_movie_mappings(movie_id) is queried by is_movie_downloading and other
-- per-movie lookups via the dm.movie_id = ? predicate. Without an index
-- SQLite SCANs the whole mappings table on every call (~2k rows × 5-10
-- calls per homepage hit). Add the index — small footprint, big win on
-- per-row N+1 lookups.
CREATE INDEX IF NOT EXISTS idx_dir_movie_mappings_movie_id
  ON dir_movie_mappings(movie_id);
