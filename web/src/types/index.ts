export interface Movie {
  id: number
  tmdb_id: number
  title: string
  original_title?: string
  year?: number
  overview?: string
  poster_url?: string
  genres?: string  // JSON string
  country?: string
  language?: string
  runtime?: number
  director?: string
  director_info?: string  // JSON string
  cast?: string  // JSON string
  tmdb_rating?: number
  tmdb_votes?: number
  keywords?: string  // JSON string
  llm_tags?: string  // JSON string
  budget?: number
  revenue?: number
  popularity?: number
  // bilingual fields
  title_zh?: string
  title_en?: string
  overview_zh?: string
  overview_en?: string
  tagline_zh?: string
  tagline_en?: string
  genres_zh?: string
  genres_en?: string
  director_info_en?: string  // JSON string (en-US TMDB crew)
  cast_en?: string            // JSON string (en-US TMDB cast)
  keywords_en?: string        // JSON string (en-US TMDB keywords)
  collection_en?: string      // JSON string (en-US TMDB collection)
  production_companies_en?: string  // JSON string (en-US TMDB companies)
  // extended TMDB fields
  imdb_id?: string
  backdrop_path?: string
  homepage?: string
  status?: string
  collection?: string  // JSON string
  production_companies?: string  // JSON string
  spoken_languages?: string  // JSON string
  origin_country?: string  // JSON string
  source?: string
}

export interface MovieCredit {
  id: number
  movie_id: number
  tmdb_person_id: number
  person_name: string
  credit_type: string
  role?: string
  department?: string
  order?: number
  profile_path?: string
  person_name_en?: string
  role_en?: string
}

export interface MovieImage {
  id: number
  movie_id: number
  image_type: string
  file_path: string
  iso_639_1?: string
  width?: number
  height?: number
  vote_average?: number
}

export interface MovieVideo {
  id: number
  movie_id: number
  video_key: string
  site?: string
  video_type?: string
  name?: string
  iso_639_1?: string
  official?: number
  published_at?: string
}

export interface MovieReview {
  id: number
  movie_id: number
  tmdb_review_id: string
  author?: string
  author_username?: string
  content?: string
  rating?: number
  created_at?: string
  updated_at?: string
}

export interface MovieReleaseDate {
  id: number
  movie_id: number
  iso_3166_1: string
  release_date?: string
  certification?: string
  release_type?: number
  note?: string
}

export interface MovieWatchProvider {
  id: number
  movie_id: number
  iso_3166_1: string
  provider_id: number
  provider_name?: string
  logo_path?: string
  provider_type: string
  display_priority?: number
}

export interface MovieExternalId {
  id: number
  movie_id: number
  imdb_id?: string
  facebook_id?: string
  instagram_id?: string
  twitter_id?: string
  wikidata_id?: string
}

export interface MovieAlternativeTitle {
  id: number
  movie_id: number
  iso_3166_1?: string
  title: string
  title_type?: string
}

export interface MovieTranslation {
  id: number
  movie_id: number
  iso_639_1: string
  iso_3166_1?: string
  language_name?: string
  title?: string
  overview?: string
  tagline?: string
  homepage?: string
  runtime?: number
}

export interface RelatedMovie {
  id: number
  movie_id: number
  related_tmdb_id: number
  relation_type: string
}

export interface MovieList {
  id: number
  movie_id: number
  tmdb_list_id: number
  list_name?: string
  description?: string
  item_count?: number
  iso_639_1?: string
}

export interface MovieDetail extends Movie {
  dir_paths?: string[]
  credits?: MovieCredit[]
  images?: MovieImage[]
  videos?: MovieVideo[]
  reviews?: MovieReview[]
  similar?: Movie[]
  recommendations?: Movie[]
  watch_providers?: MovieWatchProvider[]
  release_dates?: MovieReleaseDate[]
  external_ids?: MovieExternalId
  alternative_titles?: MovieAlternativeTitle[]
  translations?: MovieTranslation[]
  lists?: MovieList[]
  download_status?: {
    state: string
    progress: number
    dlspeed: number
    size: number | null
    media_type: string
  }
}

export interface Person {
  tmdb_person_id: number
  name: string
  also_known_as: string[]
  biography?: string
  profile_url?: string
  birthday?: string
  deathday?: string
  place_of_birth?: string
}

export interface CastMember {
  name: string
  tmdb_person_id?: number
  character?: string
  profile_path?: string
}

export interface DirectorInfo {
  name: string
  tmdb_person_id?: number
  profile_path?: string
}

export interface PendingDir {
  dir_id: number
  dir_name: string
  dir_path: string
  match_status: string
  confidence?: number
}

export interface TmdbCandidate {
  id: number
  title: string
  original_title?: string
  release_date?: string
  overview?: string
  poster_path?: string
  vote_average?: number
}

export interface ListResponse<T> {
  data: T[]
  total: number
  page: number
  per_page: number
}

export interface TaskCount {
  task_type: string
  status: string
  count: number
}

export interface FilterOptions {
  decades: [string, number][]
  genres: [string, number][]
  countries: [string, number][]
  languages: [string, number][]
  ratings: [string, number][]
  runtimes: [string, number][]
}

export interface RecommendItem {
  movie: Movie
  reason?: string | null
  in_library?: boolean
  downloading?: boolean
}

export interface RecommendResult {
  recommendations: RecommendItem[]
}

export interface AdminOverviewData {
  dir_total: number
  dir_status: [string, number][]
  match_status: [string, number][]
  movies_by_source: [string, number][]
  tasks: Record<string, Record<string, number>>
  year_buckets: [string, number][]
  country_top: [string, number][]
  genre_top: [string, number][]
  rating_histogram: [string, number][]
  mark_counts: Record<string, number>
}

export interface LlmLogEntry {
  filename: string
  size: number
  modified: string
}

export interface PromptInfo {
  name: string
  content: string
  default_content: string
  locale: string
  overridden: boolean
}

export interface SearchHistoryItem {
  id: number
  prompt: string
  result_count: number
  created_at: string
}

export interface SearchHistoryDetail {
  id: number
  prompt: string
  sse_events: string
  result_count: number
  created_at: string
  share_token?: string | null
}

export interface ParsedSseEvent {
  event: 'status' | 'thinking' | 'result' | string
  data: any
}

export interface BenchmarkQuery {
  id: number
  query: string
  note: string | null
  expected_ids: number[]
  not_expected_ids: number[]
  created_at: string
  updated_at: string
  source_history_id: number | null
}

export interface BenchmarkQueryRunResult {
  run_id: number
  run_started_at: string
  run_finished_at: string | null
  run_status: string
  run_note: string | null
  run_is_baseline: boolean
  hit: boolean | null
  coverage_ratio: number | null
  elapsed_ms: number | null
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  top_movies: any[]
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  intent: any | null
  error: string | null
  not_expected_ids: number[]
}

export interface BenchmarkRun {
  id: number
  started_at: string
  finished_at: string | null
  status: 'running' | 'done' | 'error' | 'canceled' | string
  total: number
  passed: number
  failed: number
  note: string | null
  is_baseline: boolean
  cancel_requested: boolean
}

export interface BenchmarkTopMovie {
  tmdb_id: number
  title: string
  in_library?: boolean
}

export interface BenchmarkResultView {
  id: number
  query_id: number
  query_text: string
  expected_ids: number[]
  not_expected_ids: number[]
  top_movies: BenchmarkTopMovie[]
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  intent_json: any | null
  hit: boolean | null
  coverage_ratio: number | null
  elapsed_ms: number | null
  error: string | null
}

export interface BenchmarkAggregateMovie {
  tmdb_id: number
  movie_id: number | null
  title: string | null
  title_zh: string | null
  title_en: string | null
  poster_url: string | null
  year: number | null
  appearance_count: number
  best_rank: number | null
  avg_rank: number | null
  latest_at: string | null
  is_expected: boolean
  is_not_expected: boolean
}

export interface BenchmarkAggregateResponse {
  query: BenchmarkQuery
  history_count: number
  total_movies: number
  page: number
  page_size: number
  movies: BenchmarkAggregateMovie[]
}

export interface BenchmarkMovieAppearance {
  history_id: number
  rank: number
  created_at: string
}

export interface BenchmarkMovieAppearancesResponse {
  tmdb_id: number
  appearances: BenchmarkMovieAppearance[]
}

export interface BenchmarkRunDetail {
  run: BenchmarkRun
  results: BenchmarkResultView[]
}

export interface BenchmarkCompareItem {
  query_id: number
  query_text: string
  expected_ids: number[]
  baseline: BenchmarkResultView | null
  current: BenchmarkResultView | null
  added_movies: BenchmarkTopMovie[]
  removed_movies: BenchmarkTopMovie[]
  intent_changed: boolean
  hit_delta: number | null
}

export interface BenchmarkCompareResponse {
  baseline_run: BenchmarkRun
  current_run: BenchmarkRun
  items: BenchmarkCompareItem[]
}

export interface LocateCandidate {
  dir_id: number
  dir_name: string
  dir_path: string
  status: string | null
  score: number
  parsed_title: string
  parsed_year: number | null
}

export interface LocateResponse {
  candidates: LocateCandidate[]
}

export interface AppConfig {
  scan: {
    enabled: boolean
    movie_dirs: string[]
    interval_hours: number
    worker_poll_secs: number
    refresh_interval_hours: number
    refresh_batch_size: number
    ssh_key_path?: string
  }
  tmdb: {
    api_key: string
    language: string
    auto_confirm_threshold: number
    proxy: string | null
  }
  llm: {
    backend: string
    base_url: string
    api_key: string
    model: string
  }
  server: {
    host: string
    port: number
  }
  database: {
    path: string
  }
  auth: {
    jwt_secret: string
    jwt_expiry_days: number
  }
  qbittorrent: {
    enabled: boolean
    base_url: string
    username: string
    password: string
    save_path: string
    poll_interval_hours: number
  }
}

export interface MultiVersionMovieSummary {
  id: number
  title: string
  title_zh: string | null
  year: number | null
  poster_url: string | null
}

export interface MultiVersionDir {
  dir_id: number
  dir_name: string
  dir_path: string
  source: string | null
  match_status: string
  match_confidence: number | null
  torrent_name: string | null
  media_type: string | null
  size_bytes: number | null
  torrent_state: string | null
  torrent_progress: number | null
}

export interface MultiVersionMovie {
  movie: MultiVersionMovieSummary
  version_count: number
  dirs: MultiVersionDir[]
}

export interface MultiVersionResponse {
  items: MultiVersionMovie[]
  total: number
  limit: number
  offset: number
}
