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
  similar?: RelatedMovie[]
  recommendations?: RelatedMovie[]
  watch_providers?: MovieWatchProvider[]
  release_dates?: MovieReleaseDate[]
  external_ids?: MovieExternalId
  alternative_titles?: MovieAlternativeTitle[]
  translations?: MovieTranslation[]
  lists?: MovieList[]
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
}

export interface RecommendResult {
  recommendations: RecommendItem[]
}

export interface AdminOverviewData {
  dir_total: number
  dir_status: [string, number][]
  match_status: [string, number][]
  tasks: Record<string, Record<string, number>>
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
}

export interface ParsedSseEvent {
  event: 'status' | 'thinking' | 'result' | string
  data: any
}
