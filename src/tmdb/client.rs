use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};

#[derive(Clone)]
pub struct TmdbClient {
    client: Client,
    api_key: String,
    language: String,
    /// Base URL for TMDB API (without trailing slash). Override via
    /// `with_base_url` in tests so requests can be intercepted.
    base_url: String,
    /// Rate limiter: max 4 concurrent requests
    semaphore: Arc<Semaphore>,
}

const DEFAULT_TMDB_BASE_URL: &str = "https://api.themoviedb.org/3";

#[derive(Debug, Deserialize)]
pub struct TmdbSearchResponse {
    pub results: Vec<TmdbSearchResult>,
    pub total_results: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TmdbSearchResult {
    pub id: i64,
    pub title: String,
    pub original_title: Option<String>,
    pub release_date: Option<String>,
    pub overview: Option<String>,
    pub poster_path: Option<String>,
    pub vote_average: Option<f64>,
    pub vote_count: Option<i64>,
    pub popularity: Option<f64>,
    pub genre_ids: Option<Vec<i64>>,
    pub original_language: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbMovieDetail {
    pub id: i64,
    pub title: String,
    pub original_title: Option<String>,
    pub release_date: Option<String>,
    pub overview: Option<String>,
    pub tagline: Option<String>,
    pub poster_path: Option<String>,
    pub genres: Option<Vec<TmdbGenre>>,
    pub runtime: Option<i64>,
    pub vote_average: Option<f64>,
    pub vote_count: Option<i64>,
    pub production_countries: Option<Vec<TmdbCountry>>,
    pub original_language: Option<String>,
    pub budget: Option<i64>,
    pub revenue: Option<i64>,
    pub popularity: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbGenre {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct TmdbCountry {
    pub iso_3166_1: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct TmdbCreditsResponse {
    pub cast: Option<Vec<TmdbCastMember>>,
    pub crew: Option<Vec<TmdbCrewMember>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TmdbCastMember {
    pub id: i64,
    pub name: String,
    pub character: Option<String>,
    pub order: Option<i64>,
    pub profile_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TmdbCrewMember {
    pub id: i64,
    pub name: String,
    pub job: String,
    pub profile_path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbKeywordsResponse {
    pub keywords: Option<Vec<TmdbKeyword>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbKeyword {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct TmdbPersonDetail {
    pub id: i64,
    pub name: String,
    pub also_known_as: Option<Vec<String>>,
    pub biography: Option<String>,
    pub profile_path: Option<String>,
    pub birthday: Option<String>,
    pub deathday: Option<String>,
    pub place_of_birth: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbImagesResponse {
    pub posters: Option<Vec<TmdbImage>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbImage {
    pub file_path: String,
    pub iso_639_1: Option<String>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub vote_average: Option<f64>,
}

/// Full movie response with all append_to_response sub-resources
#[derive(Debug, Deserialize)]
pub struct TmdbMovieFull {
    pub id: i64,
    pub title: String,
    pub original_title: Option<String>,
    pub release_date: Option<String>,
    pub overview: Option<String>,
    pub poster_path: Option<String>,
    pub genres: Option<Vec<TmdbGenre>>,
    pub runtime: Option<i64>,
    pub vote_average: Option<f64>,
    pub vote_count: Option<i64>,
    pub production_countries: Option<Vec<TmdbCountry>>,
    pub original_language: Option<String>,
    pub budget: Option<i64>,
    pub revenue: Option<i64>,
    pub popularity: Option<f64>,
    pub imdb_id: Option<String>,
    pub tagline: Option<String>,
    pub homepage: Option<String>,
    pub status: Option<String>,
    pub backdrop_path: Option<String>,
    pub belongs_to_collection: Option<TmdbCollection>,
    pub production_companies: Option<Vec<TmdbCompany>>,
    pub spoken_languages: Option<Vec<TmdbSpokenLanguage>>,
    pub origin_country: Option<Vec<String>>,
    pub credits: Option<TmdbCreditsResponse>,
    pub keywords: Option<TmdbKeywordsResponse>,
    pub images: Option<TmdbFullImagesResponse>,
    pub videos: Option<TmdbVideosResponse>,
    pub reviews: Option<TmdbReviewsResponse>,
    pub similar: Option<TmdbMovieListResponse>,
    pub recommendations: Option<TmdbMovieListResponse>,
    pub release_dates: Option<TmdbReleaseDatesResponse>,
    #[serde(rename = "watch/providers")]
    pub watch_providers: Option<TmdbWatchProvidersResponse>,
    pub external_ids: Option<TmdbExternalIds>,
    pub alternative_titles: Option<TmdbAlternativeTitlesResponse>,
    pub translations: Option<TmdbTranslationsResponse>,
    pub lists: Option<TmdbListsResponse>,
    pub changes: Option<TmdbChangesResponse>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TmdbCollection {
    pub id: i64,
    pub name: Option<String>,
    pub poster_path: Option<String>,
    pub backdrop_path: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TmdbCompany {
    pub id: i64,
    pub name: String,
    pub logo_path: Option<String>,
    pub origin_country: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TmdbSpokenLanguage {
    pub iso_639_1: String,
    pub english_name: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbFullImagesResponse {
    pub backdrops: Option<Vec<TmdbImage>>,
    pub logos: Option<Vec<TmdbImage>>,
    pub posters: Option<Vec<TmdbImage>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbVideosResponse {
    pub results: Option<Vec<TmdbVideoItem>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbVideoItem {
    pub key: String,
    pub site: Option<String>,
    #[serde(rename = "type")]
    pub video_type: Option<String>,
    pub name: Option<String>,
    pub iso_639_1: Option<String>,
    pub official: Option<bool>,
    pub published_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbReviewsResponse {
    pub results: Option<Vec<TmdbReviewItem>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbReviewItem {
    pub id: String,
    pub author: Option<String>,
    pub author_details: Option<TmdbReviewAuthor>,
    pub content: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbReviewAuthor {
    pub username: Option<String>,
    pub rating: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbMovieListResponse {
    pub results: Option<Vec<TmdbSearchResult>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbReleaseDatesResponse {
    pub results: Option<Vec<TmdbReleaseDateCountry>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbReleaseDateCountry {
    pub iso_3166_1: String,
    pub release_dates: Vec<TmdbReleaseDateEntry>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbReleaseDateEntry {
    pub release_date: Option<String>,
    pub certification: Option<String>,
    #[serde(rename = "type")]
    pub release_type: Option<i64>,
    pub note: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbWatchProvidersResponse {
    pub results: Option<std::collections::HashMap<String, TmdbWatchProviderCountry>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbWatchProviderCountry {
    pub flatrate: Option<Vec<TmdbWatchProviderItem>>,
    pub rent: Option<Vec<TmdbWatchProviderItem>>,
    pub buy: Option<Vec<TmdbWatchProviderItem>>,
    pub ads: Option<Vec<TmdbWatchProviderItem>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbWatchProviderItem {
    pub provider_id: i64,
    pub provider_name: Option<String>,
    pub logo_path: Option<String>,
    pub display_priority: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbExternalIds {
    pub imdb_id: Option<String>,
    pub facebook_id: Option<String>,
    pub instagram_id: Option<String>,
    pub twitter_id: Option<String>,
    pub wikidata_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbAlternativeTitlesResponse {
    pub titles: Option<Vec<TmdbAlternativeTitle>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbAlternativeTitle {
    pub iso_3166_1: Option<String>,
    pub title: String,
    #[serde(rename = "type")]
    pub title_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbTranslationsResponse {
    pub translations: Option<Vec<TmdbTranslation>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbTranslation {
    pub iso_639_1: String,
    pub iso_3166_1: Option<String>,
    pub name: Option<String>,
    pub english_name: Option<String>,
    pub data: Option<TmdbTranslationData>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbTranslationData {
    pub title: Option<String>,
    pub overview: Option<String>,
    pub tagline: Option<String>,
    pub homepage: Option<String>,
    pub runtime: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbListsResponse {
    pub results: Option<Vec<TmdbListItem>>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbListItem {
    pub id: i64,
    pub name: Option<String>,
    pub description: Option<String>,
    pub item_count: Option<i64>,
    pub iso_639_1: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TmdbChangesResponse {
    pub changes: Option<Vec<serde_json::Value>>,
}

impl TmdbClient {
    pub fn new(api_key: &str, language: &str, proxy: Option<&str>) -> Self {
        let mut builder = Client::builder();
        if let Some(proxy_url) = proxy {
            if let Ok(p) = reqwest::Proxy::all(proxy_url) {
                builder = builder.proxy(p);
                tracing::info!("TMDB client using proxy: {}", proxy_url);
            }
        }
        Self {
            client: builder.build().unwrap_or_else(|_| Client::new()),
            api_key: api_key.to_string(),
            language: language.to_string(),
            base_url: DEFAULT_TMDB_BASE_URL.to_string(),
            semaphore: Arc::new(Semaphore::new(4)),
        }
    }

    /// Test-only constructor that points the client at a custom base URL
    /// (e.g. a wiremock mock server). Keeps the default rate limiter but
    /// trims the inter-request sleep is NOT applied — it still runs the
    /// 260ms delay from `rate_limited_get`, which is fine for a handful of
    /// requests in a unit test.
    #[cfg(test)]
    pub fn with_base_url(api_key: &str, language: &str, base_url: &str) -> Self {
        Self {
            client: Client::new(),
            api_key: api_key.to_string(),
            language: language.to_string(),
            base_url: base_url.trim_end_matches('/').to_string(),
            semaphore: Arc::new(Semaphore::new(4)),
        }
    }

    async fn rate_limited_get(&self, url: &str) -> Result<reqwest::Response, reqwest::Error> {
        let _permit = self.semaphore.acquire().await.unwrap();
        let resp = self.client.get(url).send().await?;
        // Small delay to stay under TMDB rate limits
        sleep(Duration::from_millis(260)).await;
        Ok(resp)
    }

    pub async fn search_movie(
        &self,
        query: &str,
        year: Option<u32>,
    ) -> Result<Vec<TmdbSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        self.search_movie_with_lang(query, year, &self.language).await
    }

    pub async fn search_movie_with_lang(
        &self,
        query: &str,
        year: Option<u32>,
        language: &str,
    ) -> Result<Vec<TmdbSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        let mut url = format!(
            "{}/search/movie?api_key={}&language={}&query={}",
            self.base_url,
            self.api_key,
            language,
            urlencoding::encode(query)
        );
        if let Some(y) = year {
            url.push_str(&format!("&year={}", y));
        }

        let resp = self.rate_limited_get(&url).await?;
        let body: TmdbSearchResponse = resp.json().await?;
        Ok(body.results)
    }

    pub async fn get_movie_detail(
        &self,
        tmdb_id: i64,
    ) -> Result<TmdbMovieDetail, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/movie/{}?api_key={}&language={}",
            self.base_url, tmdb_id, self.api_key, self.language
        );
        let resp = self.rate_limited_get(&url).await?;
        let detail: TmdbMovieDetail = resp.json().await?;
        Ok(detail)
    }

    /// Fetch movie with all sub-resources via append_to_response.
    pub async fn get_movie_full(
        &self,
        tmdb_id: i64,
        language: &str,
    ) -> Result<TmdbMovieFull, Box<dyn std::error::Error + Send + Sync>> {
        let append = "credits,keywords,images,videos,reviews,similar,recommendations,release_dates,watch/providers,external_ids,alternative_titles,translations,lists,changes";
        let url = format!(
            "{}/movie/{}?api_key={}&language={}&append_to_response={}&include_image_language=en,zh,null",
            self.base_url, tmdb_id, self.api_key, language, append
        );
        let resp = self.rate_limited_get(&url).await?;
        let full: TmdbMovieFull = resp.json().await?;
        Ok(full)
    }

    /// Fetch basic movie detail in a specific language (for bilingual fields).
    pub async fn get_movie_basic(
        &self,
        tmdb_id: i64,
        language: &str,
    ) -> Result<TmdbMovieDetail, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/movie/{}?api_key={}&language={}",
            self.base_url, tmdb_id, self.api_key, language
        );
        let resp = self.rate_limited_get(&url).await?;
        let detail: TmdbMovieDetail = resp.json().await?;
        Ok(detail)
    }

    pub async fn get_movie_credits(
        &self,
        tmdb_id: i64,
    ) -> Result<TmdbCreditsResponse, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/movie/{}/credits?api_key={}&language={}",
            self.base_url, tmdb_id, self.api_key, self.language
        );
        let resp = self.rate_limited_get(&url).await?;
        let credits: TmdbCreditsResponse = resp.json().await?;
        Ok(credits)
    }

    pub async fn get_movie_keywords(
        &self,
        tmdb_id: i64,
    ) -> Result<TmdbKeywordsResponse, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/movie/{}/keywords?api_key={}",
            self.base_url, tmdb_id, self.api_key
        );
        let resp = self.rate_limited_get(&url).await?;
        let keywords: TmdbKeywordsResponse = resp.json().await?;
        Ok(keywords)
    }

    pub async fn get_person_detail(
        &self,
        person_id: i64,
    ) -> Result<TmdbPersonDetail, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/person/{}?api_key={}&language={}",
            self.base_url, person_id, self.api_key, self.language
        );
        let resp = self.rate_limited_get(&url).await?;
        let detail: TmdbPersonDetail = resp.json().await?;
        Ok(detail)
    }

    pub async fn get_movie_images(
        &self,
        tmdb_id: i64,
        language: &str,
    ) -> Result<TmdbImagesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/movie/{}/images?api_key={}&language={}&include_image_language={},null",
            self.base_url, tmdb_id, self.api_key, language, language
        );
        let resp = self.rate_limited_get(&url).await?;
        let images: TmdbImagesResponse = resp.json().await?;
        Ok(images)
    }
}
