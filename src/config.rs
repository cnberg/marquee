use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub scan: ScanConfig,
    pub tmdb: TmdbConfig,
    pub llm: LlmConfig,
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub auth: AuthConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ScanConfig {
    pub movie_dir: String,
    #[serde(default = "default_interval_hours")]
    pub interval_hours: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TmdbConfig {
    pub api_key: String,
    #[serde(default = "default_language")]
    pub language: String,
    #[serde(default = "default_threshold")]
    pub auto_confirm_threshold: f64,
    #[serde(default)]
    pub proxy: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LlmConfig {
    pub base_url: String,
    #[serde(default)]
    pub api_key: String,
    pub model: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    #[serde(default = "default_db_path")]
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    pub jwt_secret: String,
    #[serde(default = "default_jwt_expiry_days")]
    pub jwt_expiry_days: u64,
}

fn default_interval_hours() -> u32 {
    6
}
fn default_language() -> String {
    "zh-CN".to_string()
}
fn default_threshold() -> f64 {
    0.85
}
fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    8080
}
fn default_db_path() -> String {
    "./data/marquee.db".to_string()
}
fn default_jwt_expiry_days() -> u64 {
    30
}

impl Config {
    pub fn load() -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Toml::file("marquee.toml"))
            .merge(Env::prefixed("MARQUEE_").split("__"))
            .extract()
    }
}
