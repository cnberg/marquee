use figment::{
    providers::{Format, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub scan: ScanConfig,
    pub tmdb: TmdbConfig,
    pub llm: LlmConfig,
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub auth: AuthConfig,
    #[serde(default)]
    pub qbittorrent: QbittorrentConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ScanConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub movie_dirs: Vec<String>,
    #[serde(default = "default_interval_hours")]
    pub interval_hours: u32,
    #[serde(default = "default_worker_poll_secs")]
    pub worker_poll_secs: u64,
    #[serde(default = "default_refresh_interval_hours")]
    pub refresh_interval_hours: u32,
    #[serde(default = "default_refresh_batch_size")]
    pub refresh_batch_size: u32,
    /// 用于 ssh:// 远端目录扫描的私钥路径。None 时依次回落到
    /// ~/.ssh/{id_ed25519,id_rsa,id_ecdsa}。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_key_path: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TmdbConfig {
    pub api_key: String,
    #[serde(default = "default_language")]
    pub language: String,
    #[serde(default = "default_threshold")]
    pub auto_confirm_threshold: f64,
    #[serde(default)]
    pub proxy: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum LlmBackend {
    #[default]
    Openai,
    ClaudeCli,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LlmConfig {
    #[serde(default)]
    pub backend: LlmBackend,
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub model: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DatabaseConfig {
    #[serde(default = "default_db_path")]
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AuthConfig {
    pub jwt_secret: String,
    #[serde(default = "default_jwt_expiry_days")]
    pub jwt_expiry_days: u64,
}

fn default_true() -> bool {
    true
}
fn default_interval_hours() -> u32 {
    6
}
fn default_worker_poll_secs() -> u64 {
    5
}
fn default_refresh_interval_hours() -> u32 {
    1
}
fn default_refresh_batch_size() -> u32 {
    60
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct QbittorrentConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_qbt_base_url")]
    pub base_url: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub save_path: String,
    #[serde(default = "default_interval_hours")]
    pub poll_interval_hours: u32,
}

impl Default for QbittorrentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: default_qbt_base_url(),
            username: String::new(),
            password: String::new(),
            save_path: String::new(),
            poll_interval_hours: default_interval_hours(),
        }
    }
}

fn default_qbt_base_url() -> String {
    "http://localhost:8080".to_string()
}

pub const CONFIG_PATH: &str = "marquee.toml";

fn mask_secret(s: &str) -> String {
    if s.len() <= 3 {
        "***".to_string()
    } else {
        format!("***{}", &s[s.len() - 3..])
    }
}

fn is_masked(s: &str) -> bool {
    s.starts_with("***")
}

impl Config {
    pub fn load() -> Result<Self, figment::Error> {
        Figment::new()
            .merge(Toml::file(CONFIG_PATH))
            .extract()
    }

    /// Return a copy with sensitive fields masked for API display.
    pub fn masked(&self) -> Self {
        let mut c = self.clone();
        c.tmdb.api_key = mask_secret(&c.tmdb.api_key);
        c.llm.api_key = mask_secret(&c.llm.api_key);
        c.auth.jwt_secret = mask_secret(&c.auth.jwt_secret);
        c.qbittorrent.password = mask_secret(&c.qbittorrent.password);
        c
    }

    /// Merge incoming config, preserving original sensitive values when masked.
    pub fn merge_sensitive(&self, incoming: &mut Config) {
        if is_masked(&incoming.tmdb.api_key) {
            incoming.tmdb.api_key = self.tmdb.api_key.clone();
        }
        if is_masked(&incoming.llm.api_key) {
            incoming.llm.api_key = self.llm.api_key.clone();
        }
        if is_masked(&incoming.auth.jwt_secret) {
            incoming.auth.jwt_secret = self.auth.jwt_secret.clone();
        }
        if is_masked(&incoming.qbittorrent.password) {
            incoming.qbittorrent.password = self.qbittorrent.password.clone();
        }
    }

    /// Save to TOML file.
    pub fn save(&self) -> Result<(), String> {
        let toml_str = toml::to_string_pretty(self)
            .map_err(|e| format!("TOML serialize error: {}", e))?;
        std::fs::write(CONFIG_PATH, toml_str)
            .map_err(|e| format!("Failed to write {}: {}", CONFIG_PATH, e))?;
        Ok(())
    }
}
