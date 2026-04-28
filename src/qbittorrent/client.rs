use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::path::Path;

#[derive(Clone)]
pub struct QbtClient {
    client: Client,
    base_url: String,
    username: String,
    password: String,
    save_path_filter: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct QbtTorrent {
    pub hash: String,
    pub name: String,
    pub state: String,
    pub progress: f64,
    pub size: Option<i64>,
    pub total_size: Option<i64>,
    pub dlspeed: Option<i64>,
    pub upspeed: Option<i64>,
    pub ratio: Option<f64>,
    pub num_seeds: Option<i64>,
    pub save_path: Option<String>,
    pub content_path: Option<String>,
    pub category: Option<String>,
    pub added_on: Option<i64>,
}

impl QbtTorrent {
    /// Detect media type from torrent name.
    pub fn media_type(&self) -> &'static str {
        let n = self.name.to_lowercase();
        if n.ends_with(".iso") {
            return "ISO";
        }
        // Order matters: check REMUX before generic Blu-ray
        if n.contains("remux") {
            if n.contains("uhd") || n.contains("2160p") {
                return "UHD REMUX";
            }
            return "REMUX";
        }
        if n.contains("uhd") && (n.contains("blu-ray") || n.contains("bluray")) {
            return "UHD Blu-ray";
        }
        if n.contains("blu-ray") || n.contains("complete bluray") {
            return "Blu-ray";
        }
        if n.contains("bluray") || n.contains("blu ray") {
            // BluRay + encode (x264/x265/AVC/HEVC without REMUX) = encode
            return "BluRay Encode";
        }
        if n.contains("web-dl") {
            return "WEB-DL";
        }
        if n.contains("webrip") {
            return "WEBRip";
        }
        if n.contains("hdtv") {
            return "HDTV";
        }
        if n.contains("dvdrip") {
            return "DVDRip";
        }
        if n.contains("dvd") {
            return "DVD";
        }
        "Other"
    }

    /// Extract the directory name (basename) from content_path or name.
    pub fn dir_name(&self) -> String {
        if let Some(cp) = &self.content_path {
            let p = Path::new(cp);
            // content_path could be a file (single-file torrent) or directory
            // For a directory torrent: /save_path/DirName
            // For a file torrent: /save_path/filename.mkv
            if let Some(name) = p.file_name() {
                let name_str = name.to_string_lossy().to_string();
                // Strip common video extensions if it's a single file
                if let Some(stem) = name_str.strip_suffix(".mkv")
                    .or_else(|| name_str.strip_suffix(".mp4"))
                    .or_else(|| name_str.strip_suffix(".avi"))
                {
                    return stem.to_string();
                }
                return name_str;
            }
        }
        self.name.clone()
    }
}

impl QbtClient {
    pub fn new(base_url: &str, username: &str, password: &str, save_path_filter: &str) -> Self {
        let client = Client::builder()
            .cookie_store(true)
            .build()
            .expect("failed to build reqwest client");
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            username: username.to_string(),
            password: password.to_string(),
            save_path_filter: save_path_filter.to_string(),
        }
    }

    pub async fn login(&self) -> Result<(), String> {
        let url = format!("{}/api/v2/auth/login", self.base_url);
        let resp = self
            .client
            .post(&url)
            .header("Referer", &self.base_url)
            .form(&[("username", &self.username), ("password", &self.password)])
            .send()
            .await
            .map_err(|e| format!("qBT login request failed: {}", e))?;

        match resp.status() {
            StatusCode::OK => {
                let body = resp.text().await.unwrap_or_default();
                if body.contains("Fails") || body.contains("fail") {
                    Err("qBT login failed: invalid credentials".to_string())
                } else {
                    Ok(())
                }
            }
            StatusCode::FORBIDDEN => Err("qBT login failed: IP banned".to_string()),
            s => Err(format!("qBT login failed: HTTP {}", s)),
        }
    }

    pub async fn list_torrents(&self) -> Result<Vec<QbtTorrent>, String> {
        let url = format!("{}/api/v2/torrents/info", self.base_url);
        let resp = self
            .client
            .get(&url)
            .header("Referer", &self.base_url)
            .send()
            .await
            .map_err(|e| format!("qBT list_torrents failed: {}", e))?;

        if resp.status() == StatusCode::FORBIDDEN {
            return Err("qBT session expired, need re-login".to_string());
        }

        let torrents: Vec<QbtTorrent> = resp
            .json()
            .await
            .map_err(|e| format!("qBT parse torrents failed: {}", e))?;

        // Filter by save_path prefix
        if self.save_path_filter.is_empty() {
            return Ok(torrents);
        }

        let filter = self.save_path_filter.trim_end_matches('/');
        Ok(torrents
            .into_iter()
            .filter(|t| {
                t.save_path
                    .as_deref()
                    .map(|sp| sp.trim_end_matches('/') == filter || sp.starts_with(&format!("{}/", filter)))
                    .unwrap_or(false)
            })
            .collect())
    }

    /// Login and fetch torrents in one call.
    pub async fn fetch_torrents(&self) -> Result<Vec<QbtTorrent>, String> {
        self.login().await?;
        self.list_torrents().await
    }
}
