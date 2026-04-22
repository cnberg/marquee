use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct PageParams {
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_per_page")]
    pub per_page: i64,
}

pub fn default_page() -> i64 {
    1
}

pub fn default_per_page() -> i64 {
    20
}

#[derive(Debug, Serialize)]
pub struct ListResponse<T: Serialize> {
    pub data: T,
    pub page: i64,
    pub per_page: i64,
    pub total: i64,
}

pub fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
