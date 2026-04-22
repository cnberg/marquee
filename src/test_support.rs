//! Shared test helpers for API integration tests.
//!
//! Exposes a lightweight builder that assembles an [`AppState`] backed by an
//! in-memory SQLite pool, a stub LLM client, and `None` embedding
//! dependencies. This is enough to exercise every non-`recommend` route
//! end-to-end via `tower::ServiceExt::oneshot`.

#![cfg(test)]

use axum::body::{to_bytes, Body};
use axum::http::{Request, Response, StatusCode};
use axum::Router;
use serde_json::Value;
use sqlx::SqlitePool;
use tower::ServiceExt;

use crate::api::{create_router, AppState};
use crate::config::{
    AuthConfig, Config, DatabaseConfig, LlmConfig, ScanConfig, ServerConfig, TmdbConfig,
};
use crate::llm::LlmClient;

/// Build a minimal [`Config`] suitable for tests. All values are stubs.
pub fn test_config() -> Config {
    Config {
        scan: ScanConfig {
            movie_dir: "/tmp/nonexistent-marquee-test".into(),
            interval_hours: 6,
        },
        tmdb: TmdbConfig {
            api_key: "test-key".into(),
            language: "zh-CN".into(),
            auto_confirm_threshold: 0.85,
            proxy: None,
        },
        llm: LlmConfig {
            base_url: "http://localhost".into(),
            api_key: String::new(),
            model: "stub".into(),
        },
        server: ServerConfig {
            host: "127.0.0.1".into(),
            port: 0,
        },
        database: DatabaseConfig {
            path: ":memory:".into(),
        },
        auth: AuthConfig {
            jwt_secret: "test-secret-42".into(),
            jwt_expiry_days: 7,
        },
    }
}

/// Build an [`AppState`] for routes that do not need embedding deps.
pub fn test_state(pool: SqlitePool) -> AppState {
    let config = test_config();
    let llm = LlmClient::new(&config.llm);
    AppState {
        pool,
        config,
        llm,
        embedding_model: None,
        embedding_store: None,
    }
}

/// Build the full axum [`Router`] wired to a test state.
pub fn test_app(pool: SqlitePool) -> Router {
    create_router(test_state(pool))
}

/// Dispatch a `Request` through the router in-process (no network).
pub async fn oneshot(router: Router, req: Request<Body>) -> Response<Body> {
    router.oneshot(req).await.expect("router oneshot")
}

/// Convenience: POST JSON and return (status, parsed JSON body).
pub async fn post_json(
    router: Router,
    path: &str,
    body: &Value,
    bearer: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = bearer {
        req = req.header("authorization", format!("Bearer {}", token));
    }
    let req = req.body(Body::from(body.to_string())).unwrap();
    let resp = oneshot(router, req).await;
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

/// Convenience: PUT JSON and return (status, parsed JSON body).
pub async fn put_json(
    router: Router,
    path: &str,
    bearer: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("PUT")
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = bearer {
        req = req.header("authorization", format!("Bearer {}", token));
    }
    let req = req.body(Body::empty()).unwrap();
    let resp = oneshot(router, req).await;
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

/// Convenience: DELETE and return (status, parsed JSON body).
pub async fn delete_json(
    router: Router,
    path: &str,
    bearer: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method("DELETE").uri(path);
    if let Some(token) = bearer {
        req = req.header("authorization", format!("Bearer {}", token));
    }
    let req = req.body(Body::empty()).unwrap();
    let resp = oneshot(router, req).await;
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

/// Convenience: GET and return (status, parsed JSON body).
pub async fn get_json(
    router: Router,
    path: &str,
    bearer: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method("GET").uri(path);
    if let Some(token) = bearer {
        req = req.header("authorization", format!("Bearer {}", token));
    }
    let req = req.body(Body::empty()).unwrap();
    let resp = oneshot(router, req).await;
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}
