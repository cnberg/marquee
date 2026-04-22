use crate::config::LlmConfig;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone)]
pub struct LlmClient {
    http: Client,
    base_url: String,
    api_key: String,
    model: String,
    log_dir: PathBuf,
}

#[derive(Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: f64,
}

#[derive(Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatResponse {
    #[serde(default)]
    choices: Vec<ChatChoice>,
    /// OpenRouter sometimes returns HTTP 200 with a top-level `error` object
    /// instead of usable choices (e.g. provider unreachable).
    #[serde(default)]
    error: Option<UpstreamError>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ResponseMessage,
    /// OpenRouter can also embed an upstream error inside an individual choice
    /// (e.g. provider rate-limited mid-stream). When present the `message.content`
    /// is typically truncated and must NOT be trusted.
    #[serde(default)]
    error: Option<UpstreamError>,
}

#[derive(Deserialize)]
struct ResponseMessage {
    #[serde(default)]
    content: Option<String>,
}

#[derive(Deserialize)]
struct UpstreamError {
    #[serde(default)]
    code: Option<serde_json::Value>,
    #[serde(default)]
    message: Option<String>,
}

impl UpstreamError {
    fn code_str(&self) -> String {
        match &self.code {
            Some(serde_json::Value::Number(n)) => n.to_string(),
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(v) => v.to_string(),
            None => "unknown".to_string(),
        }
    }

    fn to_friendly(&self) -> String {
        let code = self.code_str();
        let msg = self.message.clone().unwrap_or_else(|| "未知错误".to_string());
        match code.as_str() {
            "429" => "LLM 上游服务繁忙（rate limit），请稍后再试".to_string(),
            "401" | "403" => "LLM 上游认证失败".to_string(),
            c if c.starts_with('5') => format!("LLM 上游服务异常（{}）：{}", c, msg),
            _ => format!("LLM 上游错误（{}）：{}", code, msg),
        }
    }
}

impl LlmClient {
    pub fn new(config: &LlmConfig) -> Self {
        let log_dir = PathBuf::from("data/llm-logs");
        std::fs::create_dir_all(&log_dir).ok();
        Self {
            http: Client::new(),
            base_url: config.base_url.trim_end_matches('/').to_string(),
            api_key: config.api_key.clone(),
            model: config.model.clone(),
            log_dir,
        }
    }

    fn write_log(&self, tag: &str, request_body: &str, status: u16, response_body: &str) {
        let ts = chrono::Local::now().format("%Y%m%d_%H%M%S");
        let filename = format!("{}_{}.log", ts, tag);
        let path = self.log_dir.join(&filename);
        let content = format!(
            "=== REQUEST ===\nPOST {}/chat/completions\nModel: {}\n\n{}\n\n=== RESPONSE (HTTP {}) ===\n{}\n",
            self.base_url, self.model, request_body, status, response_body,
        );
        if let Err(e) = std::fs::write(&path, &content) {
            tracing::warn!("failed to write LLM log to {}: {}", path.display(), e);
        } else {
            tracing::info!("LLM log written to {}", path.display());
        }
    }

    /// Send a chat completion request with system + user messages.
    /// Returns the assistant's response content string.
    pub async fn chat(
        &self,
        system: &str,
        user: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let request = ChatRequest {
            model: self.model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".into(),
                    content: system.into(),
                },
                ChatMessage {
                    role: "user".into(),
                    content: user.into(),
                },
            ],
            temperature: 0.7,
        };

        let request_json = serde_json::to_string_pretty(&request).unwrap_or_default();

        let resp = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        let status = resp.status();
        let status_code = status.as_u16();
        let body = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            self.write_log("error", &request_json, status_code, &body);
            let friendly = match status_code {
                429 => "LLM 服务繁忙（请求过于频繁），请稍后再试".to_string(),
                401 | 403 => "LLM API 认证失败，请检查 API Key 配置".to_string(),
                500..=599 => format!("LLM 服务暂时不可用（HTTP {}），请稍后再试", status_code),
                _ => format!("LLM API 错误（HTTP {}）", status_code),
            };
            tracing::warn!("LLM API error {}: {}", status_code, body);
            return Err(friendly.into());
        }

        // Detect HTML responses (wrong API URL, e.g. missing /v1)
        let trimmed = body.trim_start();
        if trimmed.starts_with('<') || trimmed.starts_with("<!") {
            self.write_log("parse_error", &request_json, status_code, &body);
            return Err(format!(
                "LLM 返回了 HTML 而非 JSON，请检查 base_url 配置是否正确（当前: {}/chat/completions）",
                self.base_url
            ).into());
        }

        let chat_resp: ChatResponse = match serde_json::from_str(&body) {
            Ok(r) => r,
            Err(e) => {
                self.write_log("parse_error", &request_json, status_code, &body);
                return Err(format!("LLM 返回格式异常: {}（日志���写入 {}）", e, self.log_dir.display()).into());
            }
        };

        // OpenRouter sometimes returns HTTP 200 with an upstream error embedded
        // either at the top level or inside a choice. The accompanying content
        // is truncated/garbage and must NOT be parsed downstream — surface a
        // friendly error so the UI can tell the user to retry.
        if let Some(err) = chat_resp.error.as_ref() {
            self.write_log("upstream_error", &request_json, status_code, &body);
            return Err(err.to_friendly().into());
        }
        if let Some(choice_err) = chat_resp.choices.first().and_then(|c| c.error.as_ref()) {
            self.write_log("upstream_error", &request_json, status_code, &body);
            return Err(choice_err.to_friendly().into());
        }

        // Log successful calls too (for debugging prompt quality)
        self.write_log("ok", &request_json, status_code, &body);

        chat_resp
            .choices
            .into_iter()
            .next()
            .and_then(|c| c.message.content)
            .ok_or_else(|| "LLM 返回内容为空".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_response_with_choice_level_upstream_error() {
        // Regression: OpenRouter returns HTTP 200 with choices[0].error set and
        // a truncated content string. We must detect the embedded error rather
        // than passing the truncated content downstream.
        let body = r#"{
            "choices": [{
                "message": {"role": "assistant", "content": "[{\"display\": \"truncated"},
                "error": {"code": 429, "message": "JSON error injected into SSE stream"}
            }]
        }"#;
        let resp: ChatResponse = serde_json::from_str(body).expect("must deserialize");
        let choice_err = resp.choices.first().and_then(|c| c.error.as_ref());
        assert!(choice_err.is_some(), "choice-level error must be detected");
        let friendly = choice_err.unwrap().to_friendly();
        assert!(friendly.contains("rate limit") || friendly.contains("429"));
    }

    #[test]
    fn parse_response_with_top_level_upstream_error() {
        let body = r#"{"choices": [], "error": {"code": "500", "message": "upstream"}}"#;
        let resp: ChatResponse = serde_json::from_str(body).unwrap();
        assert!(resp.error.is_some());
        let friendly = resp.error.unwrap().to_friendly();
        assert!(friendly.contains("500"));
    }

    #[test]
    fn parse_normal_response_has_no_errors() {
        let body = r#"{"choices": [{"message": {"role": "assistant", "content": "ok"}}]}"#;
        let resp: ChatResponse = serde_json::from_str(body).unwrap();
        assert!(resp.error.is_none());
        assert!(resp.choices[0].error.is_none());
        assert_eq!(resp.choices[0].message.content.as_deref(), Some("ok"));
    }
}
