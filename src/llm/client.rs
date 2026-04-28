use crate::config::{LlmBackend, LlmConfig};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

pub const LLM_LOGS_DIR: &str = "data/llm-logs";
const CLAUDE_CLI_TIMEOUT_SECS: u64 = 120;
const STDERR_PREVIEW_MAX: usize = 500;

pub type LlmResult = Result<String, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone)]
pub struct LlmClient {
    inner: Backend,
    log_dir: PathBuf,
}

#[derive(Clone)]
enum Backend {
    Openai(OpenaiBackend),
    ClaudeCli(ClaudeCliBackend),
}

impl LlmClient {
    pub fn new(config: &LlmConfig) -> Self {
        let log_dir = PathBuf::from(LLM_LOGS_DIR);
        std::fs::create_dir_all(&log_dir).ok();
        let inner = match config.backend {
            LlmBackend::Openai => Backend::Openai(OpenaiBackend {
                http: Client::new(),
                base_url: config.base_url.trim_end_matches('/').to_string(),
                api_key: config.api_key.clone(),
                model: config.model.clone(),
            }),
            LlmBackend::ClaudeCli => Backend::ClaudeCli(ClaudeCliBackend {
                model: if config.model.is_empty() { None } else { Some(config.model.clone()) },
            }),
        };
        Self { inner, log_dir }
    }

    /// Send a chat completion request with system + user messages.
    /// Returns the assistant's response content string.
    pub async fn chat(&self, system: &str, user: &str) -> LlmResult {
        match &self.inner {
            Backend::Openai(b) => b.chat(system, user, &self.log_dir).await,
            Backend::ClaudeCli(b) => b.chat(system, user, &self.log_dir).await,
        }
    }
}

// ---------- OpenAI-compatible HTTP backend ----------

#[derive(Clone)]
struct OpenaiBackend {
    http: Client,
    base_url: String,
    api_key: String,
    model: String,
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

impl OpenaiBackend {
    fn write_log(&self, log_dir: &Path, tag: &str, request_body: &str, status: u16, response_body: &str) {
        let ts = chrono::Local::now().format("%Y%m%d_%H%M%S");
        let filename = format!("{}_{}.log", ts, tag);
        let path = log_dir.join(&filename);
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

    async fn chat(&self, system: &str, user: &str, log_dir: &Path) -> LlmResult {
        let request = ChatRequest {
            model: self.model.clone(),
            messages: vec![
                ChatMessage { role: "system".into(), content: system.into() },
                ChatMessage { role: "user".into(), content: user.into() },
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
            self.write_log(log_dir, "error", &request_json, status_code, &body);
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
            self.write_log(log_dir, "parse_error", &request_json, status_code, &body);
            return Err(format!(
                "LLM 返回了 HTML 而非 JSON，请检查 base_url 配置是否正确（当前: {}/chat/completions）",
                self.base_url
            ).into());
        }

        let chat_resp: ChatResponse = match serde_json::from_str(&body) {
            Ok(r) => r,
            Err(e) => {
                self.write_log(log_dir, "parse_error", &request_json, status_code, &body);
                return Err(format!("LLM 返回格式异常: {}（日志已写入 {}）", e, log_dir.display()).into());
            }
        };

        // OpenRouter sometimes returns HTTP 200 with an upstream error embedded
        // either at the top level or inside a choice. The accompanying content
        // is truncated/garbage and must NOT be parsed downstream — surface a
        // friendly error so the UI can tell the user to retry.
        if let Some(err) = chat_resp.error.as_ref() {
            self.write_log(log_dir, "upstream_error", &request_json, status_code, &body);
            return Err(err.to_friendly().into());
        }
        if let Some(choice_err) = chat_resp.choices.first().and_then(|c| c.error.as_ref()) {
            self.write_log(log_dir, "upstream_error", &request_json, status_code, &body);
            return Err(choice_err.to_friendly().into());
        }

        // Log successful calls too (for debugging prompt quality)
        self.write_log(log_dir, "ok", &request_json, status_code, &body);

        chat_resp
            .choices
            .into_iter()
            .next()
            .and_then(|c| c.message.content)
            .ok_or_else(|| "LLM 返回内容为空".into())
    }
}

// ---------- Local `claude` CLI backend ----------

#[derive(Clone)]
struct ClaudeCliBackend {
    model: Option<String>,
}

impl ClaudeCliBackend {
    async fn chat(&self, system: &str, user: &str, log_dir: &Path) -> LlmResult {
        let args = build_claude_args(user, system, self.model.as_deref());
        let request_dump = format_claude_request(&args);

        let spawn_result = Command::new("claude")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn();

        let child = match spawn_result {
            Ok(c) => c,
            Err(e) => {
                let msg = if e.kind() == std::io::ErrorKind::NotFound {
                    "找不到 claude CLI，请确保已安装并在 PATH 中".to_string()
                } else {
                    format!("启动 claude CLI 失败：{}", e)
                };
                write_cli_log(log_dir, "error", &request_dump, None, "", &e.to_string());
                return Err(msg.into());
            }
        };

        let wait_fut = child.wait_with_output();
        let output = match tokio::time::timeout(Duration::from_secs(CLAUDE_CLI_TIMEOUT_SECS), wait_fut).await {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => {
                write_cli_log(log_dir, "error", &request_dump, None, "", &e.to_string());
                return Err(format!("claude CLI IO 错误：{}", e).into());
            }
            Err(_elapsed) => {
                // Timeout: process killed via kill_on_drop when child is dropped.
                write_cli_log(log_dir, "timeout", &request_dump, None, "", "");
                return Err(format!("claude CLI 调用超时（{}s）", CLAUDE_CLI_TIMEOUT_SECS).into());
            }
        };

        let exit_code = output.status.code();
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        if !output.status.success() {
            write_cli_log(log_dir, "error", &request_dump, exit_code, &stdout, &stderr);
            let preview = truncate_stderr(&stderr);
            let code_str = exit_code.map(|c| c.to_string()).unwrap_or_else(|| "signal".into());
            return Err(format!("claude CLI 异常退出（exit={}）：{}", code_str, preview).into());
        }

        let trimmed = stdout.trim().to_string();
        if trimmed.is_empty() {
            write_cli_log(log_dir, "empty", &request_dump, exit_code, &stdout, &stderr);
            return Err("claude CLI 返回内容为空".into());
        }

        write_cli_log(log_dir, "ok", &request_dump, exit_code, &stdout, &stderr);
        Ok(trimmed)
    }
}

fn build_claude_args(user: &str, system: &str, model: Option<&str>) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "-p".into(),
        user.to_string(),
        "--output-format".into(),
        "text".into(),
        "--max-turns".into(),
        "1".into(),
        "--tools".into(),
        "".into(),
        "--system-prompt".into(),
        system.to_string(),
    ];
    if let Some(m) = model.filter(|s| !s.is_empty()) {
        args.push("--model".into());
        args.push(m.to_string());
    }
    args
}

fn format_claude_request(args: &[String]) -> String {
    let mut buf = String::from("backend: claude-cli\nargv:\n  claude\n");
    for a in args {
        buf.push_str("  ");
        buf.push_str(a);
        buf.push('\n');
    }
    buf
}

fn truncate_stderr(stderr: &str) -> String {
    let trimmed = stderr.trim();
    if trimmed.chars().count() <= STDERR_PREVIEW_MAX {
        trimmed.to_string()
    } else {
        let head: String = trimmed.chars().take(STDERR_PREVIEW_MAX).collect();
        format!("{}…（已截断）", head)
    }
}

fn write_cli_log(log_dir: &Path, tag: &str, request_dump: &str, exit: Option<i32>, stdout: &str, stderr: &str) {
    let ts = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let filename = format!("{}_{}.log", ts, tag);
    let path = log_dir.join(&filename);
    let exit_str = exit.map(|c| c.to_string()).unwrap_or_else(|| "n/a".into());
    let content = format!(
        "=== REQUEST ===\n{}\n=== RESPONSE (exit {}) ===\n{}\n=== STDERR ===\n{}\n",
        request_dump, exit_str, stdout, stderr,
    );
    if let Err(e) = std::fs::write(&path, &content) {
        tracing::warn!("failed to write LLM log to {}: {}", path.display(), e);
    } else {
        tracing::info!("LLM log written to {}", path.display());
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

    #[test]
    fn build_claude_args_without_model() {
        let args = build_claude_args("hello", "you are helpful", None);
        assert!(!args.iter().any(|a| a == "--model"));
        assert_eq!(args[0], "-p");
        assert_eq!(args[1], "hello");
        // --system-prompt payload sits as its own argv element
        let sys_idx = args.iter().position(|a| a == "--system-prompt").unwrap();
        assert_eq!(args[sys_idx + 1], "you are helpful");
        // tools disabled explicitly
        let tools_idx = args.iter().position(|a| a == "--tools").unwrap();
        assert_eq!(args[tools_idx + 1], "");
    }

    #[test]
    fn build_claude_args_with_model() {
        let args = build_claude_args("u", "s", Some("claude-opus-4-7"));
        let idx = args.iter().position(|a| a == "--model").expect("must contain --model");
        assert_eq!(args[idx + 1], "claude-opus-4-7");
    }

    #[test]
    fn build_claude_args_empty_model_treated_as_none() {
        let args = build_claude_args("u", "s", Some(""));
        assert!(!args.iter().any(|a| a == "--model"));
    }

    #[test]
    fn build_claude_args_preserves_special_chars_in_single_argv_slot() {
        // Subprocess arg vector passes each element verbatim to the child — no
        // shell escaping needed. Verify a payload with newlines + quotes + backticks
        // survives as ONE argv element.
        let nasty = "line1\nline2\n\"quoted\" `cmd`";
        let args = build_claude_args(nasty, "sys", None);
        assert_eq!(args[1], nasty, "user payload must be one argv element");
        let sys_idx = args.iter().position(|a| a == "--system-prompt").unwrap();
        let args2 = build_claude_args("u", nasty, None);
        assert_eq!(args2[sys_idx + 1], nasty, "system payload must be one argv element");
    }

    #[test]
    fn truncate_stderr_short() {
        assert_eq!(truncate_stderr("short error"), "short error");
        assert_eq!(truncate_stderr("  trimmed  "), "trimmed");
    }

    #[test]
    fn truncate_stderr_long_is_truncated_with_suffix() {
        let long = "x".repeat(STDERR_PREVIEW_MAX + 50);
        let out = truncate_stderr(&long);
        assert!(out.ends_with("…（已截断）"));
        assert!(out.chars().count() > STDERR_PREVIEW_MAX);
    }
}
