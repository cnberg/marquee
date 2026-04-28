use std::io;
use std::sync::Arc;

use russh::client;
use russh::keys::{self, key::PrivateKeyWithHashAlg, PublicKey};
use russh_sftp::client::SftpSession;

/// Parsed ssh://user@host[:port]/path URL.
#[derive(Debug)]
pub struct SshUrl {
    pub user: String,
    pub host: String,
    pub port: u16,
    pub path: String,
}

pub fn parse_ssh_url(url: &str) -> Result<SshUrl, String> {
    let rest = url
        .strip_prefix("ssh://")
        .ok_or_else(|| format!("not an ssh:// URL: {}", url))?;

    // user@host[:port]/path
    let (user_host, path) = rest
        .split_once('/')
        .ok_or_else(|| format!("missing path in ssh URL: {}", url))?;

    let path = format!("/{}", path);

    let (user, host_port) = user_host
        .split_once('@')
        .ok_or_else(|| format!("missing user@ in ssh URL: {}", url))?;

    let (host, port) = if let Some((h, p)) = host_port.split_once(':') {
        let port: u16 = p.parse().map_err(|_| format!("invalid port: {}", p))?;
        (h.to_string(), port)
    } else {
        (host_port.to_string(), 22)
    };

    Ok(SshUrl {
        user: user.to_string(),
        host,
        port,
        path,
    })
}

struct ClientHandler;

impl client::Handler for ClientHandler {
    type Error = russh::Error;

    fn check_server_key(
        &mut self,
        _server_public_key: &PublicKey,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send {
        // Accept all host keys (equivalent to StrictHostKeyChecking=no).
        // This is acceptable for an internal tool on a trusted LAN.
        async { Ok(true) }
    }
}

/// 选取要尝试的私钥路径列表。
/// - `Some(p)`：只尝试 `p`，不做 fallback——用户既然显式配了，就用它，
///   读不出/认证不过都直接报错，不静默回退到不是预期的 key。
/// - `None`：依次尝试 `~/.ssh/id_ed25519`、`~/.ssh/id_rsa`、`~/.ssh/id_ecdsa`。
fn resolve_key_paths(configured: Option<&str>) -> Result<Vec<String>, io::Error> {
    if let Some(p) = configured {
        return Ok(vec![p.to_string()]);
    }
    let home = std::env::var("HOME").map_err(|_| {
        io::Error::new(io::ErrorKind::NotFound, "HOME environment variable not set")
    })?;
    Ok(["id_ed25519", "id_rsa", "id_ecdsa"]
        .iter()
        .map(|n| format!("{}/.ssh/{}", home, n))
        .collect())
}

/// List first-level subdirectory names under `remote_path` via SSH+SFTP.
/// Returns Vec<(full_remote_path, dir_name)>.
///
/// `key_path`：private key 路径。`None` 时回落到 `~/.ssh/` 三件套。
pub async fn list_remote_dirs(
    url: &SshUrl,
    key_path: Option<&str>,
) -> Result<Vec<(String, String)>, io::Error> {
    let config = Arc::new(client::Config::default());
    let handler = ClientHandler;

    let mut session = client::connect(config, (&*url.host, url.port), handler)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, format!("SSH connect to {}:{} failed: {}", url.host, url.port, e)))?;

    let key_paths = resolve_key_paths(key_path)?;
    let mut authenticated = false;
    let mut last_load_error: Option<String> = None;
    for path in &key_paths {
        let key_pair = match keys::load_secret_key(path, None) {
            Ok(k) => k,
            Err(e) => {
                last_load_error = Some(format!("{}: {}", path, e));
                continue;
            }
        };
        let key = PrivateKeyWithHashAlg::new(Arc::new(key_pair), None);
        if let Ok(russh::client::AuthResult::Success) =
            session.authenticate_publickey(&url.user, key).await
        {
            authenticated = true;
            break;
        }
    }

    if !authenticated {
        let detail = if let Some(p) = key_path {
            format!(
                "SSH auth failed for {}@{}: configured ssh_key_path '{}' did not authenticate{}",
                url.user,
                url.host,
                p,
                last_load_error
                    .as_deref()
                    .map(|e| format!(" (load error: {})", e))
                    .unwrap_or_default(),
            )
        } else {
            format!(
                "SSH auth failed for {}@{} — no matching key in ~/.ssh/ (tried id_ed25519, id_rsa, id_ecdsa)",
                url.user, url.host
            )
        };
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, detail));
    }

    // Open SFTP channel
    let channel = session
        .channel_open_session()
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("SSH channel open failed: {}", e)))?;

    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("SFTP subsystem request failed: {}", e)))?;

    let sftp = SftpSession::new(channel.into_stream())
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("SFTP session init failed: {}", e)))?;

    // Read directory entries
    let entries = sftp
        .read_dir(&url.path)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("SFTP readdir {} failed: {}", url.path, e)))?;

    let mut dirs = Vec::new();
    for entry in entries {
        let name = entry.file_name();
        // Skip hidden dirs
        if name.starts_with('.') {
            continue;
        }
        // Check if directory
        if entry.file_type().is_dir() {
            let full_path = format!("ssh://{}@{}:{}{}/{}", url.user, url.host, url.port, url.path.trim_end_matches('/'), name);
            dirs.push((full_path, name));
        }
    }

    // Close session (best-effort)
    let _ = session
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await;

    Ok(dirs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_key_path_is_used_alone() {
        let paths = resolve_key_paths(Some("/etc/marquee/ssh.key")).expect("ok");
        assert_eq!(paths, vec!["/etc/marquee/ssh.key".to_string()]);
    }

    #[test]
    fn no_config_falls_back_to_three_default_names() {
        // 这个测试依赖 HOME 已设置（CI / 开发机都满足）。
        let paths = resolve_key_paths(None).expect("HOME should be set");
        assert_eq!(paths.len(), 3);
        assert!(paths[0].ends_with("/.ssh/id_ed25519"));
        assert!(paths[1].ends_with("/.ssh/id_rsa"));
        assert!(paths[2].ends_with("/.ssh/id_ecdsa"));
    }

    #[test]
    fn parse_ssh_url_basic() {
        let url = parse_ssh_url("ssh://alice@192.168.1.100/home/alice/movies").unwrap();
        assert_eq!(url.user, "alice");
        assert_eq!(url.host, "192.168.1.100");
        assert_eq!(url.port, 22);
        assert_eq!(url.path, "/home/alice/movies");
    }

    #[test]
    fn parse_ssh_url_with_port() {
        let url = parse_ssh_url("ssh://alice@192.168.1.100:2222/movies").unwrap();
        assert_eq!(url.port, 2222);
    }

    #[test]
    fn parse_ssh_url_missing_user_errors() {
        assert!(parse_ssh_url("ssh://192.168.1.100/path").is_err());
    }

    #[test]
    fn parse_ssh_url_missing_path_errors() {
        assert!(parse_ssh_url("ssh://alice@192.168.1.100").is_err());
    }
}
