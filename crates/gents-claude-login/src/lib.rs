//! Claude subscription OAuth login (PKCE) used by `gents claude-login`.
//!
//! Owns only the browser/loopback and manual-paste code exchanges. Tokens go
//! back to the caller for immediate persistence in DefraDB. Client constants
//! mirror `gents::claude_oauth`; keep both in sync (a gents-cli unit test pins it).

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::thread;

use base64::Engine;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tiny_http::{Header, Response, Server, StatusCode as TinyStatusCode};
use tokio::sync::{mpsc, Notify};
use url::Url;

pub const CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
pub const AUTHORIZE_URL: &str = "https://claude.com/cai/oauth/authorize";
pub const TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";
pub const MANUAL_REDIRECT_URL: &str = "https://platform.claude.com/oauth/code/callback";
pub const SCOPES: &str =
    "user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoginOptions {
    pub client_id: String,
    pub authorize_url: String,
    pub token_url: String,
    pub open_browser: bool,
    /// Test hook for deterministic callback-state checks.
    pub force_state: Option<String>,
}

impl Default for LoginOptions {
    fn default() -> Self {
        Self {
            client_id: CLIENT_ID.to_string(),
            authorize_url: AUTHORIZE_URL.to_string(),
            token_url: TOKEN_URL.to_string(),
            open_browser: true,
            force_state: None,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LoginTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: Option<i64>,
    pub scope: Option<String>,
}

impl fmt::Debug for LoginTokens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoginTokens")
            .field("access_token", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .field("expires_in", &self.expires_in)
            .field("scope", &self.scope)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct ShutdownHandle {
    notify: Arc<Notify>,
}

impl ShutdownHandle {
    pub fn shutdown(&self) {
        self.notify.notify_one();
    }
}

pub struct LoginServer {
    pub auth_url: String,
    pub actual_port: u16,
    task: tokio::task::JoinHandle<io::Result<LoginTokens>>,
    shutdown: ShutdownHandle,
}

impl LoginServer {
    pub async fn block_until_done(self) -> io::Result<LoginTokens> {
        self.task
            .await
            .map_err(|error| io::Error::other(format!("login callback task failed: {error}")))?
    }

    pub fn cancel_handle(&self) -> ShutdownHandle {
        self.shutdown.clone()
    }
}

/// Loopback PKCE login: binds an ephemeral localhost port, prints/opens the
/// authorize URL, accepts one `/callback`, exchanges the code.
pub fn run_loopback_login(options: LoginOptions) -> io::Result<LoginServer> {
    let pkce = generate_pkce();
    let state = options.force_state.clone().unwrap_or_else(generate_state);
    let server = Server::http("127.0.0.1:0").map_err(io::Error::other)?;
    let actual_port = server
        .server_addr()
        .to_ip()
        .map(|address| address.port())
        .ok_or_else(|| io::Error::other("login callback server did not expose an IP port"))?;
    let server = Arc::new(server);
    let redirect_uri = format!("http://localhost:{actual_port}/callback");
    let auth_url = build_authorize_url(&options, &redirect_uri, &pkce, &state)?;

    if options.open_browser {
        if let Err(error) = webbrowser::open(&auth_url) {
            tracing::warn!(%error, "could not open the Claude login URL in a browser");
        }
    }

    let (sender, mut receiver) = mpsc::channel(8);
    let receive_server = server.clone();
    thread::spawn(move || {
        while let Ok(request) = receive_server.recv() {
            if sender.blocking_send(request).is_err() {
                break;
            }
        }
    });

    let notify = Arc::new(Notify::new());
    let task_notify = notify.clone();
    let task_server = server.clone();
    let task = tokio::spawn(async move {
        let result = loop {
            tokio::select! {
                _ = task_notify.notified() => break Err(io::Error::other("Claude login was cancelled")),
                request = receiver.recv() => {
                    let Some(request) = request else {
                        break Err(io::Error::other("Claude login callback server stopped"));
                    };
                    let outcome =
                        handle_callback_request(request.url(), &options, &redirect_uri, &pkce, &state)
                            .await;
                    let (status, body, completed) = outcome.into_parts();
                    let response = text_response(status, body);
                    let _ = tokio::task::spawn_blocking(move || request.respond(response)).await;
                    if let Some(result) = completed {
                        break result;
                    }
                }
            }
        };
        task_server.unblock();
        result
    });

    Ok(LoginServer {
        auth_url,
        actual_port,
        task,
        shutdown: ShutdownHandle { notify },
    })
}

/// Manual-paste login for hosts without a browser: prints the authorize URL
/// (redirect = Anthropic's code page), the caller supplies the pasted
/// `code#state` string, we exchange it.
pub async fn run_manual_login(
    options: LoginOptions,
    read_pasted: impl FnOnce(&str) -> io::Result<String>,
) -> io::Result<LoginTokens> {
    let pkce = generate_pkce();
    let state = options.force_state.clone().unwrap_or_else(generate_state);
    let auth_url = build_authorize_url(&options, MANUAL_REDIRECT_URL, &pkce, &state)?;
    let pasted = read_pasted(&auth_url)?;
    let code = parse_manual_code(pasted.trim(), &state)?;
    exchange_code(&options, MANUAL_REDIRECT_URL, &pkce, &code, &state).await
}

pub(crate) fn parse_manual_code(pasted: &str, expected_state: &str) -> io::Result<String> {
    let (code, state) = pasted.split_once('#').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected the pasted value to look like code#state",
        )
    })?;
    if state != expected_state {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "OAuth state mismatch; restart sign-in",
        ));
    }
    if code.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "authorization code was empty",
        ));
    }
    Ok(code.to_string())
}

enum CallbackOutcome {
    Continue {
        status: u16,
        body: String,
    },
    Complete {
        status: u16,
        body: String,
        result: io::Result<LoginTokens>,
    },
}

impl CallbackOutcome {
    fn into_parts(self) -> (u16, String, Option<io::Result<LoginTokens>>) {
        match self {
            Self::Continue { status, body } => (status, body, None),
            Self::Complete {
                status,
                body,
                result,
            } => (status, body, Some(result)),
        }
    }
}

async fn handle_callback_request(
    request_target: &str,
    options: &LoginOptions,
    redirect_uri: &str,
    pkce: &PkceCodes,
    expected_state: &str,
) -> CallbackOutcome {
    let Ok(parsed) = Url::parse(&format!("http://localhost{request_target}")) else {
        return CallbackOutcome::Continue {
            status: 400,
            body: "Invalid callback request.".into(),
        };
    };
    if parsed.path() != "/callback" {
        return CallbackOutcome::Continue {
            status: 404,
            body: "Not found.".into(),
        };
    }
    let params: HashMap<String, String> = parsed.query_pairs().into_owned().collect();
    if params.get("state").map(String::as_str) != Some(expected_state) {
        tracing::warn!("rejected Claude OAuth callback with mismatched state");
        return CallbackOutcome::Continue {
            status: 400,
            body: "OAuth state mismatch. Return to the terminal and retry sign-in.".into(),
        };
    }
    if let Some(error) = params.get("error") {
        let message = params
            .get("error_description")
            .map(String::as_str)
            .filter(|v| !v.trim().is_empty())
            .unwrap_or(error);
        return CallbackOutcome::Complete {
            status: 400,
            body: "Claude sign-in was not completed. Return to Gents for details.".into(),
            result: Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("Claude authorization failed: {message}"),
            )),
        };
    }
    let Some(code) = params
        .get("code")
        .map(String::as_str)
        .filter(|v| !v.is_empty())
    else {
        return CallbackOutcome::Continue {
            status: 400,
            body: "The callback omitted its authorization code.".into(),
        };
    };
    match exchange_code(options, redirect_uri, pkce, code, expected_state).await {
        Ok(tokens) => CallbackOutcome::Complete {
            status: 200,
            body: "Claude sign-in complete. You may close this window.".into(),
            result: Ok(tokens),
        },
        Err(error) => CallbackOutcome::Complete {
            status: 502,
            body: "Claude token exchange failed. Return to Gents for details.".into(),
            result: Err(error),
        },
    }
}

fn text_response(status: u16, body: String) -> Response<std::io::Cursor<Vec<u8>>> {
    let mut response = Response::from_string(body).with_status_code(TinyStatusCode(status));
    if let Ok(header) = Header::from_bytes("Content-Type", "text/plain; charset=utf-8") {
        response.add_header(header);
    }
    response
}

#[derive(Clone)]
pub(crate) struct PkceCodes {
    pub(crate) verifier: String,
    pub(crate) challenge: String,
}

impl fmt::Debug for PkceCodes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PkceCodes")
            .field("verifier", &"<redacted>")
            .field("challenge", &self.challenge)
            .finish()
    }
}

pub(crate) fn generate_pkce() -> PkceCodes {
    let mut bytes = [0u8; 64];
    rand::rng().fill_bytes(&mut bytes);
    let verifier = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(Sha256::digest(verifier.as_bytes()));
    PkceCodes {
        verifier,
        challenge,
    }
}

fn generate_state() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

pub(crate) fn build_authorize_url(
    options: &LoginOptions,
    redirect_uri: &str,
    pkce: &PkceCodes,
    state: &str,
) -> io::Result<String> {
    let mut url = Url::parse(&options.authorize_url)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    url.query_pairs_mut()
        .append_pair("code", "true")
        .append_pair("client_id", &options.client_id)
        .append_pair("response_type", "code")
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", SCOPES)
        .append_pair("code_challenge", &pkce.challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", state);
    Ok(url.into())
}

#[derive(Serialize)]
struct ExchangeRequest<'a> {
    grant_type: &'static str,
    code: &'a str,
    redirect_uri: &'a str,
    client_id: &'a str,
    code_verifier: &'a str,
    state: &'a str,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(default)]
    scope: Option<String>,
}

pub(crate) async fn exchange_code(
    options: &LoginOptions,
    redirect_uri: &str,
    pkce: &PkceCodes,
    code: &str,
    state: &str,
) -> io::Result<LoginTokens> {
    let request = ExchangeRequest {
        grant_type: "authorization_code",
        code,
        redirect_uri,
        client_id: &options.client_id,
        code_verifier: &pkce.verifier,
        state,
    };
    let response = reqwest::Client::new()
        .post(&options.token_url)
        .header("Content-Type", "application/json")
        .json(&request)
        .send()
        .await
        .map_err(redacted_transport_error)?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let detail = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_string))
            .unwrap_or_else(|| "no error code".to_string());
        return Err(io::Error::other(format!(
            "Claude token endpoint returned HTTP {status}: {detail}"
        )));
    }
    let tokens = response
        .json::<TokenResponse>()
        .await
        .map_err(|error| io::Error::other(format!("decoding Claude OAuth tokens: {error}")))?;
    Ok(LoginTokens {
        access_token: tokens.access_token,
        refresh_token: tokens.refresh_token,
        expires_in: tokens.expires_in,
        scope: tokens.scope,
    })
}

fn redacted_transport_error(error: reqwest::Error) -> io::Error {
    let kind = if error.is_timeout() {
        "timed out"
    } else if error.is_connect() {
        "could not connect"
    } else {
        "failed"
    };
    io::Error::other(format!("Claude token exchange {kind}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> LoginOptions {
        LoginOptions {
            open_browser: false,
            force_state: Some("state-1".into()),
            ..LoginOptions::default()
        }
    }

    #[test]
    fn authorize_url_carries_claude_code_query_shape() {
        let pkce = generate_pkce();
        let url = build_authorize_url(
            &options(),
            "http://localhost:4242/callback",
            &pkce,
            "state-1",
        )
        .unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.origin().ascii_serialization(), "https://claude.com");
        assert_eq!(parsed.path(), "/cai/oauth/authorize");
        let q: std::collections::HashMap<_, _> = parsed.query_pairs().into_owned().collect();
        assert_eq!(q["code"], "true");
        assert_eq!(q["client_id"], CLIENT_ID);
        assert_eq!(q["response_type"], "code");
        assert_eq!(q["redirect_uri"], "http://localhost:4242/callback");
        assert_eq!(q["scope"], SCOPES);
        assert_eq!(q["code_challenge"], pkce.challenge);
        assert_eq!(q["code_challenge_method"], "S256");
        assert_eq!(q["state"], "state-1");
    }

    #[test]
    fn pkce_challenge_is_s256_of_verifier() {
        let pkce = generate_pkce();
        let expected = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(pkce.verifier.as_bytes()));
        assert_eq!(pkce.challenge, expected);
        assert!(pkce.verifier.len() >= 43);
    }

    #[test]
    fn debug_output_redacts_secrets() {
        let tokens = LoginTokens {
            access_token: "access-SECRET".into(),
            refresh_token: "refresh-SECRET".into(),
            expires_in: Some(60),
            scope: Some("user:inference".into()),
        };
        let rendered = format!("{tokens:?}");
        assert!(!rendered.contains("SECRET"), "{rendered}");
        assert!(rendered.contains("expires_in: Some(60)"), "{rendered}");
        let pkce = generate_pkce();
        let rendered = format!("{pkce:?}");
        assert!(!rendered.contains(&pkce.verifier), "{rendered}");
    }

    #[test]
    fn manual_code_parsing_requires_matching_state() {
        assert_eq!(parse_manual_code("abc#state-1", "state-1").unwrap(), "abc");
        assert!(parse_manual_code("abc#state-2", "state-1").is_err());
        assert!(parse_manual_code("abc", "state-1").is_err());
        assert!(parse_manual_code("", "state-1").is_err());
    }

    #[tokio::test]
    async fn callback_with_wrong_state_is_rejected_and_keeps_waiting() {
        let pkce = generate_pkce();
        let outcome = handle_callback_request(
            "/callback?code=abc&state=wrong",
            &options(),
            "http://localhost:1/callback",
            &pkce,
            "state-1",
        )
        .await;
        let (status, _, completed) = outcome.into_parts();
        assert_eq!(status, 400);
        assert!(completed.is_none());
    }

    #[tokio::test]
    async fn exchange_posts_json_with_state_and_verifier() {
        let (url, handle) = one_shot_server(200, r#"{"access_token":"access-NEW","refresh_token":"refresh-NEW","expires_in":28800,"scope":"user:inference"}"#).await;
        let opts = LoginOptions {
            token_url: url,
            ..options()
        };
        let pkce = generate_pkce();
        let tokens = exchange_code(
            &opts,
            "http://localhost:1/callback",
            &pkce,
            "code-1",
            "state-1",
        )
        .await
        .unwrap();
        let request = handle.await.unwrap();
        let body: serde_json::Value =
            serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(body["grant_type"], "authorization_code");
        assert_eq!(body["code"], "code-1");
        assert_eq!(body["redirect_uri"], "http://localhost:1/callback");
        assert_eq!(body["client_id"], CLIENT_ID);
        assert_eq!(body["code_verifier"], pkce.verifier);
        assert_eq!(body["state"], "state-1");
        assert!(
            request.contains("content-type: application/json"),
            "{request}"
        );
        assert_eq!(tokens.access_token, "access-NEW");
        assert_eq!(tokens.refresh_token, "refresh-NEW");
        assert_eq!(tokens.expires_in, Some(28800));
    }

    #[tokio::test]
    async fn exchange_error_never_echoes_the_code() {
        let (url, _handle) = one_shot_server(401, r#"{"error":"invalid_grant"}"#).await;
        let opts = LoginOptions {
            token_url: url,
            ..options()
        };
        let pkce = generate_pkce();
        let err = exchange_code(
            &opts,
            "http://localhost:1/callback",
            &pkce,
            "code-SECRET",
            "state-1",
        )
        .await
        .unwrap_err();
        assert!(!err.to_string().contains("code-SECRET"), "{err}");
        assert!(err.to_string().contains("401"), "{err}");
    }

    /// One-shot HTTP responder (no extra dependency): accepts one request, returns it, answers with `status` + `body`.
    async fn one_shot_server(
        status: u16,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!("http://{}/v1/oauth/token", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = socket.read(&mut chunk).await.expect("read");
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf);
                if let Some(idx) = text.find("\r\n\r\n") {
                    let content_length: usize = text[..idx]
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("content-length: ")
                                .or_else(|| line.strip_prefix("Content-Length: "))
                        })
                        .and_then(|value| value.trim().parse().ok())
                        .unwrap_or(0);
                    if buf.len() >= idx + 4 + content_length {
                        break;
                    }
                }
                if n == 0 {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&buf).into_owned();
            let response = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.expect("write");
            socket.shutdown().await.ok();
            request
        });
        (url, handle)
    }
}
