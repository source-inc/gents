//! Claude subscription OAuth refresh against Anthropic's token endpoint.
//!
//! JSON body (like ChatGPT, unlike xAI's form encoding); `scope` is re-sent on
//! refresh as Claude Code does; refresh-token rotation is optional.

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::claude_oauth::{
    CLAUDE_OAUTH_CLIENT_ID, CLAUDE_OAUTH_SCOPES, CLAUDE_OAUTH_TOKEN_URL,
    CLAUDE_OAUTH_TOKEN_URL_OVERRIDE_ENV,
};
use crate::oauth_credential::{OAuthAuthProblem, RefreshedTokens};

#[derive(Serialize)]
struct RefreshRequest<'a> {
    grant_type: &'static str,
    refresh_token: &'a str,
    client_id: &'static str,
    scope: &'static str,
}

#[derive(Deserialize)]
struct RefreshResponse {
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
}

pub async fn refresh_claude_token(
    refresh_token: &str,
    http: &reqwest::Client,
) -> Result<RefreshedTokens, OAuthAuthProblem> {
    let endpoint = std::env::var(CLAUDE_OAUTH_TOKEN_URL_OVERRIDE_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| CLAUDE_OAUTH_TOKEN_URL.to_string());
    refresh_claude_token_at(&endpoint, refresh_token, http).await
}

pub(crate) async fn refresh_claude_token_at(
    endpoint: &str,
    refresh_token: &str,
    http: &reqwest::Client,
) -> Result<RefreshedTokens, OAuthAuthProblem> {
    let request = RefreshRequest {
        grant_type: "refresh_token",
        refresh_token,
        client_id: CLAUDE_OAUTH_CLIENT_ID,
        scope: CLAUDE_OAUTH_SCOPES,
    };
    let response = http
        .post(endpoint)
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .json(&request)
        .send()
        .await
        .map_err(|error| {
            OAuthAuthProblem::Other(format!(
                "Claude token refresh request failed: {}",
                transport_kind(&error)
            ))
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(OAuthAuthProblem::Expired);
        }
        if status == reqwest::StatusCode::BAD_REQUEST
            && error_code(&body).as_deref() == Some("invalid_grant")
        {
            return Err(OAuthAuthProblem::Expired);
        }
        if status == reqwest::StatusCode::FORBIDDEN {
            return Err(OAuthAuthProblem::NotEntitled);
        }
        return Err(OAuthAuthProblem::Other(format!(
            "Claude token refresh failed with HTTP {status}: {}",
            parse_error_message(&body)
        )));
    }

    let refreshed = response.json::<RefreshResponse>().await.map_err(|error| {
        OAuthAuthProblem::Other(format!("decoding Claude token refresh response: {error}"))
    })?;
    let access_token = refreshed.access_token.ok_or_else(|| {
        OAuthAuthProblem::Other("Claude token refresh response omitted access_token".to_string())
    })?;
    let access_token_expires_at = refreshed
        .expires_in
        .filter(|seconds| *seconds > 0)
        .map(|seconds| Utc::now() + Duration::seconds(seconds))
        .unwrap_or_else(|| Utc::now() + Duration::hours(1));

    Ok(RefreshedTokens {
        access_token,
        refresh_token: refreshed
            .refresh_token
            .unwrap_or_else(|| refresh_token.to_string()),
        id_token: None,
        account_id: None,
        is_fedramp: false,
        plan_type: None,
        access_token_expires_at,
    })
}

fn transport_kind(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timed out"
    } else if error.is_connect() {
        "could not connect"
    } else {
        "failed"
    }
}

fn error_code(body: &str) -> Option<String> {
    serde_json::from_str::<Value>(body)
        .ok()?
        .get("error")?
        .as_str()
        .map(str::to_string)
}

fn parse_error_message(body: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return body.to_string();
    };
    value
        .get("error_description")
        .and_then(Value::as_str)
        .or_else(|| {
            value.get("error").and_then(|error| {
                error
                    .as_str()
                    .or_else(|| error.get("message").and_then(Value::as_str))
            })
        })
        .or_else(|| value.get("message").and_then(Value::as_str))
        .unwrap_or(body)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth_credential::test_support::one_shot_token_server;

    async fn refresh_against(
        status: u16,
        body: &'static str,
    ) -> (Result<RefreshedTokens, OAuthAuthProblem>, String) {
        let (url, handle) = one_shot_token_server(status, body).await;
        let http = reqwest::Client::new();
        let result = refresh_claude_token_at(&url, "refresh-OLD", &http).await;
        (result, handle.await.expect("server"))
    }

    #[tokio::test]
    async fn refresh_posts_json_with_client_id_and_scope() {
        let (result, request) = refresh_against(
            200,
            r#"{"access_token":"access-NEW","refresh_token":"refresh-NEW","expires_in":28800}"#,
        )
        .await;
        let refreshed = result.expect("refreshed");
        assert!(request.starts_with("POST /v1/oauth/token"), "{request}");
        assert!(
            request.contains("content-type: application/json"),
            "{request}"
        );
        let body: serde_json::Value =
            serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
        assert_eq!(body["grant_type"], "refresh_token");
        assert_eq!(body["refresh_token"], "refresh-OLD");
        assert_eq!(
            body["client_id"],
            crate::claude_oauth::CLAUDE_OAUTH_CLIENT_ID
        );
        assert_eq!(body["scope"], crate::claude_oauth::CLAUDE_OAUTH_SCOPES);
        assert_eq!(refreshed.access_token, "access-NEW");
        assert_eq!(refreshed.refresh_token, "refresh-NEW");
        assert!(
            refreshed.access_token_expires_at > chrono::Utc::now() + chrono::Duration::hours(7)
        );
        assert_eq!(refreshed.id_token, None);
    }

    #[tokio::test]
    async fn refresh_keeps_old_refresh_token_when_not_rotated() {
        let (result, _) =
            refresh_against(200, r#"{"access_token":"access-NEW","expires_in":3600}"#).await;
        assert_eq!(result.expect("refreshed").refresh_token, "refresh-OLD");
    }

    #[tokio::test]
    async fn refresh_401_is_expired() {
        let (result, _) = refresh_against(401, r#"{"error":"invalid_token"}"#).await;
        assert_eq!(result.unwrap_err(), OAuthAuthProblem::Expired);
    }

    #[tokio::test]
    async fn refresh_400_invalid_grant_is_expired_but_other_400_is_other() {
        let (result, _) = refresh_against(
            400,
            r#"{"error":"invalid_grant","error_description":"revoked"}"#,
        )
        .await;
        assert_eq!(result.unwrap_err(), OAuthAuthProblem::Expired);
        let (result, _) = refresh_against(400, r#"{"error":"invalid_request"}"#).await;
        assert!(
            matches!(result.unwrap_err(), OAuthAuthProblem::Other(text) if text.contains("400") && text.contains("invalid_request"))
        );
    }

    #[tokio::test]
    async fn refresh_403_is_not_entitled() {
        let (result, _) = refresh_against(403, r#"{"error":"forbidden"}"#).await;
        assert_eq!(result.unwrap_err(), OAuthAuthProblem::NotEntitled);
    }

    #[tokio::test]
    async fn refresh_error_text_never_carries_tokens() {
        let (result, _) = refresh_against(
            500,
            r#"{"error":"server_error","error_description":"boom"}"#,
        )
        .await;
        let text = match result.unwrap_err() {
            OAuthAuthProblem::Other(text) => text,
            other => panic!("{other:?}"),
        };
        assert!(!text.contains("refresh-OLD"), "{text}");
    }
}
