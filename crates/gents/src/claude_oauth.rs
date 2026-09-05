//! Claude subscription OAuth: public client constants, the credential
//! provider name, and operator-facing error copy.
//!
//! The client id, endpoints, and scopes are Claude Code's public OAuth client
//! constants, read from the Claude Code 2.1.260 bundle on 2026-09-04 (see
//! `docs/superpowers/specs/2026-09-04-claude-oauth-credential-parity-design.md`
//! §1). They are not secrets; they can change without notice, which is why
//! they live here and nowhere else.

use std::fmt;

use chrono::{DateTime, Duration, Utc};

use crate::oauth_credential::{
    classify_oauth_auth_error, oauth_credential_id, OAuthAuthProblem, OAuthCredential, OAuthProduct,
};

pub const CLAUDE_OAUTH_PROVIDER: &str = "claude-subscription";
pub const CLAUDE_OAUTH_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
pub const CLAUDE_OAUTH_AUTHORIZE_URL: &str = "https://claude.com/cai/oauth/authorize";
pub const CLAUDE_OAUTH_TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";
pub const CLAUDE_OAUTH_MANUAL_REDIRECT_URL: &str =
    "https://platform.claude.com/oauth/code/callback";
/// The subscription (claude.ai) scope set Claude Code requests. Not the
/// API-key-creation scope.
pub const CLAUDE_OAUTH_SCOPES: &str =
    "user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload";
pub const CLAUDE_OAUTH_TOKEN_URL_OVERRIDE_ENV: &str = "GENTS_CLAUDE_OAUTH_TOKEN_URL";

pub const CLAUDE_OAUTH_PRODUCT: OAuthProduct = OAuthProduct {
    name: "Claude",
    backend_label: "Claude subscription backend",
    login_command: "claude-login",
    not_entitled_guidance:
        "Check that the Claude plan includes Claude Code (Pro/Max), or use an API-key backend.",
};

pub fn normalize_provider(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        CLAUDE_OAUTH_PROVIDER.to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn classify_claude_auth_error(
    agent_did: &str,
    provider: &str,
    problem: &OAuthAuthProblem,
) -> String {
    classify_oauth_auth_error(&CLAUDE_OAUTH_PRODUCT, agent_did, provider, problem)
}

/// Tokens returned by the authorization-code exchange. `expires_in` is seconds.
#[derive(Clone)]
pub struct ClaudeLoginTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: Option<i64>,
    pub scope: Option<String>,
}

impl fmt::Debug for ClaudeLoginTokens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClaudeLoginTokens")
            .field("access_token", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .field("expires_in", &self.expires_in)
            .field("scope", &self.scope)
            .finish()
    }
}

pub fn credential_from_login_tokens(
    agent_did: impl Into<String>,
    provider: impl Into<String>,
    tokens: &ClaudeLoginTokens,
    now: DateTime<Utc>,
) -> OAuthCredential {
    let agent_did = agent_did.into();
    let provider = provider.into();
    let access_token_expires_at = tokens
        .expires_in
        .filter(|seconds| *seconds > 0)
        .map(|seconds| now + Duration::seconds(seconds))
        .unwrap_or_else(|| now + Duration::hours(1));
    OAuthCredential {
        doc_id: None,
        credential_id: oauth_credential_id(&agent_did, &provider),
        agent_did,
        provider,
        access_token: tokens.access_token.clone(),
        refresh_token: tokens.refresh_token.clone(),
        id_token: None,
        account_id: None,
        chatgpt_plan_type: None,
        is_fedramp: false,
        access_token_expires_at,
        last_refresh: Some(now),
        enabled: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth_credential::OAuthAuthProblem;

    #[test]
    fn client_id_is_public_uuid_shape() {
        assert_eq!(CLAUDE_OAUTH_CLIENT_ID.len(), 36);
        assert_eq!(CLAUDE_OAUTH_CLIENT_ID.matches('-').count(), 4);
    }

    #[test]
    fn scopes_are_the_subscription_set() {
        let scopes: Vec<&str> = CLAUDE_OAUTH_SCOPES.split(' ').collect();
        assert_eq!(
            scopes,
            [
                "user:profile",
                "user:inference",
                "user:sessions:claude_code",
                "user:mcp_servers",
                "user:file_upload"
            ]
        );
        assert!(!CLAUDE_OAUTH_SCOPES.contains("org:create_api_key"));
    }

    #[test]
    fn normalize_provider_defaults_and_trims() {
        assert_eq!(normalize_provider(""), CLAUDE_OAUTH_PROVIDER);
        assert_eq!(normalize_provider("  "), CLAUDE_OAUTH_PROVIDER);
        assert_eq!(
            normalize_provider(" claude-subscription "),
            "claude-subscription"
        );
    }

    #[test]
    fn missing_credential_guidance_names_claude_login() {
        let text = classify_claude_auth_error(
            "did:key:z6MkTest",
            CLAUDE_OAUTH_PROVIDER,
            &OAuthAuthProblem::Missing,
        );
        assert!(
            text.contains("gents claude-login --agent-did did:key:z6MkTest"),
            "{text}"
        );
        assert!(text.contains("Claude subscription backend"), "{text}");
    }

    #[test]
    fn credential_from_login_tokens_uses_expires_in_and_fills_claude_shape() {
        let now = chrono::Utc::now();
        let tokens = ClaudeLoginTokens {
            access_token: "access-TEST".into(),
            refresh_token: "refresh-TEST".into(),
            expires_in: Some(28800),
            scope: Some(CLAUDE_OAUTH_SCOPES.into()),
        };
        let credential =
            credential_from_login_tokens("did:key:z6MkTest", CLAUDE_OAUTH_PROVIDER, &tokens, now);
        assert_eq!(
            credential.credential_id,
            "claude-subscription:did:key:z6MkTest"
        );
        assert_eq!(
            credential.access_token_expires_at,
            now + chrono::Duration::seconds(28800)
        );
        assert_eq!(credential.id_token, None);
        assert_eq!(credential.account_id, None);
        assert_eq!(credential.chatgpt_plan_type, None);
        assert!(!credential.is_fedramp);
        assert_eq!(credential.last_refresh, Some(now));
        assert!(credential.enabled);
    }

    #[test]
    fn credential_from_login_tokens_falls_back_to_one_hour() {
        let now = chrono::Utc::now();
        let tokens = ClaudeLoginTokens {
            access_token: "a".into(),
            refresh_token: "r".into(),
            expires_in: None,
            scope: None,
        };
        let credential =
            credential_from_login_tokens("did:key:z6MkTest", CLAUDE_OAUTH_PROVIDER, &tokens, now);
        assert_eq!(
            credential.access_token_expires_at,
            now + chrono::Duration::hours(1)
        );
    }

    #[test]
    fn login_tokens_debug_redacts_both_tokens() {
        let tokens = ClaudeLoginTokens {
            access_token: "access-SECRET".into(),
            refresh_token: "refresh-SECRET".into(),
            expires_in: Some(28800),
            scope: Some("user:profile".into()),
        };
        let text = format!("{tokens:?}");
        assert!(!text.contains("SECRET"), "{text}");
        assert!(text.contains("<redacted>"), "{text}");
        assert!(
            text.contains("28800") && text.contains("user:profile"),
            "{text}"
        );
    }
}
