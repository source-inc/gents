//! First-party Claude subscription OAuth login. Tokens are stored as an
//! `OAuthCredential` document for the agent DID, exactly like `codex-login`
//! and `grok-login`; the `claude` binary is not involved.

use std::io::{self, BufRead, Write};

use anyhow::{Context, Result};
use gents_claude_login::{run_loopback_login, run_manual_login, LoginOptions};
use serde_json::{json, Value};

use crate::cli::args::ClaudeLoginArgs;
use crate::config_writes::ConfigAccess;
use crate::{print_json, resolve_agent_did, resolve_config_access};

pub(crate) struct ClaudeLoginOptions {
    pub(crate) provider: String,
    pub(crate) manual: bool,
    pub(crate) open_browser: bool,
    pub(crate) client_id: Option<String>,
    pub(crate) token_url: Option<String>,
}

pub(crate) struct ClaudeLoginOutcome {
    pub(crate) doc_id: String,
    pub(crate) credential: gents::oauth_credential::OAuthCredential,
}

pub(crate) async fn claude_login(args: ClaudeLoginArgs) -> Result<()> {
    let (access, home_dir) =
        resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;
    let agent_did = resolve_agent_did(Some(&home_dir), args.agent_did.as_deref())?;
    let outcome = run_claude_login(
        &access,
        &agent_did,
        &ClaudeLoginOptions {
            provider: args.provider,
            manual: args.manual,
            open_browser: !args.no_browser,
            client_id: args.client_id,
            token_url: args.token_url,
        },
    )
    .await?;
    print_json(&claude_login_result_json(&outcome))?;
    Ok(())
}

pub(crate) async fn run_claude_login(
    access: &ConfigAccess,
    agent_did: &str,
    opts: &ClaudeLoginOptions,
) -> Result<ClaudeLoginOutcome> {
    let provider = gents::claude_oauth::normalize_provider(&opts.provider);
    let mut login_options = LoginOptions {
        open_browser: opts.open_browser,
        ..LoginOptions::default()
    };
    if let Some(client_id) = opts
        .client_id
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        login_options.client_id = client_id.to_string();
    }
    if let Some(token_url) = opts
        .token_url
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        login_options.token_url = token_url.to_string();
    }

    let tokens = if opts.manual {
        run_manual_login(login_options, |auth_url| {
            eprintln!(
                "Open this URL to sign in with Claude:\n{auth_url}\n\nPaste the code shown on the success page (code#state) and press Enter:"
            );
            io::stderr().flush()?;
            let mut line = String::new();
            io::stdin().lock().read_line(&mut line)?;
            Ok(line)
        })
        .await
        .context("Claude manual login failed")?
    } else {
        let server =
            run_loopback_login(login_options).context("starting Claude login callback server")?;
        eprintln!("Open this URL to sign in with Claude:\n{}", server.auth_url);
        server
            .block_until_done()
            .await
            .context("Claude browser login failed")?
    };

    let login_tokens = gents::claude_oauth::ClaudeLoginTokens {
        access_token: tokens.access_token,
        refresh_token: tokens.refresh_token,
        expires_in: tokens.expires_in,
        scope: tokens.scope,
    };
    let credential = gents::claude_oauth::credential_from_login_tokens(
        agent_did,
        &provider,
        &login_tokens,
        chrono::Utc::now(),
    );
    let mutation = gents::oauth_credential::oauth_credential_upsert_mutation(&credential);
    let response = access.execute(&mutation).await?;
    let doc_id = gents_protocol::graphql::extract_mutation_doc_id(&response, "OAuthCredential")?;
    Ok(ClaudeLoginOutcome { doc_id, credential })
}

pub(crate) fn claude_login_result_json(outcome: &ClaudeLoginOutcome) -> Value {
    let credential = &outcome.credential;
    json!({
        "login": "completed",
        "doc_id": outcome.doc_id,
        "credential_id": credential.credential_id,
        "agent_did": credential.agent_did,
        "provider": credential.provider,
        "access_token_expires_at": credential.access_token_expires_at,
        "last_refresh": credential.last_refresh,
        "enabled": credential.enabled,
        "access_token": "<redacted>",
        "refresh_token": "<redacted>",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_and_runtime_constants_match() {
        assert_eq!(
            gents_claude_login::CLIENT_ID,
            gents::claude_oauth::CLAUDE_OAUTH_CLIENT_ID
        );
        assert_eq!(
            gents_claude_login::AUTHORIZE_URL,
            gents::claude_oauth::CLAUDE_OAUTH_AUTHORIZE_URL
        );
        assert_eq!(
            gents_claude_login::TOKEN_URL,
            gents::claude_oauth::CLAUDE_OAUTH_TOKEN_URL
        );
        assert_eq!(
            gents_claude_login::MANUAL_REDIRECT_URL,
            gents::claude_oauth::CLAUDE_OAUTH_MANUAL_REDIRECT_URL
        );
        assert_eq!(
            gents_claude_login::SCOPES,
            gents::claude_oauth::CLAUDE_OAUTH_SCOPES
        );
    }

    #[test]
    fn result_json_redacts_tokens() {
        let credential = gents::claude_oauth::credential_from_login_tokens(
            "did:key:z6MkTest",
            "claude-subscription",
            &gents::claude_oauth::ClaudeLoginTokens {
                access_token: "access-SECRET".into(),
                refresh_token: "refresh-SECRET".into(),
                expires_in: Some(60),
                scope: None,
            },
            chrono::Utc::now(),
        );
        let json = claude_login_result_json(&ClaudeLoginOutcome {
            doc_id: "bae-1".into(),
            credential,
        });
        let text = json.to_string();
        assert!(!text.contains("SECRET"), "{text}");
        assert_eq!(json["access_token"], "<redacted>");
        assert_eq!(json["login"], "completed");
    }
}
