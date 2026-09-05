//! Provider-agnostic OAuth credential documents and owner-only bearer refresh.
//!
//! ChatGPT Codex and Grok / xAI subscription OAuth both store tokens as
//! `OAuthCredential` rows and share this cache + refresh-lock shell. Provider
//! differences live in [`OAuthRefreshKind`] and product-specific HTTP clients.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use defra_node::EmbeddedNode;
use gents_protocol::row::OAuthCredentialRow;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

const OAUTH_CREDENTIAL_FIELDS: &str = "_docID credential_id agent_did provider access_token refresh_token id_token account_id chatgpt_plan_type is_fedramp access_token_expires_at last_refresh enabled";
const REFRESH_SKEW: Duration = Duration::minutes(5);
/// After a failed refresh the bearer serves that failure again for this long
/// instead of POSTing the provider's token endpoint on every request. While
/// the cooldown holds every call returns the classified error: no provider
/// round-trip and no fast-path token. A re-login is picked up once the
/// cooldown lapses, because the forced slow path re-reads the document first.
const REFRESH_FAILURE_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(60);

/// Product copy used when classifying auth failures for operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OAuthProduct {
    pub name: &'static str,
    pub backend_label: &'static str,
    pub login_command: &'static str,
    /// What to do about a valid-grant-but-tier-gated account (HTTP 403).
    pub not_entitled_guidance: &'static str,
}

pub const CHATGPT_OAUTH_PRODUCT: OAuthProduct = OAuthProduct {
    name: "ChatGPT",
    backend_label: "ChatGPT subscription backend",
    login_command: "codex-login",
    not_entitled_guidance: "Use an API-key backend, or check the ChatGPT plan's Codex eligibility.",
};

pub const XAI_OAUTH_PRODUCT: OAuthProduct = OAuthProduct {
    name: "Grok",
    backend_label: "Grok subscription backend",
    login_command: "grok-login",
    not_entitled_guidance: "Use an API key with an OpenAI-compatible backend against \
                            https://api.x.ai/v1, or check SuperGrok / X Premium+ eligibility.",
};

/// Which token endpoint / claim mapping to use when rotating credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthRefreshKind {
    ChatGpt,
    Claude,
    Xai,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OAuthAuthProblem {
    Missing,
    WrongMode {
        found_mode: String,
    },
    Expired,
    /// Valid grant, but the account is not entitled to this OAuth surface (tier gate).
    NotEntitled,
    Other(String),
}

pub fn classify_oauth_auth_error(
    product: &OAuthProduct,
    agent_did: &str,
    provider: &str,
    problem: &OAuthAuthProblem,
) -> String {
    match problem {
        OAuthAuthProblem::Missing => format!(
            "No OAuthCredential document found for agent {agent_did} and provider {provider}.\n\
             To use the {backend}, run \
             `gents {login} --agent-did {agent_did}`.",
            backend = product.backend_label,
            login = product.login_command,
        ),
        OAuthAuthProblem::WrongMode { found_mode } => format!(
            "OAuthCredential for agent {agent_did} and provider {provider} is {found_mode}, \
             but the {backend} needs an enabled {name} OAuth credential.\n\
             Run `gents {login} --agent-did {agent_did}` or select an API-key backend.",
            backend = product.backend_label,
            name = product.name,
            login = product.login_command,
        ),
        OAuthAuthProblem::Expired => format!(
            "{name} OAuth credential for agent {agent_did} and provider {provider} is expired or revoked.\n\
             Re-authenticate with `gents {login} --agent-did {agent_did}`.",
            name = product.name,
            login = product.login_command,
        ),
        OAuthAuthProblem::NotEntitled => format!(
            "{name} OAuth credential for agent {agent_did} and provider {provider} is valid, \
             but this account is not entitled to subscription OAuth inference (HTTP 403 tier gate).\n\
             Re-login will not fix this. {guidance}",
            name = product.name,
            guidance = product.not_entitled_guidance,
        ),
        OAuthAuthProblem::Other(detail) => {
            format!(
                "{name} OAuth credential for agent {agent_did} and provider {provider} could not be used: {detail}",
                name = product.name,
            )
        }
    }
}

pub fn classify_chatgpt_auth_error(
    agent_did: &str,
    provider: &str,
    problem: &OAuthAuthProblem,
) -> String {
    classify_oauth_auth_error(&CHATGPT_OAUTH_PRODUCT, agent_did, provider, problem)
}

/// Single owner of the OAuth access-token expiry fallback chain xAI's
/// device-code login and refresh both used to duplicate: prefer the JWT's own
/// `exp` claim (authoritative when present), fall back to a positive
/// `expires_in` (seconds) added to `now`, and default to a conservative
/// 15-minute window when neither is present or valid.
pub fn resolve_access_token_expiry(
    access_token: &str,
    expires_in: Option<i64>,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    crate::chatgpt_oauth_refresh::jwt_expiration(access_token)
        .or_else(|| {
            expires_in
                .filter(|seconds| *seconds > 0)
                .map(|seconds| now + Duration::seconds(seconds))
        })
        .unwrap_or_else(|| now + Duration::minutes(15))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshedTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: Option<String>,
    pub account_id: Option<String>,
    pub is_fedramp: bool,
    pub plan_type: Option<String>,
    pub access_token_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthCredential {
    #[serde(default)]
    pub doc_id: Option<String>,
    pub credential_id: String,
    pub agent_did: String,
    pub provider: String,
    pub access_token: String,
    pub refresh_token: String,
    #[serde(default)]
    pub id_token: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub chatgpt_plan_type: Option<String>,
    pub is_fedramp: bool,
    pub access_token_expires_at: DateTime<Utc>,
    #[serde(default)]
    pub last_refresh: Option<DateTime<Utc>>,
    pub enabled: bool,
}

pub fn oauth_credential_id(agent_did: &str, provider: &str) -> String {
    format!("{provider}:{agent_did}")
}

pub fn oauth_credential_query(agent_did: &str, provider: &str) -> String {
    let agent_did = crate::graphql::escape_graphql_string(agent_did);
    let provider = crate::graphql::escape_graphql_string(provider);
    format!(
        r#"query {{
            OAuthCredential(
                filter: {{
                    agent_did: {{ _eq: "{agent_did}" }},
                    provider: {{ _eq: "{provider}" }},
                    enabled: {{ _eq: true }}
                }},
                limit: 1
            ) {{
                {OAUTH_CREDENTIAL_FIELDS}
            }}
        }}"#
    )
}

pub fn oauth_credential_by_id_query(credential_id: &str) -> String {
    let credential_id = crate::graphql::escape_graphql_string(credential_id);
    format!(
        r#"query {{
            OAuthCredential(
                filter: {{ credential_id: {{ _eq: "{credential_id}" }} }},
                limit: 1
            ) {{
                {OAUTH_CREDENTIAL_FIELDS}
            }}
        }}"#
    )
}

pub fn oauth_credential_upsert_mutation(credential: &OAuthCredential) -> String {
    let fields = oauth_credential_input_fields(credential);
    let add_input = render_oauth_input(&fields, &[]);
    // `agent_did` is `@immutable` and `credential_id` is the unique key. On this DefraDB pin the
    // immutability check rejects re-sending an immutable field on a pre-existing document, so the
    // `update` branch (re-login and per-request token rotation both land here) must omit it.
    // `credential_id` is likewise only ever written in `add`. Mirrors session/conversation.rs.
    let update_input = render_oauth_input(&fields, &["agent_did"]);
    let credential_id = crate::graphql::escape_graphql_string(&credential.credential_id);
    format!(
        r#"mutation {{
            upsert_OAuthCredential(
                filter: {{ credential_id: {{ _eq: "{credential_id}" }} }},
                add: {{
                    credential_id: "{credential_id}",
                    {add_input}
                }},
                update: {{
                    {update_input}
                }}
            ) {{ _docID }}
        }}"#
    )
}

pub async fn lookup_oauth_credential(
    node: &EmbeddedNode,
    agent_did: &str,
    provider: &str,
) -> Result<Option<OAuthCredential>> {
    let response = node
        .execute(&oauth_credential_query(agent_did, provider))
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "querying OAuthCredential returned errors: {:?}",
            response.errors
        );
    }
    let response = json!({ "data": response.data.unwrap_or(Value::Null) });
    oauth_credentials_from_response(&response)
        .into_iter()
        .next()
        .transpose()
}

pub async fn lookup_oauth_credential_by_id(
    node: &EmbeddedNode,
    credential_id: &str,
) -> Result<Option<OAuthCredential>> {
    let response = node
        .execute(&oauth_credential_by_id_query(credential_id))
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "querying OAuthCredential returned errors: {:?}",
            response.errors
        );
    }
    let response = json!({ "data": response.data.unwrap_or(Value::Null) });
    oauth_credentials_from_response(&response)
        .into_iter()
        .next()
        .transpose()
}

pub fn oauth_credentials_for_agent_query(agent_did: &str) -> String {
    let agent_did = crate::graphql::escape_graphql_string(agent_did);
    format!(
        r#"query {{
            OAuthCredential(filter: {{ agent_did: {{ _eq: "{agent_did}" }} }}) {{
                {OAUTH_CREDENTIAL_FIELDS}
            }}
        }}"#
    )
}

pub fn oauth_credential_by_doc_id_query(doc_id: &str) -> String {
    let doc_id = crate::graphql::escape_graphql_string(doc_id);
    format!(
        r#"query {{
            OAuthCredential(filter: {{ _docID: {{ _eq: "{doc_id}" }} }}, limit: 1) {{
                {OAUTH_CREDENTIAL_FIELDS}
            }}
        }}"#
    )
}

pub async fn list_oauth_credentials(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<Vec<OAuthCredential>> {
    let response = node
        .execute(&oauth_credentials_for_agent_query(agent_did))
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "querying OAuthCredential returned errors: {:?}",
            response.errors
        );
    }
    let response = json!({ "data": response.data.unwrap_or(Value::Null) });
    oauth_credentials_from_response(&response)
        .into_iter()
        .collect()
}

pub async fn lookup_oauth_credential_by_doc_id(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<Option<OAuthCredential>> {
    let response = node
        .execute(&oauth_credential_by_doc_id_query(doc_id))
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "querying OAuthCredential returned errors: {:?}",
            response.errors
        );
    }
    let response = json!({ "data": response.data.unwrap_or(Value::Null) });
    oauth_credentials_from_response(&response)
        .into_iter()
        .next()
        .transpose()
}

pub async fn upsert_oauth_credential(
    node: &EmbeddedNode,
    credential: &OAuthCredential,
) -> Result<String> {
    let response = node
        .execute(&oauth_credential_upsert_mutation(credential))
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "upserting OAuthCredential returned errors: {:?}",
            response.errors
        );
    }
    let response = json!({ "data": response.data.unwrap_or(Value::Null) });
    gents_protocol::graphql::extract_mutation_doc_id(&response, "OAuthCredential")
}

pub fn oauth_credentials_from_response(response: &Value) -> Vec<Result<OAuthCredential>> {
    gents_protocol::graphql::graphql_rows_from_response(response, "OAuthCredential")
        .into_iter()
        .map(oauth_credential_from_value)
        .collect()
}

fn oauth_credential_input_fields(credential: &OAuthCredential) -> Vec<(&'static str, String)> {
    let field = |name: &str, value: &str| {
        format!(
            r#"{name}: "{}""#,
            crate::graphql::escape_graphql_string(value)
        )
    };
    let datetime_field = |name: &str, value: Option<DateTime<Utc>>| {
        value
            .map(|value| {
                format!(
                    r#"{name}: "{}""#,
                    value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                )
            })
            .unwrap_or_else(|| format!("{name}: null"))
    };
    vec![
        ("agent_did", field("agent_did", &credential.agent_did)),
        ("provider", field("provider", &credential.provider)),
        (
            "access_token",
            field("access_token", &credential.access_token),
        ),
        (
            "refresh_token",
            field("refresh_token", &credential.refresh_token),
        ),
        (
            "id_token",
            gents_protocol::graphql::nullable_string_field(
                "id_token",
                credential.id_token.as_deref(),
            ),
        ),
        (
            "account_id",
            gents_protocol::graphql::nullable_string_field(
                "account_id",
                credential.account_id.as_deref(),
            ),
        ),
        (
            "chatgpt_plan_type",
            gents_protocol::graphql::nullable_string_field(
                "chatgpt_plan_type",
                credential.chatgpt_plan_type.as_deref(),
            ),
        ),
        (
            "is_fedramp",
            format!(
                "is_fedramp: {}",
                gents_protocol::graphql::graphql_bool_literal(credential.is_fedramp)
            ),
        ),
        (
            "access_token_expires_at",
            datetime_field(
                "access_token_expires_at",
                Some(credential.access_token_expires_at),
            ),
        ),
        (
            "last_refresh",
            datetime_field("last_refresh", credential.last_refresh),
        ),
        (
            "enabled",
            format!(
                "enabled: {}",
                gents_protocol::graphql::graphql_bool_literal(credential.enabled)
            ),
        ),
    ]
}

fn render_oauth_input(fields: &[(&'static str, String)], exclude: &[&str]) -> String {
    fields
        .iter()
        .filter(|(name, _)| !exclude.contains(name))
        .map(|(_, rendered)| rendered.as_str())
        .collect::<Vec<_>>()
        .join(",\n                    ")
}

pub(crate) fn oauth_credential_from_value(value: Value) -> Result<OAuthCredential> {
    let row: OAuthCredentialRow =
        serde_json::from_value(value).context("decoding OAuthCredential row")?;
    let access_token = required(row.access_token, "access_token")?;
    let refresh_token = required(row.refresh_token, "refresh_token")?;
    Ok(OAuthCredential {
        doc_id: row.doc_id,
        credential_id: row.credential_id,
        agent_did: required(row.agent_did, "agent_did")?,
        provider: required(row.provider, "provider")?,
        access_token,
        refresh_token,
        id_token: clean_optional(row.id_token),
        account_id: clean_optional(row.account_id),
        chatgpt_plan_type: clean_optional(row.chatgpt_plan_type),
        is_fedramp: row.is_fedramp.unwrap_or(false),
        access_token_expires_at: parse_required_datetime(
            row.access_token_expires_at,
            "access_token_expires_at",
        )?,
        last_refresh: parse_optional_datetime(row.last_refresh, "last_refresh")?,
        enabled: row.enabled.unwrap_or(true),
    })
}

fn required(value: Option<String>, field: &str) -> Result<String> {
    clean_optional(value).ok_or_else(|| anyhow::anyhow!("OAuthCredential missing {field}"))
}

fn clean_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(value)
    })
}

fn parse_required_datetime(value: Option<String>, field: &str) -> Result<DateTime<Utc>> {
    parse_optional_datetime(value, field)?
        .ok_or_else(|| anyhow::anyhow!("OAuthCredential missing {field}"))
}

fn parse_optional_datetime(value: Option<String>, field: &str) -> Result<Option<DateTime<Utc>>> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(&value)
                .map(|value| value.with_timezone(&Utc))
                .with_context(|| format!("parsing OAuthCredential {field} timestamp {value}"))
        })
        .transpose()
}

pub fn token_is_fresh(expires_at: DateTime<Utc>) -> bool {
    Utc::now() + REFRESH_SKEW < expires_at
}

pub fn apply_refreshed_tokens(credential: &mut OAuthCredential, refreshed: RefreshedTokens) {
    credential.access_token = refreshed.access_token;
    credential.refresh_token = refreshed.refresh_token;
    if refreshed.id_token.is_some() {
        credential.id_token = refreshed.id_token;
    }
    if refreshed.account_id.is_some() {
        credential.account_id = refreshed.account_id;
    }
    if refreshed.plan_type.is_some() {
        credential.chatgpt_plan_type = refreshed.plan_type;
    }
    credential.is_fedramp = refreshed.is_fedramp || credential.is_fedramp;
    credential.access_token_expires_at = refreshed.access_token_expires_at;
    credential.last_refresh = Some(Utc::now());
}

pub(crate) fn get_or_insert_arc<T>(
    registry: &std::sync::Mutex<std::collections::HashMap<String, Arc<T>>>,
    key: &str,
    make: impl FnOnce() -> T,
) -> Arc<T> {
    let mut map = registry.lock().expect("bearer registry mutex poisoned");
    map.entry(key.to_string())
        .or_insert_with(|| Arc::new(make()))
        .clone()
}

fn bearer_registry(
) -> &'static std::sync::Mutex<std::collections::HashMap<String, Arc<DbCredentialBearer>>> {
    static REGISTRY: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, Arc<DbCredentialBearer>>>,
    > = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

pub fn shared_bearer(
    credential_id: &str,
    make: impl FnOnce() -> DbCredentialBearer,
) -> Arc<DbCredentialBearer> {
    get_or_insert_arc(bearer_registry(), credential_id, make)
}

pub trait BearerSource: Send + Sync {
    fn current_bearer(&self) -> impl Future<Output = Result<String>> + Send;

    fn invalidate(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

pub struct DbCredentialBearer {
    node: Arc<EmbeddedNode>,
    agent_did: String,
    provider: String,
    credential_id: String,
    http: reqwest::Client,
    cache: Mutex<Option<OAuthCredential>>,
    /// Single-flight refresh lock. It also holds the last refresh failure, so
    /// the cooldown check and the refresh share one critical section.
    refresh_lock: Mutex<Option<(std::time::Instant, OAuthAuthProblem)>>,
    is_owner: bool,
    force_refresh: AtomicBool,
    refresh_kind: OAuthRefreshKind,
    product: OAuthProduct,
}

impl DbCredentialBearer {
    pub fn new(
        node: Arc<EmbeddedNode>,
        agent_did: impl Into<String>,
        provider: impl Into<String>,
        credential_id: impl Into<String>,
        is_owner: bool,
        refresh_kind: OAuthRefreshKind,
        product: OAuthProduct,
    ) -> Self {
        Self::with_cache(
            node,
            agent_did,
            provider,
            credential_id,
            is_owner,
            None,
            refresh_kind,
            product,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_cache(
        node: Arc<EmbeddedNode>,
        agent_did: impl Into<String>,
        provider: impl Into<String>,
        credential_id: impl Into<String>,
        is_owner: bool,
        cache_seed: Option<OAuthCredential>,
        refresh_kind: OAuthRefreshKind,
        product: OAuthProduct,
    ) -> Self {
        Self {
            node,
            agent_did: agent_did.into(),
            provider: provider.into(),
            credential_id: credential_id.into(),
            http: reqwest::Client::new(),
            cache: Mutex::new(cache_seed),
            refresh_lock: Mutex::new(None),
            is_owner,
            force_refresh: AtomicBool::new(false),
            refresh_kind,
            product,
        }
    }

    async fn load_credential(&self) -> Result<OAuthCredential> {
        let credential = lookup_oauth_credential_by_id(self.node.as_ref(), &self.credential_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(classify_oauth_auth_error(
                    &self.product,
                    &self.agent_did,
                    &self.provider,
                    &OAuthAuthProblem::Missing,
                ))
            })?;
        if !credential.enabled {
            anyhow::bail!(
                "{}",
                classify_oauth_auth_error(
                    &self.product,
                    &self.agent_did,
                    &self.provider,
                    &OAuthAuthProblem::WrongMode {
                        found_mode: "disabled".to_string(),
                    },
                )
            );
        }
        Ok(credential)
    }

    async fn cached(&self) -> Option<OAuthCredential> {
        self.cache.lock().await.clone()
    }

    async fn cache_credential(&self, credential: &OAuthCredential) {
        *self.cache.lock().await = Some(credential.clone());
    }

    async fn persist_with_retry(&self, credential: &OAuthCredential) -> Result<()> {
        let mut last_error = None;
        let mut delay_ms = 200u64;
        for attempt in 0..3u32 {
            match upsert_oauth_credential(self.node.as_ref(), credential).await {
                Ok(_) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    if attempt + 1 < 3 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        delay_ms *= 2;
                    }
                }
            }
        }
        Err(last_error.expect("persist_with_retry ran at least one failing attempt"))
    }

    fn auth_error(&self, problem: &OAuthAuthProblem) -> anyhow::Error {
        anyhow::anyhow!(classify_oauth_auth_error(
            &self.product,
            &self.agent_did,
            &self.provider,
            problem,
        ))
    }

    async fn refresh_tokens(
        &self,
        refresh_token: &str,
    ) -> Result<RefreshedTokens, OAuthAuthProblem> {
        match self.refresh_kind {
            OAuthRefreshKind::ChatGpt => {
                crate::chatgpt_oauth_refresh::refresh_chatgpt_token(refresh_token, &self.http).await
            }
            OAuthRefreshKind::Claude => {
                crate::claude_oauth_refresh::refresh_claude_token(refresh_token, &self.http).await
            }
            OAuthRefreshKind::Xai => {
                crate::xai_oauth_refresh::refresh_xai_token(refresh_token, &self.http).await
            }
        }
    }
}

impl BearerSource for DbCredentialBearer {
    async fn current_bearer(&self) -> Result<String> {
        let forced = self.force_refresh.load(Ordering::SeqCst);

        if !forced {
            if let Some(cred) = self.cached().await {
                if token_is_fresh(cred.access_token_expires_at) {
                    return Ok(cred.access_token);
                }
            }
        }

        let mut last_failure = self.refresh_lock.lock().await;

        let forced = self.force_refresh.load(Ordering::SeqCst);

        if !forced {
            if let Some(cred) = self.cached().await {
                if token_is_fresh(cred.access_token_expires_at) {
                    return Ok(cred.access_token);
                }
            }
        }

        let mut credential = match self.cached().await {
            Some(cred) => cred,
            None => {
                let cred = self.load_credential().await?;
                self.cache_credential(&cred).await;
                cred
            }
        };
        if !forced && token_is_fresh(credential.access_token_expires_at) {
            return Ok(credential.access_token);
        }

        if !self.is_owner {
            self.force_refresh.store(false, Ordering::SeqCst);
            return Ok(credential.access_token);
        }

        let db_credential = self.load_credential().await?;
        if db_credential.access_token_expires_at >= credential.access_token_expires_at {
            credential = db_credential;
            if !forced && token_is_fresh(credential.access_token_expires_at) {
                self.cache_credential(&credential).await;
                return Ok(credential.access_token);
            }
        }

        if let Some((failed_at, problem)) = last_failure.as_ref() {
            if failed_at.elapsed() < REFRESH_FAILURE_COOLDOWN {
                return Err(self.auth_error(problem));
            }
        }

        let refreshed = match self.refresh_tokens(&credential.refresh_token).await {
            Ok(refreshed) => refreshed,
            Err(problem) => {
                let error = self.auth_error(&problem);
                *last_failure = Some((std::time::Instant::now(), problem));
                return Err(error);
            }
        };
        *last_failure = None;
        apply_refreshed_tokens(&mut credential, refreshed);

        self.cache_credential(&credential).await;
        self.force_refresh.store(false, Ordering::SeqCst);
        if let Err(error) = self.persist_with_retry(&credential).await {
            tracing::error!(
                agent_did = %self.agent_did,
                credential_id = %self.credential_id,
                product = self.product.name,
                %error,
                "failed to persist rotated OAuth token to DefraDB after retries; serving \
                 the rotated token from memory. It must be re-persisted before this process exits \
                 or the rotated refresh token will be lost."
            );
        }
        Ok(credential.access_token)
    }

    async fn invalidate(&self) {
        self.force_refresh.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicUsize;

    fn sample_credential() -> OAuthCredential {
        OAuthCredential {
            doc_id: Some("doc-1".to_string()),
            credential_id: oauth_credential_id("did:key:zAgent", "chatgpt-codex"),
            agent_did: "did:key:zAgent".to_string(),
            provider: "chatgpt-codex".to_string(),
            access_token: "access-tok".to_string(),
            refresh_token: "refresh-tok".to_string(),
            id_token: Some("id-tok".to_string()),
            account_id: Some("acct-1".to_string()),
            chatgpt_plan_type: Some("pro".to_string()),
            is_fedramp: false,
            access_token_expires_at: DateTime::<Utc>::from_timestamp(1_900_000_000, 0).unwrap(),
            last_refresh: Some(DateTime::<Utc>::from_timestamp(1_800_000_000, 0).unwrap()),
            enabled: true,
        }
    }

    #[test]
    fn shared_registry_returns_one_arc_per_key_and_constructs_once() {
        use std::collections::HashMap;
        use std::sync::Mutex as StdMutex;

        let registry: StdMutex<HashMap<String, Arc<u32>>> = StdMutex::new(HashMap::new());
        let calls = AtomicUsize::new(0);

        let a = get_or_insert_arc(&registry, "k1", || {
            calls.fetch_add(1, Ordering::SeqCst);
            7u32
        });
        let b = get_or_insert_arc(&registry, "k1", || {
            calls.fetch_add(1, Ordering::SeqCst);
            99u32
        });
        let c = get_or_insert_arc(&registry, "k2", || {
            calls.fetch_add(1, Ordering::SeqCst);
            7u32
        });

        assert!(Arc::ptr_eq(&a, &b), "same key must share one Arc");
        assert!(!Arc::ptr_eq(&a, &c), "different keys must be distinct Arcs");
        assert_eq!(*a, 7, "first construction wins for a key");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "construct exactly once per distinct key"
        );
    }

    #[test]
    fn token_is_fresh_respects_refresh_skew() {
        assert!(
            token_is_fresh(Utc::now() + Duration::hours(1)),
            "comfortably-future token is fresh"
        );
        assert!(
            !token_is_fresh(Utc::now() - Duration::minutes(1)),
            "expired token is stale"
        );
        assert!(
            !token_is_fresh(Utc::now() + Duration::minutes(2)),
            "token inside the 5-minute skew window is treated as stale"
        );
    }

    #[test]
    fn apply_refreshed_tokens_preserves_omitted_optionals_and_ors_fedramp() {
        let mut credential = sample_credential();
        credential.is_fedramp = true;
        let prior_id = credential.id_token.clone();

        apply_refreshed_tokens(
            &mut credential,
            RefreshedTokens {
                access_token: "new-access".to_string(),
                refresh_token: "new-refresh".to_string(),
                id_token: None,
                account_id: None,
                is_fedramp: false,
                plan_type: None,
                access_token_expires_at: DateTime::<Utc>::from_timestamp(2_000_000_000, 0).unwrap(),
            },
        );

        assert_eq!(credential.access_token, "new-access");
        assert_eq!(credential.refresh_token, "new-refresh");
        assert_eq!(credential.id_token, prior_id, "omitted id_token preserved");
        assert_eq!(
            credential.account_id.as_deref(),
            Some("acct-1"),
            "omitted account_id preserved"
        );
        assert_eq!(
            credential.chatgpt_plan_type.as_deref(),
            Some("pro"),
            "omitted plan preserved"
        );
        assert!(credential.is_fedramp, "is_fedramp stays true via OR");
        assert_eq!(
            credential.access_token_expires_at.timestamp(),
            2_000_000_000
        );
        assert!(credential.last_refresh.is_some());
    }

    #[test]
    fn oauth_credential_from_value_applies_defaults_and_cleans_blanks() {
        let row = json!({
            "_docID": "doc-9",
            "credential_id": "chatgpt-codex:did:key:zA",
            "agent_did": "did:key:zA",
            "provider": "chatgpt-codex",
            "access_token": "acc",
            "refresh_token": "ref",
            "id_token": null,
            "account_id": "",
            "chatgpt_plan_type": null,
            "access_token_expires_at": "2030-01-01T00:00:00Z",
            "last_refresh": null,
        });

        let credential = oauth_credential_from_value(row).expect("row parses");

        assert_eq!(credential.doc_id.as_deref(), Some("doc-9"));
        assert_eq!(credential.access_token, "acc");
        assert_eq!(credential.id_token, None, "explicit null -> None");
        assert_eq!(credential.account_id, None, "blank string cleaned to None");
        assert!(!credential.is_fedramp, "missing is_fedramp defaults false");
        assert!(credential.enabled, "missing enabled defaults true");
        assert_eq!(credential.last_refresh, None);
        assert_eq!(
            credential.access_token_expires_at,
            DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        );
    }

    #[test]
    fn upsert_update_block_omits_immutable_agent_did() {
        let mutation = oauth_credential_upsert_mutation(&sample_credential());

        let update_idx = mutation
            .find("update:")
            .expect("mutation has an update block");
        let (add_block, update_block) = mutation.split_at(update_idx);

        assert!(
            !update_block.contains("agent_did:"),
            "update block must omit immutable agent_did: {update_block}"
        );
        assert!(
            update_block.contains("access_token:") && update_block.contains("refresh_token:"),
            "update block must still rotate token fields: {update_block}"
        );
        assert!(
            add_block.contains("agent_did:") && add_block.contains("credential_id:"),
            "add block must set agent_did and credential_id: {add_block}"
        );
    }

    #[test]
    fn not_entitled_copy_is_product_specific() {
        let msg = classify_oauth_auth_error(
            &CHATGPT_OAUTH_PRODUCT,
            "did:key:zAgent",
            "chatgpt-codex",
            &OAuthAuthProblem::NotEntitled,
        );
        assert!(
            !msg.contains("api.x.ai") && !msg.contains("SuperGrok"),
            "ChatGPT tier-gate copy must not carry xAI guidance: {msg}"
        );
        assert!(msg.contains("ChatGPT"), "{msg}");
    }

    #[test]
    fn classifies_not_entitled_without_relogin_guidance() {
        let msg = classify_oauth_auth_error(
            &XAI_OAUTH_PRODUCT,
            "did:key:zAgent",
            "xai-oauth",
            &OAuthAuthProblem::NotEntitled,
        );
        assert!(msg.contains("not entitled"), "{msg}");
        assert!(
            !msg.contains("gents grok-login"),
            "tier gate should not push re-login as the fix: {msg}"
        );
        assert!(msg.contains("api.x.ai"), "{msg}");
    }
}

#[cfg(test)]
mod cooldown_tests {
    use super::test_support::{one_shot_token_server, seed_credential, test_node};
    use super::*;

    /// A failed refresh is served from the cooldown on the next call instead
    /// of POSTing the token endpoint again. The one-shot server accepts one
    /// request and is gone before the second call, so a second POST would
    /// surface as a transport error rather than the cached "expired or
    /// revoked" text.
    #[tokio::test]
    async fn failed_refresh_is_not_retried_during_the_cooldown() {
        let node = Arc::new(test_node().await);
        let did = "did:key:z6MkRevoked";
        let provider = crate::xai_grok_oauth::XAI_OAUTH_PROVIDER;
        seed_credential(&node, did, provider, Utc::now() - Duration::minutes(1)).await;
        let (url, handle) = one_shot_token_server(401, r#"{"error":"invalid_grant"}"#).await;
        // Process-global; no other lib test refreshes an xAI credential.
        std::env::set_var(
            crate::xai_oauth_refresh::XAI_OAUTH_TOKEN_URL_OVERRIDE_ENV,
            &url,
        );
        let bearer = DbCredentialBearer::new(
            node,
            did,
            provider,
            oauth_credential_id(did, provider),
            true,
            OAuthRefreshKind::Xai,
            XAI_OAUTH_PRODUCT,
        );
        let first = bearer.current_bearer().await.expect_err("revoked");
        let request = handle.await.expect("server");
        let second = bearer.current_bearer().await.expect_err("cooldown");
        std::env::remove_var(crate::xai_oauth_refresh::XAI_OAUTH_TOKEN_URL_OVERRIDE_ENV);

        assert!(request.contains("grant_type=refresh_token"), "{request}");
        assert!(
            first.to_string().contains("is expired or revoked"),
            "{first}"
        );
        assert_eq!(first.to_string(), second.to_string());
    }

    /// After `invalidate()` (a provider 401) a failed refresh must keep the
    /// bearer forced: while the cooldown holds every call returns the
    /// classified error instead of taking the cache fast path and re-serving
    /// the unexpired but rejected access token.
    #[tokio::test]
    async fn failed_refresh_keeps_the_bearer_forced_and_never_reserves_the_bad_token() {
        let node = Arc::new(test_node().await);
        let did = "did:key:z6MkRejected";
        let provider = crate::chatgpt_codex::CHATGPT_CODEX_PROVIDER;
        seed_credential(&node, did, provider, Utc::now() + Duration::hours(1)).await;
        let (url, handle) = one_shot_token_server(401, r#"{"error":"invalid_grant"}"#).await;
        // Process-global; no other lib test refreshes a ChatGPT credential.
        std::env::set_var(
            gents_protocol::chatgpt_oauth::REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR,
            &url,
        );
        let bearer = DbCredentialBearer::new(
            node,
            did,
            provider,
            oauth_credential_id(did, provider),
            true,
            OAuthRefreshKind::ChatGpt,
            CHATGPT_OAUTH_PRODUCT,
        );
        bearer.invalidate().await;
        let first = bearer.current_bearer().await;
        handle.await.expect("server");
        let second = bearer.current_bearer().await;
        std::env::remove_var(gents_protocol::chatgpt_oauth::REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR);

        let first = first.expect_err("rejected refresh");
        let second = second.expect_err("cooldown must not re-serve the rejected token");
        assert!(
            first.to_string().contains("is expired or revoked"),
            "{first}"
        );
        assert_eq!(first.to_string(), second.to_string());
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use chrono::{DateTime, Utc};
    use defra_node::EmbeddedNode;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// In-memory node with the gents schemas loaded.
    pub(crate) async fn test_node() -> EmbeddedNode {
        let node = EmbeddedNode::builder()
            .build()
            .await
            .expect("embedded node");
        crate::schema::ensure_runtime_schemas(&node)
            .await
            .expect("schemas");
        node
    }

    pub(crate) async fn seed_credential(
        node: &EmbeddedNode,
        agent_did: &str,
        provider: &str,
        expires_at: DateTime<Utc>,
    ) {
        seed_credential_with_refresh_token(node, agent_did, provider, expires_at, "refresh-TEST")
            .await;
    }

    pub(crate) async fn seed_credential_with_refresh_token(
        node: &EmbeddedNode,
        agent_did: &str,
        provider: &str,
        expires_at: DateTime<Utc>,
        refresh_token: &str,
    ) {
        let credential = crate::oauth_credential::OAuthCredential {
            doc_id: None,
            credential_id: crate::oauth_credential::oauth_credential_id(agent_did, provider),
            agent_did: agent_did.to_string(),
            provider: provider.to_string(),
            access_token: "access-TEST".into(),
            refresh_token: refresh_token.into(),
            id_token: None,
            account_id: None,
            chatgpt_plan_type: None,
            is_fedramp: false,
            access_token_expires_at: expires_at,
            last_refresh: Some(Utc::now()),
            enabled: true,
        };
        crate::oauth_credential::upsert_oauth_credential(node, &credential)
            .await
            .expect("seed credential");
    }

    /// Accepts exactly one HTTP request, returns its body, and answers with `status` + `body`.
    pub(crate) async fn one_shot_token_server(
        status: u16,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
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
                    let headers = &text[..idx];
                    let content_length: usize = headers
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
