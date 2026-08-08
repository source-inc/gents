//! Backend registry — DefraDB lookups for inference backend documents.
//!
//! The runtime uses this to resolve a behavior's backend and check health.

use anyhow::Result;
use defra_node::EmbeddedNode;
use tracing::Instrument;

use crate::backend_provider::BackendProviderKind;
use crate::graphql::escape_graphql_string;
use crate::openai_wire::OpenAiWireApi;

pub const DEFAULT_MAX_QUEUE_DEPTH: i64 = 100;
pub const HEALTHY_PROBE_STATUS: &str = "healthy";
pub const UNKNOWN_PROBE_STATUS: &str = "unknown";

#[derive(Debug, Clone)]
pub struct InferenceBackend {
    pub backend_id: String,
    pub name: String,
    pub provider_kind: BackendProviderKind,
    pub openai_wire_api: Option<OpenAiWireApi>,
    pub endpoint: String,
    pub api_key: Option<String>,
    pub api_key_env_var: Option<String>,
    pub max_concurrent: i64,
    pub max_queue_depth: i64,
    pub enabled: bool,
    pub models: Vec<String>,
    pub probe_status: String,
}

impl InferenceBackend {
    pub fn from_value(v: &serde_json::Value) -> Result<Self> {
        Ok(Self {
            backend_id: v
                .get("backend_id")
                .and_then(|value| value.as_str())
                .ok_or_else(|| anyhow::anyhow!("backend_id is required"))?
                .to_string(),
            name: v
                .get("name")
                .and_then(|value| value.as_str())
                .ok_or_else(|| anyhow::anyhow!("backend name is required"))?
                .to_string(),
            provider_kind: BackendProviderKind::parse_optional(
                v.get("provider_kind").and_then(|value| value.as_str()),
            )?,
            openai_wire_api: OpenAiWireApi::parse_optional(
                v.get("openai_wire_api").and_then(|value| value.as_str()),
            )?,
            endpoint: v
                .get("endpoint")
                .and_then(|value| value.as_str())
                .ok_or_else(|| anyhow::anyhow!("backend endpoint is required"))?
                .to_string(),
            api_key: v
                .get("api_key")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(ToOwned::to_owned),
            api_key_env_var: v
                .get("api_key_env_var")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(ToOwned::to_owned),
            max_concurrent: v
                .get("max_concurrent")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| anyhow::anyhow!("max_concurrent is required"))?,
            max_queue_depth: v
                .get("max_queue_depth")
                .and_then(|value| value.as_i64())
                .unwrap_or(DEFAULT_MAX_QUEUE_DEPTH),
            enabled: v
                .get("enabled")
                .and_then(|value| value.as_bool())
                .ok_or_else(|| anyhow::anyhow!("enabled is required"))?,
            models: v
                .get("models")
                .and_then(|value| value.as_array())
                .map(|rows| {
                    rows.iter()
                        .filter_map(|row| row.as_str())
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            probe_status: v
                .get("probe_status")
                .and_then(|v| v.as_str())
                .unwrap_or(UNKNOWN_PROBE_STATUS)
                .to_string(),
        })
    }

    pub fn is_available(&self) -> bool {
        self.enabled && self.probe_status == HEALTHY_PROBE_STATUS
    }

    pub fn resolved_api_key(&self) -> Option<String> {
        if let Some(key) = self.api_key.as_ref() {
            return Some(key.clone());
        }
        self.api_key_env_var
            .as_ref()
            .and_then(|var| std::env::var(var).ok())
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    }

    pub fn display_state(&self) -> &'static str {
        derive_display_state(self.enabled, &self.probe_status)
    }
}

/// Pure function backing [`InferenceBackend::display_state`]. Lives outside
/// the impl so the Tauri bridge can call it on raw `(enabled, probe_status)`
/// pairs from the Lean witness fixtures without constructing a full
/// `InferenceBackend`.
pub fn derive_display_state(enabled: bool, probe_status: &str) -> &'static str {
    if !enabled {
        return "disabled";
    }
    match probe_status {
        "healthy" => "available",
        "unhealthy" => "unhealthy",
        "stale" => "stale",
        "rate_limited" => "rate-limited",
        "circuit_open" => "circuit-open",
        "unknown" => "unknown",
        _ => "unknown",
    }
}

pub async fn lookup_backend(
    node: &EmbeddedNode,
    backend_id: &str,
) -> Result<Option<InferenceBackend>> {
    Ok(lookup_backend_record(node, backend_id)
        .await?
        .map(|(_, backend)| backend))
}

pub(crate) async fn lookup_backend_record(
    node: &EmbeddedNode,
    backend_id: &str,
) -> Result<Option<(String, InferenceBackend)>> {
    let escaped_id = escape_graphql_string(backend_id);
    let query = format!(
        r#"query {{ InferenceBackend(filter: {{backend_id: {{_eq: "{}"}}}}) {{ _docID backend_id name provider_kind openai_wire_api endpoint api_key api_key_env_var max_concurrent max_queue_depth enabled models probe_status }} }}"#,
        escaped_id
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query InferenceBackend failed: {:?}", resp.errors);
    }

    let backend = resp
        .data
        .as_ref()
        .and_then(|d| d.get("InferenceBackend"))
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .map(|row| {
            Ok::<_, anyhow::Error>((
                row.get("_docID")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| anyhow::anyhow!("InferenceBackend row is missing _docID"))?
                    .to_string(),
                InferenceBackend::from_value(row)?,
            ))
        })
        .transpose()?;

    Ok(backend)
}

pub(crate) async fn lookup_backend_by_doc_id(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<Option<(String, InferenceBackend)>> {
    let escaped_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"query {{ InferenceBackend(filter: {{_docID: {{_eq: "{}"}}}}, limit: 1) {{ _docID backend_id name provider_kind openai_wire_api endpoint api_key api_key_env_var max_concurrent max_queue_depth enabled models probe_status }} }}"#,
        escaped_id
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query InferenceBackend by _docID failed: {:?}", resp.errors);
    }

    let backend = resp
        .data
        .as_ref()
        .and_then(|d| d.get("InferenceBackend"))
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .map(|row| {
            Ok::<_, anyhow::Error>((
                row.get("_docID")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| anyhow::anyhow!("InferenceBackend row is missing _docID"))?
                    .to_string(),
                InferenceBackend::from_value(row)?,
            ))
        })
        .transpose()?;

    Ok(backend)
}

pub(crate) async fn lookup_backend_at_cid(
    node: &EmbeddedNode,
    composite_commit_cid: &str,
) -> Result<Option<(String, InferenceBackend)>> {
    let escaped_cid = escape_graphql_string(composite_commit_cid);
    let query = format!(
        r#"query {{ InferenceBackend(cid: ["{escaped_cid}"]) {{ _docID backend_id name provider_kind openai_wire_api endpoint api_key api_key_env_var max_concurrent max_queue_depth enabled models probe_status }} }}"#,
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query InferenceBackend at CID failed: {:?}", resp.errors);
    }
    let backend = resp
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceBackend"))
        .and_then(|value| value.as_array())
        .and_then(|rows| rows.first())
        .map(|row| {
            Ok::<_, anyhow::Error>((
                row.get("_docID")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| anyhow::anyhow!("InferenceBackend row is missing _docID"))?
                    .to_string(),
                InferenceBackend::from_value(row)?,
            ))
        })
        .transpose()?;
    Ok(backend)
}

pub(crate) async fn list_backend_records(
    node: &EmbeddedNode,
) -> Result<Vec<(String, InferenceBackend)>> {
    let query = r#"query {
        InferenceBackend(order: { backend_id: ASC }) {
            _docID
            backend_id
            name
            provider_kind
            openai_wire_api
            endpoint
            api_key
            api_key_env_var
            max_concurrent
            max_queue_depth
            enabled
            models
            probe_status
        }
    }"#;

    let resp = node.execute(query).await;
    if resp.has_errors() {
        anyhow::bail!("list InferenceBackend failed: {:?}", resp.errors);
    }

    let backends = resp
        .data
        .as_ref()
        .and_then(|d| d.get("InferenceBackend"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|row| {
                    Ok::<_, anyhow::Error>((
                        row.get("_docID")
                            .and_then(|value| value.as_str())
                            .ok_or_else(|| {
                                anyhow::anyhow!("InferenceBackend row is missing _docID")
                            })?
                            .to_string(),
                        InferenceBackend::from_value(row)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(backends)
}

pub async fn list_all_backends(node: &EmbeddedNode) -> Result<Vec<InferenceBackend>> {
    Ok(list_backend_records(node)
        .await?
        .into_iter()
        .map(|(_, backend)| backend)
        .collect())
}

pub async fn list_enabled_backends(node: &EmbeddedNode) -> Result<Vec<InferenceBackend>> {
    let query = r#"query { InferenceBackend(filter: {enabled: {_eq: true}}) { backend_id name provider_kind openai_wire_api endpoint api_key api_key_env_var max_concurrent max_queue_depth enabled probe_status models last_probe } }"#;

    let resp = node.execute(query).await;
    if resp.has_errors() {
        anyhow::bail!("query InferenceBackend failed: {:?}", resp.errors);
    }

    let backends = resp
        .data
        .as_ref()
        .and_then(|d| d.get("InferenceBackend"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(InferenceBackend::from_value)
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(backends)
}

pub async fn set_backend_probe_status(
    node: &EmbeddedNode,
    backend_id: &str,
    probe_status: &str,
) -> Result<()> {
    let mutation = format!(
        r#"mutation {{
            update_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{}" }} }},
                input: {{ probe_status: "{}" }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(backend_id),
        escape_graphql_string(probe_status),
    );
    let resp = node.execute(&mutation).await;
    if resp.has_errors() {
        anyhow::bail!(
            "update InferenceBackend probe_status for {backend_id} failed: {:?}",
            resp.errors
        );
    }
    Ok(())
}

pub async fn set_backend_probe_status_with_last_probe(
    node: &EmbeddedNode,
    backend_id: &str,
    probe_status: &str,
    last_probe: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let mutation = format!(
        r#"mutation {{
            update_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{}" }} }},
                input: {{ probe_status: "{}", last_probe: "{}" }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(backend_id),
        escape_graphql_string(probe_status),
        last_probe.to_rfc3339(),
    );
    let resp = node.execute(&mutation).await;
    if resp.has_errors() {
        anyhow::bail!(
            "update InferenceBackend probe_status/last_probe for {backend_id} failed: {:?}",
            resp.errors
        );
    }
    Ok(())
}

pub async fn probe_and_promote_enabled_backends(node: &EmbeddedNode) {
    let backends = match list_enabled_backends(node).await {
        Ok(backends) => backends,
        Err(error) => {
            tracing::warn!(error = %error, "startup backend probe: could not list backends");
            return;
        }
    };

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(error = %error, "startup backend probe: could not build HTTP client");
            return;
        }
    };

    for backend in backends {
        if backend.probe_status == HEALTHY_PROBE_STATUS {
            continue;
        }
        if backend.provider_kind.is_agent_scoped_oauth() {
            tracing::info!(
                backend_id = %backend.backend_id,
                endpoint = %backend.endpoint,
                provider_kind = %backend.provider_kind,
                "startup backend probe: skipping OAuth backend because OAuthCredential is agent-scoped"
            );
            continue;
        }
        async {
            let api_key = backend.resolved_api_key();
            match crate::backend_provider::discover_models(
                &client,
                backend.provider_kind,
                &backend.endpoint,
                api_key.as_deref(),
                None,
            )
            .await
            {
                Ok(models) => {
                    tracing::Span::current().record("model_count", models.len() as i64);
                    match set_backend_probe_status_with_last_probe(
                        node,
                        &backend.backend_id,
                        HEALTHY_PROBE_STATUS,
                        chrono::Utc::now(),
                    )
                    .await
                    {
                        Ok(()) => tracing::info!(
                            backend_id = %backend.backend_id,
                            endpoint = %backend.endpoint,
                            "startup backend probe: promoted to healthy and stamped last_probe"
                        ),
                        Err(error) => tracing::warn!(
                            backend_id = %backend.backend_id,
                            error = %error,
                            "startup backend probe: reachable but failed to persist healthy status and last_probe"
                        ),
                    }
                }
                Err(error) => tracing::warn!(
                    backend_id = %backend.backend_id,
                    endpoint = %backend.endpoint,
                    error = %error,
                    "startup backend probe: unreachable, leaving probe_status unchanged"
                ),
            }
        }
        .instrument(tracing::info_span!(
            "backend.startup_probe",
            backend_id = %backend.backend_id,
            endpoint = %backend.endpoint,
            provider_kind = %backend.provider_kind,
            previous_probe_status = %backend.probe_status,
            model_count = tracing::field::Empty,
        ))
        .await;
    }
}

#[cfg(test)]
mod tests;
