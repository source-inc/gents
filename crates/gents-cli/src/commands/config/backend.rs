use std::time::Duration;

use anyhow::{Context, Result};
use gents::graphql::escape_graphql_string;
use gents::{discover_backend_models, BackendProviderKind, InferenceBackend};
use gents_protocol::graphql::{extract_mutation_doc_id, string_list_field};
use serde_json::{json, Value};

use crate::cli::*;
use crate::config_writes::{
    write_inference_backend_document, ConfigAccess, InferenceBackendUpsertDocument,
};
use crate::print_json;
use crate::shared::*;
use crate::{
    normalize_optional_string, post_graphql, resolve_agent_did, BackendResolutionMode,
    EXPORT_INFERENCE_BACKEND_FIELDS,
};

/// Decode this command's resolved args into the document type
/// `InferenceBackend::validate` owns. `models` isn't a `backend set` flag —
/// this writer always stamps the `"default"` placeholder
/// `write_inference_backend_document` below actually sends — so the
/// no-lockout conjunct never fires here (it needs a non-empty advertised
/// list to compare a current model against).
fn to_document_backend(
    args: &BackendUpsertArgs,
    backend: &ResolvedBackendConfig,
) -> InferenceBackend {
    InferenceBackend {
        backend_id: args.backend_id.clone(),
        name: args.name.clone(),
        provider_kind: backend.provider_kind,
        openai_wire_api: backend.openai_wire_api,
        endpoint: backend.endpoint.clone(),
        api_key: backend.api_key.clone(),
        api_key_env_var: backend.api_key_env_var.clone(),
        max_concurrent: args.max_concurrent,
        max_queue_depth: args.max_queue_depth,
        enabled: args.enabled,
        models: vec!["default".to_string()],
        probe_status: args.probe_status.clone(),
    }
}

pub(super) async fn backend_set(args: BackendUpsertArgs) -> Result<()> {
    let backend = resolve_backend_upsert_config(&args)?;
    // Document rules (backend_id/endpoint non-empty, api_key shape,
    // max_concurrent/max_queue_depth positive) are owned by
    // `InferenceBackend::validate` (#1331) — previously unchecked by this
    // writer entirely (the api_key-xor-env_var shape check in
    // `resolve_backend_upsert_config` above is a separate, earlier,
    // raw-flag sanity check shared with `gents init`; this is the
    // document-shape gate right before the write).
    to_document_backend(&args, &backend).validate(None)?;
    let access = ConfigAccess::Graphql(args.graphql.clone());
    let doc = InferenceBackendUpsertDocument {
        backend_id: args.backend_id.clone(),
        name: args.name.clone(),
        provider_kind: backend.provider_kind,
        openai_wire_api: backend.openai_wire_api,
        endpoint: backend.endpoint.clone(),
        api_key: backend.api_key.clone(),
        api_key_env_var: backend.api_key_env_var.clone(),
        max_concurrent: args.max_concurrent,
        max_queue_depth: args.max_queue_depth,
        enabled: args.enabled,
        models_on_add: vec!["default".to_string()],
        models_on_update: None,
        probe_status: args.probe_status.clone(),
    };
    let doc_id = write_inference_backend_document(&access, &doc).await?;
    let output = json!({
        "doc_id": doc_id,
        "backend_id": args.backend_id,
        "backend_preset": args.backend_preset.map(BackendPresetArg::as_str),
        "provider_kind": backend.provider_kind.as_str(),
        "openai_wire_api": backend.openai_wire_api.map(gents::OpenAiWireApi::as_str),
        "endpoint": backend.endpoint,
        "api_key": backend.api_key.as_ref().map(|_| "<redacted>"),
        "api_key_env_var": backend.api_key_env_var,
        "max_concurrent": args.max_concurrent,
        "max_queue_depth": args.max_queue_depth,
        "enabled": args.enabled,
        "probe_status": args.probe_status,
    });
    print_json(&output)?;
    Ok(())
}

pub(super) async fn backend_discover_models(args: BackendDiscoverModelsArgs) -> Result<()> {
    if args.write && normalize_optional_string(args.backend_id.as_deref()).is_none() {
        anyhow::bail!(
            "--write requires --backend-id: the discovered models are written to that backend document"
        );
    }
    let target = resolve_backend_discovery_target(&args).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("building backend discovery client")?;
    let is_oauth = target.provider_kind.is_agent_scoped_oauth();
    let (oauth_credential, oauth_agent_did) = if is_oauth {
        let (credential, agent_did) =
            load_oauth_credential_for_discovery(&args, target.provider_kind).await?;
        (credential, Some(agent_did))
    } else {
        (None, None)
    };
    let discovered_models = match discover_backend_models(
        &client,
        target.provider_kind,
        &target.endpoint,
        target.api_key.as_deref(),
        oauth_credential.as_ref(),
    )
    .await
    {
        Ok(models) => models,
        Err(error)
            if target.provider_kind == BackendProviderKind::ChatGptCodex
                && discovery_error_is_auth(&error) =>
        {
            let guidance = gents::oauth_credential::classify_chatgpt_auth_error(
                oauth_agent_did.as_deref().unwrap_or(""),
                gents::chatgpt_codex::CHATGPT_CODEX_PROVIDER,
                &gents::oauth_credential::OAuthAuthProblem::Expired,
            );
            anyhow::bail!("{error:#}\n{guidance}");
        }
        Err(error)
            if target.provider_kind == BackendProviderKind::XaiGrokOAuth
                && discovery_error_is_auth(&error) =>
        {
            let guidance = gents::xai_grok_oauth::classify_xai_auth_error(
                oauth_agent_did.as_deref().unwrap_or(""),
                gents::xai_grok_oauth::XAI_OAUTH_PROVIDER,
                &gents::oauth_credential::OAuthAuthProblem::Expired,
            );
            anyhow::bail!("{error:#}\n{guidance}");
        }
        Err(error)
            if target.provider_kind == BackendProviderKind::ClaudeCliSubscription
                && discovery_error_is_auth(&error) =>
        {
            let guidance = gents::claude_oauth::classify_claude_auth_error(
                oauth_agent_did.as_deref().unwrap_or(""),
                gents::claude_oauth::CLAUDE_OAUTH_PROVIDER,
                &gents::oauth_credential::OAuthAuthProblem::Expired,
            );
            anyhow::bail!("{error:#}\n{guidance}");
        }
        Err(error) => return Err(error),
    };

    // An empty list would render `models: null` and wipe the column, so it is never written.
    let models_written = if args.write && !discovered_models.is_empty() {
        write_discovered_models(
            args.graphql
                .as_deref()
                .expect("checked graphql when backend_id is set"),
            target
                .backend_id
                .as_deref()
                .expect("checked backend_id when --write is set"),
            &discovered_models,
        )
        .await?
    } else {
        0
    };
    let output = json!({
        "backend_id": target.backend_id,
        "backend_preset": target.preset.map(BackendPresetArg::as_str),
        "provider_kind": target.provider_kind.as_str(),
        "endpoint": target.endpoint,
        "api_key": target.api_key.as_ref().map(|_| "<redacted>"),
        "api_key_env_var": target.api_key_env_var,
        "discovered_models": discovered_models,
        "models_written": models_written,
        "write_skipped": (args.write && models_written == 0)
            .then_some("discovery returned no models; models[] left unchanged"),
    });
    print_json(&output)?;
    Ok(())
}

async fn load_oauth_credential_for_discovery(
    args: &BackendDiscoverModelsArgs,
    provider_kind: BackendProviderKind,
) -> Result<(Option<gents::oauth_credential::OAuthCredential>, String)> {
    let (provider, login) = match provider_kind {
        BackendProviderKind::ChatGptCodex => (
            gents::chatgpt_codex::CHATGPT_CODEX_PROVIDER,
            "gents codex-login",
        ),
        BackendProviderKind::XaiGrokOAuth => (
            gents::xai_grok_oauth::XAI_OAUTH_PROVIDER,
            "gents grok-login",
        ),
        BackendProviderKind::ClaudeCliSubscription => (
            gents::claude_oauth::CLAUDE_OAUTH_PROVIDER,
            "gents claude-login",
        ),
        _ => anyhow::bail!("load_oauth_credential_for_discovery called for non-OAuth provider"),
    };
    let Some(graphql) = normalize_optional_string(args.graphql.as_deref()) else {
        anyhow::bail!(
            "--graphql is required to discover models for a {provider_kind} backend: its OAuth \
             credential is a DefraDB document. Run `{login}` first if needed."
        );
    };
    let agent_did = resolve_agent_did(args.home.as_deref(), args.agent_did.as_deref())?;
    let access = ConfigAccess::Graphql(graphql);
    let credential =
        crate::commands::codex_auth_probe::load_oauth_credential(&access, &agent_did, provider)
            .await?;
    Ok((credential, agent_did))
}

/// Rewrites `models[]` on the stored backend document and nothing else.
async fn write_discovered_models(
    graphql: &str,
    backend_id: &str,
    models: &[String],
) -> Result<usize> {
    let models_field = string_list_field("models", models)
        .ok_or_else(|| anyhow::anyhow!("backend models field could not be rendered"))?;
    let mutation = format!(
        r#"mutation {{
            update_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{}" }} }},
                input: {{ {} }}
            ) {{ _docID }}
        }}"#,
        escape_graphql_string(backend_id),
        models_field,
    );
    let response = post_graphql(graphql, &mutation).await?;
    extract_mutation_doc_id(&response, "InferenceBackend")
        .with_context(|| format!("updating models on backend {backend_id}"))?;
    Ok(models.len())
}

/// Whether a model-discovery error is an authentication failure (HTTP 401/403), so ChatGptCodex
/// discovery can append re-login guidance the bare error omits. Inspects the typed status carried
/// by [`ModelDiscoveryHttpError`] rather than scraping the rendered message (which also contains the
/// endpoint URL and response body, and could otherwise match "401"/"403" spuriously).
fn discovery_error_is_auth(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<gents::backend_provider::ModelDiscoveryHttpError>()
            .is_some_and(|http| http.is_auth())
    })
}

async fn resolve_backend_discovery_target(
    args: &BackendDiscoverModelsArgs,
) -> Result<DiscoveredBackendTarget> {
    if let Some(backend_id) = normalize_optional_string(args.backend_id.as_deref()) {
        if args.graphql.is_none() {
            anyhow::bail!("--graphql is required when --backend-id is set");
        }
        if args.backend_preset.is_some()
            || normalize_optional_string(args.provider_kind.as_deref()).is_some()
            || normalize_optional_string(args.endpoint.as_deref()).is_some()
            || normalize_optional_string(args.api_key.as_deref()).is_some()
            || normalize_optional_string(args.api_key_env_var.as_deref()).is_some()
        {
            anyhow::bail!(
                "--backend-id uses the stored backend document; do not combine it with explicit preset, endpoint, provider, or auth flags"
            );
        }
        let backend = load_backend_row(
            args.graphql
                .as_deref()
                .expect("checked graphql when backend_id is set"),
            &backend_id,
        )
        .await?;
        let provider_kind = BackendProviderKind::parse_optional(
            backend.get("provider_kind").and_then(Value::as_str),
        )?;
        let endpoint = backend
            .get("endpoint")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("backend {backend_id} is missing endpoint"))?
            .to_string();
        let api_key = normalize_optional_string(backend.get("api_key").and_then(Value::as_str));
        let api_key_env_var =
            normalize_optional_string(backend.get("api_key_env_var").and_then(Value::as_str));
        if api_key.is_some() && api_key_env_var.is_some() {
            anyhow::bail!(
                "backend {backend_id} sets both raw api_key and api_key_env_var; discovery is ambiguous"
            );
        }
        let resolved_api_key = match (api_key, api_key_env_var.clone()) {
            (Some(raw), None) => Some(raw),
            (None, Some(name)) => Some(resolve_required_env_api_key(&name)?),
            (None, None) => None,
            (Some(_), Some(_)) => unreachable!("guarded above"),
        };
        return Ok(DiscoveredBackendTarget {
            backend_id: Some(backend_id),
            preset: None,
            provider_kind,
            endpoint,
            api_key: resolved_api_key,
            api_key_env_var,
        });
    }

    let preset = args.backend_preset;
    let api_key = normalize_optional_string(args.api_key.as_deref());
    let explicit_api_key_env_var = normalize_optional_string(args.api_key_env_var.as_deref());
    if api_key.is_some() && explicit_api_key_env_var.is_some() {
        anyhow::bail!("provide either --api-key or --api-key-env-var, not both");
    }
    let endpoint = resolve_backend_endpoint(
        args.endpoint.as_deref(),
        preset,
        BackendResolutionMode::ConfigWrite,
    )?;
    let provider_kind = resolve_backend_provider_kind(args.provider_kind.as_deref(), preset)?;
    let api_key_env_var =
        resolve_backend_api_key_env_var(explicit_api_key_env_var, api_key.is_some(), preset);
    let resolved_api_key = match (api_key, api_key_env_var.clone()) {
        (Some(raw), None) => Some(raw),
        (None, Some(name)) => Some(resolve_required_env_api_key(&name)?),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("guarded above"),
    };
    Ok(DiscoveredBackendTarget {
        backend_id: None,
        preset,
        provider_kind,
        endpoint,
        api_key: resolved_api_key,
        api_key_env_var,
    })
}

fn resolve_required_env_api_key(name: &str) -> Result<String> {
    let value = std::env::var(name)
        .with_context(|| format!("required backend API key env var {name} is not set"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        anyhow::bail!("required backend API key env var {name} is empty");
    }
    Ok(trimmed.to_string())
}

async fn load_backend_row(graphql: &str, backend_id: &str) -> Result<Value> {
    let response = post_graphql(
        graphql,
        &format!(
            r#"{{
                InferenceBackend(
                    filter: {{ backend_id: {{ _eq: "{}" }} }},
                    limit: 1
                ) {{
                    {}
                }}
            }}"#,
            escape_graphql_string(backend_id),
            EXPORT_INFERENCE_BACKEND_FIELDS,
        ),
    )
    .await?;
    response
        .get("data")
        .and_then(|data| data.get("InferenceBackend"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("backend {backend_id} not found"))
}

fn resolve_backend_upsert_config(args: &BackendUpsertArgs) -> Result<ResolvedBackendConfig> {
    crate::resolve_helpers::resolve_backend_config_with_preset(
        args.backend_preset,
        args.endpoint.as_deref(),
        args.provider_kind.as_deref(),
        args.openai_wire_api,
        args.api_key.as_deref(),
        args.api_key_env_var.as_deref(),
        BackendResolutionMode::ConfigWrite,
    )
}

fn resolve_backend_endpoint(
    explicit: Option<&str>,
    preset: Option<BackendPresetArg>,
    mode: BackendResolutionMode,
) -> Result<String> {
    normalize_optional_string(explicit)
        .or_else(|| preset.and_then(|candidate| candidate.default_endpoint().map(str::to_string)))
        .or_else(|| {
            (mode == BackendResolutionMode::Init)
                .then(|| std::env::var("INFERENCE_ENDPOINT").ok())
                .flatten()
                .and_then(|value| {
                    let trimmed = value.trim();
                    (!trimmed.is_empty()).then(|| trimmed.to_string())
                })
        })
        .or_else(|| {
            (mode == BackendResolutionMode::Init).then(|| crate::DEFAULT_INIT_ENDPOINT.to_string())
        })
        .ok_or_else(|| match mode {
            BackendResolutionMode::Init => anyhow::anyhow!(
                "an inference endpoint is required\nNext:\n  1. Pass it explicitly: `gents init --inference-url http://HOST:PORT/v1 --model-name MODEL`\n  2. Or choose a preset with a default endpoint: `gents init --backend-preset openrouter --model-name MODEL`\n  3. Or set INFERENCE_ENDPOINT before running `gents init`"
            ),
            BackendResolutionMode::ConfigWrite => anyhow::anyhow!(
                "an inference endpoint is required\nNext:\n  1. Pass --inference-url explicitly\n  2. Or choose a preset with a default endpoint, such as --backend-preset openrouter"
            ),
        })
}

fn resolve_backend_provider_kind(
    explicit: Option<&str>,
    preset: Option<BackendPresetArg>,
) -> Result<BackendProviderKind> {
    match normalize_optional_string(explicit) {
        Some(value) => BackendProviderKind::parse_optional(Some(&value)),
        None => Ok(
            preset.map_or_else(BackendProviderKind::default, |candidate| {
                candidate.provider_kind()
            }),
        ),
    }
}

fn resolve_backend_api_key_env_var(
    explicit: Option<String>,
    raw_api_key_present: bool,
    preset: Option<BackendPresetArg>,
) -> Option<String> {
    explicit.or_else(|| {
        (!raw_api_key_present)
            .then(|| preset.and_then(|candidate| candidate.default_api_key_env_var()))
            .flatten()
            .map(ToOwned::to_owned)
    })
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    /// `backend set` validates the document before writing (#1331). The
    /// claude-cli-subscription preset must pass that gate on its own: a
    /// non-empty placeholder endpoint, no api_key, positive max_*.
    #[test]
    fn config_backend_claude_cli_subscription_preset_passes_validation() {
        let cli = Cli::try_parse_from([
            "gents",
            "config",
            "backend",
            "set",
            "--graphql",
            "http://127.0.0.1:1/graphql",
            "--backend-id",
            "claude-max",
            "--name",
            "Claude Max",
            "--backend-preset",
            "claude-cli-subscription",
            "--max-concurrent",
            "1",
        ])
        .expect("parse");
        let Command::Config {
            command:
                ConfigCommand::Backend {
                    command: BackendCommand::Set(args),
                },
        } = cli.command
        else {
            panic!("expected config backend set")
        };
        let backend = resolve_backend_upsert_config(&args).expect("resolve preset");
        assert_eq!(
            backend.provider_kind,
            BackendProviderKind::ClaudeCliSubscription
        );
        assert_eq!(
            backend.endpoint,
            gents::claude_subscription::default_backend_endpoint()
        );
        assert_eq!(backend.api_key, None);
        assert_eq!(backend.api_key_env_var, None);
        to_document_backend(&args, &backend)
            .validate(None)
            .expect("preset document validates");
    }
}
