mod backends;
mod schema;
mod tool_ceiling;

use anyhow::Result;
use gents_protocol::row::{project_behavior_readiness_summary, ProjectedBehaviorReadinessSummary};
use serde_json::{json, Value};

use crate::cli::args::{DiagnoseArgs, P2pTransportArg};
use crate::shared::ConfigExportBundle;
use crate::{
    build_config_export_bundle, graphql_endpoint_available, print_json, read_init_config,
    read_runtime_state, resolve_agent_did, resolve_config_access, resolve_home_dir,
    CONFIG_EXPORT_FORMAT,
};

use backends::diagnose_backends;
use schema::{diagnose_schema_presence, load_runtime_row};
use tool_ceiling::diagnose_tool_ceiling;

pub(crate) async fn diagnose(args: DiagnoseArgs) -> Result<()> {
    let home_dir = resolve_home_dir(args.home.as_deref());
    let init_config = read_init_config(&home_dir)?;
    let runtime_state = read_runtime_state(&home_dir)?;
    let graphql = args
        .graphql
        .clone()
        .or_else(|| runtime_state.as_ref().map(|state| state.graphql.clone()));
    let graphql_reachable = match graphql.as_deref() {
        Some(endpoint) => graphql_endpoint_available(endpoint).await,
        None => false,
    };
    let agent_did = resolve_agent_did(args.home.as_deref(), args.agent_did.as_deref())?;
    let (access, _) = resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;

    let schema_checks = diagnose_schema_presence(&access).await;
    let bundle_result = build_config_export_bundle(&access, &agent_did).await;
    let config_load_error = bundle_result.as_ref().err().map(ToString::to_string);
    let bundle = bundle_result.unwrap_or_else(|_| ConfigExportBundle {
        format: CONFIG_EXPORT_FORMAT.to_string(),
        agent_did: agent_did.clone(),
        exported_at: chrono::Utc::now().to_rfc3339(),
        access_mode: access.mode().to_string(),
        agent_principal: None,
        agent_behaviors: Vec::new(),
        skills: Vec::new(),
        datastore_tool_surfaces: Vec::new(),
        chain_key_bindings: Vec::new(),
        eth_tools: Vec::new(),
        workspace_roots: Vec::new(),
        tool_selections: Vec::new(),
        inference_backends: Vec::new(),
        inference_profiles: Vec::new(),
        tool_service_registries: Vec::new(),
        projection_acp_bindings: Vec::new(),
        tasks: Vec::new(),
        schedules: Vec::new(),
        event_triggers: Vec::new(),
    });
    let runtime_row = match load_runtime_row(&access, &agent_did).await {
        Ok(Some(row)) => row,
        Ok(None) => Value::Null,
        Err(error) => json!({
            "error": error.to_string(),
        }),
    };
    let live_runtime = graphql_reachable && runtime_row.get("agent_did").is_some();
    let readiness_row = crate::commands::status::load_behavior_readiness(&access, &agent_did).await;
    let (runtime_behavior_readiness, runtime_behavior_readiness_check) = match readiness_row {
        Ok(row) => {
            match project_behavior_readiness_summary(row.as_ref(), &agent_did, chrono::Utc::now()) {
                ProjectedBehaviorReadinessSummary::Observed(summary) => {
                    let unavailable = summary
                        .unavailable_behaviors
                        .iter()
                        .map(|(behavior_id, reason)| {
                            json!({
                                "behavior_id": behavior_id,
                                "reason": reason,
                                "message": reason.public_message(),
                            })
                        })
                        .collect::<Vec<_>>();
                    let observed_ready = unavailable.is_empty();
                    (
                        serde_json::to_value(&summary.snapshot).unwrap_or(Value::Null),
                        json!({
                            "ok": !live_runtime || observed_ready,
                            "required": live_runtime,
                            "status": if observed_ready { "ready" } else { "degraded" },
                            "ready_behavior_count": summary.ready_count,
                            "unavailable_behaviors": unavailable,
                        }),
                    )
                }
                ProjectedBehaviorReadinessSummary::Unknown(reason) => (
                    json!({ "state": "unknown", "reason": reason }),
                    json!({
                        "ok": !live_runtime,
                        "required": live_runtime,
                        "status": "unknown",
                        "reason": reason,
                    }),
                ),
            }
        }
        Err(error) => (
            json!({ "state": "unknown", "error": error.to_string() }),
            json!({
                "ok": !live_runtime,
                "required": live_runtime,
                "status": "unknown",
                "error": error.to_string(),
            }),
        ),
    };
    let runtime_behavior_readiness_ok = runtime_behavior_readiness_check
        .get("ok")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let behavior_ids = bundle
        .agent_behaviors
        .iter()
        .filter_map(|row| {
            row.get("behavior_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect::<std::collections::BTreeSet<_>>();
    let default_behavior_id = bundle
        .agent_principal
        .as_ref()
        .and_then(|row| row.get("default_behavior_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let default_behavior_check = match default_behavior_id.as_deref() {
        Some(behavior_id) if behavior_ids.contains(behavior_id) => json!({
            "ok": true,
            "default_behavior_id": behavior_id,
        }),
        Some(behavior_id) => json!({
            "ok": false,
            "default_behavior_id": behavior_id,
            "error": format!("default behavior {} is not present in AgentBehavior documents", behavior_id),
        }),
        None => json!({
            "ok": false,
            "error": format!("AgentPrincipal {} is missing or has no default_behavior_id", agent_did),
        }),
    };
    let tool_ceiling_check = diagnose_tool_ceiling(init_config.as_ref());
    let backend_reports = diagnose_backends(&bundle).await;
    let matching_runtime_state = runtime_state.as_ref().filter(|state| {
        graphql
            .as_deref()
            .is_some_and(|endpoint| endpoint == state.graphql)
    });
    let p2p_status = match graphql.as_deref().filter(|_| graphql_reachable) {
        Some(endpoint) => {
            crate::commands::p2p::load_live_http_p2p_status(args.home.as_deref(), endpoint).await
        }
        None => crate::commands::p2p::persisted_p2p_status(matching_runtime_state),
    };
    let p2p_transport = p2p_status
        .get("p2p_transport")
        .and_then(Value::as_str)
        .unwrap_or(P2pTransportArg::None.as_str());
    let p2p_peer_id = p2p_status
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let p2p_connected_peers = p2p_status
        .get("p2p_connected_peers")
        .and_then(Value::as_array)
        .map(|rows| rows.len())
        .unwrap_or(0);
    let p2p_error = p2p_status
        .get("p2p_error")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let p2p_ok = if p2p_transport == P2pTransportArg::None.as_str() {
        true
    } else {
        p2p_peer_id.is_some() && p2p_error.is_none()
    };
    let schemas_ok = schema_checks
        .iter()
        .filter(|check| check.get("required_for_config").and_then(Value::as_bool) == Some(true))
        .all(|check| check.get("ok").and_then(Value::as_bool) == Some(true));
    let backends_ok = backend_reports
        .iter()
        .all(|check| check.get("ok").and_then(Value::as_bool) == Some(true));
    let default_behavior_ok = default_behavior_check
        .get("ok")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let tool_ceiling_ok = tool_ceiling_check
        .get("ok")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let principal_present = bundle.agent_principal.is_some();

    let chatgpt_provider = gents::chatgpt_codex::CHATGPT_CODEX_PROVIDER;
    let chatgpt_auth_check = match crate::commands::codex_auth_probe::load_oauth_credential(
        &access,
        &agent_did,
        chatgpt_provider,
    )
    .await
    {
        Ok(Some(credential))
            if gents::oauth_credential::token_is_fresh(credential.access_token_expires_at) =>
        {
            json!({
                "ok": true,
                "credential_id": credential.credential_id,
                "provider": credential.provider,
                "account_id": credential.account_id,
                "chatgpt_plan_type": credential.chatgpt_plan_type,
                "expires_at": credential.access_token_expires_at,
            })
        }
        Ok(Some(credential)) => json!({
            "ok": false,
            "credential_id": credential.credential_id,
            "provider": credential.provider,
            "expires_at": credential.access_token_expires_at,
            "guidance": gents::oauth_credential::classify_chatgpt_auth_error(
                &agent_did,
                chatgpt_provider,
                &gents::oauth_credential::OAuthAuthProblem::Expired,
            ),
        }),
        Ok(None) => json!({
            "ok": false,
            "provider": chatgpt_provider,
            "guidance": gents::oauth_credential::classify_chatgpt_auth_error(
                &agent_did,
                chatgpt_provider,
                &gents::oauth_credential::OAuthAuthProblem::Missing,
            ),
        }),
        Err(error) => json!({
            "ok": false,
            "provider": chatgpt_provider,
            "error": error.to_string(),
        }),
    };

    let xai_provider = gents::xai_grok_oauth::XAI_OAUTH_PROVIDER;
    let xai_auth_check = match crate::commands::grok_auth_probe::load_oauth_credential(
        &access,
        &agent_did,
        xai_provider,
    )
    .await
    {
        Ok(Some(credential))
            if gents::oauth_credential::token_is_fresh(credential.access_token_expires_at) =>
        {
            json!({
                "ok": true,
                "credential_id": credential.credential_id,
                "provider": credential.provider,
                "expires_at": credential.access_token_expires_at,
            })
        }
        Ok(Some(credential)) => json!({
            "ok": false,
            "credential_id": credential.credential_id,
            "provider": credential.provider,
            "expires_at": credential.access_token_expires_at,
            "guidance": gents::xai_grok_oauth::classify_xai_auth_error(
                &agent_did,
                xai_provider,
                &gents::oauth_credential::OAuthAuthProblem::Expired,
            ),
        }),
        Ok(None) => json!({
            "ok": false,
            "provider": xai_provider,
            "guidance": gents::xai_grok_oauth::classify_xai_auth_error(
                &agent_did,
                xai_provider,
                &gents::oauth_credential::OAuthAuthProblem::Missing,
            ),
        }),
        Err(error) => json!({
            "ok": false,
            "provider": xai_provider,
            "error": error.to_string(),
        }),
    };

    let claude_provider = gents::claude_oauth::CLAUDE_OAUTH_PROVIDER;
    let claude_auth_check = match crate::commands::grok_auth_probe::load_oauth_credential(
        &access,
        &agent_did,
        claude_provider,
    )
    .await
    {
        Ok(Some(credential))
            if gents::oauth_credential::token_is_fresh(credential.access_token_expires_at) =>
        {
            json!({
                "ok": true,
                "credential_id": credential.credential_id,
                "provider": credential.provider,
                "expires_at": credential.access_token_expires_at,
            })
        }
        Ok(Some(credential)) => json!({
            "ok": false,
            "credential_id": credential.credential_id,
            "provider": credential.provider,
            "expires_at": credential.access_token_expires_at,
            "guidance": gents::claude_oauth::classify_claude_auth_error(
                &agent_did,
                claude_provider,
                &gents::oauth_credential::OAuthAuthProblem::Expired,
            ),
        }),
        Ok(None) => json!({
            "ok": false,
            "provider": claude_provider,
            "guidance": gents::claude_oauth::classify_claude_auth_error(
                &agent_did,
                claude_provider,
                &gents::oauth_credential::OAuthAuthProblem::Missing,
            ),
        }),
        Err(error) => json!({
            "ok": false,
            "provider": claude_provider,
            "error": error.to_string(),
        }),
    };

    // An auth failure only degrades overall health when an OAuth backend is actually
    // configured and enabled — deployments that don't use that backend have no credential
    // and must still report `ok`.
    let chatgpt_backend_configured = backend_reports.iter().any(|report| {
        report.get("provider_kind").and_then(Value::as_str)
            == Some(gents::backend_provider::BackendProviderKind::ChatGptCodex.as_str())
            && report.get("enabled").and_then(Value::as_bool) == Some(true)
    });
    let chatgpt_auth_ok = !chatgpt_backend_configured
        || chatgpt_auth_check.get("ok").and_then(Value::as_bool) == Some(true);

    let xai_backend_configured = backend_reports.iter().any(|report| {
        report.get("provider_kind").and_then(Value::as_str)
            == Some(gents::backend_provider::BackendProviderKind::XaiGrokOAuth.as_str())
            && report.get("enabled").and_then(Value::as_bool) == Some(true)
    });
    let xai_auth_ok =
        !xai_backend_configured || xai_auth_check.get("ok").and_then(Value::as_bool) == Some(true);

    let claude_backend_configured = backend_reports.iter().any(|report| {
        report.get("provider_kind").and_then(Value::as_str)
            == Some(gents::backend_provider::BackendProviderKind::ClaudeCliSubscription.as_str())
            && report.get("enabled").and_then(Value::as_bool) == Some(true)
    });
    let claude_auth_ok = claude_auth_gate(claude_backend_configured, &claude_auth_check);

    let status = if schemas_ok
        && principal_present
        && default_behavior_ok
        && tool_ceiling_ok
        && backends_ok
        && chatgpt_auth_ok
        && xai_auth_ok
        && claude_auth_ok
        && p2p_ok
        && runtime_behavior_readiness_ok
        && config_load_error.is_none()
    {
        "ok"
    } else {
        "degraded"
    };

    let mut output = json!({
        "status": status,
        "home": home_dir,
        "agent_did": agent_did,
        "access_mode": access.mode(),
        "graphql": graphql,
        "graphql_reachable": graphql_reachable,
        "runtime": runtime_row,
        "runtime_behavior_readiness": runtime_behavior_readiness,
        "p2p": p2p_status,
        "checks": {
            "schemas": schema_checks,
            "config_documents_loadable": {
                "ok": config_load_error.is_none(),
                "error": config_load_error,
            },
            "agent_principal_present": principal_present,
            "default_behavior": default_behavior_check,
            "runtime_behavior_readiness": runtime_behavior_readiness_check,
            "tool_ceiling": tool_ceiling_check,
            "chatgpt_auth": chatgpt_auth_check,
            "xai_auth": xai_auth_check,
            "claude_auth": claude_auth_check,
            "backends": backend_reports,
            "p2p": {
                "ok": p2p_ok,
                "transport": p2p_transport,
                "peer_id": p2p_peer_id,
                "connected_peer_count": p2p_connected_peers,
                "error": p2p_error,
            },
        },
        "config_counts": {
            "agent_behaviors": bundle.agent_behaviors.len(),
            "tool_selections": bundle.tool_selections.len(),
            "inference_backends": bundle.inference_backends.len(),
            "inference_profiles": bundle.inference_profiles.len(),
            "tool_service_registries": bundle.tool_service_registries.len(),
            "tasks": bundle.tasks.len(),
            "schedules": bundle.schedules.len(),
        },
    });
    if let Some(map) = output.as_object_mut() {
        let p2p_value = map.get("p2p").cloned().unwrap_or(Value::Null);
        crate::commands::p2p::flatten_p2p_fields(map, &p2p_value);
    }
    print_json(&output)?;
    Ok(())
}

/// `checks.claude_auth.ok` reports token freshness, but a stale access token
/// alone does not degrade the overall status: the prober counts such a
/// credential healthy and the next request refreshes it. Only a credential
/// that could not be read at all (missing, or a decode error) degrades — the
/// agent snapshot gate refuses the behavior in that case.
fn claude_auth_gate(backend_configured: bool, check: &Value) -> bool {
    !backend_configured || check.get("credential_id").is_some()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::claude_auth_gate;

    #[test]
    fn claude_auth_degrades_status_only_when_no_credential_can_be_read() {
        let fresh = json!({"ok": true, "credential_id": "claude-subscription:did:key:z6MkT"});
        let stale = json!({
            "ok": false,
            "credential_id": "claude-subscription:did:key:z6MkT",
            "guidance": "expired or revoked",
        });
        let missing = json!({"ok": false, "guidance": "run gents claude-login"});
        let read_error = json!({"ok": false, "error": "querying OAuthCredential returned errors"});
        assert!(claude_auth_gate(true, &fresh));
        assert!(
            claude_auth_gate(true, &stale),
            "staleness alone must not degrade: the next request refreshes"
        );
        assert!(!claude_auth_gate(true, &missing));
        assert!(!claude_auth_gate(true, &read_error));
        assert!(
            claude_auth_gate(false, &missing),
            "no enabled Claude backend: the check is not folded in"
        );
    }
}
