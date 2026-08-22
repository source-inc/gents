use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::{Map, Value};

use super::normalize::normalize_manifest;
use super::validate::{
    normalize_tool_service_mcp_path, normalize_tool_service_string, optional_i64_from_value,
    optional_string_from_value,
};
use super::{
    peer_pairing_manifest_source, DesiredPeerPairing, DesiredStateManifest,
    DesiredToolServiceRegistry, TOOL_SERVICE_ADDRESS_FIELDS,
};

pub(crate) fn tool_service_registry_from_live_value(
    value: &Value,
) -> Result<DesiredToolServiceRegistry> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("expected ToolServiceRegistry live row to be an object"))?;
    let service_id = object
        .get("service_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("ToolServiceRegistry live row is missing service_id"))?
        .to_string();

    Ok(DesiredToolServiceRegistry {
        service_id,
        display_name: optional_string_from_value("display_name", object.get("display_name"))?,
        description: optional_string_from_value("description", object.get("description"))?,
        hostname: optional_string_from_value("hostname", object.get("hostname"))?,
        tailscale_ip: optional_string_from_value("tailscale_ip", object.get("tailscale_ip"))?,
        lan_ip: optional_string_from_value("lan_ip", object.get("lan_ip"))?,
        mcp_port: optional_i64_from_value("mcp_port", object.get("mcp_port"))?,
        mcp_path: optional_string_from_value("mcp_path", object.get("mcp_path"))?,
        send_agent_did: object
            .get("send_agent_did")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

pub(crate) fn normalize_tool_service_registry_storage_fields(
    object: &mut Map<String, Value>,
) -> Result<()> {
    for field in TOOL_SERVICE_ADDRESS_FIELDS {
        let normalized =
            normalize_tool_service_string(optional_string_from_value(field, object.get(*field))?);
        object.insert((*field).to_string(), Value::String(normalized));
    }

    let mcp_path = normalize_tool_service_mcp_path(optional_string_from_value(
        "mcp_path",
        object.get("mcp_path"),
    )?);
    object.insert("mcp_path".to_string(), Value::String(mcp_path));

    Ok(())
}

pub(crate) fn manifest_from_export_bundle(
    bundle: &super::super::ConfigExportBundle,
) -> Result<DesiredStateManifest> {
    let principal = bundle
        .agent_principal
        .as_ref()
        .ok_or_else(|| anyhow!("config export bundle is missing agent_principal"))?;

    let mut manifest = DesiredStateManifest {
        agent_principal: desired_from_value(
            principal,
            &[
                "agent_did",
                "display_name",
                "default_behavior_id",
                "enabled",
            ],
        )?,
        agent_behaviors: bundle
            .agent_behaviors
            .iter()
            .map(|value| {
                desired_from_value(
                    value,
                    &[
                        "behavior_id",
                        "agent_did",
                        "display_name",
                        "description",
                        "summary",
                        "system_prompt",
                        "request_context_template",
                        "backend_id",
                        "model_name",
                        "tool_selection_id",
                        "inference_profile_id",
                        "compaction_strategy",
                        "compaction_threshold",
                        "enabled",
                        "skill_refs",
                        "skill_excludes",
                    ],
                )
            })
            .collect::<Result<Vec<_>>>()?,
        skills: bundle
            .skills
            .iter()
            .map(|value| {
                desired_from_value(
                    value,
                    &[
                        "skill_id",
                        "agent_did",
                        "scope",
                        "name",
                        "description",
                        "instructions",
                        "tool_refs",
                        "display_name",
                        "interface_json",
                        "enabled",
                    ],
                )
            })
            .collect::<Result<Vec<_>>>()?,
        datastore_tool_surfaces: bundle
            .datastore_tool_surfaces
            .iter()
            .map(|value| {
                desired_from_value(
                    value,
                    &[
                        "surface_id",
                        "agent_did",
                        "display_name",
                        "enabled",
                        "entries",
                    ],
                )
            })
            .collect::<Result<Vec<_>>>()?,
        tool_selections: bundle
            .tool_selections
            .iter()
            .map(|value| {
                desired_from_value(
                    value,
                    &[
                        "selection_id",
                        "agent_did",
                        "display_name",
                        "tool_policy_version",
                        "enable_file_tools",
                        "file_tools_mode",
                        "file_tool_root",
                        "enable_bash",
                        "bash_mode",
                        "command_execution_policy",
                        "command_allowed_argv_prefixes",
                        "command_forbidden_argv_prefixes",
                        "read_only_command_allowlist",
                        "command_network_mode",
                        "cli_tool_names",
                        "enable_meta_tools",
                        "allowed_mcp_service_ids",
                        "delegate_to",
                        "backgroundable_tool_names",
                        "enable_memory",
                        "enable_session_history_tool",
                        "enable_context_budget",
                        "enable_defra_query",
                        "defra_query_collections",
                        "subagent_targets",
                        "subagent_spawn_enabled",
                        "subagent_steering_enabled",
                        "subagent_background_enabled",
                        "subagent_default_await_mode",
                        "subagent_allow_cross_deployment",
                        "cross_deployment_spawn_timeout_seconds",
                        "write_tools",
                        "datastore_tool_surface_ids",
                        "enable_self_config",
                        "self_config_categories",
                        "self_config_no_lockout",
                        "self_config_dry_run",
                        "enable_lsp",
                        "lsp_config",
                    ],
                )
            })
            .collect::<Result<Vec<_>>>()?,
        inference_backends: bundle
            .inference_backends
            .iter()
            .map(|value| {
                desired_from_value(
                    value,
                    &[
                        "backend_id",
                        "name",
                        "provider_kind",
                        "openai_wire_api",
                        "endpoint",
                        "api_key",
                        "api_key_env_var",
                        "max_concurrent",
                        "max_queue_depth",
                        "enabled",
                        "models",
                    ],
                )
            })
            .collect::<Result<Vec<_>>>()?,
        inference_profiles: bundle
            .inference_profiles
            .iter()
            .map(|value| {
                desired_from_value(
                    value,
                    &[
                        "profile_id",
                        "display_name",
                        "context_window",
                        "max_output_tokens",
                        "max_turns",
                        "temperature",
                        "top_p",
                        "top_k",
                        "seed",
                        "min_p",
                        "frequency_penalty",
                        "presence_penalty",
                        "repetition_penalty",
                        "reasoning_effort",
                        "stream_batch_ms",
                        "stream_liveness_timeout_secs",
                        "deadline_duration_secs",
                        "retry_max_transport",
                        "retry_backoff_ms",
                        "retry_max_resample",
                        "retry_allow_repair",
                        "retry_interactive_max",
                    ],
                )
            })
            .collect::<Result<Vec<_>>>()?,
        tool_service_registries: bundle
            .tool_service_registries
            .iter()
            .map(tool_service_registry_from_live_value)
            .collect::<Result<Vec<_>>>()?,
        projection_acp_bindings: bundle
            .projection_acp_bindings
            .iter()
            .map(|value| {
                desired_from_value(
                    value,
                    &[
                        "binding_id",
                        "agent_did",
                        "behavior_id",
                        "projection_id",
                        "policy_id",
                        "staged_policy_id",
                        "previous_policy_id",
                        "resource_map_json",
                        "publication_status",
                        "published_at",
                        "enabled",
                    ],
                )
            })
            .collect::<Result<Vec<_>>>()?,
        peer_pairings: bundle
            .peer_pairings
            .iter()
            .map(peer_pairing_from_live_value)
            .collect::<Result<Vec<_>>>()?,
        tasks: bundle
            .tasks
            .iter()
            .map(|value| {
                desired_from_value(
                    value,
                    &[
                        "task_id",
                        "name",
                        "description",
                        "behavior_id",
                        "prompt_template",
                        "enabled",
                        "output_schema_ref",
                    ],
                )
            })
            .collect::<Result<Vec<_>>>()?,
        schedules: bundle
            .schedules
            .iter()
            .map(|value| {
                desired_from_value(
                    value,
                    &[
                        "schedule_id",
                        "task_id",
                        "interval_secs",
                        "cron",
                        "timezone",
                        "missed_run_policy",
                        "enabled",
                        "concurrency",
                    ],
                )
            })
            .collect::<Result<Vec<_>>>()?,
        event_triggers: bundle
            .event_triggers
            .iter()
            .map(|value| {
                desired_from_value(
                    value,
                    &[
                        "trigger_id",
                        "task_id",
                        "source_collection",
                        "event_kind",
                        "filter",
                        "correlation_field",
                        "fire_mode",
                        "expected_count",
                        "expected_count_field",
                        "group_timeout_secs",
                        "group_min_count",
                        "workspace_authority",
                        "enabled",
                        "concurrency",
                    ],
                )
            })
            .collect::<Result<Vec<_>>>()?,
        callback_bindings: Vec::new(),
        repository_placements: Vec::new(),
    };
    normalize_manifest(&mut manifest);
    Ok(manifest)
}

pub(crate) fn export_bundle_from_manifest(
    manifest: &DesiredStateManifest,
    access_mode: &str,
) -> Result<super::DesiredApplyBundle> {
    let bundle = super::super::ConfigExportBundle {
        format: super::super::CONFIG_EXPORT_FORMAT.to_string(),
        agent_did: manifest.agent_principal.agent_did.clone(),
        exported_at: chrono::Utc::now().to_rfc3339(),
        access_mode: access_mode.to_string(),
        agent_principal: Some(serde_json::to_value(&manifest.agent_principal)?),
        agent_behaviors: manifest
            .agent_behaviors
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
        skills: manifest
            .skills
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
        datastore_tool_surfaces: manifest
            .datastore_tool_surfaces
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
        // WorkspaceRoot isn't tracked in DesiredStateManifest yet; see
        // shared::ConfigExportBundle::workspace_roots.
        workspace_roots: Vec::new(),
        tool_selections: manifest
            .tool_selections
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
        inference_backends: manifest
            .inference_backends
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
        inference_profiles: manifest
            .inference_profiles
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
        tool_service_registries: manifest
            .tool_service_registries
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
        projection_acp_bindings: manifest
            .projection_acp_bindings
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
        peer_pairings: manifest
            .peer_pairings
            .iter()
            .filter(|pairing| pairing.enabled)
            .map(|pairing| peer_pairing_apply_value(pairing, manifest))
            .collect::<Result<Vec<_>>>()?,
        tasks: manifest
            .tasks
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
        schedules: manifest
            .schedules
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
        event_triggers: manifest
            .event_triggers
            .iter()
            .map(serde_json::to_value)
            .collect::<serde_json::Result<Vec<_>>>()?,
    };
    Ok(super::DesiredApplyBundle::from_trusted_bundle(bundle))
}

fn peer_pairing_from_live_value(value: &Value) -> Result<DesiredPeerPairing> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("expected PeerPairingDesired live row to be an object"))?;
    let peer_id = object
        .get("peer_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("PeerPairingDesired live row is missing peer_id"))?;
    let peer_did = object
        .get("agent_did")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    let addresses = object
        .get("replicator_addresses")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let template = object
        .get("template")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(gents::agent::p2p_reconcile::engine::DEFAULT_PAIRING_TEMPLATE);

    Ok(DesiredPeerPairing {
        peer_did: peer_did.to_string(),
        addresses,
        template: template.to_string(),
        enabled: true,
        peer_id: peer_id.to_string(),
    })
}

fn peer_pairing_apply_value(
    pairing: &DesiredPeerPairing,
    manifest: &DesiredStateManifest,
) -> Result<Value> {
    let peer_id = pairing.resolved_peer_id().ok_or_else(|| {
        anyhow!(
            "enabled peer pairing {:?} has no derivable peer_id",
            pairing.peer_did
        )
    })?;
    let template = gents::agent::p2p_reconcile::resolve_template(&pairing.template)
        .ok_or_else(|| anyhow!("unknown peer pairing template {:?}", pairing.template))?;
    let collections = template
        .collections
        .iter()
        .map(|collection| Value::String((*collection).to_string()))
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "peer_id": peer_id,
        "agent_did": pairing.peer_did,
        "collections": collections,
        "replicator_addresses": pairing.addresses,
        "profiles": Value::Null,
        "template": pairing.template,
        "source": peer_pairing_manifest_source(&manifest.agent_principal.agent_did),
    }))
}

pub(crate) fn desired_from_value<T>(value: &Value, allowed_fields: &[&str]) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("expected object while projecting desired-state document"))?;
    let projected = allowed_fields
        .iter()
        .filter_map(|field| {
            object
                .get(*field)
                .filter(|value| !value.is_null())
                .map(|value| ((*field).to_string(), value.clone()))
        })
        .collect::<Map<String, Value>>();
    Ok(serde_json::from_value(Value::Object(projected))?)
}
