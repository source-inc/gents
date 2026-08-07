use serde_json::{Map, Value};

use super::DesiredStateManifest;

pub(crate) fn normalize_manifest(manifest: &mut DesiredStateManifest) {
    normalize_optional_string(&mut manifest.agent_principal.display_name);
    normalize_optional_string(&mut manifest.agent_principal.default_behavior_id);

    manifest
        .agent_behaviors
        .sort_by(|left, right| left.behavior_id.cmp(&right.behavior_id));
    manifest
        .tool_selections
        .sort_by(|left, right| left.selection_id.cmp(&right.selection_id));
    manifest
        .inference_backends
        .sort_by(|left, right| left.backend_id.cmp(&right.backend_id));
    manifest
        .inference_profiles
        .sort_by(|left, right| left.profile_id.cmp(&right.profile_id));
    manifest
        .tool_service_registries
        .sort_by(|left, right| left.service_id.cmp(&right.service_id));
    manifest
        .projection_acp_bindings
        .sort_by(|left, right| left.binding_id.cmp(&right.binding_id));
    manifest
        .peer_pairings
        .sort_by(|left, right| left.peer_did.cmp(&right.peer_did));
    manifest
        .tasks
        .sort_by(|left, right| left.task_id.cmp(&right.task_id));
    manifest
        .schedules
        .sort_by(|left, right| left.schedule_id.cmp(&right.schedule_id));
    manifest
        .event_triggers
        .sort_by(|left, right| left.trigger_id.cmp(&right.trigger_id));

    for behavior in &mut manifest.agent_behaviors {
        normalize_optional_string(&mut behavior.display_name);
        normalize_optional_string(&mut behavior.system_prompt);
        normalize_optional_string(&mut behavior.request_context_template);
        normalize_optional_string(&mut behavior.backend_id);
        normalize_optional_string(&mut behavior.model_name);
        normalize_optional_string(&mut behavior.tool_selection_id);
        normalize_optional_string(&mut behavior.inference_profile_id);
        normalize_optional_string(&mut behavior.compaction_strategy);
    }
    for selection in &mut manifest.tool_selections {
        normalize_optional_string(&mut selection.display_name);
        normalize_optional_string(&mut selection.file_tool_root);
        normalize_optional_string(&mut selection.command_execution_policy);
        normalize_optional_string(&mut selection.command_network_mode);
        selection.command_allowed_argv_prefixes.sort();
        selection.command_allowed_argv_prefixes.dedup();
        selection.command_forbidden_argv_prefixes.sort();
        selection.command_forbidden_argv_prefixes.dedup();
        selection.read_only_command_allowlist.sort();
        selection.read_only_command_allowlist.dedup();
        selection.cli_tool_names.sort();
        selection.cli_tool_names.dedup();
        selection.allowed_mcp_service_ids.sort();
        selection.allowed_mcp_service_ids.dedup();
        selection.backgroundable_tool_names.sort();
        selection.backgroundable_tool_names.dedup();
        selection.subagent_targets.sort();
        selection.subagent_targets.dedup();
        selection.self_config_categories.sort();
        selection.self_config_categories.dedup();
    }
    for backend in &mut manifest.inference_backends {
        normalize_optional_string(&mut backend.api_key);
        normalize_optional_string(&mut backend.api_key_env_var);
        backend.models.sort();
        backend.models.dedup();
    }
    for profile in &mut manifest.inference_profiles {
        normalize_optional_string(&mut profile.display_name);
        normalize_optional_string(&mut profile.reasoning_effort);
    }
    for task in &mut manifest.tasks {
        normalize_optional_string(&mut task.description);
        normalize_optional_string(&mut task.output_schema_ref);
    }
    for trigger in &mut manifest.event_triggers {
        normalize_optional_string(&mut trigger.filter);
    }
    for binding in &mut manifest.projection_acp_bindings {
        normalize_optional_string(&mut binding.agent_did);
        normalize_optional_string(&mut binding.behavior_id);
        normalize_optional_string(&mut binding.projection_id);
        normalize_optional_string(&mut binding.staged_policy_id);
        normalize_optional_string(&mut binding.previous_policy_id);
        normalize_optional_string(&mut binding.resource_map_json);
        normalize_optional_string(&mut binding.publication_status);
        normalize_optional_string(&mut binding.published_at);
    }
    for pairing in &mut manifest.peer_pairings {
        pairing.peer_did = pairing.peer_did.trim().to_string();
        pairing.template = pairing.template.trim().to_string();
        pairing.addresses = pairing
            .addresses
            .iter()
            .map(|address| address.trim())
            .filter(|address| !address.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        pairing.addresses.sort();
        pairing.addresses.dedup();
        if pairing.peer_id.trim().is_empty() {
            pairing.peer_id = pairing.resolved_peer_id().unwrap_or_default();
        }
    }
}

pub(crate) fn default_max_queue_depth() -> i64 {
    100
}

fn normalize_optional_string(value: &mut Option<String>) {
    *value = value
        .as_deref()
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .map(ToOwned::to_owned);
}

pub(crate) fn strip_deprecated_inference_backend_fields(object: &mut Map<String, Value>) {
    for field in [
        "supports_tool_calls",
        "supports_streaming",
        "supports_structured_outputs",
        "supports_json_schema",
        "context_window",
        "max_output_tokens",
    ] {
        object.remove(field);
    }
}
