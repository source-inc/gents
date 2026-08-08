use std::collections::{BTreeSet, HashSet};

use anyhow::Result;
use gents::template::{
    catalog::{default_catalog, Site},
    reads::validate_system_template,
    validate_request_context_template,
};
use gents::{
    is_reserved_builtin_tool_name, parse_template_for_validation,
    schedule_cron::validate_cron_schedule, CommandExecutionMode, CommandNetworkMode,
    SubagentTarget, VariableRef, WriteToolDecl,
};

use super::{DesiredDatastoreToolSurface, DesiredStateManifest, DesiredToolSelection};

use crate::config_writes::ConfigAccess;

const PROJECTION_ACP_BINDING_PROJECTION_IDS: &[&str] = &[
    "openai_codex_run_trace",
    "langgraph_state_history",
    "multi_agent_task",
];

const PROJECTION_ACP_RUNTIME_COLLECTIONS: &[&str] = &[
    "AgentRequest",
    "AgentMessage",
    "AgentToolCall",
    "AgentResponse",
    "AgentSession",
    "AgentConversation",
];

pub(crate) fn validate_manifest(manifest: &DesiredStateManifest, errors: &mut Vec<String>) {
    let principal_agent_did = manifest.agent_principal.agent_did.trim();
    if principal_agent_did.is_empty() {
        errors.push("agent-principal.json must contain a non-empty agent_did".to_string());
    }

    let mut pairing_dids = BTreeSet::new();
    let mut pairing_peer_ids = BTreeSet::new();
    for pairing in &manifest.peer_pairings {
        let peer_did = pairing.peer_did.trim();
        if peer_did.is_empty() {
            errors.push("peer-pairings manifest contains an empty peer_did".to_string());
        } else {
            if !pairing_dids.insert(peer_did.to_string()) {
                errors.push(format!(
                    "duplicate peer_did in peer-pairings manifest: {peer_did}"
                ));
            }
            if !principal_agent_did.is_empty() && peer_did == principal_agent_did {
                errors.push(format!(
                    "peer pairing {peer_did} points at this manifest's own agent_did"
                ));
            }
        }

        let template = pairing.template.trim();
        if template.is_empty() {
            errors.push(format!(
                "peer pairing {peer_did:?} must contain a non-empty template"
            ));
        } else {
            use gents::agent::p2p_reconcile::templates::{
                builtin_templates, resolve_template, APP_COLLECTIONS_TEMPLATE,
            };
            if template == APP_COLLECTIONS_TEMPLATE {
                errors.push(format!(
                    "peer pairing {peer_did:?} uses data-plane-only template {template:?}"
                ));
            } else if resolve_template(template).is_none() {
                let known = builtin_templates()
                    .iter()
                    .map(|template| template.id)
                    .collect::<Vec<_>>()
                    .join(", ");
                errors.push(format!(
                    "peer pairing {peer_did:?} has unknown template {template:?}; known templates: {known}"
                ));
            }
        }

        if pairing.enabled && pairing.addresses.is_empty() {
            errors.push(format!(
                "enabled peer pairing {peer_did:?} must contain at least one address"
            ));
        }
        let mut row_peer_id = None::<String>;
        for address in &pairing.addresses {
            match p2p::iroh::parse_public_peer_addr(address.trim()) {
                Ok((peer_id, _)) => {
                    let peer_id = peer_id.to_string();
                    if let Err(error) = peer_id.parse::<iroh::EndpointId>() {
                        errors.push(format!(
                            "peer pairing {peer_did:?} address {address:?} has invalid iroh peer id {peer_id:?}: {error}"
                        ));
                        continue;
                    }
                    if let Some(expected) = row_peer_id.as_deref() {
                        if expected != peer_id {
                            errors.push(format!(
                                "peer pairing {peer_did:?} mixes addresses for peer ids {expected:?} and {peer_id:?}"
                            ));
                        }
                    } else {
                        row_peer_id = Some(peer_id);
                    }
                }
                Err(error) => errors.push(format!(
                    "peer pairing {peer_did:?} has invalid address {address:?}: {error}"
                )),
            }
        }
        if let Some(peer_id) = row_peer_id {
            if !pairing_peer_ids.insert(peer_id.clone()) {
                errors.push(format!(
                    "duplicate peer_id {peer_id:?} derived by peer-pairings manifest"
                ));
            }
        }
    }

    let mut behavior_ids = BTreeSet::new();
    let mut backend_ids = BTreeSet::new();
    let mut tool_selection_ids = BTreeSet::new();

    let mut surface_ids = BTreeSet::new();
    for surface in &manifest.datastore_tool_surfaces {
        let surface_id = surface.surface_id.trim();
        if surface_id.is_empty() {
            errors.push("DatastoreToolSurface has empty surface_id".to_string());
        } else if !surface_ids.insert(surface_id.to_string()) {
            errors.push(format!(
                "duplicate DatastoreToolSurface surface_id {surface_id}"
            ));
        }
        if !principal_agent_did.is_empty() && surface.agent_did.trim() != principal_agent_did {
            errors.push(format!(
                "DatastoreToolSurface {} agent_did does not match principal",
                surface.surface_id
            ));
        }
        validate_write_tools(
            &format!("surface:{}", surface.surface_id),
            &surface.entries,
            &[],
            errors,
        );
    }

    let mut profile_ids = BTreeSet::new();
    let mut service_ids = BTreeSet::new();
    let mut projection_binding_ids = BTreeSet::new();

    for backend in &manifest.inference_backends {
        let backend_id = backend.backend_id.trim();
        if backend_id.is_empty() {
            errors.push(
                "inference-backends.json contains a backend with an empty backend_id".to_string(),
            );
        } else if !backend_ids.insert(backend_id.to_string()) {
            errors.push(format!(
                "duplicate backend_id in inference-backends.json: {backend_id}"
            ));
        }

        if backend.endpoint.trim().is_empty() {
            errors.push(format!(
                "backend {} in inference-backends.json must contain a non-empty endpoint",
                backend.backend_id
            ));
        }

        if backend
            .api_key
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| value.is_empty())
        {
            errors.push(format!(
                "backend {} in inference-backends.json contains an empty api_key",
                backend.backend_id
            ));
        }

        if backend
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
            && backend
                .api_key_env_var
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some()
        {
            errors.push(format!(
                "backend {} in inference-backends.json must not set both api_key and api_key_env_var",
                backend.backend_id
            ));
        }
    }

    for selection in &manifest.tool_selections {
        let selection_id = selection.selection_id.trim();
        if selection_id.is_empty() {
            errors.push(
                "tool-selections.json contains a tool selection with an empty selection_id"
                    .to_string(),
            );
        } else if !tool_selection_ids.insert(selection_id.to_string()) {
            errors.push(format!(
                "duplicate selection_id in tool-selections.json: {selection_id}"
            ));
        }

        if !principal_agent_did.is_empty() && selection.agent_did.trim() != principal_agent_did {
            errors.push(format!(
                "tool selection {} belongs to {} not {}",
                selection.selection_id, selection.agent_did, manifest.agent_principal.agent_did
            ));
        }

        if let Some(mode) = selection
            .subagent_default_await_mode
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            match mode {
                "foreground" => {}
                "background" if selection.subagent_background_enabled => {}
                "background" => errors.push(format!(
                    "tool selection {} sets subagent_default_await_mode=background but subagent_background_enabled is false",
                    selection.selection_id
                )),
                other => errors.push(format!(
                    "tool selection {} has invalid subagent_default_await_mode {other:?}; expected foreground or background",
                    selection.selection_id
                )),
            }
        }

        if let Some(mode) = selection.command_execution_policy.as_deref() {
            if let Err(error) = CommandExecutionMode::parse(mode) {
                errors.push(format!(
                    "tool selection {} has invalid command_execution_policy: {error}",
                    selection.selection_id
                ));
            }
        }

        for (index, tool_name) in selection.backgroundable_tool_names.iter().enumerate() {
            if tool_name.trim().is_empty() {
                errors.push(format!(
                    "tool selection {} has empty backgroundable_tool_names[{index}]",
                    selection.selection_id
                ));
            }
        }
        for (index, target) in selection.subagent_targets.iter().enumerate() {
            if target.trim().is_empty() {
                errors.push(format!(
                    "tool selection {} has empty subagent_targets[{index}]",
                    selection.selection_id
                ));
            }
        }
        if let Some(mode) = selection.command_network_mode.as_deref() {
            if let Err(error) = CommandNetworkMode::parse(mode) {
                errors.push(format!(
                    "tool selection {} has invalid command_network_mode: {error}",
                    selection.selection_id
                ));
            }
        }
        validate_argv_prefixes(
            &selection.selection_id,
            "command_allowed_argv_prefixes",
            &selection.command_allowed_argv_prefixes,
            errors,
        );
        validate_argv_prefixes(
            &selection.selection_id,
            "command_forbidden_argv_prefixes",
            &selection.command_forbidden_argv_prefixes,
            errors,
        );
        validate_non_empty_values(
            &selection.selection_id,
            "allowed_mcp_service_ids",
            &selection.allowed_mcp_service_ids,
            errors,
        );
        validate_subagent_targets(
            &selection.selection_id,
            selection.agent_did.trim(),
            selection.subagent_allow_cross_deployment,
            &selection.subagent_targets,
            errors,
        );
        // Field-level write-tool checks run once, inside the link validation,
        // over the merged inline ∪ surface list (which equals the inline list
        // when no surfaces are linked).
        validate_datastore_surface_links(manifest, selection, errors);
        if selection.subagent_spawn_enabled {
            if selection.subagent_targets.is_empty() {
                errors.push(format!(
                    "tool selection {} sets subagent_spawn_enabled but has no subagent_targets; the tools would be inert",
                    selection.selection_id
                ));
            }
        }
    }

    for profile in &manifest.inference_profiles {
        let profile_id = profile.profile_id.trim();
        if profile_id.is_empty() {
            errors.push(
                "inference-profiles.json contains a profile with an empty profile_id".to_string(),
            );
        } else if !profile_ids.insert(profile_id.to_string()) {
            errors.push(format!(
                "duplicate profile_id in inference-profiles.json: {profile_id}"
            ));
        }
        if profile
            .stream_liveness_timeout_secs
            .is_some_and(|value| value <= 0)
        {
            errors.push(format!(
                "InferenceProfile {profile_id} stream_liveness_timeout_secs must be positive"
            ));
        }
        // An empty value is not an invalid reasoning level, it is an *unset*
        // one. `document_config::graphql_string_field` writes `""` for a
        // `None` reasoning effort, so every profile `gents init` creates
        // materializes as an empty string, `config export` copies it into the
        // manifest, and rejecting it here would make the CLI refuse to apply
        // the manifest it just exported. The runtime already resolves it the
        // same way (`gents::agent`, "older/default Defra rows may materialize
        // nullable strings as an empty value").
        if profile
            .reasoning_effort
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some_and(|value| {
                !matches!(
                    value,
                    "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max" | "ultra"
                )
            })
        {
            errors.push(format!(
                "InferenceProfile {profile_id} reasoning_effort must be one of: none, minimal, low, medium, high, xhigh, max, ultra"
            ));
        }
    }

    for service in &manifest.tool_service_registries {
        let service_id = service.service_id.trim();
        if service_id.is_empty() {
            errors.push(
                "tool-services manifest contains a service with an empty service_id".to_string(),
            );
        } else if !service_ids.insert(service_id.to_string()) {
            errors.push(format!(
                "duplicate service_id in tool-services manifest: {service_id}"
            ));
        }

        if service.mcp_port.unwrap_or_default() <= 0 {
            errors.push(format!(
                "service {} in tool-services manifest must contain a positive mcp_port",
                service.service_id
            ));
        }

        if non_empty(&service.hostname).is_none()
            && non_empty(&service.tailscale_ip).is_none()
            && non_empty(&service.lan_ip).is_none()
        {
            errors.push(format!(
                "service {} in tool-services manifest must contain at least one of hostname, tailscale_ip, or lan_ip",
                service.service_id
            ));
        }
    }

    let mut skill_ids = BTreeSet::new();
    for skill in &manifest.skills {
        let skill_id = skill.skill_id.trim();
        if skill_id.is_empty() {
            errors.push("skills manifest contains a skill with an empty skill_id".to_string());
        } else if !skill_ids.insert(skill_id.to_string()) {
            errors.push(format!("duplicate skill_id in skills manifest: {skill_id}"));
        }

        if !principal_agent_did.is_empty() && skill.agent_did.trim() != principal_agent_did {
            errors.push(format!(
                "skill {} belongs to {} not {}",
                skill.skill_id, skill.agent_did, manifest.agent_principal.agent_did
            ));
        }

        if !matches!(skill.scope.trim(), "principal" | "behavior") {
            errors.push(format!(
                "skill {} has invalid scope {:?}; expected \"principal\" or \"behavior\"",
                skill.skill_id, skill.scope
            ));
        }

        if skill.name.trim().is_empty() {
            errors.push(format!(
                "skill {} in skills manifest must contain a non-empty name",
                skill.skill_id
            ));
        }
    }

    for behavior in &manifest.agent_behaviors {
        let behavior_id = behavior.behavior_id.trim();
        if behavior_id.is_empty() {
            errors.push(
                "agent-behaviors.json contains a behavior with an empty behavior_id".to_string(),
            );
        } else if !behavior_ids.insert(behavior_id.to_string()) {
            errors.push(format!(
                "duplicate behavior_id in agent-behaviors.json: {behavior_id}"
            ));
        }

        if !principal_agent_did.is_empty() && behavior.agent_did.trim() != principal_agent_did {
            errors.push(format!(
                "behavior {} belongs to {} not {}",
                behavior.behavior_id, behavior.agent_did, manifest.agent_principal.agent_did
            ));
        }

        if let Some(backend_id) = non_empty(&behavior.backend_id) {
            if !backend_ids.contains(backend_id) {
                errors.push(format!(
                    "behavior {} references missing backend_id {}",
                    behavior.behavior_id, backend_id
                ));
            }
        }

        if let Some(selection_id) = non_empty(&behavior.tool_selection_id) {
            if !tool_selection_ids.contains(selection_id) {
                errors.push(format!(
                    "behavior {} references missing tool_selection_id {}",
                    behavior.behavior_id, selection_id
                ));
            }
        }

        if let Some(profile_id) = non_empty(&behavior.inference_profile_id) {
            if !profile_ids.contains(profile_id) {
                errors.push(format!(
                    "behavior {} references missing inference_profile_id {}",
                    behavior.behavior_id, profile_id
                ));
            }
        }

        if let Some(system_prompt) = behavior.system_prompt.as_deref() {
            validate_behavior_system_template(&behavior.behavior_id, system_prompt, errors);
        }

        if let Some(request_context_template) = behavior.request_context_template.as_deref() {
            validate_behavior_request_context_template(
                &behavior.behavior_id,
                request_context_template,
                errors,
            );
        }

        for skill_ref in &behavior.skill_refs {
            let skill_ref = skill_ref.trim();
            if !skill_ref.is_empty() && !skill_ids.contains(skill_ref) {
                errors.push(format!(
                    "behavior {} references missing skill_ref {} (import the skill first)",
                    behavior.behavior_id, skill_ref
                ));
            }
        }
        for skill_exclude in &behavior.skill_excludes {
            let skill_exclude = skill_exclude.trim();
            if !skill_exclude.is_empty() && !skill_ids.contains(skill_exclude) {
                errors.push(format!(
                    "behavior {} references missing skill_exclude {}",
                    behavior.behavior_id, skill_exclude
                ));
            }
        }
    }

    for binding in &manifest.projection_acp_bindings {
        let binding_id = binding.binding_id.trim();
        if binding_id.is_empty() {
            errors.push(
                "projection-acp-bindings manifest contains a binding with an empty binding_id"
                    .to_string(),
            );
        } else if !projection_binding_ids.insert(binding_id.to_string()) {
            errors.push(format!(
                "duplicate binding_id in projection-acp-bindings manifest: {binding_id}"
            ));
        }

        if non_empty(&binding.agent_did).is_none() {
            errors.push(format!(
                "projection ACP binding {} must contain a non-empty agent_did",
                binding.binding_id
            ));
        } else if !principal_agent_did.is_empty()
            && non_empty(&binding.agent_did) != Some(principal_agent_did)
        {
            errors.push(format!(
                "projection ACP binding {} belongs to {} not {}",
                binding.binding_id,
                binding.agent_did.as_deref().unwrap_or_default(),
                manifest.agent_principal.agent_did
            ));
        }

        if binding.policy_id.trim().is_empty() {
            errors.push(format!(
                "projection ACP binding {} must contain a non-empty policy_id",
                binding.binding_id
            ));
        }

        if let Some(behavior_id) = non_empty(&binding.behavior_id) {
            if !behavior_ids.contains(behavior_id) {
                errors.push(format!(
                    "projection ACP binding {} references missing behavior_id {}",
                    binding.binding_id, behavior_id
                ));
            }
        }

        validate_projection_id(binding, errors);
        validate_projection_policy_lifecycle(binding, errors);
        validate_projection_resource_map_json(binding, errors);
    }

    match non_empty(&manifest.agent_principal.default_behavior_id) {
        Some(default_behavior_id) => {
            if !behavior_ids.contains(default_behavior_id) {
                errors.push(format!(
                    "agent-principal.json default_behavior_id {} is not present in agent-behaviors.json",
                    default_behavior_id
                ));
            }
        }
        None => errors
            .push("agent-principal.json must contain a non-empty default_behavior_id".to_string()),
    }

    let mut task_ids = BTreeSet::new();
    for task in &manifest.tasks {
        let task_id = task.task_id.trim();
        if task_id.is_empty() {
            errors.push("tasks manifest contains a task with an empty task_id".to_string());
        } else if !task_ids.insert(task_id.to_string()) {
            errors.push(format!("duplicate task_id in tasks manifest: {task_id}"));
        }

        if task.name.trim().is_empty() {
            errors.push(format!(
                "task {} in tasks manifest must contain a non-empty name",
                task.task_id
            ));
        }

        let behavior_id = task.behavior_id.trim();
        if behavior_id.is_empty() {
            errors.push(format!(
                "task {} in tasks manifest must contain a non-empty behavior_id",
                task.task_id
            ));
        } else if !behavior_ids.contains(behavior_id) {
            errors.push(format!(
                "task {} references missing behavior_id {}",
                task.task_id, behavior_id
            ));
        }

        validate_task_template_catalog_scope(task, errors);
    }

    let mut schedule_ids = BTreeSet::new();
    for schedule in &manifest.schedules {
        let schedule_id = schedule.schedule_id.trim();
        if schedule_id.is_empty() {
            errors.push(
                "schedules manifest contains a schedule with an empty schedule_id".to_string(),
            );
        } else if !schedule_ids.insert(schedule_id.to_string()) {
            errors.push(format!(
                "duplicate schedule_id in schedules manifest: {schedule_id}"
            ));
        }

        let task_id = schedule.task_id.trim();
        if task_id.is_empty() {
            errors.push(format!(
                "schedule {} in schedules manifest must contain a non-empty task_id",
                schedule.schedule_id
            ));
        } else if !task_ids.contains(task_id) {
            errors.push(format!(
                "schedule {} references missing task_id {}",
                schedule.schedule_id, task_id
            ));
        }

        validate_schedule_cadence(schedule, errors);

        match schedule.concurrency.trim() {
            "parallel" | "serial" | "latest_only" => {}
            other => errors.push(format!(
                "schedule {} in schedules manifest has unknown concurrency {}",
                schedule.schedule_id, other
            )),
        }

        if !task_id.is_empty() {
            if let Some(task) = manifest.tasks.iter().find(|task| task.task_id == task_id) {
                match parse_template_for_validation(&task.prompt_template) {
                    Ok(refs) => {
                        let mut reported: BTreeSet<&str> = BTreeSet::new();
                        for var in &refs {
                            if let Some(root) = var.root() {
                                if (root == "doc" || root == "args") && reported.insert(root) {
                                    errors.push(format!(
                                        "schedule {} prompt template references forbidden scope: {}; schedule scope only permits event.*, node.*, and ctx.now",
                                        schedule.schedule_id,
                                        format_variable_ref(var),
                                    ));
                                }
                            }
                        }
                    }
                    Err(err) => errors.push(format!(
                        "schedule {} prompt template failed to parse: {}",
                        schedule.schedule_id, err
                    )),
                }
            }
        }
    }

    let mut event_trigger_ids = BTreeSet::new();
    for trig in &manifest.event_triggers {
        let trigger_id = trig.trigger_id.trim();
        if trigger_id.is_empty() {
            errors.push(
                "event-triggers manifest contains a trigger with an empty trigger_id".to_string(),
            );
            continue;
        }
        if !event_trigger_ids.insert(trigger_id.to_string()) {
            errors.push(format!(
                "duplicate trigger_id in event-triggers manifest: {trigger_id}"
            ));
        }

        let task_id = trig.task_id.trim();
        if task_id.is_empty() {
            errors.push(format!(
                "event_trigger {} in event-triggers manifest must contain a non-empty task_id",
                trig.trigger_id
            ));
        }

        if trig.source_collection.trim().is_empty() {
            errors.push(format!(
                "event_trigger {} in event-triggers manifest must contain a non-empty source_collection",
                trig.trigger_id
            ));
        }

        if trig.event_kind != "created" {
            errors.push(format!(
                "event_trigger {} uses unsupported event_kind {:?} (v1 supports only \"created\")",
                trig.trigger_id, trig.event_kind
            ));
        }

        match trig.concurrency.trim() {
            "parallel" | "serial" | "latest_only" => {}
            other => errors.push(format!(
                "event_trigger {} in event-triggers manifest has unknown concurrency {}; expected parallel|serial|latest_only",
                trig.trigger_id, other
            )),
        }

        if !task_id.is_empty() && !manifest.tasks.iter().any(|t| t.task_id == task_id) {
            errors.push(format!(
                "event_trigger {} references unknown task_id {}",
                trig.trigger_id, trig.task_id
            ));
        }

        if !task_id.is_empty() {
            if let Some(task) = manifest.tasks.iter().find(|t| t.task_id == task_id) {
                match parse_template_for_validation(&task.prompt_template) {
                    Ok(refs) => {
                        let mut reported: BTreeSet<&str> = BTreeSet::new();
                        for vref in &refs {
                            if let Some(root) = vref.root() {
                                if root == "args" && reported.insert("args") {
                                    errors.push(format!(
                                        "event_trigger {} prompt template references forbidden scope: args; event scope only permits event.*, doc.*, node.*, and ctx.now",
                                        trig.trigger_id
                                    ));
                                }
                            }
                        }
                    }
                    Err(err) => errors.push(format!(
                        "event_trigger {} prompt template failed to parse: {}",
                        trig.trigger_id, err
                    )),
                }
            }
        }
    }
}

fn validate_behavior_system_template(
    behavior_id: &str,
    system_prompt: &str,
    errors: &mut Vec<String>,
) {
    if !contains_template_marker(system_prompt) {
        return;
    }
    let catalog = default_catalog();
    if let Err(error) = validate_system_template(system_prompt, &catalog) {
        errors.push(format!(
            "behavior {behavior_id} system_prompt template is invalid: {error}"
        ));
    }
}

fn validate_behavior_request_context_template(
    behavior_id: &str,
    request_context_template: &str,
    errors: &mut Vec<String>,
) {
    if !contains_template_marker(request_context_template) {
        return;
    }
    let catalog = default_catalog();
    if let Err(error) = validate_request_context_template(request_context_template, &catalog) {
        errors.push(format!(
            "behavior {behavior_id} request_context_template is invalid: {error}"
        ));
    }
}

fn validate_task_template_catalog_scope(task: &super::DesiredTask, errors: &mut Vec<String>) {
    let refs = match parse_template_for_validation(&task.prompt_template) {
        Ok(refs) => refs,
        Err(error) => {
            errors.push(format!(
                "task {} prompt template failed to parse: {}",
                task.task_id, error
            ));
            return;
        }
    };
    let catalog = default_catalog();
    let mut reported: BTreeSet<String> = BTreeSet::new();
    for var in refs {
        let Some(root) = var.root() else {
            continue;
        };
        if root != "node" && root != "ctx" {
            continue;
        }
        let full_ref = format_variable_ref(&var);
        if catalog.is_available_at(&full_ref, Site::Task) {
            continue;
        }
        if reported.insert(full_ref.clone()) {
            errors.push(format!(
                "task {} prompt_template references unavailable template variable {}; task scope permits node.node_did, node.behavior_id, and ctx.now",
                task.task_id, full_ref
            ));
        }
    }
}

fn contains_template_marker(value: &str) -> bool {
    value.contains("{{") || value.contains("{%") || value.contains("{#")
}

fn validate_projection_resource_map_json(
    binding: &super::DesiredProjectionAcpBinding,
    errors: &mut Vec<String>,
) {
    let Some(raw) = non_empty(&binding.resource_map_json) else {
        return;
    };
    let parsed = serde_json::from_str::<std::collections::BTreeMap<String, String>>(raw);
    let resource_map = match parsed {
        Ok(resource_map) => resource_map,
        Err(error) => {
            errors.push(format!(
                "projection ACP binding {} has invalid resource_map_json: {}",
                binding.binding_id, error
            ));
            return;
        }
    };
    for (collection, resource_name) in resource_map {
        let collection = collection.trim();
        let resource_name = resource_name.trim();
        if collection.is_empty() || resource_name.is_empty() {
            errors.push(format!(
                "projection ACP binding {} resource_map_json must map non-empty collection names to non-empty ACP resource names",
                binding.binding_id
            ));
            break;
        }
        if !PROJECTION_ACP_RUNTIME_COLLECTIONS.contains(&collection) {
            errors.push(format!(
                "projection ACP binding {} resource_map_json contains unknown runtime collection {}; expected one of {}",
                binding.binding_id,
                collection,
                PROJECTION_ACP_RUNTIME_COLLECTIONS.join(", ")
            ));
        }
    }
}

fn validate_projection_id(binding: &super::DesiredProjectionAcpBinding, errors: &mut Vec<String>) {
    let Some(projection_id) = non_empty(&binding.projection_id) else {
        return;
    };
    if !PROJECTION_ACP_BINDING_PROJECTION_IDS.contains(&projection_id) {
        errors.push(format!(
            "projection ACP binding {} has invalid projection_id {}; expected one of {}",
            binding.binding_id,
            projection_id,
            PROJECTION_ACP_BINDING_PROJECTION_IDS.join(", ")
        ));
    }
}

fn validate_projection_policy_lifecycle(
    binding: &super::DesiredProjectionAcpBinding,
    errors: &mut Vec<String>,
) {
    let policy_id = binding.policy_id.trim();
    let staged_policy_id = non_empty(&binding.staged_policy_id);
    let previous_policy_id = non_empty(&binding.previous_policy_id);
    if let Some(staged_policy_id) = staged_policy_id {
        if staged_policy_id == policy_id {
            errors.push(format!(
                "projection ACP binding {} staged_policy_id must differ from active policy_id",
                binding.binding_id
            ));
        }
        if previous_policy_id == Some(staged_policy_id) {
            errors.push(format!(
                "projection ACP binding {} staged_policy_id must differ from previous_policy_id",
                binding.binding_id
            ));
        }
    }
    if previous_policy_id == Some(policy_id) {
        errors.push(format!(
            "projection ACP binding {} previous_policy_id must differ from active policy_id",
            binding.binding_id
        ));
    }

    match non_empty(&binding.publication_status) {
        None => {
            if staged_policy_id.is_some() {
                errors.push(format!(
                    "projection ACP binding {} staged_policy_id requires publication_status staged or rotating",
                    binding.binding_id
                ));
            }
        }
        Some("draft") => {
            if binding.enabled {
                errors.push(format!(
                    "projection ACP binding {} publication_status draft must not be enabled",
                    binding.binding_id
                ));
            }
            if staged_policy_id.is_some() {
                errors.push(format!(
                    "projection ACP binding {} publication_status draft must not keep staged_policy_id",
                    binding.binding_id
                ));
            }
        }
        Some("staged") => {
            if binding.enabled {
                errors.push(format!(
                    "projection ACP binding {} publication_status staged must not be enabled",
                    binding.binding_id
                ));
            }
            if staged_policy_id.is_none() {
                errors.push(format!(
                    "projection ACP binding {} publication_status staged requires staged_policy_id",
                    binding.binding_id
                ));
            }
        }
        Some("published") => {
            if staged_policy_id.is_some() {
                errors.push(format!(
                    "projection ACP binding {} publication_status published must not keep staged_policy_id; promote it to policy_id",
                    binding.binding_id
                ));
            }
        }
        Some("rotating") => {
            if staged_policy_id.is_none() {
                errors.push(format!(
                    "projection ACP binding {} publication_status rotating requires staged_policy_id",
                    binding.binding_id
                ));
            }
        }
        Some("retired") => {
            if binding.enabled {
                errors.push(format!(
                    "projection ACP binding {} publication_status retired must not be enabled",
                    binding.binding_id
                ));
            }
        }
        Some(status) => errors.push(format!(
            "projection ACP binding {} has invalid publication_status {}; expected draft, staged, published, rotating, or retired",
            binding.binding_id, status
        )),
    }
}

fn validate_schedule_cadence(schedule: &super::DesiredSchedule, errors: &mut Vec<String>) {
    let interval_secs = schedule.interval_secs;
    let cron = schedule
        .cron
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match (interval_secs, cron) {
        (Some(interval_secs), None) if interval_secs >= 1 => {}
        (Some(_), Some(_)) => errors.push(format!(
            "schedule {} in schedules manifest must contain exactly one of interval_secs or cron",
            schedule.schedule_id
        )),
        (Some(_), None) => errors.push(format!(
            "schedule {} in schedules manifest must contain an interval_secs >= 1",
            schedule.schedule_id
        )),
        (None, Some(expression)) => {
            let timezone = schedule
                .timezone
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let Some(timezone) = timezone else {
                errors.push(format!(
                    "schedule {} in schedules manifest must contain a timezone when cron is set",
                    schedule.schedule_id
                ));
                return;
            };
            if let Err(error) =
                validate_cron_schedule(expression, timezone, schedule.missed_run_policy.as_deref())
            {
                errors.push(format!(
                    "schedule {} in schedules manifest has invalid cron schedule: {}",
                    schedule.schedule_id, error
                ));
            }
        }
        (None, None) => errors.push(format!(
            "schedule {} in schedules manifest must contain exactly one of interval_secs or cron",
            schedule.schedule_id
        )),
    }
}

/// Live-DB validation that complements the pure `validate_manifest`.
///
/// Unlike `validate_manifest`, this checks pairing ownership and probes the
/// live database schema and filter syntax for every `EventTrigger`. The full
/// validator is an apply-time gate. `config diff` invokes only the narrower
/// pairing ownership check so it can report an unsafe plan without hiding the
/// rest of the diff.
///
/// Two checks per trigger:
///
/// 1. **Filter syntax probe.** Run `{collection}(filter: <trigger.filter>,
///    limit: 1) { _docID }` — DefraDB surfaces parse errors as GraphQL
///    errors, which `ConfigAccess::execute` turns into an `Err`. We catch
///    it and report the underlying message. An empty / absent filter is a
///    no-op (engine substitutes an always-match filter).
///
/// 2. **Template `doc.*` field resolution.** Parse the referenced Task's
///    `prompt_template`, extract every `doc.<field>` root, and introspect
///    the source collection's GraphQL type. Reject when any top-level
///    `doc.X` field does not exist on the source. Deep-path (`doc.a.b`)
///    resolution is explicitly out of scope for v1 — top-level existence
///    is the guarantee we offer.
pub(crate) async fn validate_manifest_against_live(
    manifest: &DesiredStateManifest,
    access: &ConfigAccess,
) -> Result<Vec<String>> {
    let mut errors = validate_peer_pairing_ownership_against_live(manifest, access).await?;
    for trig in &manifest.event_triggers {
        let source_collection = trig.source_collection.trim();
        let trigger_id = trig.trigger_id.trim();
        if source_collection.is_empty() || trigger_id.is_empty() {
            continue;
        }

        if let Some(filter) = trig.filter.as_deref().map(str::trim) {
            if !filter.is_empty() {
                let probe = format!(
                    r#"query {{ {collection}(filter: {filter}, limit: 1) {{ _docID }} }}"#,
                    collection = source_collection,
                    filter = filter,
                );
                match access.execute(&probe).await {
                    Ok(_) => {}
                    Err(err) => {
                        errors.push(format!(
                            "event_trigger {} filter syntax error: {}",
                            trigger_id, err
                        ));
                    }
                }
            }
        }

        let task_id = trig.task_id.trim();
        if task_id.is_empty() {
            continue;
        }
        let Some(task) = manifest.tasks.iter().find(|t| t.task_id.trim() == task_id) else {
            continue;
        };
        let refs = match parse_template_for_validation(&task.prompt_template) {
            Ok(refs) => refs,
            Err(_) => {
                continue;
            }
        };
        let doc_paths: Vec<Vec<String>> = refs
            .into_iter()
            .filter(|v| v.root() == Some("doc"))
            .map(|v| v.path.clone())
            .collect();
        if doc_paths.is_empty() {
            continue;
        }

        let introspect = format!(
            r#"query {{ __type(name: "{name}") {{ fields {{ name }} }} }}"#,
            name = source_collection,
        );
        let response = match access.execute(&introspect).await {
            Ok(response) => response,
            Err(err) => {
                errors.push(format!(
                    "event_trigger {} introspection of source_collection {} failed: {}",
                    trigger_id, source_collection, err
                ));
                continue;
            }
        };
        let type_node = response.get("data").and_then(|d| d.get("__type"));
        let fields = type_node
            .filter(|v| !v.is_null())
            .and_then(|t| t.get("fields"))
            .and_then(serde_json::Value::as_array);
        let Some(fields) = fields else {
            errors.push(format!(
                "event_trigger {} references unknown source_collection {}",
                trigger_id, source_collection
            ));
            continue;
        };
        let top_level: HashSet<&str> = fields
            .iter()
            .filter_map(|f| f.get("name").and_then(|n| n.as_str()))
            .collect();
        let mut reported: BTreeSet<String> = BTreeSet::new();
        for path in &doc_paths {
            let Some(first) = path.get(1).map(String::as_str) else {
                continue;
            };
            if top_level.contains(first) {
                continue;
            }
            if !reported.insert(first.to_string()) {
                continue;
            }
            errors.push(format!(
                "event_trigger {} template references doc.{} but {} has no such field",
                trigger_id, first, source_collection
            ));
        }
    }

    Ok(errors)
}

pub(crate) async fn validate_peer_pairing_ownership_against_live(
    manifest: &DesiredStateManifest,
    access: &ConfigAccess,
) -> Result<Vec<String>> {
    let mut errors = Vec::new();
    if manifest.peer_pairings.is_empty() {
        return Ok(errors);
    }

    let rows = crate::graphql_rows(
        access,
        "PeerPairingDesired",
        r#"query {
            PeerPairingDesired {
                peer_id
                agent_did
                source
            }
        }"#,
    )
    .await?;
    let desired_dids = manifest
        .peer_pairings
        .iter()
        .map(|pairing| pairing.peer_did.trim())
        .filter(|peer_did| !peer_did.is_empty())
        .collect::<BTreeSet<_>>();
    let desired_peer_ids = manifest
        .peer_pairings
        .iter()
        .filter_map(|pairing| pairing.resolved_peer_id())
        .collect::<BTreeSet<_>>();
    let expected_source = super::peer_pairing_manifest_source(&manifest.agent_principal.agent_did);

    for row in rows {
        let peer_id = row
            .get("peer_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .unwrap_or_default();
        let peer_did = row
            .get("agent_did")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .unwrap_or_default();
        if !desired_dids.contains(peer_did) && !desired_peer_ids.contains(peer_id) {
            continue;
        }
        let source = row
            .get("source")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .unwrap_or("operator");
        if source != expected_source {
            errors.push(format!(
                "peer pairing {peer_did:?} (peer_id {peer_id:?}) is owned by source {source:?}, not this manifest; refusing to overwrite or delete it"
            ));
        }
    }
    Ok(errors)
}

fn format_variable_ref(var: &VariableRef) -> String {
    if var.path.is_empty() {
        String::new()
    } else {
        var.path.join(".")
    }
}

fn validate_argv_prefixes(
    selection_id: &str,
    field: &str,
    prefixes: &[String],
    errors: &mut Vec<String>,
) {
    for prefix in prefixes {
        let trimmed = prefix.trim();
        if trimmed.is_empty() {
            errors.push(format!(
                "tool selection {selection_id} has an empty {field} entry"
            ));
            continue;
        }

        if trimmed.starts_with('[') {
            match serde_json::from_str::<Vec<String>>(trimmed) {
                Ok(tokens)
                    if !tokens.is_empty() && tokens.iter().all(|token| !token.trim().is_empty()) => {}
                Ok(_) => errors.push(format!(
                    "tool selection {selection_id} {field} JSON entry must contain non-empty argv tokens"
                )),
                Err(error) => errors.push(format!(
                    "tool selection {selection_id} {field} JSON entry is invalid: {error}"
                )),
            }
        }
    }
}

fn validate_subagent_targets(
    selection_id: &str,
    selection_agent_did: &str,
    allow_cross_deployment: bool,
    entries: &[String],
    errors: &mut Vec<String>,
) {
    let mut seen_names: HashSet<String> = HashSet::new();
    for entry in entries {
        let target = match SubagentTarget::parse(entry) {
            Ok(target) => target,
            Err(error) => {
                errors.push(format!(
                    "tool selection {selection_id} subagent_targets entry {entry:?} is not valid SubagentTarget JSON: {error}"
                ));
                continue;
            }
        };
        if !target.is_structurally_valid() {
            errors.push(format!(
                "tool selection {selection_id} subagent_targets entry {entry:?} must have non-empty name, agent_did, and behavior_id"
            ));
            continue;
        }
        if !seen_names.insert(target.name.trim().to_string()) {
            errors.push(format!(
                "tool selection {selection_id} has a duplicate subagent target name {:?}",
                target.name
            ));
        }
        if !allow_cross_deployment
            && !selection_agent_did.is_empty()
            && target.agent_did.trim() != selection_agent_did
        {
            errors.push(format!(
                "cross-deployment subagent delegation is deferred; remote target {} requires subagent_allow_cross_deployment=true (trusted-fleet only).",
                target.name
            ));
        }
    }
}

fn validate_datastore_surface_links(
    manifest: &DesiredStateManifest,
    selection: &DesiredToolSelection,
    errors: &mut Vec<String>,
) {
    use gents::{is_reserved_builtin_tool_name, WriteToolDecl};
    use std::collections::{BTreeMap, BTreeSet};

    let surfaces: BTreeMap<&str, &DesiredDatastoreToolSurface> = manifest
        .datastore_tool_surfaces
        .iter()
        .map(|s| (s.surface_id.as_str(), s))
        .collect();

    let mut merged: Vec<String> = selection.write_tools.clone();
    let mut seen_names: BTreeSet<String> = BTreeSet::new();
    for entry in &selection.write_tools {
        if let Ok(decl) = serde_json::from_str::<WriteToolDecl>(entry) {
            seen_names.insert(decl.tool_name);
        }
    }

    for surface_id in &selection.datastore_tool_surface_ids {
        let surface_id = surface_id.trim();
        if surface_id.is_empty() {
            errors.push(format!(
                "tool selection {} has an empty datastore_tool_surface_ids entry",
                selection.selection_id
            ));
            continue;
        }
        let Some(surface) = surfaces.get(surface_id) else {
            errors.push(format!(
                "tool selection {} references missing DatastoreToolSurface {}",
                selection.selection_id, surface_id
            ));
            continue;
        };
        if surface.agent_did.trim() != selection.agent_did.trim() {
            errors.push(format!(
                "tool selection {} references DatastoreToolSurface {} owned by a different agent",
                selection.selection_id, surface_id
            ));
            continue;
        }
        if !surface.enabled {
            errors.push(format!(
                "tool selection {} references disabled DatastoreToolSurface {}",
                selection.selection_id, surface_id
            ));
            continue;
        }
        for entry in &surface.entries {
            match serde_json::from_str::<WriteToolDecl>(entry) {
                Ok(decl) => {
                    if !decl.is_well_formed() {
                        errors.push(format!(
                            "DatastoreToolSurface {} has a malformed entry (tool_name/collection required)",
                            surface_id
                        ));
                        continue;
                    }
                    if is_reserved_builtin_tool_name(&decl.tool_name) {
                        errors.push(format!(
                            "DatastoreToolSurface {} tool_name {:?} collides with a built-in tool",
                            surface_id, decl.tool_name
                        ));
                    }
                    if !seen_names.insert(decl.tool_name.clone()) {
                        errors.push(format!(
                            "duplicate write tool_name {:?} after expanding DatastoreToolSurface {} for tool selection {}",
                            decl.tool_name, surface_id, selection.selection_id
                        ));
                    }
                    merged.push(entry.clone());
                }
                Err(error) => errors.push(format!(
                    "DatastoreToolSurface {} entry is not valid WriteToolDecl JSON: {error}",
                    surface_id
                )),
            }
        }
    }

    // Re-run field-level checks over the merged list.
    validate_write_tools(
        &selection.selection_id,
        &merged,
        &selection.cli_tool_names,
        errors,
    );
}

fn validate_write_tools(
    selection_id: &str,
    entries: &[String],
    cli_tool_names: &[String],
    errors: &mut Vec<String>,
) {
    let cli_tool_names: HashSet<&str> = cli_tool_names.iter().map(|name| name.trim()).collect();
    let mut seen_tool_names: HashSet<String> = HashSet::new();
    for entry in entries {
        let decl: WriteToolDecl = match serde_json::from_str(entry) {
            Ok(decl) => decl,
            Err(error) => {
                errors.push(format!(
                    "tool selection {selection_id} write_tools entry {entry:?} is not valid WriteToolDecl JSON: {error}"
                ));
                continue;
            }
        };
        if decl.tool_name.trim().is_empty() {
            errors.push(format!(
                "tool selection {selection_id} write_tools entry {entry:?} must have a non-empty tool_name"
            ));
        }
        if decl.collection.trim().is_empty() {
            errors.push(format!(
                "tool selection {selection_id} write_tools tool {:?} must have a non-empty collection",
                decl.tool_name
            ));
        }
        if is_reserved_builtin_tool_name(&decl.tool_name) {
            errors.push(format!(
                "tool selection {selection_id} write_tools tool_name {:?} collides with a \
                 built-in tool; declared write tools must use a name not already provided by the \
                 native, meta, subagent, or built-in (defra_query, context_budget, sessions, \
                 memory) tool surface",
                decl.tool_name.trim()
            ));
        }
        if cli_tool_names.contains(decl.tool_name.trim()) {
            errors.push(format!(
                "tool selection {selection_id} write_tools tool_name {:?} collides with a \
                 cli_tool_names entry in the same tool selection; each tool must have a unique name",
                decl.tool_name.trim()
            ));
        }
        let mut seen_field_names: HashSet<String> = HashSet::new();
        for field in &decl.fields {
            if field.name.trim().is_empty() {
                errors.push(format!(
                    "tool selection {selection_id} write_tools tool {:?} has a field with an empty name",
                    decl.tool_name
                ));
            } else if !seen_field_names.insert(field.name.trim().to_string()) {
                errors.push(format!(
                    "tool selection {selection_id} write_tools tool {:?} has a duplicate field name {:?}",
                    decl.tool_name,
                    field.name.trim()
                ));
            }
        }
        if !decl.tool_name.trim().is_empty()
            && !seen_tool_names.insert(decl.tool_name.trim().to_string())
        {
            errors.push(format!(
                "tool selection {selection_id} has a duplicate write_tools tool_name {:?}",
                decl.tool_name.trim()
            ));
        }
    }
}

fn validate_non_empty_values(
    selection_id: &str,
    field: &str,
    values: &[String],
    errors: &mut Vec<String>,
) {
    for value in values {
        if value.trim().is_empty() {
            errors.push(format!(
                "tool selection {selection_id} has an empty {field} entry"
            ));
        }
    }
}

pub(crate) fn non_empty(value: &Option<String>) -> Option<&str> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub(crate) fn normalize_tool_service_string(value: Option<String>) -> String {
    value.unwrap_or_default().trim().to_string()
}

pub(crate) fn normalize_tool_service_mcp_path(value: Option<String>) -> String {
    use super::DEFAULT_TOOL_SERVICE_MCP_PATH;
    let trimmed = value.as_deref().unwrap_or_default().trim();
    if trimmed.is_empty() {
        DEFAULT_TOOL_SERVICE_MCP_PATH.to_string()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        DesiredAgentBehavior, DesiredAgentPrincipal, DesiredStateManifest, DesiredTask,
    };
    use super::*;

    fn manifest(system_prompt: Option<&str>, task_prompt: Option<&str>) -> DesiredStateManifest {
        DesiredStateManifest {
            agent_principal: DesiredAgentPrincipal {
                agent_did: "did:key:test-template-validation".to_string(),
                display_name: None,
                default_behavior_id: Some("default".to_string()),
                enabled: true,
            },
            agent_behaviors: vec![DesiredAgentBehavior {
                behavior_id: "default".to_string(),
                agent_did: "did:key:test-template-validation".to_string(),
                display_name: None,
                description: None,
                summary: None,
                system_prompt: system_prompt.map(str::to_string),
                request_context_template: None,
                backend_id: None,
                model_name: None,
                tool_selection_id: None,
                inference_profile_id: None,
                compaction_strategy: None,
                compaction_threshold: None,
                enabled: true,
                skill_refs: Vec::new(),
                skill_excludes: Vec::new(),
            }],
            skills: Vec::new(),
            datastore_tool_surfaces: Vec::new(),
            tool_selections: Vec::new(),
            inference_backends: Vec::new(),
            inference_profiles: Vec::new(),
            tool_service_registries: Vec::new(),
            projection_acp_bindings: Vec::new(),
            peer_pairings: Vec::new(),
            tasks: task_prompt
                .map(|prompt| {
                    vec![DesiredTask {
                        task_id: "task".to_string(),
                        name: "Task".to_string(),
                        description: None,
                        behavior_id: "default".to_string(),
                        prompt_template: prompt.to_string(),
                        enabled: true,
                        output_schema_ref: None,
                    }]
                })
                .unwrap_or_default(),
            schedules: Vec::new(),
            event_triggers: Vec::new(),
        }
    }

    fn validate_errors(manifest: DesiredStateManifest) -> Vec<String> {
        let mut errors = Vec::new();
        validate_manifest(&manifest, &mut errors);
        errors
    }

    fn manifest_with_reasoning_effort(reasoning_effort: Option<&str>) -> DesiredStateManifest {
        let mut manifest = manifest(None, None);
        manifest
            .inference_profiles
            .push(super::super::DesiredInferenceProfile {
                profile_id: "default-profile".to_string(),
                display_name: None,
                context_window: None,
                max_output_tokens: None,
                max_turns: None,
                temperature: None,
                top_p: None,
                top_k: None,
                min_p: None,
                frequency_penalty: None,
                presence_penalty: None,
                repetition_penalty: None,
                reasoning_effort: reasoning_effort.map(str::to_string),
                stream_batch_ms: None,
                stream_liveness_timeout_secs: None,
                deadline_duration_secs: None,
                retry_max_transport: None,
                retry_backoff_ms: None,
                retry_max_resample: None,
                retry_allow_repair: None,
                retry_interactive_max: None,
            });
        manifest
    }

    /// `gents init` writes no reasoning effort, which DefraDB materializes as
    /// an empty string, which `config export` copies into the manifest. Treating
    /// that as an invalid level made `config export` produce a manifest
    /// `config apply` refused — the round trip the CLI's own shim test drives.
    #[test]
    fn an_unset_reasoning_effort_round_trips_through_export_and_apply() {
        for unset in [Some(""), Some("   "), None] {
            let errors = validate_errors(manifest_with_reasoning_effort(unset));
            assert!(
                errors.is_empty(),
                "an unset reasoning_effort ({unset:?}) must validate: {errors:?}"
            );
        }
    }

    #[test]
    fn a_reasoning_effort_outside_the_vocabulary_is_still_rejected() {
        let errors = validate_errors(manifest_with_reasoning_effort(Some("extreme")));
        assert!(
            errors
                .iter()
                .any(|error| error.contains("reasoning_effort must be one of")),
            "expected the vocabulary rejection, got {errors:?}"
        );
    }

    #[test]
    fn system_prompt_rejects_per_request_ref() {
        let errors = validate_errors(manifest(Some("now {{ ctx.now }}"), None));

        assert!(
            errors
                .iter()
                .any(|error| error.contains("per-request variable `ctx.now`")),
            "expected ctx.now rejection, got {errors:?}"
        );
    }

    #[test]
    fn system_prompt_accepts_literal_raw_and_node_refs() {
        for prompt in [
            "literal text with no MiniJinja markers",
            "{% raw %}{{ ctx.now }}{% endraw %}",
            "node {{ node.node_did }} / {{ node.behavior_id }}",
        ] {
            let errors = validate_errors(manifest(Some(prompt), None));
            assert!(errors.is_empty(), "prompt {prompt:?} failed: {errors:?}");
        }
    }

    fn manifest_with_request_context(template: &str) -> DesiredStateManifest {
        let mut m = manifest(None, None);
        m.agent_behaviors[0].request_context_template = Some(template.to_string());
        m
    }

    #[test]
    fn request_context_template_validated_at_apply() {
        let ok = validate_errors(manifest_with_request_context(
            "seat at {{ ctx.now }} on {{ node.node_did }}",
        ));
        assert!(
            ok.is_empty(),
            "valid request-context template failed: {ok:?}"
        );

        let bad = validate_errors(manifest_with_request_context("{{ ctx.bogus_unknown }}"));
        assert!(
            bad.iter()
                .any(|e| e.contains("request_context_template") && e.contains("ctx.bogus_unknown")),
            "expected unknown ctx ref rejection at apply, got {bad:?}"
        );

        let hidden = validate_errors(manifest_with_request_context(
            "{% set x = ctx.bogus_unknown %}{{ x }}",
        ));
        assert!(
            hidden
                .iter()
                .any(|e| e.contains("request_context_template") && e.contains("ctx.bogus_unknown")),
            "expected unknown ctx ref inside set to be rejected at apply, got {hidden:?}"
        );
    }

    #[test]
    fn task_template_raw_block_is_not_scope_checked() {
        let errors = validate_errors(manifest(
            None,
            Some("{% raw %}{{ ctx.collection_summary }}{% endraw %} at {{ ctx.now }}"),
        ));
        assert!(
            errors.is_empty(),
            "raw-wrapped task-unavailable var must not be scope-rejected: {errors:?}"
        );
    }

    #[test]
    fn task_template_rejects_task_unavailable_ctx_ref() {
        let errors = validate_errors(manifest(None, Some("{{ ctx.collection_summary }}")));
        assert!(
            errors.iter().any(|e| e.contains("ctx.collection_summary")),
            "expected task-unavailable ctx ref rejection, got {errors:?}"
        );
    }

    #[test]
    fn task_prompt_accepts_task_catalog_refs() {
        let errors = validate_errors(manifest(
            None,
            Some("run {{ node.node_did }} {{ node.behavior_id }} {{ ctx.now }}"),
        ));

        assert!(errors.is_empty(), "task refs should pass: {errors:?}");
    }

    #[test]
    fn task_prompt_rejects_request_context_only_ref() {
        let errors = validate_errors(manifest(None, Some("state {{ ctx.collection_summary }}")));

        assert!(
            errors.iter().any(|error| error.contains(
                "unavailable template variable ctx.collection_summary"
            )),
            "expected ctx.collection_summary rejection, got {errors:?}"
        );
    }
}

#[cfg(test)]
mod live_tests {
    use anyhow::Result;
    use gents::defra_node::{EmbeddedNode, StorageBackend};
    use gents::ensure_runtime_schemas;

    use super::*;
    use crate::config_writes::ConfigAccess;

    fn manifest_with_subagent_targets(targets: Vec<SubagentTarget>) -> DesiredStateManifest {
        use super::super::{DesiredAgentPrincipal, DesiredStateManifest, DesiredToolSelection};
        let targets: Vec<String> = targets.iter().map(SubagentTarget::to_entry).collect();
        DesiredStateManifest {
            agent_principal: DesiredAgentPrincipal {
                agent_did: "did:key:test-live-validate".to_string(),
                display_name: None,
                default_behavior_id: None,
                enabled: true,
            },
            agent_behaviors: Vec::new(),
            skills: Vec::new(),
            datastore_tool_surfaces: Vec::new(),
            tool_selections: vec![DesiredToolSelection {
                selection_id: "live-test-sel".to_string(),
                agent_did: "did:key:test-live-validate".to_string(),
                display_name: None,
                tool_policy_version: None,
                enable_file_tools: false,
                file_tools_mode: "ReadOnly".to_string(),
                file_tool_root: None,
                enable_bash: false,
                bash_mode: "ReadOnly".to_string(),
                command_execution_policy: None,
                command_allowed_argv_prefixes: Vec::new(),
                command_forbidden_argv_prefixes: Vec::new(),
                read_only_command_allowlist: Vec::new(),
                command_network_mode: None,
                cli_tool_names: Vec::new(),
                enable_meta_tools: false,
                allowed_mcp_service_ids: Vec::new(),
                delegate_to: Vec::new(),
                backgroundable_tool_names: Vec::new(),
                enable_memory: false,
                enable_session_history_tool: false,
                enable_context_budget: true,
                enable_defra_query: true,
                defra_query_collections: Vec::new(),
                subagent_targets: targets,
                subagent_spawn_enabled: true,
                orchestration_enabled: false,
                subagent_steering_enabled: false,
                subagent_background_enabled: false,
                subagent_default_await_mode: None,
                subagent_allow_cross_deployment: false,
                cross_deployment_spawn_timeout_seconds: None,
                write_tools: Vec::new(),
                datastore_tool_surface_ids: Vec::new(),
                enable_self_config: false,
                self_config_categories: Vec::new(),
                self_config_no_lockout: false,
                self_config_dry_run: false,
            }],
            inference_backends: Vec::new(),
            inference_profiles: Vec::new(),
            tool_service_registries: Vec::new(),
            projection_acp_bindings: Vec::new(),
            peer_pairings: Vec::new(),
            tasks: Vec::new(),
            schedules: Vec::new(),
            event_triggers: Vec::new(),
        }
    }

    #[tokio::test]
    async fn live_validate_does_not_resolve_remote_subagent_target() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let data_dir = tempdir.path().join("data");
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;

        let access = ConfigAccess::Local(std::sync::Arc::new(node));

        let manifest = manifest_with_subagent_targets(vec![SubagentTarget {
            name: "remote-researcher".to_string(),
            agent_did: "did:key:zRemotePeer".to_string(),
            behavior_id: "does-not-exist-locally".to_string(),
            description: None,
        }]);
        let errors = validate_manifest_against_live(&manifest, &access).await?;

        assert!(
            !errors
                .iter()
                .any(|msg| msg.contains("does-not-exist-locally") || msg.contains("live-test-sel")),
            "remote subagent target must not trigger live resolution errors, got {errors:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn live_validate_passes_for_known_subagent_target() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let data_dir = tempdir.path().join("data");
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;

        let access = ConfigAccess::Local(std::sync::Arc::new(node));

        let manifest = manifest_with_subagent_targets(vec![SubagentTarget {
            name: "researcher".to_string(),
            agent_did: "did:key:test-live-validate".to_string(),
            behavior_id: "amy-research".to_string(),
            description: None,
        }]);
        let errors = validate_manifest_against_live(&manifest, &access).await?;

        assert!(
            !errors
                .iter()
                .any(|msg| msg.contains("amy-research") || msg.contains("live-test-sel")),
            "expected no subagent errors for known target, got {errors:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn live_validate_rejects_non_manifest_pairing_collision_and_diff_reports_it() -> Result<()>
    {
        use super::super::DesiredPeerPairing;
        use crate::commands::config::binding::{
            BoundDesiredManifest, ManifestBindMode, ManifestBindingContext,
        };
        use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
        use crate::config_import::apply_desired_state_changes;
        use crate::desired_state::{diff_manifests, export_bundle_from_manifest};
        use gents::graphql::escape_graphql_string;

        let tempdir = tempfile::tempdir()?;
        let node = EmbeddedNode::builder()
            .data_path(tempdir.path().join("data"))
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;
        let access = ConfigAccess::Local(std::sync::Arc::new(node));
        let peer_id = "aa".repeat(32);
        let peer_did = "did:key:remote";
        let address = format!("{peer_id}@127.0.0.1:4100");
        access
            .execute(&format!(
                r#"mutation {{ create_PeerPairingDesired(input: {{
                    peer_id: "{}",
                    agent_did: "{}",
                    collections: ["AgentRequest"],
                    replicator_addresses: ["{}"],
                    template: "conversation",
                    source: "operator"
                }}) {{ _docID }} }}"#,
                escape_graphql_string(&peer_id),
                escape_graphql_string(peer_did),
                escape_graphql_string(&address),
            ))
            .await?;

        let mut manifest = manifest_with_subagent_targets(Vec::new());
        manifest.peer_pairings.push(DesiredPeerPairing {
            peer_did: peer_did.to_string(),
            addresses: vec![address],
            template: "conversation".to_string(),
            enabled: false,
            peer_id,
        });
        let errors = validate_manifest_against_live(&manifest, &access).await?;
        assert!(errors.iter().any(|error| {
            error.contains("source \"operator\"")
                && error.contains("refusing to overwrite or delete")
        }));

        let owner_did = manifest.agent_principal.agent_did.clone();
        let bound = BoundDesiredManifest {
            context: ManifestBindingContext {
                bind_mode: ManifestBindMode::Manifest,
                target_agent_did: owner_did.clone(),
                source_manifest_dids: std::collections::BTreeSet::from([owner_did]),
            },
            manifest: manifest.clone(),
        };
        let report = crate::commands::config::diff::diff_bound_desired_manifest(
            std::path::Path::new("/ownership-collision"),
            &access,
            &bound,
        )
        .await?;
        assert_eq!(report.status, "diffed");
        assert!(!report.ok);
        assert!(report.live_validation_errors.iter().any(|error| {
            error.contains("source \"operator\"")
                && error.contains("refusing to overwrite or delete")
        }));

        manifest.peer_pairings.clear();
        manifest.tool_selections.clear();
        let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
        let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
        let planned = diff_manifests(
            std::path::Path::new("/ownership-safe"),
            access.mode(),
            &manifest,
            live_principal.as_ref(),
            &live_manifest,
            false,
        );
        assert!(planned.collections.peer_pairings.delete.is_empty());
        let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
        let txn = access.begin_apply_txn().await?;
        apply_desired_state_changes(&txn, &bundle, &planned).await?;
        txn.commit().await?;
        let rows = crate::graphql_rows(
            &access,
            "PeerPairingDesired",
            "{ PeerPairingDesired { peer_id source } }",
        )
        .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["source"], "operator");
        Ok(())
    }

    #[tokio::test]
    async fn preboot_pairing_apply_is_idempotent_and_restart_loader_consumes_seed() -> Result<()> {
        use std::sync::Arc;

        use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
        use crate::config_import::apply_desired_state_changes;
        use crate::desired_state::{diff_manifests, export_bundle_from_manifest};
        use gents::agent::p2p_reconcile::{
            reconcile_peer_tick, GraphqlPairingStateStore, PairingFilters, PairingStateStore,
            RemoteP2pAdmin, RemoteP2pAdminResult, RemoteReplicator,
        };
        use gents::KeyIdentity;

        let tempdir = tempfile::tempdir()?;
        let data_path = tempdir.path().join("data");
        let node = EmbeddedNode::builder()
            .data_path(&data_path)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;
        let access = ConfigAccess::Local(std::sync::Arc::new(node));
        let peer_id = "bb".repeat(32);
        let address = format!("{peer_id}@127.0.0.1:4100");
        let mut manifest = manifest_with_subagent_targets(Vec::new());
        manifest.tool_selections.clear();
        manifest
            .peer_pairings
            .push(super::super::DesiredPeerPairing {
                peer_did: "did:key:remote".to_string(),
                addresses: vec![address.clone()],
                template: "conversation".to_string(),
                enabled: true,
                peer_id: peer_id.clone(),
            });

        let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
        let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
        let planned = diff_manifests(
            std::path::Path::new("/preboot"),
            access.mode(),
            &manifest,
            live_principal.as_ref(),
            &live_manifest,
            false,
        );
        assert_eq!(
            planned.collections.peer_pairings.create,
            vec![peer_id.clone()]
        );
        let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
        let txn = access.begin_apply_txn().await?;
        let counts = apply_desired_state_changes(&txn, &bundle, &planned).await?;
        txn.commit().await?;
        assert_eq!(counts.peer_pairings, 1);

        let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
        let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
        let noop = diff_manifests(
            std::path::Path::new("/preboot"),
            access.mode(),
            &manifest,
            live_principal.as_ref(),
            &live_manifest,
            false,
        );
        assert_eq!(
            noop.collections.peer_pairings.unchanged,
            vec![peer_id.clone()]
        );
        assert!(!noop.counts.has_pending_apply());
        let txn = access.begin_apply_txn().await?;
        let repeated = apply_desired_state_changes(&txn, &bundle, &noop).await?;
        txn.commit().await?;
        assert_eq!(repeated.peer_pairings, 0);
        drop(access);

        let identity = Arc::new(KeyIdentity::load_or_create(
            tempdir.path().join("restart-identity.key"),
            None,
        )?);
        let restarted_node = Arc::new(
            EmbeddedNode::builder()
                .data_path(&data_path)
                .with_storage_backend(StorageBackend::RocksDb)
                .build()
                .await?,
        );
        let restarted_store =
            GraphqlPairingStateStore::new(restarted_node.clone(), identity.clone());
        let loaded = restarted_store
            .load_desired(&peer_id)
            .await?
            .expect("seeded pairing is visible to restarted reconciler");
        assert!(loaded.replicator_addresses.contains(&address));

        #[derive(Default)]
        struct RestartAdmin {
            added_replicators: std::sync::Mutex<Vec<String>>,
        }

        #[async_trait::async_trait]
        impl RemoteP2pAdmin for RestartAdmin {
            async fn peer_info(&self) -> RemoteP2pAdminResult<Vec<String>> {
                Ok(Vec::new())
            }
            async fn active_peers(&self) -> RemoteP2pAdminResult<Vec<String>> {
                Ok(Vec::new())
            }
            async fn connect(&self, _addresses: &[String]) -> RemoteP2pAdminResult<()> {
                Ok(())
            }
            async fn list_replicators(&self) -> RemoteP2pAdminResult<Vec<RemoteReplicator>> {
                Ok(self
                    .added_replicators
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|address| RemoteReplicator {
                        id: Some(address.clone()),
                        collections: Vec::new(),
                        address: Some(address.clone()),
                    })
                    .collect())
            }
            async fn add_replicator(
                &self,
                addresses: &[String],
                _collections: &[String],
                _filters: &PairingFilters,
            ) -> RemoteP2pAdminResult<()> {
                self.added_replicators
                    .lock()
                    .unwrap()
                    .extend_from_slice(addresses);
                Ok(())
            }
            async fn delete_replicator(
                &self,
                id: &str,
                _collections: &[String],
            ) -> RemoteP2pAdminResult<()> {
                self.added_replicators
                    .lock()
                    .unwrap()
                    .retain(|address| address != id);
                Ok(())
            }
            async fn list_p2p_collections(&self) -> RemoteP2pAdminResult<Vec<String>> {
                Ok(Vec::new())
            }
            async fn resolve_collection_id(
                &self,
                name: &str,
            ) -> RemoteP2pAdminResult<Option<String>> {
                Ok(Some(name.to_string()))
            }
            async fn resolve_collection_name(
                &self,
                id: &str,
            ) -> RemoteP2pAdminResult<Option<String>> {
                Ok(Some(id.to_string()))
            }
            async fn add_p2p_collections(
                &self,
                _collections: &[String],
            ) -> RemoteP2pAdminResult<()> {
                Ok(())
            }
            async fn delete_p2p_collections(
                &self,
                _collections: &[String],
            ) -> RemoteP2pAdminResult<()> {
                Ok(())
            }
            async fn list_p2p_documents(&self) -> RemoteP2pAdminResult<Vec<String>> {
                Ok(Vec::new())
            }
            async fn add_p2p_documents(&self, _doc_ids: &[String]) -> RemoteP2pAdminResult<()> {
                Ok(())
            }
            async fn delete_p2p_documents(&self, _doc_ids: &[String]) -> RemoteP2pAdminResult<()> {
                Ok(())
            }
            async fn sync_documents(
                &self,
                _collection_name: &str,
                _doc_ids: &[String],
                _timeout: Option<std::time::Duration>,
            ) -> RemoteP2pAdminResult<()> {
                Ok(())
            }
            async fn sync_collection_versions(
                &self,
                _version_ids: &[String],
                _timeout: Option<std::time::Duration>,
            ) -> RemoteP2pAdminResult<()> {
                Ok(())
            }
            async fn sync_branchable_collection(
                &self,
                _collection_id: &str,
                _timeout: Option<std::time::Duration>,
            ) -> RemoteP2pAdminResult<()> {
                Ok(())
            }
        }

        let admin = RestartAdmin::default();
        let outcome = reconcile_peer_tick(&admin, &restarted_store, &peer_id).await?;
        assert!(!outcome.ops_applied.is_empty());
        assert_eq!(
            admin.added_replicators.lock().unwrap().as_slice(),
            &[address.clone()]
        );
        drop(restarted_store);
        drop(restarted_node);

        manifest.peer_pairings[0].enabled = false;
        let node = EmbeddedNode::builder()
            .data_path(&data_path)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await?;
        let access = ConfigAccess::Local(std::sync::Arc::new(node));
        let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
        let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
        let removal = diff_manifests(
            std::path::Path::new("/preboot"),
            access.mode(),
            &manifest,
            live_principal.as_ref(),
            &live_manifest,
            false,
        );
        assert_eq!(
            removal.collections.peer_pairings.delete,
            vec![peer_id.clone()]
        );
        let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
        let txn = access.begin_apply_txn().await?;
        apply_desired_state_changes(&txn, &bundle, &removal).await?;
        txn.commit().await?;
        let rows = crate::graphql_rows(
            &access,
            "PeerPairingDesired",
            "{ PeerPairingDesired { peer_id } }",
        )
        .await?;
        assert!(rows.is_empty());
        drop(access);
        let removal_node = Arc::new(
            EmbeddedNode::builder()
                .data_path(&data_path)
                .with_storage_backend(StorageBackend::RocksDb)
                .build()
                .await?,
        );
        let removal_store = GraphqlPairingStateStore::new(removal_node, identity);
        let outcome = reconcile_peer_tick(&admin, &removal_store, &peer_id).await?;
        assert!(!outcome.ops_applied.is_empty());
        assert!(admin.added_replicators.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn all_subagent_fields_persist_and_apply_is_idempotent() -> Result<()> {
        use std::path::PathBuf;

        use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
        use crate::config_import::{apply_desired_state_changes, diff_has_pending_apply};
        use crate::desired_state::{diff_manifests, export_bundle_from_manifest};

        let tempdir = tempfile::tempdir()?;
        let data_dir = tempdir.path().join("data");
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;

        let access = ConfigAccess::Local(std::sync::Arc::new(node));

        {
            use gents::graphql::escape_graphql_string;
            let did = escape_graphql_string("did:key:test-subagent-idempotency");
            access
                .execute(&format!(
                    r#"mutation {{ create_AgentPrincipal(input: {{ agent_did: "{did}", enabled: true }}) {{ _docID }} }}"#
                ))
                .await?;
        }

        let desired_manifest = {
            use super::super::{DesiredAgentPrincipal, DesiredStateManifest, DesiredToolSelection};
            DesiredStateManifest {
                agent_principal: DesiredAgentPrincipal {
                    agent_did: "did:key:test-subagent-idempotency".to_string(),
                    display_name: None,
                    default_behavior_id: None,
                    enabled: true,
                },
                agent_behaviors: Vec::new(),
                skills: Vec::new(),
                datastore_tool_surfaces: Vec::new(),
                tool_selections: vec![DesiredToolSelection {
                    selection_id: "subagent-idempotency-sel".to_string(),
                    agent_did: "did:key:test-subagent-idempotency".to_string(),
                    display_name: None,
                    tool_policy_version: None,
                    enable_file_tools: false,
                    file_tools_mode: "ReadOnly".to_string(),
                    file_tool_root: None,
                    enable_bash: false,
                    bash_mode: "ReadOnly".to_string(),
                    command_execution_policy: None,
                    command_allowed_argv_prefixes: Vec::new(),
                    command_forbidden_argv_prefixes: Vec::new(),
                    read_only_command_allowlist: Vec::new(),
                    command_network_mode: None,
                    cli_tool_names: Vec::new(),
                    enable_meta_tools: false,
                    allowed_mcp_service_ids: Vec::new(),
                    delegate_to: Vec::new(),
                    backgroundable_tool_names: Vec::new(),
                    enable_memory: false,
                    enable_session_history_tool: false,
                    enable_context_budget: true,
                    enable_defra_query: true,
                    defra_query_collections: Vec::new(),
                    subagent_targets: vec![SubagentTarget {
                        name: "researcher".to_string(),
                        agent_did: "did:key:test-subagent-idempotency".to_string(),
                        behavior_id: "amy-research".to_string(),
                        description: None,
                    }
                    .to_entry()],
                    subagent_spawn_enabled: true,
                    orchestration_enabled: true,
                    subagent_steering_enabled: true,
                    subagent_background_enabled: true,
                    subagent_default_await_mode: Some("background".to_string()),
                    subagent_allow_cross_deployment: true,
                    cross_deployment_spawn_timeout_seconds: Some(90),
                    write_tools: Vec::new(),
                    datastore_tool_surface_ids: Vec::new(),
                    enable_self_config: false,
                    self_config_categories: Vec::new(),
                    self_config_no_lockout: false,
                    self_config_dry_run: false,
                }],
                inference_backends: Vec::new(),
                inference_profiles: Vec::new(),
                tool_service_registries: Vec::new(),
                projection_acp_bindings: Vec::new(),
                peer_pairings: Vec::new(),
                tasks: Vec::new(),
                schedules: Vec::new(),
                event_triggers: Vec::new(),
            }
        };

        let root = PathBuf::from(".");
        let desired_bundle = export_bundle_from_manifest(&desired_manifest, "local")?;

        let live_bundle = build_desired_state_live_bundle(&access, &desired_manifest).await?;
        let (live_principal, live_manifest) =
            live_manifest_from_bundle(&desired_manifest, &live_bundle)?;
        let planned = diff_manifests(
            &root,
            "local",
            &desired_manifest,
            live_principal.as_ref(),
            &live_manifest,
            false,
        );

        let txn = access.begin_apply_txn().await?;
        apply_desired_state_changes(&txn, &desired_bundle, &planned).await?;
        txn.commit().await?;

        let remaining_bundle = build_desired_state_live_bundle(&access, &desired_manifest).await?;
        let (remaining_principal, remaining_manifest) =
            live_manifest_from_bundle(&desired_manifest, &remaining_bundle)?;

        let live_sel = remaining_manifest
            .tool_selections
            .iter()
            .find(|s| s.selection_id == "subagent-idempotency-sel")
            .expect("ToolSelection should exist after apply");

        assert_eq!(
            live_sel.subagent_targets,
            vec![SubagentTarget {
                name: "researcher".to_string(),
                agent_did: "did:key:test-subagent-idempotency".to_string(),
                behavior_id: "amy-research".to_string(),
                description: None,
            }
            .to_entry()],
            "subagent_targets must persist through apply"
        );
        assert_eq!(
            live_sel.subagent_spawn_enabled, true,
            "subagent_spawn_enabled must persist through apply"
        );
        assert_eq!(
            live_sel.orchestration_enabled, true,
            "orchestration_enabled must persist through apply"
        );
        assert_eq!(
            live_sel.subagent_steering_enabled, true,
            "subagent_steering_enabled must persist through apply"
        );
        assert_eq!(
            live_sel.subagent_background_enabled, true,
            "subagent_background_enabled must persist through apply"
        );
        assert_eq!(
            live_sel.subagent_default_await_mode.as_deref(),
            Some("background"),
            "subagent_default_await_mode must persist through apply"
        );
        assert_eq!(
            live_sel.subagent_allow_cross_deployment, true,
            "subagent_allow_cross_deployment must persist through apply"
        );
        assert_eq!(
            live_sel.cross_deployment_spawn_timeout_seconds,
            Some(90),
            "cross_deployment_spawn_timeout_seconds must persist through apply"
        );

        let second_diff = diff_manifests(
            &root,
            "local",
            &desired_manifest,
            remaining_principal.as_ref(),
            &remaining_manifest,
            false,
        );

        assert!(
            !diff_has_pending_apply(&second_diff.counts),
            "second diff must have no pending apply (idempotent); got: {:?}",
            second_diff.counts
        );
        assert!(
            second_diff
                .collections
                .tool_selections
                .unchanged
                .contains(&"subagent-idempotency-sel".to_string()),
            "tool selection must be in the 'unchanged' set after re-apply; got: {:?}",
            second_diff.collections.tool_selections
        );

        Ok(())
    }

    #[tokio::test]
    async fn behavior_description_and_summary_persist_and_apply_is_idempotent() -> Result<()> {
        use std::path::PathBuf;

        use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
        use crate::config_import::{apply_desired_state_changes, diff_has_pending_apply};
        use crate::desired_state::{diff_manifests, export_bundle_from_manifest};

        let tempdir = tempfile::tempdir()?;
        let data_dir = tempdir.path().join("data");
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;

        let access = ConfigAccess::Local(std::sync::Arc::new(node));

        {
            use gents::graphql::escape_graphql_string;
            let did = escape_graphql_string("did:key:test-behavior-desc-idempotency");
            access
                .execute(&format!(
                    r#"mutation {{ create_AgentPrincipal(input: {{ agent_did: "{did}", enabled: true }}) {{ _docID }} }}"#
                ))
                .await?;
        }

        let desired_manifest = {
            use super::super::{DesiredAgentBehavior, DesiredAgentPrincipal, DesiredStateManifest};
            DesiredStateManifest {
                agent_principal: DesiredAgentPrincipal {
                    agent_did: "did:key:test-behavior-desc-idempotency".to_string(),
                    display_name: None,
                    default_behavior_id: None,
                    enabled: true,
                },
                agent_behaviors: vec![DesiredAgentBehavior {
                    behavior_id: "desc-idempotency-behavior".to_string(),
                    agent_did: "did:key:test-behavior-desc-idempotency".to_string(),
                    display_name: Some("Research Assistant".to_string()),
                    description: Some(
                        "A general-purpose assistant for research and writing tasks.".to_string(),
                    ),
                    summary: Some("Research assistant".to_string()),
                    system_prompt: None,
                    request_context_template: None,
                    backend_id: None,
                    model_name: None,
                    tool_selection_id: None,
                    inference_profile_id: None,
                    compaction_strategy: None,
                    compaction_threshold: None,
                    enabled: true,
                    skill_refs: Vec::new(),
                    skill_excludes: Vec::new(),
                }],
                skills: Vec::new(),
                datastore_tool_surfaces: Vec::new(),
                tool_selections: Vec::new(),
                inference_backends: Vec::new(),
                inference_profiles: Vec::new(),
                tool_service_registries: Vec::new(),
                projection_acp_bindings: Vec::new(),
                peer_pairings: Vec::new(),
                tasks: Vec::new(),
                schedules: Vec::new(),
                event_triggers: Vec::new(),
            }
        };

        let root = PathBuf::from(".");
        let desired_bundle = export_bundle_from_manifest(&desired_manifest, "local")?;

        let live_bundle = build_desired_state_live_bundle(&access, &desired_manifest).await?;
        let (live_principal, live_manifest) =
            live_manifest_from_bundle(&desired_manifest, &live_bundle)?;
        let planned = diff_manifests(
            &root,
            "local",
            &desired_manifest,
            live_principal.as_ref(),
            &live_manifest,
            false,
        );

        let txn = access.begin_apply_txn().await?;
        apply_desired_state_changes(&txn, &desired_bundle, &planned).await?;
        txn.commit().await?;

        let remaining_bundle = build_desired_state_live_bundle(&access, &desired_manifest).await?;
        let (remaining_principal, remaining_manifest) =
            live_manifest_from_bundle(&desired_manifest, &remaining_bundle)?;

        let live_behavior = remaining_manifest
            .agent_behaviors
            .iter()
            .find(|b| b.behavior_id == "desc-idempotency-behavior")
            .expect("AgentBehavior should exist after apply");

        assert_eq!(
            live_behavior.description,
            Some("A general-purpose assistant for research and writing tasks.".to_string()),
            "description must persist through apply"
        );
        assert_eq!(
            live_behavior.summary,
            Some("Research assistant".to_string()),
            "summary must persist through apply"
        );

        let second_diff = diff_manifests(
            &root,
            "local",
            &desired_manifest,
            remaining_principal.as_ref(),
            &remaining_manifest,
            false,
        );

        assert!(
            !diff_has_pending_apply(&second_diff.counts),
            "second diff must have no pending apply (idempotent); got: {:?}",
            second_diff.counts
        );
        assert!(
            second_diff
                .collections
                .agent_behaviors
                .unchanged
                .contains(&"desc-idempotency-behavior".to_string()),
            "behavior must be in the 'unchanged' set after re-apply; got: {:?}",
            second_diff.collections.agent_behaviors
        );

        Ok(())
    }

    fn backend_entry(backend_id: &str) -> super::super::DesiredInferenceBackend {
        super::super::DesiredInferenceBackend {
            backend_id: backend_id.to_string(),
            name: backend_id.to_string(),
            provider_kind: Default::default(),
            openai_wire_api: None,
            endpoint: "http://127.0.0.1:9990/v1".to_string(),
            api_key: None,
            api_key_env_var: None,
            max_concurrent: 1,
            max_queue_depth: 8,
            enabled: true,
            models: Vec::new(),
        }
    }

    /// Regression test for #981: a live InferenceBackend absent from the
    /// manifest (e.g. after a backend rename) must be reported live_only and
    /// deleted by prune, even when no behavior references it.
    #[tokio::test]
    async fn diff_prune_detects_and_deletes_live_only_inference_backends() -> Result<()> {
        use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
        use crate::config_import::apply_desired_state_changes;
        use crate::desired_state::{diff_manifests, export_bundle_from_manifest};

        let tempdir = tempfile::tempdir()?;
        let node = EmbeddedNode::builder()
            .data_path(tempdir.path().join("data"))
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;
        let access = ConfigAccess::Local(std::sync::Arc::new(node));

        let mut manifest = manifest_with_subagent_targets(Vec::new());
        manifest.tool_selections.clear();
        manifest.inference_backends = vec![
            backend_entry("openai-sol-high"),
            backend_entry("openai-terra"),
        ];

        let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
        let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
        let planned = diff_manifests(
            std::path::Path::new("/backend-prune"),
            access.mode(),
            &manifest,
            live_principal.as_ref(),
            &live_manifest,
            false,
        );
        let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
        let txn = access.begin_apply_txn().await?;
        apply_desired_state_changes(&txn, &bundle, &planned).await?;
        txn.commit().await?;

        // Rename openai-sol-high -> openai-sol in the manifest; the live
        // document for the old id is now referenced by nothing.
        manifest.inference_backends =
            vec![backend_entry("openai-sol"), backend_entry("openai-terra")];

        let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
        let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
        let drift = diff_manifests(
            std::path::Path::new("/backend-prune"),
            access.mode(),
            &manifest,
            live_principal.as_ref(),
            &live_manifest,
            false,
        );
        assert_eq!(
            drift.collections.inference_backends.live_only,
            vec!["openai-sol-high".to_string()],
            "stale backend must be reported live_only; got: {:?}",
            drift.collections.inference_backends
        );

        let planned = diff_manifests(
            std::path::Path::new("/backend-prune"),
            access.mode(),
            &manifest,
            live_principal.as_ref(),
            &live_manifest,
            true,
        );
        assert_eq!(
            planned.collections.inference_backends.delete,
            vec!["openai-sol-high".to_string()],
            "prune must plan the stale backend for deletion; got: {:?}",
            planned.collections.inference_backends
        );
        let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
        let txn = access.begin_apply_txn().await?;
        apply_desired_state_changes(&txn, &bundle, &planned).await?;
        txn.commit().await?;

        let rows = crate::graphql_rows(
            &access,
            "InferenceBackend",
            "{ InferenceBackend { backend_id } }",
        )
        .await?;
        let mut ids = rows
            .iter()
            .filter_map(|row| row.get("backend_id").and_then(serde_json::Value::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(
            ids,
            vec!["openai-sol".to_string(), "openai-terra".to_string()]
        );
        Ok(())
    }

    /// InferenceBackend documents are node-global: a backend referenced by
    /// another agent's behavior must never be treated as live_only (or
    /// pruned) by this agent's manifest, while a backend referenced by no
    /// one remains prunable.
    #[tokio::test]
    async fn prune_spares_backends_referenced_by_other_agents() -> Result<()> {
        use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
        use crate::config_import::apply_desired_state_changes;
        use crate::desired_state::{diff_manifests, export_bundle_from_manifest};

        let tempdir = tempfile::tempdir()?;
        let node = EmbeddedNode::builder()
            .data_path(tempdir.path().join("data"))
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await?;
        ensure_runtime_schemas(&node).await?;
        let access = ConfigAccess::Local(std::sync::Arc::new(node));

        let mut manifest = manifest_with_subagent_targets(Vec::new());
        manifest.tool_selections.clear();
        manifest.inference_backends = vec![backend_entry("openai-sol")];

        let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
        let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
        let planned = diff_manifests(
            std::path::Path::new("/backend-prune-foreign"),
            access.mode(),
            &manifest,
            live_principal.as_ref(),
            &live_manifest,
            false,
        );
        let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
        let txn = access.begin_apply_txn().await?;
        apply_desired_state_changes(&txn, &bundle, &planned).await?;
        txn.commit().await?;

        access
            .execute(
                r#"mutation { create_InferenceBackend(input: {
                    backend_id: "other-agent-backend",
                    name: "other-agent-backend",
                    endpoint: "http://127.0.0.1:9991/v1",
                    max_concurrent: 1,
                    max_queue_depth: 8,
                    enabled: true
                }) { _docID } }"#,
            )
            .await?;
        access
            .execute(
                r#"mutation { create_AgentBehavior(input: {
                    behavior_id: "other-agent-behavior",
                    agent_did: "did:key:some-other-agent",
                    backend_id: "other-agent-backend",
                    enabled: true
                }) { _docID } }"#,
            )
            .await?;
        access
            .execute(
                r#"mutation { create_InferenceBackend(input: {
                    backend_id: "stale-backend",
                    name: "stale-backend",
                    endpoint: "http://127.0.0.1:9992/v1",
                    max_concurrent: 1,
                    max_queue_depth: 8,
                    enabled: true
                }) { _docID } }"#,
            )
            .await?;

        let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
        let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
        let planned = diff_manifests(
            std::path::Path::new("/backend-prune-foreign"),
            access.mode(),
            &manifest,
            live_principal.as_ref(),
            &live_manifest,
            true,
        );
        assert_eq!(
            planned.collections.inference_backends.delete,
            vec!["stale-backend".to_string()],
            "only the unreferenced backend may be planned for deletion; got: {:?}",
            planned.collections.inference_backends
        );
        assert!(
            planned.collections.inference_backends.live_only.is_empty(),
            "the foreign-referenced backend must not appear live_only; got: {:?}",
            planned.collections.inference_backends
        );

        let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
        let txn = access.begin_apply_txn().await?;
        apply_desired_state_changes(&txn, &bundle, &planned).await?;
        txn.commit().await?;

        let rows = crate::graphql_rows(
            &access,
            "InferenceBackend",
            "{ InferenceBackend { backend_id } }",
        )
        .await?;
        let mut ids = rows
            .iter()
            .filter_map(|row| row.get("backend_id").and_then(serde_json::Value::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(
            ids,
            vec!["openai-sol".to_string(), "other-agent-backend".to_string()]
        );
        Ok(())
    }
}

pub(super) fn optional_string_from_value(
    field: &str,
    value: Option<&serde_json::Value>,
) -> anyhow::Result<Option<String>> {
    use anyhow::anyhow;
    use serde_json::Value;
    match value {
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(Value::Null) | None => Ok(None),
        Some(value) => Err(anyhow!(
            "ToolServiceRegistry field {field} must be a string or null, got {value}"
        )),
    }
}

pub(super) fn optional_i64_from_value(
    field: &str,
    value: Option<&serde_json::Value>,
) -> anyhow::Result<Option<i64>> {
    use anyhow::anyhow;
    use serde_json::Value;
    match value {
        Some(Value::Number(value)) => value
            .as_i64()
            .map(Some)
            .ok_or_else(|| anyhow!("ToolServiceRegistry field {field} must be an integer")),
        Some(Value::Null) | None => Ok(None),
        Some(value) => Err(anyhow!(
            "ToolServiceRegistry field {field} must be an integer or null, got {value}"
        )),
    }
}
