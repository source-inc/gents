use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::Result;
use gents::template::{
    catalog::{default_catalog, Site},
    reads::validate_system_template,
    validate_request_context_template,
};
use gents::{
    is_reserved_builtin_tool_name, parse_template_for_validation,
    schedule_cron::validate_cron_schedule, CommandExecutionMode, CommandNetworkMode,
    SubagentTarget, SurfaceToolDecl, VariableRef, WriteToolDecl,
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
    let mut backend_models = HashMap::<String, BTreeSet<String>>::new();
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
        validate_surface_entries(
            &format!("surface:{}", surface.surface_id),
            &surface.entries,
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

        if !backend_id.is_empty() {
            backend_models.insert(
                backend_id.to_string(),
                backend
                    .models
                    .iter()
                    .map(|model| model.trim())
                    .filter(|model| !model.is_empty())
                    .map(str::to_string)
                    .collect(),
            );
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
        if profile.seed.is_some_and(|value| value < 0) {
            errors.push(format!(
                "InferenceProfile {profile_id} seed must be non-negative"
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
            } else if let Some(model_name) = non_empty(&behavior.model_name) {
                let advertised = backend_models
                    .get(backend_id)
                    .expect("known backend has a model entry");
                if !advertised.is_empty() && !advertised.contains(model_name) {
                    errors.push(format!(
                        "behavior {} selects model {} which backend {} does not advertise",
                        behavior.behavior_id, model_name, backend_id
                    ));
                }
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
                                if (root == "doc" || root == "args" || root == "group")
                                    && reported.insert(root)
                                {
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
        } else if let Err(error) =
            gents::graphql::validate_collection_identifier(trig.source_collection.trim())
        {
            errors.push(format!(
                "event_trigger {} has invalid source_collection {:?}: {}",
                trig.trigger_id, trig.source_collection, error
            ));
        }

        if trig.event_kind != "created" {
            errors.push(format!(
                "event_trigger {} uses unsupported event_kind {:?} (v1 supports only \"created\")",
                trig.trigger_id, trig.event_kind
            ));
        }

        if let Some(authority) = trig
            .workspace_authority
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if let Err(error) = gents::toolset::WorkspaceAuthority::parse(authority) {
                errors.push(format!(
                    "event_trigger {} has invalid workspace_authority {authority:?}: {error}",
                    trig.trigger_id
                ));
            }
        }

        match trig.concurrency.trim() {
            "parallel" | "serial" | "latest_only" => {}
            other => errors.push(format!(
                "event_trigger {} in event-triggers manifest has unknown concurrency {}; expected parallel|serial|latest_only",
                trig.trigger_id, other
            )),
        }

        let fire_mode = trig
            .fire_mode
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("per_document");
        if !matches!(fire_mode, "per_document" | "per_group") {
            errors.push(format!(
                "event_trigger {} has unknown fire_mode {:?}; expected per_document|per_group",
                trig.trigger_id, fire_mode
            ));
        }
        for (label, field) in [
            ("correlation_field", trig.correlation_field.as_deref()),
            ("expected_count_field", trig.expected_count_field.as_deref()),
        ] {
            if let Some(field) = field.map(str::trim).filter(|value| !value.is_empty()) {
                if let Err(error) = gents::graphql::validate_graphql_name(field) {
                    errors.push(format!(
                        "event_trigger {} has invalid {} {:?}: {}",
                        trig.trigger_id, label, field, error
                    ));
                }
            }
        }
        let has_correlation = trig
            .correlation_field
            .as_deref()
            .is_some_and(|field| !field.trim().is_empty());
        let has_expected_field = trig
            .expected_count_field
            .as_deref()
            .is_some_and(|field| !field.trim().is_empty());
        let has_timeout = trig.group_timeout_secs.is_some();
        if trig
            .expected_count
            .is_some_and(|count| count <= 0 || count as usize > gents::MAX_EVENT_TRIGGER_GROUP_DOCS)
        {
            errors.push(format!(
                "event_trigger {} expected_count must be in 1..={}",
                trig.trigger_id,
                gents::MAX_EVENT_TRIGGER_GROUP_DOCS
            ));
        }
        if trig.group_timeout_secs.is_some_and(|seconds| seconds <= 0) {
            errors.push(format!(
                "event_trigger {} group_timeout_secs must be positive",
                trig.trigger_id
            ));
        }
        if trig
            .group_min_count
            .is_some_and(|count| count <= 0 || count as usize > gents::MAX_EVENT_TRIGGER_GROUP_DOCS)
        {
            errors.push(format!(
                "event_trigger {} group_min_count must be in 1..={}",
                trig.trigger_id,
                gents::MAX_EVENT_TRIGGER_GROUP_DOCS
            ));
        }
        if trig.group_min_count.is_some() && !has_timeout {
            errors.push(format!(
                "event_trigger {} group_min_count requires group_timeout_secs",
                trig.trigger_id
            ));
        }
        if let (Some(minimum), Some(expected)) = (trig.group_min_count, trig.expected_count) {
            if minimum > expected {
                errors.push(format!(
                    "event_trigger {} group_min_count cannot exceed expected_count",
                    trig.trigger_id
                ));
            }
        }
        match fire_mode {
            "per_document" => {
                if trig.expected_count.is_some()
                    || has_expected_field
                    || has_timeout
                    || trig.group_min_count.is_some()
                {
                    errors.push(format!(
                        "event_trigger {} per_document mode cannot configure group count or timeout fields",
                        trig.trigger_id
                    ));
                }
            }
            "per_group" => {
                if !has_correlation {
                    errors.push(format!(
                        "event_trigger {} per_group mode requires correlation_field",
                        trig.trigger_id
                    ));
                }
                if trig.expected_count.is_some() && has_expected_field {
                    errors.push(format!(
                        "event_trigger {} must configure only one of expected_count or expected_count_field",
                        trig.trigger_id
                    ));
                }
                if trig.expected_count.is_none() && !has_expected_field && !has_timeout {
                    errors.push(format!(
                        "event_trigger {} per_group mode requires a count source or group_timeout_secs",
                        trig.trigger_id
                    ));
                }
            }
            _ => {}
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
                                if root == "group"
                                    && fire_mode != "per_group"
                                    && reported.insert("group")
                                {
                                    errors.push(format!(
                                        "event_trigger {} prompt template references group.* outside per_group mode",
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

    for binding in &manifest.callback_bindings {
        let binding_id = binding.binding_id.trim();
        if binding_id.is_empty() {
            errors.push(
                "callback-bindings manifest contains a binding with an empty binding_id"
                    .to_string(),
            );
            continue;
        }
        if binding.source_collection.trim().is_empty() {
            errors.push(format!(
                "callback_binding {binding_id} must contain a non-empty source_collection"
            ));
        } else if let Err(error) =
            gents::graphql::validate_collection_identifier(binding.source_collection.trim())
        {
            errors.push(format!(
                "callback_binding {binding_id} has invalid source_collection {:?}: {error}",
                binding.source_collection
            ));
        }
        if binding.event_kind.trim() != "created" {
            errors.push(format!(
                "callback_binding {binding_id} uses unsupported event_kind {:?} (v1 supports only \"created\")",
                binding.event_kind
            ));
        }
        if binding
            .builtin_emitter
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
            && binding
                .module_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
        {
            errors.push(format!(
                "callback_binding {binding_id} needs builtin_emitter or module_id"
            ));
        }
        if let Err(error) = gents::reject_secret_bearing_callback_fields(
            binding_id,
            binding.filter.as_deref(),
            binding.source_fields.as_deref(),
        ) {
            errors.push(error.to_string());
        }
    }

    for placement in &manifest.repository_placements {
        if placement.repository_id.trim().is_empty() {
            errors.push(
                "repository-placements manifest contains a placement with an empty repository_id"
                    .to_string(),
            );
        }
        if placement.host_path.trim().is_empty() {
            errors.push(format!(
                "repository_placement {} must contain a non-empty host_path",
                placement.repository_id
            ));
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
        if let Err(error) = gents::graphql::validate_collection_identifier(source_collection) {
            errors.push(format!(
                "event_trigger {} has invalid source_collection {:?}: {}",
                trigger_id, trig.source_collection, error
            ));
            continue;
        }

        if let Some(filter) = trig.filter.as_deref().map(str::trim) {
            if !filter.is_empty() {
                // `filter` is interpolated into the probe query as a raw filter
                // fragment; validate it like the runtime trigger engine does
                // (`trigger_engine::event_source`) before building the probe.
                // `source_collection` is already validated by the guard above.
                if let Err(err) = gents::graphql::validate_graphql_filter_fragment(filter) {
                    errors.push(format!(
                        "event_trigger {} filter is not a valid filter fragment: {}",
                        trigger_id, err
                    ));
                } else {
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
        if doc_paths.is_empty()
            && trig.correlation_field.is_none()
            && trig.expected_count_field.is_none()
        {
            continue;
        }

        let introspect = format!(
            r#"query {{ __type(name: "{name}") {{ fields {{ name type {{ name kind }} }} }} }}"#,
            name = gents::graphql::escape_graphql_string(source_collection),
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
        let field_types: HashMap<&str, &str> = fields
            .iter()
            .filter_map(|field| {
                Some((
                    field.get("name")?.as_str()?,
                    field.get("type")?.get("name")?.as_str()?,
                ))
            })
            .collect();
        if let Some(field) = trig
            .correlation_field
            .as_deref()
            .map(str::trim)
            .filter(|field| !field.is_empty())
        {
            match field_types.get(field).copied() {
                Some("String") => {}
                Some(actual) => errors.push(format!(
                    "event_trigger {} correlation_field {} must be String, found {}",
                    trigger_id, field, actual
                )),
                None => errors.push(format!(
                    "event_trigger {} correlation_field {} does not exist on {}",
                    trigger_id, field, source_collection
                )),
            }
        }
        if let Some(field) = trig
            .expected_count_field
            .as_deref()
            .map(str::trim)
            .filter(|field| !field.is_empty())
        {
            match field_types.get(field).copied() {
                Some("String" | "Int") => {}
                Some(actual) => errors.push(format!(
                    "event_trigger {} expected_count_field {} must be String or Int, found {}",
                    trigger_id, field, actual
                )),
                None => errors.push(format!(
                    "event_trigger {} expected_count_field {} does not exist on {}",
                    trigger_id, field, source_collection
                )),
            }
        }
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
    use gents::{is_reserved_builtin_tool_name, SurfaceToolDecl};
    use std::collections::{BTreeMap, BTreeSet};

    // Trimmed to match the uniqueness check above and the lookup below.
    let surfaces: BTreeMap<&str, &DesiredDatastoreToolSurface> = manifest
        .datastore_tool_surfaces
        .iter()
        .map(|s| (s.surface_id.trim(), s))
        .collect();

    let mut merged: Vec<String> = selection.write_tools.clone();
    let mut seen_names: BTreeSet<String> = BTreeSet::new();
    for entry in &selection.write_tools {
        if let Ok(decl) = serde_json::from_str::<WriteToolDecl>(entry) {
            seen_names.insert(decl.tool_name);
        }
    }

    let mut linked_ids: BTreeSet<&str> = BTreeSet::new();
    for surface_id in &selection.datastore_tool_surface_ids {
        let surface_id = surface_id.trim();
        if surface_id.is_empty() {
            errors.push(format!(
                "tool selection {} has an empty datastore_tool_surface_ids entry",
                selection.selection_id
            ));
            continue;
        }
        if !linked_ids.insert(surface_id) {
            // Expanding twice would trip the tool_name collision check and
            // blame the wrong thing.
            errors.push(format!(
                "tool selection {} lists DatastoreToolSurface {} more than once",
                selection.selection_id, surface_id
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
            match serde_json::from_str::<SurfaceToolDecl>(entry) {
                Ok(decl) => {
                    if let Err(error) = decl.validate() {
                        errors.push(format!(
                            "DatastoreToolSurface {surface_id} has a malformed entry: {error}"
                        ));
                        continue;
                    }
                    if !seen_names.insert(decl.tool_name().to_string()) {
                        errors.push(format!(
                            "duplicate tool_name {:?} after expanding DatastoreToolSurface {} for tool selection {}",
                            decl.tool_name(),
                            surface_id,
                            selection.selection_id
                        ));
                    }
                    match decl {
                        SurfaceToolDecl::Create(_) => merged.push(entry.clone()),
                        SurfaceToolDecl::Query(_) => {
                            // Creates are re-checked by `validate_write_tools`
                            // below; query entries never enter that list.
                            if is_reserved_builtin_tool_name(decl.tool_name()) {
                                errors.push(format!(
                                    "DatastoreToolSurface {} tool_name {:?} collides with a built-in tool",
                                    surface_id,
                                    decl.tool_name()
                                ));
                            }
                            if selection
                                .cli_tool_names
                                .iter()
                                .any(|name| name.trim() == decl.tool_name())
                            {
                                errors.push(format!(
                                    "DatastoreToolSurface {} tool_name {:?} collides with a cli_tool_names entry in tool selection {}",
                                    surface_id,
                                    decl.tool_name(),
                                    selection.selection_id
                                ));
                            }
                        }
                    }
                }
                Err(error) => errors.push(format!(
                    "DatastoreToolSurface {} entry is not valid create/query tool JSON: {error}",
                    surface_id
                )),
            }
        }
    }

    // Re-run field-level checks over the merged create list.
    validate_write_tools(
        &selection.selection_id,
        &merged,
        &selection.cli_tool_names,
        errors,
    );
}

fn validate_surface_entries(label: &str, entries: &[String], errors: &mut Vec<String>) {
    let mut seen_tool_names: HashSet<String> = HashSet::new();
    for entry in entries {
        let decl: SurfaceToolDecl = match serde_json::from_str(entry) {
            Ok(decl) => decl,
            Err(error) => {
                errors.push(format!(
                    "{label} entry {entry:?} is not valid create/query tool JSON: {error}"
                ));
                continue;
            }
        };
        if !decl.is_well_formed() {
            errors.push(format!(
                "{label} entry {entry:?} is malformed (tool_name/collection required; query entries also need a projection)"
            ));
            continue;
        }
        if is_reserved_builtin_tool_name(decl.tool_name()) {
            errors.push(format!(
                "{label} tool_name {:?} collides with a built-in tool",
                decl.tool_name()
            ));
        }
        if !seen_tool_names.insert(decl.tool_name().to_string()) {
            errors.push(format!(
                "{label} has a duplicate tool_name {:?}",
                decl.tool_name()
            ));
        }
        if let SurfaceToolDecl::Create(create) = decl {
            if !create.output_obligation_is_well_formed() {
                errors.push(format!(
                    "{label} tool {:?} output_obligation.minimum_writes must be greater than zero and output_obligation.expected_count_field, when present, must name a required model-provided field",
                    create.tool_name
                ));
            }
        }
    }
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
        if let Err(error) = decl.validate() {
            errors.push(format!(
                "tool selection {selection_id} write_tools entry for tool {:?} is malformed: {error}",
                decl.tool_name
            ));
        }
        if !decl.output_obligation_is_well_formed() {
            errors.push(format!(
                "tool selection {selection_id} write_tools tool {:?} output_obligation.minimum_writes must be greater than zero and output_obligation.expected_count_field, when present, must name a required model-provided field",
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
            if !seen_field_names.insert(field.name.trim().to_string()) {
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
            callback_bindings: Vec::new(),
            repository_placements: Vec::new(),
        }
    }

    fn validate_errors(manifest: DesiredStateManifest) -> Vec<String> {
        let mut errors = Vec::new();
        validate_manifest(&manifest, &mut errors);
        errors
    }

    #[test]
    fn behavior_model_must_be_advertised_by_its_backend() {
        let mut manifest = manifest(None, None);
        manifest
            .inference_backends
            .push(super::super::DesiredInferenceBackend {
                backend_id: "reviewers".to_string(),
                name: "Reviewers".to_string(),
                provider_kind: Default::default(),
                openai_wire_api: None,
                endpoint: "http://127.0.0.1:8000/v1".to_string(),
                api_key: None,
                api_key_env_var: None,
                max_concurrent: 4,
                max_queue_depth: 8,
                enabled: true,
                models: vec!["d4f".to_string()],
            });
        manifest.agent_behaviors[0].backend_id = Some("reviewers".to_string());
        manifest.agent_behaviors[0].model_name = Some("GLM-5.2".to_string());

        let errors = validate_errors(manifest);
        assert!(errors.iter().any(|error| {
            error.contains(
                "behavior default selects model GLM-5.2 which backend reviewers does not advertise",
            )
        }));
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
                seed: None,
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
mod live_tests;
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
