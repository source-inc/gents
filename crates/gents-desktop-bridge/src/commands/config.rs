use anyhow::{anyhow, bail, Result};
use gents_desktop_core::client::ClientCore;
use gents_protocol::row::{
    AgentBehaviorRow, AgentPrincipalRow, InferenceBackendRow, InferenceProfileRow, SkillRow,
    ToolSelectionRow,
};

use super::super::types::{
    AgentConfigSaveRequest, BackendDeleteRequest, BackendSaveRequest, BehaviorDeleteRequest,
    BehaviorSaveRequest, EventTriggerDeleteRequest, InferenceProfileDeleteRequest,
    InferenceProfileSaveRequest, ScheduleDeleteRequest, SkillDeleteRequest, SkillSaveRequest,
    TaskDeleteRequest, ToolSelectionDeleteRequest, ToolSelectionSaveRequest,
    ToolServiceDeleteRequest,
};
use super::util::{require_trimmed, sanitize_id_list, trim_optional};

pub async fn save_agent_config(core: &ClientCore, request: AgentConfigSaveRequest) -> Result<()> {
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    let display_name = require_trimmed("display_name", request.display_name)?;
    let default_behavior_id = require_trimmed("default_behavior_id", request.default_behavior_id)?;

    let store = core.store().snapshot();
    if !store.behaviors.iter().any(|behavior| {
        behavior.agent_did.as_deref() == Some(agent_did.as_str())
            && behavior.behavior_id == default_behavior_id
    }) {
        bail!("default_behavior_id {default_behavior_id} does not exist for {agent_did}");
    }

    let mut row = store
        .agent_principals
        .iter()
        .find(|row| row.agent_did == agent_did)
        .cloned()
        .unwrap_or_else(|| AgentPrincipalRow {
            agent_did: agent_did.clone(),
            display_name: None,
            default_behavior_id: None,
            enabled: Some(true),
            created_at: None,
            created_by: Some(agent_did.clone()),
        });
    row.display_name = Some(display_name);
    row.default_behavior_id = Some(default_behavior_id);
    row.enabled = Some(request.enabled.unwrap_or(true));
    core.save_agent_principal(&row).await?;
    Ok(())
}

pub async fn save_behavior_config(core: &ClientCore, request: BehaviorSaveRequest) -> Result<()> {
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    let behavior_id = require_trimmed("behavior_id", request.behavior_id)?;
    let display_name = require_trimmed("display_name", request.display_name)?;

    let store = core.store().snapshot();
    let mut row = store
        .behavior_row(&agent_did, &behavior_id)
        .cloned()
        .unwrap_or_else(|| AgentBehaviorRow {
            behavior_id: behavior_id.clone(),
            agent_did: Some(agent_did.clone()),
            display_name: None,
            system_prompt: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: None,
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            enabled: Some(true),
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            created_at: None,
        });
    let inference_profile_id = trim_optional(request.inference_profile_id)
        .ok_or_else(|| anyhow!("inference_profile_id is required"))?;
    if !store
        .inference_profiles
        .iter()
        .any(|profile| profile.profile_id == inference_profile_id)
    {
        bail!("inference_profile_id {inference_profile_id} does not exist");
    }
    row.display_name = Some(display_name);
    row.agent_did = Some(agent_did);
    row.system_prompt = Some(request.system_prompt);
    row.backend_id = trim_optional(request.backend_id);
    row.tool_selection_id = trim_optional(request.tool_selection_id);
    row.inference_profile_id = Some(inference_profile_id);
    row.compaction_strategy = trim_optional(request.compaction_strategy);
    row.compaction_threshold = request.compaction_threshold;
    row.enabled = request.enabled.or(row.enabled).or(Some(true));
    row.skill_refs = sanitize_id_list(request.skill_refs);
    row.skill_excludes = sanitize_id_list(request.skill_excludes);
    if let Some(backend_id) = row.backend_id.as_deref() {
        if let Some(model_name) = store
            .inference_backends
            .iter()
            .find(|backend| backend.backend_id == backend_id)
            .and_then(|backend| backend.models.first())
            .cloned()
        {
            row.model_name = Some(model_name);
        }
    }
    core.save_behavior(&row).await?;
    Ok(())
}

#[cfg_attr(test, allow(dead_code))]
pub async fn save_skill_config(core: &ClientCore, request: SkillSaveRequest) -> Result<()> {
    let skill_id = require_trimmed("skill_id", request.skill_id)?;
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    let name = require_trimmed("name", request.name)?;
    let scope = require_trimmed("scope", request.scope)?;
    if scope != "principal" && scope != "behavior" {
        bail!("scope must be \"principal\" or \"behavior\", got {scope:?}");
    }
    let instructions = require_trimmed("instructions", request.instructions)?;

    let store = core.store().snapshot();
    let mut row = store
        .skills
        .iter()
        .find(|row| row.skill_id == skill_id)
        .cloned()
        .unwrap_or_else(|| SkillRow {
            skill_id: skill_id.clone(),
            agent_did: Some(agent_did.clone()),
            scope: None,
            name: None,
            description: None,
            instructions: None,
            tool_refs: Vec::new(),
            display_name: None,
            interface_json: None,
            enabled: Some(true),
            created_at: None,
        });
    row.agent_did = Some(agent_did);
    row.scope = Some(scope);
    row.name = Some(name);
    row.description = trim_optional(request.description);
    row.instructions = Some(instructions);
    row.tool_refs = sanitize_id_list(request.tool_refs);
    row.display_name = trim_optional(request.display_name);
    row.enabled = request.enabled.or(row.enabled).or(Some(true));
    core.save_skill(&row).await?;
    Ok(())
}

#[cfg_attr(test, allow(dead_code))]
pub async fn delete_skill_config(core: &ClientCore, request: SkillDeleteRequest) -> Result<()> {
    let skill_id = require_trimmed("skill_id", request.skill_id)?;
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    core.delete_skill(&skill_id, &agent_did).await
}

#[cfg_attr(test, allow(dead_code))]
pub async fn delete_task_config(core: &ClientCore, request: TaskDeleteRequest) -> Result<()> {
    let task_id = require_trimmed("task_id", request.task_id)?;
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    core.delete_task(&task_id, &agent_did).await
}

#[cfg_attr(test, allow(dead_code))]
pub async fn delete_schedule_config(
    core: &ClientCore,
    request: ScheduleDeleteRequest,
) -> Result<()> {
    let schedule_id = require_trimmed("schedule_id", request.schedule_id)?;
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    core.delete_schedule(&schedule_id, &agent_did).await
}

#[cfg_attr(test, allow(dead_code))]
pub async fn delete_event_trigger_config(
    core: &ClientCore,
    request: EventTriggerDeleteRequest,
) -> Result<()> {
    let trigger_id = require_trimmed("trigger_id", request.trigger_id)?;
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    core.delete_event_trigger(&trigger_id, &agent_did).await
}

#[cfg_attr(test, allow(dead_code))]
pub async fn delete_backend_config(core: &ClientCore, request: BackendDeleteRequest) -> Result<()> {
    let backend_id = require_trimmed("backend_id", request.backend_id)?;
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    core.delete_inference_backend(&backend_id, &agent_did).await
}

#[cfg_attr(test, allow(dead_code))]
pub async fn delete_inference_profile_config(
    core: &ClientCore,
    request: InferenceProfileDeleteRequest,
) -> Result<()> {
    let profile_id = require_trimmed("profile_id", request.profile_id)?;
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    core.delete_inference_profile(&profile_id, &agent_did).await
}

#[cfg_attr(test, allow(dead_code))]
pub async fn delete_tool_selection_config(
    core: &ClientCore,
    request: ToolSelectionDeleteRequest,
) -> Result<()> {
    let selection_id = require_trimmed("selection_id", request.selection_id)?;
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    core.delete_tool_selection(&selection_id, &agent_did).await
}

#[cfg_attr(test, allow(dead_code))]
pub async fn delete_tool_service_config(
    core: &ClientCore,
    request: ToolServiceDeleteRequest,
) -> Result<()> {
    let service_id = require_trimmed("service_id", request.service_id)?;
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    core.delete_tool_service(&service_id, &agent_did).await
}

#[cfg_attr(test, allow(dead_code))]
pub async fn delete_behavior_config(
    core: &ClientCore,
    request: BehaviorDeleteRequest,
) -> Result<()> {
    let behavior_id = require_trimmed("behavior_id", request.behavior_id)?;
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    core.delete_behavior(&behavior_id, &agent_did).await
}

pub async fn save_backend_config(core: &ClientCore, request: BackendSaveRequest) -> Result<()> {
    let backend_id = require_trimmed("backend_id", request.backend_id)?;
    let name = require_trimmed("name", request.name)?;
    let provider_kind = require_trimmed("provider_kind", request.provider_kind)?;
    let endpoint = require_trimmed("endpoint", request.endpoint)?;
    let models = request
        .models
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if models.is_empty() {
        bail!("at least one model is required");
    }

    let store = core.store().snapshot();
    let mut row = store
        .inference_backends
        .iter()
        .find(|row| row.backend_id == backend_id)
        .cloned()
        .unwrap_or_else(|| InferenceBackendRow {
            backend_id: backend_id.clone(),
            name: None,
            provider_kind: None,
            openai_wire_api: None,
            endpoint: None,
            api_key: None,
            api_key_env_var: None,
            max_concurrent: None,
            max_queue_depth: None,
            enabled: Some(true),
            models: Vec::new(),
            last_probe: None,
            probe_status: None,
        });
    row.name = Some(name);
    row.provider_kind = Some(provider_kind);
    if request.openai_wire_api.is_some() {
        row.openai_wire_api = trim_optional(request.openai_wire_api);
    }
    row.endpoint = Some(endpoint);
    if request.clear_api_key.unwrap_or(false) {
        row.api_key = None;
    } else if request.api_key.is_some() {
        row.api_key = trim_optional(request.api_key);
    }
    if request.api_key_env_var.is_some() {
        row.api_key_env_var = trim_optional(request.api_key_env_var);
    }
    row.models = models;
    row.max_concurrent = request.max_concurrent;
    row.max_queue_depth = request.max_queue_depth;
    row.enabled = request.enabled.or(row.enabled).or(Some(true));
    row.probe_status = Some("healthy".to_string());
    core.save_backend(&row).await?;
    Ok(())
}

pub async fn save_inference_profile_config(
    core: &ClientCore,
    request: InferenceProfileSaveRequest,
) -> Result<()> {
    let profile_id = require_trimmed("profile_id", request.profile_id)?;
    let display_name = require_trimmed("display_name", request.display_name)?;
    if request
        .stream_liveness_timeout_secs
        .is_some_and(|value| value <= 0)
    {
        anyhow::bail!("stream_liveness_timeout_secs must be positive");
    }
    if request.reasoning_effort.as_deref().is_some_and(|value| {
        !matches!(
            value,
            "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max" | "ultra"
        )
    }) {
        anyhow::bail!(
            "reasoning_effort must be one of: none, minimal, low, medium, high, xhigh, max, ultra"
        );
    }

    let store = core.store().snapshot();
    let mut row = store
        .inference_profiles
        .iter()
        .find(|row| row.profile_id == profile_id)
        .cloned()
        .unwrap_or_else(|| InferenceProfileRow {
            profile_id: profile_id.clone(),
            display_name: None,
            context_window: None,
            max_output_tokens: None,
            max_turns: None,
            temperature: None,
            stream_batch_ms: None,
            stream_liveness_timeout_secs: None,
            deadline_duration_secs: None,
            retry_max_transport: None,
            retry_backoff_ms: None,
            retry_max_resample: None,
            retry_allow_repair: None,
            retry_interactive_max: None,
            top_p: None,
            top_k: None,
            seed: None,
            min_p: None,
            frequency_penalty: None,
            presence_penalty: None,
            repetition_penalty: None,
            reasoning_effort: None,
        });
    row.display_name = Some(display_name);
    row.context_window = request.context_window;
    row.max_output_tokens = request.max_output_tokens;
    row.max_turns = request.max_turns;
    row.temperature = request.temperature;
    row.reasoning_effort = request.reasoning_effort;
    row.stream_batch_ms = request.stream_batch_ms;
    row.stream_liveness_timeout_secs = request.stream_liveness_timeout_secs;
    row.deadline_duration_secs = request.deadline_duration_secs;
    core.save_inference_profile(&row).await?;
    Ok(())
}

pub async fn save_tool_selection_config(
    core: &ClientCore,
    request: ToolSelectionSaveRequest,
) -> Result<()> {
    let agent_did = require_trimmed("agent_did", request.agent_did)?;
    let selection_id = require_trimmed("selection_id", request.selection_id)?;
    let display_name = require_trimmed("display_name", request.display_name)?;

    let store = core.store().snapshot();
    let mut row = store
        .tool_selections
        .iter()
        .find(|row| row.selection_id == selection_id)
        .cloned()
        .unwrap_or_else(|| ToolSelectionRow {
            selection_id: selection_id.clone(),
            agent_did: Some(agent_did.clone()),
            display_name: None,
            tool_policy_version: None,
            subagent_default_await_mode: None,
            write_tools: Vec::new(),
            enable_self_config: None,
            self_config_categories: Vec::new(),
            self_config_no_lockout: None,
            self_config_dry_run: None,
            enable_file_tools: Some(false),
            file_tools_mode: None,
            file_tool_root: None,
            enable_bash: Some(false),
            bash_mode: None,
            command_execution_policy: None,
            read_only_command_allowlist: Vec::new(),
            command_allowed_argv_prefixes: Vec::new(),
            command_forbidden_argv_prefixes: Vec::new(),
            command_network_mode: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: Some(false),
            allowed_mcp_service_ids: Vec::new(),
            delegate_to: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            subagent_targets: Vec::new(),
            subagent_spawn_enabled: Some(false),
            orchestration_enabled: Some(false),
            subagent_steering_enabled: Some(false),
            subagent_background_enabled: Some(false),
            subagent_allow_cross_deployment: Some(false),
            cross_deployment_spawn_timeout_seconds: None,
            enable_memory: Some(false),
            enable_session_history_tool: Some(false),
            enable_context_budget: Some(true),
            enable_defra_query: Some(false),
            defra_query_collections: Vec::new(),
        });
    row.agent_did = Some(agent_did);
    row.display_name = Some(display_name);
    row.enable_file_tools = request.enable_file_tools.or(row.enable_file_tools);
    row.file_tools_mode = trim_optional(request.file_tools_mode);
    row.file_tool_root = trim_optional(request.file_tool_root);
    row.enable_bash = request.enable_bash.or(row.enable_bash);
    row.bash_mode = trim_optional(request.bash_mode);
    row.command_execution_policy = trim_optional(request.command_execution_policy);
    row.command_allowed_argv_prefixes = request
        .command_allowed_argv_prefixes
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    row.command_forbidden_argv_prefixes = request
        .command_forbidden_argv_prefixes
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    row.command_network_mode = trim_optional(request.command_network_mode);
    row.cli_tool_names = request
        .cli_tool_names
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    row.enable_meta_tools = request.enable_meta_tools.or(row.enable_meta_tools);
    row.allowed_mcp_service_ids = request
        .allowed_mcp_service_ids
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    row.delegate_to = request
        .delegate_to
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    row.backgroundable_tool_names = request
        .backgroundable_tool_names
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    row.subagent_targets = request
        .subagent_targets
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    row.subagent_spawn_enabled = request
        .subagent_spawn_enabled
        .or(row.subagent_spawn_enabled);
    row.subagent_steering_enabled = request
        .subagent_steering_enabled
        .or(row.subagent_steering_enabled);
    row.subagent_background_enabled = request
        .subagent_background_enabled
        .or(row.subagent_background_enabled);
    row.subagent_allow_cross_deployment = request
        .subagent_allow_cross_deployment
        .or(row.subagent_allow_cross_deployment);
    row.cross_deployment_spawn_timeout_seconds = request.cross_deployment_spawn_timeout_seconds;
    row.enable_memory = request.enable_memory.or(row.enable_memory);
    row.enable_session_history_tool = request
        .enable_session_history_tool
        .or(row.enable_session_history_tool);
    row.enable_context_budget = request.enable_context_budget.or(row.enable_context_budget);
    row.enable_defra_query = request.enable_defra_query.or(row.enable_defra_query);
    if let Some(collections) = request.defra_query_collections {
        row.defra_query_collections = collections
            .into_iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();
    }
    row.subagent_default_await_mode =
        trim_optional(request.subagent_default_await_mode).or(row.subagent_default_await_mode);
    row.orchestration_enabled = request.orchestration_enabled.or(row.orchestration_enabled);
    core.save_tool_selection(&row).await?;
    Ok(())
}
