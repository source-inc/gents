use std::collections::HashMap;

use gents::{BashMode, FileToolMode};
use gents_desktop_core::client::{ClientCore, ClientPeerStatus, PeerRecord};

use super::super::types::{
    normalize_optional, turn_state_label, AgentPrincipalView, BehaviorEnvironmentView,
    BehaviorView, ConversationSummary, DeploymentView, DesktopRuntimeSnapshot, EventTriggerView,
    InferenceBackendView, InferenceProfileView, RuntimeView, ScheduleView, SkillView, TaskView,
    ToolSelectionView, ToolServiceRegistryView,
};
use super::runtime_tasks::{
    conversation_task_tag, recent_runs_for_task_views, request_backed_conversation_summaries,
    retain_latest_conversation_summaries, source_matches_agent, task_run_history,
};
use super::to_health_view;

pub async fn build_runtime_snapshot(core: &ClientCore) -> DesktopRuntimeSnapshot {
    let store = core.store().snapshot();
    let peer_records = core.peer_records().await;
    let peer_statuses: HashMap<String, ClientPeerStatus> = core
        .peer_statuses()
        .into_iter()
        .map(|status| (status.agent_did.clone(), status))
        .collect();

    let mut deployments = peer_records
        .into_iter()
        .map(|peer| {
            let status = peer_statuses.get(&peer.agent_did);
            let require_source_scope = peer.is_bearer_pairing()
                || peer
                    .graphql
                    .as_deref()
                    .is_some_and(|graphql| !graphql.trim().is_empty());
            let principal = store
                .agent_principals
                .iter()
                .find(|row| row.agent_did == peer.agent_did);
            let mut agent_principal = principal
                .map(|row| AgentPrincipalView {
                    agent_did: row.agent_did.clone(),
                    display_name: normalize_optional(row.display_name.as_deref()),
                    default_behavior_id: normalize_optional(row.default_behavior_id.as_deref()),
                    enabled: row.enabled,
                    created_at: normalize_optional(row.created_at.as_deref()),
                    created_by: normalize_optional(row.created_by.as_deref()),
                })
                .unwrap_or_else(|| AgentPrincipalView {
                    agent_did: peer.agent_did.clone(),
                    display_name: Some(peer.label.clone()),
                    default_behavior_id: None,
                    enabled: Some(true),
                    created_at: None,
                    created_by: None,
                });
            let mut default_behavior_id = store
                .default_behavior_id_for_agent(&peer.agent_did)
                .map(str::to_owned)
                .or_else(|| normalize_optional(peer.default_behavior_id.as_deref()));
            let mut runtime = store
                .latest_runtime(&peer.agent_did)
                .map(|row| RuntimeView {
                    process_state: normalize_optional(row.process_state.as_deref()),
                    reconcile_phase: normalize_optional(row.reconcile_phase.as_deref()),
                    last_reconcile_result: normalize_optional(row.last_reconcile_result.as_deref()),
                    last_reconcile_error: normalize_optional(row.last_reconcile_error.as_deref()),
                    updated_at: normalize_optional(row.updated_at.as_deref()),
                    behavior_executor_capacity: row.behavior_executor_capacity,
                    behavior_executor_queue_depth: row.behavior_executor_queue_depth,
                    runnable_behavior_count: row.runnable_behavior_count,
                    unavailable_behavior_count: row.unavailable_behavior_count,
                });

            let mut behaviors = store
                .behavior_rows(&peer.agent_did)
                .into_iter()
                .map(|row| BehaviorView {
                    behavior_id: row.behavior_id.clone(),
                    display_name: normalize_optional(row.display_name.as_deref())
                        .unwrap_or_else(|| row.behavior_id.clone()),
                    system_prompt: normalize_optional(row.system_prompt.as_deref()),
                    backend_id: normalize_optional(row.backend_id.as_deref()),
                    model_name: normalize_optional(row.model_name.as_deref()),
                    tool_selection_id: normalize_optional(row.tool_selection_id.as_deref()),
                    inference_profile_id: normalize_optional(row.inference_profile_id.as_deref()),
                    compaction_strategy: normalize_optional(row.compaction_strategy.as_deref()),
                    compaction_threshold: row.compaction_threshold,
                    enabled: row.enabled.unwrap_or(true),
                    is_default: default_behavior_id.as_deref() == Some(row.behavior_id.as_str()),
                    skill_refs: row.skill_refs.clone(),
                    skill_excludes: row.skill_excludes.clone(),
                })
                .collect::<Vec<_>>();
            if peer_can_infer_behaviors(&peer) && behaviors.is_empty() {
                let behavior_ids = inferred_peer_behavior_ids(
                    store
                        .conversation_rows(&peer.agent_did)
                        .into_iter()
                        .filter_map(|row| row.behavior_id.as_deref())
                        .chain(peer.default_behavior_id.as_deref()),
                );
                default_behavior_id = inferred_default_behavior_id(
                    &peer.agent_did,
                    default_behavior_id.as_deref(),
                    &behavior_ids,
                );
                agent_principal.default_behavior_id = default_behavior_id.clone();
                behaviors = behavior_ids
                    .into_iter()
                    .map(|behavior_id| BehaviorView {
                        display_name: inferred_behavior_display_name(
                            &behavior_id,
                            &peer.label,
                            default_behavior_id.as_deref() == Some(behavior_id.as_str()),
                        ),
                        is_default: default_behavior_id.as_deref() == Some(behavior_id.as_str()),
                        behavior_id,
                        system_prompt: None,
                        backend_id: None,
                        model_name: None,
                        tool_selection_id: None,
                        inference_profile_id: None,
                        compaction_strategy: None,
                        compaction_threshold: None,
                        enabled: true,
                        skill_refs: Vec::new(),
                        skill_excludes: Vec::new(),
                    })
                    .collect();
            }
            behaviors.sort_by(|left, right| {
                right
                    .is_default
                    .cmp(&left.is_default)
                    .then_with(|| left.display_name.cmp(&right.display_name))
            });
            let behavior_ids = behaviors
                .iter()
                .map(|behavior| behavior.behavior_id.as_str())
                .collect::<Vec<_>>();
            let mut inference_backends = store
                .inference_backends
                .iter()
                .enumerate()
                .filter(|(index, _row)| {
                    source_matches_agent(
                        &store.inference_backend_source_agent_dids,
                        *index,
                        &peer.agent_did,
                        false,
                    )
                })
                .map(|(_index, row)| InferenceBackendView {
                    backend_id: row.backend_id.clone(),
                    name: normalize_optional(row.name.as_deref()),
                    provider_kind: normalize_optional(row.provider_kind.as_deref()),
                    openai_wire_api: normalize_optional(row.openai_wire_api.as_deref()),
                    endpoint: normalize_optional(row.endpoint.as_deref()),
                    api_key_configured: normalize_optional(row.api_key.as_deref()).is_some(),
                    api_key_env_var: normalize_optional(row.api_key_env_var.as_deref()),
                    max_concurrent: row.max_concurrent,
                    max_queue_depth: row.max_queue_depth,
                    enabled: row.enabled,
                    models: row.models.clone(),
                    probe_status: normalize_optional(row.probe_status.as_deref()),
                })
                .collect::<Vec<_>>();
            inference_backends.sort_by(|left, right| left.backend_id.cmp(&right.backend_id));

            let mut inference_profiles = store
                .inference_profiles
                .iter()
                .enumerate()
                .filter(|(index, _row)| {
                    source_matches_agent(
                        &store.inference_profile_source_agent_dids,
                        *index,
                        &peer.agent_did,
                        false,
                    )
                })
                .map(|(_index, row)| InferenceProfileView {
                    profile_id: row.profile_id.clone(),
                    display_name: normalize_optional(row.display_name.as_deref()),
                    context_window: row.context_window,
                    max_output_tokens: row.max_output_tokens,
                    max_turns: row.max_turns,
                    temperature: row.temperature,
                    reasoning_effort: normalize_optional(row.reasoning_effort.as_deref()),
                    stream_batch_ms: row.stream_batch_ms,
                    stream_liveness_timeout_secs: row.stream_liveness_timeout_secs,
                    deadline_duration_secs: row.deadline_duration_secs,
                })
                .collect::<Vec<_>>();
            inference_profiles.sort_by(|left, right| left.profile_id.cmp(&right.profile_id));

            let mut tool_selections = store
                .tool_selections
                .iter()
                .filter(|row| row.agent_did.as_deref() == Some(peer.agent_did.as_str()))
                .map(|row| ToolSelectionView {
                    selection_id: row.selection_id.clone(),
                    agent_did: normalize_optional(row.agent_did.as_deref()),
                    display_name: normalize_optional(row.display_name.as_deref()),
                    enable_file_tools: row.enable_file_tools,
                    file_tools_mode: normalize_optional(row.file_tools_mode.as_deref()),
                    file_tool_root: normalize_optional(row.file_tool_root.as_deref()),
                    enable_bash: row.enable_bash,
                    bash_mode: normalize_optional(row.bash_mode.as_deref()),
                    command_execution_policy: normalize_optional(
                        row.command_execution_policy.as_deref(),
                    ),
                    command_allowed_argv_prefixes: row.command_allowed_argv_prefixes.clone(),
                    command_forbidden_argv_prefixes: row.command_forbidden_argv_prefixes.clone(),
                    command_network_mode: normalize_optional(row.command_network_mode.as_deref()),
                    cli_tool_names: row.cli_tool_names.clone(),
                    enable_meta_tools: row.enable_meta_tools,
                    allowed_mcp_service_ids: row.allowed_mcp_service_ids.clone(),
                    delegate_to: row.delegate_to.clone(),
                    backgroundable_tool_names: row.backgroundable_tool_names.clone(),
                    subagent_targets: row.subagent_targets.clone(),
                    subagent_spawn_enabled: row.subagent_spawn_enabled,
                    subagent_steering_enabled: row.subagent_steering_enabled,
                    subagent_background_enabled: row.subagent_background_enabled,
                    subagent_allow_cross_deployment: row.subagent_allow_cross_deployment,
                    cross_deployment_spawn_timeout_seconds: row
                        .cross_deployment_spawn_timeout_seconds,
                    enable_memory: row.enable_memory,
                    enable_session_history_tool: row.enable_session_history_tool,
                    enable_context_budget: row.enable_context_budget,
                    enable_defra_query: row.enable_defra_query,
                    defra_query_collections: row.defra_query_collections.clone(),
                    write_tools: row.write_tools.clone(),
                    tool_policy_version: normalize_optional(row.tool_policy_version.as_deref()),
                    subagent_default_await_mode: normalize_optional(
                        row.subagent_default_await_mode.as_deref(),
                    ),
                })
                .collect::<Vec<_>>();
            tool_selections.sort_by(|left, right| left.selection_id.cmp(&right.selection_id));

            let mut tool_service_registries = store
                .tool_service_registries
                .iter()
                .enumerate()
                .filter(|(index, _row)| {
                    source_matches_agent(
                        &store.tool_service_registry_source_agent_dids,
                        *index,
                        &peer.agent_did,
                        false,
                    )
                })
                .map(|(_index, row)| ToolServiceRegistryView {
                    service_id: row.service_id.clone(),
                    display_name: normalize_optional(row.display_name.as_deref()),
                    description: normalize_optional(row.description.as_deref()),
                    hostname: normalize_optional(row.hostname.as_deref()),
                    tailscale_ip: normalize_optional(row.tailscale_ip.as_deref()),
                    lan_ip: normalize_optional(row.lan_ip.as_deref()),
                    mcp_port: row.mcp_port,
                    mcp_path: normalize_optional(row.mcp_path.as_deref()),
                    status: normalize_optional(row.status.as_deref()),
                    version: normalize_optional(row.version.as_deref()),
                    updated_at: normalize_optional(row.updated_at.as_deref()),
                })
                .collect::<Vec<_>>();
            tool_service_registries.sort_by(|left, right| left.service_id.cmp(&right.service_id));

            let mut skills = store
                .skills
                .iter()
                .enumerate()
                .filter(|(index, row)| {
                    source_matches_agent(
                        &store.skill_source_agent_dids,
                        *index,
                        &peer.agent_did,
                        false,
                    ) && row.agent_did.as_deref() == Some(peer.agent_did.as_str())
                })
                .map(|(_index, row)| SkillView {
                    skill_id: row.skill_id.clone(),
                    agent_did: normalize_optional(row.agent_did.as_deref()),
                    scope: normalize_optional(row.scope.as_deref()),
                    name: normalize_optional(row.name.as_deref()),
                    description: normalize_optional(row.description.as_deref()),
                    instructions: normalize_optional(row.instructions.as_deref()),
                    tool_refs: row.tool_refs.clone(),
                    display_name: normalize_optional(row.display_name.as_deref()),
                    enabled: row.enabled,
                    created_at: normalize_optional(row.created_at.as_deref()),
                })
                .collect::<Vec<_>>();
            skills.sort_by(|left, right| left.skill_id.cmp(&right.skill_id));

            let scoped_task_rows = store
                .tasks
                .iter()
                .enumerate()
                .filter(|(index, row)| {
                    source_matches_agent(
                        &store.task_source_agent_dids,
                        *index,
                        &peer.agent_did,
                        false,
                    ) && row
                        .behavior_id
                        .as_deref()
                        .is_some_and(|behavior_id| behavior_ids.contains(&behavior_id))
                })
                .collect::<Vec<_>>();
            let task_ids = scoped_task_rows
                .iter()
                .map(|(_index, task)| task.task_id.as_str())
                .collect::<Vec<_>>();

            let mut schedules = store
                .schedules
                .iter()
                .enumerate()
                .filter(|(index, row)| {
                    source_matches_agent(
                        &store.schedule_source_agent_dids,
                        *index,
                        &peer.agent_did,
                        false,
                    ) && row
                        .task_id
                        .as_deref()
                        .is_some_and(|task_id| task_ids.contains(&task_id))
                })
                .map(|(_index, row)| ScheduleView {
                    schedule_id: row.schedule_id.clone(),
                    task_id: normalize_optional(row.task_id.as_deref()),
                    interval_secs: row.interval_secs,
                    cron: normalize_optional(row.cron.as_deref()),
                    timezone: normalize_optional(row.timezone.as_deref()),
                    missed_run_policy: normalize_optional(row.missed_run_policy.as_deref()),
                    enabled: row.enabled,
                    concurrency: normalize_optional(row.concurrency.as_deref()),
                    next_run_at: normalize_optional(row.next_run_at.as_deref()),
                    last_attempt_at: normalize_optional(row.last_attempt_at.as_deref()),
                    last_status: normalize_optional(row.last_status.as_deref()),
                    last_error: normalize_optional(row.last_error.as_deref()),
                    fire_count: row.fire_count,
                })
                .collect::<Vec<_>>();
            schedules.sort_by(|left, right| left.schedule_id.cmp(&right.schedule_id));

            let mut event_triggers = store
                .event_triggers
                .iter()
                .enumerate()
                .filter(|(index, row)| {
                    source_matches_agent(
                        &store.event_trigger_source_agent_dids,
                        *index,
                        &peer.agent_did,
                        false,
                    ) && row
                        .task_id
                        .as_deref()
                        .is_some_and(|task_id| task_ids.contains(&task_id))
                })
                .map(|(_index, row)| EventTriggerView {
                    trigger_id: row.trigger_id.clone(),
                    task_id: normalize_optional(row.task_id.as_deref()),
                    source_collection: normalize_optional(row.source_collection.as_deref()),
                    event_kind: normalize_optional(row.event_kind.as_deref()),
                    filter: normalize_optional(row.filter.as_deref()),
                    enabled: row.enabled,
                    concurrency: normalize_optional(row.concurrency.as_deref()),
                    last_attempt_at: normalize_optional(row.last_attempt_at.as_deref()),
                    last_fired_source_doc_id: normalize_optional(
                        row.last_fired_source_doc_id.as_deref(),
                    ),
                    last_status: normalize_optional(row.last_status.as_deref()),
                    last_error: normalize_optional(row.last_error.as_deref()),
                    fire_count: row.fire_count,
                })
                .collect::<Vec<_>>();
            event_triggers.sort_by(|left, right| left.trigger_id.cmp(&right.trigger_id));

            let mut tasks = scoped_task_rows
                .into_iter()
                .map(|(_index, row)| TaskView {
                    task_id: row.task_id.clone(),
                    name: normalize_optional(row.name.as_deref()),
                    description: normalize_optional(row.description.as_deref()),
                    behavior_id: normalize_optional(row.behavior_id.as_deref()),
                    prompt_template: normalize_optional(row.prompt_template.as_deref()),
                    enabled: row.enabled,
                    output_schema_ref: normalize_optional(row.output_schema_ref.as_deref()),
                    recent_runs: recent_runs_for_task_views(
                        &schedules,
                        &event_triggers,
                        &row.task_id,
                    ),
                    run_history: task_run_history(
                        store.as_ref(),
                        &peer.agent_did,
                        require_source_scope,
                        &row.task_id,
                        &schedules,
                        &event_triggers,
                    ),
                })
                .collect::<Vec<_>>();
            tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));

            let mut conversations = store
                .conversation_rows(&peer.agent_did)
                .into_iter()
                .map(|row| {
                    let transcript = store.transcript_for_agent(&row.session_id, &peer.agent_did);
                    let task_tag = conversation_task_tag(
                        store.as_ref(),
                        &peer.agent_did,
                        require_source_scope,
                        &row.session_id,
                        &tasks,
                        &schedules,
                        &event_triggers,
                    );
                    ConversationSummary {
                        session_id: row.session_id.clone(),
                        title: normalize_optional(row.title.as_deref()),
                        preview_text: normalize_optional(row.preview_text.as_deref()),
                        status: normalize_optional(row.status.as_deref()),
                        behavior_id: normalize_optional(row.behavior_id.as_deref()),
                        latest_request_id: store.latest_request_id_for_session_for_agent(
                            &row.session_id,
                            &peer.agent_did,
                        ),
                        task_id: task_tag.as_ref().map(|tag| tag.task_id.clone()),
                        task_name: task_tag.as_ref().and_then(|tag| tag.task_name.clone()),
                        trigger_id: task_tag.as_ref().and_then(|tag| tag.trigger_id.clone()),
                        trigger_kind: task_tag.as_ref().and_then(|tag| tag.trigger_kind.clone()),
                        created_at: normalize_optional(row.created_at.as_deref()),
                        updated_at: normalize_optional(row.updated_at.as_deref()),
                        turn_state: store
                            .latest_request_id_for_session_for_agent(
                                &row.session_id,
                                &peer.agent_did,
                            )
                            .as_deref()
                            .and_then(|request_id| store.derive_turn_for_request(request_id))
                            .map(turn_state_label)
                            .map(str::to_owned),
                        message_count: transcript.messages.len(),
                        tool_call_count: transcript.tool_calls.len(),
                    }
                })
                .collect::<Vec<_>>();
            conversations.extend(request_backed_conversation_summaries(
                store.as_ref(),
                &peer.agent_did,
                require_source_scope,
                &tasks,
                &schedules,
                &event_triggers,
            ));
            conversations.sort_by(|left, right| {
                right
                    .updated_at
                    .cmp(&left.updated_at)
                    .then_with(|| right.created_at.cmp(&left.created_at))
            });
            retain_latest_conversation_summaries(&mut conversations);

            let mut behavior_environments = resolve_behavior_environments(
                &behaviors,
                &inference_backends,
                &inference_profiles,
                &tool_selections,
                &skills,
                &conversations,
            );

            let pairing_ready = peer.is_chat_ready();
            if !pairing_ready {
                default_behavior_id = None;
                agent_principal.default_behavior_id = None;
                runtime = None;
                behaviors.clear();
                behavior_environments.clear();
                inference_backends.clear();
                inference_profiles.clear();
                tool_selections.clear();
                tool_service_registries.clear();
                skills.clear();
                tasks.clear();
                schedules.clear();
                event_triggers.clear();
                conversations.clear();
            }

            DeploymentView {
                peer_id: peer.peer_id,
                label: peer.label,
                agent_did: peer.agent_did,
                addr: peer.addr,
                source: peer.source,
                graphql: peer.graphql,
                dial_succeeded: status.is_some_and(|status| status.dial_succeeded),
                pairing_ready,
                last_error: status.and_then(|status| status.last_error.clone()),
                default_behavior_id,
                agent_principal,
                runtime,
                behaviors,
                behavior_environments,
                inference_backends,
                inference_profiles,
                tool_selections,
                tool_service_registries,
                skills,
                tasks,
                schedules,
                event_triggers,
                conversations,
            }
        })
        .collect::<Vec<_>>();

    deployments.sort_by(|left, right| left.label.cmp(&right.label));

    DesktopRuntimeSnapshot {
        local_peer_id: core.local_peer_id().to_string(),
        listen_addresses: core.listen_addresses().to_vec(),
        p2p_health: to_health_view(&core.p2p_health()),
        bootstrap_errors: core.bootstrap_errors().to_vec(),
        last_mutation_error: core.last_mutation_error(),
        focused_request_id: core.store().focused_request_id(),
        configured_peer_count: core.configured_peer_count(),
        dialed_peer_count: core.dialed_peer_count(),
        peer_issue_count: core.peer_issue_count(),
        row_count: store.row_count(),
        approx_serialized_bytes: store.approx_serialized_bytes(),
        deployments,
    }
}

fn resolve_behavior_environments(
    behaviors: &[BehaviorView],
    backends: &[InferenceBackendView],
    profiles: &[InferenceProfileView],
    tool_selections: &[ToolSelectionView],
    skills: &[SkillView],
    conversations: &[ConversationSummary],
) -> Vec<BehaviorEnvironmentView> {
    behaviors
        .iter()
        .map(|behavior| {
            let tool_selection = behavior
                .tool_selection_id
                .as_deref()
                .and_then(|selection_id| {
                    tool_selections
                        .iter()
                        .find(|selection| selection.selection_id == selection_id)
                });
            let backend = behavior.backend_id.as_deref().and_then(|backend_id| {
                backends
                    .iter()
                    .find(|backend| backend.backend_id == backend_id)
            });
            let profile = behavior
                .inference_profile_id
                .as_deref()
                .and_then(|profile_id| {
                    profiles
                        .iter()
                        .find(|profile| profile.profile_id == profile_id)
                });
            let matches_behavior = |conversation: &&ConversationSummary| {
                conversation.behavior_id.as_deref() == Some(behavior.behavior_id.as_str())
            };
            let matching_conversations = conversations
                .iter()
                .filter(matches_behavior)
                .collect::<Vec<_>>();
            let skill_names = behavior
                .skill_refs
                .iter()
                .filter(|skill_id| !behavior.skill_excludes.contains(skill_id))
                .map(|skill_id| {
                    skills
                        .iter()
                        .find(|skill| skill.skill_id == *skill_id)
                        .and_then(|skill| skill.display_name.clone().or_else(|| skill.name.clone()))
                        .unwrap_or_else(|| skill_id.clone())
                })
                .collect();

            BehaviorEnvironmentView {
                behavior_id: behavior.behavior_id.clone(),
                display_name: behavior.display_name.clone(),
                enabled: behavior.enabled,
                is_default: behavior.is_default,
                model_name: behavior
                    .model_name
                    .clone()
                    .or_else(|| backend.and_then(|backend| backend.models.first().cloned())),
                inference_profile_name: profile
                    .and_then(|profile| profile.display_name.clone())
                    .or_else(|| behavior.inference_profile_id.clone()),
                workspace_root: tool_selection
                    .and_then(|selection| selection.file_tool_root.clone()),
                file_access: file_access_label(tool_selection).to_string(),
                bash_access: bash_access_label(tool_selection).to_string(),
                network_access: tool_selection
                    .and_then(|selection| selection.command_network_mode.clone()),
                skill_names,
                session_count: matching_conversations.len(),
                active_session_count: matching_conversations
                    .iter()
                    .filter(|conversation| conversation_is_active(conversation))
                    .count(),
            }
        })
        .collect()
}

fn file_access_label(selection: Option<&ToolSelectionView>) -> &'static str {
    let Some(selection) = selection else {
        return "off";
    };
    if selection.enable_file_tools != Some(true) {
        return "off";
    }
    match selection.file_tools_mode.as_deref() {
        None => "read-only",
        Some(value) => match FileToolMode::parse(value) {
            Ok(FileToolMode::Off) => "off",
            Ok(FileToolMode::ReadOnly) => "read-only",
            Ok(FileToolMode::ReadWrite) => "read / write",
            Err(_) => "unknown",
        },
    }
}

fn bash_access_label(selection: Option<&ToolSelectionView>) -> &'static str {
    let Some(selection) = selection else {
        return "off";
    };
    if selection.enable_bash != Some(true) {
        return "off";
    }
    match selection.bash_mode.as_deref() {
        None => "read-only",
        Some(value) => match BashMode::parse(value) {
            Ok(BashMode::Off) => "off",
            Ok(BashMode::ReadOnly) => "read-only",
            Ok(BashMode::Unrestricted) => "unrestricted",
            Err(_) => "unknown",
        },
    }
}

fn conversation_is_active(conversation: &ConversationSummary) -> bool {
    let state = conversation
        .turn_state
        .as_deref()
        .or(conversation.status.as_deref());
    let Some(state) = state else {
        return false;
    };
    !matches!(
        state.to_ascii_lowercase().as_str(),
        "completed"
            | "failed"
            | "error"
            | "dead"
            | "superseded"
            | "interrupted"
            | "cancelled"
            | "idle"
    )
}

fn peer_can_infer_behaviors(peer: &PeerRecord) -> bool {
    (peer.is_bearer_pairing() && peer.pairing_ready)
        || (peer.source.as_deref() == Some("server-status") && peer.default_behavior_id.is_some())
}

fn inferred_peer_behavior_ids<'a>(behavior_ids: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut behavior_ids = behavior_ids
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    behavior_ids.sort();
    behavior_ids.dedup();
    behavior_ids
}

fn inferred_default_behavior_id(
    agent_did: &str,
    configured_default: Option<&str>,
    behavior_ids: &[String],
) -> Option<String> {
    configured_default
        .filter(|candidate| behavior_ids.iter().any(|value| value == candidate))
        .or_else(|| {
            behavior_ids
                .iter()
                .find(|value| value.as_str() == "default")
                .map(String::as_str)
        })
        .or_else(|| {
            let scoped_default = gents::default_behavior_id_for_agent(agent_did);
            behavior_ids
                .iter()
                .find(|value| value.as_str() == scoped_default)
                .map(String::as_str)
        })
        .or_else(|| behavior_ids.first().map(String::as_str))
        .map(str::to_owned)
}

fn inferred_behavior_display_name(behavior_id: &str, peer_label: &str, is_default: bool) -> String {
    if is_default {
        return peer_label.to_string();
    }
    behavior_id
        .split(['-', '_'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod inferred_peer_behavior_tests {
    use super::*;

    #[test]
    fn deduplicates_behavior_ids_and_prefers_named_default() {
        let ids =
            inferred_peer_behavior_ids(["session-classifier", "default", "default"].into_iter());

        assert_eq!(ids, vec!["default", "session-classifier"]);
        assert_eq!(
            inferred_default_behavior_id("did:key:amy", None, &ids).as_deref(),
            Some("default")
        );
    }

    #[test]
    fn labels_default_as_peer_and_humanizes_other_behaviors() {
        assert_eq!(
            inferred_behavior_display_name("default", "Amy", true),
            "Amy"
        );
        assert_eq!(
            inferred_behavior_display_name("session-classifier", "Amy", false),
            "Session Classifier"
        );
    }

    #[test]
    fn signed_default_behavior_bootstraps_a_fresh_bearer_peer() {
        let ids = inferred_peer_behavior_ids(std::iter::empty::<&str>().chain(Some("default")));

        assert_eq!(ids, vec!["default"]);
        assert_eq!(
            inferred_default_behavior_id("did:key:amy", Some("default"), &ids).as_deref(),
            Some("default")
        );
    }

    #[test]
    fn pending_bearer_peer_cannot_infer_chat_behavior() {
        let mut peer = PeerRecord::new("Remote", "endpoint", "did:key:remote");
        peer.source = Some("bearer-pairing".to_string());
        peer.default_behavior_id = Some("default".to_string());

        assert!(!peer_can_infer_behaviors(&peer));
        peer.pairing_ready = true;
        assert!(peer_can_infer_behaviors(&peer));
    }

    #[test]
    fn status_peer_with_imported_default_can_render_a_behavior_before_snapshot_hydration() {
        let mut peer = PeerRecord::new("Amy", "endpoint-amy", "did:key:amy");
        peer.source = Some("server-status".to_string());
        peer.default_behavior_id = Some("default".to_string());

        assert!(peer_can_infer_behaviors(&peer));
        assert_eq!(
            inferred_peer_behavior_ids(
                std::iter::empty::<&str>().chain(peer.default_behavior_id.as_deref())
            ),
            vec!["default"]
        );
    }

    #[test]
    fn p2p_config_without_legacy_source_tags_remains_visible() {
        assert!(source_matches_agent(&[None], 0, "did:key:amy", false));
        assert!(!source_matches_agent(
            &[Some("did:key:other".to_string())],
            0,
            "did:key:amy",
            false,
        ));
    }
}

#[cfg(test)]
mod behavior_environment_tests {
    use super::*;

    fn behavior() -> BehaviorView {
        BehaviorView {
            behavior_id: "default".to_string(),
            display_name: "Amy".to_string(),
            system_prompt: None,
            backend_id: Some("backend".to_string()),
            model_name: None,
            tool_selection_id: Some("tools".to_string()),
            inference_profile_id: Some("profile".to_string()),
            compaction_strategy: None,
            compaction_threshold: None,
            enabled: true,
            is_default: true,
            skill_refs: vec!["diagnostics".to_string(), "missing".to_string()],
            skill_excludes: vec!["missing".to_string()],
        }
    }

    fn backend() -> InferenceBackendView {
        InferenceBackendView {
            backend_id: "backend".to_string(),
            name: Some("Local inference".to_string()),
            provider_kind: None,
            openai_wire_api: None,
            endpoint: None,
            api_key_configured: false,
            api_key_env_var: None,
            max_concurrent: None,
            max_queue_depth: None,
            enabled: Some(true),
            models: vec!["gpt-test".to_string()],
            probe_status: None,
        }
    }

    fn profile() -> InferenceProfileView {
        InferenceProfileView {
            profile_id: "profile".to_string(),
            display_name: Some("Long context".to_string()),
            context_window: None,
            max_output_tokens: None,
            max_turns: None,
            temperature: None,
            reasoning_effort: None,
            stream_batch_ms: None,
            stream_liveness_timeout_secs: None,
            deadline_duration_secs: None,
        }
    }

    fn tool_selection() -> ToolSelectionView {
        ToolSelectionView {
            selection_id: "tools".to_string(),
            agent_did: None,
            display_name: Some("Workspace tools".to_string()),
            enable_file_tools: Some(true),
            file_tools_mode: Some("ReadWrite".to_string()),
            file_tool_root: Some("/work/amygdala".to_string()),
            enable_bash: Some(true),
            bash_mode: Some("ReadOnly".to_string()),
            command_execution_policy: None,
            command_allowed_argv_prefixes: vec![],
            command_forbidden_argv_prefixes: vec![],
            command_network_mode: Some("Disabled".to_string()),
            cli_tool_names: vec![],
            enable_meta_tools: None,
            allowed_mcp_service_ids: vec![],
            delegate_to: vec![],
            backgroundable_tool_names: vec![],
            subagent_targets: vec![],
            subagent_spawn_enabled: None,
            subagent_steering_enabled: None,
            subagent_background_enabled: None,
            subagent_allow_cross_deployment: None,
            cross_deployment_spawn_timeout_seconds: None,
            enable_memory: None,
            enable_session_history_tool: None,
            enable_context_budget: None,
            enable_defra_query: None,
            defra_query_collections: vec![],
            write_tools: vec![],
            tool_policy_version: None,
            subagent_default_await_mode: None,
        }
    }

    fn skill() -> SkillView {
        SkillView {
            skill_id: "diagnostics".to_string(),
            agent_did: None,
            scope: None,
            name: Some("diagnostics".to_string()),
            description: None,
            instructions: None,
            tool_refs: vec![],
            display_name: Some("Host diagnostics".to_string()),
            enabled: Some(true),
            created_at: None,
        }
    }

    fn conversation(
        session_id: &str,
        behavior_id: Option<&str>,
        turn_state: Option<&str>,
    ) -> ConversationSummary {
        ConversationSummary {
            session_id: session_id.to_string(),
            title: None,
            preview_text: None,
            status: None,
            behavior_id: behavior_id.map(str::to_string),
            latest_request_id: None,
            task_id: None,
            task_name: None,
            trigger_id: None,
            trigger_kind: None,
            created_at: None,
            updated_at: None,
            turn_state: turn_state.map(str::to_string),
            message_count: 0,
            tool_call_count: 0,
        }
    }

    #[test]
    fn resolves_runnable_environment_once_for_clients() {
        let mut status_only_active = conversation("status-active", Some("default"), None);
        status_only_active.status = Some("active".to_string());
        let unassigned = conversation("unassigned", None, Some("processing"));
        let environments = resolve_behavior_environments(
            &[behavior()],
            &[backend()],
            &[profile()],
            &[tool_selection()],
            &[skill()],
            &[
                conversation("active", Some("default"), Some("processing")),
                conversation("complete", Some("default"), Some("completed")),
                status_only_active,
                unassigned,
            ],
        );

        let environment = &environments[0];
        assert_eq!(environment.display_name, "Amy");
        assert_eq!(environment.model_name.as_deref(), Some("gpt-test"));
        assert_eq!(
            environment.inference_profile_name.as_deref(),
            Some("Long context")
        );
        assert_eq!(
            environment.workspace_root.as_deref(),
            Some("/work/amygdala")
        );
        assert_eq!(environment.file_access, "read / write");
        assert_eq!(environment.bash_access, "read-only");
        assert_eq!(environment.skill_names, vec!["Host diagnostics"]);
        assert_eq!(environment.session_count, 3);
        assert_eq!(environment.active_session_count, 2);
    }

    #[test]
    fn invalid_tool_modes_are_visible_instead_of_misreported() {
        let mut selection = tool_selection();
        selection.file_tools_mode = Some("FutureFileMode".to_string());
        selection.bash_mode = Some("FutureBashMode".to_string());

        assert_eq!(file_access_label(Some(&selection)), "unknown");
        assert_eq!(bash_access_label(Some(&selection)), "unknown");
    }
}
