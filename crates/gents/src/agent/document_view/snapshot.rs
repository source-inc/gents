use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use defra_node::EmbeddedNode;
use gents_protocol::row::BehaviorReadinessUnavailableReason;

use crate::admission::{backend_admission_configs_from_backends, BackendAvailability};
use crate::config::AgentBehavior;
use crate::document_config::{
    default_behavior_id_for_agent, AgentBehavior as AgentBehaviorDocument,
};
use crate::runtime_snapshot::{
    ConcurrencyMode, EventTriggerFireMode, ResolvedEventTrigger, ResolvedRuntimeSnapshot,
    ResolvedSchedule, ResolvedTask, ScheduleCadence, UnavailableBehavior,
    MAX_EVENT_TRIGGER_GROUP_DOCS,
};
use crate::schedule_cron::{validate_cron_schedule, CronMissedRunPolicy};
use crate::tool_surface::ToolSelection;

use super::{validate_subagent_targets_resolve, DocumentRuntimeView};

use crate::agent::{
    assemble_principal_and_behaviors, behavior_config_from_documents,
    subagent_tool_config_from_document, tool_selection_from_document, BehaviorBuildError,
    DocumentResolveContext,
};
use crate::identity::AgentPrincipal;
use crate::tool_surface::SubagentToolConfig;

struct BehaviorResolutionError {
    code: BehaviorReadinessUnavailableReason,
    detail: anyhow::Error,
}

impl BehaviorResolutionError {
    fn new(code: BehaviorReadinessUnavailableReason, detail: anyhow::Error) -> Self {
        Self { code, detail }
    }
}

pub(crate) async fn resolve_document_runtime_snapshot_from_view(
    node: &EmbeddedNode,
    context: &DocumentResolveContext,
    view: &DocumentRuntimeView,
) -> Result<ResolvedRuntimeSnapshot> {
    if !view.principal.value.enabled {
        anyhow::bail!(
            "agent principal {} is disabled",
            view.principal.value.agent_did
        );
    }

    let default_behavior_id = view
        .principal
        .value
        .default_behavior_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| default_behavior_id_for_agent(context.identity.did()));

    let principal_data = AgentPrincipal {
        agent_did: view.principal.value.agent_did.clone(),
        identity: context.identity.clone(),
        default_behavior_id: default_behavior_id.clone(),
        display_name: view.principal.value.display_name.clone(),
        enabled: view.principal.value.enabled,
    };

    let measured_vetoed = context.backend_health.vetoed_backend_ids().await;
    let backend_admission_configs = backend_admission_configs_from_backends(
        view.backends.values().map(|record| &record.value),
        &measured_vetoed,
    )?;

    let mut unavailable_behaviors = HashMap::new();
    let mut behavior_factories: Vec<
        Box<
            dyn FnOnce(
                    Arc<AgentPrincipal>,
                ) -> std::result::Result<AgentBehavior, BehaviorBuildError>
                + Send,
        >,
    > = Vec::new();

    let all_skills = sorted_skills(view);

    for behavior_record in view.behaviors.values() {
        let behavior = &behavior_record.value;
        if !behavior.enabled {
            unavailable_behaviors.insert(
                behavior.behavior_id.clone(),
                UnavailableBehavior::new(
                    BehaviorReadinessUnavailableReason::BehaviorDisabled,
                    format!("behavior {} is disabled", behavior.behavior_id),
                ),
            );
            continue;
        }

        let resolved_result: std::result::Result<_, BehaviorResolutionError> = (|| {
            let backend_id = behavior
                .backend_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    BehaviorResolutionError::new(
                        BehaviorReadinessUnavailableReason::BackendNotConfigured,
                        anyhow!("behavior {} has no backend binding", behavior.behavior_id),
                    )
                })?;
            let backend = view
                .backends
                .get(backend_id)
                .map(|record| record.value.clone())
                .ok_or_else(|| {
                    BehaviorResolutionError::new(
                        BehaviorReadinessUnavailableReason::BackendNotConfigured,
                        anyhow!(
                            "behavior {} references missing backend {}",
                            behavior.behavior_id,
                            backend_id
                        ),
                    )
                })?;
            // `BackendAdmissionConfig::availability` is the single owner of
            // the enabled/probe_status/measured_unhealthy comparison; built
            // once above for every backend and reused here and for the
            // snapshot's admission configs below.
            let admission_config = backend_admission_configs
                .get(&backend.backend_id)
                .cloned()
                .ok_or_else(|| {
                    BehaviorResolutionError::new(
                        BehaviorReadinessUnavailableReason::BackendNotConfigured,
                        anyhow!(
                            "behavior {} references missing backend {}",
                            behavior.behavior_id,
                            backend.backend_id
                        ),
                    )
                })?;
            match admission_config.availability() {
                BackendAvailability::Available => {}
                BackendAvailability::Disabled => {
                    return Err(BehaviorResolutionError::new(
                        BehaviorReadinessUnavailableReason::BackendDisabled,
                        anyhow!(
                            "behavior {} backend {} is unavailable (enabled={} probe_status={})",
                            behavior.behavior_id,
                            backend.backend_id,
                            backend.enabled,
                            backend.probe_status
                        ),
                    ));
                }
                BackendAvailability::ProbeNotHealthy => {
                    return Err(BehaviorResolutionError::new(
                        BehaviorReadinessUnavailableReason::BackendTemporarilyUnavailable,
                        anyhow!(
                            "behavior {} backend {} is not ready (probe_status={})",
                            behavior.behavior_id,
                            backend.backend_id,
                            backend.probe_status
                        ),
                    ));
                }
                BackendAvailability::MeasuredUnhealthy => {
                    return Err(BehaviorResolutionError::new(
                        BehaviorReadinessUnavailableReason::BackendTemporarilyUnavailable,
                        anyhow!(
                            "behavior {} backend {} is measured unhealthy by the local prober \
                             (document probe_status={} is operator intent; routing resumes on \
                             the next successful probe)",
                            behavior.behavior_id,
                            backend.backend_id,
                            backend.probe_status
                        ),
                    ));
                }
            }
            if backend.provider_kind == crate::backend_provider::BackendProviderKind::ChatGptCodex
                && !view.has_enabled_oauth_credential(crate::chatgpt_codex::CHATGPT_CODEX_PROVIDER)
            {
                let agent_did = view.principal.value.agent_did.as_str();
                return Err(BehaviorResolutionError::new(
                    BehaviorReadinessUnavailableReason::CredentialsRequired,
                    anyhow!(
                        "behavior {} ChatGptCodex backend {} has no enabled OAuthCredential for agent \
                         {agent_did}; run `gents codex-login --agent-did {agent_did}`",
                        behavior.behavior_id,
                        backend.backend_id,
                    ),
                ));
            }
            if backend.provider_kind == crate::backend_provider::BackendProviderKind::XaiGrokOAuth
                && !view.has_enabled_oauth_credential(crate::xai_grok_oauth::XAI_OAUTH_PROVIDER)
            {
                let agent_did = view.principal.value.agent_did.as_str();
                return Err(BehaviorResolutionError::new(
                    BehaviorReadinessUnavailableReason::CredentialsRequired,
                    anyhow!(
                        "behavior {} XaiGrokOAuth backend {} has no enabled OAuthCredential for agent \
                         {agent_did}; run `gents grok-login --agent-did {agent_did}`",
                        behavior.behavior_id,
                        backend.backend_id,
                    ),
                ));
            }
            if backend.provider_kind
                == crate::backend_provider::BackendProviderKind::ClaudeCliSubscription
                && !view.has_enabled_oauth_credential(crate::claude_oauth::CLAUDE_OAUTH_PROVIDER)
            {
                let agent_did = view.principal.value.agent_did.as_str();
                return Err(BehaviorResolutionError::new(
                    BehaviorReadinessUnavailableReason::CredentialsRequired,
                    anyhow!(
                        "behavior {} ClaudeCliSubscription backend {} has no enabled OAuthCredential for agent \
                         {agent_did}; run `gents claude-login --agent-did {agent_did}`",
                        behavior.behavior_id,
                        backend.backend_id,
                    ),
                ));
            }
            let profile_id = behavior
                .inference_profile_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    BehaviorResolutionError::new(
                        BehaviorReadinessUnavailableReason::InferenceProfileInvalid,
                        anyhow!(
                            "behavior {} has no inference profile binding",
                            behavior.behavior_id
                        ),
                    )
                })?;
            let inference_profile = view
                .inference_profiles
                .get(profile_id)
                .map(|record| record.value.clone())
                .ok_or_else(|| {
                    BehaviorResolutionError::new(
                        BehaviorReadinessUnavailableReason::InferenceProfileInvalid,
                        anyhow!(
                            "behavior {} references missing inference profile {}",
                            behavior.behavior_id,
                            profile_id
                        ),
                    )
                })?;
            let (tool_selection, subagent_tools) = match behavior
                .tool_selection_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                Some(selection_id) => match view.tool_selections.get(selection_id) {
                    Some(record) => {
                        let mut selection = record.value.clone();
                        let merged =
                            crate::agent::document_view::merge_surface_tools(&record.value, view)
                                .map_err(|error| {
                                BehaviorResolutionError::new(
                                    BehaviorReadinessUnavailableReason::ToolConfigurationInvalid,
                                    error,
                                )
                            })?;
                        selection.write_tools = Some(merged.write_tools);
                        selection.validate().map_err(|error| {
                            BehaviorResolutionError::new(
                                BehaviorReadinessUnavailableReason::ToolConfigurationInvalid,
                                error,
                            )
                        })?;
                        validate_subagent_targets_resolve(&selection, view).map_err(|error| {
                            BehaviorResolutionError::new(
                                BehaviorReadinessUnavailableReason::ToolConfigurationInvalid,
                                error,
                            )
                        })?;
                        let mut tool_selection =
                            tool_selection_from_document(&selection).map_err(|error| {
                                BehaviorResolutionError::new(
                                    BehaviorReadinessUnavailableReason::ToolConfigurationInvalid,
                                    error,
                                )
                            })?;
                        tool_selection.query_tools = merged.query_tools;
                        let expanded =
                            crate::agent::document_view::expand_eth_tools(&record.value, view)
                                .map_err(|error| {
                                    BehaviorResolutionError::new(
                                BehaviorReadinessUnavailableReason::ToolConfigurationInvalid,
                                error,
                            )
                                })?;
                        tool_selection.eth_queries = expanded.queries;
                        tool_selection.eth_calls = expanded.calls;
                        (
                            tool_selection,
                            subagent_tool_config_from_document(&selection),
                        )
                    }
                    None => {
                        return Err(BehaviorResolutionError::new(
                            BehaviorReadinessUnavailableReason::ToolConfigurationInvalid,
                            anyhow!(
                                "behavior {} references missing tool selection {}",
                                behavior.behavior_id,
                                selection_id
                            ),
                        ));
                    }
                },
                None => (ToolSelection::default(), SubagentToolConfig::default()),
            };
            Ok((backend, inference_profile, tool_selection, subagent_tools))
        })();

        match resolved_result {
            Ok((backend, inference_profile, tool_selection, subagent_tools)) => {
                let behavior_id = behavior.behavior_id.clone();
                let behavior_value = behavior.clone();
                let tool_ceiling = context.tool_ceiling.clone();
                let behavior_skills = crate::skills::effective_skills(
                    &all_skills,
                    &behavior.agent_did,
                    &behavior.skill_refs,
                    &behavior.skill_excludes,
                )
                .into_iter()
                .cloned()
                .collect::<Vec<crate::skills::Skill>>();
                let factory: Box<
                    dyn FnOnce(
                            Arc<AgentPrincipal>,
                        )
                            -> std::result::Result<AgentBehavior, BehaviorBuildError>
                        + Send,
                > = Box::new(move |principal| {
                    behavior_config_from_documents(
                        principal,
                        &behavior_value,
                        &backend,
                        &inference_profile,
                        tool_selection,
                        subagent_tools,
                        &tool_ceiling,
                        behavior_skills,
                    )
                    .map_err(|error| BehaviorBuildError {
                        behavior_id: behavior_id.clone(),
                        error,
                    })
                });
                behavior_factories.push(factory);
            }
            Err(error) => {
                unavailable_behaviors.insert(
                    behavior.behavior_id.clone(),
                    UnavailableBehavior::new(error.code, error.detail.to_string()),
                );
            }
        }
    }

    let (principal, behavior_results) =
        assemble_principal_and_behaviors(principal_data, behavior_factories);

    let mut behaviors = Vec::<Arc<AgentBehavior>>::new();
    for result in behavior_results {
        match result {
            Ok(behavior_arc) => behaviors.push(behavior_arc),
            Err(BehaviorBuildError { behavior_id, error }) => {
                unavailable_behaviors.insert(
                    behavior_id,
                    UnavailableBehavior::new(
                        BehaviorReadinessUnavailableReason::RuntimeConfigurationInvalid,
                        error.to_string(),
                    ),
                );
            }
        }
    }

    let own_agent_did = context.identity.did().to_string();
    let candidate_behavior_ids = behaviors
        .iter()
        .map(|behavior| behavior.behavior_id.clone())
        .collect::<HashSet<_>>();
    let mut behavior_surfaces = Vec::with_capacity(behaviors.len());
    for behavior in behaviors {
        match behavior
            .tools
            .resolve_with_available_subagent_targets(node, &own_agent_did, &candidate_behavior_ids)
            .await
        {
            Ok(tool_surface) => behavior_surfaces.push((behavior, tool_surface)),
            Err(error) => {
                unavailable_behaviors.insert(
                    behavior.behavior_id.clone(),
                    UnavailableBehavior::new(
                        BehaviorReadinessUnavailableReason::ToolSurfaceUnavailable,
                        error.to_string(),
                    ),
                );
            }
        }
    }

    let active_behavior_ids = behavior_surfaces
        .iter()
        .map(|(behavior, _)| behavior.behavior_id.clone())
        .collect::<HashSet<_>>();
    let mut behaviors = Vec::with_capacity(behavior_surfaces.len());
    let mut tool_surfaces = HashMap::with_capacity(behavior_surfaces.len());
    for (behavior, mut tool_surface) in behavior_surfaces {
        for target in tool_surface.subagent_targets() {
            if target.agent_did == own_agent_did
                && !active_behavior_ids.contains(&target.behavior_id)
            {
                tracing::warn!(
                    behavior_id = %behavior.behavior_id,
                    target_name = %target.name,
                    target_behavior_id = %target.behavior_id,
                    "dropping LOCAL subagent target: target behavior is not active \
                     (behavior may be disabled or its backend/MCP resolution failed)"
                );
            }
        }
        tool_surface.retain_subagent_targets(&own_agent_did, &active_behavior_ids);
        tool_surfaces.insert(behavior.behavior_id.clone(), Arc::new(tool_surface));
        behaviors.push(behavior);
    }

    let (active_schedules, unavailable_schedules) = resolve_schedules(view, &unavailable_behaviors);
    let (active_event_triggers, unavailable_event_triggers) =
        resolve_event_triggers(view, &unavailable_behaviors);
    let active_tasks = resolve_tasks(view, &unavailable_behaviors);
    Ok(ResolvedRuntimeSnapshot::from_parts_with_admission_configs(
        default_behavior_id,
        behaviors,
        tool_surfaces,
        backend_admission_configs,
        unavailable_behaviors,
    )
    .with_principal(principal)
    .with_local_did(context.identity.did().to_string())
    .with_schedules(active_schedules, unavailable_schedules)
    .with_event_triggers(active_event_triggers, unavailable_event_triggers)
    .with_tasks(active_tasks))
}

fn resolve_tasks(
    view: &DocumentRuntimeView,
    unavailable_behaviors: &HashMap<String, UnavailableBehavior>,
) -> HashMap<String, ResolvedTask> {
    let mut active_tasks = HashMap::new();

    for (task_id, task_record) in &view.tasks {
        let task = &task_record.value;

        if !task.enabled {
            continue;
        }

        let behavior_id = match task.behavior_id.as_deref().and_then(non_empty) {
            Some(id) => id,
            None => continue,
        };

        let behavior_record = match view.behaviors.get(behavior_id) {
            Some(record) => record,
            None => continue,
        };
        if !behavior_record.value.enabled {
            continue;
        }
        if unavailable_behaviors.contains_key(behavior_id) {
            continue;
        }

        let resolved_task = ResolvedTask {
            task_id: task.task_id.clone(),
            name: task.name.clone(),
            behavior_id: behavior_id.to_string(),
            prompt_template: task.prompt_template.clone().unwrap_or_default(),
            goal_objective_template: task.goal_objective_template.clone(),
            goal_token_budget: task.goal_token_budget,
            output_schema_ref: task.output_schema_ref.clone(),
        };
        active_tasks.insert(task_id.clone(), resolved_task);
    }

    active_tasks
}

fn resolve_schedules(
    view: &DocumentRuntimeView,
    unavailable_behaviors: &HashMap<String, UnavailableBehavior>,
) -> (HashMap<String, ResolvedSchedule>, HashSet<String>) {
    let mut active_schedules = HashMap::new();
    let mut unavailable_schedules = HashSet::new();

    for schedule_record in view.schedules.values() {
        let schedule = &schedule_record.value;
        let schedule_id = schedule.schedule_id.clone();

        let concurrency =
            match ConcurrencyMode::parse(schedule.concurrency.as_deref().unwrap_or("")) {
                Some(mode) => mode,
                None => {
                    unavailable_schedules.insert(schedule_id);
                    continue;
                }
            };

        if !schedule.enabled {
            unavailable_schedules.insert(schedule_id);
            continue;
        }

        let task_id = schedule.task_id.as_deref().unwrap_or("");
        let task_record = match view.tasks.get(task_id) {
            Some(record) => record,
            None => {
                unavailable_schedules.insert(schedule_id);
                continue;
            }
        };
        let task = &task_record.value;

        if !task.enabled {
            unavailable_schedules.insert(schedule_id);
            continue;
        }

        let behavior_id = task.behavior_id.as_deref().unwrap_or("");
        let behavior_record = match view.behaviors.get(behavior_id) {
            Some(record) => record,
            None => {
                unavailable_schedules.insert(schedule_id);
                continue;
            }
        };
        if !behavior_record.value.enabled {
            unavailable_schedules.insert(schedule_id);
            continue;
        }
        if unavailable_behaviors.contains_key(behavior_id) {
            unavailable_schedules.insert(schedule_id);
            continue;
        }
        if task_template_references_group(task.prompt_template.as_deref().unwrap_or_default()) {
            tracing::warn!(
                schedule_id = %schedule_id,
                "schedule quarantined: group.* template scope is only available to per_group event triggers",
            );
            unavailable_schedules.insert(schedule_id);
            continue;
        }

        let cadence = match resolve_schedule_cadence(schedule) {
            Ok(cadence) => cadence,
            Err(_) => {
                unavailable_schedules.insert(schedule_id);
                continue;
            }
        };

        let resolved_task = ResolvedTask {
            task_id: task.task_id.clone(),
            name: task.name.clone(),
            behavior_id: task.behavior_id.clone().unwrap_or_default(),
            prompt_template: task.prompt_template.clone().unwrap_or_default(),
            goal_objective_template: task.goal_objective_template.clone(),
            goal_token_budget: task.goal_token_budget,
            output_schema_ref: task.output_schema_ref.clone(),
        };
        let resolved_schedule = ResolvedSchedule {
            trigger_doc_id: schedule_record.doc_id.clone(),
            schedule_id: schedule.schedule_id.clone(),
            task_id: schedule.task_id.clone().unwrap_or_default(),
            task: resolved_task,
            cadence,
            enabled: schedule.enabled,
            concurrency,
        };
        active_schedules.insert(resolved_schedule.schedule_id.clone(), resolved_schedule);
    }

    (active_schedules, unavailable_schedules)
}

fn resolve_schedule_cadence(
    schedule: &crate::document_config::Schedule,
) -> Result<ScheduleCadence> {
    let cron = schedule
        .cron
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let interval = schedule.interval_secs;

    match (interval, cron) {
        (Some(interval_secs), None) if interval_secs >= 1 => {
            Ok(ScheduleCadence::Interval { interval_secs })
        }
        (Some(_), Some(_)) => Err(anyhow!(
            "schedule cannot define both interval_secs and cron"
        )),
        (Some(interval_secs), None) => Err(anyhow!(
            "schedule interval_secs must be >= 1; got {interval_secs}"
        )),
        (None, Some(expression)) => {
            let timezone = schedule
                .timezone
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("cron schedule requires timezone"))?;
            let missed_run_policy =
                CronMissedRunPolicy::parse(schedule.missed_run_policy.as_deref())?;
            validate_cron_schedule(expression, timezone, schedule.missed_run_policy.as_deref())?;
            Ok(ScheduleCadence::Cron {
                expression: expression.to_string(),
                timezone: timezone.to_string(),
                missed_run_policy,
            })
        }
        (None, None) => Err(anyhow!("schedule must define interval_secs or cron")),
    }
}

fn resolve_event_triggers(
    view: &DocumentRuntimeView,
    unavailable_behaviors: &HashMap<String, UnavailableBehavior>,
) -> (HashMap<String, ResolvedEventTrigger>, HashSet<String>) {
    let mut active_event_triggers = HashMap::new();
    let mut unavailable_event_triggers = HashSet::new();

    for trigger_record in view.event_triggers.values() {
        let trigger = &trigger_record.value;
        let trigger_id = trigger.trigger_id.clone();

        let concurrency = match ConcurrencyMode::parse(trigger.concurrency.as_deref().unwrap_or(""))
        {
            Some(mode) => mode,
            None => {
                unavailable_event_triggers.insert(trigger_id);
                continue;
            }
        };

        if trigger.enabled != Some(true) {
            unavailable_event_triggers.insert(trigger_id);
            continue;
        }

        let task_id = trigger.task_id.as_deref().unwrap_or("");
        let task_record = match view.tasks.get(task_id) {
            Some(record) => record,
            None => {
                unavailable_event_triggers.insert(trigger_id);
                continue;
            }
        };
        let task = &task_record.value;

        if !task.enabled {
            unavailable_event_triggers.insert(trigger_id);
            continue;
        }

        let behavior_id = task.behavior_id.as_deref().unwrap_or("");
        let behavior_record = match view.behaviors.get(behavior_id) {
            Some(record) => record,
            None => {
                unavailable_event_triggers.insert(trigger_id);
                continue;
            }
        };
        if !behavior_record.value.enabled {
            unavailable_event_triggers.insert(trigger_id);
            continue;
        }
        if unavailable_behaviors.contains_key(behavior_id) {
            unavailable_event_triggers.insert(trigger_id);
            continue;
        }

        let source_collection = trigger.source_collection.clone().unwrap_or_default();
        let event_kind = trigger.event_kind.clone().unwrap_or_default();
        if source_collection.is_empty() || event_kind.is_empty() {
            unavailable_event_triggers.insert(trigger_id);
            continue;
        }
        // Self-config rejects invalid names at write time, but trigger
        // documents can also arrive without passing that boundary (e.g.
        // replicated from a peer). `source_collection` lands in GraphQL
        // identifier positions where escaping cannot apply, so a
        // non-conforming name is quarantined here rather than activated.
        if let Err(error) = crate::graphql::validate_collection_identifier(&source_collection) {
            tracing::warn!(
                trigger_id = %trigger_id,
                source_collection = %source_collection,
                %error,
                "event trigger quarantined: source_collection is not a valid \
                 GraphQL collection identifier",
            );
            unavailable_event_triggers.insert(trigger_id);
            continue;
        }
        // Same reasoning for the filter fragment: a trigger can reach the
        // snapshot without passing self-config (replicated from a peer), and
        // the fragment is spliced whole into the probe query.
        if let Some(filter) = trigger
            .filter
            .as_deref()
            .map(str::trim)
            .filter(|filter| !filter.is_empty())
        {
            if let Err(error) = crate::graphql::validate_graphql_filter_fragment(filter) {
                tracing::warn!(
                    trigger_id = %trigger_id,
                    %error,
                    "event trigger quarantined: filter is not a well-formed \
                     GraphQL filter object",
                );
                unavailable_event_triggers.insert(trigger_id);
                continue;
            }
        }

        let fire_mode = match EventTriggerFireMode::parse(trigger.fire_mode.as_deref()) {
            Some(mode) => mode,
            None => {
                tracing::warn!(
                    trigger_id = %trigger_id,
                    fire_mode = ?trigger.fire_mode,
                    "event trigger quarantined: fire_mode must be per_document or per_group",
                );
                unavailable_event_triggers.insert(trigger_id);
                continue;
            }
        };
        if fire_mode != EventTriggerFireMode::PerGroup
            && task_template_references_group(task.prompt_template.as_deref().unwrap_or_default())
        {
            tracing::warn!(
                trigger_id = %trigger_id,
                "event trigger quarantined: group.* template scope requires per_group mode",
            );
            unavailable_event_triggers.insert(trigger_id);
            continue;
        }
        let correlation_field = trigger
            .correlation_field
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        let expected_count_field = trigger
            .expected_count_field
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        if correlation_field
            .as_deref()
            .into_iter()
            .chain(expected_count_field.as_deref())
            .any(|field| crate::graphql::validate_graphql_name(field).is_err())
        {
            tracing::warn!(
                trigger_id = %trigger_id,
                "event trigger quarantined: correlation/count field is not a GraphQL name",
            );
            unavailable_event_triggers.insert(trigger_id);
            continue;
        }
        let expected_count = trigger
            .expected_count
            .and_then(|value| usize::try_from(value).ok());
        let group_timeout_secs = trigger
            .group_timeout_secs
            .and_then(|value| u64::try_from(value).ok())
            .filter(|value| *value > 0);
        let group_min_count = trigger
            .group_min_count
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(1);
        let group_config_valid = match fire_mode {
            EventTriggerFireMode::PerDocument => {
                trigger.expected_count.is_none()
                    && expected_count_field.is_none()
                    && trigger.group_timeout_secs.is_none()
                    && trigger.group_min_count.is_none()
            }
            EventTriggerFireMode::PerGroup => {
                correlation_field.is_some()
                    && trigger
                        .expected_count
                        .is_none_or(|_| expected_count.is_some())
                    && expected_count
                        .is_none_or(|count| (1..=MAX_EVENT_TRIGGER_GROUP_DOCS).contains(&count))
                    && trigger.group_min_count.is_none_or(|_| {
                        trigger
                            .group_min_count
                            .and_then(|value| usize::try_from(value).ok())
                            .is_some()
                    })
                    && group_min_count > 0
                    && group_min_count <= MAX_EVENT_TRIGGER_GROUP_DOCS
                    && trigger
                        .group_min_count
                        .is_none_or(|_| group_timeout_secs.is_some())
                    && expected_count.is_none_or(|expected| group_min_count <= expected)
                    && match (expected_count, expected_count_field.as_ref()) {
                        (Some(_), None) | (None, Some(_)) => true,
                        (None, None) => group_timeout_secs.is_some(),
                        (Some(_), Some(_)) => false,
                    }
                    && trigger
                        .group_timeout_secs
                        .is_none_or(|_| group_timeout_secs.is_some())
            }
        };
        if !group_config_valid {
            tracing::warn!(
                trigger_id = %trigger_id,
                "event trigger quarantined: invalid per-group cardinality/timeout configuration",
            );
            unavailable_event_triggers.insert(trigger_id);
            continue;
        }

        let resolved_task = ResolvedTask {
            task_id: task.task_id.clone(),
            name: task.name.clone(),
            behavior_id: task.behavior_id.clone().unwrap_or_default(),
            prompt_template: task.prompt_template.clone().unwrap_or_default(),
            goal_objective_template: task.goal_objective_template.clone(),
            goal_token_budget: task.goal_token_budget,
            output_schema_ref: task.output_schema_ref.clone(),
        };
        let resolved_trigger = ResolvedEventTrigger {
            trigger_doc_id: trigger_record.doc_id.clone(),
            trigger_id: trigger.trigger_id.clone(),
            task_id: trigger.task_id.clone().unwrap_or_default(),
            task: resolved_task,
            source_collection,
            event_kind,
            filter: trigger.filter.clone(),
            enabled: true,
            concurrency,
            fire_mode,
            correlation_field,
            expected_count,
            expected_count_field,
            group_timeout_secs,
            group_min_count,
            workspace_authority: trigger
                .workspace_authority
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
        };
        active_event_triggers.insert(resolved_trigger.trigger_id.clone(), resolved_trigger);
    }

    (active_event_triggers, unavailable_event_triggers)
}

pub(super) fn collect_unresolved_behavior_references(
    view: &DocumentRuntimeView,
    behavior: &AgentBehaviorDocument,
    details: &mut Vec<String>,
) {
    if let Some(selection_id) = behavior.tool_selection_id.as_deref().and_then(non_empty) {
        if !view.tool_selections.contains_key(selection_id) {
            details.push(format!(
                "behavior {} references missing tool selection {}",
                behavior.behavior_id, selection_id
            ));
        }
    }

    if let Some(profile_id) = behavior.inference_profile_id.as_deref().and_then(non_empty) {
        if !view.inference_profiles.contains_key(profile_id) {
            details.push(format!(
                "behavior {} references missing inference profile {}",
                behavior.behavior_id, profile_id
            ));
        }
    }

    if let Some(backend_id) = behavior.backend_id.as_deref().and_then(non_empty) {
        if !view.backends.contains_key(backend_id) {
            details.push(format!(
                "behavior {} references missing backend {}",
                behavior.behavior_id, backend_id
            ));
        }
    }
}

pub(super) fn behavior_references_ready(
    view: &DocumentRuntimeView,
    behavior: &AgentBehaviorDocument,
) -> bool {
    let mut details = Vec::new();
    collect_unresolved_behavior_references(view, behavior, &mut details);
    details.is_empty()
}

pub(super) fn non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

fn task_template_references_group(template: &str) -> bool {
    crate::template::parse_template_for_validation(template).is_ok_and(|refs| {
        refs.iter()
            .any(|reference| reference.root() == Some("group"))
    })
}

// The runtime configuration fingerprint is compared across independently
// resolved views. Every collection map is keyed/sorted by the projector;
// skills are the one value vector embedded in AgentBehavior's Debug value, so
// canonicalize it before both prompt construction and fingerprinting.
pub(super) fn sorted_skills(view: &DocumentRuntimeView) -> Vec<crate::skills::Skill> {
    let mut skills = view
        .skills
        .values()
        .map(|record| skill_from_document(&record.value))
        .collect::<Vec<_>>();
    skills.sort_by(|left, right| left.skill_id.cmp(&right.skill_id));
    skills
}

fn skill_from_document(doc: &crate::document_config::SkillDocument) -> crate::skills::Skill {
    crate::skills::Skill {
        skill_id: doc.skill_id.clone(),
        agent_did: doc.agent_did.clone(),
        scope: doc
            .scope
            .as_deref()
            .and_then(crate::skills::SkillScope::parse)
            .unwrap_or(crate::skills::SkillScope::Behavior),
        name: doc.name.clone().unwrap_or_default(),
        description: doc.description.clone().unwrap_or_default(),
        instructions: doc.instructions.clone().unwrap_or_default(),
        tool_refs: doc.tool_refs.clone(),
        display_name: doc.display_name.clone(),
        enabled: doc.enabled,
    }
}
