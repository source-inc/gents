use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use defra_node::EmbeddedNode;
use serde::Deserialize;

use crate::admission::backend_admission_configs_from_backends;
use crate::config::AgentBehavior;
use crate::document_config::{
    default_behavior_id_for_agent, AgentBehavior as AgentBehaviorDocument,
};
use crate::runtime_snapshot::{
    ConcurrencyMode, ResolvedEventTrigger, ResolvedRuntimeSnapshot, ResolvedSchedule, ResolvedTask,
    ScheduleCadence,
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

    let mut unavailable_behaviors = HashMap::new();
    let mut behavior_config_provenance = HashMap::new();
    let mut behavior_factories: Vec<
        Box<
            dyn FnOnce(
                    Arc<AgentPrincipal>,
                ) -> std::result::Result<AgentBehavior, BehaviorBuildError>
                + Send,
        >,
    > = Vec::new();

    let all_skills: Vec<crate::skills::Skill> = view
        .skills
        .values()
        .map(|record| skill_from_document(&record.value))
        .collect();

    for behavior_record in view.behaviors.values() {
        let behavior = &behavior_record.value;
        if !behavior.enabled {
            unavailable_behaviors.insert(
                behavior.behavior_id.clone(),
                format!("behavior {} is disabled", behavior.behavior_id),
            );
            continue;
        }

        let resolved_result: Result<_, anyhow::Error> = (|| {
            let backend_id = behavior
                .backend_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    anyhow!("behavior {} has no backend binding", behavior.behavior_id)
                })?;
            let backend_record = view.backends.get(backend_id).ok_or_else(|| {
                anyhow!(
                    "behavior {} references missing backend {}",
                    behavior.behavior_id,
                    backend_id
                )
            })?;
            let backend = backend_record.value.clone();
            if !backend.is_available() {
                anyhow::bail!(
                    "behavior {} backend {} is unavailable (enabled={} probe_status={})",
                    behavior.behavior_id,
                    backend.backend_id,
                    backend.enabled,
                    backend.probe_status
                );
            }
            if measured_vetoed.contains(&backend.backend_id) {
                anyhow::bail!(
                    "behavior {} backend {} is measured unhealthy by the local prober \
                     (document probe_status={} is operator intent; routing resumes on \
                     the next successful probe)",
                    behavior.behavior_id,
                    backend.backend_id,
                    backend.probe_status
                );
            }
            if backend.provider_kind == crate::backend_provider::BackendProviderKind::ChatGptCodex
                && !view.has_enabled_oauth_credential(crate::chatgpt_codex::CHATGPT_CODEX_PROVIDER)
            {
                let agent_did = view.principal.value.agent_did.as_str();
                anyhow::bail!(
                    "behavior {} ChatGptCodex backend {} has no enabled OAuthCredential for agent \
                     {agent_did}; run `gents codex-login --agent-did {agent_did}`",
                    behavior.behavior_id,
                    backend.backend_id,
                );
            }
            if backend.provider_kind == crate::backend_provider::BackendProviderKind::XaiGrokOAuth
                && !view.has_enabled_oauth_credential(crate::xai_grok_oauth::XAI_OAUTH_PROVIDER)
            {
                let agent_did = view.principal.value.agent_did.as_str();
                anyhow::bail!(
                    "behavior {} XaiGrokOAuth backend {} has no enabled OAuthCredential for agent \
                     {agent_did}; run `gents grok-login --agent-did {agent_did}`",
                    behavior.behavior_id,
                    backend.backend_id,
                );
            }
            let profile_id = behavior
                .inference_profile_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "behavior {} has no inference profile binding",
                        behavior.behavior_id
                    )
                })?;
            let inference_profile_record =
                view.inference_profiles.get(profile_id).ok_or_else(|| {
                    anyhow!(
                        "behavior {} references missing inference profile {}",
                        behavior.behavior_id,
                        profile_id
                    )
                })?;
            let inference_profile = inference_profile_record.value.clone();
            let (tool_selection, subagent_tools, tool_selection_fact) = match behavior
                .tool_selection_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                Some(selection_id) => match view.tool_selections.get(selection_id) {
                    Some(record) => {
                        record.value.validate()?;
                        validate_subagent_targets_resolve(&record.value, view)?;
                        (
                            tool_selection_from_document(&record.value)?,
                            subagent_tool_config_from_document(&record.value),
                            Some(record.fact.clone()),
                        )
                    }
                    None => anyhow::bail!(
                        "behavior {} references missing tool selection {}",
                        behavior.behavior_id,
                        selection_id
                    ),
                },
                None => (
                    ToolSelection::default(),
                    SubagentToolConfig::default(),
                    None,
                ),
            };
            Ok((
                backend,
                inference_profile,
                tool_selection,
                subagent_tools,
                backend_record.fact.clone(),
                inference_profile_record.fact.clone(),
                tool_selection_fact,
            ))
        })();

        match resolved_result {
            Ok((
                backend,
                inference_profile,
                tool_selection,
                subagent_tools,
                backend_fact,
                inference_profile_fact,
                tool_selection_fact,
            )) => {
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
                let skill_facts = behavior_skills
                    .iter()
                    .map(|skill| {
                        view.skills
                            .get(&skill.skill_id)
                            .map(|record| record.fact.clone())
                            .ok_or_else(|| {
                                anyhow!(
                                    "effective skill {} has no exact source fact",
                                    skill.skill_id
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let config_provenance = crate::ResolvedBehaviorConfigProvenance {
                    principal: view.principal.fact.clone(),
                    behavior: behavior_record.fact.clone(),
                    inference_backend: backend_fact,
                    inference_profile: inference_profile_fact,
                    tool_selection: tool_selection_fact,
                    skills: skill_facts,
                    resolution_algorithm_version: 1,
                };
                config_provenance.validate_for_behavior(&behavior_id, &behavior.agent_did)?;
                behavior_config_provenance.insert(behavior_id.clone(), Arc::new(config_provenance));
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
                unavailable_behaviors.insert(behavior.behavior_id.clone(), error.to_string());
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
                unavailable_behaviors.insert(behavior_id, error.to_string());
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
                unavailable_behaviors.insert(behavior.behavior_id.clone(), error.to_string());
            }
        }
    }

    let active_behavior_ids = behavior_surfaces
        .iter()
        .map(|(behavior, _)| behavior.behavior_id.clone())
        .collect::<HashSet<_>>();
    behavior_config_provenance.retain(|behavior_id, _| active_behavior_ids.contains(behavior_id));
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

    let backend_admission_configs = backend_admission_configs_from_backends(
        view.backends.values().map(|record| &record.value),
        &measured_vetoed,
    )?;

    let (active_schedules, unavailable_schedules) = resolve_schedules(view, &unavailable_behaviors);
    let (active_event_triggers, unavailable_event_triggers) =
        resolve_event_triggers(view, &unavailable_behaviors);
    let active_tasks = resolve_tasks(view, &unavailable_behaviors);
    let paired_peer_dids = load_paired_peer_dids(node, context.identity.did()).await?;

    let snapshot = ResolvedRuntimeSnapshot::from_parts_with_admission_configs(
        default_behavior_id,
        behaviors,
        tool_surfaces,
        backend_admission_configs,
        unavailable_behaviors,
    )
    .with_principal(principal)
    .with_local_did(context.identity.did().to_string())
    .with_paired_peer_dids(paired_peer_dids)
    .with_schedules(active_schedules, unavailable_schedules)
    .with_event_triggers(active_event_triggers, unavailable_event_triggers)
    .with_tasks(active_tasks);
    snapshot.with_reconciled_document_runtime_config_provenance(behavior_config_provenance)
}

#[derive(Debug, Deserialize)]
struct PeerPairingDesiredDidRow {
    peer_id: String,
    agent_did: Option<String>,
}

async fn load_paired_peer_dids(node: &EmbeddedNode, local_did: &str) -> Result<HashSet<String>> {
    let query = r#"{
        PeerPairingDesired {
            peer_id
            agent_did
        }
    }"#;
    let response = node.execute(query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query PeerPairingDesired for paired peer DIDs failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<PeerPairingDesiredDidRow> = response
        .data
        .as_ref()
        .and_then(|d| d.get("PeerPairingDesired"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    let local_did = local_did.trim();
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            row.agent_did
                .as_deref()
                .map(str::trim)
                .filter(|did| !did.is_empty())
                .map(ToOwned::to_owned)
                .or_else(|| {
                    let peer_id = row.peer_id.trim();
                    peer_id.starts_with("did:").then(|| peer_id.to_string())
                })
        })
        .filter(|did| !did.trim().is_empty() && did.trim() != local_did)
        .collect())
}

fn resolve_tasks(
    view: &DocumentRuntimeView,
    unavailable_behaviors: &HashMap<String, String>,
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
            output_schema_ref: task.output_schema_ref.clone(),
        };
        active_tasks.insert(task_id.clone(), resolved_task);
    }

    active_tasks
}

fn resolve_schedules(
    view: &DocumentRuntimeView,
    unavailable_behaviors: &HashMap<String, String>,
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
            output_schema_ref: task.output_schema_ref.clone(),
        };
        let resolved_schedule = ResolvedSchedule {
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
    unavailable_behaviors: &HashMap<String, String>,
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

        let resolved_task = ResolvedTask {
            task_id: task.task_id.clone(),
            name: task.name.clone(),
            behavior_id: task.behavior_id.clone().unwrap_or_default(),
            prompt_template: task.prompt_template.clone().unwrap_or_default(),
            output_schema_ref: task.output_schema_ref.clone(),
        };
        let resolved_trigger = ResolvedEventTrigger {
            trigger_id: trigger.trigger_id.clone(),
            task_id: trigger.task_id.clone().unwrap_or_default(),
            task: resolved_task,
            source_collection,
            event_kind,
            filter: trigger.filter.clone(),
            enabled: true,
            concurrency,
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
