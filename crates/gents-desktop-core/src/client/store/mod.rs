mod indexing;
mod turns;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use gents_protocol::client_protocol::ClientTurnState;
use gents_protocol::row::{
    AgentBehaviorRow, AgentConversationRow, AgentMessageRow, AgentPrincipalRow, AgentRequestRow,
    AgentResponseRow, AgentRuntimeRow, AgentSessionRow, AgentToolCallRow, AgentToolResultRow,
    CompactionEntryRow, EventTriggerRow, GoalRow, InferenceBackendRow, InferenceProfileRow,
    ScheduleRow, SkillRow, TaskRow, ToolSelectionRow, ToolServiceRegistryRow,
};
use serde::Serialize;

use self::indexing::{clean_string, indexes_to_refs};

#[derive(Debug, Clone, Default, Serialize)]
pub struct ClientStoreRows {
    pub agent_principals: Vec<AgentPrincipalRow>,
    pub behaviors: Vec<AgentBehaviorRow>,
    pub runtimes: Vec<AgentRuntimeRow>,
    pub conversations: Vec<AgentConversationRow>,
    pub requests: Vec<AgentRequestRow>,
    pub responses: Vec<AgentResponseRow>,
    pub messages: Vec<AgentMessageRow>,
    pub sessions: Vec<AgentSessionRow>,
    pub goals: Vec<GoalRow>,
    pub tool_calls: Vec<AgentToolCallRow>,
    pub tool_results: Vec<AgentToolResultRow>,
    pub compaction_entries: Vec<CompactionEntryRow>,
    #[serde(skip)]
    pub message_source_agent_dids: Vec<Option<String>>,
    #[serde(skip)]
    pub session_source_agent_dids: Vec<Option<String>>,
    #[serde(skip)]
    pub tool_call_source_agent_dids: Vec<Option<String>>,
    #[serde(skip)]
    pub tool_result_source_agent_dids: Vec<Option<String>>,
    #[serde(skip)]
    pub compaction_entry_source_agent_dids: Vec<Option<String>>,
    pub tasks: Vec<TaskRow>,
    pub schedules: Vec<ScheduleRow>,
    pub event_triggers: Vec<EventTriggerRow>,
    #[serde(skip)]
    pub task_source_agent_dids: Vec<Option<String>>,
    #[serde(skip)]
    pub schedule_source_agent_dids: Vec<Option<String>>,
    #[serde(skip)]
    pub event_trigger_source_agent_dids: Vec<Option<String>>,
    pub skills: Vec<SkillRow>,
    #[serde(skip)]
    pub skill_source_agent_dids: Vec<Option<String>>,
    pub tool_selections: Vec<ToolSelectionRow>,
    pub inference_backends: Vec<InferenceBackendRow>,
    pub inference_profiles: Vec<InferenceProfileRow>,
    pub tool_service_registries: Vec<ToolServiceRegistryRow>,
    #[serde(skip)]
    pub inference_backend_source_agent_dids: Vec<Option<String>>,
    #[serde(skip)]
    pub inference_profile_source_agent_dids: Vec<Option<String>>,
    #[serde(skip)]
    pub tool_service_registry_source_agent_dids: Vec<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct ClientStore {
    pub agent_principals: Vec<AgentPrincipalRow>,
    pub behaviors: Vec<AgentBehaviorRow>,
    pub runtimes: Vec<AgentRuntimeRow>,
    pub conversations: Vec<AgentConversationRow>,
    pub requests: Vec<AgentRequestRow>,
    pub responses: Vec<AgentResponseRow>,
    pub messages: Vec<AgentMessageRow>,
    pub sessions: Vec<AgentSessionRow>,
    pub goals: Vec<GoalRow>,
    pub tool_calls: Vec<AgentToolCallRow>,
    pub tool_results: Vec<AgentToolResultRow>,
    pub compaction_entries: Vec<CompactionEntryRow>,
    pub message_source_agent_dids: Vec<Option<String>>,
    pub session_source_agent_dids: Vec<Option<String>>,
    pub tool_call_source_agent_dids: Vec<Option<String>>,
    pub tool_result_source_agent_dids: Vec<Option<String>>,
    pub compaction_entry_source_agent_dids: Vec<Option<String>>,
    pub tasks: Vec<TaskRow>,
    pub schedules: Vec<ScheduleRow>,
    pub event_triggers: Vec<EventTriggerRow>,
    pub task_source_agent_dids: Vec<Option<String>>,
    pub schedule_source_agent_dids: Vec<Option<String>>,
    pub event_trigger_source_agent_dids: Vec<Option<String>>,
    pub skills: Vec<SkillRow>,
    pub skill_source_agent_dids: Vec<Option<String>>,
    pub tool_selections: Vec<ToolSelectionRow>,
    pub inference_backends: Vec<InferenceBackendRow>,
    pub inference_profiles: Vec<InferenceProfileRow>,
    pub tool_service_registries: Vec<ToolServiceRegistryRow>,
    pub inference_backend_source_agent_dids: Vec<Option<String>>,
    pub inference_profile_source_agent_dids: Vec<Option<String>>,
    pub tool_service_registry_source_agent_dids: Vec<Option<String>>,
    conversations_by_agent_did: HashMap<String, Vec<usize>>,
    messages_by_session_id: HashMap<String, Vec<usize>>,
    requests_by_session_id: HashMap<String, Vec<usize>>,
    tool_calls_by_session_id: HashMap<String, Vec<usize>>,
    tool_results_by_session_id: HashMap<String, Vec<usize>>,
    runtimes_by_agent_did: HashMap<String, usize>,
    latest_response_by_request_id: HashMap<String, usize>,
    request_index_by_id: HashMap<String, usize>,
}

#[derive(Debug)]
pub struct TranscriptView<'a> {
    pub messages: Vec<&'a AgentMessageRow>,
    pub tool_calls: Vec<&'a AgentToolCallRow>,
    pub tool_results: Vec<&'a AgentToolResultRow>,
}

/// Aggregated recent-run bookkeeping for a task, rolled up across all
/// triggers (Schedule + EventTrigger) that reference it.
///
/// The apply path owns the `Task` description while the trigger engine
/// owns per-trigger fire bookkeeping on `Schedule` and `EventTrigger`.
/// Operators looking at a single task need to see "how often has this
/// task actually been fired, and what happened last time?" without
/// having to click into every trigger individually -- this struct rolls
/// those numbers up for the Task detail view.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct TaskRecentRuns {
    pub total_fires: u64,
    pub last_attempt_at: Option<String>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub schedule_count: usize,
    pub event_trigger_count: usize,
}

impl Default for ClientStore {
    fn default() -> Self {
        Self::from_rows(ClientStoreRows::default())
    }
}

impl ClientStore {
    pub fn merge_snapshot(&self, snapshot: ClientStore) -> Self {
        let mut rows = self.to_rows();
        let incoming = snapshot.to_rows();

        upsert_rows_by_key(
            &mut rows.agent_principals,
            incoming.agent_principals,
            |row| row.agent_did.clone(),
        );
        upsert_rows_by_key(&mut rows.behaviors, incoming.behaviors, behavior_merge_key);
        upsert_rows_by_key(&mut rows.runtimes, incoming.runtimes, |row| {
            row.agent_did.clone()
        });
        upsert_rows_by_key(
            &mut rows.conversations,
            incoming.conversations,
            conversation_merge_key,
        );
        upsert_rows_by_key(&mut rows.requests, incoming.requests, request_merge_key);
        upsert_rows_by_key(&mut rows.responses, incoming.responses, response_merge_key);
        upsert_rows_with_sources_by_key(
            &mut rows.messages,
            &mut rows.message_source_agent_dids,
            incoming.messages,
            incoming.message_source_agent_dids,
            message_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.sessions,
            &mut rows.session_source_agent_dids,
            incoming.sessions,
            incoming.session_source_agent_dids,
            session_merge_key,
        );
        upsert_goal_rows(&mut rows.goals, incoming.goals);
        upsert_rows_with_sources_by_key(
            &mut rows.tool_calls,
            &mut rows.tool_call_source_agent_dids,
            incoming.tool_calls,
            incoming.tool_call_source_agent_dids,
            tool_call_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.tool_results,
            &mut rows.tool_result_source_agent_dids,
            incoming.tool_results,
            incoming.tool_result_source_agent_dids,
            tool_result_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.compaction_entries,
            &mut rows.compaction_entry_source_agent_dids,
            incoming.compaction_entries,
            incoming.compaction_entry_source_agent_dids,
            compaction_entry_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.tasks,
            &mut rows.task_source_agent_dids,
            incoming.tasks,
            incoming.task_source_agent_dids,
            task_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.schedules,
            &mut rows.schedule_source_agent_dids,
            incoming.schedules,
            incoming.schedule_source_agent_dids,
            schedule_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.event_triggers,
            &mut rows.event_trigger_source_agent_dids,
            incoming.event_triggers,
            incoming.event_trigger_source_agent_dids,
            event_trigger_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.skills,
            &mut rows.skill_source_agent_dids,
            incoming.skills,
            incoming.skill_source_agent_dids,
            skill_merge_key,
        );
        upsert_rows_by_key(
            &mut rows.tool_selections,
            incoming.tool_selections,
            tool_selection_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.inference_backends,
            &mut rows.inference_backend_source_agent_dids,
            incoming.inference_backends,
            incoming.inference_backend_source_agent_dids,
            inference_backend_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.inference_profiles,
            &mut rows.inference_profile_source_agent_dids,
            incoming.inference_profiles,
            incoming.inference_profile_source_agent_dids,
            inference_profile_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.tool_service_registries,
            &mut rows.tool_service_registry_source_agent_dids,
            incoming.tool_service_registries,
            incoming.tool_service_registry_source_agent_dids,
            tool_service_registry_merge_key,
        );

        ClientStore::from_rows(rows)
    }

    /// Replace one agent's authoritative projection instead of additively
    /// merging it. This is used for scoped reloads and delete recovery so rows
    /// absent from the database snapshot (including foreign-requester legacy
    /// bearer rows) cannot survive indefinitely in memory.
    pub fn replace_agent_scope(&self, agent_did: &str, snapshot: ClientStore) -> Self {
        self.replace_agent_scope_inner(agent_did, snapshot, true)
    }

    /// Replace rows fetched from a legacy remote authority. Remote snapshots
    /// are source-stamped, so unstamped local rows must survive even when a
    /// remote agent reuses the same session ID.
    pub fn replace_remote_agent_scope(&self, agent_did: &str, snapshot: ClientStore) -> Self {
        self.replace_agent_scope_inner(agent_did, snapshot, false)
    }

    fn replace_agent_scope_inner(
        &self,
        agent_did: &str,
        snapshot: ClientStore,
        replace_unstamped: bool,
    ) -> Self {
        let mut rows = self.to_rows();
        let mut agent_session_ids = rows
            .conversations
            .iter()
            .filter(|row| row.agent_did.as_deref() == Some(agent_did))
            .map(|row| row.session_id.clone())
            .collect::<HashSet<_>>();
        agent_session_ids.extend(
            rows.requests
                .iter()
                .filter(|row| row.agent_did.as_deref() == Some(agent_did))
                .filter_map(|row| row.session_id.clone()),
        );

        rows.agent_principals
            .retain(|row| row.agent_did != agent_did);
        rows.behaviors
            .retain(|row| row.agent_did.as_deref() != Some(agent_did));
        rows.runtimes.retain(|row| row.agent_did != agent_did);
        rows.conversations
            .retain(|row| row.agent_did.as_deref() != Some(agent_did));
        rows.requests
            .retain(|row| row.agent_did.as_deref() != Some(agent_did));
        rows.responses
            .retain(|row| row.agent_did.as_deref() != Some(agent_did));
        rows.goals.retain(|row| row.agent_did != agent_did);
        rows.tool_results
            .retain(|row| row.agent_did.as_deref() != Some(agent_did));
        rows.tool_selections
            .retain(|row| row.agent_did.as_deref() != Some(agent_did));

        retain_rows_and_sources(
            &mut rows.messages,
            &mut rows.message_source_agent_dids,
            |row, source| {
                source != Some(agent_did)
                    && !(replace_unstamped
                        && source.is_none()
                        && row
                            .session_id
                            .as_deref()
                            .is_some_and(|session_id| agent_session_ids.contains(session_id)))
            },
        );
        retain_rows_and_sources(
            &mut rows.sessions,
            &mut rows.session_source_agent_dids,
            |row, source| {
                source != Some(agent_did)
                    && !(replace_unstamped
                        && source.is_none()
                        && agent_session_ids.contains(&row.session_id))
            },
        );
        retain_rows_and_sources(
            &mut rows.tool_calls,
            &mut rows.tool_call_source_agent_dids,
            |row, source| {
                source != Some(agent_did)
                    && !(replace_unstamped
                        && source.is_none()
                        && row
                            .session_id
                            .as_deref()
                            .is_some_and(|session_id| agent_session_ids.contains(session_id)))
            },
        );
        retain_rows_and_sources(
            &mut rows.compaction_entries,
            &mut rows.compaction_entry_source_agent_dids,
            |row, source| {
                source != Some(agent_did)
                    && !(replace_unstamped
                        && source.is_none()
                        && row
                            .session_id
                            .as_deref()
                            .is_some_and(|session_id| agent_session_ids.contains(session_id)))
            },
        );

        // Scoped snapshots reload the complete local control plane. Replace
        // local rows (source=None) and this remote agent's rows, while retaining
        // rows explicitly stamped as belonging to other remote agents.
        retain_rows_and_sources(
            &mut rows.tasks,
            &mut rows.task_source_agent_dids,
            |_row, source| source != Some(agent_did) && (!replace_unstamped || source.is_some()),
        );
        retain_rows_and_sources(
            &mut rows.schedules,
            &mut rows.schedule_source_agent_dids,
            |_row, source| source != Some(agent_did) && (!replace_unstamped || source.is_some()),
        );
        retain_rows_and_sources(
            &mut rows.event_triggers,
            &mut rows.event_trigger_source_agent_dids,
            |_row, source| source != Some(agent_did) && (!replace_unstamped || source.is_some()),
        );
        retain_rows_and_sources(
            &mut rows.skills,
            &mut rows.skill_source_agent_dids,
            |row, source| {
                row.agent_did.as_deref() != Some(agent_did)
                    && source != Some(agent_did)
                    && (!replace_unstamped || source.is_some())
            },
        );
        retain_rows_and_sources(
            &mut rows.inference_backends,
            &mut rows.inference_backend_source_agent_dids,
            |_row, source| source != Some(agent_did) && (!replace_unstamped || source.is_some()),
        );
        retain_rows_and_sources(
            &mut rows.inference_profiles,
            &mut rows.inference_profile_source_agent_dids,
            |_row, source| source != Some(agent_did) && (!replace_unstamped || source.is_some()),
        );
        retain_rows_and_sources(
            &mut rows.tool_service_registries,
            &mut rows.tool_service_registry_source_agent_dids,
            |_row, source| source != Some(agent_did) && (!replace_unstamped || source.is_some()),
        );

        ClientStore::from_rows(rows).merge_snapshot(snapshot)
    }

    pub fn merge_chat_patch(&self, patch: ClientStore) -> Self {
        let mut rows = self.to_rows();
        let patch_rows = patch.to_rows();

        upsert_rows_by_key(
            &mut rows.conversations,
            patch_rows.conversations,
            conversation_merge_key,
        );
        upsert_rows_by_key(&mut rows.requests, patch_rows.requests, request_merge_key);
        upsert_rows_by_key(
            &mut rows.responses,
            patch_rows.responses,
            response_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.messages,
            &mut rows.message_source_agent_dids,
            patch_rows.messages,
            patch_rows.message_source_agent_dids,
            message_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.sessions,
            &mut rows.session_source_agent_dids,
            patch_rows.sessions,
            patch_rows.session_source_agent_dids,
            session_merge_key,
        );
        upsert_goal_rows(&mut rows.goals, patch_rows.goals);
        upsert_rows_with_sources_by_key(
            &mut rows.tool_calls,
            &mut rows.tool_call_source_agent_dids,
            patch_rows.tool_calls,
            patch_rows.tool_call_source_agent_dids,
            tool_call_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.tool_results,
            &mut rows.tool_result_source_agent_dids,
            patch_rows.tool_results,
            patch_rows.tool_result_source_agent_dids,
            tool_result_merge_key,
        );
        upsert_rows_with_sources_by_key(
            &mut rows.compaction_entries,
            &mut rows.compaction_entry_source_agent_dids,
            patch_rows.compaction_entries,
            patch_rows.compaction_entry_source_agent_dids,
            compaction_entry_merge_key,
        );

        ClientStore::from_rows(rows)
    }

    pub fn stamp_source_agent_did(&mut self, agent_did: &str) {
        let source = Some(agent_did.to_string());
        self.message_source_agent_dids = vec![source.clone(); self.messages.len()];
        self.session_source_agent_dids = vec![source.clone(); self.sessions.len()];
        self.tool_call_source_agent_dids = vec![source.clone(); self.tool_calls.len()];
        self.tool_result_source_agent_dids = vec![source.clone(); self.tool_results.len()];
        self.compaction_entry_source_agent_dids =
            vec![source.clone(); self.compaction_entries.len()];
        self.task_source_agent_dids = vec![source.clone(); self.tasks.len()];
        self.schedule_source_agent_dids = vec![source.clone(); self.schedules.len()];
        self.event_trigger_source_agent_dids = vec![source.clone(); self.event_triggers.len()];
        self.skill_source_agent_dids = vec![source.clone(); self.skills.len()];
        self.inference_backend_source_agent_dids =
            vec![source.clone(); self.inference_backends.len()];
        self.inference_profile_source_agent_dids =
            vec![source.clone(); self.inference_profiles.len()];
        self.tool_service_registry_source_agent_dids =
            vec![source; self.tool_service_registries.len()];
    }

    pub fn to_rows(&self) -> ClientStoreRows {
        ClientStoreRows {
            agent_principals: self.agent_principals.clone(),
            behaviors: self.behaviors.clone(),
            runtimes: self.runtimes.clone(),
            conversations: self.conversations.clone(),
            requests: self.requests.clone(),
            responses: self.responses.clone(),
            messages: self.messages.clone(),
            sessions: self.sessions.clone(),
            goals: self.goals.clone(),
            tool_calls: self.tool_calls.clone(),
            tool_results: self.tool_results.clone(),
            compaction_entries: self.compaction_entries.clone(),
            message_source_agent_dids: self.message_source_agent_dids.clone(),
            session_source_agent_dids: self.session_source_agent_dids.clone(),
            tool_call_source_agent_dids: self.tool_call_source_agent_dids.clone(),
            tool_result_source_agent_dids: self.tool_result_source_agent_dids.clone(),
            compaction_entry_source_agent_dids: self.compaction_entry_source_agent_dids.clone(),
            tasks: self.tasks.clone(),
            schedules: self.schedules.clone(),
            event_triggers: self.event_triggers.clone(),
            task_source_agent_dids: self.task_source_agent_dids.clone(),
            schedule_source_agent_dids: self.schedule_source_agent_dids.clone(),
            event_trigger_source_agent_dids: self.event_trigger_source_agent_dids.clone(),
            skills: self.skills.clone(),
            skill_source_agent_dids: self.skill_source_agent_dids.clone(),
            tool_selections: self.tool_selections.clone(),
            inference_backends: self.inference_backends.clone(),
            inference_profiles: self.inference_profiles.clone(),
            tool_service_registries: self.tool_service_registries.clone(),
            inference_backend_source_agent_dids: self.inference_backend_source_agent_dids.clone(),
            inference_profile_source_agent_dids: self.inference_profile_source_agent_dids.clone(),
            tool_service_registry_source_agent_dids: self
                .tool_service_registry_source_agent_dids
                .clone(),
        }
    }

    pub fn default_behavior_id_for_agent(&self, agent_did: &str) -> Option<&str> {
        self.agent_principals
            .iter()
            .find(|row| row.agent_did == agent_did)
            .and_then(|row| row.default_behavior_id.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn behavior_rows(&self, agent_did: &str) -> Vec<&AgentBehaviorRow> {
        self.behaviors
            .iter()
            .filter(|row| row.agent_did.as_deref() == Some(agent_did))
            .collect()
    }

    pub fn behavior_row(&self, agent_did: &str, behavior_id: &str) -> Option<&AgentBehaviorRow> {
        self.behaviors.iter().find(|row| {
            row.agent_did.as_deref() == Some(agent_did) && row.behavior_id == behavior_id
        })
    }

    pub fn session_behavior_id(&self, session_id: &str, agent_did: Option<&str>) -> Option<String> {
        self.conversations
            .iter()
            .find(|row| {
                row.session_id == session_id
                    && agent_did.is_none_or(|agent_did| row.agent_did.as_deref() == Some(agent_did))
            })
            .and_then(|row| clean_string(row.behavior_id.as_deref()))
            .or_else(|| {
                self.sessions
                    .iter()
                    .find(|row| row.session_id == session_id)
                    .and_then(|row| clean_string(row.behavior_id.as_deref()))
            })
    }

    pub fn conversations_for_behavior(
        &self,
        agent_did: &str,
        behavior_id: &str,
    ) -> Vec<&AgentConversationRow> {
        self.conversations
            .iter()
            .filter(|row| {
                row.agent_did.as_deref() == Some(agent_did)
                    && clean_string(row.behavior_id.as_deref()).as_deref() == Some(behavior_id)
            })
            .collect()
    }

    pub fn requests_for_behavior(
        &self,
        agent_did: &str,
        behavior_id: &str,
    ) -> Vec<&AgentRequestRow> {
        self.requests
            .iter()
            .filter(|row| {
                row.agent_did.as_deref() == Some(agent_did)
                    && clean_string(row.behavior_id.as_deref()).as_deref() == Some(behavior_id)
            })
            .collect()
    }

    /// Return every `Task` bound to the given behavior.
    ///
    /// `Task` rows are not scoped by `agent_did` — they carry a single
    /// `behavior_id` and are addressed globally by `task_id`. The
    /// `_agent_did` parameter is kept so call sites that pass an agent scope
    /// (today's behavior-diagnostics view, for example) stay ergonomic; the
    /// filter is intentionally behavior-scoped only.
    pub fn tasks_for_behavior(&self, _agent_did: &str, behavior_id: &str) -> Vec<&TaskRow> {
        self.tasks
            .iter()
            .filter(|row| clean_string(row.behavior_id.as_deref()).as_deref() == Some(behavior_id))
            .collect()
    }

    /// Return every `Schedule` whose `task_id` matches one of the provided
    /// tasks. Useful for listing the schedules attached to a behavior
    /// indirectly (via its tasks).
    pub fn schedules_for_tasks(&self, task_ids: &[&str]) -> Vec<&ScheduleRow> {
        if task_ids.is_empty() {
            return Vec::new();
        }
        self.schedules
            .iter()
            .filter(|row| {
                row.task_id
                    .as_deref()
                    .is_some_and(|task_id| task_ids.contains(&task_id))
            })
            .collect()
    }

    /// Return every `EventTrigger` whose `task_id` matches one of the
    /// provided tasks. Mirrors `schedules_for_tasks` so manage views can
    /// list the triggers attached to a behavior indirectly (via its
    /// tasks).
    pub fn event_triggers_for_tasks(&self, task_ids: &[&str]) -> Vec<&EventTriggerRow> {
        if task_ids.is_empty() {
            return Vec::new();
        }
        self.event_triggers
            .iter()
            .filter(|row| {
                row.task_id
                    .as_deref()
                    .is_some_and(|task_id| task_ids.contains(&task_id))
            })
            .collect()
    }

    /// Roll up the trigger-engine bookkeeping for a `Task` across every
    /// `Schedule` and `EventTrigger` that references it.
    ///
    /// Both trigger kinds carry their own independent `fire_count`,
    /// `last_attempt_at`, `last_status`, and `last_error` fields. This
    /// helper sums the fires and picks the most recent `last_attempt_at`
    /// (lexicographic max on the ISO-8601 timestamp strings -- the
    /// trigger engine always writes RFC3339/Z-suffixed stamps, so
    /// lexical order matches chronological order), then surfaces the
    /// status/error from the trigger that produced that most-recent
    /// attempt. Used by the Task detail view to show operators a single
    /// rolled-up "Recent Runs" summary instead of forcing them to click
    /// into each individual trigger.
    pub fn recent_runs_for_task(&self, task_id: &str) -> TaskRecentRuns {
        let schedules: Vec<&ScheduleRow> = self
            .schedules
            .iter()
            .filter(|s| s.task_id.as_deref() == Some(task_id))
            .collect();
        let events: Vec<&EventTriggerRow> = self
            .event_triggers
            .iter()
            .filter(|t| t.task_id.as_deref() == Some(task_id))
            .collect();

        let total_fires = schedules
            .iter()
            .map(|s| s.fire_count.unwrap_or(0).max(0) as u64)
            .sum::<u64>()
            + events
                .iter()
                .map(|t| t.fire_count.unwrap_or(0).max(0) as u64)
                .sum::<u64>();

        // Find the most recent attempt_at across all triggers.
        let all_attempts: Vec<&str> = schedules
            .iter()
            .filter_map(|s| s.last_attempt_at.as_deref())
            .chain(events.iter().filter_map(|t| t.last_attempt_at.as_deref()))
            .collect();
        let last_attempt_at = all_attempts.iter().max().map(ToString::to_string);

        // Resolve status + error from the trigger whose timestamp
        // equals the max. Ties (two triggers firing in the same second
        // on the same task) resolve in favor of the first schedule
        // found, then the first event trigger found -- rare in
        // practice, and the operator still sees the aggregate
        // fire-count.
        let (last_status, last_error) = if let Some(ref target_ts) = last_attempt_at {
            let mut pair = None;
            for s in &schedules {
                if s.last_attempt_at.as_deref() == Some(target_ts.as_str()) {
                    pair = Some((s.last_status.clone(), s.last_error.clone()));
                    break;
                }
            }
            if pair.is_none() {
                for t in &events {
                    if t.last_attempt_at.as_deref() == Some(target_ts.as_str()) {
                        pair = Some((t.last_status.clone(), t.last_error.clone()));
                        break;
                    }
                }
            }
            pair.unwrap_or((None, None))
        } else {
            (None, None)
        };

        TaskRecentRuns {
            total_fires,
            last_attempt_at,
            last_status,
            last_error,
            schedule_count: schedules.len(),
            event_trigger_count: events.len(),
        }
    }

    pub fn conversation_rows(&self, agent_did: &str) -> Vec<&AgentConversationRow> {
        self.conversations_by_agent_did
            .get(agent_did)
            .into_iter()
            .flat_map(|indexes| indexes.iter())
            .map(|index| &self.conversations[*index])
            .collect()
    }

    pub fn transcript(&self, session_id: &str) -> TranscriptView<'_> {
        TranscriptView {
            messages: indexes_to_refs(&self.messages, self.messages_by_session_id.get(session_id)),
            tool_calls: indexes_to_refs(
                &self.tool_calls,
                self.tool_calls_by_session_id.get(session_id),
            ),
            tool_results: indexes_to_refs(
                &self.tool_results,
                self.tool_results_by_session_id.get(session_id),
            ),
        }
    }

    pub fn transcript_for_agent(&self, session_id: &str, agent_did: &str) -> TranscriptView<'_> {
        let message_indexes = self
            .messages_by_session_id
            .get(session_id)
            .into_iter()
            .flat_map(|indexes| indexes.iter())
            .copied()
            .filter(|index| {
                source_agent_matches(&self.message_source_agent_dids, *index, agent_did)
            })
            .collect::<Vec<_>>();
        let tool_call_indexes = self
            .tool_calls_by_session_id
            .get(session_id)
            .into_iter()
            .flat_map(|indexes| indexes.iter())
            .copied()
            .filter(|index| {
                source_agent_matches(&self.tool_call_source_agent_dids, *index, agent_did)
            })
            .collect::<Vec<_>>();
        let tool_result_indexes = self
            .tool_results_by_session_id
            .get(session_id)
            .into_iter()
            .flat_map(|indexes| indexes.iter())
            .copied()
            .filter(|index| {
                let row = &self.tool_results[*index];
                row_agent_matches(row.agent_did.as_deref(), agent_did)
                    && source_agent_matches(&self.tool_result_source_agent_dids, *index, agent_did)
            })
            .collect::<Vec<_>>();

        TranscriptView {
            messages: message_indexes
                .into_iter()
                .map(|index| &self.messages[index])
                .collect(),
            tool_calls: tool_call_indexes
                .into_iter()
                .map(|index| &self.tool_calls[index])
                .collect(),
            tool_results: tool_result_indexes
                .into_iter()
                .map(|index| &self.tool_results[index])
                .collect(),
        }
    }

    pub fn requests_for_session(&self, session_id: &str) -> Vec<&AgentRequestRow> {
        indexes_to_refs(&self.requests, self.requests_by_session_id.get(session_id))
    }

    pub fn requests_for_session_for_agent(
        &self,
        session_id: &str,
        agent_did: &str,
    ) -> Vec<&AgentRequestRow> {
        self.requests_for_session(session_id)
            .into_iter()
            .filter(|row| row_agent_matches(row.agent_did.as_deref(), agent_did))
            .collect()
    }

    pub fn latest_request_id_for_session(&self, session_id: &str) -> Option<String> {
        self.conversations
            .iter()
            .find(|row| row.session_id == session_id)
            .and_then(|row| clean_string(row.latest_request_id.as_deref()))
            .filter(|request_id| {
                !self.requests.iter().any(|request| {
                    request.request_id == *request_id
                        && request.session_id.as_deref() == Some(session_id)
                        && is_deprecated_background_completion_request(request)
                })
            })
            .or_else(|| {
                self.requests_by_session_id
                    .get(session_id)
                    .and_then(|indexes| {
                        indexes.iter().rev().find(|index| {
                            !is_deprecated_background_completion_request(&self.requests[**index])
                        })
                    })
                    .copied()
                    .map(|index| self.requests[index].request_id.clone())
            })
    }

    pub fn latest_request_id_for_session_for_agent(
        &self,
        session_id: &str,
        agent_did: &str,
    ) -> Option<String> {
        self.conversations
            .iter()
            .find(|row| row.session_id == session_id && row.agent_did.as_deref() == Some(agent_did))
            .and_then(|row| clean_string(row.latest_request_id.as_deref()))
            .filter(|request_id| {
                !self.requests.iter().any(|request| {
                    request.request_id == *request_id
                        && request.session_id.as_deref() == Some(session_id)
                        && row_agent_matches(request.agent_did.as_deref(), agent_did)
                        && is_deprecated_background_completion_request(request)
                })
            })
            .or_else(|| {
                self.requests_by_session_id
                    .get(session_id)
                    .and_then(|indexes| {
                        indexes.iter().rev().find(|index| {
                            row_agent_matches(
                                self.requests[**index].agent_did.as_deref(),
                                agent_did,
                            ) && !is_deprecated_background_completion_request(
                                &self.requests[**index],
                            )
                        })
                    })
                    .map(|index| self.requests[*index].request_id.clone())
            })
    }

    pub fn latest_runtime(&self, agent_did: &str) -> Option<&AgentRuntimeRow> {
        self.runtimes_by_agent_did
            .get(agent_did)
            .map(|index| &self.runtimes[*index])
    }

    pub fn latest_response_for_request(&self, request_id: &str) -> Option<&AgentResponseRow> {
        self.latest_response_by_request_id
            .get(request_id)
            .map(|index| &self.responses[*index])
    }

    pub fn latest_response_for_request_for_agent(
        &self,
        request_id: &str,
        agent_did: &str,
    ) -> Option<&AgentResponseRow> {
        self.responses
            .iter()
            .filter(|row| {
                row.request_id.as_deref() == Some(request_id)
                    && row_agent_matches(row.agent_did.as_deref(), agent_did)
            })
            .max_by(|left, right| {
                left.progress_seq
                    .unwrap_or_default()
                    .cmp(&right.progress_seq.unwrap_or_default())
                    .then_with(|| {
                        left.completed_at
                            .as_deref()
                            .unwrap_or_default()
                            .cmp(right.completed_at.as_deref().unwrap_or_default())
                    })
                    .then_with(|| {
                        left.created_at
                            .as_deref()
                            .unwrap_or_default()
                            .cmp(right.created_at.as_deref().unwrap_or_default())
                    })
                    .then_with(|| left.response_key.cmp(&right.response_key))
            })
    }

    pub fn request_row(&self, request_id: &str) -> Option<&AgentRequestRow> {
        self.request_index_by_id
            .get(request_id)
            .map(|index| &self.requests[*index])
    }

    pub fn row_count(&self) -> usize {
        self.agent_principals.len()
            + self.behaviors.len()
            + self.runtimes.len()
            + self.conversations.len()
            + self.requests.len()
            + self.responses.len()
            + self.messages.len()
            + self.sessions.len()
            + self.goals.len()
            + self.tool_calls.len()
            + self.tool_results.len()
            + self.compaction_entries.len()
            + self.tasks.len()
            + self.schedules.len()
            + self.event_triggers.len()
            + self.skills.len()
            + self.tool_selections.len()
            + self.inference_backends.len()
            + self.inference_profiles.len()
            + self.tool_service_registries.len()
    }

    pub fn approx_serialized_bytes(&self) -> usize {
        serde_json::to_vec(&self.to_rows())
            .map(|bytes| bytes.len())
            .unwrap_or_default()
    }

    pub fn derive_turn(&self, session_id: &str) -> Option<ClientTurnState> {
        turns::derive_turn(self, session_id)
    }

    pub fn derive_turn_for_agent(
        &self,
        session_id: &str,
        agent_did: &str,
    ) -> Option<ClientTurnState> {
        turns::derive_turn_for_agent(self, session_id, agent_did)
    }

    pub fn derive_turn_for_request(&self, request_id: &str) -> Option<ClientTurnState> {
        turns::derive_turn_for_request(self, request_id)
    }

    pub fn derive_turn_for_request_for_agent(
        &self,
        request_id: &str,
        agent_did: &str,
    ) -> Option<ClientTurnState> {
        turns::derive_turn_for_request_for_agent(self, request_id, agent_did)
    }
}

pub type SharedClientStore = Arc<ClientStore>;

fn row_agent_matches(row_agent_did: Option<&str>, agent_did: &str) -> bool {
    row_agent_did.map_or(true, |row_agent_did| row_agent_did == agent_did)
}

pub fn is_deprecated_background_completion_request(request: &AgentRequestRow) -> bool {
    gents::lifecycle::is_deprecated_background_completion_request(
        request.execution_origin.as_deref(),
        request.metadata.as_deref(),
    )
}

fn source_agent_matches(sources: &[Option<String>], row_index: usize, agent_did: &str) -> bool {
    sources
        .get(row_index)
        .and_then(|source| source.as_deref())
        .map_or(true, |source_agent_did| source_agent_did == agent_did)
}

fn upsert_rows_by_key<T>(target: &mut Vec<T>, incoming: Vec<T>, key_fn: impl Fn(&T) -> String) {
    let mut indexes = target
        .iter()
        .enumerate()
        .map(|(index, row)| (key_fn(row), index))
        .collect::<HashMap<_, _>>();

    for row in incoming {
        let key = key_fn(&row);
        if let Some(index) = indexes.get(&key).copied() {
            target[index] = row;
        } else {
            indexes.insert(key, target.len());
            target.push(row);
        }
    }
}

fn retain_rows_and_sources<T>(
    rows: &mut Vec<T>,
    sources: &mut Vec<Option<String>>,
    mut keep: impl FnMut(&T, Option<&str>) -> bool,
) {
    sources.resize(rows.len(), None);
    let mut kept_rows = Vec::with_capacity(rows.len());
    let mut kept_sources = Vec::with_capacity(sources.len());
    for (row, source) in std::mem::take(rows)
        .into_iter()
        .zip(std::mem::take(sources))
    {
        if keep(&row, source.as_deref()) {
            kept_rows.push(row);
            kept_sources.push(source);
        }
    }
    *rows = kept_rows;
    *sources = kept_sources;
}

/// Merge durable goals without allowing a later-created replicated twin to
/// replace the canonical row selected by the runtime. A row with the same
/// creation time and goal ID is treated as an update and replaces in place.
fn upsert_goal_rows(target: &mut Vec<GoalRow>, incoming: Vec<GoalRow>) {
    let mut indexes = target
        .iter()
        .enumerate()
        .map(|(index, row)| (goal_merge_key(row), index))
        .collect::<HashMap<_, _>>();

    for row in incoming {
        let key = goal_merge_key(&row);
        if let Some(index) = indexes.get(&key).copied() {
            let canonical_order = row
                .created_at
                .cmp(&target[index].created_at)
                .then_with(|| row.goal_id.cmp(&target[index].goal_id));
            if !canonical_order.is_gt() {
                target[index] = row;
            }
        } else {
            indexes.insert(key, target.len());
            target.push(row);
        }
    }
}

fn upsert_rows_with_sources_by_key<T>(
    target: &mut Vec<T>,
    target_sources: &mut Vec<Option<String>>,
    incoming: Vec<T>,
    incoming_sources: Vec<Option<String>>,
    key_fn: impl Fn(&T, Option<&str>) -> String,
) {
    normalize_sources(target_sources, target.len());
    let mut incoming_sources = incoming_sources;
    normalize_sources(&mut incoming_sources, incoming.len());

    let mut indexes = target
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let source = target_sources.get(index).and_then(|value| value.as_deref());
            (key_fn(row, source), index)
        })
        .collect::<HashMap<_, _>>();

    for (row, source) in incoming.into_iter().zip(incoming_sources.into_iter()) {
        let key = key_fn(&row, source.as_deref());
        if let Some(index) = indexes.get(&key).copied() {
            target[index] = row;
            target_sources[index] = source;
        } else {
            indexes.insert(key, target.len());
            target.push(row);
            target_sources.push(source);
        }
    }
}

fn normalize_sources(sources: &mut Vec<Option<String>>, row_count: usize) {
    sources.truncate(row_count);
    sources.resize_with(row_count, || None);
}

fn conversation_merge_key(row: &AgentConversationRow) -> String {
    format!(
        "{}\0{}",
        row.agent_did.as_deref().unwrap_or_default(),
        row.session_id
    )
}

fn behavior_merge_key(row: &AgentBehaviorRow) -> String {
    format!(
        "{}\0{}",
        row.agent_did.as_deref().unwrap_or_default(),
        row.behavior_id
    )
}

fn request_merge_key(row: &AgentRequestRow) -> String {
    format!(
        "{}\0{}",
        row.agent_did.as_deref().unwrap_or_default(),
        row.request_id
    )
}

fn response_merge_key(row: &AgentResponseRow) -> String {
    format!(
        "{}\0{}",
        row.agent_did.as_deref().unwrap_or_default(),
        row.response_key
    )
}

fn message_merge_key(row: &AgentMessageRow, source_agent_did: Option<&str>) -> String {
    format!(
        "{}\0{}",
        source_agent_did.unwrap_or_default(),
        row.message_key
    )
}

fn session_merge_key(row: &AgentSessionRow, source_agent_did: Option<&str>) -> String {
    format!(
        "{}\0{}",
        source_agent_did.unwrap_or_default(),
        row.session_id
    )
}

fn goal_merge_key(row: &GoalRow) -> String {
    format!("{}\0{}", row.agent_did, row.session_id)
}

fn tool_call_merge_key(row: &AgentToolCallRow, source_agent_did: Option<&str>) -> String {
    format!(
        "{}\0{}",
        source_agent_did.unwrap_or_default(),
        row.tool_call_key
    )
}

fn tool_result_merge_key(row: &AgentToolResultRow, source_agent_did: Option<&str>) -> String {
    format!(
        "{}\0{}\0{}\0{}\0{}\0{}\0{}",
        source_agent_did.unwrap_or_default(),
        row.agent_did.as_deref().unwrap_or_default(),
        row.session_id.as_deref().unwrap_or_default(),
        row.tool_name.as_deref().unwrap_or_default(),
        row.tool_input.as_deref().unwrap_or_default(),
        row.conversation_doc_id.as_deref().unwrap_or_default(),
        row.created_at.as_deref().unwrap_or_default()
    )
}

fn compaction_entry_merge_key(row: &CompactionEntryRow, source_agent_did: Option<&str>) -> String {
    format!(
        "{}\0{}",
        source_agent_did.unwrap_or_default(),
        row.compaction_key
    )
}

fn task_merge_key(row: &TaskRow, source_agent_did: Option<&str>) -> String {
    format!("{}\0{}", source_agent_did.unwrap_or_default(), row.task_id)
}

fn schedule_merge_key(row: &ScheduleRow, source_agent_did: Option<&str>) -> String {
    format!(
        "{}\0{}",
        source_agent_did.unwrap_or_default(),
        row.schedule_id
    )
}

fn event_trigger_merge_key(row: &EventTriggerRow, source_agent_did: Option<&str>) -> String {
    format!(
        "{}\0{}",
        source_agent_did.unwrap_or_default(),
        row.trigger_id
    )
}

fn skill_merge_key(row: &SkillRow, source_agent_did: Option<&str>) -> String {
    format!("{}\0{}", source_agent_did.unwrap_or_default(), row.skill_id)
}

fn tool_selection_merge_key(row: &ToolSelectionRow) -> String {
    format!(
        "{}\0{}",
        row.agent_did.as_deref().unwrap_or_default(),
        row.selection_id
    )
}

fn inference_backend_merge_key(
    row: &InferenceBackendRow,
    source_agent_did: Option<&str>,
) -> String {
    format!(
        "{}\0{}",
        source_agent_did.unwrap_or_default(),
        row.backend_id
    )
}

fn inference_profile_merge_key(
    row: &InferenceProfileRow,
    source_agent_did: Option<&str>,
) -> String {
    format!(
        "{}\0{}",
        source_agent_did.unwrap_or_default(),
        row.profile_id
    )
}

fn tool_service_registry_merge_key(
    row: &ToolServiceRegistryRow,
    source_agent_did: Option<&str>,
) -> String {
    format!(
        "{}\0{}",
        source_agent_did.unwrap_or_default(),
        row.service_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn goal_row(goal_id: &str, created_at: &str, status: &str) -> GoalRow {
        serde_json::from_value(serde_json::json!({
            "goal_id": goal_id,
            "session_id": "session-1",
            "agent_did": "did:agent:1",
            "status": status,
            "created_at": created_at
        }))
        .expect("goal row")
    }

    fn schedule_row(
        schedule_id: &str,
        task_id: &str,
        fire_count: Option<i64>,
        last_attempt_at: Option<&str>,
        last_status: Option<&str>,
        last_error: Option<&str>,
    ) -> ScheduleRow {
        ScheduleRow {
            schedule_id: schedule_id.to_string(),
            task_id: Some(task_id.to_string()),
            interval_secs: None,
            cron: None,
            timezone: None,
            missed_run_policy: None,
            enabled: None,
            concurrency: None,
            next_run_at: None,
            last_attempt_at: last_attempt_at.map(str::to_string),
            last_status: last_status.map(str::to_string),
            last_error: last_error.map(str::to_string),
            fire_count,
            created_at: None,
            updated_at: None,
        }
    }

    fn event_trigger_row(
        trigger_id: &str,
        task_id: &str,
        fire_count: Option<i64>,
        last_attempt_at: Option<&str>,
        last_status: Option<&str>,
        last_error: Option<&str>,
    ) -> EventTriggerRow {
        EventTriggerRow {
            trigger_id: trigger_id.to_string(),
            task_id: Some(task_id.to_string()),
            source_collection: None,
            event_kind: None,
            filter: None,
            enabled: None,
            concurrency: None,
            correlation_field: None,
            fire_mode: None,
            expected_count: None,
            expected_count_field: None,
            group_timeout_secs: None,
            group_min_count: None,
            workspace_authority: None,
            created_at: None,
            updated_at: None,
            last_attempt_at: last_attempt_at.map(str::to_string),
            last_fired_source_doc_id: None,
            last_status: last_status.map(str::to_string),
            last_error: last_error.map(str::to_string),
            fire_count,
        }
    }

    fn task_row(task_id: &str, behavior_id: &str) -> TaskRow {
        TaskRow {
            task_id: task_id.to_string(),
            name: None,
            description: None,
            behavior_id: Some(behavior_id.to_string()),
            prompt_template: None,
            enabled: Some(true),
            output_schema_ref: None,
            created_at: None,
            updated_at: None,
        }
    }

    fn request_row(
        request_id: &str,
        created_at: &str,
        lifecycle_state: &str,
        execution_origin: &str,
        metadata: Option<String>,
    ) -> AgentRequestRow {
        serde_json::from_value(serde_json::json!({
            "request_id": request_id,
            "agent_did": "did:agent:1",
            "behavior_id": "default",
            "session_id": "session-1",
            "content": "turn",
            "status": lifecycle_state,
            "lifecycle_state": lifecycle_state,
            "execution_origin": execution_origin,
            "metadata": metadata,
            "created_at": created_at
        }))
        .expect("request row")
    }

    #[test]
    fn legacy_wake_is_not_authoritative_latest_request() {
        let metadata = background_wake_metadata();
        let store = ClientStore::from_rows(ClientStoreRows {
            conversations: vec![AgentConversationRow {
                session_id: "session-1".to_string(),
                agent_name: None,
                agent_did: Some("did:agent:1".to_string()),
                requester_did: None,
                behavior_id: Some("default".to_string()),
                title: None,
                title_source: None,
                preview_text: None,
                status: Some("active".to_string()),
                created_at: None,
                updated_at: None,
                latest_request_id: Some("legacy-wake".to_string()),
            }],
            requests: vec![
                request_row(
                    "interactive",
                    "2026-07-01T00:00:00Z",
                    "completed",
                    "interactive",
                    None,
                ),
                request_row(
                    "legacy-wake",
                    "2026-07-01T00:00:01Z",
                    "pending",
                    "scheduled",
                    Some(metadata),
                ),
            ],
            ..ClientStoreRows::default()
        });

        assert_eq!(
            store.latest_request_id_for_session("session-1").as_deref(),
            Some("interactive")
        );
        assert_eq!(
            store
                .latest_request_id_for_session_for_agent("session-1", "did:agent:1")
                .as_deref(),
            Some("interactive")
        );
        assert_eq!(
            store.derive_turn("session-1"),
            Some(ClientTurnState::Completed)
        );
    }

    #[test]
    fn unknown_conversation_pointer_preserves_partial_observation() {
        let store = ClientStore::from_rows(ClientStoreRows {
            conversations: vec![AgentConversationRow {
                session_id: "session-1".to_string(),
                agent_name: None,
                agent_did: Some("did:agent:1".to_string()),
                requester_did: None,
                behavior_id: Some("default".to_string()),
                title: None,
                title_source: None,
                preview_text: None,
                status: Some("active".to_string()),
                created_at: None,
                updated_at: None,
                latest_request_id: Some("not-replicated-yet".to_string()),
            }],
            requests: vec![request_row(
                "old-terminal",
                "2026-07-01T00:00:00Z",
                "completed",
                "interactive",
                None,
            )],
            ..ClientStoreRows::default()
        });

        assert_eq!(
            store.latest_request_id_for_session("session-1").as_deref(),
            Some("not-replicated-yet")
        );
        assert_eq!(
            store
                .latest_request_id_for_session_for_agent("session-1", "did:agent:1")
                .as_deref(),
            Some("not-replicated-yet")
        );
        assert_eq!(
            store.derive_turn_for_agent("session-1", "did:agent:1"),
            None,
            "an unknown latest pointer must not regress to an older terminal request"
        );
    }

    #[test]
    fn remote_scope_replacement_preserves_unstamped_same_session_rows() {
        let current = ClientStore::from_rows(ClientStoreRows {
            conversations: vec![
                serde_json::from_value(serde_json::json!({
                    "session_id": "shared-session",
                    "agent_did": "did:agent:local"
                }))
                .expect("local conversation"),
                serde_json::from_value(serde_json::json!({
                    "session_id": "shared-session",
                    "agent_did": "did:agent:remote"
                }))
                .expect("remote conversation"),
            ],
            messages: vec![
                serde_json::from_value(serde_json::json!({
                    "message_key": "local:1",
                    "session_id": "shared-session",
                    "role": "user",
                    "content": "local"
                }))
                .expect("local message"),
                serde_json::from_value(serde_json::json!({
                    "message_key": "remote:1",
                    "session_id": "shared-session",
                    "role": "user",
                    "content": "remote"
                }))
                .expect("remote message"),
            ],
            message_source_agent_dids: vec![None, Some("did:agent:remote".to_string())],
            ..ClientStoreRows::default()
        });

        let replaced =
            current.replace_remote_agent_scope("did:agent:remote", ClientStore::default());

        assert_eq!(replaced.conversations.len(), 1);
        assert_eq!(
            replaced.conversations[0].agent_did.as_deref(),
            Some("did:agent:local")
        );
        assert_eq!(
            replaced
                .messages
                .iter()
                .map(|row| row.message_key.as_str())
                .collect::<Vec<_>>(),
            vec!["local:1"]
        );
        assert_eq!(replaced.message_source_agent_dids, vec![None]);
    }

    fn background_wake_metadata() -> String {
        r#"{"queue":{"source":"background_completion","policy":"coalesce","key":"child-1","queued_after_request_id":null}}"#.to_string()
    }

    #[test]
    fn recent_runs_aggregates_across_schedules_and_event_triggers() {
        let mut store = ClientStore::default();
        store.schedules.push(schedule_row(
            "s1",
            "task-1",
            Some(3),
            Some("2026-04-22T10:00:00Z"),
            Some("fired"),
            None,
        ));
        store.event_triggers.push(event_trigger_row(
            "t1",
            "task-1",
            Some(5),
            Some("2026-04-22T11:00:00Z"),
            Some("skipped"),
            Some("in-flight"),
        ));

        let runs = store.recent_runs_for_task("task-1");
        assert_eq!(runs.total_fires, 8);
        assert_eq!(
            runs.last_attempt_at.as_deref(),
            Some("2026-04-22T11:00:00Z")
        );
        assert_eq!(runs.last_status.as_deref(), Some("skipped"));
        assert_eq!(runs.last_error.as_deref(), Some("in-flight"));
        assert_eq!(runs.schedule_count, 1);
        assert_eq!(runs.event_trigger_count, 1);
    }

    #[test]
    fn recent_runs_empty_when_no_triggers() {
        let store = ClientStore::default();
        let runs = store.recent_runs_for_task("task-missing");
        assert_eq!(runs, TaskRecentRuns::default());
    }

    #[test]
    fn source_agent_dids_round_trip_with_rows() {
        let mut store = ClientStore::from_rows(ClientStoreRows {
            tasks: vec![task_row("task-1", "default")],
            schedules: vec![schedule_row("schedule-1", "task-1", None, None, None, None)],
            event_triggers: vec![event_trigger_row(
                "trigger-1",
                "task-1",
                None,
                None,
                None,
                None,
            )],
            ..ClientStoreRows::default()
        });
        store.stamp_source_agent_did("did:test:mini-1");

        let restored = ClientStore::from_rows(store.to_rows());

        assert_eq!(
            restored.task_source_agent_dids,
            vec![Some("did:test:mini-1".to_string())]
        );
        assert_eq!(
            restored.schedule_source_agent_dids,
            vec![Some("did:test:mini-1".to_string())]
        );
        assert_eq!(
            restored.event_trigger_source_agent_dids,
            vec![Some("did:test:mini-1".to_string())]
        );
    }

    #[test]
    fn goal_merge_preserves_the_earliest_canonical_twin() {
        let canonical_created_at = "2026-07-16T00:00:00Z";
        let store = ClientStore::from_rows(ClientStoreRows {
            goals: vec![
                goal_row("later-twin", "2026-07-16T00:00:01Z", "complete"),
                goal_row("canonical", canonical_created_at, "active"),
            ],
            ..ClientStoreRows::default()
        });
        assert_eq!(store.goals.len(), 1);
        assert_eq!(store.goals[0].goal_id, "canonical");

        let later_twin = ClientStore::from_rows(ClientStoreRows {
            goals: vec![goal_row(
                "arriving-twin",
                "2026-07-16T00:00:02Z",
                "complete",
            )],
            ..ClientStoreRows::default()
        });
        let store = store.merge_snapshot(later_twin);
        assert_eq!(store.goals.len(), 1);
        assert_eq!(store.goals[0].status.as_deref(), Some("active"));

        let canonical_update = ClientStore::from_rows(ClientStoreRows {
            goals: vec![goal_row("canonical", canonical_created_at, "complete")],
            ..ClientStoreRows::default()
        });
        let store = store.merge_snapshot(canonical_update);
        assert_eq!(store.goals.len(), 1);
        assert_eq!(store.goals[0].status.as_deref(), Some("complete"));
    }
}
