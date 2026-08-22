use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;
use tokio::sync::mpsc;
#[cfg(test)]
use tokio::sync::watch;

use crate::admission::BackendAdmissionConfig;
use crate::config::AgentBehavior;
use crate::identity::AgentPrincipal;
use crate::schedule_cron::{next_cron_run_after, CronMissedRunPolicy};
use crate::tool_surface::ToolSurface;
use crate::watcher::AgentRequest;

pub type DispatcherMap = HashMap<String, mpsc::Sender<AgentRequest>>;

#[derive(Debug, Clone)]
pub struct ResolvedTask {
    pub task_id: String,
    pub name: Option<String>,
    pub behavior_id: String,
    pub prompt_template: String,
    #[allow(dead_code)]
    pub output_schema_ref: Option<String>,
}

impl ResolvedTask {
    pub(crate) fn display_label(&self) -> &str {
        self.name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(&self.task_id)
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedSchedule {
    pub schedule_id: String,
    #[allow(dead_code)]
    pub task_id: String,
    pub task: ResolvedTask,
    pub cadence: ScheduleCadence,
    #[allow(dead_code)]
    pub enabled: bool,
    pub concurrency: ConcurrencyMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleCadence {
    Interval {
        interval_secs: i64,
    },
    Cron {
        expression: String,
        timezone: String,
        missed_run_policy: CronMissedRunPolicy,
    },
}

impl ScheduleCadence {
    pub(crate) fn seed_next_run_at(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
        match self {
            Self::Interval { .. } => Ok(now),
            Self::Cron {
                expression,
                timezone,
                ..
            } => next_cron_run_after(expression, timezone, now),
        }
    }

    pub(crate) fn advance_next_run_at(
        &self,
        parsed_next_run_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>> {
        match self {
            Self::Interval { interval_secs } => {
                Ok(parsed_next_run_at + ChronoDuration::seconds(*interval_secs))
            }
            Self::Cron {
                expression,
                timezone,
                missed_run_policy,
            } => match missed_run_policy {
                CronMissedRunPolicy::LatestOnly => next_cron_run_after(expression, timezone, now),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConcurrencyMode {
    Parallel,
    Serial,
    LatestOnly,
}

pub const MAX_EVENT_TRIGGER_GROUP_DOCS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventTriggerFireMode {
    PerDocument,
    PerGroup,
}

impl EventTriggerFireMode {
    pub(crate) fn parse(value: Option<&str>) -> Option<Self> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            None | Some("per_document") => Some(Self::PerDocument),
            Some("per_group") => Some(Self::PerGroup),
            Some(_) => None,
        }
    }
}

impl ConcurrencyMode {
    pub(crate) fn parse(s: &str) -> Option<Self> {
        match s {
            "parallel" => Some(Self::Parallel),
            "serial" => Some(Self::Serial),
            "latest_only" => Some(Self::LatestOnly),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedEventTrigger {
    pub trigger_id: String,
    #[allow(dead_code)]
    pub task_id: String,
    pub task: ResolvedTask,
    pub source_collection: String,
    pub event_kind: String,
    pub filter: Option<String>,
    #[allow(dead_code)]
    pub enabled: bool,
    pub concurrency: ConcurrencyMode,
    pub fire_mode: EventTriggerFireMode,
    pub correlation_field: Option<String>,
    pub expected_count: Option<usize>,
    pub expected_count_field: Option<String>,
    pub group_timeout_secs: Option<u64>,
    pub group_min_count: usize,
    pub workspace_authority: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedRuntimeSnapshot {
    pub(crate) principal: Option<Arc<AgentPrincipal>>,
    pub(crate) local_did: String,
    pub(crate) paired_peer_dids: HashSet<String>,
    pub(crate) default_behavior_id: String,
    pub(crate) behaviors: HashMap<String, Arc<AgentBehavior>>,
    pub(crate) tool_surfaces: HashMap<String, Arc<ToolSurface>>,
    pub(crate) backend_admission_configs: HashMap<String, BackendAdmissionConfig>,
    pub(crate) unavailable_behaviors: HashMap<String, String>,
    pub(crate) active_schedules: HashMap<String, ResolvedSchedule>,
    pub(crate) unavailable_schedules: HashSet<String>,
    pub(crate) active_event_triggers: HashMap<String, ResolvedEventTrigger>,
    pub(crate) unavailable_event_triggers: HashSet<String>,
    pub(crate) active_tasks: HashMap<String, ResolvedTask>,
}

impl ResolvedRuntimeSnapshot {
    #[allow(dead_code)]
    pub(crate) fn from_parts(
        default_behavior_id: String,
        behaviors: Vec<Arc<AgentBehavior>>,
        tool_surfaces: HashMap<String, Arc<ToolSurface>>,
        unavailable_behaviors: HashMap<String, String>,
    ) -> Self {
        Self::from_parts_with_admission_configs(
            default_behavior_id,
            behaviors,
            tool_surfaces,
            HashMap::new(),
            unavailable_behaviors,
        )
    }

    pub(crate) fn from_parts_with_admission_configs(
        default_behavior_id: String,
        behaviors: Vec<Arc<AgentBehavior>>,
        tool_surfaces: HashMap<String, Arc<ToolSurface>>,
        backend_admission_configs: HashMap<String, BackendAdmissionConfig>,
        unavailable_behaviors: HashMap<String, String>,
    ) -> Self {
        Self {
            principal: None,
            local_did: String::new(),
            paired_peer_dids: HashSet::new(),
            default_behavior_id,
            behaviors: behaviors
                .into_iter()
                .map(|behavior| (behavior.behavior_id.clone(), behavior))
                .collect(),
            tool_surfaces,
            backend_admission_configs,
            unavailable_behaviors,
            active_schedules: HashMap::new(),
            unavailable_schedules: HashSet::new(),
            active_event_triggers: HashMap::new(),
            unavailable_event_triggers: HashSet::new(),
            active_tasks: HashMap::new(),
        }
    }

    pub(crate) fn with_principal(mut self, principal: Arc<AgentPrincipal>) -> Self {
        self.principal = Some(principal);
        self
    }

    pub(crate) fn with_local_did(mut self, local_did: String) -> Self {
        self.local_did = local_did;
        self
    }

    pub(crate) fn with_paired_peer_dids(mut self, paired_peer_dids: HashSet<String>) -> Self {
        self.paired_peer_dids = paired_peer_dids;
        self
    }

    pub(crate) fn with_schedules(
        mut self,
        active_schedules: HashMap<String, ResolvedSchedule>,
        unavailable_schedules: HashSet<String>,
    ) -> Self {
        self.active_schedules = active_schedules;
        self.unavailable_schedules = unavailable_schedules;
        self
    }

    pub(crate) fn with_event_triggers(
        mut self,
        active_event_triggers: HashMap<String, ResolvedEventTrigger>,
        unavailable_event_triggers: HashSet<String>,
    ) -> Self {
        self.active_event_triggers = active_event_triggers;
        self.unavailable_event_triggers = unavailable_event_triggers;
        self
    }

    pub(crate) fn with_tasks(mut self, tasks: HashMap<String, ResolvedTask>) -> Self {
        self.active_tasks = tasks;
        self
    }

    #[cfg(test)]
    pub(crate) fn activate(
        self,
        generation: u64,
        dispatchers: DispatcherMap,
    ) -> ActiveRuntimeSnapshot {
        let behavior_executor_capacities = dispatchers
            .keys()
            .map(|behavior_id| (behavior_id.clone(), 1))
            .collect();
        let behavior_executor_queue_capacities = dispatchers
            .iter()
            .map(|(behavior_id, dispatcher)| (behavior_id.clone(), dispatcher.max_capacity()))
            .collect();
        self.activate_with_executor_metadata(
            generation,
            dispatchers,
            behavior_executor_capacities,
            behavior_executor_queue_capacities,
        )
    }

    pub(crate) fn activate_with_executor_metadata(
        self,
        generation: u64,
        dispatchers: DispatcherMap,
        behavior_executor_capacities: HashMap<String, usize>,
        behavior_executor_queue_capacities: HashMap<String, usize>,
    ) -> ActiveRuntimeSnapshot {
        debug_assert!(
            self.principal.is_some(),
            "ResolvedRuntimeSnapshot::activate called without principal set — \
             every production construction path must call .with_principal(...) \
             before activation",
        );
        ActiveRuntimeSnapshot {
            generation,
            principal: self.principal,
            local_did: self.local_did,
            paired_peer_dids: self.paired_peer_dids,
            default_behavior_id: self.default_behavior_id,
            behaviors: self.behaviors,
            tool_surfaces: self.tool_surfaces,
            backend_admission_configs: self.backend_admission_configs,
            unavailable_behaviors: self.unavailable_behaviors,
            active_schedules: self.active_schedules,
            unavailable_schedules: self.unavailable_schedules,
            active_event_triggers: self.active_event_triggers,
            unavailable_event_triggers: self.unavailable_event_triggers,
            active_tasks: self.active_tasks,
            dispatchers,
            behavior_executor_capacities,
            behavior_executor_queue_capacities,
        }
    }

    pub(crate) fn configuration_fingerprint(&self) -> String {
        configuration_fingerprint(
            &self.default_behavior_id,
            &self.local_did,
            &self.paired_peer_dids,
            &self.behaviors,
            &self.tool_surfaces,
            &self.backend_admission_configs,
            &self.unavailable_behaviors,
            &self.active_schedules,
            &self.unavailable_schedules,
            &self.active_event_triggers,
            &self.unavailable_event_triggers,
            &self.active_tasks,
        )
    }
}

#[derive(Clone, Debug)]
pub struct ActiveRuntimeSnapshot {
    pub generation: u64,
    pub principal: Option<Arc<AgentPrincipal>>,
    pub local_did: String,
    pub paired_peer_dids: HashSet<String>,
    pub default_behavior_id: String,
    pub behaviors: HashMap<String, Arc<AgentBehavior>>,
    pub tool_surfaces: HashMap<String, Arc<ToolSurface>>,
    pub backend_admission_configs: HashMap<String, BackendAdmissionConfig>,
    pub unavailable_behaviors: HashMap<String, String>,
    pub active_schedules: HashMap<String, ResolvedSchedule>,
    pub unavailable_schedules: HashSet<String>,
    pub active_event_triggers: HashMap<String, ResolvedEventTrigger>,
    pub unavailable_event_triggers: HashSet<String>,
    pub active_tasks: HashMap<String, ResolvedTask>,
    pub dispatchers: DispatcherMap,
    pub behavior_executor_capacities: HashMap<String, usize>,
    pub behavior_executor_queue_capacities: HashMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct BehaviorExecutorStatus {
    pub(crate) worker_capacity: usize,
    pub(crate) queue_depth: usize,
    pub(crate) queue_capacity: usize,
}

impl ActiveRuntimeSnapshot {
    pub(crate) fn behavior(&self, behavior_id: &str) -> Option<&Arc<AgentBehavior>> {
        self.behaviors.get(behavior_id)
    }

    pub(crate) fn active_schedules(&self) -> &HashMap<String, ResolvedSchedule> {
        &self.active_schedules
    }

    pub(crate) fn active_event_triggers(&self) -> &HashMap<String, ResolvedEventTrigger> {
        &self.active_event_triggers
    }

    pub(crate) fn active_tasks(&self) -> &HashMap<String, ResolvedTask> {
        &self.active_tasks
    }

    pub(crate) fn tool_surface(&self, behavior_id: &str) -> Option<&Arc<ToolSurface>> {
        self.tool_surfaces.get(behavior_id)
    }

    pub(crate) fn unavailable_reason(&self, behavior_id: &str) -> Option<&str> {
        self.unavailable_behaviors
            .get(behavior_id)
            .map(String::as_str)
    }

    pub(crate) fn behavior_executor_statuses(&self) -> BTreeMap<String, BehaviorExecutorStatus> {
        let mut behavior_ids = self
            .behaviors
            .keys()
            .chain(self.dispatchers.keys())
            .chain(self.behavior_executor_capacities.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        behavior_ids.extend(self.behavior_executor_queue_capacities.keys().cloned());

        behavior_ids
            .into_iter()
            .map(|behavior_id| {
                let dispatcher = self.dispatchers.get(&behavior_id);
                let queue_capacity = self
                    .behavior_executor_queue_capacities
                    .get(&behavior_id)
                    .copied()
                    .or_else(|| dispatcher.map(mpsc::Sender::max_capacity))
                    .unwrap_or_default();
                let queue_depth = dispatcher
                    .map(|dispatcher| queue_capacity.saturating_sub(dispatcher.capacity()))
                    .unwrap_or_default();
                let worker_capacity = self
                    .behavior_executor_capacities
                    .get(&behavior_id)
                    .copied()
                    .unwrap_or_else(|| if dispatcher.is_some() { 1 } else { 0 });
                (
                    behavior_id,
                    BehaviorExecutorStatus {
                        worker_capacity,
                        queue_depth,
                        queue_capacity,
                    },
                )
            })
            .collect()
    }

    pub(crate) fn configuration_fingerprint(&self) -> String {
        configuration_fingerprint(
            &self.default_behavior_id,
            &self.local_did,
            &self.paired_peer_dids,
            &self.behaviors,
            &self.tool_surfaces,
            &self.backend_admission_configs,
            &self.unavailable_behaviors,
            &self.active_schedules,
            &self.unavailable_schedules,
            &self.active_event_triggers,
            &self.unavailable_event_triggers,
            &self.active_tasks,
        )
    }
}

#[cfg(test)]
pub(crate) fn refresh_active_snapshot(
    active_snapshot: &mut Arc<ActiveRuntimeSnapshot>,
    active_snapshot_rx: &mut watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
) -> bool {
    match active_snapshot_rx.has_changed() {
        Ok(true) => {
            *active_snapshot = active_snapshot_rx.borrow_and_update().clone();
            true
        }
        Ok(false) | Err(_) => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn configuration_fingerprint(
    default_behavior_id: &str,
    local_did: &str,
    paired_peer_dids: &HashSet<String>,
    behaviors: &HashMap<String, Arc<AgentBehavior>>,
    tool_surfaces: &HashMap<String, Arc<ToolSurface>>,
    backend_admission_configs: &HashMap<String, BackendAdmissionConfig>,
    unavailable_behaviors: &HashMap<String, String>,
    active_schedules: &HashMap<String, ResolvedSchedule>,
    unavailable_schedules: &HashSet<String>,
    active_event_triggers: &HashMap<String, ResolvedEventTrigger>,
    unavailable_event_triggers: &HashSet<String>,
    active_tasks: &HashMap<String, ResolvedTask>,
) -> String {
    let mut fingerprint = String::new();
    fingerprint.push_str("local_did:");
    fingerprint.push_str(local_did);
    fingerprint.push('\n');
    fingerprint.push_str("paired_peer_dids:");
    let mut paired = paired_peer_dids.iter().collect::<Vec<_>>();
    paired.sort();
    for did in paired {
        fingerprint.push_str(did);
        fingerprint.push(',');
    }
    fingerprint.push('\n');
    fingerprint.push_str("default:");
    fingerprint.push_str(default_behavior_id);
    fingerprint.push('\n');

    let mut behavior_ids = behaviors.keys().cloned().collect::<Vec<_>>();
    behavior_ids.sort();
    for behavior_id in behavior_ids {
        let behavior = behaviors
            .get(&behavior_id)
            .expect("behavior id came from behaviors map");
        fingerprint.push_str("behavior:");
        fingerprint.push_str(&behavior_id);
        fingerprint.push('=');
        fingerprint.push_str(&format!("{behavior:?}"));
        fingerprint.push('\n');
    }

    let mut tool_ids = tool_surfaces.keys().cloned().collect::<Vec<_>>();
    tool_ids.sort();
    for behavior_id in tool_ids {
        let tool_surface = tool_surfaces
            .get(&behavior_id)
            .expect("behavior id came from tool surface map");
        fingerprint.push_str("tools:");
        fingerprint.push_str(&behavior_id);
        fingerprint.push('=');
        fingerprint.push_str(&format!("{tool_surface:?}"));
        fingerprint.push('\n');
    }

    let mut backend_ids = backend_admission_configs
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    backend_ids.sort();
    for backend_id in backend_ids {
        let config = backend_admission_configs
            .get(&backend_id)
            .expect("backend id came from backend admission config map");
        fingerprint.push_str("backend_admission:");
        fingerprint.push_str(&backend_id);
        fingerprint.push('=');
        fingerprint.push_str(&format!("{config:?}"));
        fingerprint.push('\n');
    }

    let mut unavailable_ids = unavailable_behaviors.keys().cloned().collect::<Vec<_>>();
    unavailable_ids.sort();
    for behavior_id in unavailable_ids {
        let reason = unavailable_behaviors
            .get(&behavior_id)
            .expect("behavior id came from unavailable behavior map");
        fingerprint.push_str("unavailable:");
        fingerprint.push_str(&behavior_id);
        fingerprint.push('=');
        fingerprint.push_str(reason);
        fingerprint.push('\n');
    }

    let mut schedule_ids = active_schedules.keys().cloned().collect::<Vec<_>>();
    schedule_ids.sort();
    for schedule_id in schedule_ids {
        let schedule = active_schedules
            .get(&schedule_id)
            .expect("schedule id came from active schedules map");
        fingerprint.push_str("schedule:");
        fingerprint.push_str(&schedule_id);
        fingerprint.push('=');
        fingerprint.push_str(&format!("{schedule:?}"));
        fingerprint.push('\n');
    }

    let mut unavailable_schedule_ids = unavailable_schedules.iter().cloned().collect::<Vec<_>>();
    unavailable_schedule_ids.sort();
    for schedule_id in unavailable_schedule_ids {
        fingerprint.push_str("unavailable_schedule:");
        fingerprint.push_str(&schedule_id);
        fingerprint.push('\n');
    }

    let mut event_trigger_ids = active_event_triggers.keys().cloned().collect::<Vec<_>>();
    event_trigger_ids.sort();
    for trigger_id in event_trigger_ids {
        let trigger = active_event_triggers
            .get(&trigger_id)
            .expect("event trigger id came from active event triggers map");
        fingerprint.push_str("event_trigger:");
        fingerprint.push_str(&trigger_id);
        fingerprint.push('=');
        fingerprint.push_str(&format!("{trigger:?}"));
        fingerprint.push('\n');
    }

    let mut unavailable_event_trigger_ids = unavailable_event_triggers
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    unavailable_event_trigger_ids.sort();
    for trigger_id in unavailable_event_trigger_ids {
        fingerprint.push_str("unavailable_event_trigger:");
        fingerprint.push_str(&trigger_id);
        fingerprint.push('\n');
    }

    let mut task_ids = active_tasks.keys().cloned().collect::<Vec<_>>();
    task_ids.sort();
    for task_id in task_ids {
        let task = active_tasks
            .get(&task_id)
            .expect("task id came from active tasks map");
        fingerprint.push_str("task:");
        fingerprint.push_str(&task_id);
        fingerprint.push('=');
        fingerprint.push_str(&format!("{task:?}"));
        fingerprint.push('\n');
    }

    fingerprint
}

#[cfg(test)]
mod tests;
