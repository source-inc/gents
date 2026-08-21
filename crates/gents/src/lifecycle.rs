use std::sync::Arc;

use anyhow::Result;
use defra_node::EmbeddedNode;

use crate::graphql::{escape_graphql_string, response_has_documents};
use crate::session;
use crate::watcher::AgentRequest;

mod background_wake_recovery;
pub use background_wake_recovery::{background_wake_next_retry_at, background_wake_retry_delay};
mod claim;
mod lookup;
pub mod manual;
pub(crate) mod materialize;
mod query;
pub(crate) mod queue;
mod recovery;
mod rows;
mod task_title;
mod transition;

pub use manual::{write_manual_agent_request, write_manual_agent_request_with_conversation_title};
pub(crate) use materialize::{
    write_pending_agent_request_with_lineage_and_conversation_title,
    write_pending_agent_request_with_lineage_workspace_and_conversation_title,
};
pub use task_title::task_run_conversation_title;

pub const DEFAULT_REQUEST_MAX_RETRIES: u32 = 3;

pub fn is_background_completion_request(metadata: Option<&str>) -> bool {
    queue::is_automated_wakeup(metadata)
}

/// Legacy runtimes persisted unversioned background-completion wakeups as
/// scheduled requests. They remain durable audit rows but must be ignored;
/// current versioned completion wakes are authoritative continuation turns.
pub fn is_deprecated_background_completion_request(
    execution_origin: Option<&str>,
    metadata: Option<&str>,
) -> bool {
    queue::is_deprecated_background_completion_wakeup(execution_origin, metadata)
}

fn graphql_retry_root_request(retry_root_request: Option<&str>, request_id: &str) -> String {
    escape_graphql_string(retry_root_request.unwrap_or(request_id))
}

fn extract_single_doc_id(response: &defra_node::QueryResponse, key: &str) -> Option<String> {
    // DefraDB's GraphQL surface accepts `create_<Collection>` while the
    // response data may expose the normalized `add_<Collection>` field. Read
    // both so callers keep the exact create result instead of falling back to
    // a non-unique logical-ID lookup.
    let normalized_add_key = key
        .strip_prefix("create_")
        .map(|collection| format!("add_{collection}"));
    response
        .data
        .as_ref()
        .and_then(|data| {
            data.get(key).or_else(|| {
                normalized_add_key
                    .as_deref()
                    .and_then(|normalized| data.get(normalized))
            })
        })
        .and_then(|value| {
            value
                .get("_docID")
                .and_then(|doc_id| doc_id.as_str())
                .or_else(|| {
                    value
                        .as_array()
                        .and_then(|rows| rows.first())
                        .and_then(|row| row.get("_docID"))
                        .and_then(|doc_id| doc_id.as_str())
                })
                .map(ToOwned::to_owned)
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalLifecycleState {
    Pending,
    Claimed,
    Streaming,
    Completed,
    Failed,
    Interrupted,
    Dead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    Claimed,
    Queued,
    Interrupted,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionOrigin {
    Interactive,
    Scheduled,
}

impl ExecutionOrigin {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Scheduled => "scheduled",
        }
    }

    pub(crate) fn from_persisted(value: Option<&str>) -> Self {
        match value {
            Some("scheduled") => Self::Scheduled,
            _ => Self::Interactive,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TriggerLineage {
    pub trigger_id: Option<String>,
    pub trigger_kind: Option<String>,
    pub source_doc_id: Option<String>,
    pub correlation: Option<String>,
    pub trigger_context: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkspaceLineage {
    pub workspace_id: Option<String>,
    pub workspace_authority: Option<String>,
    pub workspace_owner_deployment_id: Option<String>,
    pub workspace_seal_hash: Option<String>,
}

impl WorkspaceLineage {
    pub fn from_trigger_context(trigger_context: Option<&str>) -> Result<Self> {
        let context = TriggerExecutionContext::parse(trigger_context)?;
        let field = |name: &str| {
            context
                .source_fields
                .get(name)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        Ok(Self {
            workspace_id: field("workspace_id"),
            workspace_authority: field("workspace_authority"),
            workspace_owner_deployment_id: field("workspace_owner_deployment_id"),
            workspace_seal_hash: field("workspace_seal_hash"),
        })
    }

    pub fn is_bound(&self) -> bool {
        self.workspace_id
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
            || self
                .workspace_owner_deployment_id
                .as_deref()
                .map(str::trim)
                .is_some_and(|value| !value.is_empty())
    }
}

pub const MAX_TRIGGER_CONTEXT_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TriggerExecutionContext {
    pub version: u8,
    #[serde(default)]
    pub source_fields: std::collections::BTreeMap<String, String>,
}

impl TriggerExecutionContext {
    pub fn parse(value: Option<&str>) -> anyhow::Result<Self> {
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(Self::default());
        };
        if value.len() > MAX_TRIGGER_CONTEXT_BYTES {
            anyhow::bail!("trigger execution context exceeds {MAX_TRIGGER_CONTEXT_BYTES} bytes");
        }
        let context: Self = serde_json::from_str(value)?;
        if context.version != 1 {
            anyhow::bail!(
                "unsupported trigger execution context version {}",
                context.version
            );
        }
        Ok(context)
    }
}

pub(crate) fn inherited_trigger_context_graphql_fields(
    correlation: Option<&str>,
    trigger_context: Option<&str>,
) -> anyhow::Result<String> {
    let correlation = correlation
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            format!(
                "\n                caused_by_correlation: \"{}\",",
                escape_graphql_string(value)
            )
        })
        .unwrap_or_default();
    let trigger_context = trigger_context
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            TriggerExecutionContext::parse(Some(value))?;
            Ok::<_, anyhow::Error>(format!(
                "\n                caused_by_trigger_context: \"{}\",",
                escape_graphql_string(value)
            ))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(format!("{correlation}{trigger_context}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum PersistedLifecycleState {
    Pending,
    Claimed,
    Processing,
    InputRequired,
    Completed,
    Failed,
    Superseded,
    Dead,
    Interrupted,
}

impl PersistedLifecycleState {
    const ALL: [Self; 9] = [
        Self::Pending,
        Self::Claimed,
        Self::Processing,
        Self::InputRequired,
        Self::Completed,
        Self::Failed,
        Self::Superseded,
        Self::Dead,
        Self::Interrupted,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::Processing => "processing",
            Self::InputRequired => "inputRequired",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Superseded => "superseded",
            Self::Dead => "dead",
            Self::Interrupted => "interrupted",
        }
    }

    const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Superseded | Self::Dead | Self::Interrupted
        )
    }

    fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "claimed" => Some(Self::Claimed),
            "processing" => Some(Self::Processing),
            "inputRequired" => Some(Self::InputRequired),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "superseded" => Some(Self::Superseded),
            "dead" => Some(Self::Dead),
            "interrupted" => Some(Self::Interrupted),
            _ => None,
        }
    }

    #[cfg(test)]
    const fn is_nonterminal(self) -> bool {
        !self.is_terminal()
    }

    const fn is_active_runtime(self) -> bool {
        matches!(self, Self::Pending | Self::Claimed | Self::Processing)
    }
}

fn lifecycle_state_graphql_list(
    states: impl IntoIterator<Item = PersistedLifecycleState>,
) -> String {
    let states = states
        .into_iter()
        .map(|state| format!(r#""{}""#, state.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{states}]")
}

#[cfg(test)]
pub(crate) fn nonterminal_lifecycle_state_graphql_list() -> String {
    lifecycle_state_graphql_list(
        PersistedLifecycleState::ALL
            .iter()
            .copied()
            .filter(|state| state.is_nonterminal()),
    )
}

pub(crate) fn active_runtime_lifecycle_state_graphql_list() -> String {
    lifecycle_state_graphql_list(
        PersistedLifecycleState::ALL
            .iter()
            .copied()
            .filter(|state| state.is_active_runtime()),
    )
}

pub(crate) fn stuck_request_lifecycle_state_graphql_list() -> String {
    lifecycle_state_graphql_list([
        PersistedLifecycleState::Claimed,
        PersistedLifecycleState::Processing,
    ])
}

pub(crate) fn terminal_lifecycle_state_graphql_list() -> String {
    lifecycle_state_graphql_list(
        PersistedLifecycleState::ALL
            .iter()
            .copied()
            .filter(|state| state.is_terminal()),
    )
}

fn lifecycle_state_graphql_list_for(states: &[PersistedLifecycleState]) -> String {
    lifecycle_state_graphql_list(states.iter().copied())
}

pub struct RequestLifecycle {
    node: Arc<EmbeddedNode>,
    agent_name: String,
    agent_did: String,
    behavior_id: String,
    execution_origin: ExecutionOrigin,
    backend_id: String,
    failure_reason: Option<String>,
    request: AgentRequest,
    request_commit_cid: Option<String>,
    response_doc_id: Option<String>,
    progress_seq: u32,
    deadline_duration_secs: u64,
    claimed_deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    background_completion_input_through_sequence: Option<u32>,
    state: LocalLifecycleState,
    valid_until_at_claim: Option<chrono::DateTime<chrono::Utc>>,
}

impl RequestLifecycle {
    pub(crate) fn claimed_deadline_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.claimed_deadline_at
    }

    /// Exact composite commit produced by the successful claim/materialization
    /// mutation. This is the version whose request fields the runtime uses.
    pub(crate) fn request_commit_cid(&self) -> Option<&str> {
        self.request_commit_cid.as_deref()
    }

    pub fn valid_until_at_claim_for_test(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.valid_until_at_claim
    }

    /// Last transcript sequence owned by this background-completion attempt
    /// at its atomic claim boundary. Messages appended for a successor epoch
    /// must not enter this attempt's provider input.
    pub(crate) fn background_completion_input_through_sequence(&self) -> Option<u32> {
        self.background_completion_input_through_sequence
    }
}

#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub requests_recovered: usize,
    pub background_wakes_redriven: usize,
    pub responses_recovered: usize,
    pub conversations_recovered: usize,
    pub conversations_failed: usize,
    pub duplicate_conversation_sessions: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TerminalRepairReport {
    pub scanned: usize,
    pub repaired: usize,
    pub awaiting_outcome: usize,
    pub failed: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct BackgroundWakeRedriveReport {
    pub scanned: usize,
    pub redriven: usize,
    pub deferred: usize,
    pub already_redriven: usize,
    pub coalesced: usize,
    pub ineligible: usize,
    pub failed: usize,
}

impl BackgroundWakeRedriveReport {
    pub fn is_noop(&self) -> bool {
        self.redriven == 0
    }
}

impl TerminalRepairReport {
    pub fn is_noop(&self) -> bool {
        self.repaired == 0
    }
}

pub const TERMINAL_REDRIVE_CAP: u32 = 3;

pub const TERMINAL_REDRIVE_BATCH_LIMIT: usize = 64;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TerminalRedriveReport {
    pub reasserted: usize,
    pub scanned: usize,
    pub failed: usize,
}

impl TerminalRedriveReport {
    pub fn is_noop(&self) -> bool {
        self.reasserted == 0
    }
}

fn resolve_behavior_id(default_behavior_id: &str, requested_behavior_id: Option<&str>) -> String {
    requested_behavior_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_behavior_id)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lean_vocab_test::{
        assert_lean_contract_vocabulary_matches, assert_state_machine_contract_is_complete,
        lean_state_machine_contract, LeanContractVocabulary,
    };

    #[test]
    fn rust_request_lifecycle_state_vocabulary_matches_lean_model() {
        let rust_states = PersistedLifecycleState::ALL
            .iter()
            .copied()
            .map(PersistedLifecycleState::as_str)
            .collect::<Vec<_>>();
        assert_lean_contract_vocabulary_matches(LeanContractVocabulary {
            domain: "RequestState",
            rust_source: "PersistedLifecycleState::ALL",
            rust_values: &rust_states,
        });
    }

    #[test]
    fn rust_execution_origin_vocabulary_matches_lean_model() {
        let rust_origins = vec![
            ExecutionOrigin::Interactive.as_str(),
            ExecutionOrigin::Scheduled.as_str(),
        ];
        assert_lean_contract_vocabulary_matches(LeanContractVocabulary {
            domain: "ExecutionOrigin",
            rust_source: "ExecutionOrigin::{Interactive, Scheduled}",
            rust_values: &rust_origins,
        });
    }

    #[test]
    fn request_state_machine_contract_is_complete() {
        assert_state_machine_contract_is_complete("Request");
    }

    #[test]
    fn persisted_lifecycle_terminal_partition_matches_lean_contract() {
        let request_machine = lean_state_machine_contract("Request");
        let nonterminal = PersistedLifecycleState::ALL
            .iter()
            .copied()
            .filter(|state| state.is_nonterminal())
            .map(|state| state.as_str())
            .collect::<Vec<_>>();
        let terminal = PersistedLifecycleState::ALL
            .iter()
            .copied()
            .filter(|state| state.is_terminal())
            .map(|state| state.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            nonterminal,
            request_machine
                .nonterminal_states
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            terminal,
            request_machine
                .terminal_states
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        assert!(PersistedLifecycleState::InputRequired.is_nonterminal());
        assert!(!PersistedLifecycleState::InputRequired.is_active_runtime());
        assert!(PersistedLifecycleState::Interrupted.is_terminal());
        let expected_nonterminal_graphql_list = format!(
            "[{}]",
            request_machine
                .nonterminal_states
                .iter()
                .map(|state| format!(r#""{state}""#))
                .collect::<Vec<_>>()
                .join(", ")
        );
        assert_eq!(
            nonterminal_lifecycle_state_graphql_list(),
            expected_nonterminal_graphql_list
        );
        assert_eq!(
            active_runtime_lifecycle_state_graphql_list(),
            r#"["pending", "claimed", "processing"]"#
        );
        assert_eq!(
            ExecutionOrigin::from_persisted(Some("scheduled")),
            ExecutionOrigin::Scheduled
        );
        assert_eq!(
            ExecutionOrigin::from_persisted(Some("interactive")),
            ExecutionOrigin::Interactive
        );
        assert_eq!(
            ExecutionOrigin::from_persisted(None),
            ExecutionOrigin::Interactive
        );
    }
}
