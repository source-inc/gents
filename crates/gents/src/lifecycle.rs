use std::sync::Arc;

use anyhow::Result;
use defra_node::EmbeddedNode;

use crate::graphql::{escape_graphql_string, response_has_documents};
use crate::session;
use crate::watcher::AgentRequest;

mod claim;
pub(crate) use claim::{
    reconstruct_execution_provenance_from_claim_ancestry, verify_persisted_execution_provenance,
};
#[doc(hidden)]
pub mod ingest_contract;
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
pub(crate) use materialize::write_pending_agent_request_with_lineage_and_conversation_title;
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

fn extract_single_doc_id(response: &defra_node::QueryResponse, key: &str) -> Option<String> {
    response
        .data
        .as_ref()
        .and_then(|data| data.get(key))
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
    request_version: Option<crate::DocumentVersionRef>,
    execution_provenance: Option<crate::RequestExecutionProvenance>,
    response_doc_id: Option<String>,
    progress_seq: u32,
    deadline_duration_secs: u64,
    claimed_deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    state: LocalLifecycleState,
    valid_until_at_claim: Option<chrono::DateTime<chrono::Utc>>,
}

impl RequestLifecycle {
    fn require_execution_provenance(
        &self,
        operation: &str,
    ) -> anyhow::Result<&crate::RequestExecutionProvenance> {
        self.execution_provenance.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "cannot {operation} request {} without verified source-and-claim execution provenance",
                self.request.request_id
            )
        })
    }

    fn require_active_execution_provenance(&self, operation: &str) -> anyhow::Result<()> {
        if matches!(
            self.state,
            LocalLifecycleState::Claimed | LocalLifecycleState::Streaming
        ) {
            self.require_execution_provenance(operation)?;
        }
        Ok(())
    }

    pub(crate) fn claimed_deadline_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.claimed_deadline_at
    }

    pub fn valid_until_at_claim_for_test(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.valid_until_at_claim
    }

    pub fn request_version(&self) -> Option<&crate::DocumentVersionRef> {
        self.request_version.as_ref()
    }

    pub fn execution_provenance(&self) -> Option<&crate::RequestExecutionProvenance> {
        self.execution_provenance.as_ref()
    }
}

#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub requests_recovered: usize,
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
