//! Tool-call lifecycle state machine.
//!
//! Mirrors `crates/gents/src/lifecycle.rs` (`RequestLifecycle`) for tool
//! calls. Defines the persisted vocabulary, failure-class enum, and the
//! `ToolCallLifecycle` struct that owns every persistence write.
//!
//! Lifecycle is daemon-visible only; subprocess kill mechanics, output
//! streaming, and persistent processes are out of scope.
//!
//! ## R2 maintenance obligations
//!
//! This module implements R2 ("Rust subagent data plane"). Per the spec at
//! `docs/superpowers/specs/2026-05-08-r2-rust-subagent-data-plane-design.md` (removed from the tree; see git history):
//!
//! - SubagentSource (R3) consumes `create_subagent_request` and the bridge methods.
//! - Agent-facing tools (R4) are routed via hook integration that uses
//!   `new_subagent` and recognizes spawn_subagent / wait_task / etc. tool names.
//! - Cross-reference validation (target resolution, parent existence) is wired
//!   by R3's `SubagentSource` work.
//! - Cross-principal delegation (R6) lands with source-inc/gents#9.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallState {
    Pending,
    AwaitingApproval,
    Running,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
}

impl ToolCallState {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 7] = [
        Self::Pending,
        Self::AwaitingApproval,
        Self::Running,
        Self::Completed,
        Self::Failed,
        Self::TimedOut,
        Self::Cancelled,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::AwaitingApproval => "awaitingApproval",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timedOut",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "awaitingApproval" => Some(Self::AwaitingApproval),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "timedOut" => Some(Self::TimedOut),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    pub(crate) const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::TimedOut | Self::Cancelled
        )
    }

    #[cfg(test)]
    pub(crate) const fn is_cancellable(self) -> bool {
        matches!(self, Self::Pending | Self::AwaitingApproval | Self::Running)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FailureClass {
    ApprovalDenied,
    ArgumentInvalid,
    ServiceUnavailable,
    Transport,
    ToolReturnedError,
    PolicyDenied,
    External,
}

impl FailureClass {
    pub const ALL: [Self; 7] = [
        Self::ApprovalDenied,
        Self::ArgumentInvalid,
        Self::ServiceUnavailable,
        Self::Transport,
        Self::ToolReturnedError,
        Self::PolicyDenied,
        Self::External,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApprovalDenied => "approvalDenied",
            Self::ArgumentInvalid => "argumentInvalid",
            Self::ServiceUnavailable => "serviceUnavailable",
            Self::Transport => "transport",
            Self::ToolReturnedError => "toolReturnedError",
            Self::PolicyDenied => "policyDenied",
            Self::External => "external",
        }
    }

    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "approvalDenied" => Some(Self::ApprovalDenied),
            "argumentInvalid" => Some(Self::ArgumentInvalid),
            "serviceUnavailable" => Some(Self::ServiceUnavailable),
            "transport" => Some(Self::Transport),
            "toolReturnedError" => Some(Self::ToolReturnedError),
            "policyDenied" => Some(Self::PolicyDenied),
            "external" => Some(Self::External),
            _ => None,
        }
    }
}

/// Whether the parent's narrative is blocked on this tool's terminal state.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum AwaitMode {
    #[default]
    Foreground,
    Background,
}

impl AwaitMode {
    pub fn as_str(self) -> &'static str {
        match self {
            AwaitMode::Foreground => "foreground",
            AwaitMode::Background => "background",
        }
    }

    pub fn from_persisted(s: &str) -> Option<Self> {
        match s {
            "foreground" => Some(AwaitMode::Foreground),
            "background" => Some(AwaitMode::Background),
            _ => None,
        }
    }

    pub const ALL: &'static [AwaitMode] = &[AwaitMode::Foreground, AwaitMode::Background];
}

/// Whether parent termination drives the linked child request to .interrupted
/// (cascade) or detaches the child to its own deadline.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CancelPolicy {
    Cascade,
    Detach,
}

impl CancelPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            CancelPolicy::Cascade => "cascade",
            CancelPolicy::Detach => "detach",
        }
    }

    pub fn from_persisted(s: &str) -> Option<Self> {
        match s {
            "cascade" => Some(CancelPolicy::Cascade),
            "detach" => Some(CancelPolicy::Detach),
            _ => None,
        }
    }

    pub const ALL: &'static [CancelPolicy] = &[CancelPolicy::Cascade, CancelPolicy::Detach];
}

/// Why a tool-call cancellation was requested at the state-machine boundary.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CancelCause {
    Interrupted,
    Deadline,
    UserCancelled,
}

impl CancelCause {
    pub fn as_str(self) -> &'static str {
        match self {
            CancelCause::Interrupted => "interrupted",
            CancelCause::Deadline => "deadline",
            CancelCause::UserCancelled => "userCancelled",
        }
    }

    pub fn from_persisted(s: &str) -> Option<Self> {
        match s {
            "interrupted" => Some(CancelCause::Interrupted),
            "deadline" => Some(CancelCause::Deadline),
            "userCancelled" => Some(CancelCause::UserCancelled),
            _ => None,
        }
    }

    pub const ALL: &'static [CancelCause] = &[
        CancelCause::Interrupted,
        CancelCause::Deadline,
        CancelCause::UserCancelled,
    ];
}

/// The four non-.completed terminal states a child AgentRequest can reach.
/// Used as the argument shape to bridge_failure to project the child terminal
/// onto a parent ToolCallState (.failed for most, .cancelled for .interrupted).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChildTerminal {
    Failed {
        reason: String,
        failure_class: FailureClass,
    },
    Dead,
    Interrupted,
    Superseded,
}

impl ChildTerminal {
    /// Lean B2 projection: .interrupted → .cancelled, all others → .failed.
    pub fn projected_state(&self) -> ToolCallState {
        match self {
            ChildTerminal::Interrupted => ToolCallState::Cancelled,
            _ => ToolCallState::Failed,
        }
    }

    /// Persisted vocabulary names for conformance enumeration.
    pub const ALL_KIND: &'static [&'static str] = &["failed", "dead", "interrupted", "superseded"];
}

/// Returned by `bridge_cancel_cascade` (wrapped in Option). The caller — typically
/// R3's daemon interrupt dispatcher — performs the actual write to the child
/// AgentRequest's interrupt_requested_at field. Returning None from
/// bridge_cancel_cascade means no cascade is required: the bridge tool is
/// native (no child link), detached (no cascade), or not in .cancelled state.
#[derive(Clone, Debug)]
pub struct CascadeIntent {
    pub child_request_id: String,
    pub at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug)]
pub enum CascadeDispatch {
    Local(CascadeIntent),
    RemoteIntentWritten,
}

use std::sync::Arc;

use defra_node::EmbeddedNode;

pub(crate) mod query;
mod recovery;
pub(crate) mod runtime;
pub mod subagent_request;
mod transition;

pub use recovery::{
    BackgroundCompletionSideEffectReport, OrphanedBackgroundToolReport, SubagentLivenessReport,
    TerminalParentToolReport, ToolCallRecoveryReport,
};
pub use runtime::ToolOutcome;
pub use subagent_request::{
    create_subagent_request, create_subagent_request_with_request_id_for_test,
    create_subagent_request_with_trusted_parent_request_id_for_test, MAX_SUBAGENT_DEPTH,
};
pub use transition::IllegalToolCallTransition;

/// State machine struct for an individual tool call. Mirrors `RequestLifecycle`
/// from `lifecycle.rs:189-204`. Owns every persistence write for a single
/// AgentToolCall row.
pub struct ToolCallLifecycle {
    node: Arc<EmbeddedNode>,
    request_id: String,
    session_id: String,
    /// DID of the agent that owns the session this tool call belongs to. Stamped
    /// onto the AgentToolCall row at create so filtered replication can scope the
    /// collection to one agent (`@immutable` scope key).
    agent_did: String,
    requester_did: Option<String>,
    tool_call_id: String,
    message_sequence: u32,
    tool_name: String,
    args: String,
    doc_id: Option<String>,
    deadline_at: chrono::DateTime<chrono::Utc>,
    state: ToolCallState,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
    failure_class: Option<FailureClass>,
    cancel_cause: Option<CancelCause>,
    pub(crate) await_mode: AwaitMode,
    pub(crate) cancel_policy: CancelPolicy,
    pub(crate) child_request_id: Option<String>,
    pub(crate) spawn_target_did: Option<String>,
    pub(crate) unclaimed_deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) workflow_group_id: Option<String>,
    pub(crate) workflow_role: Option<String>,
}

impl ToolCallLifecycle {
    /// Construct a new lifecycle. Does NOT persist; the first transition
    /// method (`start_running`) creates the DefraDB row.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node: Arc<EmbeddedNode>,
        request_id: String,
        session_id: String,
        agent_did: String,
        tool_call_id: String,
        message_sequence: u32,
        tool_name: String,
        args: String,
        deadline_at: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            node,
            request_id,
            session_id,
            agent_did,
            requester_did: None,
            tool_call_id,
            message_sequence,
            tool_name,
            args,
            doc_id: None,
            deadline_at,
            state: ToolCallState::Pending,
            started_at: None,
            failure_class: None,
            cancel_cause: None,
            await_mode: AwaitMode::Foreground,
            cancel_policy: CancelPolicy::Cascade,
            child_request_id: None,
            spawn_target_did: None,
            unclaimed_deadline_at: None,
            workflow_group_id: None,
            workflow_role: None,
        }
    }

    pub(crate) fn with_requester_did(mut self, requester_did: Option<String>) -> Self {
        self.requester_did = requester_did.and_then(|did| {
            let did = did.trim();
            (!did.is_empty()).then(|| did.to_string())
        });
        self
    }

    /// Constructor for the subagent invocation path. Sets child_request_id (the
    /// link to the spawned child AgentRequest) and lets the caller pick await_mode
    /// and cancel_policy. Synchronous and does not persist — first transition
    /// (typically start_running) creates the row.
    #[allow(clippy::too_many_arguments)]
    pub fn new_subagent(
        node: Arc<EmbeddedNode>,
        request_id: String,
        session_id: String,
        agent_did: String,
        tool_call_id: String,
        message_sequence: u32,
        tool_name: String,
        args: String,
        deadline_at: chrono::DateTime<chrono::Utc>,
        await_mode: AwaitMode,
        cancel_policy: CancelPolicy,
        child_request_id: String,
        spawn_target_did: String,
    ) -> Self {
        Self {
            node,
            request_id,
            session_id,
            agent_did,
            requester_did: None,
            tool_call_id,
            message_sequence,
            tool_name,
            args,
            doc_id: None,
            deadline_at,
            state: ToolCallState::Pending,
            started_at: None,
            failure_class: None,
            cancel_cause: None,
            await_mode,
            cancel_policy,
            child_request_id: Some(child_request_id),
            spawn_target_did: Some(spawn_target_did),
            unclaimed_deadline_at: None,
            workflow_group_id: None,
            workflow_role: None,
        }
    }

    /// Constructor for an ordinary tool launched through the R6 background
    /// bridge. The row is a bridge row even though it has no child request.
    #[allow(clippy::too_many_arguments)]
    pub fn new_background_tool(
        node: Arc<EmbeddedNode>,
        request_id: String,
        session_id: String,
        agent_did: String,
        tool_call_id: String,
        message_sequence: u32,
        tool_name: String,
        args: String,
        deadline_at: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            node,
            request_id,
            session_id,
            agent_did,
            requester_did: None,
            tool_call_id,
            message_sequence,
            tool_name,
            args,
            doc_id: None,
            deadline_at,
            state: ToolCallState::Pending,
            started_at: None,
            failure_class: None,
            cancel_cause: None,
            await_mode: AwaitMode::Background,
            cancel_policy: CancelPolicy::Cascade,
            child_request_id: None,
            spawn_target_did: None,
            unclaimed_deadline_at: None,
            workflow_group_id: None,
            workflow_role: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_doc_id(&mut self, doc_id: Option<String>) {
        self.doc_id = doc_id;
    }

    pub(crate) fn deadline_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.deadline_at
    }

    pub(crate) fn is_subagent_bridge(&self) -> bool {
        self.child_request_id.is_some()
    }

    pub(crate) fn is_background_tool_bridge(&self) -> bool {
        self.child_request_id.is_none() && self.await_mode == AwaitMode::Background
    }

    pub(crate) fn is_bridge(&self) -> bool {
        self.is_subagent_bridge() || self.is_background_tool_bridge()
    }

    pub(crate) fn terminal_persistence_status(&self, completion_reason: Option<&str>) -> String {
        if self.is_background_tool_bridge() {
            completion_reason
                .map(|reason| format!("completionPending:{reason}"))
                .unwrap_or_else(|| "completionPending".to_string())
        } else {
            "completed".to_string()
        }
    }

    pub(crate) fn await_mode(&self) -> AwaitMode {
        self.await_mode
    }

    pub(crate) fn state(&self) -> ToolCallState {
        self.state
    }

    pub(crate) fn request_id(&self) -> &str {
        &self.request_id
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn agent_did(&self) -> &str {
        &self.agent_did
    }

    pub(crate) fn requester_did(&self) -> Option<&str> {
        self.requester_did.as_deref()
    }

    pub(crate) fn tool_name(&self) -> &str {
        &self.tool_name
    }

    pub(crate) fn tool_call_id(&self) -> &str {
        &self.tool_call_id
    }

    /// Physical DefraDB identity of the persisted AgentToolCall row.
    ///
    /// Runtime side channels such as live-output telemetry must retain this
    /// identity instead of re-resolving a row from the provider call id, which
    /// is only a logical identifier and may collide in another session.
    pub(crate) fn doc_id(&self) -> Option<&str> {
        self.doc_id.as_deref()
    }

    pub(crate) fn is_running(&self) -> bool {
        self.state == ToolCallState::Running
    }

    pub(crate) fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state == ToolCallState::Cancelled
    }

    #[cfg(test)]
    pub(crate) fn set_state(&mut self, state: ToolCallState) {
        self.state = state;
    }

    #[cfg(test)]
    pub(crate) fn set_started_at(&mut self, t: Option<chrono::DateTime<chrono::Utc>>) {
        self.started_at = t;
    }

    pub(crate) fn set_unclaimed_deadline_at(
        &mut self,
        deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    ) {
        self.unclaimed_deadline_at = deadline_at;
    }

    pub(crate) fn set_workflow_group(
        &mut self,
        group_id: impl Into<String>,
        role: impl Into<String>,
    ) {
        self.workflow_group_id = Some(group_id.into());
        self.workflow_role = Some(role.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_persisted_vocabulary() {
        for state in ToolCallState::ALL {
            assert_eq!(ToolCallState::from_persisted(state.as_str()), Some(state));
        }
        assert_eq!(ToolCallState::from_persisted("called"), None);
        assert_eq!(ToolCallState::from_persisted("unknown"), None);
    }

    #[test]
    fn cancellable_iff_non_terminal() {
        for state in ToolCallState::ALL {
            assert_eq!(state.is_cancellable(), !state.is_terminal());
        }
    }

    #[test]
    fn all_lists_seven_states() {
        assert_eq!(ToolCallState::ALL.len(), 7);
    }

    #[test]
    fn failure_class_round_trip_persisted_vocabulary() {
        for fc in FailureClass::ALL {
            assert_eq!(FailureClass::from_persisted(fc.as_str()), Some(fc));
        }
        assert_eq!(FailureClass::from_persisted("unknown"), None);
    }

    #[test]
    fn failure_class_all_lists_seven_variants() {
        assert_eq!(FailureClass::ALL.len(), 7);
    }

    #[test]
    fn lifecycle_new_signature_compiles() {
        // Compile-only sanity test: behavior verified in Bucket 3 integration tests.
        let _: fn(
            std::sync::Arc<defra_node::EmbeddedNode>,
            String,
            String,
            String,
            String,
            u32,
            String,
            String,
            chrono::DateTime<chrono::Utc>,
        ) -> ToolCallLifecycle = ToolCallLifecycle::new;
    }

    use crate::lean_vocab_test::{
        assert_lean_contract_vocabulary_matches, assert_state_machine_contract_is_complete,
        lean_state_machine_contract, LeanContractVocabulary,
    };

    #[test]
    fn rust_tool_call_state_vocabulary_matches_lean_model() {
        let rust_states = ToolCallState::ALL
            .iter()
            .copied()
            .map(ToolCallState::as_str)
            .collect::<Vec<_>>();
        assert_lean_contract_vocabulary_matches(LeanContractVocabulary {
            domain: "ToolCallState",
            rust_source: "ToolCallState::ALL",
            rust_values: &rust_states,
        });
    }

    #[test]
    fn rust_cancel_cause_vocabulary_matches_lean_model() {
        let rust_causes = CancelCause::ALL
            .iter()
            .copied()
            .map(CancelCause::as_str)
            .collect::<Vec<_>>();
        assert_lean_contract_vocabulary_matches(LeanContractVocabulary {
            domain: "CancelCause",
            rust_source: "CancelCause::ALL",
            rust_values: &rust_causes,
        });
    }

    #[test]
    fn rust_failure_class_vocabulary_matches_lean_model() {
        let rust_classes = FailureClass::ALL
            .iter()
            .copied()
            .map(FailureClass::as_str)
            .collect::<Vec<_>>();
        assert_lean_contract_vocabulary_matches(LeanContractVocabulary {
            domain: "ToolFailureClass",
            rust_source: "FailureClass::ALL",
            rust_values: &rust_classes,
        });
    }

    #[test]
    fn tool_call_state_machine_contract_is_complete() {
        assert_state_machine_contract_is_complete("ToolCall");
    }

    #[test]
    fn tool_call_terminal_partition_matches_lean_contract() {
        let machine = lean_state_machine_contract("ToolCall");
        let terminal = ToolCallState::ALL
            .iter()
            .copied()
            .filter(|s| s.is_terminal())
            .map(ToolCallState::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            terminal,
            machine
                .terminal_states
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
    }
}

#[cfg(test)]
mod bucket_1_subagent_vocabulary {
    use super::*;

    #[test]
    fn await_mode_round_trip_via_persisted_vocab() {
        for &mode in AwaitMode::ALL {
            assert_eq!(AwaitMode::from_persisted(mode.as_str()), Some(mode));
        }
    }

    #[test]
    fn await_mode_all_has_two_variants() {
        assert_eq!(AwaitMode::ALL.len(), 2);
    }

    #[test]
    fn await_mode_from_persisted_unknown_returns_none() {
        assert_eq!(AwaitMode::from_persisted("unknown"), None);
    }

    #[test]
    fn cancel_policy_round_trip_via_persisted_vocab() {
        for &policy in CancelPolicy::ALL {
            assert_eq!(CancelPolicy::from_persisted(policy.as_str()), Some(policy));
        }
    }

    #[test]
    fn cancel_policy_all_has_two_variants() {
        assert_eq!(CancelPolicy::ALL.len(), 2);
    }

    #[test]
    fn cancel_policy_from_persisted_unknown_returns_none() {
        assert_eq!(CancelPolicy::from_persisted("unknown"), None);
    }

    #[test]
    fn cancel_cause_round_trip_via_persisted_vocab() {
        for &cause in CancelCause::ALL {
            assert_eq!(CancelCause::from_persisted(cause.as_str()), Some(cause));
        }
    }

    #[test]
    fn cancel_cause_all_has_three_variants() {
        assert_eq!(CancelCause::ALL.len(), 3);
    }

    #[test]
    fn cancel_cause_from_persisted_unknown_returns_none() {
        assert_eq!(CancelCause::from_persisted("unknown"), None);
    }

    #[test]
    fn child_terminal_all_kind_has_four_variants() {
        assert_eq!(ChildTerminal::ALL_KIND.len(), 4);
        assert_eq!(
            ChildTerminal::ALL_KIND,
            &["failed", "dead", "interrupted", "superseded"]
        );
    }

    #[test]
    fn child_terminal_projection_partition() {
        // .interrupted → .cancelled; everything else → .failed
        assert_eq!(
            ChildTerminal::Failed {
                reason: "x".to_string(),
                failure_class: FailureClass::External
            }
            .projected_state(),
            ToolCallState::Failed
        );
        assert_eq!(ChildTerminal::Dead.projected_state(), ToolCallState::Failed);
        assert_eq!(
            ChildTerminal::Interrupted.projected_state(),
            ToolCallState::Cancelled
        );
        assert_eq!(
            ChildTerminal::Superseded.projected_state(),
            ToolCallState::Failed
        );
    }
}
