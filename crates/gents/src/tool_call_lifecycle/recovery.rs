//! Recovery for persisted running tool calls: the startup sweep over rows
//! orphaned by a daemon restart, the periodic subagent-liveness sweep
//! (#465) that terminalizes expired children and orphaned queued descendants
//! on the live reconciler tick, and the live terminal-parent owned-tool
//! cleanup (#837) that cancels running composites/tools whose parent is
//! already terminal without waiting for deadline or daemon restart.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use defra_node::{EmbeddedNode, ExecuteRetryPolicy, QueryRequest};
use identity::Did;
use serde::Deserialize;

use crate::background_completion::ensure_background_subagent_completion_side_effects;
use crate::background_tools::{
    child_request_completed, fail_running_subagent_tool_call, load_parent_subagent_authorization,
    project_child_terminal, render_assistant_message_text, subagent_spawn_denial,
    subagent_tool_not_allowed_payload,
};
use crate::graphql::{escape_graphql_string, response_has_documents};
use crate::interrupt::interrupt_request;
use crate::session::execute_mutation_with_retry;

use super::{
    subagent_request::{
        create_subagent_request_with_request_id, verify_current_bridge_admission,
        BridgeAdmissionSnapshot,
    },
    AwaitMode, CancelCause, CancelPolicy, ChildTerminal, FailureClass, ToolCallState,
};

#[derive(Debug, Default)]
pub struct ToolCallRecoveryReport {
    pub tool_calls_recovered: usize,
    pub notifications_repaired: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SubagentLivenessReport {
    pub expired_children_terminalized: usize,
    pub bridges_projected: usize,
    pub queued_descendants_interrupted: usize,
}

impl SubagentLivenessReport {
    pub fn is_noop(&self) -> bool {
        *self == Self::default()
    }
}

/// Live reconcile of running tool rows owned by a terminal parent (#837).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TerminalParentToolReport {
    pub tool_calls_terminalized: usize,
    pub notifications_repaired: usize,
}

impl TerminalParentToolReport {
    pub fn is_noop(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct OrphanedBackgroundToolReport {
    pub tool_calls_terminalized: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct BackgroundCompletionSideEffectReport {
    pub side_effects_converged: usize,
}

impl BackgroundCompletionSideEffectReport {
    pub fn is_noop(&self) -> bool {
        *self == Self::default()
    }
}

impl OrphanedBackgroundToolReport {
    pub fn is_noop(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Deserialize)]
struct RunningToolCallRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    #[serde(default)]
    request_id: Option<String>,
    /// Immutable owner principal stamped at create. Recovery scopes by this
    /// field — `request_id` alone is not unique across agents.
    #[serde(default)]
    agent_did: Option<String>,
    session_id: String,
    tool_call_id: String,
    #[serde(default)]
    tool_name: String,
    #[serde(default)]
    args: String,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    deadline_at: Option<String>,
    #[serde(default)]
    await_mode: Option<String>,
    #[serde(default)]
    cancel_policy: Option<String>,
    #[serde(default)]
    cancel_cause: Option<String>,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    lifecycle_state: Option<String>,
    #[serde(default)]
    completion_notification_delivered_at: Option<String>,
    #[serde(default)]
    child_request_id: Option<String>,
    #[serde(default)]
    spawn_target_did: Option<String>,
    #[serde(default)]
    unclaimed_deadline_at: Option<String>,
}

impl RunningToolCallRow {
    fn bridge_admission_snapshot(&self) -> BridgeAdmissionSnapshot {
        BridgeAdmissionSnapshot {
            request_id: self.request_id.clone(),
            agent_did: self.agent_did.clone(),
            tool_call_id: self.tool_call_id.clone(),
            tool_name: self.tool_name.clone(),
            args: self.args.clone(),
            lifecycle_state: self.lifecycle_state.clone(),
            deadline_at: self.deadline_at.clone(),
            await_mode: self.await_mode.clone(),
            cancel_policy: self.cancel_policy.clone(),
            child_request_id: self.child_request_id.clone(),
            spawn_target_did: self.spawn_target_did.clone(),
            unclaimed_deadline_at: self.unclaimed_deadline_at.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct TerminalBackgroundToolRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    tool_call_id: Option<String>,
    #[serde(default)]
    tool_name: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    result: String,
    #[serde(default)]
    lifecycle_state: Option<String>,
    #[serde(default)]
    cancel_cause: Option<String>,
    #[serde(default)]
    child_request_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ParentRequestRow {
    agent_did: String,
    status: String,
    #[serde(default)]
    lifecycle_state: Option<String>,
    #[serde(default)]
    subagent_depth: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ChildRequestLivenessRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    #[serde(default)]
    request_id: String,
    agent_did: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    lifecycle_state: Option<String>,
    #[serde(default)]
    deadline: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StreamingChildResponseRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    #[serde(default)]
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SpawnArgs {
    #[serde(default)]
    name: Option<String>,
    /// Resolved owning DID of the target behavior (#377). Absent on legacy
    /// fixtures, which fall back to the parent's DID.
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(alias = "target", alias = "target_behavior_id")]
    behavior_id: String,
    #[serde(alias = "message", alias = "content")]
    prompt: String,
    #[serde(default)]
    deadline: Option<String>,
}

impl SpawnArgs {
    fn target_name(&self) -> &str {
        self.name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&self.behavior_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryOutcome {
    TimedOut,
    Cancelled,
    Failed,
    BackgroundInterrupted,
    UnclaimedCrossDeploymentSpawn,
}

impl super::ToolCallLifecycle {
    pub async fn recover_all(
        node: &EmbeddedNode,
        agent_did: &str,
    ) -> Result<ToolCallRecoveryReport> {
        let materialized_children = recover_orphan_subagent_children(node, agent_did).await?;
        if materialized_children > 0 {
            tracing::info!(
                materialized_children,
                "materialized orphan subagent child requests during tool-call recovery"
            );
        }

        let tool_calls_recovered = recover_stuck_running_tool_calls(node, agent_did).await?;
        let pending_side_effects =
            Self::reconcile_background_completion_side_effects(node, agent_did)
                .await?
                .side_effects_converged;
        let legacy_notifications =
            repair_missing_background_tool_notifications(node, agent_did).await?;

        Ok(ToolCallRecoveryReport {
            tool_calls_recovered,
            notifications_repaired: pending_side_effects + legacy_notifications,
        })
    }

    /// Periodic subagent-liveness reconciliation (#465; Lean:
    /// `Recovery.expiredSubagentChildSweep` / `Recovery.queuedDescendantSweep`,
    /// cadence `periodic`). Startup recovery already terminalizes expired
    /// children and bridges terminal children — but only on restart. Without a
    /// restart, a background child whose executor died past its deadline stays
    /// `processing` forever: the bridge never projects a terminal result and
    /// the parent's response wait wedges. This applies the same transitions on
    /// the live reconciler tick:
    ///
    /// 1. Terminalize locally-owned claimed/processing children of running
    ///    bridges whose deadline has passed (a live executor enforces its own
    ///    request deadline, so an expired non-terminal row means the executor
    ///    is gone). Safe against races: the underlying mutation only flips
    ///    non-terminal rows.
    /// 2. For BACKGROUND bridges, immediately project the now-terminal child
    ///    onto the bridge (failed/deadline) and queue the parent wake
    ///    notification. Foreground bridges are left to their live waiter,
    ///    which polls the child edge and owns the bridge lifecycle in-memory.
    /// 3. Interrupt pending (queued) descendants whose parent request is
    ///    already terminal — they can never legally run.
    pub async fn reconcile_subagent_liveness(
        node: &EmbeddedNode,
        agent_did: &str,
    ) -> Result<SubagentLivenessReport> {
        let mut report = SubagentLivenessReport::default();

        let bridge_rows = load_running_subagent_bridge_rows(node).await?;
        // One batched liveness read for every bridge's child, instead of a
        // per-bridge query on the 5s tick.
        let child_ids = bridge_rows
            .iter()
            .filter_map(child_request_id)
            .collect::<Vec<_>>();
        let children = load_child_liveness_rows(node, &child_ids).await?;

        for row in &bridge_rows {
            let Some(child) = child_request_id(row).and_then(|id| children.get(id)) else {
                continue;
            };
            if !terminalize_expired_child_with_row(node, agent_did, row, child).await? {
                continue;
            }
            report.expired_children_terminalized += 1;
            if is_background_subagent_tool(row)
                && recover_bridge_terminal_child(node, agent_did, row).await?
            {
                report.bridges_projected += 1;
            }
        }

        report.queued_descendants_interrupted =
            interrupt_queued_descendants_of_terminal_parents(node, agent_did).await?;

        if !report.is_noop() {
            tracing::info!(
                expired_children_terminalized = report.expired_children_terminalized,
                bridges_projected = report.bridges_projected,
                queued_descendants_interrupted = report.queued_descendants_interrupted,
                "reconciled subagent liveness"
            );
        }
        Ok(report)
    }

    /// Live reconcile: terminalize running tool calls whose parent request is
    /// already terminal (#837). Unlike full startup `recover_all`, this does
    /// **not** interrupt live-parent background tools (restart-only path).
    ///
    /// Scope and ordering:
    /// 1. Load only tool rows stamped with this agent's immutable `agent_did`
    ///    (not global `request_id` matches — that field is not unique).
    /// 2. Resolve the parent under the same DID; skip missing/foreign parents.
    /// 3. Require a terminal parent before any write.
    /// 4. Leave every native background row to the registry-aware orphan
    ///    sweep, regardless of parent state.
    /// 5. Detached bridges under an *interrupted* parent are left running.
    /// 6. Child-linked bridges under a *cleanly completed* parent are left
    ///    running — clean completion is not a cancel signal (live cascade and
    ///    recovery only cancel on cancel-worthy terminals).
    /// 7. Then project already-terminal children onto bridges (matches startup
    ///    child-precedence so restart and live ticks converge).
    ///
    /// Covers the durable bad state observed for `fan_out_and_synthesize`:
    /// parent interrupted, outer composite still `running`, no executor active.
    pub async fn reconcile_terminal_parent_owned_tools(
        node: &EmbeddedNode,
        agent_did: &str,
    ) -> Result<TerminalParentToolReport> {
        let rows = load_running_tool_call_rows_for_agent(node, agent_did).await?;
        let mut report = TerminalParentToolReport::default();
        let mut parent_cache: std::collections::HashMap<String, Option<ParentRequestRow>> =
            std::collections::HashMap::new();

        for row in rows {
            // Defense in depth: never mutate a row whose stamped owner differs.
            if row.agent_did.as_deref() != Some(agent_did) {
                continue;
            }
            // Native background rows belong exclusively to the orphan sweep,
            // which checks volatile ownership and applies deadline/unclaimed
            // precedence before parent state. Keeping this sweep disjoint
            // prevents its earlier periodic slot from bypassing that classifier.
            if is_background_tool_row(&row) {
                continue;
            }

            let parent = match row
                .request_id
                .as_deref()
                .filter(|request_id| !request_id.is_empty())
            {
                Some(request_id) => {
                    if let Some(cached) = parent_cache.get(request_id) {
                        cached.clone()
                    } else {
                        let loaded = lookup_parent_request(node, agent_did, request_id).await?;
                        parent_cache.insert(request_id.to_string(), loaded.clone());
                        loaded
                    }
                }
                None => None,
            };
            // Ownership gate: parent must resolve under this agent's DID.
            let Some(parent) = parent else {
                continue;
            };
            // Live parents are out of scope for this sweep.
            if !request_is_terminal(&parent) {
                continue;
            }

            // Detached bridges may outlive an interrupted parent by design
            // (startup recovery leaves them running too).
            if is_detached_subagent_tool(&row) && request_is_interrupted(&parent) {
                continue;
            }

            // Child-terminal precedence only after owner + terminal-parent gates.
            if recover_bridge_terminal_child(node, agent_did, &row).await? {
                report.tool_calls_terminalized += 1;
                tracing::info!(
                    doc_id = %row.doc_id,
                    request_id = row.request_id.as_deref().unwrap_or(""),
                    tool_call_id = %row.tool_call_id,
                    "reconciled running bridge from already-terminal child"
                );
                continue;
            }

            // Clean parent completion is not a cancel signal for linked
            // background/cascade children — leave the bridge running.
            if request_is_cleanly_completed(&parent) && child_request_id(&row).is_some() {
                continue;
            }

            let outcome = if request_is_interrupted(&parent) {
                RecoveryOutcome::Cancelled
            } else {
                // Cancel-worthy non-interrupt terminals, or a native composite
                // stranded under a cleanly-completed parent.
                RecoveryOutcome::Failed
            };

            // Cascade only for cancel-worthy terminals — never on clean complete.
            let mut remote_cancel_intent_at = None;
            if request_is_cancel_worthy_terminal(&parent) {
                if let Some(child_request_id) = cascade_child_request_id(&row) {
                    if child_request_is_locally_owned(node, agent_did, child_request_id).await? {
                        if let Err(error) = interrupt_request(node, child_request_id).await {
                            tracing::warn!(
                                doc_id = %row.doc_id,
                                request_id = row.request_id.as_deref().unwrap_or(""),
                                tool_call_id = %row.tool_call_id,
                                child_request_id,
                                error = %error,
                                "failed to cascade live terminal-parent cancel to child request"
                            );
                        }
                    } else {
                        remote_cancel_intent_at = Some(Utc::now());
                    }
                }
            }

            let deadline_at = parse_datetime(row.deadline_at.as_deref());
            let updated = match recover_tool_call_row(
                node,
                &row,
                deadline_at,
                outcome,
                true,
                remote_cancel_intent_at,
            )
            .await
            {
                Ok(updated) => updated,
                Err(error) => {
                    tracing::warn!(
                        doc_id = %row.doc_id,
                        request_id = row.request_id.as_deref().unwrap_or(""),
                        tool_call_id = %row.tool_call_id,
                        error = %error,
                        "failed to terminalize running tool owned by terminal parent"
                    );
                    continue;
                }
            };
            if !updated {
                // Lost CAS: concurrent complete/fail/cancel already terminalized.
                continue;
            }

            if is_background_tool_row(&row) {
                append_recovered_background_tool_completion(node, &row, outcome).await;
            }

            report.tool_calls_terminalized += 1;
            tracing::info!(
                doc_id = %row.doc_id,
                request_id = row.request_id.as_deref().unwrap_or(""),
                tool_call_id = %row.tool_call_id,
                lifecycle_state = %outcome.lifecycle_state().as_str(),
                "reconciled running tool owned by terminal parent"
            );
        }

        report.notifications_repaired =
            repair_missing_background_tool_notifications(node, agent_did).await?;

        if !report.is_noop() {
            tracing::info!(
                tool_calls_terminalized = report.tool_calls_terminalized,
                notifications_repaired = report.notifications_repaired,
                "reconciled terminal-parent owned tools"
            );
        }
        Ok(report)
    }

    /// Periodic repair for a durable native-background row whose volatile
    /// process owner is absent. Live workers are skipped by registry identity;
    /// an empty registry after restart (or panic cleanup) re-applies the same
    /// classifier used by startup recovery.
    pub async fn reconcile_orphaned_background_tools(
        node: &EmbeddedNode,
        agent_did: &str,
        executions: &crate::hook::BackgroundExecutionRegistry,
    ) -> Result<OrphanedBackgroundToolReport> {
        let rows = load_running_tool_call_rows_for_agent(node, agent_did).await?;
        let mut report = OrphanedBackgroundToolReport::default();

        for row in rows {
            if row.agent_did.as_deref() != Some(agent_did)
                || !is_background_tool_row(&row)
                || executions.contains(&row.tool_call_id).await
            {
                continue;
            }
            let parent = match row.request_id.as_deref().filter(|id| !id.is_empty()) {
                Some(request_id) => lookup_parent_request(node, agent_did, request_id).await?,
                None => None,
            };
            let deadline_at = parse_datetime(row.deadline_at.as_deref());
            let Some(outcome) = classify_running_tool_recovery(&row, parent.as_ref(), Utc::now())
            else {
                continue;
            };

            let updated = match recover_tool_call_row(
                node,
                &row,
                deadline_at,
                outcome,
                parent.is_some(),
                None,
            )
            .await
            {
                Ok(updated) => updated,
                Err(error) => {
                    tracing::warn!(
                        doc_id = %row.doc_id,
                        request_id = row.request_id.as_deref().unwrap_or(""),
                        session_id = %row.session_id,
                        tool_call_id = %row.tool_call_id,
                        error = %error,
                        "failed to reconcile orphaned background tool"
                    );
                    continue;
                }
            };
            if !updated {
                continue;
            }

            if parent.is_some() {
                append_recovered_background_tool_completion(node, &row, outcome).await;
            }
            report.tool_calls_terminalized += 1;
        }

        if !report.is_noop() {
            tracing::info!(
                tool_calls_terminalized = report.tool_calls_terminalized,
                "reconciled orphaned background tools"
            );
        }
        Ok(report)
    }

    /// Redrive the idempotent notification + session wake after the lifecycle
    /// row is already terminal. Persisted `status=completionPending:<reason>`
    /// (or the legacy unsuffixed cursor) advances to `completed` only after
    /// both side effects converge, so transient failures remain discoverable.
    pub async fn reconcile_background_completion_side_effects(
        node: &EmbeddedNode,
        agent_did: &str,
    ) -> Result<BackgroundCompletionSideEffectReport> {
        let rows = load_pending_background_completion_rows(node, agent_did).await?;
        let mut report = BackgroundCompletionSideEffectReport::default();

        for row in rows {
            if row.agent_did.as_deref() != Some(agent_did)
                || non_empty(row.child_request_id.as_deref()).is_some()
            {
                continue;
            }
            let Some(request_id) = non_empty(row.request_id.as_deref()) else {
                continue;
            };
            if lookup_parent_request(node, agent_did, request_id)
                .await?
                .is_none()
            {
                continue;
            }
            let Some(session_id) = non_empty(row.session_id.as_deref()) else {
                tracing::warn!(doc_id = %row.doc_id, "skipping completion redrive without session_id");
                continue;
            };
            let Some(tool_call_id) = non_empty(row.tool_call_id.as_deref()) else {
                tracing::warn!(doc_id = %row.doc_id, "skipping completion redrive without tool_call_id");
                continue;
            };
            let Some((status, reason)) = background_completion_projection(&row) else {
                continue;
            };

            match crate::background_completion::append_background_tool_completion(
                node,
                session_id,
                request_id,
                tool_call_id,
                &row.tool_name,
                status,
                &row.result,
                reason,
            )
            .await
            {
                Ok(()) => report.side_effects_converged += 1,
                Err(error) => tracing::warn!(
                    doc_id = %row.doc_id,
                    tool_call_id,
                    error = %error,
                    "failed to redrive background completion side effects"
                ),
            }
        }

        if !report.is_noop() {
            tracing::info!(
                side_effects_converged = report.side_effects_converged,
                "reconciled background completion side effects"
            );
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_recovery_persists_external_failure_class() {
        assert_eq!(
            RecoveryOutcome::TimedOut.failure_class(),
            Some(FailureClass::External)
        );
        assert_eq!(RecoveryOutcome::Cancelled.failure_class(), None);
    }

    #[test]
    fn background_completion_reason_comes_from_cursor_not_tool_text() {
        let row = TerminalBackgroundToolRow {
            doc_id: "doc-1".to_string(),
            request_id: Some("request-1".to_string()),
            agent_did: Some("did:test:agent".to_string()),
            session_id: Some("session-1".to_string()),
            tool_call_id: Some("tool-1".to_string()),
            tool_name: "test_tool".to_string(),
            status: "completionPending:tool_failed".to_string(),
            result: "tool-controlled text says background tool panicked".to_string(),
            lifecycle_state: Some("failed".to_string()),
            cancel_cause: None,
            child_request_id: None,
        };
        assert_eq!(
            background_completion_projection(&row),
            Some(("failed", Some("tool_failed")))
        );
    }

    #[test]
    fn background_completion_redrive_preserves_custom_cancel_reason() {
        let row = TerminalBackgroundToolRow {
            doc_id: "doc-custom".to_string(),
            request_id: Some("request-custom".to_string()),
            agent_did: Some("did:test:agent".to_string()),
            session_id: Some("session-custom".to_string()),
            tool_call_id: Some("tool-custom".to_string()),
            tool_name: "test_tool".to_string(),
            status: "completionPending:operator requested drain".to_string(),
            result: String::new(),
            lifecycle_state: Some("cancelled".to_string()),
            cancel_cause: Some("userCancelled".to_string()),
            child_request_id: None,
        };
        assert_eq!(
            background_completion_projection(&row),
            Some(("cancelled", Some("operator requested drain")))
        );
    }

    #[test]
    fn cancel_worthy_field_takes_precedence_over_divergent_completed_status() {
        let divergent = ParentRequestRow {
            agent_did: "did:test:x".to_string(),
            status: "completed".to_string(),
            lifecycle_state: Some("interrupted".to_string()),
            subagent_depth: None,
        };
        assert!(request_has_cancel_worthy_field(&divergent));
        assert!(request_is_cancel_worthy_terminal(&divergent));
        assert!(request_is_interrupted(&divergent));
        assert!(
            !request_is_cleanly_completed(&divergent),
            "status=completed must not suppress lifecycle_state=interrupted"
        );
    }

    #[test]
    fn clean_completion_requires_no_cancel_worthy_fields() {
        let clean = ParentRequestRow {
            agent_did: "did:test:x".to_string(),
            status: "completed".to_string(),
            lifecycle_state: Some("completed".to_string()),
            subagent_depth: None,
        };
        assert!(!request_has_cancel_worthy_field(&clean));
        assert!(request_is_cleanly_completed(&clean));
        assert!(!request_is_cancel_worthy_terminal(&clean));
    }
}

async fn recover_orphan_subagent_children(node: &EmbeddedNode, agent_did: &str) -> Result<usize> {
    let rows = load_running_tool_call_rows_for_agent(node, agent_did).await?;
    let mut materialized = 0;

    for row in rows {
        if row.agent_did.as_deref() != Some(agent_did) {
            continue;
        }
        let Some(child_request_id) = child_request_id(&row).map(str::to_string) else {
            continue;
        };
        if child_request_exists(node, &child_request_id).await? {
            continue;
        }
        if row
            .unclaimed_deadline_at
            .as_deref()
            .is_some_and(|deadline| !deadline.is_empty())
        {
            continue;
        }

        let bridge_admission = match verify_current_bridge_admission(
            node,
            &row.doc_id,
            &row.bridge_admission_snapshot(),
        )
        .await
        {
            Ok(admission) if admission.signer_did == agent_did => admission,
            Ok(admission) => {
                let error = anyhow::anyhow!(
                    "bridge signer {} does not match recovering agent {agent_did}",
                    admission.signer_did
                );
                tracing::warn!(
                    doc_id = %row.doc_id,
                    tool_call_id = %row.tool_call_id,
                    %error,
                    "cannot materialize orphan subagent child from foreign-signed bridge"
                );
                continue;
            }
            Err(error) => {
                tracing::warn!(
                    doc_id = %row.doc_id,
                    tool_call_id = %row.tool_call_id,
                    %error,
                    "cannot materialize orphan subagent child without exact signed bridge admission"
                );
                continue;
            }
        };
        tracing::debug!(
            doc_id = %row.doc_id,
            bridge_commit_cid = %bridge_admission.composite_commit_cid,
            bridge_signer_did = %bridge_admission.signer_did,
            "verified orphan subagent bridge admission"
        );

        let parent_request_id = match row
            .request_id
            .as_deref()
            .filter(|request_id| !request_id.is_empty())
        {
            Some(request_id) => request_id.to_string(),
            None => {
                tracing::warn!(
                    doc_id = %row.doc_id,
                    session_id = %row.session_id,
                    tool_call_id = %row.tool_call_id,
                    child_request_id = %child_request_id,
                    "cannot materialize orphan subagent child without parent request_id"
                );
                continue;
            }
        };

        let Some(parent) = lookup_parent_request(node, agent_did, &parent_request_id).await? else {
            tracing::warn!(
                doc_id = %row.doc_id,
                request_id = %parent_request_id,
                session_id = %row.session_id,
                tool_call_id = %row.tool_call_id,
                child_request_id = %child_request_id,
                "cannot materialize orphan subagent child because parent AgentRequest is missing"
            );
            continue;
        };

        let spawn_args = match serde_json::from_str::<SpawnArgs>(&row.args) {
            Ok(spawn_args) => spawn_args,
            Err(error) => {
                tracing::warn!(
                    doc_id = %row.doc_id,
                    request_id = %parent_request_id,
                    session_id = %row.session_id,
                    tool_call_id = %row.tool_call_id,
                    child_request_id = %child_request_id,
                    error = %error,
                    "cannot materialize orphan subagent child because tool args are invalid"
                );
                continue;
            }
        };
        let row_spawn_target_did =
            non_empty(row.spawn_target_did.as_deref()).map(ToOwned::to_owned);
        let args_target_did = non_empty(spawn_args.agent_did.as_deref()).map(ToOwned::to_owned);
        if let (Some(row_did), Some(args_did)) = (&row_spawn_target_did, &args_target_did) {
            if row_did != args_did {
                let failed = fail_unauthorized_orphan_subagent_tool_call(
                    node,
                    &row,
                    "/agent_did",
                    args_did,
                    "subagent target DID args do not match immutable spawn_target_did",
                    &[],
                )
                .await?;
                tracing::warn!(
                    doc_id = %row.doc_id,
                    request_id = %parent_request_id,
                    session_id = %row.session_id,
                    tool_call_id = %row.tool_call_id,
                    child_request_id = %child_request_id,
                    spawn_target_did = %row_did,
                    args_agent_did = %args_did,
                    failed_tool_call = failed,
                    "cannot materialize orphan subagent child because target DID fields differ"
                );
                continue;
            }
        }
        let resolved_target_did = row_spawn_target_did.or(args_target_did);

        let parent_depth = parent
            .subagent_depth
            .and_then(|depth| u32::try_from(depth).ok())
            .unwrap_or(0);
        let deadline =
            effective_deadline(row.deadline_at.as_deref(), spawn_args.deadline.as_deref());

        let authorization = match load_parent_subagent_authorization(node, &parent_request_id).await
        {
            Ok(authorization) => authorization,
            Err(error) => {
                let failed = fail_unauthorized_orphan_subagent_tool_call(
                    node,
                    &row,
                    "/name",
                    spawn_args.target_name(),
                    "subagent authorization could not be verified for this behavior",
                    &[],
                )
                .await?;
                tracing::warn!(
                    doc_id = %row.doc_id,
                    request_id = %parent_request_id,
                    session_id = %row.session_id,
                    tool_call_id = %row.tool_call_id,
                    child_request_id = %child_request_id,
                    target_name = %spawn_args.target_name(),
                    failed_tool_call = failed,
                    error = %error,
                    "cannot materialize orphan subagent child because parent authorization could not be verified"
                );
                continue;
            }
        };
        let row_await_mode = await_mode(&row);
        let tool_name = subagent_tool_name(&row);
        if let Some(denial) = subagent_spawn_denial(
            &authorization,
            spawn_args.target_name(),
            row_await_mode,
            tool_name,
            agent_did,
        ) {
            let failed = fail_unauthorized_orphan_subagent_tool_call(
                node,
                &row,
                denial.path,
                &denial.requested,
                denial.message,
                &authorization.allowed_target_names(),
            )
            .await?;
            tracing::warn!(
                doc_id = %row.doc_id,
                request_id = %parent_request_id,
                session_id = %row.session_id,
                tool_call_id = %row.tool_call_id,
                child_request_id = %child_request_id,
                parent_behavior_id = %authorization.behavior_id,
                target_name = %spawn_args.target_name(),
                await_mode = %row_await_mode.as_str(),
                failed_tool_call = failed,
                "cannot materialize orphan subagent child because spawn is not authorized"
            );
            continue;
        }

        let child_agent_did = resolved_target_did.unwrap_or_else(|| parent.agent_did.clone());
        if let Err(error) = create_subagent_request_with_request_id(
            node,
            child_request_id.clone(),
            parent_request_id.clone(),
            row.tool_call_id.clone(),
            parent_depth,
            child_agent_did,
            spawn_args.behavior_id,
            spawn_args.prompt,
            deadline,
        )
        .await
        {
            tracing::warn!(
                doc_id = %row.doc_id,
                request_id = %parent_request_id,
                session_id = %row.session_id,
                tool_call_id = %row.tool_call_id,
                child_request_id = %child_request_id,
                error = %error,
                "failed to materialize orphan subagent child request during recovery"
            );
            continue;
        }

        materialized += 1;
        tracing::info!(
            doc_id = %row.doc_id,
            request_id = %parent_request_id,
            session_id = %row.session_id,
            tool_call_id = %row.tool_call_id,
            child_request_id = %child_request_id,
            "materialized orphan subagent child request during recovery"
        );
    }

    Ok(materialized)
}

async fn fail_unauthorized_orphan_subagent_tool_call(
    node: &EmbeddedNode,
    row: &RunningToolCallRow,
    path: &str,
    requested: &str,
    message: impl Into<String>,
    allowed_targets: &[String],
) -> Result<bool> {
    let tool_name = subagent_tool_name(row);
    let payload =
        subagent_tool_not_allowed_payload(tool_name, path, requested, message, allowed_targets);
    fail_running_subagent_tool_call(
        node,
        &row.doc_id,
        row.started_at.as_deref(),
        row.deadline_at.as_deref(),
        &payload,
        FailureClass::ServiceUnavailable,
    )
    .await
}

async fn recover_stuck_running_tool_calls(node: &EmbeddedNode, agent_did: &str) -> Result<usize> {
    let rows = load_running_tool_call_rows_for_agent(node, agent_did).await?;

    let mut recovered = 0;
    for row in rows {
        if row.agent_did.as_deref() != Some(agent_did) {
            continue;
        }

        let deadline_at = parse_datetime(row.deadline_at.as_deref());
        let parent = match row
            .request_id
            .as_deref()
            .filter(|request_id| !request_id.is_empty())
        {
            Some(request_id) => lookup_parent_request(node, agent_did, request_id).await?,
            None => None,
        };

        if child_request_id(&row).is_some() {
            let _ = terminalize_expired_local_child_request(node, agent_did, &row).await?;
        }

        if recover_bridge_terminal_child(node, agent_did, &row).await? {
            recovered += 1;
            continue;
        }

        let outcome = classify_running_tool_recovery(&row, parent.as_ref(), Utc::now());

        let Some(outcome) = outcome else {
            if is_background_subagent_tool(&row) {
                tracing::info!(
                    doc_id = %row.doc_id,
                    request_id = row.request_id.as_deref().unwrap_or(""),
                    session_id = %row.session_id,
                    tool_call_id = %row.tool_call_id,
                    child_request_id = row.child_request_id.as_deref().unwrap_or(""),
                    "leaving background subagent tool call running during recovery"
                );
            }
            continue;
        };

        // Cascade only on cancel-worthy parent terminals (not clean completion).
        let mut remote_cancel_intent_at = None;
        let should_cascade = outcome != RecoveryOutcome::UnclaimedCrossDeploymentSpawn
            && parent
                .as_ref()
                .is_none_or(|p| !request_is_cleanly_completed(p));
        if should_cascade {
            if let Some(child_request_id) = cascade_child_request_id(&row) {
                if child_request_is_locally_owned(node, agent_did, child_request_id).await? {
                    if let Err(error) = interrupt_request(node, child_request_id).await {
                        tracing::warn!(
                            doc_id = %row.doc_id,
                            request_id = row.request_id.as_deref().unwrap_or(""),
                            session_id = %row.session_id,
                            tool_call_id = %row.tool_call_id,
                            child_request_id,
                            error = %error,
                            "failed to cascade recovery interrupt to child request"
                        );
                    }
                } else {
                    remote_cancel_intent_at = Some(Utc::now());
                }
            }
        }

        let updated = match recover_tool_call_row(
            node,
            &row,
            deadline_at,
            outcome,
            parent.is_some(),
            remote_cancel_intent_at,
        )
        .await
        {
            Ok(updated) => updated,
            Err(error) => {
                tracing::warn!(
                    doc_id = %row.doc_id,
                    request_id = row.request_id.as_deref().unwrap_or(""),
                    session_id = %row.session_id,
                    tool_call_id = %row.tool_call_id,
                    error = %error,
                    "failed to recover running tool call"
                );
                continue;
            }
        };
        if !updated {
            // Lost CAS against a concurrent terminal writer — leave the durable
            // terminal untouched (first-writer-wins).
            continue;
        }

        if is_background_tool_row(&row) {
            append_recovered_background_tool_completion(node, &row, outcome).await;
        }

        recovered += 1;
        tracing::info!(
            doc_id = %row.doc_id,
            request_id = row.request_id.as_deref().unwrap_or(""),
            session_id = %row.session_id,
            tool_call_id = %row.tool_call_id,
            lifecycle_state = %outcome.lifecycle_state().as_str(),
            "recovered stuck running tool call"
        );
    }

    Ok(recovered)
}

async fn append_recovered_background_tool_completion(
    node: &EmbeddedNode,
    row: &RunningToolCallRow,
    outcome: RecoveryOutcome,
) {
    let Some(parent_request_id) = row.request_id.as_deref().filter(|id| !id.is_empty()) else {
        return;
    };
    let status = match outcome {
        RecoveryOutcome::Cancelled | RecoveryOutcome::BackgroundInterrupted => "cancelled",
        RecoveryOutcome::TimedOut
        | RecoveryOutcome::Failed
        | RecoveryOutcome::UnclaimedCrossDeploymentSpawn => "failed",
    };
    let reason = outcome.notification_reason();
    if let Err(error) = crate::background_completion::append_background_tool_completion(
        node,
        &row.session_id,
        parent_request_id,
        &row.tool_call_id,
        &row.tool_name,
        status,
        "",
        Some(reason),
    )
    .await
    {
        tracing::warn!(
            doc_id = %row.doc_id,
            request_id = parent_request_id,
            session_id = %row.session_id,
            tool_call_id = %row.tool_call_id,
            error = %error,
            "failed to append recovered background tool notification"
        );
    }
}

/// Running tool rows owned by `agent_did` (immutable scope key on create).
async fn load_running_tool_call_rows_for_agent(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<Vec<RunningToolCallRow>> {
    let escaped = escape_graphql_string(agent_did);
    load_running_tool_call_rows_with_filter(
        node,
        &format!(r#", agent_did: {{ _eq: "{escaped}" }}"#),
    )
    .await
}

/// Running bridge rows only (`child_request_id` set) — the periodic liveness
/// sweep's scope, filtered server-side so the 5s tick never pays for
/// non-subagent tool rows.
async fn load_running_subagent_bridge_rows(node: &EmbeddedNode) -> Result<Vec<RunningToolCallRow>> {
    load_running_tool_call_rows_with_filter(node, r#", child_request_id: { _ne: "" }"#).await
}

async fn load_running_tool_call_rows_with_filter(
    node: &EmbeddedNode,
    extra_filter: &str,
) -> Result<Vec<RunningToolCallRow>> {
    let query = format!(
        r#"{{
        AgentToolCall(
            filter: {{ lifecycle_state: {{ _eq: "running" }}{extra_filter} }}
        ) {{
            _docID
            request_id
            agent_did
            session_id
            tool_call_id
            tool_name
            args
            started_at
            deadline_at
            await_mode
            cancel_policy
            cancel_cause
            result
            lifecycle_state
            completion_notification_delivered_at
            child_request_id
            spawn_target_did
            unclaimed_deadline_at
        }}
    }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("querying stuck running tool calls: {:?}", resp.errors);
    }

    let values = resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    let mut rows = Vec::with_capacity(values.len());
    for value in values {
        match serde_json::from_value::<RunningToolCallRow>(value.clone()) {
            Ok(row) => rows.push(row),
            Err(error) => {
                tracing::warn!(
                    doc_id = value
                        .get("_docID")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or(""),
                    error = %error,
                    "skipping malformed running tool-call row during recovery"
                );
            }
        }
    }
    Ok(rows)
}

async fn load_pending_background_completion_rows(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<Vec<TerminalBackgroundToolRow>> {
    let agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentToolCall(filter: {{
                agent_did: {{ _eq: "{agent_did}" }},
                await_mode: {{ _eq: "background" }},
                lifecycle_state: {{ _in: ["completed", "failed", "timedOut", "cancelled"] }},
                status: {{ _like: "completionPending%" }}
            }}) {{
                _docID
                request_id
                agent_did
                session_id
                tool_call_id
                tool_name
                status
                result
                lifecycle_state
                cancel_cause
                child_request_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "querying pending background completion side effects: {:?}",
            response.errors
        );
    }
    let values = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut rows = Vec::with_capacity(values.len());
    for value in values {
        match serde_json::from_value::<TerminalBackgroundToolRow>(value.clone()) {
            Ok(row) => rows.push(row),
            Err(error) => tracing::warn!(
                doc_id = value
                    .get("_docID")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(""),
                error = %error,
                "skipping malformed terminal background row during side-effect recovery"
            ),
        }
    }
    Ok(rows)
}

fn background_completion_projection(
    row: &TerminalBackgroundToolRow,
) -> Option<(&str, Option<&str>)> {
    let persisted_reason = row.status.strip_prefix("completionPending:");
    match row.lifecycle_state.as_deref()? {
        "completed" => Some(("completed", None)),
        "timedOut" => Some((
            "failed",
            Some(persisted_reason.unwrap_or("deadline_exceeded")),
        )),
        "cancelled" => Some((
            "cancelled",
            Some(persisted_reason.unwrap_or_else(|| {
                if row.cancel_cause.as_deref() == Some("userCancelled") {
                    "explicit_cancel"
                } else {
                    "parent_interrupted"
                }
            })),
        )),
        "failed" => Some(("failed", Some(persisted_reason.unwrap_or("tool_failed")))),
        _ => None,
    }
}

/// Repair terminal background rows written before the retryable
/// `completionPending:<reason>` cursor was introduced. New rows are handled by
/// `reconcile_background_completion_side_effects`, which preserves the exact
/// durable reason and advances its cursor only after notification and wake.
async fn repair_missing_background_tool_notifications(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<usize> {
    let agent_did_escaped = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    agent_did: {{ _eq: "{agent_did_escaped}" }},
                    await_mode: {{ _eq: "background" }},
                    lifecycle_state: {{ _in: ["completed", "failed", "timedOut", "cancelled"] }},
                    status: {{ _eq: "completed" }},
                    completion_notification_delivered_at: {{ _eq: null }}
                }}
            ) {{
                _docID
                request_id
                agent_did
                session_id
                tool_call_id
                tool_name
                result
                lifecycle_state
                cancel_cause
                await_mode
                child_request_id
                completion_notification_delivered_at
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "querying missing background tool notifications: {:?}",
            response.errors
        );
    }
    let values = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut repaired = 0;
    for value in values {
        let row = match serde_json::from_value::<RunningToolCallRow>(value.clone()) {
            Ok(row) => row,
            Err(error) => {
                tracing::warn!(
                    doc_id = value
                        .get("_docID")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or(""),
                    error = %error,
                    "skipping malformed legacy background row during notification repair"
                );
                continue;
            }
        };
        if row.agent_did.as_deref() != Some(agent_did)
            || child_request_id(&row).is_some()
            || row.completion_notification_delivered_at.is_some()
        {
            continue;
        }
        let Some(parent_request_id) = row.request_id.as_deref().filter(|id| !id.is_empty()) else {
            continue;
        };
        let lifecycle_state = row.lifecycle_state.as_deref().unwrap_or_default();
        let (status, result, reason) = match lifecycle_state {
            "completed" => ("completed", row.result.as_deref().unwrap_or_default(), None),
            "cancelled" => (
                "cancelled",
                "",
                Some(row.cancel_cause.as_deref().unwrap_or("cancelled")),
            ),
            "timedOut" => ("failed", "", Some("deadline_exceeded")),
            "failed" => (
                "failed",
                row.result.as_deref().unwrap_or_default(),
                Some(row.cancel_cause.as_deref().unwrap_or("tool_failed")),
            ),
            _ => continue,
        };
        match crate::background_completion::append_background_tool_completion(
            node,
            &row.session_id,
            parent_request_id,
            &row.tool_call_id,
            &row.tool_name,
            status,
            result,
            reason,
        )
        .await
        {
            Ok(()) => repaired += 1,
            Err(error) => {
                tracing::warn!(
                    doc_id = %row.doc_id,
                    request_id = parent_request_id,
                    session_id = %row.session_id,
                    tool_call_id = %row.tool_call_id,
                    error = %error,
                    "failed to repair missing background tool notification"
                );
            }
        }
    }
    Ok(repaired)
}

async fn lookup_parent_request(
    node: &EmbeddedNode,
    agent_did: &str,
    request_id: &str,
) -> Result<Option<ParentRequestRow>> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    request_id: {{ _eq: "{escaped_request_id}" }}
                }},
                limit: 1
            ) {{
                agent_did
                status
                lifecycle_state
                subagent_depth
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "querying parent request for tool-call recovery request_id={request_id}: {:?}",
            resp.errors
        );
    }

    let rows: Vec<ParentRequestRow> = resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    Ok(rows.into_iter().next())
}

#[derive(Debug, Deserialize)]
struct PendingDescendantRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    #[serde(default)]
    caused_by_parent_request_id: Option<String>,
    #[serde(default)]
    caused_by_parent_tool_call_id: Option<String>,
}

/// Interrupt pending (queued) subagent child requests whose parent request is
/// already terminal (#465; Lean: `Recovery.queuedDescendantSweep`). A queued
/// spawn child of a terminal parent can never legally run; leaving it pending
/// wedges the live queue forever. This is the queued-side analogue of the
/// running-child cascade interrupt, applied as a direct filtered terminal
/// write because a pending row has no executor to observe an interrupt.
///
/// Scope guard (Lean: `QueuedDescendantRow.bridgeLinked`): only requests
/// referenced by an `AgentToolCall` bridge (`child_request_id == request_id`)
/// qualify. Queue rows that merely CARRY spawn lineage —
/// background-completion wake notifications, steering messages — are never
/// referenced by a bridge and must survive a terminal caller, so lineage
/// fields alone are deliberately not trusted.
///
/// The parent is looked up by `request_id` alone (no agent_did filter) so a
/// CROSS-DEPLOYMENT terminal parent whose replicated row is visible here also
/// releases its queued children; a parent row that has not replicated yet
/// yields `None` and the child is conservatively left pending.
async fn interrupt_queued_descendants_of_terminal_parents(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<usize> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    lifecycle_state: {{ _eq: "pending" }},
                    caused_by_parent_tool_call_id: {{ _ne: "" }}
                }}
            ) {{
                _docID
                request_id
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("querying pending descendant requests: {:?}", resp.errors);
    }
    let rows: Vec<PendingDescendantRow> = resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();

    let candidates = rows
        .iter()
        .filter(|row| {
            row.caused_by_parent_request_id
                .as_deref()
                .is_some_and(|id| !id.is_empty())
                && row
                    .caused_by_parent_tool_call_id
                    .as_deref()
                    .is_some_and(|id| !id.is_empty())
        })
        .collect::<Vec<_>>();
    let bridged_children = load_bridged_child_ids(
        node,
        &candidates
            .iter()
            .map(|row| row.request_id.as_str())
            .collect::<Vec<_>>(),
    )
    .await?;

    let mut parent_terminal_cache: std::collections::HashMap<String, bool> =
        std::collections::HashMap::new();
    let mut interrupted = 0usize;
    for row in candidates {
        let Some(parent_request_id) = row
            .caused_by_parent_request_id
            .as_deref()
            .filter(|id| !id.is_empty())
        else {
            continue;
        };

        let parent_terminal = match parent_terminal_cache.get(parent_request_id) {
            Some(&terminal) => terminal,
            None => {
                // By request_id alone: the parent of a cross-deployment spawn
                // carries a remote agent_did, and its replicated terminal row
                // must still release the queued child here.
                let terminal = load_request_liveness_row(node, parent_request_id)
                    .await?
                    .is_some_and(|parent| {
                        request_status_or_lifecycle_is_terminal(
                            parent.status.as_deref(),
                            parent.lifecycle_state.as_deref(),
                        )
                    });
                parent_terminal_cache.insert(parent_request_id.to_string(), terminal);
                terminal
            }
        };
        if !parent_terminal {
            continue;
        }
        if !bridged_children.contains(&row.request_id) {
            continue;
        }

        if interrupt_pending_descendant_row(node, &row.doc_id, agent_did, parent_request_id).await?
        {
            interrupted += 1;
            tracing::info!(
                doc_id = %row.doc_id,
                request_id = %row.request_id,
                parent_request_id,
                "interrupted queued subagent descendant of terminal parent"
            );
        }
    }
    Ok(interrupted)
}

/// One `_in` query for the bridge-existence scope guard: which of these
/// pending request ids are referenced by an `AgentToolCall` bridge as its
/// child (`child_request_id == request_id`)?
async fn load_bridged_child_ids(
    node: &EmbeddedNode,
    child_request_ids: &[&str],
) -> Result<std::collections::HashSet<String>> {
    if child_request_ids.is_empty() {
        return Ok(std::collections::HashSet::new());
    }
    let id_list = child_request_ids
        .iter()
        .map(|id| format!("\"{}\"", escape_graphql_string(id)))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{ child_request_id: {{ _in: [{id_list}] }} }}
            ) {{ child_request_id }}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("querying bridges for pending children: {:?}", resp.errors);
    }
    #[derive(Debug, Deserialize)]
    struct BridgeChildRow {
        #[serde(default)]
        child_request_id: Option<String>,
    }
    let rows: Vec<BridgeChildRow> = resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter_map(|row| row.child_request_id)
        .collect())
}

async fn interrupt_pending_descendant_row(
    node: &EmbeddedNode,
    doc_id: &str,
    agent_did: &str,
    parent_request_id: &str,
) -> Result<bool> {
    let reason = format!(
        "parent request {parent_request_id} reached a terminal state before this queued child was claimed"
    );
    let escaped_doc_id = escape_graphql_string(doc_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_reason = escape_graphql_string(&reason);
    let terminalized_at = escape_graphql_string(&Utc::now().to_rfc3339());
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{
                    _docID: {{ _eq: "{escaped_doc_id}" }},
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    lifecycle_state: {{ _eq: "pending" }}
                }},
                input: {{
                    status: "interrupted",
                    lifecycle_state: "interrupted",
                    failure_reason: "{escaped_reason}",
                    terminalized_at: "{terminalized_at}",
                    terminal_redrive_attempts: 0
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = crate::retry::execute_graphql_with_terminal_persistence_retry(
        node,
        &mutation,
        "interrupt_queued_descendant",
    )
    .await?;
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get("update_AgentRequest"))
        .is_some_and(response_has_documents))
}

async fn child_request_exists(node: &EmbeddedNode, request_id: &str) -> Result<bool> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 1
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!(
            "querying child request for tool-call recovery: {:?}",
            resp.errors
        );
    }
    Ok(resp
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(serde_json::Value::as_array)
        .is_some_and(|rows| !rows.is_empty()))
}

async fn recover_bridge_terminal_child(
    node: &EmbeddedNode,
    agent_did: &str,
    row: &RunningToolCallRow,
) -> Result<bool> {
    let Some(child_request_id) = child_request_id(row) else {
        return Ok(false);
    };
    let Some(child) =
        crate::background_tools::load_child_terminal_row(node, child_request_id).await?
    else {
        return Ok(false);
    };

    if child_request_completed(&child) {
        let result = load_child_completion_result(node, child_request_id)
            .await?
            .unwrap_or_else(|| format!("child request {child_request_id} completed"));
        recover_bridge_completed_row(node, row, &result).await?;
        ensure_background_subagent_projection_side_effects(node, agent_did, row, child_request_id)
            .await?;
        return Ok(true);
    }

    let Some(terminal) = project_child_terminal(&child) else {
        return Ok(false);
    };
    recover_bridge_failed_row(node, row, &terminal).await?;
    ensure_background_subagent_projection_side_effects(node, agent_did, row, child_request_id)
        .await?;
    Ok(true)
}

async fn ensure_background_subagent_projection_side_effects(
    node: &EmbeddedNode,
    agent_did: &str,
    row: &RunningToolCallRow,
    child_request_id: &str,
) -> Result<()> {
    if !is_background_subagent_tool(row) {
        return Ok(());
    }
    let outcome =
        ensure_background_subagent_completion_side_effects(node, child_request_id, agent_did)
            .await?;
    tracing::debug!(
        doc_id = %row.doc_id,
        request_id = row.request_id.as_deref().unwrap_or(""),
        session_id = %row.session_id,
        tool_call_id = %row.tool_call_id,
        child_request_id,
        outcome = ?outcome,
        "ensured recovered background subagent projection side effects"
    );
    Ok(())
}

async fn terminalize_expired_local_child_request(
    node: &EmbeddedNode,
    agent_did: &str,
    row: &RunningToolCallRow,
) -> Result<bool> {
    let Some(child_request_id) = child_request_id(row) else {
        return Ok(false);
    };
    let Some(child) = load_request_liveness_row(node, child_request_id).await? else {
        return Ok(false);
    };
    terminalize_expired_child_with_row(node, agent_did, row, &child).await
}

/// `terminalize_expired_local_child_request` over a preloaded child liveness
/// row, so the periodic sweep can batch the reads.
async fn terminalize_expired_child_with_row(
    node: &EmbeddedNode,
    agent_did: &str,
    row: &RunningToolCallRow,
    child: &ChildRequestLivenessRow,
) -> Result<bool> {
    let Some(child_request_id) = child_request_id(row) else {
        return Ok(false);
    };
    if child.agent_did != agent_did {
        return Ok(false);
    }
    if request_status_or_lifecycle_is_terminal(
        child.status.as_deref(),
        child.lifecycle_state.as_deref(),
    ) {
        return Ok(false);
    }
    let Some(deadline_at) = parse_datetime(child.deadline.as_deref()) else {
        return Ok(false);
    };
    if Utc::now() < deadline_at {
        return Ok(false);
    }

    let reason = format!(
        "child request deadline exceeded at {} before terminal response",
        deadline_at.to_rfc3339()
    );
    if !mark_child_request_dead(node, child, &reason).await? {
        return Ok(false);
    }
    finalize_streaming_child_response(node, child_request_id, &reason).await?;
    tracing::info!(
        doc_id = %row.doc_id,
        request_id = row.request_id.as_deref().unwrap_or(""),
        session_id = %row.session_id,
        tool_call_id = %row.tool_call_id,
        child_request_id,
        child_deadline_at = %deadline_at,
        "terminalized expired subagent child request during tool-call recovery"
    );
    Ok(true)
}

async fn load_request_liveness_row(
    node: &EmbeddedNode,
    request_id: &str,
) -> Result<Option<ChildRequestLivenessRow>> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                limit: 1
            ) {{
                _docID
                request_id
                agent_did
                status
                lifecycle_state
                deadline
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query AgentRequest liveness for {request_id} failed: {:?}",
            response.errors
        );
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| {
            serde_json::from_value::<Vec<ChildRequestLivenessRow>>(value.clone()).ok()
        })
        .and_then(|mut rows| rows.pop()))
}

/// Batched form of `load_child_liveness_row`: one `_in` query for every
/// bridge's child on the periodic tick, keyed by `request_id`.
async fn load_child_liveness_rows(
    node: &EmbeddedNode,
    child_request_ids: &[&str],
) -> Result<std::collections::HashMap<String, ChildRequestLivenessRow>> {
    if child_request_ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let id_list = child_request_ids
        .iter()
        .map(|id| format!("\"{}\"", escape_graphql_string(id)))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _in: [{id_list}] }} }}
            ) {{
                _docID
                request_id
                agent_did
                status
                lifecycle_state
                deadline
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "batched child AgentRequest liveness query failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<ChildRequestLivenessRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .map(|row| (row.request_id.clone(), row))
        .collect())
}

async fn mark_child_request_dead(
    node: &EmbeddedNode,
    child: &ChildRequestLivenessRow,
    reason: &str,
) -> Result<bool> {
    let active_runtime_states = crate::lifecycle::active_runtime_lifecycle_state_graphql_list();
    let escaped_doc_id = escape_graphql_string(&child.doc_id);
    let escaped_agent_did = escape_graphql_string(&child.agent_did);
    let escaped_reason = escape_graphql_string(reason);
    let terminalized_at = escape_graphql_string(&Utc::now().to_rfc3339());
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{
                    _docID: {{ _eq: "{escaped_doc_id}" }},
                    agent_did: {{ _eq: "{escaped_agent_did}" }},
                    lifecycle_state: {{ _in: {active_runtime_states} }},
                    status: {{ _nin: ["completed", "interrupted", "dead", "superseded", "error"] }}
                }},
                input: {{
                    status: "dead",
                    lifecycle_state: "dead",
                    failure_reason: "{escaped_reason}",
                    terminalized_at: "{terminalized_at}",
                    terminal_redrive_attempts: 0
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = crate::retry::execute_graphql_with_terminal_persistence_retry(
        node,
        &mutation,
        "terminalize_expired_child_request",
    )
    .await?;
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get("update_AgentRequest"))
        .is_some_and(response_has_documents))
}

async fn finalize_streaming_child_response(
    node: &EmbeddedNode,
    child_request_id: &str,
    reason: &str,
) -> Result<()> {
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{
                    request_id: {{ _eq: "{escaped_child_request_id}" }},
                    status: {{ _eq: "streaming" }}
                }}
            ) {{
                _docID
                content
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query streaming child AgentResponse {child_request_id} failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<StreamingChildResponseRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentResponse"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    let now = Utc::now().to_rfc3339();
    let escaped_reason = escape_graphql_string(reason);
    for row in rows {
        let content = row.content.unwrap_or_default();
        let final_content = if content.trim().is_empty() {
            format!("Error: {reason}")
        } else {
            format!("{content}\n\n[Response interrupted - {reason}]")
        };
        let escaped_doc_id = escape_graphql_string(&row.doc_id);
        let escaped_content = escape_graphql_string(&final_content);
        let escaped_now = escape_graphql_string(&now);
        let mutation = format!(
            r#"mutation {{
                update_AgentResponse(
                    filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                    input: {{
                        content: "{escaped_content}",
                        status: "error",
                        error_message: "{escaped_reason}",
                        completed_at: "{escaped_now}"
                    }}
                ) {{ _docID }}
            }}"#
        );
        execute_mutation_with_retry(node, &mutation, "finalize_expired_child_response").await?;
    }
    Ok(())
}

async fn load_child_completion_result(
    node: &EmbeddedNode,
    child_request_id: &str,
) -> Result<Option<String>> {
    #[derive(Deserialize)]
    struct ChildIdentityRow {
        #[serde(rename = "_docID")]
        doc_id: String,
        request_id: String,
        session_id: String,
        agent_did: String,
    }

    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_child_request_id}" }} }}
            ) {{
                _docID
                request_id
                session_id
                agent_did
            }}
        }}"#
    );
    let reader_did = node.node_identity_did().ok_or_else(|| {
        anyhow::anyhow!("child completion recovery requires a DefraDB query identity")
    })?;
    let identity = Did::new(reader_did).context("parsing child completion reader DID")?;
    let response = node
        .execute_request_with_retry(
            QueryRequest::new(query).with_identity(Some(identity)),
            ExecuteRetryPolicy::default(),
        )
        .await;
    if response.has_errors() {
        anyhow::bail!(
            "query child AgentRequest {child_request_id} identity for bridge recovery failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<ChildIdentityRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    let child = match rows.as_slice() {
        [] => return Ok(None),
        [child] => child,
        rows => anyhow::bail!(
            "child AgentRequest {child_request_id} resolved to {} physical documents during bridge recovery",
            rows.len()
        ),
    };
    let Some(message) = crate::response_outcome::load_verified_complete_response_message(
        node,
        &child.agent_did,
        &child.doc_id,
        &child.request_id,
        &child.session_id,
    )
    .await?
    else {
        return Ok(None);
    };
    tracing::debug!(
        child_request_doc_id = %child.doc_id,
        final_message_doc_id = %message.fact.doc_id,
        final_message_composite_commit_cid = %message.fact.composite_commit_cid,
        "recovered bridge from verified immutable child completion"
    );
    Ok(Some(render_assistant_message_text(&message.content)?))
}

async fn recover_bridge_completed_row(
    node: &EmbeddedNode,
    row: &RunningToolCallRow,
    child_result: &str,
) -> Result<()> {
    let now = Utc::now();
    let started_at = parse_datetime(row.started_at.as_deref()).unwrap_or(now);
    let deadline_at = parse_datetime(row.deadline_at.as_deref()).unwrap_or(now);
    let latency_ms = (now - started_at).num_milliseconds().max(0);
    let escaped_doc_id = escape_graphql_string(&row.doc_id);
    let escaped_result = escape_graphql_string(child_result);
    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{
                    _docID: {{ _eq: "{escaped_doc_id}" }},
                    lifecycle_state: {{ _eq: "running" }}
                }},
                input: {{
                    result: "{escaped_result}",
                    status: "completed",
                    lifecycle_state: "completed",
                    started_at: "{started_at}",
                    deadline_at: "{deadline_at}",
                    completed_at: "{completed_at}",
                    latency_ms: {latency_ms},
                    unclaimed_deadline_at: null
                }}
            ) {{ _docID }}
        }}"#,
        started_at = started_at.to_rfc3339(),
        deadline_at = deadline_at.to_rfc3339(),
        completed_at = now.to_rfc3339(),
    );

    execute_mutation_with_retry(node, &mutation, "recover_bridge_completed_child")
        .await
        .context("recover bridge completed child mutation")?;
    Ok(())
}

async fn recover_bridge_failed_row(
    node: &EmbeddedNode,
    row: &RunningToolCallRow,
    terminal: &ChildTerminal,
) -> Result<()> {
    let now = Utc::now();
    let started_at = parse_datetime(row.started_at.as_deref()).unwrap_or(now);
    let deadline_at = parse_datetime(row.deadline_at.as_deref()).unwrap_or(now);
    let latency_ms = (now - started_at).num_milliseconds().max(0);
    let escaped_doc_id = escape_graphql_string(&row.doc_id);
    let projected = terminal.projected_state().as_str();
    let cancel_cause_field = if terminal.projected_state() == ToolCallState::Cancelled {
        let cause = row
            .cancel_cause
            .as_deref()
            .and_then(CancelCause::from_persisted)
            .unwrap_or(CancelCause::Interrupted)
            .as_str();
        format!(r#"cancel_cause: "{cause}","#)
    } else {
        String::new()
    };
    let optional_fields = match terminal {
        ChildTerminal::Failed {
            reason,
            failure_class,
        } => {
            let escaped_reason = escape_graphql_string(reason);
            let failure_class = failure_class.as_str();
            format!(
                r#"result: "{escaped_reason}",
                    tool_failure_class: "{failure_class}","#
            )
        }
        _ => String::new(),
    };
    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{
                    _docID: {{ _eq: "{escaped_doc_id}" }},
                    lifecycle_state: {{ _eq: "running" }}
                }},
                input: {{
                    {optional_fields}
                    {cancel_cause_field}
                    status: "completed",
                    lifecycle_state: "{projected}",
                    started_at: "{started_at}",
                    deadline_at: "{deadline_at}",
                    completed_at: "{completed_at}",
                    latency_ms: {latency_ms},
                    unclaimed_deadline_at: null
                }}
            ) {{ _docID }}
        }}"#,
        started_at = started_at.to_rfc3339(),
        deadline_at = deadline_at.to_rfc3339(),
        completed_at = now.to_rfc3339(),
    );

    execute_mutation_with_retry(node, &mutation, "recover_bridge_terminal_child")
        .await
        .context("recover bridge terminal child mutation")?;
    Ok(())
}

/// Terminalize a running tool-call row. Returns `Ok(true)` when the
/// compare-and-set updated the row, `Ok(false)` when a concurrent writer
/// already left `running` (first terminal wins — do not overwrite).
async fn recover_tool_call_row(
    node: &EmbeddedNode,
    row: &RunningToolCallRow,
    deadline_at: Option<DateTime<Utc>>,
    outcome: RecoveryOutcome,
    completion_side_effects_owed: bool,
    remote_cancel_intent_at: Option<DateTime<Utc>>,
) -> Result<bool> {
    let now = Utc::now();
    let started_at = parse_datetime(row.started_at.as_deref()).unwrap_or(now);
    let latency_ms = (now - started_at).num_milliseconds().max(0);
    let escaped_doc_id = escape_graphql_string(&row.doc_id);
    let escaped_result = escape_graphql_string(&outcome.result_text(deadline_at));
    let started_at_str = started_at.to_rfc3339();
    let completed_at_str = now.to_rfc3339();
    let deadline_field = deadline_at
        .map(|deadline| format!(r#", deadline_at: "{}""#, deadline.to_rfc3339()))
        .unwrap_or_default();
    let failure_class_field = outcome
        .failure_class()
        .map(|failure| format!(r#", tool_failure_class: "{}""#, failure.as_str()))
        .unwrap_or_default();
    let cancel_cause_field = outcome
        .cancel_cause(row.cancel_cause.as_deref())
        .map(|cause| format!(r#", cancel_cause: "{}""#, cause.as_str()))
        .unwrap_or_default();
    let remote_cancel_intent_fields = remote_cancel_intent_at
        .map(|at| {
            format!(
                r#", cancel_cascade_intent_at: "{}", cancel_pending_remote_ack: true"#,
                escape_graphql_string(&at.to_rfc3339())
            )
        })
        .unwrap_or_default();
    let terminal_status = if is_background_tool_row(row) && completion_side_effects_owed {
        format!("completionPending:{}", outcome.notification_reason())
    } else {
        "completed".to_string()
    };

    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{
                    _docID: {{ _eq: "{escaped_doc_id}" }},
                    lifecycle_state: {{ _eq: "running" }}
                }},
                input: {{
                    result: "{escaped_result}",
                    status: "{terminal_status}",
                    lifecycle_state: "{lifecycle_state}",
                    started_at: "{started_at_str}"{deadline_field},
                    completed_at: "{completed_at_str}",
                    latency_ms: {latency_ms},
                    unclaimed_deadline_at: null{failure_class_field}{cancel_cause_field}{remote_cancel_intent_fields}
                }}
            ) {{ _docID }}
        }}"#,
        lifecycle_state = outcome.lifecycle_state().as_str(),
    );

    let response = execute_mutation_with_retry(node, &mutation, "recover_running_tool_call")
        .await
        .context("recover running tool call mutation")?;
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get("update_AgentToolCall"))
        .is_some_and(response_has_documents))
}

fn parse_datetime(value: Option<&str>) -> Option<DateTime<Utc>> {
    non_empty(value)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|datetime| datetime.with_timezone(&Utc))
}

/// Shared startup/periodic classifier. Branch order is Lean-fenced by
/// `restartDisposition` and `orphanedBackgroundToolCause`.
fn classify_running_tool_recovery(
    row: &RunningToolCallRow,
    parent: Option<&ParentRequestRow>,
    now: DateTime<Utc>,
) -> Option<RecoveryOutcome> {
    if parse_datetime(row.deadline_at.as_deref()).is_some_and(|deadline| now >= deadline) {
        Some(RecoveryOutcome::TimedOut)
    } else if parse_datetime(row.unclaimed_deadline_at.as_deref())
        .is_some_and(|deadline| now >= deadline)
    {
        Some(RecoveryOutcome::UnclaimedCrossDeploymentSpawn)
    } else if is_background_tool_row(row)
        && parent.is_some_and(|parent| !request_is_terminal(parent))
    {
        Some(RecoveryOutcome::BackgroundInterrupted)
    } else if is_detached_subagent_tool(row) && parent.is_some_and(request_is_interrupted) {
        None
    } else if parent.is_some_and(request_is_cleanly_completed) && child_request_id(row).is_some() {
        None
    } else if parent.is_some_and(request_is_interrupted) {
        Some(RecoveryOutcome::Cancelled)
    } else if parent.is_some_and(request_is_terminal) {
        Some(RecoveryOutcome::Failed)
    } else {
        None
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

fn effective_deadline(
    tool_deadline: Option<&str>,
    args_deadline: Option<&str>,
) -> Option<DateTime<Utc>> {
    match (parse_datetime(tool_deadline), parse_datetime(args_deadline)) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn request_is_interrupted(parent: &ParentRequestRow) -> bool {
    parent.status == "interrupted" || parent.lifecycle_state.as_deref() == Some("interrupted")
}

/// Cancel-worthy evidence on either field takes precedence over a divergent
/// `completed` sibling field. Mirrors
/// `subagent_source::parent_reached_cancel_worthy_terminal` (plus `failed` on
/// status for rows that stamp that vocabulary).
fn request_has_cancel_worthy_field(parent: &ParentRequestRow) -> bool {
    matches!(
        parent.status.as_str(),
        "error" | "failed" | "superseded" | "dead" | "interrupted"
    ) || matches!(
        parent.lifecycle_state.as_deref(),
        Some("failed" | "error" | "superseded" | "dead" | "interrupted")
    )
}

/// Parent reached a successful terminal **without** any cancel-worthy field
/// evidence — not a cancel signal for linked background/cascade children.
///
/// Divergent replications such as `status=completed` + `lifecycle_state=interrupted`
/// are **not** clean: cancel-worthy evidence wins.
fn request_is_cleanly_completed(parent: &ParentRequestRow) -> bool {
    if request_has_cancel_worthy_field(parent) {
        return false;
    }
    matches!(parent.status.as_str(), "completed" | "complete")
        || matches!(
            parent.lifecycle_state.as_deref(),
            Some("completed" | "complete")
        )
}

/// Terminal parent whose terminal is cancel-worthy (interrupt, failure, dead,
/// supersede, …). Cancel-worthy field evidence is checked first so a stale
/// `completed` on the other column cannot suppress cascade.
fn request_is_cancel_worthy_terminal(parent: &ParentRequestRow) -> bool {
    request_has_cancel_worthy_field(parent)
}

fn request_is_terminal(parent: &ParentRequestRow) -> bool {
    request_status_or_lifecycle_is_terminal(
        Some(parent.status.as_str()),
        parent.lifecycle_state.as_deref(),
    )
}

fn request_status_or_lifecycle_is_terminal(
    status: Option<&str>,
    lifecycle_state: Option<&str>,
) -> bool {
    matches!(
        status,
        Some("completed" | "complete" | "error" | "failed" | "superseded" | "dead" | "interrupted")
    ) || matches!(
        lifecycle_state,
        Some("completed" | "complete" | "failed" | "error" | "superseded" | "dead" | "interrupted")
    )
}

fn child_request_id(row: &RunningToolCallRow) -> Option<&str> {
    row.child_request_id.as_deref().filter(|id| !id.is_empty())
}

fn cancel_policy(row: &RunningToolCallRow) -> CancelPolicy {
    row.cancel_policy
        .as_deref()
        .and_then(CancelPolicy::from_persisted)
        .unwrap_or(CancelPolicy::Cascade)
}

fn await_mode(row: &RunningToolCallRow) -> AwaitMode {
    row.await_mode
        .as_deref()
        .and_then(AwaitMode::from_persisted)
        .unwrap_or(AwaitMode::Foreground)
}

fn subagent_tool_name(row: &RunningToolCallRow) -> &str {
    row.tool_name
        .as_str()
        .trim()
        .is_empty()
        .then_some("spawn_subagent")
        .unwrap_or(row.tool_name.as_str())
}

fn is_background_subagent_tool(row: &RunningToolCallRow) -> bool {
    child_request_id(row).is_some() && await_mode(row) == AwaitMode::Background
}

fn is_background_tool_row(row: &RunningToolCallRow) -> bool {
    child_request_id(row).is_none() && await_mode(row) == AwaitMode::Background
}

fn is_detached_subagent_tool(row: &RunningToolCallRow) -> bool {
    child_request_id(row).is_some() && cancel_policy(row) == CancelPolicy::Detach
}

fn cascade_child_request_id(row: &RunningToolCallRow) -> Option<&str> {
    let child_request_id = child_request_id(row)?;
    (cancel_policy(row) == CancelPolicy::Cascade).then_some(child_request_id)
}

async fn child_request_is_locally_owned(
    node: &EmbeddedNode,
    local_did: &str,
    child_request_id: &str,
) -> Result<bool> {
    let escaped = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped}" }} }},
                limit: 1
            ) {{ agent_did }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query AgentRequest for recovery cascade ownership failed: {:?}",
            response.errors
        );
    }
    let did = response
        .data
        .as_ref()
        .and_then(|d| d.get("AgentRequest"))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("agent_did"))
        .and_then(|v| v.as_str());
    Ok(did == Some(local_did))
}

impl RecoveryOutcome {
    fn notification_reason(self) -> &'static str {
        match self {
            Self::TimedOut => "deadline_exceeded",
            Self::Cancelled => "parent_interrupted",
            Self::Failed => "parent_terminal",
            Self::BackgroundInterrupted => "interrupted_on_restart",
            Self::UnclaimedCrossDeploymentSpawn => "unclaimed_spawn_timeout",
        }
    }

    fn lifecycle_state(self) -> ToolCallState {
        match self {
            Self::TimedOut => ToolCallState::TimedOut,
            Self::Cancelled | Self::BackgroundInterrupted => ToolCallState::Cancelled,
            Self::Failed | Self::UnclaimedCrossDeploymentSpawn => ToolCallState::Failed,
        }
    }

    fn failure_class(self) -> Option<FailureClass> {
        match self {
            Self::TimedOut | Self::Failed => Some(FailureClass::External),
            Self::UnclaimedCrossDeploymentSpawn => Some(FailureClass::ServiceUnavailable),
            Self::Cancelled | Self::BackgroundInterrupted => None,
        }
    }

    fn result_text(self, deadline_at: Option<DateTime<Utc>>) -> String {
        match self {
            Self::TimedOut => match deadline_at {
                Some(deadline_at) => {
                    format!(
                        "tool call deadline exceeded at {}",
                        deadline_at.to_rfc3339()
                    )
                }
                None => "tool call deadline exceeded".to_string(),
            },
            Self::Cancelled => {
                "tool call cancelled because parent request was interrupted".to_string()
            }
            Self::BackgroundInterrupted => {
                "backgrounded tool call interrupted on restart".to_string()
            }
            Self::Failed => {
                "tool call failed because parent request was already terminal".to_string()
            }
            Self::UnclaimedCrossDeploymentSpawn => {
                "no peer claimed subagent spawn before the unclaimed spawn deadline".to_string()
            }
        }
    }

    fn cancel_cause(self, persisted: Option<&str>) -> Option<CancelCause> {
        persisted
            .and_then(CancelCause::from_persisted)
            .or(match self {
                Self::TimedOut => Some(CancelCause::Deadline),
                Self::Cancelled | Self::BackgroundInterrupted => Some(CancelCause::Interrupted),
                Self::Failed | Self::UnclaimedCrossDeploymentSpawn => None,
            })
    }
}
