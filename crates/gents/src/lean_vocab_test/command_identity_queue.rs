use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct LeanCommandPolicyCase {
    pub(crate) name: String,
    pub(crate) category: String,
    pub(crate) mode: String,
    pub(crate) allowed_argv_prefixes: Vec<Vec<String>>,
    pub(crate) forbidden_argv_prefixes: Vec<Vec<String>>,
    pub(crate) network_mode: String,
    pub(crate) read_only_allowlist: Vec<String>,
    pub(crate) command: String,
    pub(crate) lookup_command: String,
    pub(crate) args: Vec<String>,
    pub(crate) decision: String,
    pub(crate) denial_reason: Option<String>,
    pub(crate) matched_prefix: Option<Vec<String>>,
    pub(crate) denied_argv: Option<Vec<String>>,
    pub(crate) denied_command: Option<String>,
    pub(crate) denied_argument: Option<String>,
    pub(crate) denied_subcommand: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanCommandSandboxCase {
    pub(crate) name: String,
    pub(crate) category: String,
    pub(crate) mode: String,
    pub(crate) workspace_write_sandbox_enforced: bool,
    pub(crate) decision: String,
    pub(crate) sandbox: Option<String>,
    pub(crate) denial_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LeanCommandEnvCase {
    pub(crate) name: String,
    pub(crate) env_key: String,
    pub(crate) input_present: bool,
    pub(crate) input_name: String,
    pub(crate) input_value: String,
    pub(crate) output_name: String,
    pub(crate) expected_value_kind: Option<String>,
    pub(crate) expected_output_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanLiveOverlayCase {
    pub(crate) name: String,
    #[serde(rename = "responseStatus")]
    pub(crate) response_status: String,
    pub(crate) materialized: bool,
    #[serde(rename = "precedingToolCalls")]
    pub(crate) preceding_tool_calls: u64,
    #[serde(rename = "turnTerminal")]
    pub(crate) turn_terminal: bool,
    #[serde(rename = "turnLabel")]
    pub(crate) turn_label: String,
    #[serde(rename = "hasContent")]
    pub(crate) has_content: bool,
    #[serde(rename = "hasReasoning")]
    pub(crate) has_reasoning: bool,
    #[serde(rename = "expectOverlay")]
    pub(crate) expect_overlay: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanIdentityPrincipal {
    pub(crate) did: String,
    pub(crate) enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanIdentityBehavior {
    pub(crate) id: String,
    pub(crate) principal: String,
    pub(crate) enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanIdentityDeployment {
    pub(crate) id: String,
    pub(crate) principal: String,
    pub(crate) host_id: String,
    pub(crate) enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanIdentityStructuralCase {
    pub(crate) name: String,
    pub(crate) principals: Vec<LeanIdentityPrincipal>,
    pub(crate) behaviors: Vec<LeanIdentityBehavior>,
    pub(crate) deployments: Vec<LeanIdentityDeployment>,
    pub(crate) well_formed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanIdentityPermissionGrant {
    pub(crate) principal: String,
    pub(crate) permission: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanIdentityPermissionCase {
    pub(crate) name: String,
    pub(crate) principals: Vec<LeanIdentityPrincipal>,
    pub(crate) behaviors: Vec<LeanIdentityBehavior>,
    pub(crate) deployments: Vec<LeanIdentityDeployment>,
    pub(crate) grants: Vec<LeanIdentityPermissionGrant>,
    pub(crate) permission: String,
    pub(crate) row_owner: String,
    pub(crate) actor_behavior: String,
    pub(crate) peer_behavior: String,
    pub(crate) expected_actor_principal: String,
    pub(crate) expected_peer_principal: String,
    pub(crate) expected_actor_allowed: bool,
    pub(crate) expected_peer_allowed: bool,
    pub(crate) same_principal: bool,
    pub(crate) expected_decisions_equal: bool,
    pub(crate) host_deployment: String,
    pub(crate) expected_actor_hostable: bool,
    pub(crate) expected_peer_hostable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanIdentityContract {
    pub(crate) name: String,
    pub(crate) statement: String,
    pub(crate) enforced: bool,
    pub(crate) tracked_by: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanQueueDeadlineConformanceCase {
    pub(crate) name: String,
    pub(crate) group: String,
    pub(crate) action: String,
    pub(crate) session_id: usize,
    pub(crate) legal: bool,
    pub(crate) pre_active_request_id: Option<usize>,
    pub(crate) post_active_request_id: Option<usize>,
    pub(crate) pre_pending_request_ids: Vec<usize>,
    pub(crate) post_pending_request_ids: Vec<usize>,
    pub(crate) claimed_request_id: Option<usize>,
    pub(crate) blocked_by_active: bool,
    pub(crate) superseded_request_ids: Vec<usize>,
    pub(crate) queue_key: Option<String>,
    pub(crate) post_coalesced_pending_count: usize,
    pub(crate) automated_drained_request_ids: Vec<usize>,
    pub(crate) preserved_user_pending_request_ids: Vec<usize>,
    pub(crate) post_terminal_request_ids: Vec<usize>,
    pub(crate) pre_request_deadline: Option<usize>,
    pub(crate) synthesized_claim_deadline: Option<usize>,
    pub(crate) post_deadline: Option<usize>,
    pub(crate) explicit_deadline_preserved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanRecoverySweepCase {
    pub(crate) name: String,
    pub(crate) sweep_id: String,
    pub(crate) collection: String,
    pub(crate) rust_function: String,
    pub(crate) cadence: String,
    pub(crate) implementation_status: String,
    pub(crate) pre_state: String,
    pub(crate) terminal_state: String,
    pub(crate) measure_before: usize,
    pub(crate) measure_after: usize,
    pub(crate) deadline_expired: Option<bool>,
    pub(crate) unclaimed_expired: Option<bool>,
    pub(crate) parent_live: Option<bool>,
    pub(crate) parent_interrupted: Option<bool>,
    pub(crate) parent_terminal: Option<bool>,
    pub(crate) execution_registered: Option<bool>,
    pub(crate) recovery_cause: Option<String>,
    pub(crate) notification_reason: Option<String>,
    pub(crate) deadline_audit_ref: String,
}

/// Startup restart-disposition witness (#937): the shape of one running
/// `AgentToolCall` row and what `ToolCallLifecycle::recover_all` must do with
/// it — terminalize with a pinned cause/terminal state (plus, for the native
/// background interrupt, a durable notification and coalesced wake), or leave
/// the row running.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanRestartDispositionCase {
    pub(crate) name: String,
    pub(crate) rust_function: String,
    pub(crate) await_mode: String,
    pub(crate) cancel_policy: String,
    pub(crate) child_linked: bool,
    pub(crate) parent_observation: String,
    pub(crate) deadline_expired: bool,
    pub(crate) unclaimed_expired: bool,
    pub(crate) disposition: String,
    pub(crate) cause: Option<String>,
    pub(crate) terminal_state: Option<String>,
    pub(crate) notification_reason: Option<String>,
    pub(crate) queue_source: Option<String>,
    pub(crate) queue_key_prefix: Option<String>,
    #[allow(dead_code)]
    pub(crate) theorem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct LeanRecoveryEquivalenceCase {
    pub(crate) name: String,
    pub(crate) source_sweep_case: String,
    pub(crate) sweep_id: String,
    pub(crate) collection: String,
    pub(crate) rust_function: String,
    pub(crate) cadence: String,
    pub(crate) pre_state: String,
    pub(crate) recovered_state: String,
    pub(crate) uninterrupted_state: String,
    pub(crate) equivalent: bool,
    pub(crate) reexecutes: bool,
    pub(crate) can_hang: bool,
    pub(crate) theorem: String,
    pub(crate) aggregate_theorem: String,
}
