//! Background conformance home: R6 tool backgrounding, background-theorem
//! witnesses (admission budget, cascade cancellation), subagent delegation
//! graph, and R4c background-work observable shapes.

use super::*;

const BACKGROUND_THEOREM_PARENT_BEHAVIOR_ID: &str = "r6-background-theorem-parent";
const BACKGROUND_THEOREM_CHILD_BEHAVIOR_ID: &str = "r6-background-theorem-child";

struct PendingTool;

impl ToolDyn for PendingTool {
    fn name(&self) -> String {
        "slow_tool".to_string()
    }

    fn definition<'a>(&'a self, _prompt: String) -> BoxFuture<'a, ToolDefinition> {
        Box::pin(async {
            ToolDefinition {
                name: "slow_tool".to_string(),
                description: "test tool".to_string(),
                parameters: json!({"type":"object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        Box::pin(std::future::pending())
    }
}

#[derive(Debug, Deserialize)]
struct BackgroundTheoremToolCallRow {
    await_mode: Option<String>,
    cancel_policy: Option<String>,
    child_request_id: Option<String>,
    lifecycle_state: Option<String>,
    result: Option<String>,
    cancel_cause: Option<String>,
    cancel_cascade_intent_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BackgroundedRow {
    lifecycle_state: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct BackgroundTheoremChildRequestRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    request_id: String,
    agent_did: String,
    requester_did: Option<String>,
    behavior_id: Option<String>,
    session_id: String,
    content: String,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<i64>,
    seed: Option<i64>,
    max_tokens: Option<i64>,
    max_total_tokens: Option<i64>,
    metadata: Option<String>,
    execution_origin: Option<String>,
    created_at: String,
    deadline: Option<String>,
    subagent_depth: Option<u32>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_request_doc_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
    caused_by_parent_tool_call_doc_id: Option<String>,
    status: String,
    lifecycle_state: Option<String>,
}

impl BackgroundTheoremChildRequestRow {
    fn into_agent_request(self) -> gents::watcher::AgentRequest {
        gents::watcher::AgentRequest {
            doc_id: self.doc_id,
            request_id: self.request_id,
            agent_did: self.agent_did,
            requester_did: self.requester_did,
            behavior_id: self
                .behavior_id
                .and_then(|value| (!value.trim().is_empty()).then_some(value)),
            session_id: self.session_id,
            content: self.content,
            temperature: self.temperature,
            top_p: self.top_p,
            top_k: self.top_k,
            seed: self.seed,
            max_tokens: self.max_tokens,
            max_total_tokens: self.max_total_tokens,
            metadata: self.metadata,
            execution_origin: self
                .execution_origin
                .and_then(|value| (!value.trim().is_empty()).then_some(value)),
            created_at: self.created_at,
            deadline: self
                .deadline
                .and_then(|value| (!value.trim().is_empty()).then_some(value)),
            subagent_depth: self.subagent_depth.unwrap_or(0),
            caused_by_parent_request_id: self.caused_by_parent_request_id,
            caused_by_parent_request_doc_id: self.caused_by_parent_request_doc_id,
            caused_by_parent_tool_call_id: self.caused_by_parent_tool_call_id,
            caused_by_parent_tool_call_doc_id: self.caused_by_parent_tool_call_doc_id,
            caused_by_trigger_id: None,
            caused_by_trigger_kind: None,
            caused_by_source_doc_id: None,
            caused_by_correlation: None,
            caused_by_trigger_context: None,
            workspace_id: None,
            workspace_authority: None,
            workspace_owner_deployment_id: None,
            workspace_seal_hash: None,
        }
    }
}

fn background_tool_registry(
    tools: Vec<Box<dyn ToolDyn>>,
    allowlist: &[&str],
) -> BackgroundToolRegistry {
    BackgroundToolRegistry::from_tools(
        tools,
        &allowlist
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>(),
    )
}

fn skip_reason_json(action: ToolCallHookAction) -> Value {
    let ToolCallHookAction::Skip { reason } = action else {
        panic!("expected Skip action, got {action:?}");
    };
    serde_json::from_str(&reason).expect("skip reason should be JSON")
}

async fn setup_background_tool_hook(
    test_name: &str,
    registry: BackgroundToolRegistry,
) -> (support::TestDb, DefraSessionHook, String, String) {
    let db = test_db(test_name).await;
    let session_id = format!("{test_name}-session");
    let request_id = format!("{test_name}-request");
    support::create_request(
        db.node.as_ref(),
        &request_id,
        &session_id,
        "processing",
        "2026-05-19T00:00:00Z",
    )
    .await;

    let hook = DefraSessionHook::resume_or_create_with_identity_policy(
        db.node.clone(),
        &session_id,
        "r6-background-theorem",
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .expect("resume background theorem hook")
    .with_background_tool_registry(registry);
    hook.set_active_request_id(Some(request_id.clone())).await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::minutes(5)))
        .await;
    (db, hook, session_id, request_id)
}

async fn setup_background_spawn_fixture(
    test_name: &str,
    targets: Vec<&str>,
    parent_subagent_depth: u32,
    background_enabled: bool,
) -> (
    support::TestDb,
    DefraSessionHook,
    String,
    String,
    chrono::DateTime<chrono::Utc>,
) {
    let db = test_db(test_name).await;
    let parent_deadline = chrono::Utc::now() + chrono::Duration::minutes(5);
    let selection_id = format!("{test_name}-tools");

    upsert_tool_selection(
        db.node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: AGENT_DID.to_string(),
            subagent_targets: Some(
                targets
                    .into_iter()
                    .map(|behavior_id| {
                        gents::subagent_target_entry(behavior_id, AGENT_DID, behavior_id, None)
                    })
                    .collect(),
            ),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(background_enabled),
            ..Default::default()
        },
    )
    .await
    .expect("upsert theorem tool selection");
    upsert_agent_behavior(
        db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: BACKGROUND_THEOREM_PARENT_BEHAVIOR_ID.to_string(),
            agent_did: AGENT_DID.to_string(),
            display_name: Some("R6 theorem parent".to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: Some(selection_id),
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            enabled: true,
            created_at: Some("2026-05-19T00:00:00Z".to_string()),
        },
    )
    .await
    .expect("upsert theorem parent behavior");
    upsert_agent_behavior(
        db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: BACKGROUND_THEOREM_CHILD_BEHAVIOR_ID.to_string(),
            agent_did: AGENT_DID.to_string(),
            display_name: Some("R6 theorem child".to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: None,
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            enabled: true,
            created_at: Some("2026-05-19T00:00:01Z".to_string()),
        },
    )
    .await
    .expect("upsert theorem child behavior");

    let session_id = format!("{test_name}-session");
    let request_id = format!("{test_name}-parent");
    create_background_theorem_parent_request(
        db.node.as_ref(),
        &request_id,
        &session_id,
        parent_subagent_depth,
        parent_deadline,
    )
    .await;

    let hook = DefraSessionHook::resume_or_create_with_identity_policy(
        db.node.clone(),
        &session_id,
        BACKGROUND_THEOREM_PARENT_BEHAVIOR_ID,
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .expect("resume background theorem parent hook");
    let request_doc_id = crate::support::exact_request_doc_id(db.node.as_ref(), &request_id).await;
    hook.set_active_request_binding(Some(request_id.clone()), Some(request_doc_id), None)
        .await;
    hook.set_request_deadline_at(Some(parent_deadline)).await;

    (db, hook, session_id, request_id, parent_deadline)
}

async fn create_background_theorem_parent_request(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    subagent_depth: u32,
    deadline: chrono::DateTime<chrono::Utc>,
) {
    let request_id = escape_graphql_string(request_id);
    let session_id = escape_graphql_string(session_id);
    let behavior_id = escape_graphql_string(BACKGROUND_THEOREM_PARENT_BEHAVIOR_ID);
    let agent_did = escape_graphql_string(AGENT_DID);
    let created_at = chrono::Utc::now().to_rfc3339();
    let deadline = deadline.to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "{agent_did}",
                behavior_id: "{behavior_id}",
                session_id: "{session_id}",
                retry_parent_request: "",
                retry_root_request: "{request_id}",
                superseded_by_request: "",
                content: "parent prompt",
                status: "processing",
                lifecycle_state: "processing",
                backend_id: "",
                execution_origin: "interactive",
                metadata: "",
                failure_reason: "",
                created_at: "{created_at}",
                deadline: "{deadline}",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: {subagent_depth}
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create background theorem parent AgentRequest failed: {:?}",
        response.errors
    );
}

async fn fetch_background_theorem_tool_call(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
) -> BackgroundTheoremToolCallRow {
    let session_id = escape_graphql_string(session_id);
    let tool_call_id = escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    tool_call_id: {{ _eq: "{tool_call_id}" }}
                }}
                limit: 1
            ) {{
                await_mode
                cancel_policy
                child_request_id
                lifecycle_state
                result
                cancel_cause
                cancel_cascade_intent_at
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentToolCall")
}

async fn count_live_backgrounded_rows(
    node: &EmbeddedNode,
    request_id: &str,
) -> anyhow::Result<usize> {
    let request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    request_id: {{ _eq: "{request_id}" }},
                    await_mode: {{ _eq: "background" }}
                }}
            ) {{
                lifecycle_state
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query live backgrounded tool count for request failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<BackgroundedRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter(|row| {
            !matches!(
                row.lifecycle_state.as_deref(),
                Some("completed" | "failed" | "timedOut" | "cancelled")
            )
        })
        .count())
}

async fn count_tool_calls_by_name(node: &EmbeddedNode, session_id: &str, tool_name: &str) -> usize {
    let session_id = escape_graphql_string(session_id);
    let tool_name = escape_graphql_string(tool_name);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    tool_name: {{ _eq: "{tool_name}" }}
                }}
            ) {{
                tool_call_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "count AgentToolCall by name failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|value| value.as_array())
        .map(Vec::len)
        .unwrap_or(0)
}

async fn fetch_background_theorem_child_request(
    node: &EmbeddedNode,
    child_request_id: &str,
) -> BackgroundTheoremChildRequestRow {
    let child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{child_request_id}" }} }}
                limit: 1
            ) {{
                _docID
                request_id
                agent_did
                requester_did
                behavior_id
                session_id
                content
                temperature
                top_p
                top_k
                seed
                max_tokens
                metadata
                execution_origin
                created_at
                deadline
                subagent_depth
                caused_by_parent_request_id
                caused_by_parent_request_doc_id
                caused_by_parent_tool_call_id
                caused_by_parent_tool_call_doc_id
                status
                lifecycle_state
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentRequest")
}

async fn fetch_background_theorem_child_request_optional(
    node: &EmbeddedNode,
    child_request_id: &str,
) -> Option<BackgroundTheoremChildRequestRow> {
    let child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{child_request_id}" }} }}
                limit: 1
            ) {{
                _docID
                request_id
                agent_did
                requester_did
                behavior_id
                session_id
                content
                temperature
                top_p
                top_k
                seed
                max_tokens
                metadata
                execution_origin
                created_at
                deadline
                subagent_depth
                caused_by_parent_request_id
                caused_by_parent_request_doc_id
                caused_by_parent_tool_call_id
                caused_by_parent_tool_call_doc_id
                status
                lifecycle_state
            }}
        }}"#
    );
    first_optional_row(&node.execute(&query).await, "AgentRequest")
}

async fn wait_for_background_theorem_child_lifecycle_state(
    node: &EmbeddedNode,
    child_request_id: &str,
    expected_state: &str,
) -> BackgroundTheoremChildRequestRow {
    let timeout_at = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        if let Some(row) =
            fetch_background_theorem_child_request_optional(node, child_request_id).await
        {
            if row.lifecycle_state.as_deref() == Some(expected_state) {
                return row;
            }

            if tokio::time::Instant::now() >= timeout_at {
                panic!(
                    "timed out waiting for child {child_request_id} lifecycle_state={expected_state}; last row: {row:?}"
                );
            }
        } else if tokio::time::Instant::now() >= timeout_at {
            panic!(
                "timed out waiting for child {child_request_id} to be materialized (expected lifecycle_state={expected_state})"
            );
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn generated_r6_backgrounding_cases_drive_tool_backgrounding_contract() {
    let cases = lean_r6_backgrounding_cases();
    assert_eq!(cases.len(), 34);

    let names = cases
        .iter()
        .map(|case| case.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        names,
        [
            "background_tool_budget_count_7_admits_spawn",
            "background_tool_budget_count_8_rejects_spawn",
            "tool_kind_background_mode_executes",
            "tool_kind_bridge_complete_persists_result",
            "tool_kind_explicit_cancel_projects_explicit_cancel",
            "background_recovery_running_live_parent_to_cancelled",
            "background_completion_source_writes_canonical_key",
            "terminal_completion_message_precedes_claimed_continuation",
            "failed_background_wake_with_budget_redrives",
            "failed_background_wake_exhausted_budget_stops",
            "generic_scheduled_failure_is_not_background_redrive",
            "non_latest_background_wake_does_not_redrive",
            "aged_background_wake_precedes_new_descendant",
            "fresh_background_wake_preserves_fifo",
            "completed_wake_acknowledges_exact_claim_snapshot",
            "failed_wake_retains_claim_snapshot_unacknowledged",
            "restart_before_claim_preserves_pending_notification",
            "inference_failure_retains_snapshot_for_bounded_redrive",
            "response_persisted_before_crash_recovers_completed_ack",
            "acknowledgement_projection_restart_is_atomic",
            "legacy_subagent_completion_source_aliases_canonical_key",
            "list_processes_same_requester_next_turn_authorized",
            "read_process_same_requester_next_turn_authorized",
            "wait_process_same_requester_next_turn_authorized",
            "cancel_process_same_requester_next_turn_authorized",
            "originating_request_authorizes_legacy_row_without_requester",
            "absent_requester_next_turn_authorized",
            "empty_requester_does_not_alias_absent",
            "process_control_cross_session_denied",
            "process_control_cross_agent_denied",
            "process_control_cross_requester_denied",
            "wait_timeout_preserves_running_process",
            "caller_interrupt_preserves_running_process",
            "caller_deadline_preserves_running_process",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>()
    );

    for case in cases {
        assert_eq!(case.max_backgrounded, 8, "{}", case.name);
        assert_eq!(case.await_mode.as_str(), "background", "{}", case.name);
        assert_eq!(case.cancel_policy.as_str(), "cascade", "{}", case.name);
        assert_eq!(case.child_request_id.as_deref(), None, "{}", case.name);
    }

    let admit = lean_r6_backgrounding_case("background_tool_budget_count_7_admits_spawn");
    assert!(admit.legal);
    assert_eq!(admit.pre_live_count, 7);
    assert_eq!(admit.terminal_state.as_str(), "running");

    let reject = lean_r6_backgrounding_case("background_tool_budget_count_8_rejects_spawn");
    assert!(!reject.legal);
    assert_eq!(reject.pre_live_count, 8);
    assert_eq!(
        reject.error_code.as_deref(),
        Some("background_tool_budget_exceeded")
    );

    let completed = lean_r6_backgrounding_case("tool_kind_bridge_complete_persists_result");
    assert!(completed.legal);
    assert_eq!(completed.terminal_state.as_str(), "completed");
    assert_eq!(completed.result.as_deref(), Some("done"));

    let cancelled =
        lean_r6_backgrounding_case("tool_kind_explicit_cancel_projects_explicit_cancel");
    assert_eq!(cancelled.terminal_state.as_str(), "cancelled");
    assert_eq!(cancelled.reason.as_deref(), Some("explicit_cancel"));

    let backgrounded = lean_r6_backgrounding_case("tool_kind_background_mode_executes");
    assert!(backgrounded.legal);
    assert_eq!(backgrounded.action.as_str(), "background");
    assert_eq!(backgrounded.terminal_state.as_str(), "running");

    let recovered =
        lean_r6_backgrounding_case("background_recovery_running_live_parent_to_cancelled");
    assert_eq!(
        recovered.action.as_str(),
        "TerminalizeBackgroundedAsInterrupted"
    );
    assert_eq!(recovered.terminal_state.as_str(), "cancelled");
    assert_eq!(recovered.reason.as_deref(), Some("interrupted_on_restart"));
    assert_eq!(
        recovered.queue_source.as_deref(),
        Some("background_completion")
    );
    assert_eq!(
        recovered.queue_key.as_deref(),
        Some("background_completion:900")
    );

    let canonical = lean_r6_backgrounding_case("background_completion_source_writes_canonical_key");
    assert_eq!(
        canonical.queue_source.as_deref(),
        Some("background_completion")
    );
    assert_eq!(
        canonical.queue_key.as_deref(),
        Some("background_completion:900")
    );

    let legacy =
        lean_r6_backgrounding_case("legacy_subagent_completion_source_aliases_canonical_key");
    assert_eq!(legacy.queue_source.as_deref(), Some("subagent_completion"));
    assert_eq!(legacy.queue_key.as_deref(), canonical.queue_key.as_deref());

    let redrive = lean_r6_backgrounding_case("failed_background_wake_with_budget_redrives");
    assert!(redrive.legal);
    assert_eq!(redrive.group, "completion_redrive");
    assert_eq!(redrive.action, "redrive_failed_background_wake");
    assert_eq!(redrive.retry_count, Some(1));
    assert_eq!(redrive.max_retries, Some(3));
    assert_eq!(redrive.post_retry_count, Some(2));
    assert_eq!(redrive.retry_delay_seconds, Some(10));
    assert_eq!(
        gents::lifecycle::background_wake_retry_delay(redrive.retry_count.unwrap() as i64)
            .num_seconds(),
        redrive.retry_delay_seconds.unwrap() as i64
    );
    assert_eq!(redrive.is_latest, Some(true));

    let exhausted = lean_r6_backgrounding_case("failed_background_wake_exhausted_budget_stops");
    assert!(!exhausted.legal);
    assert_eq!(exhausted.retry_count, exhausted.max_retries);
    assert_eq!(exhausted.post_retry_count, None);

    let generic = lean_r6_backgrounding_case("generic_scheduled_failure_is_not_background_redrive");
    assert!(!generic.legal);
    assert_eq!(generic.queue_source.as_deref(), Some("user"));

    let non_latest = lean_r6_backgrounding_case("non_latest_background_wake_does_not_redrive");
    assert!(!non_latest.legal);
    assert_eq!(non_latest.is_latest, Some(false));

    let aged = lean_r6_backgrounding_case("aged_background_wake_precedes_new_descendant");
    assert!(aged.legal);
    assert_eq!(aged.group, "completion_admission");
    assert_eq!(aged.action, "rank_pending_background_wake");
    assert_eq!(aged.reason.as_deref(), Some("aged_priority"));

    let fresh = lean_r6_backgrounding_case("fresh_background_wake_preserves_fifo");
    assert!(!fresh.legal);
    assert_eq!(fresh.group, "completion_admission");
    assert_eq!(fresh.reason.as_deref(), Some("fifo"));

    let acknowledged =
        lean_r6_backgrounding_case("completed_wake_acknowledges_exact_claim_snapshot");
    assert!(acknowledged.legal);
    assert_eq!(acknowledged.group, "completion_acknowledgement");
    assert_eq!(acknowledged.terminal_state, "completed");
    assert_eq!(
        acknowledged.result.as_deref(),
        Some("attempted=1,acknowledged=1")
    );
    assert_eq!(acknowledged.reason.as_deref(), Some("completed_ack"));

    let retained = lean_r6_backgrounding_case("failed_wake_retains_claim_snapshot_unacknowledged");
    assert!(retained.legal);
    assert_eq!(retained.group, "completion_acknowledgement");
    assert_eq!(retained.terminal_state, "failed");
    assert_eq!(
        retained.result.as_deref(),
        Some("attempted=1,acknowledged=0")
    );
    assert_eq!(retained.reason.as_deref(), Some("failed_unacknowledged"));

    let before_claim =
        lean_r6_backgrounding_case("restart_before_claim_preserves_pending_notification");
    assert!(before_claim.legal);
    assert_eq!(before_claim.group, "completion_failure_boundary");
    assert_eq!(before_claim.action, "restart_before_claim");
    assert_eq!(before_claim.terminal_state, "pending");
    assert_eq!(
        before_claim.result.as_deref(),
        Some("attempted=0,acknowledged=0")
    );
    assert_eq!(before_claim.reason.as_deref(), Some("pending_reclaim"));

    let during_inference =
        lean_r6_backgrounding_case("inference_failure_retains_snapshot_for_bounded_redrive");
    assert!(during_inference.legal);
    assert_eq!(during_inference.action, "fail_during_inference");
    assert_eq!(during_inference.terminal_state, "failed");
    assert_eq!(
        during_inference.result.as_deref(),
        Some("attempted=1,acknowledged=0")
    );
    assert_eq!(during_inference.reason.as_deref(), Some("bounded_retry"));

    let after_response =
        lean_r6_backgrounding_case("response_persisted_before_crash_recovers_completed_ack");
    assert!(after_response.legal);
    assert_eq!(after_response.action, "recover_after_response_persistence");
    assert_eq!(after_response.terminal_state, "completed");
    assert_eq!(
        after_response.result.as_deref(),
        Some("attempted=1,acknowledged=1")
    );
    assert_eq!(
        after_response.reason.as_deref(),
        Some("recovered_completed_ack")
    );

    let during_ack = lean_r6_backgrounding_case("acknowledgement_projection_restart_is_atomic");
    assert!(during_ack.legal);
    assert_eq!(during_ack.action, "project_acknowledgement_after_restart");
    assert_eq!(during_ack.terminal_state, "completed");
    assert_eq!(
        during_ack.result.as_deref(),
        Some("attempted=1,acknowledged=1")
    );
    assert_eq!(during_ack.reason.as_deref(), Some("atomic_ack_projection"));

    for case in cases.iter().filter(|case| case.group == "native_lifecycle") {
        drive_r6_native_lifecycle_case(case).await;
    }

    let continuation =
        lean_r6_backgrounding_case("terminal_completion_message_precedes_claimed_continuation");
    drive_r6_completion_continuation_case(continuation).await;

    for action in [
        "list_processes",
        "read_process",
        "wait_process",
        "cancel_process",
    ] {
        let case = cases
            .iter()
            .find(|case| {
                case.group == "process_control_authorization"
                    && case.action == action
                    && case.reason.as_deref() == Some("same_requester_next_turn")
            })
            .unwrap_or_else(|| panic!("missing same-principal process control case for {action}"));
        assert!(case.legal, "{} must remain authorized", case.name);
    }

    for scenario in ["cross_session", "cross_agent", "cross_requester"] {
        let case = cases
            .iter()
            .find(|case| {
                case.group == "process_control_authorization"
                    && case.reason.as_deref() == Some(scenario)
            })
            .unwrap_or_else(|| panic!("missing denied process control case for {scenario}"));
        assert!(!case.legal, "{} must be denied", case.name);
    }

    assert!(lean_r6_backgrounding_case("absent_requester_next_turn_authorized").legal);
    assert!(!lean_r6_backgrounding_case("empty_requester_does_not_alias_absent").legal);

    for reason in [
        "wait_timeout",
        "caller_interrupted",
        "caller_deadline_exceeded",
    ] {
        let case = cases
            .iter()
            .find(|case| case.group == "wait_boundary" && case.reason.as_deref() == Some(reason))
            .unwrap_or_else(|| panic!("missing wait boundary case for {reason}"));
        assert!(case.legal, "{} must not request cancellation", case.name);
        assert_eq!(case.terminal_state, "running", "{}", case.name);
    }
}

#[derive(Debug, Deserialize)]
struct CompletionWakeRow {
    request_id: String,
    lifecycle_state: Option<String>,
    metadata: Option<String>,
}

async fn fetch_completion_wakes(node: &EmbeddedNode, session_id: &str) -> Vec<CompletionWakeRow> {
    let session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }}
                    execution_origin: {{ _eq: "scheduled" }}
                }}
                order: {{ created_at: ASC }}
            ) {{
                request_id
                lifecycle_state
                metadata
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "fetch completion wakes failed: {:?}",
        response.errors
    );
    let data = response.data.expect("completion wake query data");
    serde_json::from_value(data["AgentRequest"].clone()).expect("parse completion wake rows")
}

async fn drive_r6_completion_continuation_case(case: &lean_vocab_test::LeanR6BackgroundingCase) {
    use gents::background_completion::{
        project_background_subagent_completion, BackgroundCompletionOutcome,
    };

    assert!(case.legal, "composed Lean acceptance path must execute");
    assert_eq!(case.action, "terminalize_append_notification_enqueue_claim");
    assert_eq!(case.terminal_state, "completed");
    assert_eq!(
        case.result.as_deref(),
        Some("assistant_wait_precedes_notification")
    );
    assert_eq!(case.reason.as_deref(), Some("continuation_claimed"));
    assert_eq!(case.queue_source.as_deref(), Some("background_completion"));

    let bridge_case = lean_bridge_step_cases()
        .iter()
        .find(|candidate| candidate.name == "bridge_step_complete_child_completed")
        .expect("generated completed bridge case");
    let (db, _lifecycle, _tool_call_id, child_request_id, parent_session_id) =
        seed_bridge_step_fixture(bridge_case).await;
    let parent_request_id = format!("{}-parent", bridge_case.name);
    let child_session_id = fetch_child_session_id(db.node.as_ref(), &child_request_id).await;

    // Materialize the model-visible wait call before the child terminalizes.
    // This is the durable sequence reservation represented by the composed
    // Lean witness.
    let wait_message = serde_json::to_string(&Message::Assistant {
        id: None,
        content: vec![AssistantContent::ToolCall(ToolCall {
            id: "r6-wait-result".to_string(),
            call_id: Some("r6-wait-call".to_string()),
            function: ToolFunction {
                name: "wait_subagent".to_string(),
                arguments: json!({ "child_request_id": child_request_id }),
            },
            signature: None,
            additional_params: None,
        })],
    })
    .expect("serialize wait assistant message");
    let escaped_session_id = escape_graphql_string(&parent_session_id);
    let escaped_request_id = escape_graphql_string(&parent_request_id);
    let escaped_wait_message = escape_graphql_string(&wait_message);
    let reserve = db
        .node
        .execute(&format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "{escaped_session_id}:1"
                    session_id: "{escaped_session_id}"
                    agent_did: "{AGENT_DID}"
                    request_id: "{escaped_request_id}"
                    sequence: 1
                    role: "assistant"
                    content: "{escaped_wait_message}"
                    reasoning: ""
                    timestamp: "2026-05-19T00:00:02Z"
                }}) {{ _docID }}
            }}"#
        ))
        .await;
    assert!(
        !reserve.has_errors(),
        "reserve assistant wait row failed: {:?}",
        reserve.errors
    );

    persist_bridge_step_child_completion(db.node.as_ref(), &child_request_id, &child_session_id)
        .await;

    let outcome =
        project_background_subagent_completion(db.node.clone(), &child_request_id, AGENT_DID)
            .await
            .expect("project terminal background completion");
    assert!(
        matches!(outcome, BackgroundCompletionOutcome::Projected { .. }),
        "terminal bridge must project before continuation: {outcome:?}"
    );

    let messages_before_claim =
        fetch_message_snapshots_for_session(db.node.as_ref(), &parent_session_id).await;
    assert_eq!(
        messages_before_claim.len(),
        2,
        "reserved assistant wait and terminal notification must both remain durable"
    );
    assert_eq!(messages_before_claim[0].role, "assistant");
    assert!(
        messages_before_claim[0]
            .content
            .contains("\"wait_subagent\""),
        "model-visible wait envelope missing: {:?}",
        messages_before_claim[0].content
    );
    assert_eq!(messages_before_claim[1].role, "user");
    assert!(
        messages_before_claim[1]
            .content
            .contains("<subagent-notification"),
        "model-visible completion envelope missing: {:?}",
        messages_before_claim[1].content
    );
    assert!(
        messages_before_claim[0].sequence < messages_before_claim[1].sequence,
        "assistant wait must precede its terminal notification: {messages_before_claim:#?}"
    );

    let wakes = fetch_completion_wakes(db.node.as_ref(), &parent_session_id).await;
    assert_eq!(wakes.len(), 1, "one completion must enqueue one wake");
    assert_eq!(wakes[0].lifecycle_state.as_deref(), Some("pending"));
    let metadata: Value =
        serde_json::from_str(wakes[0].metadata.as_deref().expect("wake metadata"))
            .expect("wake metadata JSON");
    assert_eq!(metadata["queue"]["source"], "background_completion");
    assert_eq!(metadata["queue"]["policy"], "coalesce");
    assert_eq!(
        metadata["queue"]["key"],
        format!("background_completion:{parent_session_id}")
    );
    assert_eq!(
        case.queue_key.as_deref(),
        Some("background_completion:900"),
        "Lean uses opaque session 900 as the canonical runtime-key representative"
    );

    // The foreground parent owns the session until terminal. Its completion
    // releases the FIFO head, after which the normal watcher claims the
    // generated wake as the next agent turn.
    set_request_status_lifecycle_by_request_id(
        db.node.as_ref(),
        &parent_request_id,
        "completed",
        "completed",
    )
    .await;
    let mut watcher = DefraWatcher::new(db.node.clone(), AGENT_DID);
    let claimed = tokio::time::timeout(Duration::from_secs(2), watcher.next_request())
        .await
        .expect("completion wake should become claimable")
        .expect("watcher should remain open")
        .expect("completion wake should load");
    assert_eq!(claimed.request_id, wakes[0].request_id);
    assert_eq!(claimed.session_id, parent_session_id);

    let messages_after_claim =
        fetch_message_snapshots_for_session(db.node.as_ref(), &parent_session_id).await;
    assert_eq!(
        messages_after_claim, messages_before_claim,
        "claiming the continuation must retain the notification provider history"
    );
}

async fn drive_r6_native_lifecycle_case(case: &lean_vocab_test::LeanR6BackgroundingCase) {
    let db = test_db(&format!("r6-native-lifecycle-{}", case.name)).await;
    let request_id = format!("{}-request", case.name);
    let session_id = format!("{}-session", case.name);
    let tool_call_id = format!("{}-tool", case.name);
    let deadline = chrono::Utc::now() + chrono::Duration::minutes(5);
    let mut lifecycle = if case.action == "background" {
        ToolCallLifecycle::new(
            db.node.clone(),
            request_id,
            session_id.clone(),
            AGENT_DID.to_string(),
            tool_call_id.clone(),
            1,
            "bash_unrestricted".to_string(),
            "{}".to_string(),
            deadline,
        )
    } else {
        ToolCallLifecycle::new_background_tool(
            db.node.clone(),
            request_id,
            session_id.clone(),
            AGENT_DID.to_string(),
            tool_call_id.clone(),
            1,
            "bash_unrestricted".to_string(),
            "{}".to_string(),
            deadline,
        )
    };
    lifecycle
        .start_running()
        .await
        .unwrap_or_else(|error| panic!("{} start_running failed: {error:#}", case.name));

    match case.action.as_str() {
        "background" => lifecycle.background().await.unwrap(),
        "bridge_complete" => {
            assert!(
                lifecycle
                    .bridge_complete(case.result.clone().unwrap_or_default())
                    .await
                    .unwrap(),
                "{} must win the running-state compare",
                case.name
            );
        }
        "bridge_failure" => {
            assert!(
                lifecycle
                    .bridge_failure(gents::tool_call_lifecycle::ChildTerminal::Interrupted)
                    .await
                    .unwrap(),
                "{} must win the running-state compare",
                case.name
            );
        }
        other => panic!("unhandled native lifecycle action {other}"),
    }

    let row =
        fetch_background_theorem_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert_eq!(
        row.await_mode.as_deref(),
        Some(case.await_mode.as_str()),
        "{} await mode drifted",
        case.name
    );
    assert_eq!(
        row.cancel_policy.as_deref(),
        Some(case.cancel_policy.as_str()),
        "{} cancel policy drifted",
        case.name
    );
    assert_eq!(
        row.child_request_id.as_deref(),
        case.child_request_id.as_deref(),
        "{} child-link kind drifted",
        case.name
    );
    assert_eq!(
        row.lifecycle_state.as_deref(),
        Some(case.terminal_state.as_str()),
        "{} lifecycle projection drifted",
        case.name
    );
    if let Some(expected) = case.result.as_deref() {
        assert_eq!(row.result.as_deref(), Some(expected), "{}", case.name);
    }
    if case.action == "bridge_failure" {
        assert_eq!(
            row.cancel_cause.as_deref(),
            Some("interrupted"),
            "{} cancellation cause drifted",
            case.name
        );
    }
}

pub(super) async fn generated_r6_background_theorem_witnesses_drive_admission_budget_invariant() {
    let witnesses = lean_r6_background_theorem_witnesses();
    assert_eq!(witnesses.len(), 2);

    let witness =
        lean_r6_background_theorem_witness("Subagent.BridgedState.backgrounded_budget_bounded");
    assert_eq!(witness.witness_kind.as_str(), "state_invariant");
    assert_eq!(
        witness.scenario.as_str(),
        "background_tool_admission_respects_max_backgrounded_per_parent"
    );

    let max_backgrounded = witness.numeric_bound;
    let await_mode_expected = witness.kind_field("await_mode");
    let cancel_policy_expected = witness.kind_field("cancel_policy");
    let error_code_expected = witness.kind_field("error_code_on_violation");

    let (db, hook, session_id, request_id) = setup_background_tool_hook(
        "r6-background-theorem-budget",
        background_tool_registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;

    for index in 0..max_backgrounded {
        let internal_call_id = format!("meta-theorem-bg-{index}");
        let receipt = skip_reason_json(
            hook.on_tool_call(
                "spawn_process",
                None,
                &internal_call_id,
                r#"{"tool_name":"slow_tool","args":{}}"#,
            )
            .await,
        );
        assert_eq!(receipt["status"].as_str(), Some("running"));
        assert_eq!(receipt["await_mode"].as_str(), Some(await_mode_expected));
        let background_tool_call_id = receipt["tool_call_id"]
            .as_str()
            .expect("background receipt tool_call_id");

        let row = fetch_background_theorem_tool_call(
            db.node.as_ref(),
            &session_id,
            background_tool_call_id,
        )
        .await;
        assert_eq!(row.await_mode.as_deref(), Some(await_mode_expected));
        assert_eq!(row.cancel_policy.as_deref(), Some(cancel_policy_expected));

        let live = count_live_backgrounded_rows(db.node.as_ref(), &request_id)
            .await
            .expect("count live backgrounded rows");
        assert!(
            live <= max_backgrounded,
            "live count {live} exceeded witness bound {max_backgrounded} after admit #{index}"
        );
        assert_eq!(live, index + 1);
    }

    let denied = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-theorem-bg-overflow",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    assert_eq!(denied["code"].as_str(), Some(error_code_expected));
    assert_eq!(
        denied["current_backgrounded"]
            .as_u64()
            .map(|value| value as usize),
        Some(max_backgrounded)
    );
    assert_eq!(
        denied["max_backgrounded"]
            .as_u64()
            .map(|value| value as usize),
        Some(max_backgrounded)
    );

    let live_after = count_live_backgrounded_rows(db.node.as_ref(), &request_id)
        .await
        .expect("count live backgrounded rows after denial");
    assert_eq!(live_after, max_backgrounded);
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "slow_tool").await,
        max_backgrounded
    );
}

/// Drives the local cascade-dispatch trace witness through the child request's
/// persisted `interrupted` post-state.
pub(super) async fn generated_r6_background_theorem_witnesses_drive_cascade_cancellation_trace() {
    let witness = lean_r6_background_theorem_witness("Subagent.BridgedState.cascade_cancels_child");
    assert_eq!(witness.witness_kind.as_str(), "reachability_trace");
    assert_eq!(
        witness.scenario.as_str(),
        "parent_terminal_with_cascade_bridge_interrupts_processing_child"
    );
    assert_eq!(witness.numeric_bound, 2);

    let cancel_policy_expected = witness.kind_field("cancel_policy");
    let child_post_state_expected = witness.kind_field("child_post_state");
    assert_eq!(witness.kind_field("child_pre_state"), "processing");
    assert_eq!(witness.kind_field("child_pre_admission"), "executing");

    let (db, hook, session_id, _request_id, _parent_deadline) = setup_background_spawn_fixture(
        "r6-background-theorem-cascade",
        vec![BACKGROUND_THEOREM_CHILD_BEHAVIOR_ID],
        0,
        true,
    )
    .await;
    // After spawn convergence (#377) the child AgentRequest is materialized by
    // SubagentSource, not synchronously by the hook.  Hold a standalone source
    // for the lifetime of this test so the bridge row produces a child request.
    let _source = super::support::fixtures::spawn_subagent_source(
        db.node.clone(),
        AGENT_DID,
        BACKGROUND_THEOREM_PARENT_BEHAVIOR_ID,
        BACKGROUND_THEOREM_CHILD_BEHAVIOR_ID,
    );
    let args = json!({
        "name": BACKGROUND_THEOREM_CHILD_BEHAVIOR_ID,
        "prompt": "child for cascade theorem witness",
        "await_mode": "background"
    })
    .to_string();

    let action = hook
        .on_tool_call(
            "spawn_subagent",
            Some("model-call-theorem-cascade".to_string()),
            "internal-theorem-cascade",
            &args,
        )
        .await;
    let receipt = skip_reason_json(action);
    let child_request_id = receipt["child_request_id"]
        .as_str()
        .expect("child_request_id")
        .to_string();

    let tool = fetch_background_theorem_tool_call(
        db.node.as_ref(),
        &session_id,
        "internal-theorem-cascade",
    )
    .await;
    assert_eq!(tool.cancel_policy.as_deref(), Some(cancel_policy_expected));
    assert_eq!(
        tool.child_request_id.as_deref(),
        Some(child_request_id.as_str())
    );

    // Wait for SubagentSource to materialize the child (post-convergence #377:
    // the child is no longer created synchronously by the hook).
    let child = wait_for_background_theorem_child_lifecycle_state(
        db.node.as_ref(),
        &child_request_id,
        "pending",
    )
    .await;
    assert_eq!(child.lifecycle_state.as_deref(), Some("pending"));
    let mut child_lifecycle = RequestLifecycle::new_with_execution_binding(
        db.node.clone(),
        BACKGROUND_THEOREM_CHILD_BEHAVIOR_ID,
        AGENT_DID,
        child.into_agent_request(),
        DEADLINE_SECS,
        ExecutionOrigin::Interactive,
        BACKEND_ID,
    );
    assert_eq!(
        child_lifecycle.claim_with_identity().await.unwrap(),
        ClaimOutcome::Claimed
    );
    child_lifecycle.begin_execution().await.unwrap();
    let child_pre =
        fetch_background_theorem_child_request(db.node.as_ref(), &child_request_id).await;
    assert_eq!(child_pre.status.as_str(), "processing");
    assert_eq!(
        child_pre.lifecycle_state.as_deref(),
        Some(witness.kind_field("child_pre_state"))
    );

    let mut lifecycle =
        ToolCallLifecycle::load(db.node.clone(), &session_id, "internal-theorem-cascade")
            .await
            .expect("load bridge lifecycle")
            .expect("bridge should be persisted");
    let dispatch = lifecycle
        .cancel_during_run_with_cascade_dispatch(CancelCause::Interrupted, AGENT_DID)
        .await
        .expect("cancel bridge with cascade dispatch")
        .expect("cascade dispatch");
    let CascadeDispatch::Local(intent) = dispatch else {
        panic!("local child must use local cascade dispatch");
    };
    assert_eq!(intent.child_request_id, child_request_id);

    interrupt_request(db.node.as_ref(), &intent.child_request_id)
        .await
        .expect("interrupt child request");
    // This isolated consumer has no daemon observer running, so explicitly
    // drive the same request-lifecycle interrupt arm used by the daemon.
    child_lifecycle
        .transition_to_interrupted()
        .await
        .expect("drive child interrupt_processing transition");

    let tool = fetch_background_theorem_tool_call(
        db.node.as_ref(),
        &session_id,
        "internal-theorem-cascade",
    )
    .await;
    assert_eq!(tool.cancel_cause.as_deref(), Some("interrupted"));
    assert!(
        tool.cancel_cascade_intent_at.is_none(),
        "local cascade dispatch must not leave a remote bridge intent"
    );
    let child_post = wait_for_background_theorem_child_lifecycle_state(
        db.node.as_ref(),
        &child_request_id,
        child_post_state_expected,
    )
    .await;
    assert_eq!(child_post.status.as_str(), child_post_state_expected);
    assert_eq!(
        child_post.lifecycle_state.as_deref(),
        Some(child_post_state_expected)
    );
    let child_interrupt = fetch_interrupt_requested_at(db.node.as_ref(), &child_post.request_id)
        .await
        .expect("fetch child interrupt_requested_at");
    assert!(
        child_interrupt.is_some(),
        "cascade trace must preserve child interrupt_requested_at through {child_post_state_expected}"
    );
}

pub(super) fn generated_subagent_delegation_graph_cases_pin_gap2_contract() {
    let cases = lean_subagent_delegation_graph_cases();
    assert_eq!(
        cases.len(),
        3,
        "Lean should emit termination, acyclicity, and cascade graph witnesses"
    );

    let by_property = cases
        .iter()
        .map(|case| (case.property.as_str(), case))
        .collect::<HashMap<_, _>>();
    for property in ["termination", "acyclicity", "cascade_cancel"] {
        assert!(
            by_property.contains_key(property),
            "missing subagent delegation graph property {property}"
        );
    }

    for case in cases {
        assert_eq!(
            case.max_depth,
            usize::try_from(MAX_SUBAGENT_DEPTH).expect("MAX_SUBAGENT_DEPTH fits usize"),
            "Lean maxSubagentDepth drifted from Rust MAX_SUBAGENT_DEPTH"
        );
        assert!(
            case.path_length <= case.max_depth,
            "case {} exceeds the generated depth bound",
            case.name
        );
        assert!(case.acyclic, "case {} must assert acyclicity", case.name);
        assert!(case.bounded, "case {} must assert bounded paths", case.name);
        assert!(
            !case.theorem_name.trim().is_empty(),
            "case {} must cite a Lean theorem",
            case.name
        );
        assert!(
            case.edge_theorem.starts_with("Subagent.DelegationGraph."),
            "case {} must cite a graph edge/path theorem",
            case.name
        );
        if case.cascade_path {
            assert!(
                case.cascade_covered,
                "cascade graph case {} must assert edge interrupt coverage",
                case.name
            );
            assert_eq!(
                case.cascade_edge_theorem.as_deref(),
                Some("Subagent.BridgedState.cascade_cancels_child")
            );
        } else {
            assert!(!case.cascade_covered);
            assert!(case.cascade_edge_theorem.is_none());
        }
    }

    let termination = by_property["termination"];
    assert_eq!(
        termination.theorem_name.as_str(),
        "Subagent.DelegationGraph.delegation_path_length_bounded"
    );
    assert_eq!(
        termination.witness_kind.as_str(),
        "arbitrary_delegation_path"
    );
    assert_eq!(termination.parent_depth, 0);
    assert_eq!(termination.terminal_depth, termination.max_depth);

    let acyclicity = by_property["acyclicity"];
    assert_eq!(
        acyclicity.theorem_name.as_str(),
        "Subagent.DelegationGraph.delegation_paths_acyclic"
    );
    assert_eq!(
        acyclicity.edge_theorem.as_str(),
        "Subagent.DelegationGraph.no_self_delegation_edge"
    );

    let cascade = by_property["cascade_cancel"];
    assert_eq!(
        cascade.theorem_name.as_str(),
        "Subagent.DelegationGraph.cascade_cancel_covers_path"
    );
    assert_eq!(cascade.witness_kind.as_str(), "arbitrary_cascade_path");
}

pub(super) fn generated_r4c_background_work_cases_pin_observable_shapes() {
    let cases = lean_r4c_background_work_cases();
    assert_eq!(cases.len(), 7);

    let names = cases
        .iter()
        .map(LeanR4cBackgroundWorkCase::witness)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        names,
        [
            "r4c.list_subagents.lineage_rejects",
            "r4c.list_subagents.unmaterialized_child_visible",
            "r4c.read_subagent_transcript.cursor_advances",
            "r4c.read_subagent_transcript.hides_bridge_rows",
            "r4c.read_tool_output.dispatch_by_state",
            "r4c.steer_subagent.append_preserves_lineage",
            "r4c.steer_subagent.interrupt_composes",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>()
    );

    match lean_r4c_background_work_case("r4c.list_subagents.lineage_rejects") {
        LeanR4cBackgroundWorkCase::ListSubagentsLineageRejects {
            caller_request_id,
            sibling_request_id,
            sibling_child_id,
            caller_sees_sibling_child,
        } => {
            assert_eq!(caller_request_id, "r4c-w1-caller");
            assert_eq!(sibling_request_id, "r4c-w1-sibling");
            assert_eq!(sibling_child_id, "r4c-w1-sibling-child");
            assert!(!*caller_sees_sibling_child);
        }
        other => panic!("unexpected R4c witness variant: {other:?}"),
    }

    match lean_r4c_background_work_case("r4c.read_subagent_transcript.cursor_advances") {
        LeanR4cBackgroundWorkCase::ReadTranscriptCursorAdvances {
            child_session_id,
            first_since_sequence,
            first_through_sequence,
            first_next_sequence,
            second_since_sequence,
            second_through_sequence,
            no_gap,
            no_overlap,
        } => {
            assert_eq!(child_session_id, "r4c-w2-session");
            assert_eq!(*first_since_sequence, 0);
            assert_eq!(*first_through_sequence, 5);
            assert_eq!(*first_next_sequence, 6);
            assert_eq!(*second_since_sequence, 6);
            assert_eq!(*second_through_sequence, 10);
            assert_eq!(first_next_sequence, second_since_sequence);
            assert!(*no_gap);
            assert!(*no_overlap);
        }
        other => panic!("unexpected R4c witness variant: {other:?}"),
    }

    match lean_r4c_background_work_case("r4c.read_subagent_transcript.hides_bridge_rows") {
        LeanR4cBackgroundWorkCase::ReadTranscriptHidesBridgeRows {
            child_session_id,
            bridge_call_id,
            rendered_transcript,
        } => {
            assert_eq!(child_session_id, "r4c-w3-session");
            assert_eq!(bridge_call_id, "r4c-w3-bridge-call");
            assert_eq!(
                rendered_transcript,
                "[assistant seq=2]\nplain assistant message\n"
            );
            assert!(
                !rendered_transcript.contains(bridge_call_id),
                "rendered transcript must hide bridge tool-call rows"
            );
        }
        other => panic!("unexpected R4c witness variant: {other:?}"),
    }

    match lean_r4c_background_work_case("r4c.read_tool_output.dispatch_by_state") {
        LeanR4cBackgroundWorkCase::ReadToolOutputDispatchesByState {
            tool_call_id,
            running_source,
            running_no_buffer_source,
            terminal_source,
            running_payload,
            running_no_buffer_payload,
            terminal_payload,
            running_next_offset,
            running_total_bytes,
            running_has_more,
            terminal_total_bytes,
        } => {
            assert_eq!(tool_call_id, "r4c-w4-tool-call");
            // #937 realignment: the live ring buffer exists in production
            // (`LiveToolOutputRegistry`), so a running read with a snapshot
            // serves the live tail; a running read with NO snapshot — the
            // post-restart shape, the registry is volatile — serves empty
            // output; a terminal read serves the persisted completion. The
            // full dispatch is driven against the real hook by
            // `generated_read_tool_output_witness_drives_hook_dispatch`.
            assert_eq!(running_source, "live_ring_buffer");
            assert_eq!(running_no_buffer_source, "none");
            assert_eq!(terminal_source, "persisted_tool_completion");
            assert_eq!(running_payload, "live");
            assert_eq!(running_no_buffer_payload, "");
            assert_eq!(terminal_payload, "livedone");
            assert_eq!(*running_next_offset, 4);
            assert_eq!(*running_total_bytes, 4);
            assert!(!*running_has_more);
            assert_eq!(*terminal_total_bytes, 8);
            assert_ne!(
                terminal_payload, running_no_buffer_payload,
                "terminal reads serve the persisted result, never the restart-empty payload"
            );
        }
        other => panic!("unexpected R4c witness variant: {other:?}"),
    }

    match lean_r4c_background_work_case("r4c.steer_subagent.append_preserves_lineage") {
        LeanR4cBackgroundWorkCase::SteerAppendPreservesLineage {
            caller_request_id,
            caller_request_doc_id,
            child_session_id,
            queued_request_id,
            caused_by_parent_request_id,
            caused_by_parent_request_doc_id,
            caused_by_parent_tool_call_id_present,
            caused_by_parent_tool_call_doc_id_present,
            lineage_admissible,
            depth_zero_lineage_admissible,
            background_completion_depth_zero_admissible,
            request_visible_before_message_allowed,
            message_then_request_allowed,
            queue_source,
            queue_policy,
        } => {
            assert_eq!(caller_request_id, "r4c-w5-caller");
            assert_eq!(child_session_id, "r4c-w5-child-session");
            assert_eq!(queued_request_id, "r4c-w5-queued");
            assert_eq!(caused_by_parent_request_id, caller_request_id);
            assert_eq!(caused_by_parent_request_doc_id, caller_request_doc_id);
            assert!(!caused_by_parent_tool_call_id_present);
            assert!(!caused_by_parent_tool_call_doc_id_present);
            assert!(*lineage_admissible);
            assert!(*depth_zero_lineage_admissible);
            assert!(*background_completion_depth_zero_admissible);
            assert!(!*request_visible_before_message_allowed);
            assert!(*message_then_request_allowed);
            assert_eq!(queue_source, "steering");
            assert_eq!(queue_policy, "append");
        }
        other => panic!("unexpected R4c witness variant: {other:?}"),
    }

    // #593: a returned background child id never disappears from the parent
    // control plane. The projected status must be the exact string the runtime
    // serves from `list_subagents`/`read_subagent`, the projection must be
    // non-terminal (never fake a terminal outcome for an unmaterialized
    // child), and the wait payload must be retryable.
    match lean_r4c_background_work_case("r4c.list_subagents.unmaterialized_child_visible") {
        LeanR4cBackgroundWorkCase::UnmaterializedChildVisible {
            caller_request_id,
            bridge_tool_call_id,
            child_request_id,
            child_materialized,
            bridge_lifecycle_state,
            listed_status,
            listed_under_all_filter,
            listed_under_running_filter,
            read_lifecycle_state,
            read_terminal,
            wait_retryable,
        } => {
            assert_eq!(caller_request_id, "r4c-w7-caller");
            assert_eq!(bridge_tool_call_id, "r4c-w7-bridge-call");
            assert_eq!(child_request_id, "r4c-w7-child");
            assert!(!*child_materialized);
            assert_eq!(bridge_lifecycle_state, "running");
            assert_eq!(
                listed_status,
                gents::__test_internals::AWAITING_CHILD_MATERIALIZATION,
                "Lean witness and runtime must agree on the projected status string"
            );
            assert_eq!(read_lifecycle_state, listed_status);
            assert!(
                *listed_under_all_filter,
                "list_subagents(all) must show the unmaterialized handle"
            );
            assert!(
                *listed_under_running_filter,
                "the projection is non-terminal, so the default running filter shows it"
            );
            assert!(
                !*read_terminal,
                "an unmaterialized child must never read as terminal"
            );
            assert!(*wait_retryable, "wait_subagent must explain-and-retry");
        }
        other => panic!("unexpected R4c witness variant: {other:?}"),
    }

    match lean_r4c_background_work_case("r4c.steer_subagent.interrupt_composes") {
        LeanR4cBackgroundWorkCase::SteerInterruptComposes {
            caller_request_id,
            child_session_id,
            interrupted_active_request_id,
            drained_wake_up_request_ids,
            drained_wake_up_queue_key,
            queued_request_id,
            queue_interrupted_request_id,
        } => {
            assert_eq!(caller_request_id, "r4c-w6-caller");
            assert_eq!(child_session_id, "r4c-w6-child-session");
            assert_eq!(interrupted_active_request_id, "r4c-w6-interrupted");
            assert_eq!(
                drained_wake_up_request_ids,
                &vec!["r4c-w6-wake-1".to_string(), "r4c-w6-wake-2".to_string()]
            );
            assert_eq!(
                drained_wake_up_queue_key,
                "background_completion:r4c-w6-child-session"
            );
            assert_eq!(
                drained_wake_up_queue_key,
                &format!("background_completion:{child_session_id}")
            );
            assert_eq!(queued_request_id, "r4c-w6-queued");
            assert_eq!(queue_interrupted_request_id, interrupted_active_request_id);
        }
        other => panic!("unexpected R4c witness variant: {other:?}"),
    }
}

/// Drives the Lean `r4c.read_tool_output.dispatch_by_state` witness (#937)
/// through the real hook: a running native background tool serves its live
/// ring-buffer tail; a later-request hook sharing the process registry serves
/// the same live output; an explicitly unshared hook models daemon restart and
/// serves empty output for the still-running row. After completion every hook
/// serves the persisted result. Payloads and paging numbers are the
/// Lean-computed witness values.
pub(super) async fn generated_read_tool_output_witness_drives_hook_dispatch() {
    let LeanR4cBackgroundWorkCase::ReadToolOutputDispatchesByState {
        running_payload,
        running_no_buffer_payload,
        terminal_payload,
        running_next_offset,
        running_total_bytes,
        running_has_more,
        terminal_total_bytes,
        ..
    } = lean_r4c_background_work_case("r4c.read_tool_output.dispatch_by_state")
    else {
        panic!("read_tool_output witness variant drifted");
    };

    let tempdir = tempfile::tempdir().expect("tempdir");
    let tools = gents::ToolSet::builder()
        .bash_unrestricted(tempdir.path())
        .build()
        .build_native_tools()
        .expect("native tools should build");
    let (db, hook, session_id, request_id) = setup_background_tool_hook(
        "r4c-read-dispatch-witness",
        background_tool_registry(tools, &["bash_unrestricted"]),
    )
    .await;
    let shared_processes = gents::BackgroundExecutionRegistry::default();
    let hook = hook.with_background_execution_registry(shared_processes.clone());

    let spawn = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "read-dispatch-spawn",
            &json!({
                "tool_name": "bash_unrestricted",
                "args": {
                    "command": format!(
                        "printf {running_payload}; sleep 5; printf done"
                    ),
                    "args": [],
                    "timeout_secs": 10
                }
            })
            .to_string(),
        )
        .await,
    );
    let tool_call_id = spawn["tool_call_id"]
        .as_str()
        .expect("spawn receipt tool_call_id")
        .to_string();

    // Running + live snapshot → the ring-buffer tail, with the Lean-computed
    // continuation cursor. Bounded poll: the payload lands as soon as the
    // tool's first printf is flushed into the live writer.
    let mut running = json!({});
    for attempt in 0..80 {
        running = skip_reason_json(
            hook.on_tool_call(
                "read_process",
                None,
                &format!("read-dispatch-running-{attempt}"),
                &json!({ "tool_call_id": tool_call_id }).to_string(),
            )
            .await,
        );
        if running["output"].as_str() == Some(running_payload) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(running["status"].as_str(), Some("running"));
    assert_eq!(running["output"].as_str(), Some(running_payload.as_str()));
    assert_eq!(running["next_offset"].as_u64(), Some(*running_next_offset));
    assert_eq!(running["total_bytes"].as_u64(), Some(*running_total_bytes));
    assert_eq!(running["has_more"].as_bool(), Some(*running_has_more));
    assert_eq!(running["exited"].as_bool(), Some(false));

    // A new request gets a new hook, but the daemon-owned process registry
    // carries the live ring buffer across that request boundary.
    let next_request_id = format!("{request_id}-next");
    support::create_request(
        db.node.as_ref(),
        &next_request_id,
        &session_id,
        "processing",
        "2026-05-19T00:00:01Z",
    )
    .await;
    let next_turn_hook = DefraSessionHook::resume_or_create_with_identity_policy(
        db.node.clone(),
        &session_id,
        "r6-background-theorem",
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .expect("resume next-turn hook")
    .with_background_execution_registry(shared_processes);
    next_turn_hook
        .set_active_request_id(Some(next_request_id))
        .await;
    next_turn_hook
        .set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::minutes(5)))
        .await;

    let next_turn_read = skip_reason_json(
        next_turn_hook
            .on_tool_call(
                "read_process",
                None,
                "read-dispatch-next-turn",
                &json!({ "tool_call_id": tool_call_id }).to_string(),
            )
            .await,
    );
    assert_eq!(next_turn_read["status"].as_str(), Some("running"));
    assert_eq!(
        next_turn_read["output"].as_str(),
        Some(running_payload.as_str()),
        "a later request must observe the originating request's live output"
    );
    assert_eq!(
        next_turn_read["total_bytes"].as_u64(),
        Some(*running_total_bytes)
    );

    let next_turn_list = skip_reason_json(
        next_turn_hook
            .on_tool_call("list_processes", None, "list-dispatch-next-turn", "{}")
            .await,
    );
    let listed = next_turn_list["entries"]
        .as_array()
        .expect("list_processes entries")
        .iter()
        .find(|entry| entry["tool_call_id"].as_str() == Some(tool_call_id.as_str()))
        .expect("running process listed on later request");
    assert_eq!(listed["stdout_bytes"].as_u64(), Some(*running_total_bytes));

    // Running + NO snapshot: a second hook on the same session has a fresh
    // (empty) live-output registry — exactly what a restarted daemon would
    // observe for this still-running row before recovery interrupts it.
    let restarted_hook = DefraSessionHook::resume_or_create_with_identity_policy(
        db.node.clone(),
        &session_id,
        "r6-background-theorem",
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .expect("resume restart-shaped hook");
    restarted_hook
        .set_active_request_id(Some(request_id.clone()))
        .await;
    restarted_hook
        .set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::minutes(5)))
        .await;
    let no_buffer = skip_reason_json(
        restarted_hook
            .on_tool_call(
                "read_process",
                None,
                "read-dispatch-no-buffer",
                &json!({ "tool_call_id": tool_call_id }).to_string(),
            )
            .await,
    );
    assert_eq!(
        no_buffer["status"].as_str(),
        Some("running"),
        "restart-shaped read must still observe the durable running row"
    );
    assert_eq!(
        no_buffer["output"].as_str(),
        Some(running_no_buffer_payload.as_str()),
        "a running row with no live snapshot must serve empty output"
    );
    assert_eq!(no_buffer["exited"].as_bool(), Some(false));

    // Terminal → persisted completion, from BOTH hooks: the durable result
    // does not depend on the volatile registry.
    let waited = skip_reason_json(
        hook.on_tool_call(
            "wait_process",
            None,
            "read-dispatch-wait",
            &json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(waited["status"].as_str(), Some("completed"));
    for (label, reader) in [
        ("live", &hook),
        ("next-turn", &next_turn_hook),
        ("restarted", &restarted_hook),
    ] {
        let terminal = skip_reason_json(
            reader
                .on_tool_call(
                    "read_process",
                    None,
                    &format!("read-dispatch-terminal-{label}"),
                    &json!({ "tool_call_id": tool_call_id }).to_string(),
                )
                .await,
        );
        assert_eq!(terminal["status"].as_str(), Some("completed"), "{label}");
        assert_eq!(
            terminal["output"].as_str(),
            Some(terminal_payload.as_str()),
            "{label}: terminal reads serve the persisted completion"
        );
        assert_eq!(
            terminal["total_bytes"].as_u64(),
            Some(*terminal_total_bytes),
            "{label}"
        );
        assert_eq!(terminal["exited"].as_bool(), Some(true), "{label}");
    }
}

/// Drives the Lean `bridge_step_cases` (#937) — outcomes computed by running
/// `Subagent.BridgedState.step` — through the production seams: child
/// terminals project through `project_background_subagent_completion` (the
/// chokepoint that owns the complete/failure guards) and cascade decisions
/// through `ToolCallLifecycle::bridge_cancel_cascade`.
pub(super) async fn generated_bridge_step_cases_drive_bridge_lifecycle() {
    let cases = lean_bridge_step_cases();
    assert_eq!(cases.len(), 10, "Lean bridge-step case family drifted");

    let mut driven = 0usize;
    let mut model_only = 0usize;
    for case in cases {
        match case.event.as_str() {
            "bridge_complete" | "bridge_failure" => {
                if !case.bridge_committed {
                    // Model-only guard: at this seam a persisted bridge row is
                    // committed by construction (`start_running` persisted it),
                    // so the uncommitted shape cannot be seeded. Pin its
                    // contract shape instead of silently skipping.
                    assert!(!case.legal, "{}", case.name);
                    assert_eq!(case.post_tool_state, None, "{}", case.name);
                    model_only += 1;
                    continue;
                }
                drive_bridge_step_projection_case(case).await;
                driven += 1;
            }
            "bridge_cancel_cascade" => {
                drive_bridge_step_cascade_case(case).await;
                driven += 1;
            }
            other => panic!("unhandled bridge step event {other}"),
        }
    }
    assert_eq!(driven, 9, "every seedable bridge-step row must be driven");
    assert_eq!(
        model_only, 1,
        "exactly the uncommitted-bridge row is model-only"
    );
}

async fn seed_bridge_step_fixture(
    case: &lean_vocab_test::LeanBridgeStepCase,
) -> (support::TestDb, ToolCallLifecycle, String, String, String) {
    let db = test_db(&format!("bridge-step-{}", case.name)).await;
    let parent_request_id = format!("{}-parent", case.name);
    let parent_session_id = format!("{}-parent-session", case.name);
    let tool_call_id = format!("{}-tool", case.name);
    let child_request_id = format!("{}-child", case.name);

    upsert_agent_behavior(
        db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: BACKGROUND_THEOREM_PARENT_BEHAVIOR_ID.to_string(),
            agent_did: AGENT_DID.to_string(),
            display_name: Some("bridge step parent".to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: None,
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            enabled: true,
            created_at: Some("2026-05-19T00:00:00Z".to_string()),
        },
    )
    .await
    .expect("upsert bridge step parent behavior");
    create_background_theorem_parent_request(
        db.node.as_ref(),
        &parent_request_id,
        &parent_session_id,
        0,
        chrono::Utc::now() + chrono::Duration::minutes(5),
    )
    .await;
    if case.parent_state == "interrupted" {
        set_request_status_lifecycle_by_request_id(
            db.node.as_ref(),
            &parent_request_id,
            "interrupted",
            "interrupted",
        )
        .await;
    }

    let cancel_policy = match case.cancel_policy.as_str() {
        "cascade" => CancelPolicy::Cascade,
        "detach" => CancelPolicy::Detach,
        other => panic!("unhandled cancel policy {other}"),
    };
    let parent_request_doc_id =
        crate::support::exact_request_doc_id(db.node.as_ref(), &parent_request_id).await;
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.clone(),
        parent_session_id.clone(),
        "did:test:test".to_string(),
        tool_call_id.clone(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        cancel_policy,
        child_request_id.clone(),
        "did:test:target".to_string(),
    )
    .with_request_doc_id(Some(parent_request_doc_id.clone()));
    lifecycle.start_running().await.unwrap();
    let parent_tool_call_doc_id = lifecycle.doc_id().expect("bridge document id").to_string();

    gents::tool_call_lifecycle::create_subagent_request_with_request_id(
        db.node.as_ref(),
        child_request_id.clone(),
        parent_request_id.clone(),
        parent_request_doc_id,
        tool_call_id.clone(),
        parent_tool_call_doc_id,
        0,
        AGENT_DID.to_string(),
        "bridge-step-child".to_string(),
        format!("prompt for {tool_call_id}"),
        Some(chrono::Utc::now() + chrono::Duration::minutes(4)),
    )
    .await
    .expect("create bridged child request");

    (
        db,
        lifecycle,
        tool_call_id,
        child_request_id,
        parent_session_id,
    )
}

async fn drive_bridge_step_projection_case(case: &lean_vocab_test::LeanBridgeStepCase) {
    use gents::background_completion::{
        project_background_subagent_completion, BackgroundCompletionOutcome,
    };

    let (db, _lifecycle, tool_call_id, child_request_id, _parent_session_id) =
        seed_bridge_step_fixture(case).await;
    let child_session_id = fetch_child_session_id(db.node.as_ref(), &child_request_id).await;

    match case.child_state.as_str() {
        "completed" => {
            persist_bridge_step_child_completion(
                db.node.as_ref(),
                &child_request_id,
                &child_session_id,
            )
            .await;
        }
        "processing" => {
            set_request_status_lifecycle_by_request_id(
                db.node.as_ref(),
                &child_request_id,
                "processing",
                "processing",
            )
            .await;
        }
        "interrupted" => {
            set_request_status_lifecycle_by_request_id(
                db.node.as_ref(),
                &child_request_id,
                "interrupted",
                "interrupted",
            )
            .await;
        }
        "failed" => {
            set_request_status_lifecycle_by_request_id(
                db.node.as_ref(),
                &child_request_id,
                "error",
                "failed",
            )
            .await;
        }
        "dead" => {
            set_request_status_lifecycle_by_request_id(
                db.node.as_ref(),
                &child_request_id,
                "dead",
                "dead",
            )
            .await;
        }
        other => panic!("unhandled child state {other}"),
    }

    let outcome =
        project_background_subagent_completion(db.node.clone(), &child_request_id, AGENT_DID)
            .await
            .expect("project background completion");
    let row_state = fetch_bridge_step_tool_state(db.node.as_ref(), &tool_call_id).await;

    if case.legal {
        assert!(
            matches!(outcome, BackgroundCompletionOutcome::Projected { .. }),
            "{}: durable child terminal must project, got {outcome:?}",
            case.name
        );
        assert_eq!(
            row_state.as_deref(),
            case.post_tool_state.as_deref(),
            "{}: projected bridge state drifted from the Lean step",
            case.name
        );
    } else if case.child_state == "processing" {
        assert!(
            matches!(outcome, BackgroundCompletionOutcome::NotTerminal),
            "{}: a live child must not project, got {outcome:?}",
            case.name
        );
        assert_eq!(
            row_state.as_deref(),
            Some("running"),
            "{}: rejected step must leave the bridge running",
            case.name
        );
    } else {
        // bridge_failure with a completed child: the failure projection can
        // never fire — the projection dispatches on the actual durable
        // terminal, so the bridge completes instead of failing.
        assert_eq!(case.child_state, "completed", "{}", case.name);
        assert!(
            matches!(outcome, BackgroundCompletionOutcome::Projected { .. }),
            "{}: completed child projects completion, got {outcome:?}",
            case.name
        );
        assert_eq!(
            row_state.as_deref(),
            Some("completed"),
            "{}: a completed child must never project a failure state",
            case.name
        );
    }
}

async fn drive_bridge_step_cascade_case(case: &lean_vocab_test::LeanBridgeStepCase) {
    let (db, mut lifecycle, _tool_call_id, child_request_id, _parent_session_id) =
        seed_bridge_step_fixture(case).await;
    set_request_status_lifecycle_by_request_id(
        db.node.as_ref(),
        &child_request_id,
        "processing",
        "processing",
    )
    .await;

    if case.parent_state == "processing" {
        // Rejected shape: the bridge is still running (and the parent live),
        // so the cascade decision is illegal at the Rust seam too.
        assert!(!case.legal, "{}", case.name);
        assert!(
            lifecycle.bridge_cancel_cascade().await.is_err(),
            "{}: cascade on a running bridge must be rejected",
            case.name
        );
        return;
    }

    lifecycle
        .cancel_during_run(CancelCause::Interrupted)
        .await
        .expect("cancel bridge before cascade decision");
    let intent = lifecycle
        .bridge_cancel_cascade()
        .await
        .expect("cascade decision");
    if case.post_child_interrupt_set {
        assert!(case.legal, "{}", case.name);
        let intent = intent.expect("cascade policy must produce a cascade intent");
        assert_eq!(
            intent.child_request_id, child_request_id,
            "{}: cascade intent must target the bridged child",
            case.name
        );
    } else {
        assert!(!case.legal, "{}", case.name);
        assert!(
            intent.is_none(),
            "{}: detach must not produce a cascade intent",
            case.name
        );
    }
}

async fn set_request_status_lifecycle_by_request_id(
    node: &EmbeddedNode,
    request_id: &str,
    status: &str,
    lifecycle_state: &str,
) {
    let request_id = escape_graphql_string(request_id);
    let status = escape_graphql_string(status);
    let lifecycle_state = escape_graphql_string(lifecycle_state);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                input: {{ status: "{status}", lifecycle_state: "{lifecycle_state}" }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "set request status/lifecycle failed: {:?}",
        response.errors
    );
}

async fn fetch_child_session_id(node: &EmbeddedNode, child_request_id: &str) -> String {
    #[derive(Deserialize)]
    struct SessionRow {
        session_id: String,
    }
    let child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{child_request_id}" }} }},
                limit: 1
            ) {{ session_id }}
        }}"#
    );
    first_row::<SessionRow>(&node.execute(&query).await, "AgentRequest").session_id
}

async fn fetch_bridge_step_tool_state(node: &EmbeddedNode, tool_call_id: &str) -> Option<String> {
    #[derive(Deserialize)]
    struct StateRow {
        lifecycle_state: Option<String>,
    }
    let tool_call_id = escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{ tool_call_id: {{ _eq: "{tool_call_id}" }} }},
                limit: 1
            ) {{ lifecycle_state }}
        }}"#
    );
    first_row::<StateRow>(&node.execute(&query).await, "AgentToolCall").lifecycle_state
}

async fn persist_bridge_step_child_completion(
    node: &EmbeddedNode,
    child_request_id: &str,
    child_session_id: &str,
) {
    set_request_status_lifecycle_by_request_id(node, child_request_id, "completed", "completed")
        .await;

    let assistant = Message::Assistant {
        id: None,
        content: vec![AssistantContent::Text(Text {
            text: "bridge step child final".to_string(),
        })],
    };
    let escaped_message = escape_graphql_string(&serde_json::to_string(&assistant).unwrap());
    let escaped_child_session_id = escape_graphql_string(child_session_id);
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let now = chrono::Utc::now().to_rfc3339();
    let create_message = format!(
        r#"mutation {{
            create_AgentMessage(input: {{
                message_key: "{escaped_child_session_id}:1",
                session_id: "{escaped_child_session_id}",
                sequence: 1,
                role: "assistant",
                content: "{escaped_message}",
                timestamp: "{now}"
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&create_message).await;
    assert!(
        !response.has_errors(),
        "create bridge-step child AgentMessage failed: {:?}",
        response.errors
    );

    let create_response = format!(
        r#"mutation {{
            create_AgentResponse(input: {{
                response_key: "{escaped_child_request_id}",
                request_id: "{escaped_child_request_id}",
                agent_did: "{AGENT_DID}",
                behavior_id: "bridge-step-child",
                session_id: "{escaped_child_session_id}",
                content: "",
                reasoning: "",
                status: "completed",
                error_message: "",
                token_count: 0,
                progress_seq: 0,
                materialized_message_sequence: 1,
                materialized_at: "{now}",
                created_at: "{now}",
                completed_at: "{now}"
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&create_response).await;
    assert!(
        !response.has_errors(),
        "create bridge-step child AgentResponse failed: {:?}",
        response.errors
    );
}
