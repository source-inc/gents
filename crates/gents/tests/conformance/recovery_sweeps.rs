use super::*;
use std::collections::BTreeMap;
use std::sync::Arc;

const RECOVERY_CREATED_AT: &str = "2026-03-23T00:00:00Z";

pub(super) async fn generated_recovery_sweep_cases_drive_startup_recovery_contract() {
    let cases = lean_recovery_sweep_cases();
    assert_eq!(
        cases.len(),
        37,
        "Lean should emit one row per registered recovery predicate witness"
    );

    let expected_sweep_ids = [
        "request_lifecycle_recover_all_requests",
        "request_lifecycle_recover_all_streaming_responses",
        "tool_call_lifecycle_recover_all_running_calls",
        "tool_call_lifecycle_reconcile_orphaned_background_tools",
        "tool_call_lifecycle_reconcile_background_completion_side_effects",
        "tool_call_lifecycle_reconcile_terminal_parent_owned_tools",
        "tool_call_lifecycle_recover_detached_bridge_rows",
        "inference_call_recover_all_stale_calls",
        "subagent_liveness_terminalize_expired_children",
        "subagent_liveness_interrupt_queued_descendants",
        "request_lifecycle_recover_all_conversations",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let actual_sweep_ids = cases
        .iter()
        .map(|case| case.sweep_id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_sweep_ids, expected_sweep_ids,
        "Lean recovery sweep registry drifted"
    );
    assert_periodic_recovery_registry_matches_lean(cases);

    for case in cases {
        assert_recovery_case_metadata(case);
        drive_recovery_sweep_case(case).await;
    }
}

pub(super) fn generated_recovery_equivalence_cases_pin_uninterrupted_convergence_contract() {
    let sweep_cases = lean_recovery_sweep_cases();
    let equivalence_cases = lean_recovery_equivalence_cases();
    assert_eq!(
        equivalence_cases.len(),
        sweep_cases.len(),
        "Lean must emit one uninterrupted-equivalence witness per recovery sweep case"
    );
    assert_eq!(
        equivalence_cases.len(),
        37,
        "Lean recovery equivalence witness count drifted"
    );

    let sweep_by_name = sweep_cases
        .iter()
        .map(|case| (case.name.as_str(), case))
        .collect::<HashMap<_, _>>();
    let mut seen_sources = BTreeSet::new();
    for case in equivalence_cases {
        let source = sweep_by_name
            .get(case.source_sweep_case.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "recovery equivalence case {} references unknown sweep case {}",
                    case.name, case.source_sweep_case
                )
            });
        assert!(
            seen_sources.insert(case.source_sweep_case.as_str()),
            "duplicate recovery equivalence witness for {}",
            case.source_sweep_case
        );
        assert_eq!(case.sweep_id, source.sweep_id, "sweep id drifted");
        assert_eq!(case.collection, source.collection, "collection drifted");
        assert_eq!(
            case.rust_function, source.rust_function,
            "Rust function drifted"
        );
        assert_eq!(
            case.cadence, source.cadence,
            "recovery equivalence cadence drifted from its source sweep case"
        );
        assert_eq!(case.pre_state, source.pre_state, "pre-state drifted");
        assert_eq!(
            case.recovered_state, source.terminal_state,
            "recovery terminal state drifted"
        );
        assert_eq!(
            case.uninterrupted_state, source.terminal_state,
            "uninterrupted terminal state drifted"
        );
        assert!(
            case.equivalent,
            "recovery case {} must equal the uninterrupted terminalization path",
            case.name
        );
        assert!(
            !case.reexecutes,
            "recovery case {} must not claim tool/request re-execution",
            case.name
        );
        assert!(
            !case.can_hang,
            "recovery case {} must not permit hanging after startup recovery",
            case.name
        );
        assert_eq!(
            case.theorem.as_str(),
            expected_recovery_equivalence_theorem(case.sweep_id.as_str()),
            "wrong concrete Lean equivalence theorem for {}",
            case.name
        );
        assert_eq!(
            case.aggregate_theorem.as_str(),
            "Recovery.RecoveryEquivalence.finite_stale_rows_converge_to_uninterrupted"
        );
    }
    assert_eq!(seen_sources.len(), sweep_cases.len());
}

fn expected_recovery_equivalence_theorem(sweep_id: &str) -> &'static str {
    match sweep_id {
        "request_lifecycle_recover_all_requests" => "Recovery.requestRecover_matches_uninterrupted",
        "request_lifecycle_recover_all_streaming_responses" => {
            "Recovery.responseRecover_matches_uninterrupted"
        }
        "tool_call_lifecycle_recover_all_running_calls" => {
            "Recovery.toolCallRecover_matches_uninterrupted"
        }
        "tool_call_lifecycle_reconcile_orphaned_background_tools" => {
            "Recovery.orphanedBackgroundToolRecover_matches_uninterrupted"
        }
        "tool_call_lifecycle_reconcile_background_completion_side_effects" => {
            "Recovery.backgroundCompletionSideEffectRecover_matches_uninterrupted"
        }
        "tool_call_lifecycle_reconcile_terminal_parent_owned_tools" => {
            "Recovery.terminalParentToolRecover_matches_uninterrupted"
        }
        "tool_call_lifecycle_recover_detached_bridge_rows" => {
            "Recovery.detachedBridgeRecover_matches_uninterrupted"
        }
        "inference_call_recover_all_stale_calls" => {
            "Recovery.inferenceCallRecover_matches_uninterrupted"
        }
        "subagent_liveness_terminalize_expired_children" => {
            "Recovery.expiredChildRecover_matches_uninterrupted"
        }
        "subagent_liveness_interrupt_queued_descendants" => {
            "Recovery.queuedDescendantRecover_matches_uninterrupted"
        }
        "request_lifecycle_recover_all_conversations" => {
            "Recovery.conversation_recover_matches_uninterrupted"
        }
        other => panic!("unhandled recovery equivalence sweep id {other}"),
    }
}

fn assert_recovery_case_metadata(case: &lean_vocab_test::LeanRecoverySweepCase) {
    let expected_cadence = if rust_periodic_recovery_sweep_ids().contains(case.sweep_id.as_str()) {
        "periodic"
    } else {
        "startup"
    };
    assert_eq!(case.cadence.as_str(), expected_cadence, "{}", case.name);
    assert_eq!(
        case.implementation_status.as_str(),
        "implemented",
        "recovery case {} must be implemented before the runtime drive can consume it",
        case.name
    );
    assert!(
        case.measure_before > case.measure_after,
        "recovery case {} must decrease its measure",
        case.name
    );
    assert_eq!(
        case.measure_after, 0,
        "recovery case {} must reach zero measure",
        case.name
    );
    assert_ne!(
        case.terminal_state.as_str(),
        "running",
        "recovery case {} must not leave a stale row running",
        case.name
    );
    assert!(
        !case.deadline_audit_ref.trim().is_empty(),
        "recovery case {} must name its audit reference",
        case.name
    );
}

fn assert_periodic_recovery_registry_matches_lean(
    cases: &[lean_vocab_test::LeanRecoverySweepCase],
) {
    let mut lean_periodic_by_id = BTreeMap::new();
    for case in cases.iter().filter(|case| case.cadence == "periodic") {
        if let Some(previous) =
            lean_periodic_by_id.insert(case.sweep_id.as_str(), case.rust_function.as_str())
        {
            assert_eq!(
                previous,
                case.rust_function.as_str(),
                "Lean emitted conflicting Rust functions for periodic recovery sweep {}",
                case.sweep_id
            );
        }
    }

    let mut rust_periodic_by_id = BTreeMap::new();
    for metadata in gents::periodic_recovery_sweep_metadata() {
        assert!(
            !metadata.sweep_ids.is_empty(),
            "periodic recovery registry entry {} must name at least one Lean sweep id",
            metadata.rust_function
        );
        for sweep_id in metadata.sweep_ids {
            assert!(
                rust_periodic_by_id
                    .insert(*sweep_id, metadata.rust_function)
                    .is_none(),
                "periodic recovery sweep id {sweep_id} registered more than once"
            );
        }
    }

    assert_eq!(
        rust_periodic_by_id.keys().copied().collect::<BTreeSet<_>>(),
        lean_periodic_by_id.keys().copied().collect::<BTreeSet<_>>(),
        "Rust periodic recovery registry drifted from Lean cadence=periodic sweeps"
    );
    for (sweep_id, rust_function) in rust_periodic_by_id {
        assert_eq!(
            Some(&rust_function),
            lean_periodic_by_id.get(sweep_id),
            "periodic recovery registry Rust function drifted for {sweep_id}"
        );
    }
}

fn rust_periodic_recovery_sweep_ids() -> BTreeSet<&'static str> {
    gents::periodic_recovery_sweep_metadata()
        .iter()
        .flat_map(|metadata| metadata.sweep_ids.iter().copied())
        .collect()
}

async fn drive_recovery_sweep_case(case: &lean_vocab_test::LeanRecoverySweepCase) {
    match (case.collection.as_str(), case.sweep_id.as_str()) {
        ("AgentRequest", "subagent_liveness_terminalize_expired_children") => {
            drive_expired_child_recovery_case(case).await
        }
        ("AgentRequest", "subagent_liveness_interrupt_queued_descendants") => {
            drive_queued_descendant_recovery_case(case).await
        }
        ("AgentRequest", _) => drive_request_recovery_case(case).await,
        ("AgentResponse", _) => drive_response_recovery_case(case).await,
        ("AgentToolCall", _) => drive_tool_call_recovery_case(case).await,
        ("InferenceCall", _) => drive_inference_call_recovery_case(case).await,
        ("AgentConversation", _) => drive_conversation_recovery_case(case).await,
        (other, _) => panic!("unhandled recovery collection {other} for {}", case.name),
    }
}

async fn drive_expired_child_recovery_case(case: &lean_vocab_test::LeanRecoverySweepCase) {
    let db = signed_materializer_test_db(&format!("recovery-sweep-{}", case.name)).await;
    let parent_request_id = format!("{}-parent", case.name);
    let parent_session_id = format!("{}-parent-session", case.name);
    create_request(
        &db.node,
        &parent_request_id,
        &parent_session_id,
        "processing",
        RECOVERY_CREATED_AT,
    )
    .await;

    let child_request_id = format!("{}-child", case.name);
    let child_session_id = format!("{child_request_id}-session");
    let child_doc_id = create_request(
        &db.node,
        &child_request_id,
        &child_session_id,
        "processing",
        RECOVERY_CREATED_AT,
    )
    .await;
    if case.pre_state == "claimed" {
        set_request_status_and_lifecycle(&db.node, &child_doc_id, "processing", "claimed").await;
    }
    set_request_deadline(
        &db.node,
        &child_doc_id,
        &(chrono::Utc::now() - chrono::Duration::seconds(5)).to_rfc3339(),
    )
    .await;

    let tool_call_id = format!("{}-bridge", case.name);
    start_running_background_bridge(
        db.node.clone(),
        &parent_request_id,
        &parent_session_id,
        &tool_call_id,
        1,
        &child_request_id,
    )
    .await;

    let report = ToolCallLifecycle::reconcile_subagent_liveness(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(
        report.expired_children_terminalized, 1,
        "{}: expired child terminalized",
        case.name
    );
    assert_eq!(
        report.bridges_projected, 1,
        "{}: background bridge projected the dead child",
        case.name
    );

    let child = fetch_request_recovery_row(&db.node, &child_request_id).await;
    assert_eq!(
        child.lifecycle_state.as_str(),
        case.terminal_state.as_str(),
        "{}: child terminal state drifted",
        case.name
    );
    let bridge_row = fetch_tool_recovery_row(&db.node, &tool_call_id).await;
    assert_eq!(
        bridge_row.lifecycle_state.as_deref(),
        Some("failed"),
        "{}: bridge must reach terminal failed so the parent unblocks",
        case.name
    );

    let second = ToolCallLifecycle::reconcile_subagent_liveness(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert!(
        second.is_noop(),
        "{}: reconciliation must be idempotent across ticks, got {second:?}",
        case.name
    );
}

async fn drive_queued_descendant_recovery_case(case: &lean_vocab_test::LeanRecoverySweepCase) {
    let db = test_db(&format!("recovery-sweep-{}", case.name)).await;
    let parent_request_id = format!("{}-parent", case.name);
    let parent_session_id = format!("{}-parent-session", case.name);
    let parent_doc_id = create_request(
        &db.node,
        &parent_request_id,
        &parent_session_id,
        "processing",
        RECOVERY_CREATED_AT,
    )
    .await;
    set_request_status_and_lifecycle(&db.node, &parent_doc_id, "completed", "completed").await;

    let tool_call_id = format!("{}-bridge", case.name);
    let child_request_id = format!("{}-child", case.name);
    create_linked_pending_child(
        &db.node,
        &child_request_id,
        &format!("{child_request_id}-session"),
        &parent_request_id,
        &tool_call_id,
    )
    .await;
    start_running_background_bridge(
        db.node.clone(),
        &parent_request_id,
        &parent_session_id,
        &tool_call_id,
        1,
        &child_request_id,
    )
    .await;

    let bystander_request_id = format!("{}-wake", case.name);
    create_linked_pending_child(
        &db.node,
        &bystander_request_id,
        &parent_session_id,
        &parent_request_id,
        &tool_call_id,
    )
    .await;

    let remote_parent_request_id = format!("{}-remote-parent", case.name);
    create_remote_terminal_parent(&db.node, &remote_parent_request_id).await;
    let remote_tool_call_id = format!("{}-remote-bridge", case.name);
    let remote_child_request_id = format!("{}-remote-child", case.name);
    create_linked_pending_child(
        &db.node,
        &remote_child_request_id,
        &format!("{remote_child_request_id}-session"),
        &remote_parent_request_id,
        &remote_tool_call_id,
    )
    .await;
    start_running_background_bridge(
        db.node.clone(),
        &remote_parent_request_id,
        &format!("{remote_parent_request_id}-session"),
        &remote_tool_call_id,
        1,
        &remote_child_request_id,
    )
    .await;

    let report = ToolCallLifecycle::reconcile_subagent_liveness(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(
        report.queued_descendants_interrupted, 2,
        "{}: both bridged queued descendants (local + remote parent) interrupted",
        case.name
    );

    let child = fetch_request_recovery_row(&db.node, &child_request_id).await;
    assert_eq!(
        child.lifecycle_state.as_str(),
        case.terminal_state.as_str(),
        "{}: queued descendant terminal state drifted",
        case.name
    );
    let remote_child = fetch_request_recovery_row(&db.node, &remote_child_request_id).await;
    assert_eq!(
        remote_child.lifecycle_state.as_str(),
        case.terminal_state.as_str(),
        "{}: queued descendant of replicated remote terminal parent released",
        case.name
    );
    let bystander = fetch_request_recovery_row(&db.node, &bystander_request_id).await;
    assert_eq!(
        bystander.lifecycle_state.as_str(),
        "pending",
        "{}: lineage-only queue rows (wake/steering) must survive",
        case.name
    );

    let second = ToolCallLifecycle::reconcile_subagent_liveness(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert!(
        second.is_noop(),
        "{}: reconciliation must be idempotent across ticks, got {second:?}",
        case.name
    );
}

/// Issue #1001 defect 2: the startup inference-call sweep is parent-gated, so
/// it must run after request repair. Drives the real ordered startup sweep
/// (`gents::startup_recovery::run_startup_recovery`) over a crash shape — a
/// parent stuck `processing` with a linked `running` call and no live loop —
/// and requires the orphan to terminalize in the FIRST startup pass.
/// Lean: `Recovery.request_before_inference_converges`
/// (`Proofs/Recovery/StartupOrder.lean`).
pub(super) async fn startup_recovery_order_terminalizes_crash_orphaned_calls() {
    let db = test_db("startup-recovery-order-1001").await;
    let request_id = "startup-order-1001-request";
    let session_id = "startup-order-1001-session";
    create_request(
        &db.node,
        request_id,
        session_id,
        "processing",
        RECOVERY_CREATED_AT,
    )
    .await;
    insert_inference_call(&db.node, request_id, "running").await;

    let outcome = gents::startup_recovery::run_startup_recovery(&db.node, AGENT_DID).await;
    let requests = outcome.requests.expect("startup request recovery");
    assert!(
        requests.requests_recovered >= 1,
        "crash-stuck parent must terminalize at startup: {requests:?}"
    );
    let calls = outcome.inference_calls.expect("startup inference recovery");
    assert_eq!(
        calls.calls_recovered, 1,
        "crash-orphaned running call must be terminalized in the first startup \
         pass, not survive until the next restart (#1001)"
    );

    let parent = fetch_request_recovery_row(&db.node, request_id).await;
    assert_eq!(
        parent.lifecycle_state.as_str(),
        "failed",
        "crash-stuck parent repairs to failed from its recovery error response"
    );
    let row = fetch_inference_recovery_row(&db.node, request_id).await;
    assert_eq!(
        row.call_state.as_str(),
        "failed",
        "orphaned running call must not keep holding a reconstructed slot"
    );
    let slot_row = InferenceCallSlotRow::new(BACKEND_ID, row.call_state.as_str());
    assert_eq!(
        reconstructed_running_slot_count([slot_row], BACKEND_ID),
        0,
        "post-recovery rows must reconstruct zero held slots"
    );

    let second = gents::startup_recovery::run_startup_recovery(&db.node, AGENT_DID).await;
    assert_eq!(
        second
            .inference_calls
            .expect("second startup inference recovery")
            .calls_recovered,
        0,
        "startup recovery must be idempotent across restarts"
    );
}

pub(super) async fn subagent_liveness_reconciliation_converges_expired_processing_to_zero() {
    let db = test_db("recovery-465-convergence").await;

    let parent_request_id = "convergence-465-parent";
    let parent_session_id = "convergence-465-parent-session";
    create_request(
        &db.node,
        parent_request_id,
        parent_session_id,
        "processing",
        RECOVERY_CREATED_AT,
    )
    .await;

    for index in 1..=2 {
        let child_request_id = format!("convergence-465-child-{index}");
        let child_doc_id = create_request(
            &db.node,
            &child_request_id,
            &format!("{child_request_id}-session"),
            "processing",
            RECOVERY_CREATED_AT,
        )
        .await;
        set_request_deadline(
            &db.node,
            &child_doc_id,
            &(chrono::Utc::now() - chrono::Duration::seconds(5)).to_rfc3339(),
        )
        .await;
        start_running_background_bridge(
            db.node.clone(),
            parent_request_id,
            parent_session_id,
            &format!("convergence-465-bridge-{index}"),
            index,
            &child_request_id,
        )
        .await;
    }

    let terminal_parent_request_id = "convergence-465-done-parent";
    let terminal_parent_doc_id = create_request(
        &db.node,
        terminal_parent_request_id,
        "convergence-465-done-parent-session",
        "processing",
        RECOVERY_CREATED_AT,
    )
    .await;
    set_request_status_and_lifecycle(&db.node, &terminal_parent_doc_id, "completed", "completed")
        .await;
    let queued_child_request_id = "convergence-465-queued-child";
    create_linked_pending_child(
        &db.node,
        queued_child_request_id,
        "convergence-465-queued-child-session",
        terminal_parent_request_id,
        "convergence-465-done-bridge",
    )
    .await;
    start_running_background_bridge(
        db.node.clone(),
        terminal_parent_request_id,
        "convergence-465-done-parent-session",
        "convergence-465-done-bridge",
        1,
        queued_child_request_id,
    )
    .await;

    assert_eq!(
        count_expired_active_requests(&db.node).await,
        2,
        "wedge precondition: expired processing children visible to status"
    );

    let report = ToolCallLifecycle::reconcile_subagent_liveness(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.expired_children_terminalized, 2);
    assert_eq!(report.bridges_projected, 2);
    assert_eq!(report.queued_descendants_interrupted, 1);

    assert_eq!(
        count_expired_active_requests(&db.node).await,
        0,
        "expired processing measure must converge to zero after one tick"
    );
    for index in 1..=2 {
        let bridge_row =
            fetch_tool_recovery_row(&db.node, &format!("convergence-465-bridge-{index}")).await;
        assert_eq!(
            bridge_row.lifecycle_state.as_deref(),
            Some("failed"),
            "bridge {index} must project its dead child so the parent unblocks"
        );
    }
    let queued_child = fetch_request_recovery_row(&db.node, queued_child_request_id).await;
    assert_eq!(queued_child.lifecycle_state.as_str(), "interrupted");

    let second = ToolCallLifecycle::reconcile_subagent_liveness(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert!(
        second.is_noop(),
        "converged state must be stable across status polls, got {second:?}"
    );
    assert_eq!(count_expired_active_requests(&db.node).await, 0);
}

async fn count_expired_active_requests(node: &EmbeddedNode) -> usize {
    #[derive(Debug, Deserialize)]
    struct DeadlineRow {
        #[serde(default)]
        deadline: Option<String>,
    }
    let query = r#"{
        AgentRequest(
            filter: { lifecycle_state: { _in: ["claimed", "processing"] } }
        ) { deadline }
    }"#;
    let response = node.execute(query).await;
    assert!(
        !response.has_errors(),
        "query active requests failed: {:?}",
        response.errors
    );
    let rows: Vec<DeadlineRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    let now = chrono::Utc::now();
    rows.iter()
        .filter(|row| {
            row.deadline
                .as_deref()
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                .is_some_and(|deadline| now > deadline.with_timezone(&chrono::Utc))
        })
        .count()
}

async fn create_remote_terminal_parent(node: &EmbeddedNode, request_id: &str) {
    let escaped_request_id = escape_graphql_string(request_id);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "did:test:remote-deployment",
                behavior_id: "{AGENT_NAME}",
                session_id: "{escaped_request_id}-session",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "remote parent prompt",
                status: "completed",
                lifecycle_state: "completed",
                backend_id: "",
                execution_origin: "interactive",
                created_at: "{RECOVERY_CREATED_AT}",
                retry_count: 0,
                max_retries: 3
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create remote terminal parent failed: {:?}",
        resp.errors
    );
}

async fn start_running_background_bridge(
    node: Arc<EmbeddedNode>,
    parent_request_id: &str,
    parent_session_id: &str,
    tool_call_id: &str,
    sequence: u32,
    child_request_id: &str,
) {
    let mut bridge = ToolCallLifecycle::new_subagent(
        node,
        parent_request_id.to_string(),
        parent_session_id.to_string(),
        "did:test:test".to_string(),
        tool_call_id.to_string(),
        sequence,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        "did:test:target".to_string(),
    );
    bridge.start_running().await.unwrap();
}

async fn set_request_deadline(node: &EmbeddedNode, doc_id: &str, deadline: &str) {
    let doc_id = escape_graphql_string(doc_id);
    let deadline = escape_graphql_string(deadline);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{ deadline: "{deadline}" }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "set request deadline failed: {:?}",
        resp.errors
    );
}

async fn create_linked_pending_child(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    parent_request_id: &str,
    parent_tool_call_id: &str,
) {
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_parent_request_id = escape_graphql_string(parent_request_id);
    let escaped_parent_tool_call_id = escape_graphql_string(parent_tool_call_id);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{AGENT_DID}",
                behavior_id: "{AGENT_NAME}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "queued child prompt",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "interactive",
                created_at: "{RECOVERY_CREATED_AT}",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: 1,
                caused_by_parent_request_id: "{escaped_parent_request_id}",
                caused_by_parent_tool_call_id: "{escaped_parent_tool_call_id}"
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create linked pending child failed: {:?}",
        resp.errors
    );
}

async fn drive_request_recovery_case(case: &lean_vocab_test::LeanRecoverySweepCase) {
    let db = test_db(&format!("recovery-sweep-{}", case.name)).await;
    let request_id = format!("{}-request", case.name);
    let session_id = format!("{}-session", case.name);
    let doc_id = create_request(
        &db.node,
        &request_id,
        &session_id,
        "processing",
        RECOVERY_CREATED_AT,
    )
    .await;
    set_request_lifecycle_state(&db.node, &doc_id, case.pre_state.as_str()).await;
    let response_status = if case.terminal_state == "completed" {
        "complete"
    } else {
        "error"
    };
    let response_doc_id = create_response_with_status(
        &db.node,
        &request_id,
        &request_id,
        &session_id,
        response_status,
    )
    .await;
    if case.terminal_state == "interrupted" {
        let escaped_response_doc_id = escape_graphql_string(&response_doc_id);
        let response = db
            .node
            .execute(&format!(
                r#"mutation {{
                    update_AgentResponse(
                        filter: {{ _docID: {{ _eq: "{escaped_response_doc_id}" }} }},
                        input: {{
                            error_message: "interrupted",
                            interrupted_at: "2026-07-09T00:00:00Z"
                        }}
                    ) {{ _docID }}
                }}"#
            ))
            .await;
        assert!(
            !response.has_errors(),
            "seed interrupted response intent failed: {:?}",
            response.errors
        );
    }

    let report = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(
        report.requests_recovered, 1,
        "request recovery case {} should recover one request",
        case.name
    );

    let row = fetch_request_recovery_row(&db.node, &request_id).await;
    assert_eq!(
        row.lifecycle_state.as_str(),
        case.terminal_state.as_str(),
        "request recovery case {} terminal state drifted",
        case.name
    );
}

async fn drive_response_recovery_case(case: &lean_vocab_test::LeanRecoverySweepCase) {
    let db = test_db(&format!("recovery-sweep-{}", case.name)).await;
    let request_id = format!("{}-request", case.name);
    let session_id = format!("{}-session", case.name);
    create_response_with_status(
        &db.node,
        &request_id,
        &request_id,
        &session_id,
        case.pre_state.as_str(),
    )
    .await;

    let report = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(
        report.responses_recovered, 1,
        "response recovery case {} should recover one response",
        case.name
    );

    let row = fetch_response_recovery_row(&db.node, &request_id).await;
    assert_eq!(
        row.status.as_str(),
        case.terminal_state.as_str(),
        "response recovery case {} terminal state drifted",
        case.name
    );
}

async fn drive_tool_call_recovery_case(case: &lean_vocab_test::LeanRecoverySweepCase) {
    let db = signed_materializer_test_db(&format!("recovery-sweep-{}", case.name)).await;
    let parent_request_id = format!("{}-parent", case.name);
    let parent_session_id = format!("{}-parent-session", case.name);
    let tool_call_id = format!("{}-tool", case.name);
    seed_tool_parent_and_row(
        db.node.clone(),
        case,
        &parent_request_id,
        &parent_session_id,
        &tool_call_id,
    )
    .await;

    if case.sweep_id == "tool_call_lifecycle_reconcile_terminal_parent_owned_tools" {
        let report = ToolCallLifecycle::reconcile_terminal_parent_owned_tools(&db.node, AGENT_DID)
            .await
            .unwrap();
        assert_eq!(
            report.tool_calls_terminalized, 1,
            "live terminal-parent tool case {} should terminalize one tool call",
            case.name
        );
        let second = ToolCallLifecycle::reconcile_terminal_parent_owned_tools(&db.node, AGENT_DID)
            .await
            .unwrap();
        assert_eq!(
            second.tool_calls_terminalized, 0,
            "live terminal-parent tool case {} must be idempotent",
            case.name
        );
    } else if case.sweep_id == "tool_call_lifecycle_reconcile_background_completion_side_effects" {
        let report =
            ToolCallLifecycle::reconcile_background_completion_side_effects(&db.node, AGENT_DID)
                .await
                .unwrap();
        assert_eq!(
            report.side_effects_converged, 1,
            "background completion case {} should converge one obligation",
            case.name
        );
        let second =
            ToolCallLifecycle::reconcile_background_completion_side_effects(&db.node, AGENT_DID)
                .await
                .unwrap();
        assert!(
            second.is_noop(),
            "background completion case {} must be idempotent",
            case.name
        );
        assert_eq!(
            load_restart_notification_messages(&db.node, &parent_session_id)
                .await
                .len(),
            1,
            "background completion case {} must converge exactly one notification",
            case.name
        );
        assert_eq!(
            load_restart_wake_rows(&db.node, &parent_session_id)
                .await
                .len(),
            1,
            "background completion case {} must converge exactly one wake",
            case.name
        );
    } else if case.sweep_id == "tool_call_lifecycle_reconcile_orphaned_background_tools" {
        assert_eq!(
            case.execution_registered,
            Some(false),
            "orphaned background witness {} must be unregistered",
            case.name
        );
        let registry = gents::BackgroundExecutionRegistry::default();
        let report =
            ToolCallLifecycle::reconcile_orphaned_background_tools(&db.node, AGENT_DID, &registry)
                .await
                .unwrap();
        assert_eq!(
            report.tool_calls_terminalized, 1,
            "orphaned background case {} should terminalize one tool call",
            case.name
        );
        let notifications = load_restart_notification_messages(&db.node, &parent_session_id).await;
        match case.notification_reason.as_deref() {
            Some(reason) => {
                assert_eq!(
                    notifications.len(),
                    1,
                    "orphaned background case {} must append exactly one notification",
                    case.name
                );
                assert!(
                    notifications[0].contains(&format!("<reason>{reason}</reason>")),
                    "{}: orphan notification must carry Lean-pinned reason {reason}: {}",
                    case.name,
                    notifications[0]
                );
            }
            None => assert!(
                notifications.is_empty(),
                "orphaned background case {} without an observed parent cannot notify",
                case.name
            ),
        }
    } else {
        let report = ToolCallLifecycle::recover_all(&db.node, AGENT_DID)
            .await
            .unwrap();
        assert_eq!(
            report.tool_calls_recovered, 1,
            "tool recovery case {} should recover one tool call",
            case.name
        );
    }

    let row = fetch_tool_recovery_row(&db.node, &tool_call_id).await;
    assert_eq!(
        row.lifecycle_state.as_deref(),
        Some(case.terminal_state.as_str()),
        "tool recovery case {} terminal state drifted",
        case.name
    );
    assert_eq!(
        row.status.as_deref(),
        Some("completed"),
        "tool recovery case {} must persist completed status with terminal lifecycle_state",
        case.name
    );
    if case.sweep_id == "tool_call_lifecycle_reconcile_orphaned_background_tools" {
        match case.recovery_cause.as_deref() {
            Some("deadlineExceeded") | Some("parentTerminal") => assert_eq!(
                row.tool_failure_class.as_deref(),
                Some("external"),
                "{}: Lean-pinned recovery cause must preserve external failure classification",
                case.name
            ),
            Some("unclaimedCrossDeploymentSpawn") => assert_eq!(
                row.tool_failure_class.as_deref(),
                Some("serviceUnavailable"),
                "{}: unclaimed recovery must preserve service-unavailable classification",
                case.name
            ),
            Some("TerminalizeBackgroundedAsInterrupted") | Some("parentInterrupted") => {
                assert_eq!(
                    row.tool_failure_class, None,
                    "{}: cancellation recovery must not invent a failure class",
                    case.name
                );
                assert_eq!(
                    row.cancel_cause.as_deref(),
                    Some("interrupted"),
                    "{}: cancellation recovery must preserve interrupted cause",
                    case.name
                );
            }
            other => panic!(
                "{}: missing or unsupported Lean recovery cause {other:?}",
                case.name
            ),
        }
    }
    if case.terminal_state == "timedOut" {
        assert_eq!(
            row.tool_failure_class.as_deref(),
            Some("external"),
            "timeout recovery should persist external failure class"
        );
        assert_eq!(
            row.cancel_cause.as_deref(),
            Some("deadline"),
            "timeout recovery should persist cancel_cause=deadline"
        );
    }
    if case.terminal_state == "cancelled" {
        assert_eq!(
            row.cancel_cause.as_deref(),
            Some("interrupted"),
            "cancel recovery should persist cancel_cause=interrupted"
        );
    }
}

async fn drive_inference_call_recovery_case(case: &lean_vocab_test::LeanRecoverySweepCase) {
    let db = test_db(&format!("recovery-sweep-{}", case.name)).await;
    let request_id = format!("{}-request", case.name);
    let session_id = format!("{}-session", case.name);
    let parent_doc_id = create_request(
        &db.node,
        &request_id,
        &session_id,
        "processing",
        RECOVERY_CREATED_AT,
    )
    .await;
    match case.name.as_str() {
        "inference_interrupted_parent_to_cancelled" => {
            set_request_status_and_lifecycle(
                &db.node,
                &parent_doc_id,
                "interrupted",
                "interrupted",
            )
            .await;
        }
        "inference_queued_stale_to_cancelled" | "inference_running_stale_to_failed" => {
            set_request_status_and_lifecycle(&db.node, &parent_doc_id, "completed", "completed")
                .await;
        }
        other => panic!("unhandled inference recovery case {other}"),
    }
    insert_inference_call(&db.node, &request_id, case.pre_state.as_str()).await;

    let report = InferenceCall::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(
        report.calls_recovered, 1,
        "inference recovery case {} should recover one call",
        case.name
    );

    let row = fetch_inference_recovery_row(&db.node, &request_id).await;
    assert_eq!(
        row.call_state.as_str(),
        case.terminal_state.as_str(),
        "inference recovery case {} terminal state drifted",
        case.name
    );
    let terminal_row = InferenceCallSlotRow::new(BACKEND_ID, row.call_state.as_str());
    assert_eq!(slot_contribution(terminal_row, BACKEND_ID), 0);
    assert_eq!(
        reconstructed_running_slot_count([terminal_row], BACKEND_ID),
        0,
        "terminal InferenceCall recovery case {} must reconstruct zero running slots",
        case.name
    );
}

async fn seed_tool_parent_and_row(
    node: Arc<EmbeddedNode>,
    case: &lean_vocab_test::LeanRecoverySweepCase,
    parent_request_id: &str,
    parent_session_id: &str,
    tool_call_id: &str,
) {
    let parent_doc_id = create_request(
        &node,
        parent_request_id,
        parent_session_id,
        "processing",
        RECOVERY_CREATED_AT,
    )
    .await;
    let future_deadline = chrono::Utc::now() + chrono::Duration::minutes(5);
    let past_deadline = chrono::Utc::now() - chrono::Duration::seconds(5);
    let is_orphan_case = case.sweep_id == "tool_call_lifecycle_reconcile_orphaned_background_tools";
    let parent_observed = case.parent_live == Some(true)
        || case.parent_interrupted == Some(true)
        || case.parent_terminal == Some(true);
    let mut lifecycle = if is_orphan_case {
        ToolCallLifecycle::new_background_tool(
            node.clone(),
            if parent_observed {
                parent_request_id.to_string()
            } else {
                format!("{parent_request_id}-missing")
            },
            parent_session_id.to_string(),
            "did:test:test".to_string(),
            tool_call_id.to_string(),
            1,
            "spawn_process".to_string(),
            "{}".to_string(),
            future_deadline,
        )
    } else {
        match case.name.as_str() {
            "tool_backgrounded_running_live_parent_to_cancelled"
            | "terminal_background_tool_missing_completion_side_effects_to_converged" => {
                ToolCallLifecycle::new_background_tool(
                    node.clone(),
                    parent_request_id.to_string(),
                    parent_session_id.to_string(),
                    "did:test:test".to_string(),
                    tool_call_id.to_string(),
                    1,
                    "spawn_process".to_string(),
                    "{}".to_string(),
                    future_deadline,
                )
            }
            "tool_running_child_completed_to_completed"
            | "tool_running_child_failed_to_failed"
            | "tool_running_child_dead_to_failed"
            | "tool_running_child_interrupted_to_cancelled" => {
                let child_request_id = format!("{tool_call_id}-child");
                let child_state = match case.name.as_str() {
                    "tool_running_child_completed_to_completed" => "completed",
                    "tool_running_child_failed_to_failed" => "failed",
                    "tool_running_child_dead_to_failed" => "dead",
                    "tool_running_child_interrupted_to_cancelled" => "interrupted",
                    _ => unreachable!(),
                };
                seed_child_request(&node, &child_request_id, child_state).await;
                ToolCallLifecycle::new_subagent(
                    node.clone(),
                    parent_request_id.to_string(),
                    parent_session_id.to_string(),
                    "did:test:test".to_string(),
                    tool_call_id.to_string(),
                    1,
                    "spawn_subagent".to_string(),
                    "{}".to_string(),
                    future_deadline,
                    AwaitMode::Foreground,
                    CancelPolicy::Cascade,
                    child_request_id,
                    "did:test:target".to_string(),
                )
            }
            "detached_bridge_child_completed_to_completed"
            | "detached_bridge_child_failed_to_failed"
            | "detached_bridge_child_interrupted_to_cancelled"
            | "detached_bridge_terminal_parent_to_failed"
            | "detached_bridge_deadline_exceeded_to_timed_out" => {
                let child_request_id = format!("{tool_call_id}-child");
                let child_state = match case.name.as_str() {
                    "detached_bridge_child_completed_to_completed" => "completed",
                    "detached_bridge_child_failed_to_failed" => "failed",
                    "detached_bridge_child_interrupted_to_cancelled" => "interrupted",
                    _ => "processing",
                };
                seed_child_request(&node, &child_request_id, child_state).await;
                if case.name == "detached_bridge_terminal_parent_to_failed" {
                    set_request_status_and_lifecycle(&node, &parent_doc_id, "error", "failed")
                        .await;
                }
                ToolCallLifecycle::new_subagent(
                    node.clone(),
                    parent_request_id.to_string(),
                    parent_session_id.to_string(),
                    "did:test:test".to_string(),
                    tool_call_id.to_string(),
                    1,
                    "spawn_subagent".to_string(),
                    "{}".to_string(),
                    if case.name == "detached_bridge_deadline_exceeded_to_timed_out" {
                        past_deadline
                    } else {
                        future_deadline
                    },
                    AwaitMode::Background,
                    CancelPolicy::Detach,
                    child_request_id,
                    "did:test:target".to_string(),
                )
            }
            "tool_running_deadline_exceeded_to_timed_out" => ToolCallLifecycle::new(
                node.clone(),
                parent_request_id.to_string(),
                parent_session_id.to_string(),
                "did:test:test".to_string(),
                tool_call_id.to_string(),
                1,
                "slow_tool".to_string(),
                "{}".to_string(),
                past_deadline,
            ),
            "tool_running_parent_interrupted_to_cancelled"
            | "live_running_composite_parent_interrupted_to_cancelled" => {
                set_request_status_and_lifecycle(
                    &node,
                    &parent_doc_id,
                    "interrupted",
                    "interrupted",
                )
                .await;
                ToolCallLifecycle::new(
                    node.clone(),
                    parent_request_id.to_string(),
                    parent_session_id.to_string(),
                    "did:test:test".to_string(),
                    tool_call_id.to_string(),
                    1,
                    if case.name == "live_running_composite_parent_interrupted_to_cancelled" {
                        "fan_out_and_synthesize"
                    } else {
                        "slow_tool"
                    }
                    .to_string(),
                    "{}".to_string(),
                    future_deadline,
                )
            }
            "tool_running_terminal_parent_to_failed"
            | "live_running_tool_parent_terminal_to_failed" => {
                set_request_status_and_lifecycle(&node, &parent_doc_id, "completed", "completed")
                    .await;
                ToolCallLifecycle::new(
                    node.clone(),
                    parent_request_id.to_string(),
                    parent_session_id.to_string(),
                    "did:test:test".to_string(),
                    tool_call_id.to_string(),
                    1,
                    "slow_tool".to_string(),
                    "{}".to_string(),
                    future_deadline,
                )
            }
            "live_detached_bridge_parent_failed_to_failed" => {
                set_request_status_and_lifecycle(&node, &parent_doc_id, "error", "failed").await;
                let child_request_id = format!("{tool_call_id}-detached-child");
                seed_child_request(&node, &child_request_id, "processing").await;
                ToolCallLifecycle::new_subagent(
                    node.clone(),
                    parent_request_id.to_string(),
                    parent_session_id.to_string(),
                    "did:test:test".to_string(),
                    tool_call_id.to_string(),
                    1,
                    "spawn_subagent".to_string(),
                    "{}".to_string(),
                    future_deadline,
                    AwaitMode::Background,
                    CancelPolicy::Detach,
                    child_request_id,
                    "did:test:target".to_string(),
                )
            }
            "tool_running_unclaimed_cross_deployment_spawn_to_failed" => {
                let child_request_id = format!("{tool_call_id}-remote-child");
                ToolCallLifecycle::new_subagent(
                    node.clone(),
                    parent_request_id.to_string(),
                    parent_session_id.to_string(),
                    "did:test:test".to_string(),
                    tool_call_id.to_string(),
                    1,
                    "spawn_subagent".to_string(),
                    "{}".to_string(),
                    future_deadline,
                    AwaitMode::Background,
                    CancelPolicy::Cascade,
                    child_request_id,
                    "did:test:target".to_string(),
                )
            }
            other => panic!("unhandled tool recovery case {other}"),
        }
    };
    lifecycle.start_running().await.unwrap();

    if case.name == "tool_running_unclaimed_cross_deployment_spawn_to_failed"
        || case.unclaimed_expired == Some(true)
    {
        set_tool_unclaimed_deadline(&node, tool_call_id, "2020-01-01T00:00:00Z").await;
    }
    if case.deadline_expired == Some(true) {
        set_tool_deadline(&node, tool_call_id, "2020-01-01T00:00:00Z").await;
    }
    if case.parent_interrupted == Some(true) {
        set_request_status_and_lifecycle(&node, &parent_doc_id, "interrupted", "interrupted").await;
    } else if case.parent_terminal == Some(true) {
        set_request_status_and_lifecycle(&node, &parent_doc_id, "completed", "completed").await;
    }
    if case.name == "terminal_background_tool_missing_completion_side_effects_to_converged" {
        lifecycle
            .bridge_failure(ChildTerminal::Failed {
                reason: "seed terminal background failure".to_string(),
                failure_class: FailureClass::External,
            })
            .await
            .unwrap();
    }
}

async fn seed_child_request(node: &EmbeddedNode, request_id: &str, lifecycle_state: &str) {
    let session_id = format!("{request_id}-session");
    match lifecycle_state {
        "completed" => {
            create_request(
                node,
                request_id,
                &session_id,
                "completed",
                RECOVERY_CREATED_AT,
            )
            .await;
            create_response_with_content_and_status(
                node,
                request_id,
                request_id,
                &session_id,
                "child final answer",
                "complete",
            )
            .await;
        }
        "failed" => {
            create_request(node, request_id, &session_id, "error", RECOVERY_CREATED_AT).await;
        }
        "interrupted" => {
            let doc_id = create_request(
                node,
                request_id,
                &session_id,
                "processing",
                RECOVERY_CREATED_AT,
            )
            .await;
            set_request_status_and_lifecycle(node, &doc_id, "interrupted", "interrupted").await;
        }
        "dead" => {
            let doc_id = create_request(
                node,
                request_id,
                &session_id,
                "processing",
                RECOVERY_CREATED_AT,
            )
            .await;
            set_request_status_and_lifecycle(node, &doc_id, "dead", "dead").await;
        }
        "processing" => {
            create_request(
                node,
                request_id,
                &session_id,
                "processing",
                RECOVERY_CREATED_AT,
            )
            .await;
        }
        other => panic!("unsupported child lifecycle state {other}"),
    };
}

async fn set_request_status_and_lifecycle(
    node: &EmbeddedNode,
    doc_id: &str,
    status: &str,
    lifecycle_state: &str,
) {
    let doc_id = escape_graphql_string(doc_id);
    let status = escape_graphql_string(status);
    let lifecycle_state = escape_graphql_string(lifecycle_state);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{ status: "{status}", lifecycle_state: "{lifecycle_state}" }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "set request status/lifecycle failed: {:?}",
        resp.errors
    );
}

async fn set_tool_unclaimed_deadline(node: &EmbeddedNode, tool_call_id: &str, at: &str) {
    #[derive(Debug, Deserialize)]
    struct ToolDateTimeRow {
        started_at: Option<String>,
        deadline_at: Option<String>,
    }

    let escaped_tool_call_id = escape_graphql_string(tool_call_id);
    let read_query = format!(
        r#"{{
            AgentToolCall(filter: {{ tool_call_id: {{ _eq: "{escaped_tool_call_id}" }} }}, limit: 1) {{
                started_at
                deadline_at
            }}
        }}"#
    );
    let row: ToolDateTimeRow = first_row(&node.execute(&read_query).await, "AgentToolCall");
    let started_at = datetime_update_field("started_at", row.started_at.as_deref());
    let deadline_at = datetime_update_field("deadline_at", row.deadline_at.as_deref());
    let at = escape_graphql_string(at);
    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{ tool_call_id: {{ _eq: "{escaped_tool_call_id}" }} }},
                input: {{ unclaimed_deadline_at: "{at}"{started_at}{deadline_at} }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "set tool unclaimed deadline failed: {:?}",
        resp.errors
    );
}

async fn set_tool_deadline(node: &EmbeddedNode, tool_call_id: &str, at: &str) {
    #[derive(Debug, Deserialize)]
    struct ToolDateTimeRow {
        started_at: Option<String>,
    }

    let escaped_tool_call_id = escape_graphql_string(tool_call_id);
    let read_query = format!(
        r#"{{
            AgentToolCall(filter: {{ tool_call_id: {{ _eq: "{escaped_tool_call_id}" }} }}, limit: 1) {{
                started_at
            }}
        }}"#
    );
    let row: ToolDateTimeRow = first_row(&node.execute(&read_query).await, "AgentToolCall");
    let started_at = datetime_update_field("started_at", row.started_at.as_deref());
    let at = escape_graphql_string(at);
    let mutation = format!(
        r#"mutation {{
            update_AgentToolCall(
                filter: {{ tool_call_id: {{ _eq: "{escaped_tool_call_id}" }} }},
                input: {{ deadline_at: "{at}"{started_at} }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "set tool deadline failed: {:?}",
        resp.errors
    );
}

fn datetime_update_field(field: &str, value: Option<&str>) -> String {
    value
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!(r#", {field}: "{}""#, escape_graphql_string(value)))
        .unwrap_or_default()
}

async fn insert_inference_call(node: &EmbeddedNode, request_id: &str, call_state: &str) {
    let call_id = format!("{request_id}-call");
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            add_InferenceCall(input: {{
                call_id: "{call_id}",
                runtime_instance_id: "runtime-recovery-test",
                request_id: "{request_id}",
                call_seq: 1,
                backend_id: "{BACKEND_ID}",
                behavior_id: "{AGENT_NAME}",
                agent_did: "{AGENT_DID}",
                call_kind: "inference",
                attempt: 1,
                call_state: "{call_state}",
                queued_at: "{now}",
                started_at: "{now}",
                priority: 0,
                queue_depth_at_enqueue: 0,
                controller_generation: 0,
                backend_config_fingerprint: "test"
            }}) {{ _docID }}
        }}"#,
        call_id = escape_graphql_string(&call_id),
        request_id = escape_graphql_string(request_id),
        call_state = escape_graphql_string(call_state),
        now = escape_graphql_string(&now),
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "insert inference call failed: {:?}",
        resp.errors
    );
}

#[derive(Debug, Deserialize)]
struct RequestRecoveryRow {
    lifecycle_state: String,
}

async fn fetch_request_recovery_row(node: &EmbeddedNode, request_id: &str) -> RequestRecoveryRow {
    let request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                lifecycle_state
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentRequest")
}

#[derive(Debug, Deserialize)]
struct ResponseRecoveryRow {
    status: String,
}

async fn fetch_response_recovery_row(node: &EmbeddedNode, request_id: &str) -> ResponseRecoveryRow {
    let request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentResponse(filter: {{ request_id: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                status
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentResponse")
}

#[derive(Debug, Deserialize)]
struct ToolRecoveryRow {
    status: Option<String>,
    lifecycle_state: Option<String>,
    tool_failure_class: Option<String>,
    cancel_cause: Option<String>,
}

async fn fetch_tool_recovery_row(node: &EmbeddedNode, tool_call_id: &str) -> ToolRecoveryRow {
    let tool_call_id = escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{
            AgentToolCall(filter: {{ tool_call_id: {{ _eq: "{tool_call_id}" }} }}, limit: 1) {{
                status
                lifecycle_state
                tool_failure_class
                cancel_cause
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentToolCall")
}

#[derive(Debug, Deserialize)]
struct InferenceRecoveryRow {
    call_state: String,
}

async fn fetch_inference_recovery_row(
    node: &EmbeddedNode,
    request_id: &str,
) -> InferenceRecoveryRow {
    let request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            InferenceCall(filter: {{ request_id: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                call_state
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "InferenceCall")
}

async fn drive_conversation_recovery_case(case: &lean_vocab_test::LeanRecoverySweepCase) {
    let db = test_db("generated-conversation-recovery").await;
    let session_id = format!("session-{}", case.name);
    let request_id = format!("request-{}", case.name);

    let request_status = if case.terminal_state == "completed" {
        "completed"
    } else {
        "error"
    };
    create_request(
        &db.node,
        &request_id,
        &session_id,
        request_status,
        RECOVERY_CREATED_AT,
    )
    .await;

    let doc_id = create_conversation_row(
        &db.node,
        &session_id,
        "Conversation",
        "hello",
        &case.pre_state,
        RECOVERY_CREATED_AT,
        RECOVERY_CREATED_AT,
        &request_id,
    )
    .await;

    let report = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .expect("conversation recovery");
    assert_eq!(
        report.conversations_recovered, 1,
        "case {} must recover exactly one session",
        case.name
    );
    assert_eq!(report.conversations_failed, 0, "case {}", case.name);
    assert_eq!(
        conversation_status_by_doc_id(&db.node, &doc_id).await,
        case.terminal_state,
        "case {} must reach the Lean-computed terminal state",
        case.name
    );
}

pub(super) async fn generated_recovery_outcome_cases_fence_duplicate_tolerant_counting() {
    let cases = lean_recovery_outcome_cases();
    assert!(!cases.is_empty(), "Lean emitted no recovery outcome cases");

    for case in cases {
        assert_eq!(
            case.collection, "AgentConversation",
            "outcome cases currently model the conversation sweep only"
        );
        assert_eq!(
            case.sweep_id, "request_lifecycle_recover_all_conversations",
            "case {}",
            case.name
        );
        // The write must be addressed by _docID: a session_id filter matches
        // every duplicate and DefraDB refuses it (#693 defect 1).
        assert_eq!(
            case.target_selector, "_docID",
            "case {} must address the canonical doc by _docID",
            case.name
        );
        if !case.write_succeeds {
            assert_eq!(case.expected_recovered, 0, "case {}", case.name);
            assert!(case.measure_after > 0, "case {}", case.name);
            continue;
        }
        assert!(
            case.expected_recovered <= 1,
            "case {} must count sessions, not documents",
            case.name
        );
    }

    drive_duplicate_conversation_outcome_case().await;
}

async fn drive_duplicate_conversation_outcome_case() {
    let case = lean_recovery_outcome_cases()
        .iter()
        .find(|case| case.duplicated && case.write_succeeds && case.expected_recovered == 1)
        .expect("Lean must emit a recovering duplicate-group case");

    let db =
        test_db_with_duplicate_tolerant_conversations("generated-conversation-duplicate").await;
    create_request(
        &db.node,
        "dup-request",
        "session-dup",
        "completed",
        RECOVERY_CREATED_AT,
    )
    .await;

    let canonical = create_conversation_row(
        &db.node,
        "session-dup",
        "Real conversation",
        "hello",
        "processing",
        RECOVERY_CREATED_AT,
        "2026-03-23T00:05:00Z",
        "dup-request",
    )
    .await;
    let duplicate = create_conversation_row(
        &db.node,
        "session-dup",
        "",
        "",
        "processing",
        RECOVERY_CREATED_AT,
        "2026-03-22T00:00:00Z",
        "",
    )
    .await;
    assert_eq!(
        case.doc_count, 2,
        "the Lean case models a two-document duplicate group"
    );

    let report = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .expect("recovery must be total on a duplicate store");
    assert_eq!(report.conversations_recovered, case.expected_recovered);
    assert_eq!(report.conversations_failed, case.expected_failed);
    assert_eq!(report.duplicate_conversation_sessions, 1);

    assert_eq!(
        conversation_status_by_doc_id(&db.node, &canonical).await,
        "completed"
    );
    assert_eq!(
        conversation_status_by_doc_id(&db.node, &duplicate).await,
        "completed"
    );

    let second = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .expect("second pass");
    assert_eq!(
        second.conversations_recovered, 0,
        "an already-recovered store must report no recoveries"
    );
}

/// Drives the Lean restart-disposition witnesses (#937) through the real
/// startup sweep. The leave-running rows are the previously unfenced half of
/// the contract: `recover_all` must preserve a running background subagent
/// bridge under a live parent (R5) while interrupting a native background
/// tool under the same parent (R6), and the interrupt owes a durable
/// `interrupted_on_restart` notification plus one coalesced wake.
pub(super) async fn generated_restart_disposition_cases_drive_recover_all() {
    let cases = lean_restart_disposition_cases();
    assert_eq!(
        cases.len(),
        10,
        "Lean restart-disposition case family drifted"
    );
    assert!(
        cases.iter().any(|case| case.disposition == "leave_running"),
        "family must include leave-running rows"
    );
    assert!(
        cases.iter().any(|case| case.disposition == "terminalize"),
        "family must include terminalize rows"
    );

    for case in cases {
        assert_eq!(
            case.rust_function, "ToolCallLifecycle::recover_all",
            "restart disposition case {} names the wrong Rust owner",
            case.name
        );
        drive_restart_disposition_case(case).await;
    }
}

async fn drive_restart_disposition_case(case: &lean_vocab_test::LeanRestartDispositionCase) {
    let db = signed_materializer_test_db(&format!("restart-disposition-{}", case.name)).await;
    let parent_request_id = format!("{}-parent", case.name);
    let parent_session_id = format!("{}-parent-session", case.name);
    let tool_call_id = format!("{}-tool", case.name);

    // Parent per the Lean observation vocabulary. `missing` seeds no parent
    // row at all: the bridge's request_id resolves to nothing.
    if case.parent_observation != "missing" {
        let parent_doc_id = create_request(
            &db.node,
            &parent_request_id,
            &parent_session_id,
            "processing",
            RECOVERY_CREATED_AT,
        )
        .await;
        match case.parent_observation.as_str() {
            "live" => {}
            "interrupted" => {
                set_request_status_and_lifecycle(
                    &db.node,
                    &parent_doc_id,
                    "interrupted",
                    "interrupted",
                )
                .await;
            }
            "cleanlyCompleted" => {
                set_request_status_and_lifecycle(
                    &db.node,
                    &parent_doc_id,
                    "completed",
                    "completed",
                )
                .await;
            }
            "otherTerminal" => {
                set_request_status_and_lifecycle(&db.node, &parent_doc_id, "error", "failed").await;
            }
            other => panic!("unhandled parent observation {other}"),
        }
    }

    let deadline = if case.deadline_expired {
        chrono::Utc::now() - chrono::Duration::seconds(5)
    } else {
        chrono::Utc::now() + chrono::Duration::minutes(5)
    };
    let await_mode = match case.await_mode.as_str() {
        "background" => AwaitMode::Background,
        "foreground" => AwaitMode::Foreground,
        other => panic!("unhandled await mode {other}"),
    };
    let cancel_policy = match case.cancel_policy.as_str() {
        "cascade" => CancelPolicy::Cascade,
        "detach" => CancelPolicy::Detach,
        other => panic!("unhandled cancel policy {other}"),
    };

    let mut lifecycle = if case.child_linked {
        // Non-terminal child: rows reaching the classifier have no durable
        // child terminal (child precedence is covered by the sweep cases).
        let child_request_id = format!("{tool_call_id}-child");
        seed_child_request(&db.node, &child_request_id, "processing").await;
        ToolCallLifecycle::new_subagent(
            db.node.clone(),
            parent_request_id.clone(),
            parent_session_id.clone(),
            "did:test:test".to_string(),
            tool_call_id.clone(),
            1,
            "spawn_subagent".to_string(),
            "{}".to_string(),
            deadline,
            await_mode,
            cancel_policy,
            child_request_id,
            "did:test:target".to_string(),
        )
    } else if await_mode == AwaitMode::Background {
        assert_eq!(
            case.cancel_policy, "cascade",
            "native background rows always persist cascade cancel policy"
        );
        ToolCallLifecycle::new_background_tool(
            db.node.clone(),
            parent_request_id.clone(),
            parent_session_id.clone(),
            "did:test:test".to_string(),
            tool_call_id.clone(),
            1,
            "spawn_process".to_string(),
            "{}".to_string(),
            deadline,
        )
    } else {
        ToolCallLifecycle::new(
            db.node.clone(),
            parent_request_id.clone(),
            parent_session_id.clone(),
            "did:test:test".to_string(),
            tool_call_id.clone(),
            1,
            "slow_tool".to_string(),
            "{}".to_string(),
            deadline,
        )
    };
    lifecycle.start_running().await.unwrap();
    if case.unclaimed_expired {
        set_tool_unclaimed_deadline(&db.node, &tool_call_id, "2020-01-01T00:00:00Z").await;
    }

    let report = ToolCallLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    let row = fetch_tool_recovery_row(&db.node, &tool_call_id).await;

    match case.disposition.as_str() {
        "leave_running" => {
            assert_eq!(
                report.tool_calls_recovered, 0,
                "leave-running case {} must not count a recovery",
                case.name
            );
            assert_eq!(
                row.lifecycle_state.as_deref(),
                Some("running"),
                "leave-running case {} must preserve the running row",
                case.name
            );
            assert_eq!(case.cause, None, "{}", case.name);
            assert_eq!(case.terminal_state, None, "{}", case.name);
            if case.child_linked {
                // Preserving the bridge must not secretly cascade an
                // interrupt to the child request either.
                let child_interrupt =
                    fetch_interrupt_requested_at(&db.node, &format!("{tool_call_id}-child"))
                        .await
                        .expect("fetch child interrupt_requested_at");
                assert!(
                    child_interrupt.is_none(),
                    "leave-running case {} must not interrupt the child request",
                    case.name
                );
            }
        }
        "terminalize" => {
            assert_eq!(
                report.tool_calls_recovered, 1,
                "terminalize case {} must recover exactly one row",
                case.name
            );
            assert_eq!(
                row.lifecycle_state.as_deref(),
                case.terminal_state.as_deref(),
                "terminalize case {} landed on the wrong terminal state",
                case.name
            );
        }
        other => panic!("unhandled disposition {other}"),
    }

    let notifications = load_restart_notification_messages(&db.node, &parent_session_id).await;
    let wakes = load_restart_wake_rows(&db.node, &parent_session_id).await;
    if let Some(reason) = case.notification_reason.as_deref() {
        assert_eq!(
            notifications.len(),
            1,
            "restart recovery case {} must append exactly one notification",
            case.name
        );
        assert!(
            notifications[0].contains("<tool-completion"),
            "{}: notification must be a tool completion: {}",
            case.name,
            notifications[0]
        );
        let notification_status = if case.terminal_state.as_deref() == Some("cancelled") {
            "cancelled"
        } else {
            "failed"
        };
        assert!(
            notifications[0].contains(&format!(r#"status="{notification_status}""#)),
            "{}: notification must carry status {notification_status}",
            case.name
        );
        assert!(
            notifications[0].contains(&format!("<reason>{reason}</reason>")),
            "{}: notification must carry the Lean-pinned reason {reason}",
            case.name
        );

        let queue_source = case
            .queue_source
            .as_deref()
            .expect("restart recovery case must pin the queue source");
        let queue_key = format!(
            "{}{}",
            case.queue_key_prefix
                .as_deref()
                .expect("restart recovery case must pin the queue key prefix"),
            parent_session_id
        );
        assert_eq!(
            wakes.len(),
            1,
            "restart recovery case {} must enqueue exactly one coalesced wake",
            case.name
        );
        let metadata: serde_json::Value = serde_json::from_str(
            wakes[0]
                .as_deref()
                .expect("wake request must carry queue metadata"),
        )
        .expect("wake metadata must be JSON");
        assert_eq!(metadata["queue"]["source"], queue_source, "{}", case.name);
        assert_eq!(metadata["queue"]["key"], queue_key, "{}", case.name);

        // Idempotence: a second startup pass finds no running row, appends no
        // duplicate notification, and enqueues no second wake.
        let second = ToolCallLifecycle::recover_all(&db.node, AGENT_DID)
            .await
            .unwrap();
        assert_eq!(second.tool_calls_recovered, 0, "{}", case.name);
        assert_eq!(
            load_restart_notification_messages(&db.node, &parent_session_id)
                .await
                .len(),
            1,
            "{}: second recovery pass must not duplicate the notification",
            case.name
        );
        assert_eq!(
            load_restart_wake_rows(&db.node, &parent_session_id)
                .await
                .len(),
            1,
            "{}: second recovery pass must not duplicate the wake",
            case.name
        );
    } else {
        assert!(
            notifications.is_empty(),
            "case {} owes no restart notification, found {:?}",
            case.name,
            notifications
        );
        assert!(wakes.is_empty(), "case {} owes no restart wake", case.name);
    }
}

async fn load_restart_notification_messages(node: &EmbeddedNode, session_id: &str) -> Vec<String> {
    let session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{session_id}" }} }},
                order: {{ sequence: ASC }}
            ) {{ content }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "load restart notification messages failed: {:?}",
        response.errors
    );
    #[derive(Deserialize)]
    struct MessageRow {
        content: String,
    }
    let rows: Vec<MessageRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    rows.into_iter().map(|row| row.content).collect()
}

async fn load_restart_wake_rows(node: &EmbeddedNode, session_id: &str) -> Vec<Option<String>> {
    let session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    execution_origin: {{ _eq: "scheduled" }}
                }}
            ) {{ metadata }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "load restart wake rows failed: {:?}",
        response.errors
    );
    #[derive(Deserialize)]
    struct WakeRow {
        metadata: Option<String>,
    }
    let rows: Vec<WakeRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    rows.into_iter().map(|row| row.metadata).collect()
}
