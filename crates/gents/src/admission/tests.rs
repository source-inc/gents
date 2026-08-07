use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use defra_node::EmbeddedNode;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::client::scope_call_with_token;
use super::{
    scope_call, scope_call_with_token_and_failure_reason, scope_request,
    set_terminal_failure_reason,
    slot_accounting::{reconstructed_running_slot_count, slot_contribution, InferenceCallSlotRow},
    terminal_failure_reason_observer, AdmissionCallContext, AdmissionRegistry,
    BackendAdmissionConfig, CallKind,
};
use crate::lean_vocab_test::{
    assert_lean_contract_vocabulary_set_matches, assert_lean_transition_is_illegal,
    assert_lean_transition_is_legal, assert_state_machine_contract_is_complete,
    lean_fleet_slot_accounting_case, lean_inference_slot_accounting_cases, lean_vocabulary_values,
    LeanContractVocabulary, LeanFleetSlotAccountingCase,
};
use crate::schema::ensure_schemas;
use crate::watcher::AgentRequest;

async fn test_node() -> Arc<EmbeddedNode> {
    let node = Arc::new(EmbeddedNode::builder().build().await.unwrap());
    ensure_schemas(node.as_ref()).await.unwrap();
    node
}

fn config(
    backend_id: &str,
    max_concurrent: usize,
    max_queue_depth: usize,
) -> BackendAdmissionConfig {
    BackendAdmissionConfig {
        backend_id: backend_id.to_string(),
        max_concurrent,
        max_queue_depth,
        enabled: true,
        probe_status: "healthy".to_string(),
        measured_unhealthy: false,
        config_fingerprint: format!("{backend_id}:{max_concurrent}:{max_queue_depth}"),
    }
}

fn request(request_id: &str) -> AgentRequest {
    AgentRequest {
        doc_id: format!("doc-{request_id}"),
        request_id: request_id.to_string(),
        agent_did: "did:test:test".to_string(),
        requester_did: None,
        behavior_id: Some("default".to_string()),
        session_id: format!("session-{request_id}"),
        content: "hello".to_string(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: "2026-04-15T00:00:00Z".to_string(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    }
}

#[tokio::test]
async fn current_session_id_reflects_request_scope() {
    // Outside any admission scope there is no session id to attach.
    assert_eq!(super::current_session_id(), None);

    let context = AdmissionCallContext::for_request(&request("req-x"), "default", "backend-1");
    scope_request(context, async {
        assert_eq!(
            super::current_session_id().as_deref(),
            Some("session-req-x"),
            "the in-flight request's session id must be visible for x-session-id tagging"
        );
    })
    .await;

    // It is cleared again once the request scope ends.
    assert_eq!(super::current_session_id(), None);
}

/// `next_call` publishes the minted call identity to the shared slot, and
/// `current_call_join()` reads it back on the same task — the seam the
/// rendered-request capture uses for its exact `InferenceCall` join.
#[tokio::test]
async fn current_call_join_reflects_the_minted_call() {
    assert!(super::current_call_join().is_none());

    let context = AdmissionCallContext::for_request(&request("req-j"), "default", "backend-1");
    // Keep a handle sharing the same slot Arc, exactly like scope_call clones do.
    let minting_handle = context.clone();
    scope_request(context, async move {
        // Scoped but nothing admitted yet: no join to report.
        assert!(super::current_call_join().is_none());

        let first = minting_handle.next_call("runtime-test");
        let join = super::current_call_join().expect("join after mint");
        assert_eq!(join.call_id, first.call_id);
        assert_eq!(join.call_seq, 1);

        // A second admitted call replaces the slot; the join always names the
        // call currently in flight.
        let second = minting_handle.next_call("runtime-test");
        let join = super::current_call_join().expect("join after second mint");
        assert_eq!(join.call_id, second.call_id);
        assert_eq!(join.call_seq, 2);
        assert_ne!(first.call_id, second.call_id);
    })
    .await;

    // Cleared with the scope: no ambient join leaks across requests.
    assert!(super::current_call_join().is_none());
}

const ADMISSION_TERMINAL_REASON_SOURCES: &[&str] = &[
    include_str!("controller.rs"),
    include_str!("permit.rs"),
    include_str!("registry.rs"),
];
const ADMISSION_CALL_STATE_SOURCES: &[&str] = &[
    include_str!("controller.rs"),
    include_str!("permit.rs"),
    include_str!("persistence.rs"),
    include_str!("registry.rs"),
];

fn lean_inference_call_states() -> Vec<&'static str> {
    lean_vocabulary_values("InferenceCallState")
}

fn string_literals_after(source: &'static str, needle: &str) -> Vec<&'static str> {
    let mut rest = source;
    let mut values = Vec::new();
    while let Some(start) = rest.find(needle) {
        let value_start = start + needle.len();
        let after_start = &rest[value_start..];
        let value_end = after_start.find('"').expect("string literal must close");
        values.push(&after_start[..value_end]);
        rest = &after_start[value_end + 1..];
    }
    values
}

fn rust_literal_terminal_reasons_from_admission_sources() -> Vec<&'static str> {
    let mut values = ADMISSION_TERMINAL_REASON_SOURCES
        .iter()
        .flat_map(|source| string_literals_after(source, "Some(\""))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    values
}

fn rust_literal_call_states_from_admission_sources() -> Vec<&'static str> {
    let patterns = [
        "call_state: \"",
        "add_call_mutation(call, \"",
        "persist_terminal_call(node, call, \"",
        "persist_existing_call_terminal(node, &call, \"",
    ];
    let mut values = ADMISSION_CALL_STATE_SOURCES
        .iter()
        .flat_map(|source| {
            patterns
                .iter()
                .flat_map(|pattern| string_literals_after(source, pattern))
        })
        .filter(|value| !value.contains('{'))
        .collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    values
}

fn assert_inference_call_rows_use_lean_vocabulary(rows: &[Value]) {
    let lean_states = lean_inference_call_states();
    for row in rows {
        let state = row
            .get("call_state")
            .and_then(Value::as_str)
            .expect("InferenceCall row must include call_state");
        assert!(
            lean_states.contains(&state),
            "InferenceCall.call_state={state:?} is not in the Lean InferenceCallState vocabulary"
        );
    }
}

async fn call_rows(node: &EmbeddedNode) -> Vec<Value> {
    let response = node
        .execute(
            r#"{
                InferenceCall(order: { call_seq: ASC }) {
                    request_id
                    call_seq
                    backend_id
                    behavior_id
                    call_kind
                    call_state
                    failure_reason
                    queue_depth_at_enqueue
                }
            }"#,
        )
        .await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceCall"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_inference_call_rows_use_lean_vocabulary(&rows);
    rows
}

fn running_slot_count_for_backend(rows: &[Value], backend_id: &str) -> usize {
    reconstructed_running_slot_count(rows.iter().map(slot_row_from_value), backend_id)
}

fn slot_row_from_value(row: &Value) -> InferenceCallSlotRow<'_> {
    InferenceCallSlotRow::new(
        row.get("backend_id")
            .and_then(Value::as_str)
            .expect("InferenceCall row must include backend_id"),
        row.get("call_state")
            .and_then(Value::as_str)
            .expect("InferenceCall row must include call_state"),
    )
}

fn state_count_for_backend(rows: &[Value], backend_id: &str, call_state: &str) -> usize {
    rows.iter()
        .filter(|row| {
            row.get("backend_id").and_then(Value::as_str) == Some(backend_id)
                && row.get("call_state").and_then(Value::as_str) == Some(call_state)
        })
        .count()
}

fn assert_reconstructed_slot_count(rows: &[Value], backend_id: &str, expected: usize) {
    assert_eq!(
        running_slot_count_for_backend(rows, backend_id),
        expected,
        "held backend slots are reconstructed from persisted InferenceCall rows with call_state=running"
    );
}

fn assert_reconstructed_slot_count_at_most(
    rows: &[Value],
    backend_id: &str,
    max_concurrent: usize,
) {
    let reconstructed = running_slot_count_for_backend(rows, backend_id);
    assert!(
        reconstructed <= max_concurrent,
        "reconstructed running-row slot count {reconstructed} exceeded max_concurrent {max_concurrent}"
    );
}

fn assert_fleet_case_matches_call_row(case: &LeanFleetSlotAccountingCase, row: &Value) {
    assert!(
        case.row_backend_ids == vec![case.backend_id.clone()],
        "single-row Fleet slot helper called with non-single-backend case {}: {:?}",
        case.name,
        case.row_backend_ids
    );
    assert!(
        case.row_states.len() == 1,
        "single-row Fleet slot helper called with multi-row case {}: {:?}",
        case.name,
        case.row_states
    );
    if case.admission_state == "released" {
        let expected_terminal_state = match case.request_state.as_str() {
            "completed" => "completed",
            "failed" => "failed",
            "interrupted" | "superseded" | "dead" => "cancelled",
            other => panic!(
                "Fleet slot released case {} has non-terminal request_state={other}",
                case.name
            ),
        };
        assert_eq!(
            case.row_states[0].as_str(),
            expected_terminal_state,
            "Fleet slot released case {} projected the wrong terminal InferenceCall state",
            case.name
        );
    }
    assert_eq!(
        row.get("backend_id").and_then(Value::as_str),
        Some(case.row_backend_ids[0].as_str()),
        "Fleet slot case {} projected backend drifted from persisted InferenceCall row",
        case.name
    );
    assert_eq!(
        row.get("call_state").and_then(Value::as_str),
        Some(case.row_states[0].as_str()),
        "Fleet slot case {} projected call_state drifted from persisted InferenceCall row",
        case.name
    );

    let contribution = slot_contribution(slot_row_from_value(row), &case.backend_id);
    assert_eq!(
        contribution, case.expected_contribution,
        "Fleet slot case {} drifted from admission slot contribution",
        case.name
    );
    assert_eq!(
        contribution, case.reconstructed_running_count,
        "Fleet slot case {} one-row projection drifted from admission reconstruction",
        case.name
    );
}

fn sorted_call_states_for_backend(rows: &[Value], backend_id: &str) -> Vec<String> {
    let mut states = rows
        .iter()
        .filter(|row| row.get("backend_id").and_then(Value::as_str) == Some(backend_id))
        .map(|row| {
            row.get("call_state")
                .and_then(Value::as_str)
                .expect("InferenceCall row must include call_state")
                .to_string()
        })
        .collect::<Vec<_>>();
    states.sort();
    states
}

async fn wait_for_call_row_count(node: &EmbeddedNode, expected: usize) -> Vec<Value> {
    let mut last = Vec::new();
    for _ in 0..100 {
        let rows = call_rows(node).await;
        if rows.len() >= expected {
            return rows;
        }
        last = rows;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for at least {expected} InferenceCall rows, last rows={last:?}");
}

async fn wait_for_request_call_state(
    node: &EmbeddedNode,
    request_id: &str,
    expected_state: &str,
) -> Value {
    let mut last = Vec::new();
    for _ in 0..100 {
        let rows = call_rows(node).await;
        if let Some(row) = rows.iter().find(|row| {
            row.get("request_id").and_then(Value::as_str) == Some(request_id)
                && row.get("call_state").and_then(Value::as_str) == Some(expected_state)
        }) {
            return row.clone();
        }
        last = rows;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for request_id={request_id} InferenceCall state={expected_state}, last rows={last:?}"
    );
}

#[test]
fn rust_inference_call_state_vocabulary_matches_lean_model() {
    let rust_states = rust_literal_call_states_from_admission_sources();
    assert_lean_contract_vocabulary_set_matches(LeanContractVocabulary {
        domain: "InferenceCallState",
        rust_source: "admission source call_state literals",
        rust_values: &rust_states,
    });
}

#[test]
fn rust_inference_call_terminal_reason_vocabulary_matches_lean_model() {
    let rust_reasons = rust_literal_terminal_reasons_from_admission_sources();
    assert_lean_contract_vocabulary_set_matches(LeanContractVocabulary {
        domain: "InferenceCallTerminalReason",
        rust_source: "admission system terminal reason literals",
        rust_values: &rust_reasons,
    });
}

#[test]
fn rust_inference_call_transition_table_matches_lean_contract() {
    assert_state_machine_contract_is_complete("InferenceCall");
    assert_lean_transition_is_legal("InferenceCall", "queued", "running");
    assert_lean_transition_is_legal("InferenceCall", "queued", "cancelled");
    assert_lean_transition_is_legal("InferenceCall", "running", "completed");
    assert_lean_transition_is_legal("InferenceCall", "running", "failed");
    assert_lean_transition_is_legal("InferenceCall", "running", "cancelled");
    assert_lean_transition_is_illegal("InferenceCall", "queued", "completed");
    assert_lean_transition_is_illegal("InferenceCall", "completed", "running");
}

#[test]
fn generated_inference_slot_accounting_cases_match_admission_reconstruction_logic() {
    let cases = lean_inference_slot_accounting_cases();
    assert_eq!(
        cases.len(),
        11,
        "Lean should emit the finite InferenceCall slot-accounting cases"
    );

    for case in cases {
        assert_eq!(
            case.row_backend_ids.len(),
            case.row_states.len(),
            "Lean case {} emitted mismatched row arrays",
            case.name
        );

        if case.row_states.len() == 1 {
            let row = InferenceCallSlotRow::new(
                case.row_backend_ids[0].as_str(),
                case.row_states[0].as_str(),
            );
            assert_eq!(
                slot_contribution(row, &case.backend_id),
                case.expected_contribution,
                "generated case {} drifted from admission slot contribution",
                case.name
            );
            assert_eq!(
                case.contribution, case.expected_contribution,
                "Lean case {} should compute its expected contribution",
                case.name
            );
        }

        let reconstructed = reconstructed_running_slot_count(
            case.row_backend_ids
                .iter()
                .zip(&case.row_states)
                .map(|(backend_id, state)| {
                    InferenceCallSlotRow::new(backend_id.as_str(), state.as_str())
                }),
            &case.backend_id,
        );
        assert_eq!(
            reconstructed, case.reconstructed_running_count,
            "generated case {} drifted from admission reconstructed running count",
            case.name
        );
        assert_eq!(
            case.bounded_by_max_concurrent,
            reconstructed <= case.max_concurrent,
            "generated case {} drifted from max_concurrent bound",
            case.name
        );

        if matches!(
            case.property.as_str(),
            "terminal_release" | "permit_drop_terminalization"
        ) {
            assert_eq!(case.pre_state.as_str(), "running", "{}", case.name);
            assert_eq!(case.pre_contribution, 1, "{}", case.name);
            assert_eq!(case.post_contribution, 0, "{}", case.name);
            assert!(case.released_slot, "{}", case.name);
        }
        if case.property == "permit_drop_terminalization" {
            assert!(case.permit_drop_terminalization, "{}", case.name);
        }
    }
}

#[tokio::test]
async fn generated_slot_accounting_fleet_cases_match_admission_runtime_boundary() {
    let waiting = lean_fleet_slot_accounting_case("fleet_waiting_contributes_zero");
    let acquired = lean_fleet_slot_accounting_case("fleet_acquired_contributes_one");
    let executing = lean_fleet_slot_accounting_case("fleet_executing_contributes_one");
    let released = lean_fleet_slot_accounting_case("fleet_released_terminal_contributes_zero");
    let bounded = lean_fleet_slot_accounting_case(
        "fleet_reconstructed_running_count_bounded_by_max_concurrent",
    );
    let backend_id = acquired.backend_id.clone();
    assert_eq!(waiting.backend_id, backend_id);
    assert_eq!(executing.backend_id, backend_id);
    assert_eq!(released.backend_id, backend_id);
    assert_eq!(bounded.backend_id, backend_id);

    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([(backend_id.clone(), config(&backend_id, 1, 1))]),
    );

    let running_context =
        AdmissionCallContext::for_request(&request("req-fleet-running"), "default", &backend_id);
    let queued_context =
        AdmissionCallContext::for_request(&request("req-fleet-waiting"), "default", &backend_id);

    scope_request(running_context, async {
        let mut running_permit = registry.acquire_current_call().await.unwrap();
        let running_row = wait_for_request_call_state(
            node.as_ref(),
            "req-fleet-running",
            &acquired.row_states[0],
        )
        .await;
        assert_fleet_case_matches_call_row(acquired, &running_row);
        assert_fleet_case_matches_call_row(executing, &running_row);

        let queued_registry = registry.clone();
        let queued = tokio::spawn(async move {
            scope_request(queued_context, async move {
                let mut queued_permit = queued_registry.acquire_current_call().await.unwrap();
                queued_permit.finish_success(None).await;
            })
            .await;
        });
        let queued_row =
            wait_for_request_call_state(node.as_ref(), "req-fleet-waiting", &waiting.row_states[0])
                .await;
        assert_fleet_case_matches_call_row(waiting, &queued_row);

        running_permit.finish_success(None).await;
        drop(running_permit);
        queued.await.unwrap();
    })
    .await;

    let completed_row =
        wait_for_request_call_state(node.as_ref(), "req-fleet-running", &released.row_states[0])
            .await;
    assert_fleet_case_matches_call_row(released, &completed_row);

    let bounded_node = test_node().await;
    let bounded_registry = AdmissionRegistry::new(bounded_node.clone());
    bounded_registry.reconcile(
        1,
        &HashMap::from([(
            backend_id.clone(),
            config(&backend_id, bounded.max_concurrent, 1),
        )]),
    );

    let completed_context = AdmissionCallContext::for_request(
        &request("req-fleet-bound-completed"),
        "default",
        &backend_id,
    );
    scope_request(completed_context, async {
        let mut permit = bounded_registry.acquire_current_call().await.unwrap();
        permit.finish_success(None).await;
        drop(permit);
    })
    .await;

    let first_context = AdmissionCallContext::for_request(
        &request("req-fleet-bound-running-1"),
        "default",
        &backend_id,
    );
    let second_context = AdmissionCallContext::for_request(
        &request("req-fleet-bound-running-2"),
        "default",
        &backend_id,
    );
    let queued_context = AdmissionCallContext::for_request(
        &request("req-fleet-bound-queued"),
        "default",
        &backend_id,
    );

    let mut first = scope_request(first_context, async {
        bounded_registry.acquire_current_call().await.unwrap()
    })
    .await;
    let mut second = scope_request(second_context, async {
        bounded_registry.acquire_current_call().await.unwrap()
    })
    .await;
    let queued_registry = bounded_registry.clone();
    let queued = tokio::spawn(async move {
        scope_request(queued_context, async move {
            let mut permit = queued_registry.acquire_current_call().await.unwrap();
            permit.finish_success(None).await;
        })
        .await;
    });

    wait_for_request_call_state(bounded_node.as_ref(), "req-fleet-bound-queued", "queued").await;
    let rows = wait_for_call_row_count(bounded_node.as_ref(), bounded.active_count).await;
    let reconstructed = running_slot_count_for_backend(&rows, &backend_id);
    assert_eq!(
        reconstructed, bounded.reconstructed_running_count,
        "generated fleet bounded case drifted from runtime admission reconstruction"
    );
    assert_eq!(reconstructed, bounded.slot_count);
    assert!(reconstructed <= bounded.max_concurrent);

    let mut expected_states = bounded.row_states.clone();
    expected_states.sort();
    assert_eq!(
        sorted_call_states_for_backend(&rows, &backend_id),
        expected_states,
        "generated fleet bounded projection must match the real admission row states"
    );

    first.finish_success(None).await;
    drop(first);
    queued.await.unwrap();
    second.finish_success(None).await;
    drop(second);
}

#[tokio::test]
async fn missing_backend_persists_backend_gone_cancelled_terminal() {
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    let context =
        AdmissionCallContext::for_request(&request("req-backend-gone"), "default", "missing");

    scope_request(context, async {
        let error = match registry.acquire_current_call().await {
            Ok(_) => panic!("missing backend should reject without a permit"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("BackendGone"));
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["backend_id"], "missing");
    assert_eq!(rows[0]["call_state"], "cancelled");
    assert_eq!(rows[0]["failure_reason"], "BackendGone");
    assert_reconstructed_slot_count(&rows, "missing", 0);
}

#[tokio::test]
async fn max_queue_depth_zero_allows_immediate_permit_and_rejects_saturated_backend() {
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 0))]),
    );
    let context = AdmissionCallContext::for_request(&request("req-zero"), "default", "backend-a");

    scope_request(context, async {
        let mut first = registry.acquire_current_call().await.unwrap();
        let error = match registry.acquire_current_call().await {
            Ok(_) => panic!("saturated backend should reject without queue capacity"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("QueueFull"));
        let rows = wait_for_call_row_count(node.as_ref(), 2).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["call_state"], "running");
        assert_eq!(rows[1]["call_state"], "failed");
        assert_eq!(rows[1]["failure_reason"], "QueueFull");
        assert_reconstructed_slot_count(&rows, "backend-a", 1);
        first.finish_success(None).await;
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["call_state"], "completed");
    assert_eq!(rows[1]["call_state"], "failed");
    assert_eq!(rows[1]["failure_reason"], "QueueFull");
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
}

#[tokio::test]
async fn reconstructed_running_rows_never_exceed_max_concurrent_under_contention() {
    const TASKS: usize = 5;
    const MAX_CONCURRENT: usize = 2;

    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([(
            "backend-a".to_string(),
            config("backend-a", MAX_CONCURRENT, TASKS),
        )]),
    );

    let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel::<usize>();
    let mut release_senders = HashMap::new();
    let mut handles = Vec::new();

    for idx in 0..TASKS {
        let context = AdmissionCallContext::for_request(
            &request(&format!("req-contention-{idx}")),
            "default",
            "backend-a",
        );
        let task_registry = registry.clone();
        let task_acquired_tx = acquired_tx.clone();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        release_senders.insert(idx, release_tx);

        handles.push(tokio::spawn(async move {
            scope_request(context, async move {
                let mut permit = task_registry.acquire_current_call().await.unwrap();
                task_acquired_tx
                    .send(idx)
                    .expect("test acquired receiver must stay open");
                let _ = release_rx.await;
                permit.finish_success(None).await;
            })
            .await;
        }));
    }
    drop(acquired_tx);

    let mut acquired = Vec::new();
    while acquired.len() < MAX_CONCURRENT {
        acquired.push(
            acquired_rx
                .recv()
                .await
                .expect("expected initial permits to acquire"),
        );
    }

    let rows = wait_for_call_row_count(node.as_ref(), TASKS).await;
    assert_reconstructed_slot_count(&rows, "backend-a", MAX_CONCURRENT);
    assert_reconstructed_slot_count_at_most(&rows, "backend-a", MAX_CONCURRENT);
    assert_eq!(state_count_for_backend(&rows, "backend-a", "queued"), 3);

    let released = acquired[0];
    release_senders
        .remove(&released)
        .expect("release sender for acquired permit")
        .send(())
        .expect("held task should still be waiting for release");
    acquired.push(
        acquired_rx
            .recv()
            .await
            .expect("queued task should acquire after one permit release"),
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_reconstructed_slot_count(&rows, "backend-a", MAX_CONCURRENT);
    assert_reconstructed_slot_count_at_most(&rows, "backend-a", MAX_CONCURRENT);
    assert_eq!(state_count_for_backend(&rows, "backend-a", "completed"), 1);

    for (_, release_tx) in release_senders {
        let _ = release_tx.send(());
    }
    for handle in handles {
        handle.await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
    assert_eq!(
        state_count_for_backend(&rows, "backend-a", "completed"),
        TASKS
    );
}

#[tokio::test]
async fn queued_calls_start_in_tokio_registration_order_after_permit_release() {
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 2))]),
    );
    let first_context =
        AdmissionCallContext::for_request(&request("req-ordered"), "default", "backend-a");
    let second_context = first_context.clone();

    scope_request(first_context, async {
        let mut first = registry.acquire_current_call().await.unwrap();
        let second_registry = registry.clone();
        let second = tokio::spawn(async move {
            scope_request(second_context, async move {
                let mut permit = second_registry.acquire_current_call().await.unwrap();
                permit.finish_success(None).await;
            })
            .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let rows = call_rows(node.as_ref()).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["call_state"], "running");
        assert_eq!(rows[1]["call_state"], "queued");
        assert_eq!(
            running_slot_count_for_backend(&rows, "backend-a"),
            1,
            "the aggregate slot count is reconstructed from running InferenceCall rows; queued rows do not hold slots"
        );

        first.finish_success(None).await;
        drop(first);
        second.await.unwrap();
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["call_state"], "completed");
    assert_eq!(rows[1]["call_state"], "completed");
    assert_eq!(rows[1]["queue_depth_at_enqueue"], 1);
    assert_eq!(
        running_slot_count_for_backend(&rows, "backend-a"),
        0,
        "terminal InferenceCall rows reconstruct zero held scheduler slots"
    );
}

#[tokio::test]
async fn cancelling_queued_call_terminalizes_without_holding_slot() {
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 1))]),
    );
    let running_context =
        AdmissionCallContext::for_request(&request("req-running-holder"), "default", "backend-a");
    let queued_context =
        AdmissionCallContext::for_request(&request("req-queued-cancel"), "default", "backend-a");

    scope_request(running_context, async {
        let mut first = registry.acquire_current_call().await.unwrap();
        let queued_registry = registry.clone();
        let queued = tokio::spawn(async move {
            scope_request(queued_context, async move {
                let _permit = queued_registry.acquire_current_call().await.unwrap();
            })
            .await;
        });

        wait_for_request_call_state(node.as_ref(), "req-queued-cancel", "queued").await;
        let rows = call_rows(node.as_ref()).await;
        assert_reconstructed_slot_count(&rows, "backend-a", 1);
        assert_eq!(state_count_for_backend(&rows, "backend-a", "queued"), 1);

        queued.abort();
        let _ = queued.await;
        wait_for_request_call_state(node.as_ref(), "req-queued-cancel", "cancelled").await;
        let rows = call_rows(node.as_ref()).await;
        assert_reconstructed_slot_count(&rows, "backend-a", 1);
        assert_eq!(state_count_for_backend(&rows, "backend-a", "cancelled"), 1);

        first.finish_success(None).await;
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
    assert_eq!(state_count_for_backend(&rows, "backend-a", "completed"), 1);
    assert_eq!(state_count_for_backend(&rows, "backend-a", "cancelled"), 1);
}

#[tokio::test]
async fn explicit_failure_releases_reconstructed_slot() {
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 1))]),
    );
    let context =
        AdmissionCallContext::for_request(&request("req-explicit-failure"), "default", "backend-a");

    scope_request(context, async {
        let mut permit = registry.acquire_current_call().await.unwrap();
        let rows = call_rows(node.as_ref()).await;
        assert_reconstructed_slot_count(&rows, "backend-a", 1);
        permit.finish_failure("provider failed").await;
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["call_state"], "failed");
    assert_eq!(rows[0]["failure_reason"], "provider failed");
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
}

#[tokio::test]
async fn repeated_inference_scope_calls_persist_per_attempt_rows() {
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 1))]),
    );
    let context =
        AdmissionCallContext::for_request(&request("req-retry-rows"), "default", "backend-a");

    scope_request(context, async {
        scope_call(CallKind::Inference, 1, async {
            let mut permit = registry.acquire_current_call().await.unwrap();
            permit.finish_failure("transient one").await;
        })
        .await;
        scope_call(CallKind::Inference, 1, async {
            let mut permit = registry.acquire_current_call().await.unwrap();
            permit.finish_failure("transient two").await;
        })
        .await;
        scope_call(CallKind::Inference, 1, async {
            let mut permit = registry.acquire_current_call().await.unwrap();
            permit.finish_success(None).await;
        })
        .await;
    })
    .await;

    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter()
            .map(|row| row["call_seq"].as_i64().expect("call_seq"))
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        rows.iter()
            .map(|row| row["call_state"].as_str().expect("call_state"))
            .collect::<Vec<_>>(),
        vec!["failed", "failed", "completed"]
    );
    assert_eq!(
        rows.iter()
            .map(|row| row["call_kind"].as_str().expect("call_kind"))
            .collect::<Vec<_>>(),
        vec!["inference", "inference", "inference"]
    );
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
}

#[tokio::test]
async fn scoped_scheduled_calls_are_persisted_with_scheduled_kind() {
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 1))]),
    );
    let context =
        AdmissionCallContext::for_request(&request("req-scheduled"), "default", "backend-a");

    scope_request(context, async {
        scope_call(CallKind::Scheduled, 1, async {
            let mut permit = registry.acquire_current_call().await.unwrap();
            permit.finish_success(None).await;
        })
        .await;
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["call_kind"], "scheduled");
    assert_eq!(rows[0]["call_state"], "completed");
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
}

#[tokio::test]
async fn compaction_calls_share_backend_capacity_with_inference_calls() {
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 1))]),
    );
    let inference_context =
        AdmissionCallContext::for_request(&request("req-compaction"), "default", "backend-a");
    let compaction_context = inference_context.clone();

    scope_request(inference_context, async {
        let mut inference = registry.acquire_current_call().await.unwrap();
        let compaction_registry = registry.clone();
        let compaction = tokio::spawn(async move {
            scope_request(compaction_context, async move {
                scope_call(CallKind::Compaction, 1, async {
                    let mut permit = compaction_registry.acquire_current_call().await.unwrap();
                    permit.finish_success(None).await;
                })
                .await;
            })
            .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let rows = call_rows(node.as_ref()).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["call_kind"], "inference");
        assert_eq!(rows[0]["call_state"], "running");
        assert_eq!(rows[1]["call_kind"], "compaction");
        assert_eq!(rows[1]["call_state"], "queued");
        assert_reconstructed_slot_count(&rows, "backend-a", 1);

        inference.finish_success(None).await;
        drop(inference);
        compaction.await.unwrap();
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["call_state"], "completed");
    assert_eq!(rows[1]["call_state"], "completed");
    assert_eq!(rows[1]["queue_depth_at_enqueue"], 1);
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
}

#[tokio::test]
async fn scoped_oneoff_calls_are_persisted_with_oneoff_kind() {
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 1))]),
    );
    let context = AdmissionCallContext::for_request(&request("req-oneoff"), "default", "backend-a");

    scope_request(context, async {
        scope_call(CallKind::OneOff, 1, async {
            let mut permit = registry.acquire_current_call().await.unwrap();
            permit.finish_success(None).await;
        })
        .await;
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["call_kind"], "oneoff");
    assert_eq!(rows[0]["call_state"], "completed");
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
}

#[tokio::test]
async fn dropped_permit_with_cancelled_token_persists_cancelled_terminal() {
    // Validates the ComposedState::interrupted_request_cancels_live_linked_call
    // runtime bridge for the mid-stream path: if the inference_token is cancelled
    // at permit Drop time (e.g. daemon dropped the stream future because
    // the request was interrupted), the persisted InferenceCall row lands
    // as cancelled/Cancelled rather than the default
    // failed/StreamDroppedBeforeTerminalResponse fallback.
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 1))]),
    );
    let context =
        AdmissionCallContext::for_request(&request("req-cancel-drop"), "default", "backend-a");

    let token = CancellationToken::new();
    token.cancel();

    scope_request(context, async {
        scope_call_with_token(CallKind::Inference, 1, token, async {
            let permit = registry.acquire_current_call().await.unwrap();
            let rows = call_rows(node.as_ref()).await;
            assert_reconstructed_slot_count(&rows, "backend-a", 1);
            // Drop without calling finish_success/finish_failure — simulates
            // the daemon dropping the stream future mid-stream after the
            // request-level cancellation token fires.
            drop(permit);
        })
        .await;
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["call_state"], "cancelled");
    assert_eq!(rows[0]["failure_reason"], "Cancelled");
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
}

#[tokio::test]
async fn dropped_permit_with_terminal_failure_reason_persists_failed_reason() {
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 1))]),
    );
    let context =
        AdmissionCallContext::for_request(&request("req-timeout-drop"), "default", "backend-a");

    let token = CancellationToken::new();
    let observer = terminal_failure_reason_observer();
    let observer_for_scope = observer.clone();
    let observer_for_drop = observer.clone();

    scope_request(context, async {
        scope_call_with_token_and_failure_reason(
            CallKind::Inference,
            1,
            token,
            observer_for_scope,
            async {
                let permit = registry.acquire_current_call().await.unwrap();
                let rows = call_rows(node.as_ref()).await;
                assert_reconstructed_slot_count(&rows, "backend-a", 1);
                set_terminal_failure_reason(
                    &observer_for_drop,
                    "stream liveness timeout: no data received for 1800s",
                );
                drop(permit);
            },
        )
        .await;
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["call_state"], "failed");
    assert_eq!(
        rows[0]["failure_reason"],
        "stream liveness timeout: no data received for 1800s"
    );
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
}

#[tokio::test]
async fn dropped_permit_without_cancelled_token_persists_failed_terminal() {
    // Protects the existing default-terminal behavior for non-interrupt
    // scenarios: when the inference_token is absent (or present but not
    // cancelled), a permit dropped without an explicit terminal still
    // lands as failed/StreamDroppedBeforeTerminalResponse — i.e. a real
    // provider-side stream drop, not a user interrupt.
    let node = test_node().await;
    let registry = AdmissionRegistry::new(node.clone());
    registry.reconcile(
        1,
        &HashMap::from([("backend-a".to_string(), config("backend-a", 1, 1))]),
    );
    let context =
        AdmissionCallContext::for_request(&request("req-default-drop"), "default", "backend-a");

    scope_request(context, async {
        let permit = registry.acquire_current_call().await.unwrap();
        let rows = call_rows(node.as_ref()).await;
        assert_reconstructed_slot_count(&rows, "backend-a", 1);
        drop(permit);
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let rows = call_rows(node.as_ref()).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["call_state"], "failed");
    assert_eq!(
        rows[0]["failure_reason"],
        "StreamDroppedBeforeTerminalResponse"
    );
    assert_reconstructed_slot_count(&rows, "backend-a", 0);
}

fn pending_call(request_id: &str, backend_id: &str) -> super::controller::PendingCallMetadata {
    super::controller::PendingCallMetadata {
        call_id: format!("call-{request_id}"),
        runtime_instance_id: "runtime-test".to_string(),
        request_id: request_id.to_string(),
        call_seq: 1,
        backend_id: backend_id.to_string(),
        behavior_id: "default".to_string(),
        agent_did: "did:test:test".to_string(),
        call_kind: CallKind::Inference,
        attempt: 1,
    }
}

fn call_state_for_request(rows: &[Value], request_id: &str) -> Option<String> {
    rows.iter()
        .find(|row| row.get("request_id").and_then(Value::as_str) == Some(request_id))
        .and_then(|row| row.get("call_state").and_then(Value::as_str))
        .map(ToOwned::to_owned)
}

/// Issue #1001 defect 1. A failed durable queued write must release the
/// queue-waiter unit; pre-fix, the waiter counter leaked one unit per persist
/// failure and `max_queue_depth` failures wedged the backend at `QueueFull`
/// while idle. Lean:
/// `InferenceCall.ControllerBookkeeping.persist_error_releases_waiter`.
#[tokio::test]
async fn queued_persist_failure_releases_queue_capacity() {
    let node = test_node().await;
    // A node without ensured schemas rejects InferenceCall writes, forcing
    // the durable queued write inside the queue path to fail while the
    // in-memory counters run their real paths.
    let schemaless = Arc::new(EmbeddedNode::builder().build().await.unwrap());
    let controller = super::controller::BackendAdmissionController::new(
        1,
        config("backend-queue-leak", 1, 1),
        std::sync::Weak::new(),
    );

    let held = controller
        .clone()
        .acquire(
            node.clone(),
            pending_call("req-leak-a", "backend-queue-leak"),
            None,
            None,
        )
        .await
        .expect("first admission fills the only slot");

    let error = match controller
        .clone()
        .acquire(
            schemaless,
            pending_call("req-leak-b", "backend-queue-leak"),
            None,
            None,
        )
        .await
    {
        Ok(_) => panic!("queued persist must fail without schemas"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("persisting InferenceCall failed"),
        "unexpected error: {error}"
    );
    assert_eq!(
        controller.queue_waiters_for_test(),
        0,
        "persist failure leaked a queue-waiter unit (#1001)"
    );

    // Queue capacity must be intact: the next caller queues instead of being
    // wedged into QueueFull, and completes once the held permit releases.
    let queued = tokio::spawn(controller.clone().acquire(
        node.clone(),
        pending_call("req-leak-c", "backend-queue-leak"),
        None,
        None,
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(held);
    let permit = tokio::time::timeout(Duration::from_secs(30), queued)
        .await
        .expect("queued acquire must not hang")
        .expect("queued acquire task join")
        .expect("queue capacity must be available after the failed persist");
    drop(permit);
}

/// Issue #1001 defect 3. A permit assigned to a parked waiter whose task has
/// not resumed must stay visible to drain detection; pre-fix, the in-flight
/// count was incremented only after the acquire resumed, so
/// `AdmissionRegistry::reconcile` could observe a closed controller as
/// drained and install a fresh full-capacity controller while the old permit
/// was live, briefly exceeding `max_concurrent`. Lean:
/// `InferenceCall.ControllerBookkeeping.drained_no_outstanding_permits`.
#[tokio::test]
async fn assigned_permit_is_visible_to_drain_detection() {
    use std::future::Future;

    let node = test_node().await;
    let controller = super::controller::BackendAdmissionController::new(
        1,
        config("backend-drain-race", 1, 4),
        std::sync::Weak::new(),
    );

    let held = controller
        .clone()
        .acquire(
            node.clone(),
            pending_call("req-drain-a", "backend-drain-race"),
            None,
            None,
        )
        .await
        .expect("first admission fills the only slot");

    // Park a second acquire in the queue, driving it manually with a no-op
    // waker so the runtime never resumes it: the semaphore can then hand it
    // the released permit while its task is unpolled — the acquire→count
    // window from #1001.
    let mut queued = Box::pin(controller.clone().acquire(
        node.clone(),
        pending_call("req-drain-b", "backend-drain-race"),
        None,
        None,
    ));
    let waker = futures::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    let parked_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            queued.as_mut().poll(&mut cx).is_pending(),
            "queued acquire cannot complete while the permit is held"
        );
        let rows = call_rows(node.as_ref()).await;
        if call_state_for_request(&rows, "req-drain-b").as_deref() == Some("queued") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < parked_deadline,
            "queued InferenceCall row never became durable"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The durable write is the last await before the semaphore park; a few
    // more polls carry the future onto the semaphore waiter list so the
    // released permit below is assigned rather than returned to the pool.
    // The assertions do not depend on this heuristic landing: in-flight is
    // counted from acquisition intent, so `is_drained()` stays false either
    // way — and pre-#1001 it reported drained either way.
    for _ in 0..20 {
        assert!(queued.as_mut().poll(&mut cx).is_pending());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Releasing the held permit assigns it to the parked waiter without the
    // waiter's task running.
    drop(held);

    assert!(
        !controller.is_drained(),
        "controller with an assigned-but-unresumed permit reported drained (#1001)"
    );

    let permit = tokio::time::timeout(Duration::from_secs(30), queued)
        .await
        .expect("queued acquire resumes once the permit is assigned")
        .expect("the parked waiter holds a real permit");
    assert!(!controller.is_drained());
    drop(permit);
    assert!(
        controller.is_drained(),
        "released admission must drain the controller"
    );
}

/// Issue #1001 review follow-up: the drained signal must imply the semaphore
/// permit is already returned. `release_in_flight` can synchronously install
/// a replacement controller (`controller_drained` → `install_pending_if_ready`),
/// so if the permit outlived the release, a fresh full-capacity controller
/// could coexist with an outstanding old permit — and the window is unbounded
/// because `AdmissionPermit::drop` locks the terminal-failure observer after
/// the release. Holding that lock from the test stalls the drop mid-body
/// deterministically. Lean:
/// `InferenceCall.ControllerBookkeeping.drained_no_outstanding_permits`.
#[tokio::test]
async fn drained_signal_implies_permit_returned() {
    let node = test_node().await;
    let controller = super::controller::BackendAdmissionController::new(
        1,
        config("backend-drain-order", 1, 4),
        std::sync::Weak::new(),
    );
    let observer: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));

    let permit = controller
        .clone()
        .acquire(
            node.clone(),
            pending_call("req-drain-order-a", "backend-drain-order"),
            None,
            Some(observer.clone()),
        )
        .await
        .expect("admission fills the only slot");
    controller.close();

    // Stall the drop after its in-flight release point: the drop body locks
    // the observer before it finishes, and the permit field cannot be
    // destroyed until the body returns.
    let stall = observer.lock().unwrap();
    let dropper = std::thread::spawn(move || drop(permit));
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !controller.is_drained() {
        assert!(
            std::time::Instant::now() < deadline,
            "permit drop never released the in-flight unit"
        );
        std::thread::yield_now();
    }

    assert_eq!(
        controller.available_permits_for_test(),
        1,
        "drained controller must hold no outstanding semaphore permits (#1001)"
    );

    drop(stall);
    dropper.join().expect("permit drop thread");
    assert_eq!(controller.available_permits_for_test(), 1);
    assert!(controller.is_drained());
}
