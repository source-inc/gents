//! InferenceCall conformance home: drives the generated slot-accounting cases
//! against REAL persisted `InferenceCall` rows. The scheduler's fleet slot
//! state is a derived view over these rows (Boundaries.lean:
//! `boundary.inference-slots.running-row-derived`), so the integration witness
//! seeds each case's rows in DefraDB, reads them back, and reconstructs the
//! running slot count exactly as admission does — pinning the S7 capacity
//! bound (`reconstructed ≤ max_concurrent`) over the persisted projection.
//! The pure transition/vocabulary checks remain in `admission::tests`.

use super::*;
use gents::defra_node::ExecuteRetryPolicy;
use tokio::sync::Barrier;

#[derive(Debug, Deserialize)]
struct PersistedSlotRow {
    call_id: String,
    backend_id: String,
    call_state: String,
}

#[derive(Debug, Deserialize)]
struct PersistedExactTargetRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    call_id: String,
    request_id: String,
    call_state: String,
    runtime_instance_id: String,
    controller_generation: i64,
}

pub(super) async fn generated_inference_slot_accounting_cases_drive_db_backed_reconstruction() {
    let cases = lean_inference_slot_accounting_cases();
    assert_eq!(
        cases.len(),
        11,
        "Lean should emit the finite InferenceCall slot-accounting cases"
    );

    let db = test_db("inference-slot-accounting").await;

    for case in cases {
        assert_eq!(
            case.row_backend_ids.len(),
            case.row_states.len(),
            "Lean case {} emitted mismatched row arrays",
            case.name
        );

        for (index, (backend_id, state)) in case
            .row_backend_ids
            .iter()
            .zip(&case.row_states)
            .enumerate()
        {
            insert_inference_call_row(&db.node, &case.name, index, backend_id, state).await;
        }

        let rows = fetch_persisted_slot_rows_for_case(&db.node, &case.name).await;
        assert_eq!(
            rows.len(),
            case.row_states.len(),
            "case {} must read back every seeded InferenceCall row",
            case.name
        );

        if let [row] = rows.as_slice() {
            assert_eq!(
                slot_contribution(
                    InferenceCallSlotRow::new(&row.backend_id, &row.call_state),
                    &case.backend_id,
                ),
                case.expected_contribution,
                "case {} drifted from admission slot contribution over the persisted row",
                case.name
            );
        }

        let reconstructed = reconstructed_running_slot_count(
            rows.iter()
                .map(|row| InferenceCallSlotRow::new(&row.backend_id, &row.call_state)),
            &case.backend_id,
        );
        assert_eq!(
            reconstructed, case.reconstructed_running_count,
            "case {} drifted from admission reconstruction over persisted rows",
            case.name
        );
        assert_eq!(
            case.bounded_by_max_concurrent,
            reconstructed <= case.max_concurrent,
            "case {} drifted from the max_concurrent capacity bound",
            case.name
        );
    }
}

pub(super) async fn generated_inference_call_exact_target_cases_drive_fenced_updates() {
    let cases = lean_inference_call_exact_target_cases();
    assert_eq!(
        cases.len(),
        12,
        "Lean should emit every exact-target success and rejection class"
    );

    let db = test_db("inference-call-exact-target").await;

    for (index, case) in cases.iter().enumerate() {
        let request_id = format!("exact-target::{index}");
        let sibling_doc_id = insert_exact_target_row(
            &db.node,
            &format!("exact-target::{index}::sibling"),
            &request_id,
            &case.sibling_pre_state,
            case.target_owner,
            case.target_epoch,
        )
        .await;
        let target_doc_id = if let Some(target_pre_state) = case.target_pre_state.as_deref() {
            Some(
                insert_exact_target_row(
                    &db.node,
                    &format!("exact-target::{index}::target"),
                    &request_id,
                    target_pre_state,
                    case.target_owner,
                    case.target_epoch,
                )
                .await,
            )
        } else {
            None
        };

        assert_eq!(
            case.target_present,
            target_doc_id.is_some(),
            "case {} target-presence witness drifted",
            case.name
        );
        assert!(case.sibling_isolated, "Lean must prove sibling isolation");
        assert!(
            !case.terminal_pre_state || case.terminal_irreversible,
            "Lean must reject every terminal-row rewrite"
        );
        if case.target_present {
            assert!(
                case.same_logical_call_id,
                "the formal fixture must make logical identity unable to explain isolation"
            );
        }

        let write_doc_id = match case.write_target.as_str() {
            "target" => target_doc_id
                .as_deref()
                .expect("target-labelled generated case must create a target row"),
            "missing" => "018f0000-0000-7000-8000-000000000000",
            other => panic!("unmodelled exact-target selector {other:?}"),
        };

        let matched = apply_generated_fenced_update(
            &db.node,
            write_doc_id,
            &case.expected_state,
            case.expected_owner,
            case.expected_epoch,
            &case.action,
            &case.requested_post_state,
        )
        .await;
        assert_eq!(
            matched, case.write_matched,
            "case {} matched a different physical/state fence",
            case.name
        );

        let target_after = match target_doc_id.as_deref() {
            Some(doc_id) => load_exact_target_row(&db.node, doc_id)
                .await
                .map(|row| row.call_state),
            None => None,
        };
        assert_eq!(
            target_after, case.target_post_state,
            "case {} produced the wrong target state",
            case.name
        );

        let sibling_after = load_exact_target_row(&db.node, &sibling_doc_id)
            .await
            .expect("exact-target sibling must remain present");
        assert_eq!(
            sibling_after.call_state, case.sibling_post_state,
            "case {} mutated its sibling",
            case.name
        );
        assert_eq!(
            sibling_after.request_id, request_id,
            "case {} rebound sibling request correlation",
            case.name
        );
        assert_eq!(
            sibling_after.call_id,
            format!("exact-target::{index}::sibling"),
            "case {} rebound sibling logical identity",
            case.name
        );
        assert_eq!(
            sibling_after.runtime_instance_id,
            format!("owner-{}", case.target_owner),
            "case {} rebound sibling owner",
            case.name
        );
        assert_eq!(
            sibling_after.controller_generation, case.target_epoch as i64,
            "case {} rebound sibling epoch",
            case.name
        );
    }

    drive_exact_target_trace_cases(&db.node).await;
}

async fn drive_exact_target_trace_cases(node: &EmbeddedNode) {
    let traces = lean_inference_call_exact_target_trace_cases();
    assert_eq!(traces.len(), 3, "Lean must emit all two-write CAS traces");

    for (index, trace) in traces.iter().enumerate() {
        assert!(matches!(
            trace.scenario.as_str(),
            "strict_cas_then_idempotent_observation"
                | "logical_conflict_rejects_admission"
                | "recovery_like_source_state_flip"
        ));
        assert!(
            trace.unique_admission_required,
            "trace {} must retain the visible-conflict admission fence",
            trace.name
        );
        assert!(
            trace.visible_logical_document_count > 0,
            "trace {} must observe at least its selected physical document",
            trace.name
        );
        let request_id = format!("exact-target-trace::{index}");
        let target_doc_id = insert_exact_target_row(
            node,
            &format!("exact-target-trace::{index}::target"),
            &request_id,
            &trace.target_pre_state,
            trace.first_expected_owner,
            trace.first_expected_epoch,
        )
        .await;
        let sibling_doc_id = insert_exact_target_row(
            node,
            &format!("exact-target-trace::{index}::sibling"),
            &request_id,
            &trace.sibling_pre_state,
            trace.first_expected_owner,
            trace.first_expected_epoch,
        )
        .await;
        let doc_id_for = |label: &str| match label {
            "target" => target_doc_id.as_str(),
            "sibling" => sibling_doc_id.as_str(),
            other => panic!("unmodelled trace target {other:?}"),
        };

        let first_matched = apply_generated_admitted_update(
            node,
            trace.visible_logical_document_count,
            trace.unique_admission_required,
            doc_id_for(&trace.first_target),
            &trace.first_expected_state,
            trace.first_expected_owner,
            trace.first_expected_epoch,
            &trace.first_action,
            &trace.first_requested_post_state,
        )
        .await;
        assert_eq!(
            first_matched, trace.first_cas_matched,
            "trace {} first CAS drifted",
            trace.name
        );

        let second_doc_id = doc_id_for(&trace.second_target);
        let second_matched = apply_generated_admitted_update(
            node,
            trace.visible_logical_document_count,
            trace.unique_admission_required,
            second_doc_id,
            &trace.second_expected_state,
            trace.second_expected_owner,
            trace.second_expected_epoch,
            &trace.second_action,
            &trace.second_requested_post_state,
        )
        .await;
        assert_eq!(
            second_matched, trace.second_cas_matched,
            "trace {} second strict CAS drifted",
            trace.name
        );

        let observed = load_exact_target_row(node, second_doc_id)
            .await
            .expect("trace target must remain present");
        let second_disposition = if trace.unique_admission_required
            && trace.visible_logical_document_count != 1
        {
            "rejected"
        } else if second_matched {
            "applied"
        } else if action_accepts_expected_source(&trace.second_action, &trace.second_expected_state)
            && observed.call_state == trace.second_requested_post_state
            && observed.runtime_instance_id == format!("owner-{}", trace.second_expected_owner)
            && observed.controller_generation == trace.second_expected_epoch as i64
        {
            "observed_desired"
        } else {
            "rejected"
        };
        assert_eq!(
            second_disposition, trace.second_disposition,
            "trace {} conflated strict CAS with idempotent observation",
            trace.name
        );

        if trace.raw_independent_cas_possible {
            assert_eq!(
                trace.visible_logical_document_count, 2,
                "only the duplicate-row witness should expose raw sibling CAS independence"
            );
            assert!(
                !first_matched && !second_matched,
                "logical conflict admission must reject both writes before physical CAS"
            );
        }

        assert_eq!(
            load_exact_target_row(node, &target_doc_id)
                .await
                .expect("trace target must exist")
                .call_state,
            trace.final_target_state,
            "trace {} ended with the wrong target state",
            trace.name
        );
        assert_eq!(
            load_exact_target_row(node, &sibling_doc_id)
                .await
                .expect("trace sibling must exist")
                .call_state,
            trace.final_sibling_state,
            "trace {} ended with the wrong sibling state",
            trace.name
        );
    }
}

async fn apply_generated_admitted_update(
    node: &EmbeddedNode,
    visible_logical_document_count: usize,
    unique_admission_required: bool,
    doc_id: &str,
    expected_state: &str,
    expected_owner: usize,
    expected_epoch: usize,
    action: &str,
    requested_post_state: &str,
) -> bool {
    if unique_admission_required && visible_logical_document_count != 1 {
        return false;
    }
    apply_generated_fenced_update(
        node,
        doc_id,
        expected_state,
        expected_owner,
        expected_epoch,
        action,
        requested_post_state,
    )
    .await
}

fn action_accepts_expected_source(action: &str, expected_state: &str) -> bool {
    matches!(
        (action, expected_state),
        ("start", "queued")
            | ("complete", "running")
            | ("fail", "running")
            | ("cancel", "queued" | "running")
    )
}

async fn apply_generated_fenced_update(
    node: &EmbeddedNode,
    doc_id: &str,
    expected_state: &str,
    expected_owner: usize,
    expected_epoch: usize,
    action: &str,
    requested_post_state: &str,
) -> bool {
    if !action_accepts_expected_source(action, expected_state) {
        return false;
    }

    let mutation = format!(
        r#"mutation {{
            update_InferenceCall(
                filter: {{
                    _docID: {{ _eq: "{doc_id}" }},
                    call_state: {{ _eq: "{expected_state}" }},
                    runtime_instance_id: {{ _eq: "owner-{expected_owner}" }},
                    controller_generation: {{ _eq: {expected_epoch} }}
                }},
                input: {{ call_state: "{requested_post_state}" }}
            ) {{ _docID }}
        }}"#,
        doc_id = escape_graphql_string(doc_id),
        expected_state = escape_graphql_string(expected_state),
        requested_post_state = escape_graphql_string(requested_post_state),
    );
    let response = node
        .execute_with_retry(
            &mutation,
            ExecuteRetryPolicy::new(64, Duration::from_millis(1), Duration::from_millis(10)),
        )
        .await;
    assert!(
        !response.has_errors(),
        "exact-target update failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("update_InferenceCall"))
        .is_some_and(graphql_value_has_documents)
}

pub(super) async fn concurrent_exact_target_cas_serializes_one_winner_and_terminal_absorbs() {
    const CONTENDERS: usize = 16;
    const OWNER: usize = 17;
    const EPOCH: usize = 3;

    let db = test_db("inference-call-exact-target-concurrency").await;
    let doc_id = insert_exact_target_row(
        &db.node,
        "exact-target-concurrency::call",
        "exact-target-concurrency::request",
        "queued",
        OWNER,
        EPOCH,
    )
    .await;

    // This is the concrete DefraDB refinement of `applyFenced`: query-plan
    // update revalidates its filter and captures the expected document
    // (`crates/query-plan/src/plan/mutation/update.rs`), then the auto-commit
    // mutator takes the per-document write queue, reloads under that guard, and
    // compares the complete expected document
    // (`crates/db/src/auto_commit_mutator/update.rs`). Conflict retry reruns the
    // filter, so only one queued -> running contender can return this `_docID`.
    let start_barrier = Arc::new(Barrier::new(CONTENDERS));
    let mut start_tasks = Vec::with_capacity(CONTENDERS);
    for _ in 0..CONTENDERS {
        let node = db.node.clone();
        let doc_id = doc_id.clone();
        let barrier = start_barrier.clone();
        start_tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            apply_generated_fenced_update(
                &node, &doc_id, "queued", OWNER, EPOCH, "start", "running",
            )
            .await
        }));
    }
    let mut start_winners = 0;
    for task in start_tasks {
        start_winners += usize::from(task.await.expect("start contender must not panic"));
    }
    assert_eq!(start_winners, 1, "exactly one queued CAS must win");
    assert_eq!(
        load_exact_target_row(&db.node, &doc_id)
            .await
            .expect("raced row must remain present")
            .call_state,
        "running"
    );

    let terminal_barrier = Arc::new(Barrier::new(CONTENDERS));
    let mut terminal_tasks = Vec::with_capacity(CONTENDERS);
    for index in 0..CONTENDERS {
        let node = db.node.clone();
        let doc_id = doc_id.clone();
        let barrier = terminal_barrier.clone();
        terminal_tasks.push(tokio::spawn(async move {
            let (action, desired) = if index % 2 == 0 {
                ("complete", "completed")
            } else {
                ("fail", "failed")
            };
            barrier.wait().await;
            apply_generated_fenced_update(&node, &doc_id, "running", OWNER, EPOCH, action, desired)
                .await
        }));
    }
    let mut terminal_winners = 0;
    for task in terminal_tasks {
        terminal_winners += usize::from(task.await.expect("terminal contender must not panic"));
    }
    assert_eq!(
        terminal_winners, 1,
        "different terminal outcomes must serialize to one winner"
    );

    let terminal_state = load_exact_target_row(&db.node, &doc_id)
        .await
        .expect("terminal row must remain present")
        .call_state;
    assert!(matches!(terminal_state.as_str(), "completed" | "failed"));

    let complete_again = apply_generated_fenced_update(
        &db.node,
        &doc_id,
        "running",
        OWNER,
        EPOCH,
        "complete",
        "completed",
    )
    .await;
    let fail_again =
        apply_generated_fenced_update(&db.node, &doc_id, "running", OWNER, EPOCH, "fail", "failed")
            .await;
    let reopen = apply_generated_fenced_update(
        &db.node, &doc_id, "queued", OWNER, EPOCH, "start", "running",
    )
    .await;
    assert!(!complete_again && !fail_again && !reopen);
    assert_eq!(
        load_exact_target_row(&db.node, &doc_id)
            .await
            .expect("terminal row must remain present")
            .call_state,
        terminal_state,
        "a terminal winner must remain absorbing"
    );
}

fn graphql_value_has_documents(value: &Value) -> bool {
    value.as_array().is_some_and(|rows| !rows.is_empty())
        || value
            .as_object()
            .is_some_and(|row| row.get("_docID").is_some_and(Value::is_string))
}

async fn insert_exact_target_row(
    node: &EmbeddedNode,
    call_id: &str,
    request_id: &str,
    call_state: &str,
    owner: usize,
    epoch: usize,
) -> String {
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            add_InferenceCall(input: {{
                call_id: "{call_id}",
                runtime_instance_id: "owner-{owner}",
                request_id: "{request_id}",
                call_seq: 1,
                backend_id: "{BACKEND_ID}",
                behavior_id: "{AGENT_NAME}",
                agent_did: "{AGENT_DID}",
                call_kind: "inference",
                attempt: 1,
                call_state: "{call_state}",
                queued_at: "{now}",
                priority: 0,
                queue_depth_at_enqueue: 0,
                controller_generation: {epoch},
                backend_config_fingerprint: "test"
            }}) {{ _docID }}
        }}"#,
        call_id = escape_graphql_string(call_id),
        request_id = escape_graphql_string(request_id),
        call_state = escape_graphql_string(call_state),
        now = escape_graphql_string(&now),
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "insert exact-target InferenceCall failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("add_InferenceCall"))
        .and_then(|value| {
            value.get("_docID").and_then(Value::as_str).or_else(|| {
                value
                    .as_array()
                    .and_then(|rows| rows.first())
                    .and_then(|row| row.get("_docID"))
                    .and_then(Value::as_str)
            })
        })
        .expect("add_InferenceCall must return its physical _docID")
        .to_string()
}

async fn load_exact_target_row(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Option<PersistedExactTargetRow> {
    let query = format!(
        r#"{{
            InferenceCall(filter: {{ _docID: {{ _eq: "{doc_id}" }} }}, limit: 1) {{
                _docID call_id request_id call_state runtime_instance_id controller_generation
            }}
        }}"#,
        doc_id = escape_graphql_string(doc_id),
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "load exact-target InferenceCall failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceCall"))
        .and_then(|value| {
            serde_json::from_value::<Vec<PersistedExactTargetRow>>(value.clone()).ok()
        })
        .and_then(|mut rows| rows.pop())
        .inspect(|row| assert_eq!(row.doc_id, doc_id))
}

fn case_call_id(case_name: &str, index: usize) -> String {
    format!("{case_name}::call-{index}")
}

async fn insert_inference_call_row(
    node: &EmbeddedNode,
    case_name: &str,
    index: usize,
    backend_id: &str,
    call_state: &str,
) {
    let call_id = case_call_id(case_name, index);
    let request_id = format!("{case_name}::request-{index}");
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            add_InferenceCall(input: {{
                call_id: "{call_id}",
                runtime_instance_id: "runtime-slot-conformance",
                request_id: "{request_id}",
                call_seq: 1,
                backend_id: "{backend_id}",
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
        request_id = escape_graphql_string(&request_id),
        backend_id = escape_graphql_string(backend_id),
        call_state = escape_graphql_string(call_state),
        now = escape_graphql_string(&now),
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "insert InferenceCall slot row failed: {:?}",
        resp.errors
    );
}

async fn fetch_persisted_slot_rows_for_case(
    node: &EmbeddedNode,
    case_name: &str,
) -> Vec<PersistedSlotRow> {
    let query = r#"{
        InferenceCall {
            call_id
            backend_id
            call_state
        }
    }"#;
    let response = node.execute(query).await;
    assert!(
        !response.has_errors(),
        "query InferenceCall slot rows failed: {:?}",
        response.errors
    );
    let prefix = format!("{case_name}::call-");
    response
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceCall"))
        .and_then(|value| serde_json::from_value::<Vec<PersistedSlotRow>>(value.clone()).ok())
        .unwrap_or_default()
        .into_iter()
        .filter(|row| row.call_id.starts_with(&prefix))
        .collect()
}
