use super::*;

use gents::config::DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS;
use gents::StreamWriter;
use gents_protocol::transcript::present_persisted_message;

use super::support::interrupt::{
    create_runtime_request, wait_for_inference_call_state, wait_for_request_lifecycle_state,
    wait_for_response_content_contains, wait_for_response_doc_id, wait_for_runtime_ready,
    BootedAgent, InferenceCallSnapshot,
};
use super::support::streaming_backend::{MockStreamingBackend, StreamScript};

const INTERRUPT_FLOW_MODEL: &str = "default";
const INTERRUPT_FLOW_BACKEND_ID: &str = "backend-streaming-response-interrupt-flow";
const INTERRUPT_FLOW_MARKER: &str = "streaming-response-interrupt-flow";
const INTERRUPT_FLOW_PARTIAL: &str = "partial response content ";
const IDLE_TIMEOUT_MODEL: &str = "default";
const IDLE_TIMEOUT_BACKEND_ID: &str = "backend-streaming-response-idle-timeout";
const IDLE_TIMEOUT_MARKER: &str = "streaming-response-idle-timeout";
const IDLE_TIMEOUT_PARTIAL: &str = "partial before idle timeout ";
const IDLE_TIMEOUT_CONFIGURED_SECS: u64 = 5;

#[derive(Debug, Deserialize)]
struct StreamingResponseRow {
    content: String,
    reasoning: Option<String>,
    status: String,
    error_message: Option<String>,
    token_count: i64,
    progress_seq: i64,
    materialized_message_sequence: Option<i64>,
    interrupted_at: Option<String>,
    completed_at: Option<String>,
}

pub(super) async fn generated_streaming_response_cases_pin_lifecycle_contract() {
    let cases = lean_response_transition_cases();
    assert_eq!(cases.len(), 12);

    let expected_names = [
        "begin_emits_streaming_empty",
        "write_tokens_advances_progress",
        "write_reasoning_no_token_bump",
        "flush_pending_is_abstract_noop",
        "reset_tail_clears_but_preserves_tokens",
        "finalize_complete_clears_and_materializes",
        "finalize_error_inference_failed_clears",
        "finalize_error_idle_timeout_requires_deadline",
        "recover_interrupted_keeps_content",
        "observe_idempotent_finalize_is_noop",
        "set_interrupted_at_does_not_change_status",
        "bridge_completed_pairs_request_committed",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(
        cases
            .iter()
            .map(|case| case.name.as_str())
            .collect::<BTreeSet<_>>(),
        expected_names
    );

    for finalize_name in [
        "finalize_complete_clears_and_materializes",
        "bridge_completed_pairs_request_committed",
    ] {
        let case = lean_response_transition_cases()
            .iter()
            .find(|case| case.name == finalize_name)
            .unwrap_or_else(|| panic!("{finalize_name} contract case should be emitted"));
        assert_eq!(
            case.post_live_tail, "empty",
            "{finalize_name}: #64 live-tail clear preserved"
        );
        assert_eq!(
            case.pre_tail_reasoning, "nonEmpty",
            "{finalize_name}: reasoning present in the live tail pre-finalize"
        );
        assert_eq!(
            case.pre_durable_reasoning, "empty",
            "{finalize_name}: no durable reasoning before materialize"
        );
        assert_eq!(
            case.post_durable_reasoning, "nonEmpty",
            "{finalize_name}: reasoning durably copied at materialize (#492)"
        );
    }

    for case in cases {
        drive_streaming_response_case(case).await;
    }
}

pub(super) async fn generated_streaming_response_interrupt_flow_cases_drive_daemon_contract() {
    let cases = lean_response_interrupt_flow_cases();
    assert_eq!(cases.len(), 1);
    let expected_names = ["daemon_interrupt_terminalizes_response_and_request"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        cases
            .iter()
            .map(|case| case.name.as_str())
            .collect::<BTreeSet<_>>(),
        expected_names
    );

    for case in cases {
        drive_streaming_response_interrupt_flow_case(case).await;
    }
}

pub(super) async fn generated_streaming_response_idle_timeout_case_drives_daemon_contract() {
    let case = lean_response_transition_cases()
        .iter()
        .find(|case| case.name == "finalize_error_idle_timeout_requires_deadline")
        .expect("Lean idle-timeout response transition case should be emitted");

    assert!(case.legal);
    assert_eq!(case.group, "normal");
    assert_eq!(case.action, "finalize_error");
    assert_eq!(case.pre_status, "streaming");
    assert_eq!(case.post_status, "error");
    assert_eq!(case.pre_live_tail, "nonEmpty");
    assert_eq!(case.post_live_tail, "empty");
    assert_eq!(case.error_reason.as_deref(), Some("streamIdleTimeout"));
    assert_eq!(case.expected_request_state.as_deref(), Some("failed"));
    assert_eq!(
        case.expected_request_persistence.as_deref(),
        Some("committed")
    );

    drive_streaming_response_idle_timeout_case(case).await;
}

async fn drive_streaming_response_idle_timeout_case(
    case: &lean_vocab_test::LeanResponseTransitionCase,
) {
    let db = signed_materializer_test_db(&format!("streaming-idle-timeout-{}", case.name)).await;
    let backend = MockStreamingBackend::start(
        IDLE_TIMEOUT_MODEL,
        vec![StreamScript::paused(
            IDLE_TIMEOUT_MARKER,
            [IDLE_TIMEOUT_PARTIAL],
        )],
    )
    .expect("start mock streaming backend");
    let agent = boot_streaming_idle_timeout_agent(&db, &case.name, backend.endpoint()).await;

    let request_id = format!("{}-{}", case.name, uuid::Uuid::new_v4());
    let session_id = format!("session-{}", uuid::Uuid::new_v4());
    let request_doc_id = create_runtime_request(
        db.node.as_ref(),
        agent.agent_did.as_str(),
        AGENT_NAME,
        &request_id,
        &session_id,
        IDLE_TIMEOUT_MARKER,
    )
    .await;

    wait_for_backend_chunks_realtime(&backend, IDLE_TIMEOUT_MARKER, 1).await;
    let response_doc_id = wait_for_response_doc_id_realtime(db.node.as_ref(), &request_id).await;
    let pre_response = wait_for_response_content_contains_realtime(
        db.node.as_ref(),
        &response_doc_id,
        IDLE_TIMEOUT_PARTIAL,
    )
    .await;
    assert_eq!(pre_response.status, case.pre_status);
    assert_eq!(live_tail_shape(&pre_response), case.pre_live_tail);
    assert!(pre_response.token_count > 0);

    let pre_request =
        wait_for_request_lifecycle_state_realtime(db.node.as_ref(), &request_doc_id, "processing")
            .await;
    assert_eq!(pre_request.status, "processing");
    let pre_call =
        wait_for_latest_inference_call_state_realtime(db.node.as_ref(), &request_id, "running")
            .await;
    assert!(!inference_call_state_is_terminal(&pre_call.call_state));

    // The progress write above is the final operation for the first stream
    // item. Give the processor a turn to install its next-poll idle deadline,
    // then cross that deadline exactly once. Repeated virtual-time advances
    // while terminal persistence is running can incorrectly exhaust DefraDB's
    // own query timeout under host load.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(IDLE_TIMEOUT_CONFIGURED_SECS + 1)).await;
    tokio::task::yield_now().await;

    let post_response =
        wait_for_response_status_realtime(db.node.as_ref(), &response_doc_id, &case.post_status)
            .await;
    assert_eq!(live_tail_shape(&post_response), case.post_live_tail);
    assert_eq!(post_response.token_count, pre_response.token_count);
    assert!(
        post_response
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains(&format!(
                "no data received for {IDLE_TIMEOUT_CONFIGURED_SECS}s"
            ))),
        "{}: response error should preserve stream liveness timeout reason; actual={:?}",
        case.name,
        post_response.error_message
    );
    assert!(post_response
        .completed_at
        .as_deref()
        .is_some_and(|value| !value.is_empty()));

    let post_request = wait_for_request_lifecycle_state_realtime(
        db.node.as_ref(),
        &request_doc_id,
        case.expected_request_state
            .as_deref()
            .expect("idle timeout case should project a request state"),
    )
    .await;
    assert_eq!(post_request.status, "error");
    assert_eq!(post_request.backend_id, IDLE_TIMEOUT_BACKEND_ID);
    assert!(request_state_is_terminal(&post_request.lifecycle_state));
    assert!(
        post_request.failure_reason.contains(&format!(
            "no data received for {IDLE_TIMEOUT_CONFIGURED_SECS}s"
        )),
        "{}: request failure should preserve stream liveness timeout reason; actual={:?}",
        case.name,
        post_request.failure_reason
    );

    let post_call =
        wait_for_latest_inference_call_state_realtime(db.node.as_ref(), &request_id, "failed")
            .await;
    assert!(inference_call_state_is_terminal(&post_call.call_state));
    assert!(
        post_call
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains(&format!(
                "no data received for {IDLE_TIMEOUT_CONFIGURED_SECS}s"
            ))),
        "{}: inference call failure should preserve stream liveness timeout reason; actual={:?}",
        case.name,
        post_call.failure_reason
    );

    drop(agent);
}

async fn wait_for_response_doc_id_realtime(node: &EmbeddedNode, request_id: &str) -> String {
    let escaped_request_id = escape_graphql_string(request_id);
    let started = std::time::Instant::now();
    loop {
        let query = format!(
            r#"{{
                AgentResponse(
                    filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                    limit: 1
                ) {{
                    _docID
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        if let Some(row) = first_optional_row::<DocIdRow>(&response, "AgentResponse") {
            return row.doc_id;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timed out waiting for AgentResponse for request_id={request_id}"
        );
        tokio::task::yield_now().await;
    }
}

async fn wait_for_backend_chunks_realtime(
    backend: &MockStreamingBackend,
    marker: &str,
    expected: usize,
) {
    let started = std::time::Instant::now();
    loop {
        let observed = backend.observed_chunks(marker);
        if observed >= expected {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timed out waiting for {expected} chunk(s) for marker {marker}, observed {observed}"
        );
        tokio::task::yield_now().await;
    }
}

async fn wait_for_response_content_contains_realtime(
    node: &EmbeddedNode,
    response_doc_id: &str,
    expected: &str,
) -> StreamingResponseRow {
    let started = std::time::Instant::now();
    loop {
        let row = load_streaming_response_row(node, response_doc_id).await;
        if row.content.contains(expected) && row.progress_seq >= 2 {
            return row;
        }
        assert_ne!(
            row.status, "error",
            "live response failed before content contained {expected:?}; error_message={:?}",
            row.error_message
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timed out waiting for response content to contain {expected:?}; last={:?}",
            row.content
        );
        tokio::task::yield_now().await;
    }
}

async fn wait_for_response_status_realtime(
    node: &EmbeddedNode,
    response_doc_id: &str,
    expected: &str,
) -> StreamingResponseRow {
    let started = std::time::Instant::now();
    loop {
        let row = load_streaming_response_row(node, response_doc_id).await;
        if row.status == expected {
            return row;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timed out waiting for AgentResponse {response_doc_id} status={expected}; last={}",
            row.status
        );
        tokio::task::yield_now().await;
    }
}

async fn wait_for_request_lifecycle_state_realtime(
    node: &EmbeddedNode,
    request_doc_id: &str,
    expected: &str,
) -> RequestSnapshot {
    let started = std::time::Instant::now();
    loop {
        let snapshot = fetch_request_snapshot(node, request_doc_id).await;
        if snapshot.lifecycle_state == expected {
            return snapshot;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timed out waiting for AgentRequest {request_doc_id} lifecycle_state={expected}; last={}",
            snapshot.lifecycle_state
        );
        tokio::task::yield_now().await;
    }
}

async fn wait_for_latest_inference_call_state_realtime(
    node: &EmbeddedNode,
    request_id: &str,
    expected: &str,
) -> InferenceCallSnapshot {
    let started = std::time::Instant::now();
    loop {
        let row = fetch_latest_inference_call_snapshot(node, request_id).await;
        if row
            .as_ref()
            .is_some_and(|row| row.call_state.as_str() == expected)
        {
            return row.expect("checked Some");
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timed out waiting for latest inference call request_id={request_id} call_state={expected}; last={row:?}"
        );
        tokio::task::yield_now().await;
    }
}

async fn fetch_latest_inference_call_snapshot(
    node: &EmbeddedNode,
    request_id: &str,
) -> Option<InferenceCallSnapshot> {
    let escaped_request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            InferenceCall(
                filter: {{
                    request_id: {{ _eq: "{escaped_request_id}" }},
                    call_kind: {{ _eq: "inference" }}
                }},
                order: {{ call_seq: DESC }},
                limit: 1
            ) {{
                call_seq
                call_state
                failure_reason
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    first_optional_row::<InferenceCallSnapshot>(&response, "InferenceCall")
}

async fn drive_streaming_response_interrupt_flow_case(
    case: &lean_vocab_test::LeanResponseInterruptFlowCase,
) {
    assert_eq!(case.group, "interrupt");
    assert_eq!(case.action, "daemon_interrupt_flow");

    let db = signed_materializer_test_db(&format!("streaming-interrupt-flow-{}", case.name)).await;
    let backend = MockStreamingBackend::start(
        INTERRUPT_FLOW_MODEL,
        vec![StreamScript::paused(
            INTERRUPT_FLOW_MARKER,
            [INTERRUPT_FLOW_PARTIAL],
        )],
    )
    .expect("start mock streaming backend");
    let agent = boot_streaming_interrupt_flow_agent(&db, &case.name, backend.endpoint()).await;

    let request_id = format!("{}-{}", case.name, uuid::Uuid::new_v4());
    let session_id = format!("session-{}", uuid::Uuid::new_v4());
    let request_doc_id = create_runtime_request(
        db.node.as_ref(),
        agent.agent_did.as_str(),
        AGENT_NAME,
        &request_id,
        &session_id,
        INTERRUPT_FLOW_MARKER,
    )
    .await;

    backend.wait_for_chunks(INTERRUPT_FLOW_MARKER, 1).await;
    let response_doc_id = wait_for_response_doc_id(db.node.as_ref(), &request_id).await;
    wait_for_response_content_contains(db.node.as_ref(), &response_doc_id, INTERRUPT_FLOW_PARTIAL)
        .await;

    let pre_request = fetch_request_snapshot(&db.node, &request_doc_id).await;
    assert_eq!(pre_request.lifecycle_state, case.pre_request_state);
    let pre_response = load_streaming_response_row(&db.node, &response_doc_id).await;
    assert_eq!(pre_response.status, case.pre_response_status);
    let pre_call = wait_for_inference_call_state(
        db.node.as_ref(),
        &request_id,
        &case.pre_inference_call_state,
    )
    .await;
    assert_eq!(pre_call.call_state, case.pre_inference_call_state);

    interrupt_request(db.node.as_ref(), &request_id)
        .await
        .expect("interrupt_request should latch interrupt_requested_at");

    wait_for_request_lifecycle_state(db.node.as_ref(), &request_doc_id, &case.post_request_state)
        .await;
    let post_call = wait_for_inference_call_state(
        db.node.as_ref(),
        &request_id,
        &case.post_inference_call_state,
    )
    .await;
    assert_eq!(post_call.call_state, case.post_inference_call_state);

    let post_request = fetch_request_snapshot(&db.node, &request_doc_id).await;
    assert_eq!(post_request.lifecycle_state, case.post_request_state);
    assert_eq!(post_request.status, case.post_request_state);
    assert_eq!(
        request_state_is_terminal(&post_request.lifecycle_state),
        case.request_terminal
    );

    let post_response = load_streaming_response_row(&db.node, &response_doc_id).await;
    assert_eq!(post_response.status, case.post_response_status);
    assert_eq!(
        response_status_is_terminal(&post_response.status),
        case.response_terminal
    );
    assert_eq!(
        post_response.error_message.as_deref(),
        Some(case.response_error_reason.as_str())
    );
    assert_eq!(
        post_response
            .interrupted_at
            .as_deref()
            .is_some_and(|value| !value.is_empty()),
        case.interrupted_at_required
    );
    assert_eq!(
        post_response
            .completed_at
            .as_deref()
            .is_some_and(|value| !value.is_empty()),
        case.completed_at_required
    );
    if case.live_tail_cleared {
        assert_eq!(post_response.content, "");
        assert!(post_response
            .reasoning
            .as_deref()
            .is_none_or(|reasoning| reasoning.is_empty()));
    }

    if case.partial_turn_materialized {
        assert!(
            post_response.materialized_message_sequence.is_some(),
            "{}: interrupted flow must link AgentResponse to the materialized partial message",
            case.name
        );
        let messages = fetch_message_snapshots_for_session(&db.node, &session_id).await;
        assert!(
            messages.iter().any(|message| {
                message.role == "assistant"
                    && present_persisted_message(&message.role, &message.content).body_markdown
                        == INTERRUPT_FLOW_PARTIAL.trim()
            }),
            "{}: interrupted flow must materialize the partial assistant turn",
            case.name
        );
    }
    assert_eq!(
        inference_call_state_is_terminal(&post_call.call_state),
        case.inference_call_terminal
    );

    agent.shutdown().await;
}

async fn drive_streaming_response_case(case: &lean_vocab_test::LeanResponseTransitionCase) {
    assert!(case.legal, "streaming case {} should be legal", case.name);
    assert!(
        case.post_token_count >= case.pre_token_count,
        "streaming case {} should not decrease token count",
        case.name
    );

    let db = test_db(&format!("streaming-{}", case.name)).await;
    let request_id = format!("{}-{}", case.name, uuid::Uuid::new_v4());
    let session_id = format!("session-{}", uuid::Uuid::new_v4());
    let created_at = chrono::Utc::now().to_rfc3339();
    let request_status = if case.pre_status == "complete" {
        "completed"
    } else {
        "processing"
    };
    let request_doc_id = create_request(
        &db.node,
        &request_id,
        &session_id,
        request_status,
        &created_at,
    )
    .await;
    let writer = DefraStreamWriter::new(db.node.clone(), AGENT_DID, Duration::from_millis(0));

    let doc_id = if case.pre_status == "complete" {
        create_manual_response(
            &db.node,
            &request_id,
            &session_id,
            &case.pre_status,
            case.pre_token_count,
            case.pre_materialized_seq,
        )
        .await
    } else {
        let doc_id = writer
            .begin(&session_id, &request_id, AGENT_NAME)
            .await
            .expect("begin streaming response");
        seed_streaming_tail(&writer, &doc_id, case.pre_token_count, &case.pre_live_tail).await;
        doc_id
    };

    assert_streaming_response_shape(&db.node, &doc_id, case, ResponsePhase::Pre).await;

    match case.action.as_str() {
        "begin" => {}
        "write_tokens" => {
            let delta = case
                .post_token_count
                .checked_sub(case.pre_token_count)
                .expect("write_tokens delta");
            writer
                .write_tokens(&doc_id, &tokens(delta))
                .await
                .expect("write tokens");
            writer.flush_pending(&doc_id).await.expect("flush tokens");
        }
        "write_reasoning" => {
            writer
                .write_reasoning(&doc_id, "reasoning trace")
                .await
                .expect("write reasoning");
            writer
                .flush_pending(&doc_id)
                .await
                .expect("flush reasoning");
        }
        "flush" => {
            writer.flush_pending(&doc_id).await.expect("flush pending");
        }
        "reset_tail" => {
            writer.reset_tail(&doc_id).await.expect("reset tail");
        }
        "finalize_complete" => {
            if let Some(sequence) = case.post_materialized_seq {
                mark_materialized(db.node.clone(), &request_id, sequence as u32).await;
            }
            writer
                .finalize(&doc_id, gents::streaming::StreamStatus::Complete)
                .await
                .expect("finalize complete");
        }
        "finalize_error" => {
            if let Some(reason) = case.error_reason.as_deref() {
                writer
                    .set_error_message(&doc_id, reason)
                    .await
                    .expect("set error reason");
            }
            writer
                .finalize(&doc_id, gents::streaming::StreamStatus::Error)
                .await
                .expect("finalize error");
        }
        "recover_interrupted" => {
            let report = RequestLifecycle::recover_all(&db.node, AGENT_DID)
                .await
                .expect("recover streaming response");
            assert_eq!(report.responses_recovered, 1, "{}", case.name);
            assert_eq!(report.requests_recovered, 1, "{}", case.name);
        }
        "observe_idempotent_finalize" => {
            writer
                .finalize(&doc_id, gents::streaming::StreamStatus::Complete)
                .await
                .expect("idempotent finalize");
        }
        "set_interrupted_at" => {
            let interrupted_at = chrono::Utc::now().to_rfc3339();
            assert!(
                writer
                    .write_interrupted_at(&doc_id, &interrupted_at)
                    .await
                    .expect("write interrupted_at"),
                "{}: interrupted_at update should match response",
                case.name
            );
        }
        other => panic!("unsupported streaming action {other:?} for {}", case.name),
    }

    assert_streaming_response_shape(&db.node, &doc_id, case, ResponsePhase::Post).await;
    assert_request_bridge_shape(&db.node, &request_doc_id, case).await;
}

#[derive(Clone, Copy)]
enum ResponsePhase {
    Pre,
    Post,
}

async fn assert_streaming_response_shape(
    node: &EmbeddedNode,
    doc_id: &str,
    case: &lean_vocab_test::LeanResponseTransitionCase,
    phase: ResponsePhase,
) {
    let row = load_streaming_response_row(node, doc_id).await;
    let (status, live_tail, token_count, materialized_sequence) = match phase {
        ResponsePhase::Pre => (
            case.pre_status.as_str(),
            case.pre_live_tail.as_str(),
            case.pre_token_count,
            case.pre_materialized_seq,
        ),
        ResponsePhase::Post => (
            case.post_status.as_str(),
            case.post_live_tail.as_str(),
            case.post_token_count,
            case.post_materialized_seq,
        ),
    };
    let phase_name = match phase {
        ResponsePhase::Pre => "pre",
        ResponsePhase::Post => "post",
    };

    assert_eq!(
        row.status.as_str(),
        status,
        "{} {phase_name}: status",
        case.name
    );
    assert_eq!(
        live_tail_shape(&row),
        live_tail,
        "{} {phase_name}: live tail",
        case.name
    );
    assert_eq!(
        row.token_count as usize, token_count,
        "{} {phase_name}: token_count",
        case.name
    );
    assert_eq!(
        row.materialized_message_sequence
            .map(|sequence| sequence as usize),
        materialized_sequence,
        "{} {phase_name}: materialized sequence",
        case.name
    );

    if matches!(phase, ResponsePhase::Post) {
        match case.error_reason.as_deref() {
            Some("daemonRestartRecovery") => {
                assert!(
                    row.content.contains("Response interrupted"),
                    "{}: recovery reason should be visible in recovered content",
                    case.name
                );
            }
            Some(reason) => {
                assert_eq!(
                    row.error_message.as_deref(),
                    Some(reason),
                    "{}: error reason",
                    case.name
                );
            }
            None => {}
        }

        if case.action == "set_interrupted_at" {
            assert!(
                row.interrupted_at
                    .as_deref()
                    .is_some_and(|value| !value.is_empty()),
                "{}: interrupted_at",
                case.name
            );
        }
    }
}

async fn assert_request_bridge_shape(
    node: &EmbeddedNode,
    request_doc_id: &str,
    case: &lean_vocab_test::LeanResponseTransitionCase,
) {
    let Some(expected_state) = case.expected_request_state.as_deref() else {
        return;
    };
    let snapshot = fetch_request_snapshot(node, request_doc_id).await;
    assert_eq!(
        snapshot.lifecycle_state.as_str(),
        expected_state,
        "{}: request lifecycle_state",
        case.name
    );
    let expected_status = match expected_state {
        "completed" => "completed",
        "failed" => "error",
        other => other,
    };
    assert_eq!(
        snapshot.status.as_str(),
        expected_status,
        "{}: request status",
        case.name
    );
    assert_eq!(
        case.expected_request_persistence.as_deref(),
        Some("committed"),
        "{}: terminal bridge persistence",
        case.name
    );
}

async fn seed_streaming_tail(
    writer: &DefraStreamWriter,
    doc_id: &str,
    token_count: usize,
    live_tail: &str,
) {
    if token_count > 0 {
        writer
            .write_tokens(doc_id, &tokens(token_count))
            .await
            .expect("seed tokens");
        writer.flush_pending(doc_id).await.expect("seed flush");
    } else if live_tail == "nonEmpty" {
        writer
            .write_reasoning(doc_id, "seed reasoning")
            .await
            .expect("seed reasoning");
        writer
            .flush_pending(doc_id)
            .await
            .expect("seed reasoning flush");
    }
}

fn tokens(count: usize) -> String {
    (0..count)
        .map(|index| format!("t{index}"))
        .collect::<Vec<_>>()
        .join(" ")
}

async fn create_manual_response(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    status: &str,
    token_count: usize,
    materialized_sequence: Option<usize>,
) -> String {
    let now = chrono::Utc::now().to_rfc3339();
    let completed_at = if matches!(status, "complete" | "error") {
        now.as_str()
    } else {
        ""
    };
    let materialized_fields = materialized_sequence
        .map(|sequence| {
            format!(r#"materialized_message_sequence: {sequence}, materialized_at: "{now}","#)
        })
        .unwrap_or_default();
    let request_id = escape_graphql_string(request_id);
    let session_id = escape_graphql_string(session_id);
    let status = escape_graphql_string(status);
    let mutation = format!(
        r#"mutation {{
            create_AgentResponse(input: {{
                response_key: "{request_id}",
                request_id: "{request_id}",
                agent_did: "{AGENT_DID}",
                behavior_id: "{AGENT_NAME}",
                session_id: "{session_id}",
                content: "",
                reasoning: "",
                status: "{status}",
                error_message: "",
                token_count: {token_count},
                progress_seq: 0,
                {materialized_fields}
                created_at: "{now}",
                completed_at: "{completed_at}"
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create manual AgentResponse failed: {:?}",
        resp.errors
    );

    let query = format!(
        r#"{{
            AgentResponse(filter: {{ response_key: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                _docID
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    support::first_row::<DocIdRow>(&resp, "AgentResponse").doc_id
}

async fn mark_materialized(node: std::sync::Arc<EmbeddedNode>, request_id: &str, sequence: u32) {
    let hook = DefraSessionHook::resume_or_create_with_identity_policy(
        node,
        "streaming-materialized-session",
        AGENT_NAME,
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .expect("resume materialization hook");
    hook.set_active_request_id(Some(request_id.to_string()))
        .await;
    hook.mark_current_response_materialized(sequence)
        .await
        .expect("mark response materialized");
}

async fn load_streaming_response_row(node: &EmbeddedNode, doc_id: &str) -> StreamingResponseRow {
    let doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            AgentResponse(filter: {{ _docID: {{ _eq: "{doc_id}" }} }}, limit: 1) {{
                content
                reasoning
                status
                error_message
                token_count
                progress_seq
                materialized_message_sequence
                interrupted_at
                completed_at
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    support::first_row::<StreamingResponseRow>(&resp, "AgentResponse")
}

fn live_tail_shape(row: &StreamingResponseRow) -> &'static str {
    let content_non_empty = !row.content.trim().is_empty();
    let reasoning_non_empty = row
        .reasoning
        .as_deref()
        .is_some_and(|reasoning| !reasoning.trim().is_empty());
    if content_non_empty || reasoning_non_empty {
        "nonEmpty"
    } else {
        "empty"
    }
}

fn request_state_is_terminal(state: &str) -> bool {
    matches!(
        state,
        "completed" | "failed" | "superseded" | "dead" | "interrupted"
    )
}

fn response_status_is_terminal(status: &str) -> bool {
    matches!(status, "complete" | "completed" | "error")
}

fn inference_call_state_is_terminal(state: &str) -> bool {
    matches!(state, "cancelled" | "completed" | "failed")
}

async fn boot_streaming_interrupt_flow_agent(
    db: &support::TestDb,
    _test_name: &str,
    endpoint: &str,
) -> BootedAgent {
    let identity = db
        .node_identity()
        .expect("streaming interrupt fixture must use the node signing identity");
    upsert_interrupt_flow_backend(db.node.as_ref(), endpoint).await;

    let agent = gents::Gents::builder()
        .node(db.node.clone())
        .identity(identity)
        .default_behavior_id(AGENT_NAME)
        .tool_ceiling(gents::ToolCeiling::meta_only())
        .behavior(AGENT_NAME)
        .backend_id(INTERRUPT_FLOW_BACKEND_ID)
        .model_name(INTERRUPT_FLOW_MODEL)
        .stream_batch_ms(0)
        .done()
        .build()
        .await
        .expect("build streaming interrupt-flow agent");
    let agent_did = agent.agent_did().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));
    wait_for_runtime_ready(db.node.as_ref(), &agent_did).await;

    BootedAgent::new(shutdown_tx, handle, agent_did)
}

async fn boot_streaming_idle_timeout_agent(
    db: &support::TestDb,
    _test_name: &str,
    endpoint: &str,
) -> BootedAgent {
    let identity = db
        .node_identity()
        .expect("streaming idle-timeout fixture must use the node signing identity");
    upsert_idle_timeout_backend(db.node.as_ref(), endpoint).await;

    let agent = gents::Gents::builder()
        .node(db.node.clone())
        .identity(identity)
        .default_behavior_id(AGENT_NAME)
        .tool_ceiling(gents::ToolCeiling::meta_only())
        .behavior(AGENT_NAME)
        .backend_id(IDLE_TIMEOUT_BACKEND_ID)
        .model_name(IDLE_TIMEOUT_MODEL)
        .stream_batch_ms(0)
        .stream_liveness_timeout_secs(IDLE_TIMEOUT_CONFIGURED_SECS)
        .deadline_duration_secs(DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS * 4)
        .done()
        .build()
        .await
        .expect("build streaming idle-timeout agent");
    let agent_did = agent.agent_did().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));
    wait_for_runtime_ready_realtime(db.node.as_ref(), &agent_did).await;

    BootedAgent::new(shutdown_tx, handle, agent_did)
}

async fn upsert_interrupt_flow_backend(node: &EmbeddedNode, endpoint: &str) {
    upsert_streaming_backend(
        node,
        INTERRUPT_FLOW_BACKEND_ID,
        endpoint,
        INTERRUPT_FLOW_MODEL,
    )
    .await;
}

async fn upsert_idle_timeout_backend(node: &EmbeddedNode, endpoint: &str) {
    upsert_streaming_backend(node, IDLE_TIMEOUT_BACKEND_ID, endpoint, IDLE_TIMEOUT_MODEL).await;
}

async fn upsert_streaming_backend(
    node: &EmbeddedNode,
    backend_id: &str,
    endpoint: &str,
    model_name: &str,
) {
    let escaped_backend_id = escape_graphql_string(backend_id);
    let escaped_endpoint = escape_graphql_string(endpoint);
    let escaped_model_name = escape_graphql_string(model_name);
    let mutation = format!(
        r#"mutation {{
            upsert_InferenceBackend(
                filter: {{ backend_id: {{ _eq: "{escaped_backend_id}" }} }},
                add: {{
                    backend_id: "{escaped_backend_id}",
                    name: "{escaped_backend_id}",
                    provider_kind: "OpenAiCompatible",
                    endpoint: "{escaped_endpoint}",
                    api_key: "",
                    api_key_env_var: "",
                    max_concurrent: 1,
                    max_queue_depth: 100,
                    enabled: true,
                    models: ["{escaped_model_name}"],
                    probe_status: "healthy"
                }},
                update: {{
                    name: "{escaped_backend_id}",
                    provider_kind: "OpenAiCompatible",
                    endpoint: "{escaped_endpoint}",
                    max_concurrent: 1,
                    max_queue_depth: 100,
                    enabled: true,
                    models: ["{escaped_model_name}"],
                    probe_status: "healthy"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "upsert streaming backend {backend_id} failed: {:?}",
        response.errors
    );
}

async fn wait_for_runtime_ready_realtime(node: &EmbeddedNode, agent_did: &str) {
    let started = std::time::Instant::now();
    loop {
        if let Some(snapshot) = support::snapshots::fetch_runtime_snapshot(node, agent_did).await {
            if snapshot.process_state == "ready"
                && snapshot.reconcile_phase == "idle"
                && snapshot.runnable_behavior_count >= 1
            {
                return;
            }
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "agent did not reach ready state"
        );
        tokio::task::yield_now().await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DocIdRow {
    doc_id: String,
}

impl<'de> Deserialize<'de> for DocIdRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Row {
            #[serde(rename = "_docID")]
            doc_id: String,
        }

        Row::deserialize(deserializer).map(|row| Self { doc_id: row.doc_id })
    }
}

pub(super) fn generated_compaction_reducer_cases_pin_contract() {
    let cases = lean_compaction_reducer_cases();
    assert_eq!(cases.len(), 17);

    let expected_names = [
        "identity_reducer_is_no_op",
        "identity_preserves_pair_atomicity",
        "identity_preserves_message_order",
        "strip_preserves_pair_atomicity",
        "strip_preserves_message_order",
        "strip_is_strictly_idempotent",
        "reduction_blocked_when_response_streaming",
        "reduction_allowed_when_response_terminal",
        "no_orphaned_tool_results_after_strip",
        "reapply_preserves_view_coherent",
        "summarize_retains_straddling_turn",
        "summarize_drops_whole_turns",
        "summarize_oversized_complete_turn",
        "summarize_blocked_when_response_streaming",
        "summarize_cannot_split_a_leading_turn",
        "provider_view_is_idempotent",
        "provider_view_drops_orphaned_result",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(
        cases
            .iter()
            .map(|case| case.name.as_str())
            .collect::<BTreeSet<_>>(),
        expected_names
    );

    for case in cases {
        drive_compaction_reducer_case(case);
    }
}

fn drive_compaction_reducer_case(case: &lean_vocab_test::LeanCompactionReducerCase) {
    assert!(case.legal, "compaction case {} should be legal", case.name);

    let input = compaction_messages_for_case(case);
    assert_eq!(
        input.len(),
        case.pre_message_count,
        "{}: pre_message_count",
        case.name
    );

    let reduced = apply_compaction_reducer(case, input.clone());
    assert_eq!(
        reduced.len(),
        case.post_message_count,
        "{}: post_message_count",
        case.name
    );
    assert_eq!(
        reduced.len(),
        case.retained_count,
        "{}: retained_count",
        case.name
    );
    assert_eq!(
        preserves_pair_closure(&input, &reduced),
        case.preserves_pairs,
        "{}: preserves_pairs",
        case.name
    );
    // Lean's `StrictlyIncreasingMessages` survives a reducer that *drops* rows,
    // so the runtime analogue is "the retained shapes are a subsequence of the
    // input shapes" — not "the shapes are unchanged", which only held while the
    // modelled reducer was `id`.
    assert_eq!(
        is_subsequence(
            &abstract_prompt_view(&reduced),
            &abstract_prompt_view(&input)
        ),
        case.preserves_order,
        "{}: preserves_order",
        case.name
    );

    let structurally_identity = abstract_prompt_view(&input) == abstract_prompt_view(&reduced);
    if case.reducer_is_identity {
        assert!(
            structurally_identity,
            "{}: reducer should be identity on the Lean structural projection",
            case.name
        );
    } else {
        assert_ne!(
            reduced, input,
            "{}: terminal safe reduction should be able to change runtime payloads",
            case.name
        );
    }

    if case.name == "strip_is_strictly_idempotent" {
        let reapplied = gents::compaction::strip_tool_results(reduced.clone()).0;
        // Full payload equality, not just the structural projection: production
        // recovers a stub's recorded facts rather than re-measuring it, which is what
        // `Compaction.strip_idempotent` states.
        assert_eq!(
            reduced, reapplied,
            "{}: strip must be idempotent on runtime payloads, not just shapes",
            case.name
        );
    }

    if case.name == "provider_view_is_idempotent" {
        let reapplied = gents::compaction::provider_view(reduced.clone()).0;
        assert_eq!(
            reduced, reapplied,
            "{}: provider_view must be idempotent on runtime payloads (Compaction.providerView_idempotent)",
            case.name
        );
    }

    if case.name == "reapply_preserves_view_coherent" {
        let reapplied = apply_compaction_reducer(case, reduced.clone());
        assert!(
            pair_closed(&reapplied),
            "{}: reapply preserves pair closure",
            case.name
        );
        assert_eq!(
            abstract_prompt_view(&reduced),
            abstract_prompt_view(&reapplied),
            "{}: reapply preserves ordering projection",
            case.name
        );
    }
}

fn apply_compaction_reducer(
    case: &lean_vocab_test::LeanCompactionReducerCase,
    input: Vec<Message>,
) -> Vec<Message> {
    match case.reducer.as_str() {
        "identity" => input,
        "strip" => gents::compaction::strip_tool_results(input).0,
        "provider_view" => gents::compaction::provider_view(input).0,
        "summarize" => drive_summarize(case, input),
        "any_valid" if case.safe_to_reduce => gents::compaction::strip_tool_results(input).0,
        "any_valid" => input,
        other => panic!("unsupported compaction reducer {other:?} for {}", case.name),
    }
}

/// Drives the summarize reducer through *production*.
///
/// The gate and the boundary are both production's: `safe_to_reduce` and
/// `pair_safe_boundary` are the functions under test, checked against the model
/// rather than reimplemented here. Before #993 this case computed the gate
/// inside the test, so the test could not detect the gate's absence from
/// production at all.
fn drive_summarize(
    case: &lean_vocab_test::LeanCompactionReducerCase,
    input: Vec<Message>,
) -> Vec<Message> {
    let gate_open = if case.safe_to_reduce {
        gents::compaction::safe_to_reduce(&input, &gents::compaction::AllTerminal)
    } else {
        gents::compaction::safe_to_reduce(&input, &gents::compaction::NoneKnown)
    };
    assert_eq!(
        gate_open, case.safe_to_reduce,
        "{}: production safe_to_reduce must agree with the modelled gate",
        case.name
    );

    let boundary = gents::compaction::pair_safe_boundary(&input, case.split_index);
    assert_eq!(
        boundary, case.safe_boundary,
        "{}: production pair_safe_boundary must match Compaction.pairSafeBoundary",
        case.name
    );
    assert_eq!(
        boundary > 0,
        case.gate_open,
        "{}: a boundary that retreats to zero leaves nothing to summarize",
        case.name
    );

    // Checking `pair_safe_boundary` alone would not notice production dropping
    // the call to it from `split_messages_for_summary`. Sweep every budget
    // through the live splitter and require the retained tail to stay
    // pair-closed — the property `summarize_preserves_pairs` states, fenced
    // against the real code path rather than a helper.
    if pair_closed(&input) {
        let total_tokens = gents::compaction::estimate_message_tokens(&input);
        for budget in 0..=total_tokens + 1 {
            let (_, recent) = gents::compaction::split_for_summary(input.clone(), budget);
            assert!(
                pair_closed(&recent),
                "{}: split_for_summary orphaned a tool result at budget {budget}",
                case.name
            );
        }
    }

    if case.name == "summarize_oversized_complete_turn" {
        let (_, recent) = gents::compaction::split_for_summary(input.clone(), 0);
        assert!(
            recent.is_empty(),
            "an oversized complete tail must be summarized rather than over-retained"
        );
    }

    if !gate_open || boundary == 0 {
        return input;
    }
    input.into_iter().skip(boundary).collect()
}

fn is_subsequence(needle: &[String], haystack: &[String]) -> bool {
    let mut cursor = haystack.iter();
    needle
        .iter()
        .all(|item| cursor.any(|candidate| candidate == item))
}

fn compaction_messages_for_case(case: &lean_vocab_test::LeanCompactionReducerCase) -> Vec<Message> {
    match case.pre_message_count {
        0 => Vec::new(),
        1 => vec![compaction_tool_result_message(
            "call-1",
            "large terminal payload",
        )],
        2 => vec![
            compaction_tool_call_message("call-1"),
            compaction_tool_result_message("call-1", "large tool payload"),
        ],
        3 => vec![
            compaction_text_message("user", "first"),
            compaction_tool_call_message("call-1"),
            compaction_tool_result_message("call-1", "large tool payload"),
        ],
        other => panic!(
            "unsupported compaction pre_message_count {other} for {}",
            case.name
        ),
    }
}

fn compaction_text_message(role: &str, text: &str) -> Message {
    match role {
        "user" => Message::User {
            content: vec![UserContent::Text(Text {
                text: text.to_string(),
            })],
        },
        "assistant" => Message::Assistant {
            id: None,
            content: vec![AssistantContent::Text(Text {
                text: text.to_string(),
            })],
        },
        other => panic!("unsupported compaction text role {other:?}"),
    }
}

fn compaction_tool_call_message(call_id: &str) -> Message {
    Message::Assistant {
        id: None,
        content: vec![AssistantContent::ToolCall(ToolCall {
            id: call_id.to_string(),
            call_id: Some(call_id.to_string()),
            function: ToolFunction {
                name: "read".to_string(),
                arguments: json!({ "file_path": "/tmp/compaction-contract.txt" }),
            },
            signature: None,
            additional_params: None,
        })],
    }
}

fn compaction_tool_result_message(call_id: &str, payload: &str) -> Message {
    Message::User {
        content: vec![UserContent::ToolResult(ToolResult {
            id: call_id.to_string(),
            call_id: Some(call_id.to_string()),
            content: vec![ToolResultContent::Text(Text {
                text: payload.to_string(),
            })],
        })],
    }
}

fn preserves_pair_closure(pre: &[Message], post: &[Message]) -> bool {
    !pair_closed(pre) || pair_closed(post)
}

fn pair_closed(messages: &[Message]) -> bool {
    let call_ids = messages
        .iter()
        .flat_map(assistant_tool_call_ids)
        .collect::<HashSet<_>>();
    messages
        .iter()
        .flat_map(user_tool_result_ids)
        .all(|call_id| call_ids.contains(&call_id))
}

fn abstract_prompt_view(messages: &[Message]) -> Vec<String> {
    messages.iter().flat_map(message_shape).collect()
}

fn message_shape(message: &Message) -> Vec<String> {
    match message {
        Message::System { .. } => vec!["system".to_string()],
        Message::Assistant { content, .. } => content
            .iter()
            .map(|item| match item {
                AssistantContent::Text(_) => "assistant:text".to_string(),
                AssistantContent::ToolCall(tool_call) => {
                    format!("assistant:tool_call:{}", tool_call_id(tool_call))
                }
                other => format!("assistant:{other:?}"),
            })
            .collect(),
        Message::User { content } => content
            .iter()
            .map(|item| match item {
                UserContent::Text(_) => "user:text".to_string(),
                UserContent::ToolResult(tool_result) => {
                    format!("user:tool_result:{}", tool_result_id(tool_result))
                }
                other => format!("user:{other:?}"),
            })
            .collect(),
    }
}

fn assistant_tool_call_ids(message: &Message) -> Vec<String> {
    let Message::Assistant { content, .. } = message else {
        return Vec::new();
    };
    content
        .iter()
        .filter_map(|item| match item {
            AssistantContent::ToolCall(tool_call) => Some(tool_call_id(tool_call)),
            _ => None,
        })
        .collect()
}

fn user_tool_result_ids(message: &Message) -> Vec<String> {
    let Message::User { content } = message else {
        return Vec::new();
    };
    content
        .iter()
        .filter_map(|item| match item {
            UserContent::ToolResult(tool_result) => Some(tool_result_id(tool_result)),
            _ => None,
        })
        .collect()
}

fn tool_call_id(tool_call: &ToolCall) -> String {
    tool_call
        .call_id
        .clone()
        .unwrap_or_else(|| tool_call.id.clone())
}

fn tool_result_id(tool_result: &ToolResult) -> String {
    tool_result
        .call_id
        .clone()
        .unwrap_or_else(|| tool_result.id.clone())
}
