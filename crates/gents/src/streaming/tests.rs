use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use defra_node::EmbeddedNode;
use serde_json::json;

use super::queries::extract_mutation_doc_id;
use super::queries::PersistedResponseState;
use super::*;

async fn build_test_node(name: &str) -> (Arc<EmbeddedNode>, PathBuf) {
    let data_path = std::env::temp_dir().join(format!("streaming-{name}-{}", uuid::Uuid::new_v4()));
    let node = Arc::new(
        defra_node::EmbeddedNode::builder()
            .data_path(&data_path)
            .build()
            .await
            .unwrap(),
    );
    crate::schema::ensure_runtime_schemas(&node).await.unwrap();
    (node, data_path)
}

async fn load_response(
    node: &EmbeddedNode,
    doc_id: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let query = format!(
        r#"{{
                AgentResponse(
                    filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                    limit: 1
                ) {{
                    _docID
                    content
                    reasoning
                    error_message
                    status
                    token_count
                    reasoning_progress_seq
                    completed_at
                    interrupted_at
                }}
            }}"#
    );
    let resp = node.execute(&query).await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    resp.data
        .as_ref()
        .and_then(|data| data.get("AgentResponse"))
        .and_then(|value| value.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_object())
        .cloned()
        .expect("response row")
}

async fn create_processing_request(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
) -> String {
    let created_at = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "did:test:test",
                behavior_id: "general",
                session_id: "{session_id}",
                retry_parent_request: "",
                retry_root_request: "{request_id}",
                superseded_by_request: "",
                content: "hello",
                status: "processing",
                lifecycle_state: "processing",
                backend_id: "",
                execution_origin: "interactive",
                failure_reason: "",
                created_at: "{created_at}",
                retry_count: 0,
                max_retries: 3
            }}) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "create_AgentRequest failed: {:?}",
        resp.errors
    );
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                limit: 1
            ) {{
                _docID
            }}
        }}"#
    );
    let resp = node.execute(&query).await;
    assert!(
        !resp.has_errors(),
        "AgentRequest lookup failed: {:?}",
        resp.errors
    );
    resp.data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| value.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("_docID"))
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned)
        .expect("request _docID")
}

async fn load_request(
    node: &EmbeddedNode,
    request_id: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let query = format!(
        r#"{{
                AgentRequest(
                    filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                    limit: 1
                ) {{
                    _docID
                    status
                    lifecycle_state
                    failure_reason
                }}
            }}"#
    );
    let resp = node.execute(&query).await;
    assert!(!resp.has_errors(), "query failed: {:?}", resp.errors);
    resp.data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| value.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_object())
        .cloned()
        .expect("request row")
}

#[test]
fn stream_status_as_str() {
    assert_eq!(StreamStatus::Streaming.as_str(), "streaming");
    assert_eq!(StreamStatus::Complete.as_str(), "complete");
    assert_eq!(StreamStatus::Error.as_str(), "error");
}

#[test]
fn live_reasoning_preview_keeps_small_reasoning_exact() {
    let mut preview = String::new();

    append_live_reasoning_preview(&mut preview, "first ");
    append_live_reasoning_preview(&mut preview, "second");

    assert_eq!(preview, "first second");
}

#[test]
fn live_reasoning_preview_is_bounded() {
    let mut preview = "prefix".to_string();
    let suffix = "x".repeat(MAX_LIVE_REASONING_BYTES + 128);

    append_live_reasoning_preview(&mut preview, &suffix);

    assert!(preview.len() <= MAX_LIVE_REASONING_BYTES);
    assert!(preview.ends_with("x"));
    assert!(!preview.contains("prefix"));
}

#[test]
fn live_reasoning_preview_keeps_tail_of_oversized_chunk() {
    let mut preview = "old prefix".to_string();
    let chunk = format!("{}tail", "x".repeat(MAX_LIVE_REASONING_BYTES + 128));

    append_live_reasoning_preview(&mut preview, &chunk);

    assert!(preview.len() <= MAX_LIVE_REASONING_BYTES);
    assert!(preview.ends_with("tail"));
    assert!(!preview.contains("old prefix"));
}

#[test]
fn extract_mutation_doc_id_accepts_upsert_create_and_add_shapes() {
    let upsert_data = json!({
        "upsert_AgentResponse": [{ "_docID": "doc-upsert" }]
    });
    assert_eq!(
        extract_mutation_doc_id(&upsert_data, "AgentResponse"),
        Some("doc-upsert")
    );

    let create_data = json!({
        "create_AgentResponse": { "_docID": "doc-create" }
    });
    assert_eq!(
        extract_mutation_doc_id(&create_data, "AgentResponse"),
        Some("doc-create")
    );

    let add_data = json!({
        "add_AgentResponse": [{ "_docID": "doc-add" }]
    });
    assert_eq!(
        extract_mutation_doc_id(&add_data, "AgentResponse"),
        Some("doc-add")
    );
}

#[test]
fn build_finalize_mutation_clears_tail_without_buffer() {
    let mutation = build_finalize_mutation(
        Some(&PersistedResponseState {
            doc_id: "doc-1".to_string(),
            request_id: "req-1".to_string(),
            agent_did: None,
            behavior_id: None,
            session_id: None,
            content: String::new(),
            status: "streaming".to_string(),
            error_message: Some("stale provider error".to_string()),
            token_count: 0,
            interrupted_at: None,
            ..PersistedResponseState::default()
        }),
        "doc-1",
        &StreamStatus::Complete,
        "2026-03-24T00:00:00Z",
        None,
        None,
        RequestFinalizeMode::UpdateRequest,
        "did:test:test",
        false,
    );

    assert!(mutation.contains(r#"status: "complete""#));
    assert!(!mutation.contains("interrupted_at:"));
    assert!(mutation.contains(r#"completed_at: "2026-03-24T00:00:00Z""#));
    assert!(mutation.contains(r#"update_AgentRequest("#));
    assert!(mutation.contains(r#"request_id: { _eq: "req-1" }"#));
    assert!(mutation.contains(r#"lifecycle_state: "completed""#));
    assert!(mutation.contains(r#"failure_reason: """#));
    assert!(
        !mutation.contains(r#"failure_reason: "stale provider error""#),
        "complete finalization must not carry a stale response error into the request"
    );
    assert!(mutation.contains(r#"terminal_redrive_attempts: 0"#));
    // content and reasoning are cleared to "" on finalize (issue #64 contract)
    assert!(mutation.contains(r#"content: """#));
    assert!(mutation.contains(r#"reasoning: """#));
    // token_count is NOT present on the crash-recovery (None) path
    assert!(!mutation.contains("token_count:"));
}

#[test]
fn build_error_finalize_atomically_carries_response_and_request_reason() {
    let mutation = build_finalize_mutation(
        Some(&PersistedResponseState {
            doc_id: "doc-1".to_string(),
            request_id: "req-1".to_string(),
            agent_did: None,
            behavior_id: None,
            session_id: None,
            content: String::new(),
            status: "streaming".to_string(),
            error_message: None,
            token_count: 0,
            interrupted_at: None,
            ..PersistedResponseState::default()
        }),
        "doc-1",
        &StreamStatus::Error,
        "2026-03-24T00:00:00Z",
        None,
        Some("provider failed"),
        RequestFinalizeMode::UpdateRequest,
        "did:test:test",
        false,
    );

    assert!(mutation.contains(r#"status: "error""#));
    assert!(mutation.contains(r#"error_message: "provider failed""#));
    assert!(mutation.contains(r#"lifecycle_state: "failed""#));
    assert!(mutation.contains(r#"failure_reason: "provider failed""#));
    assert!(mutation.contains(r#"agent_did: { _eq: "did:test:test" }"#));
    assert!(
        !mutation.contains("interrupted_at:"),
        "a plain error finalize must never stamp the interrupt marker"
    );
}

#[tokio::test]
async fn finalize_removes_buffer_after_successful_mutation() {
    let (node, data_path) = build_test_node("finalize-success").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;
    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();

    writer.write_tokens(&doc_id, "tail content").await.unwrap();
    let result = writer
        .finalize(&doc_id, StreamStatus::Complete)
        .await
        .unwrap();

    // After finalize, content/reasoning are cleared in the DB (issue #64 contract).
    // StreamResult.content reflects the post-finalize DB state (empty tail).
    assert_eq!(result.content, "");
    assert_eq!(result.token_count, 2);
    assert!(!writer.buffers.lock().await.contains_key(&doc_id));

    let row = load_response(&node, &doc_id).await;
    assert_eq!(
        row.get("content").and_then(|value| value.as_str()),
        Some("")
    );
    assert_eq!(
        row.get("reasoning").and_then(|value| value.as_str()),
        Some("")
    );
    assert_eq!(
        row.get("status").and_then(|value| value.as_str()),
        Some("complete")
    );
    assert_eq!(
        row.get("token_count").and_then(|value| value.as_u64()),
        Some(2)
    );
    assert!(row
        .get("completed_at")
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.is_empty()));

    let request_row = load_request(&node, &request_id).await;
    assert_eq!(
        request_row.get("status").and_then(|value| value.as_str()),
        Some("completed")
    );
    assert_eq!(
        request_row
            .get("lifecycle_state")
            .and_then(|value| value.as_str()),
        Some("completed")
    );

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn finalize_treats_matching_terminal_observation_as_idempotent() {
    let (node, data_path) = build_test_node("finalize-idempotent-terminal").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;
    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();

    writer.write_tokens(&doc_id, "final answer").await.unwrap();
    let first = writer
        .finalize(&doc_id, StreamStatus::Complete)
        .await
        .unwrap();
    // content is cleared on finalize (issue #64 contract)
    assert_eq!(first.content, "");
    assert!(!writer.buffers.lock().await.contains_key(&doc_id));

    let second = writer
        .finalize(&doc_id, StreamStatus::Complete)
        .await
        .unwrap();

    assert_eq!(second.status, StreamStatus::Complete);
    assert_eq!(second.content, "");
    assert_eq!(second.token_count, 2);
    assert!(!writer.buffers.lock().await.contains_key(&doc_id));

    let row = load_response(&node, &doc_id).await;
    assert_eq!(
        row.get("content").and_then(|value| value.as_str()),
        Some("")
    );
    assert_eq!(
        row.get("status").and_then(|value| value.as_str()),
        Some("complete")
    );

    let request_row = load_request(&node, &request_id).await;
    assert_eq!(
        request_row.get("status").and_then(|value| value.as_str()),
        Some("completed")
    );
    assert_eq!(
        request_row
            .get("lifecycle_state")
            .and_then(|value| value.as_str()),
        Some("completed")
    );

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn finalize_keeps_buffer_when_mutation_fails() {
    let (node, data_path) = build_test_node("finalize-failure").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let invalid_doc_id = r#"doc"broken"#.to_string();

    writer.buffers.lock().await.insert(
        invalid_doc_id.clone(),
        StreamBuffer {
            content: "lost tail".to_string(),
            reasoning: String::new(),
            token_count: 2,
            reasoning_progress_seq: 0,
            last_flush_at: Instant::now(),
        },
    );

    let error = writer
        .finalize(&invalid_doc_id, StreamStatus::Error)
        .await
        .unwrap_err();
    assert!(!error.to_string().is_empty());
    assert!(writer.buffers.lock().await.contains_key(&invalid_doc_id));

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn finalize_without_buffer_uses_fallback_mutation() {
    let (node, data_path) = build_test_node("finalize-fallback").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;
    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();

    writer.buffers.lock().await.remove(&doc_id);

    let result = writer.finalize(&doc_id, StreamStatus::Error).await.unwrap();

    assert_eq!(result.content, "");
    assert_eq!(result.token_count, 0);

    let row = load_response(&node, &doc_id).await;
    assert_eq!(
        row.get("content").and_then(|value| value.as_str()),
        Some("")
    );
    assert_eq!(
        row.get("reasoning").and_then(|value| value.as_str()),
        Some("")
    );
    assert_eq!(
        row.get("status").and_then(|value| value.as_str()),
        Some("error")
    );
    assert!(row
        .get("completed_at")
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.is_empty()));

    let request_row = load_request(&node, &request_id).await;
    assert_eq!(
        request_row.get("status").and_then(|value| value.as_str()),
        Some("error")
    );
    assert_eq!(
        request_row
            .get("lifecycle_state")
            .and_then(|value| value.as_str()),
        Some("failed")
    );

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn error_message_persists_on_error_response() {
    let (node, data_path) = build_test_node("error-message").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;
    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();

    writer
        .set_error_message(
            &doc_id,
            "stream liveness timeout: no data received for 120s",
        )
        .await
        .unwrap();
    writer.finalize(&doc_id, StreamStatus::Error).await.unwrap();

    let row = load_response(&node, &doc_id).await;
    assert_eq!(
        row.get("error_message").and_then(|value| value.as_str()),
        Some("stream liveness timeout: no data received for 120s")
    );
    assert_eq!(
        row.get("reasoning").and_then(|value| value.as_str()),
        Some("")
    );

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn write_tokens_fails_when_response_document_is_missing() {
    let (node, data_path) = build_test_node("missing-response-write").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_millis(1));
    let missing_doc_id = "missing-response-doc".to_string();

    writer.buffers.lock().await.insert(
        missing_doc_id.clone(),
        StreamBuffer {
            content: String::new(),
            reasoning: String::new(),
            token_count: 0,
            reasoning_progress_seq: 0,
            last_flush_at: Instant::now() - Duration::from_secs(1),
        },
    );

    let error = writer
        .write_tokens(&missing_doc_id, "partial")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("missing"));
    assert!(writer.buffers.lock().await.contains_key(&missing_doc_id));

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn finalize_rejects_conflicting_terminal_state() {
    let (node, data_path) = build_test_node("finalize-conflict").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;
    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();

    writer.write_tokens(&doc_id, "final answer").await.unwrap();
    writer
        .finalize(&doc_id, StreamStatus::Complete)
        .await
        .unwrap();

    let error = writer
        .finalize(&doc_id, StreamStatus::Error)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot finalize AgentResponse"));

    let row = load_response(&node, &doc_id).await;
    assert_eq!(
        row.get("status").and_then(|value| value.as_str()),
        Some("complete")
    );
    assert_eq!(
        row.get("reasoning").and_then(|value| value.as_str()),
        Some("")
    );

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn write_reasoning_persists_on_response() {
    let (node, data_path) = build_test_node("reasoning-write").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_millis(1));
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;
    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();

    writer
        .write_reasoning(&doc_id, "Need to inspect the repo structure first.")
        .await
        .unwrap();
    writer
        .finalize(&doc_id, StreamStatus::Complete)
        .await
        .unwrap();

    // reasoning is cleared on finalize (issue #64 contract — tail is empty post-finalize)
    let row = load_response(&node, &doc_id).await;
    assert_eq!(
        row.get("reasoning").and_then(|value| value.as_str()),
        Some("")
    );

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn write_reasoning_advances_progress_when_preview_is_unchanged() {
    let (node, data_path) = build_test_node("reasoning-progress-seq").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_millis(0));
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;
    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();

    let saturated = "x".repeat(MAX_LIVE_REASONING_BYTES);
    writer.write_reasoning(&doc_id, &saturated).await.unwrap();
    let before = load_response(&node, &doc_id).await;
    assert_eq!(before["reasoning"].as_str(), Some(saturated.as_str()));
    assert_eq!(before["reasoning_progress_seq"].as_i64(), Some(1));

    writer.write_reasoning(&doc_id, "x").await.unwrap();
    let after = load_response(&node, &doc_id).await;

    assert_eq!(after["reasoning"].as_str(), before["reasoning"].as_str());
    assert_eq!(after["reasoning_progress_seq"].as_i64(), Some(2));

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn begin_rejects_existing_response_document() {
    let (node, data_path) = build_test_node("begin-existing-response").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;

    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();
    writer.finalize(&doc_id, StreamStatus::Error).await.unwrap();

    let error = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already exists"));

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn finalize_existing_request_error_terminalizes_streaming_response_without_buffer() {
    let (node, data_path) = build_test_node("finalize-existing-request-error").await;
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;

    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();

    let recovery_writer =
        DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let finalized = recovery_writer
        .finalize_existing_request_error(&request_id, "shutdown requested during inference stream")
        .await
        .unwrap();

    assert!(finalized);

    let row = load_response(&node, &doc_id).await;
    assert_eq!(
        row.get("status").and_then(|value| value.as_str()),
        Some("error")
    );
    assert_eq!(
        row.get("error_message").and_then(|value| value.as_str()),
        Some("shutdown requested during inference stream")
    );

    let request_row = load_request(&node, &request_id).await;
    assert_eq!(
        request_row.get("status").and_then(|value| value.as_str()),
        Some("error")
    );
    assert_eq!(
        request_row
            .get("lifecycle_state")
            .and_then(|value| value.as_str()),
        Some("failed")
    );

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn finalize_interrupted_response_does_not_rewrite_request_failed() {
    let (node, data_path) = build_test_node("finalize-interrupted-response").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;
    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();

    writer
        .write_tokens(&doc_id, "partial response")
        .await
        .unwrap();
    let interrupted_at = chrono::Utc::now().to_rfc3339();
    assert!(writer
        .write_interrupted_at(&doc_id, &interrupted_at)
        .await
        .unwrap());
    assert!(
        !writer
            .write_interrupted_at(&doc_id, "2099-01-01T00:00:00Z")
            .await
            .unwrap(),
        "interrupted_at must be monotonic once set"
    );

    let result = writer.finalize_interrupted_response(&doc_id).await.unwrap();
    assert_eq!(result.status, StreamStatus::Error);

    let row = load_response(&node, &doc_id).await;
    assert_eq!(
        row.get("status").and_then(|value| value.as_str()),
        Some("error")
    );
    assert_eq!(
        row.get("content").and_then(|value| value.as_str()),
        Some("")
    );
    assert_eq!(
        row.get("error_message").and_then(|value| value.as_str()),
        Some("interrupted"),
        "the durable error text is user-visible in timeline projections and must stay human-readable"
    );
    assert_eq!(
        row.get("interrupted_at").and_then(|value| value.as_str()),
        Some(interrupted_at.as_str()),
        "finalize must not overwrite the earlier, more accurate interrupt stamp"
    );
    assert!(row
        .get("completed_at")
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.is_empty()));

    let request_row = load_request(&node, &request_id).await;
    assert_eq!(
        request_row.get("status").and_then(|value| value.as_str()),
        Some("processing")
    );
    assert_eq!(
        request_row
            .get("lifecycle_state")
            .and_then(|value| value.as_str()),
        Some("processing")
    );

    let _ = fs::remove_dir_all(&data_path);
}

/// The interrupt flow stamps `interrupted_at` before finalize, but that
/// standalone write can be lost. The finalize mutation must then stamp
/// `interrupted_at` itself — it is the durable marker startup/periodic repair
/// uses to classify the owner request as interrupted rather than failed.
#[tokio::test]
async fn finalize_interrupted_response_stamps_missing_interrupted_at() {
    let (node, data_path) = build_test_node("finalize-interrupted-stamp").await;
    let writer = DefraStreamWriter::new(node.clone(), "did:test:test", Duration::from_secs(60));
    let request_id = uuid::Uuid::new_v4().to_string();
    create_processing_request(&node, &request_id, "session-1").await;
    let doc_id = writer
        .begin("session-1", &request_id, "general")
        .await
        .unwrap();
    writer
        .write_tokens(&doc_id, "partial response")
        .await
        .unwrap();

    let result = writer.finalize_interrupted_response(&doc_id).await.unwrap();
    assert_eq!(result.status, StreamStatus::Error);

    let row = load_response(&node, &doc_id).await;
    assert_eq!(
        row.get("error_message").and_then(|value| value.as_str()),
        Some("interrupted")
    );
    assert!(
        row.get("interrupted_at")
            .and_then(|value| value.as_str())
            .is_some_and(|value| !value.trim().is_empty()),
        "finalize must stamp interrupted_at when the earlier standalone write was lost"
    );

    let _ = fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn reset_tail_clears_response_content_and_reasoning() {
    let (node, data_path) = build_test_node("reset-tail").await;
    let writer =
        DefraStreamWriter::new(Arc::clone(&node), "did:test:test", Duration::from_millis(0));
    let _request_doc = create_processing_request(&node, "req-reset", "session-reset").await;

    let doc_id = writer
        .begin("session-reset", "req-reset", "general")
        .await
        .expect("begin");

    writer
        .write_tokens(&doc_id, "hello")
        .await
        .expect("write tokens");
    writer
        .write_reasoning(&doc_id, "thinking")
        .await
        .expect("write reasoning");
    writer.flush_pending(&doc_id).await.expect("flush");

    let pre = load_response(&node, &doc_id).await;
    assert_eq!(pre["content"].as_str(), Some("hello"));
    assert_eq!(pre["reasoning"].as_str(), Some("thinking"));
    let pre_token_count = pre["token_count"].as_u64().expect("token_count present");

    writer.reset_tail(&doc_id).await.expect("reset_tail");

    let post = load_response(&node, &doc_id).await;
    assert_eq!(post["content"].as_str(), Some(""));
    assert_eq!(post["reasoning"].as_str(), Some(""));
    assert_eq!(
        post["token_count"].as_u64().expect("token_count present"),
        pre_token_count,
        "token_count must be cumulative across reset"
    );

    let _ = std::fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn finalize_complete_clears_tail() {
    let (node, data_path) = build_test_node("finalize-tail").await;
    let writer =
        DefraStreamWriter::new(Arc::clone(&node), "did:test:test", Duration::from_millis(0));
    let _ = create_processing_request(&node, "req-fin", "session-fin").await;
    let doc_id = writer
        .begin("session-fin", "req-fin", "general")
        .await
        .expect("begin");
    writer.write_tokens(&doc_id, "world").await.expect("write");
    writer.flush_pending(&doc_id).await.expect("flush");
    writer
        .finalize(&doc_id, StreamStatus::Complete)
        .await
        .expect("finalize");

    let row = load_response(&node, &doc_id).await;
    assert_eq!(row["status"].as_str(), Some("complete"));
    assert_eq!(row["content"].as_str(), Some(""));
    assert_eq!(row["reasoning"].as_str(), Some(""));

    let _ = std::fs::remove_dir_all(&data_path);
}

#[tokio::test]
async fn parallel_stream_flushes_do_not_surface_transaction_conflicts() {
    const WRITER_COUNT: usize = 4;
    const STREAM_COUNT: usize = 24;
    const WRITES_PER_STREAM: usize = 4;

    let (node, data_path) = build_test_node("parallel-stream-flushes").await;
    let writers = (0..WRITER_COUNT)
        .map(|_| {
            Arc::new(DefraStreamWriter::new(
                Arc::clone(&node),
                "did:test:test",
                Duration::ZERO,
            ))
        })
        .collect::<Vec<_>>();
    let mut streams = Vec::with_capacity(STREAM_COUNT);
    for index in 0..STREAM_COUNT {
        let writer = Arc::clone(&writers[index % WRITER_COUNT]);
        let doc_id = writer
            .begin(
                &format!("parallel-session-{index}"),
                &format!("parallel-request-{index}"),
                "general",
            )
            .await
            .expect("begin parallel stream");
        streams.push((writer, doc_id));
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(STREAM_COUNT));
    let mut tasks = tokio::task::JoinSet::new();
    for (index, (writer, doc_id)) in streams.iter().cloned().enumerate() {
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            for write_index in 0..WRITES_PER_STREAM {
                writer
                    .write_reasoning(&doc_id, &format!("r{index}-{write_index} "))
                    .await?;
                writer
                    .write_tokens(&doc_id, &format!("t{index}-{write_index} "))
                    .await?;
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result
            .expect("parallel stream task panicked")
            .expect("parallel stream write must not exhaust conflict retries");
    }

    for (_, doc_id) in streams {
        let response = load_response(&node, &doc_id).await;
        assert_eq!(
            response["reasoning_progress_seq"].as_u64(),
            Some(WRITES_PER_STREAM as u64)
        );
    }

    let _ = fs::remove_dir_all(&data_path);
}

/// A hostile `response_key` containing quote/brace characters must be
/// escaped into the filter's string literal — the query stays well-formed
/// and simply matches nothing (`Ok(None)`), never a GraphQL parse error.
#[tokio::test]
async fn load_response_state_by_key_escapes_hostile_key() {
    let (node, data_path) = build_test_node("hostile-response-key").await;
    let result =
        super::queries::load_response_state_by_key(&node, r#"k" }) { __typename } x("#).await;
    assert!(
        result
            .expect("hostile response_key must not break the query")
            .is_none(),
        "hostile key matches nothing"
    );
    let _ = fs::remove_dir_all(&data_path);
}

/// Same contract for `load_response_state`: the doc_id is a content-addressed
/// id in practice, but the query layer must not rely on that — a hostile
/// value is escaped, not interpolated raw.
#[tokio::test]
async fn load_response_state_escapes_hostile_doc_id() {
    let (node, data_path) = build_test_node("hostile-doc-id").await;
    let result = super::queries::load_response_state(&node, r#"d" }) { __typename } x("#).await;
    assert!(
        result
            .expect("hostile doc_id must not break the query")
            .is_none(),
        "hostile doc_id matches nothing"
    );
    let _ = fs::remove_dir_all(&data_path);
}
