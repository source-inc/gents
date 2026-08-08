use gents::{
    fetch_interrupt_requested_at,
    lifecycle::{ClaimOutcome, ExecutionOrigin},
    tool_call_lifecycle::{AwaitMode, CancelPolicy, ToolCallLifecycle},
    RequestLifecycle,
};
use serde::Deserialize;

use crate::support::snapshots::{
    fetch_message_snapshots_for_session, fetch_tool_call_snapshots_for_session,
};
use crate::support::{
    build_request, conversation_status_by_doc_id, create_conversation_row, create_request,
    create_response_with_content_and_status, create_response_with_status, first_row, test_db,
    test_db_with_duplicate_tolerant_conversations, upsert_conversation, AGENT_DID, AGENT_NAME,
    BACKEND_ID, DEADLINE_SECS,
};

#[derive(Debug, Clone, Deserialize)]
struct StatusRow {
    status: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ResponseStatusRow {
    status: String,
    content: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ConversationRow {
    status: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NotificationDeliveryRow {
    completion_notification_delivered_at: Option<String>,
}

async fn mark_request_interrupted(node: &gents::defra_node::EmbeddedNode, doc_id: &str) {
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{ status: "interrupted", lifecycle_state: "interrupted" }}
            ) {{ _docID }}
        }}"#
    );
    let resp = node.execute(&mutation).await;
    assert!(
        !resp.has_errors(),
        "mark request interrupted failed: {:?}",
        resp.errors
    );
}

#[tokio::test]
async fn recover_all_marks_requests_as_error() {
    let db = test_db("lifecycle-recover-error").await;
    create_request(
        &db.node,
        "stuck-1",
        "session-1",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;

    let report = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.requests_recovered, 1);

    let resp = db
        .node
        .execute(
            r#"{
                AgentRequest(
                    filter: { request_id: { _eq: "stuck-1" } },
                    limit: 1
                ) { status }
            }"#,
        )
        .await;
    assert_eq!(
        first_row::<StatusRow>(&resp, "AgentRequest").status,
        "error"
    );
}

#[tokio::test]
async fn recover_all_preserves_completed_response() {
    let db = test_db("lifecycle-recover-complete").await;
    create_request(
        &db.node,
        "stuck-complete",
        "session-complete",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;
    create_response_with_status(
        &db.node,
        "stuck-complete",
        "stuck-complete",
        "session-complete",
        "complete",
    )
    .await;
    upsert_conversation(
        &db.node,
        "session-complete",
        "stuck-complete",
        "hello",
        "processing",
    )
    .await;

    let report = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.requests_recovered, 1);
    assert_eq!(report.conversations_recovered, 1);

    let request_resp = db
        .node
        .execute(
            r#"{
                AgentRequest(
                    filter: { request_id: { _eq: "stuck-complete" } },
                    limit: 1
                ) { status }
            }"#,
        )
        .await;
    assert_eq!(
        first_row::<StatusRow>(&request_resp, "AgentRequest").status,
        "completed"
    );

    let conversation_resp = db
        .node
        .execute(
            r#"{
                AgentConversation(
                    filter: { session_id: { _eq: "session-complete" } },
                    limit: 1
                ) { status latest_request_id }
            }"#,
        )
        .await;
    assert_eq!(
        first_row::<ConversationRow>(&conversation_resp, "AgentConversation").status,
        "completed"
    );
}

#[tokio::test]
async fn recover_all_marks_partial_streams_error_and_reactivates_conversation() {
    let db = test_db("lifecycle-recover-partial").await;
    create_request(
        &db.node,
        "stuck-partial",
        "session-partial",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;
    create_response_with_content_and_status(
        &db.node,
        "stuck-partial",
        "stuck-partial",
        "session-partial",
        "partial reply",
        "streaming",
    )
    .await;
    upsert_conversation(
        &db.node,
        "session-partial",
        "stuck-partial",
        "hello",
        "processing",
    )
    .await;

    let report = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.responses_recovered, 1);
    assert_eq!(report.requests_recovered, 1);
    assert_eq!(report.conversations_recovered, 1);

    let response_resp = db
        .node
        .execute(
            r#"{
                AgentResponse(
                    filter: { response_key: { _eq: "stuck-partial" } },
                    limit: 1
                ) { status content }
            }"#,
        )
        .await;
    let response = first_row::<ResponseStatusRow>(&response_resp, "AgentResponse");
    assert_eq!(response.status, "error");
    assert!(response.content.contains("[Response interrupted"));

    let conversation_resp = db
        .node
        .execute(
            r#"{
                AgentConversation(
                    filter: { session_id: { _eq: "session-partial" } },
                    limit: 1
                ) { status latest_request_id }
            }"#,
        )
        .await;
    assert_eq!(
        first_row::<ConversationRow>(&conversation_resp, "AgentConversation").status,
        "active"
    );
}

#[tokio::test]
async fn recover_all_creates_error_response_when_response_doc_is_missing() {
    let db = test_db("lifecycle-recover-missing").await;
    create_request(
        &db.node,
        "stuck-missing",
        "session-missing",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;
    upsert_conversation(
        &db.node,
        "session-missing",
        "stuck-missing",
        "hello",
        "processing",
    )
    .await;

    let report = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.responses_recovered, 1);
    assert_eq!(report.requests_recovered, 1);
    assert_eq!(report.conversations_recovered, 1);

    let response_resp = db
        .node
        .execute(
            r#"{
                AgentResponse(
                    filter: { response_key: { _eq: "stuck-missing" } },
                    limit: 1
                ) { status content }
            }"#,
        )
        .await;
    let response = first_row::<ResponseStatusRow>(&response_resp, "AgentResponse");
    assert_eq!(response.status, "error");
    assert!(response
        .content
        .contains("daemon restarted before response could be generated"));
}

#[tokio::test]
async fn recover_all_times_out_expired_running_tool_calls() {
    let db = test_db("tool-call-recover-timeout").await;
    create_request(
        &db.node,
        "tool-timeout-req",
        "tool-timeout-session",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;

    let mut lifecycle = ToolCallLifecycle::new(
        db.node.clone(),
        "tool-timeout-req".to_string(),
        "tool-timeout-session".to_string(),
        "did:test:test".to_string(),
        "tool-timeout-call".to_string(),
        1,
        "never".to_string(),
        "{}".to_string(),
        chrono::Utc::now() - chrono::Duration::seconds(1),
    );
    lifecycle.start_running().await.unwrap();

    let report = ToolCallLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.tool_calls_recovered, 1);

    let snapshots = fetch_tool_call_snapshots_for_session(&db.node, "tool-timeout-session").await;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].lifecycle_state.as_deref(), Some("timedOut"));
    assert_eq!(snapshots[0].cancel_cause.as_deref(), Some("deadline"));
    assert_eq!(snapshots[0].status, "completed");
    assert!(snapshots[0].result.contains("deadline exceeded"));
}

#[tokio::test]
async fn recover_all_repairs_terminal_background_tool_notification_once() {
    let db = test_db("tool-call-repair-notification").await;
    create_request(
        &db.node,
        "tool-notification-req",
        "tool-notification-session",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;

    let mut lifecycle = ToolCallLifecycle::new_background_tool(
        db.node.clone(),
        "tool-notification-req".to_string(),
        "tool-notification-session".to_string(),
        AGENT_DID.to_string(),
        "tool-notification-call".to_string(),
        1,
        "lookup".to_string(),
        "{}".to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
    );
    lifecycle.start_running().await.unwrap();
    assert!(lifecycle
        .bridge_complete("durable result".to_string())
        .await
        .unwrap());

    assert!(
        fetch_message_snapshots_for_session(&db.node, "tool-notification-session")
            .await
            .is_empty(),
        "the test precondition is a terminal tool with a missing notification"
    );

    let first = ToolCallLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(first.notifications_repaired, 1);
    let second = ToolCallLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(second.notifications_repaired, 0);

    let messages = fetch_message_snapshots_for_session(&db.node, "tool-notification-session").await;
    assert_eq!(messages.len(), 1, "repair must be durably idempotent");
    assert!(messages[0].content.contains("durable result"));

    let response = db
        .node
        .execute(
            r#"{
                AgentToolCall(
                    filter: { tool_call_id: { _eq: "tool-notification-call" } },
                    limit: 1
                ) { completion_notification_delivered_at }
            }"#,
        )
        .await;
    let row = first_row::<NotificationDeliveryRow>(&response, "AgentToolCall");
    assert!(
        row.completion_notification_delivered_at.is_some(),
        "successful notification append must advance the delivery marker"
    );
}

#[tokio::test]
async fn recover_all_cancels_running_tool_call_for_interrupted_parent_only() {
    let db = test_db("tool-call-recover-cancel").await;
    let interrupted_doc = create_request(
        &db.node,
        "tool-cancel-req",
        "tool-cancel-session",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;
    create_request(
        &db.node,
        "tool-other-req",
        "tool-other-session",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;
    mark_request_interrupted(&db.node, &interrupted_doc).await;

    let future_deadline = chrono::Utc::now() + chrono::Duration::minutes(5);
    let mut cancelled = ToolCallLifecycle::new(
        db.node.clone(),
        "tool-cancel-req".to_string(),
        "tool-cancel-session".to_string(),
        "did:test:test".to_string(),
        "tool-cancel-call".to_string(),
        1,
        "slow".to_string(),
        "{}".to_string(),
        future_deadline,
    );
    cancelled.start_running().await.unwrap();

    let mut unrelated = ToolCallLifecycle::new(
        db.node.clone(),
        "tool-other-req".to_string(),
        "tool-other-session".to_string(),
        "did:test:test".to_string(),
        "tool-other-call".to_string(),
        1,
        "slow".to_string(),
        "{}".to_string(),
        future_deadline,
    );
    unrelated.start_running().await.unwrap();

    let report = ToolCallLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.tool_calls_recovered, 1);

    let cancelled_snapshots =
        fetch_tool_call_snapshots_for_session(&db.node, "tool-cancel-session").await;
    assert_eq!(
        cancelled_snapshots[0].lifecycle_state.as_deref(),
        Some("cancelled")
    );
    assert_eq!(
        cancelled_snapshots[0].cancel_cause.as_deref(),
        Some("interrupted")
    );

    let unrelated_snapshots =
        fetch_tool_call_snapshots_for_session(&db.node, "tool-other-session").await;
    assert_eq!(
        unrelated_snapshots[0].lifecycle_state.as_deref(),
        Some("running"),
        "unrelated running tool call should not be swept"
    );
}

#[tokio::test]
async fn recover_all_cascades_interrupted_parent_to_subagent_child() {
    let db = test_db("tool-call-recover-cascade").await;
    let interrupted_doc = create_request(
        &db.node,
        "tool-cascade-parent",
        "tool-cascade-parent-session",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;
    create_request(
        &db.node,
        "tool-cascade-child",
        "tool-cascade-child-session",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;
    mark_request_interrupted(&db.node, &interrupted_doc).await;

    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "tool-cascade-parent".to_string(),
        "tool-cascade-parent-session".to_string(),
        "did:test:test".to_string(),
        "tool-cascade-call".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        "tool-cascade-child".to_string(),
        "did:test:target".to_string(),
    );
    lifecycle.start_running().await.unwrap();

    let report = ToolCallLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.tool_calls_recovered, 1);

    let snapshots =
        fetch_tool_call_snapshots_for_session(&db.node, "tool-cascade-parent-session").await;
    assert_eq!(snapshots[0].lifecycle_state.as_deref(), Some("cancelled"));

    let child_interrupt = fetch_interrupt_requested_at(&db.node, "tool-cascade-child")
        .await
        .unwrap();
    assert!(
        child_interrupt.is_some(),
        "cascade recovery should latch child interrupt_requested_at"
    );
}

#[tokio::test]
async fn recover_all_leaves_detached_subagent_tool_running() {
    let db = test_db("tool-call-recover-detach").await;
    let interrupted_doc = create_request(
        &db.node,
        "tool-detach-parent",
        "tool-detach-parent-session",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;
    create_request(
        &db.node,
        "tool-detach-child",
        "tool-detach-child-session",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;
    mark_request_interrupted(&db.node, &interrupted_doc).await;

    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        "tool-detach-parent".to_string(),
        "tool-detach-parent-session".to_string(),
        "did:test:test".to_string(),
        "tool-detach-call".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        CancelPolicy::Detach,
        "tool-detach-child".to_string(),
        "did:test:target".to_string(),
    );
    lifecycle.start_running().await.unwrap();

    let report = ToolCallLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.tool_calls_recovered, 0);

    let snapshots =
        fetch_tool_call_snapshots_for_session(&db.node, "tool-detach-parent-session").await;
    assert_eq!(
        snapshots[0].lifecycle_state.as_deref(),
        Some("running"),
        "detached bridge tool should remain running for the subagent runtime to reconcile"
    );

    let child_interrupt = fetch_interrupt_requested_at(&db.node, "tool-detach-child")
        .await
        .unwrap();
    assert!(
        child_interrupt.is_none(),
        "detached recovery should not interrupt the child request"
    );
}

/// #693: a store carrying two `AgentConversation` docs for one `session_id`.
///
/// Before the fix this failed twice over: the `session_id`-filtered upsert was
/// refused by DefraDB (`cannot upsert multiple matching documents`), so *both*
/// docs stayed `processing` — and the sweep still reported
/// `conversations_recovered == 2`, because it counted the rows it attempted
/// rather than the writes that landed. A fully failed pass logged as healthy.
///
/// The duplicate condition is real: `session_id` is unique-indexed in the
/// shipped schema, but DefraDB cannot add an index to an existing collection,
/// so hosts whose collection predates the index carry duplicates permanently
/// (replication can mint them too). Four production stores were held back on old
/// releases by this.
#[tokio::test]
async fn recover_all_recovers_canonical_conversation_of_a_duplicated_session() {
    let db = test_db_with_duplicate_tolerant_conversations("lifecycle-recovery-duplicate").await;

    create_request(
        &db.node,
        "dup-req",
        "session-dup",
        "processing",
        "2026-03-23T00:00:00Z",
    )
    .await;
    create_response_with_status(&db.node, "dup-req", "dup-req", "session-dup", "complete").await;

    let canonical = create_conversation_row(
        &db.node,
        "session-dup",
        "Real conversation",
        "hello",
        "processing",
        "2026-03-23T00:00:00Z",
        "2026-03-23T00:05:00Z",
        "dup-req",
    )
    .await;
    let duplicate = create_conversation_row(
        &db.node,
        "session-dup",
        "",
        "",
        "processing",
        "2026-03-22T00:00:00Z",
        "2026-03-22T00:00:00Z",
        "",
    )
    .await;
    assert_ne!(canonical, duplicate, "the seed must produce two documents");

    let report = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .expect("recovery must not fail on a duplicate store");

    assert_eq!(report.conversations_recovered, 1);
    assert_eq!(report.conversations_failed, 0);
    assert_eq!(report.duplicate_conversation_sessions, 1);

    assert_eq!(
        conversation_status_by_doc_id(&db.node, &canonical).await,
        "completed",
    );
    assert_eq!(
        conversation_status_by_doc_id(&db.node, &duplicate).await,
        "completed",
    );

    let second = RequestLifecycle::recover_all(&db.node, AGENT_DID)
        .await
        .expect("second pass");
    assert_eq!(second.conversations_recovered, 0);
    assert_eq!(second.conversations_failed, 0);
}

#[tokio::test]
async fn live_request_path_survives_a_duplicated_session() {
    let db = test_db_with_duplicate_tolerant_conversations("lifecycle-duplicate-live").await;

    let canonical = create_conversation_row(
        &db.node,
        "session-live",
        "Real conversation",
        "hello",
        "active",
        "2026-03-23T00:00:00Z",
        "2026-03-23T00:05:00Z",
        "req-old",
    )
    .await;
    let duplicate = create_conversation_row(
        &db.node,
        "session-live",
        "",
        "",
        "active",
        "2026-03-22T00:00:00Z",
        "2026-03-22T00:00:00Z",
        "",
    )
    .await;

    let doc_id = create_request(
        &db.node,
        "req-new",
        "session-live",
        "pending",
        "2026-03-24T00:00:00Z",
    )
    .await;
    let request = build_request(
        doc_id,
        "req-new".to_string(),
        "session-live".to_string(),
        "2026-03-24T00:00:00Z".to_string(),
    );
    let mut lifecycle = RequestLifecycle::new_with_execution_binding(
        db.node.clone(),
        AGENT_NAME,
        AGENT_DID,
        request,
        DEADLINE_SECS,
        ExecutionOrigin::Interactive,
        BACKEND_ID,
    );
    assert_eq!(
        lifecycle.claim_without_identity_for_test().await.unwrap(),
        ClaimOutcome::Claimed
    );

    lifecycle
        .prepare_session_with_identity()
        .await
        .expect("live conversation write must survive a duplicate store");

    assert_eq!(
        conversation_status_by_doc_id(&db.node, &canonical).await,
        "processing",
    );
    assert_eq!(
        conversation_status_by_doc_id(&db.node, &duplicate).await,
        "active",
    );
}
