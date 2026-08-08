use gents::lifecycle::ClaimOutcome;
use gents::watcher::AgentRequest;
use gents::RequestLifecycle;
use serde::Deserialize;

use crate::support::{
    create_request, create_response, first_row, test_db, upsert_conversation, AGENT_DID, AGENT_NAME,
};

#[derive(Debug, Clone, Deserialize)]
struct ConversationRow {
    status: String,
    latest_request_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProgressRow {
    progress_seq: i64,
}

#[tokio::test]
async fn complete_does_not_overwrite_conversation_for_newer_request() {
    let db = test_db("lifecycle-stale-complete").await;
    let session_id = "session-stale";
    let first_doc_id = create_request(
        &db.node,
        "req-first",
        session_id,
        "pending",
        "2026-03-23T00:00:00Z",
    )
    .await;
    let first_request = AgentRequest {
        doc_id: first_doc_id,
        request_id: "req-first".into(),
        agent_did: AGENT_DID.into(),
        requester_did: None,
        behavior_id: Some(AGENT_NAME.into()),
        session_id: session_id.into(),
        content: "hello".into(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: "2026-03-23T00:00:00Z".into(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };
    let mut lifecycle = RequestLifecycle::new_with_agent_did(
        db.node.clone(),
        AGENT_NAME,
        AGENT_DID,
        first_request,
        300,
    );
    assert_eq!(
        lifecycle.claim_without_identity_for_test().await.unwrap(),
        ClaimOutcome::Claimed
    );

    upsert_conversation(&db.node, session_id, "req-second", "second", "processing").await;

    lifecycle.complete().await.unwrap();

    let conversation_resp = db
        .node
        .execute(
            r#"{
                AgentConversation(
                    filter: { session_id: { _eq: "session-stale" } },
                    limit: 1
                ) { status latest_request_id }
            }"#,
        )
        .await;
    let conversation = first_row::<ConversationRow>(&conversation_resp, "AgentConversation");
    assert_eq!(
        conversation.latest_request_id.as_deref(),
        Some("req-second")
    );
    assert_eq!(conversation.status, "processing");
}

#[tokio::test]
async fn advance_increments_progress_seq() {
    let db = test_db("lifecycle-advance").await;
    let request_doc_id = create_request(
        &db.node,
        "req-1",
        "session-1",
        "pending",
        "2026-03-23T00:00:00Z",
    )
    .await;
    let response_doc_id = create_response(&db.node, "resp-1").await;
    let request = AgentRequest {
        doc_id: request_doc_id,
        request_id: "req-1".into(),
        agent_did: AGENT_DID.into(),
        requester_did: None,
        behavior_id: Some(AGENT_NAME.into()),
        session_id: "session-1".into(),
        content: "hello".into(),
        temperature: None,
        top_p: None,
        top_k: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: "2026-03-23T00:00:00Z".into(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };

    let mut lifecycle =
        RequestLifecycle::new_with_agent_did(db.node.clone(), AGENT_NAME, AGENT_DID, request, 300);
    assert_eq!(
        lifecycle.claim_without_identity_for_test().await.unwrap(),
        ClaimOutcome::Claimed
    );
    lifecycle.set_response_doc_id(&response_doc_id).unwrap();
    lifecycle.advance().await.unwrap();
    lifecycle.advance().await.unwrap();
    lifecycle.advance().await.unwrap();

    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{ _docID: {{ _eq: "{response_doc_id}" }} }},
                limit: 1
            ) {{ progress_seq }}
        }}"#
    );
    let resp = db.node.execute(&query).await;
    assert_eq!(
        first_row::<ProgressRow>(&resp, "AgentResponse").progress_seq,
        3
    );
}
