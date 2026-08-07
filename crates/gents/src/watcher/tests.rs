use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use super::cooldown::{take_next_eligible_pending_request, PROCESSED_REQUEST_COOLDOWN};
use super::*;

#[test]
fn local_and_relayed_updates_are_both_request_wakeups() {
    fn update_message(is_relay: bool) -> events::Message {
        let block = format!("request-update-{is_relay}").into_bytes();
        let cid = defra_core::block::generate_cid_from_bytes(&block)
            .expect("fixture bytes produce a CID");
        events::Message::update(events::Update::new(
            "request-doc".to_string(),
            cid,
            "request-collection".to_string(),
            block,
            false,
            is_relay,
        ))
    }

    let local = update_message(false);
    let relayed = update_message(true);
    assert!(!request_update_wakeup(&local).unwrap().is_relay);
    assert!(request_update_wakeup(&relayed).unwrap().is_relay);
}

// ---------------------------------------------------------------------------
// validate_agent_request
// ---------------------------------------------------------------------------

#[test]
fn validate_rejects_mixed_parent_linkage_request_id_only() {
    let req = AgentRequest {
        subagent_depth: 1,
        caused_by_parent_request_id: Some("parent-req-1".to_string()),
        caused_by_parent_tool_call_id: None,
        ..base_request()
    };
    assert!(validate_agent_request(&req).is_err());
}

#[test]
fn validate_accepts_steering_request_lineage_without_tool_call_link() {
    let req = AgentRequest {
        subagent_depth: 1,
        metadata: Some(
            r#"{"queue":{"source":"steering","policy":"append","key":null,"queued_after_request_id":null}}"#
                .to_string(),
        ),
        caused_by_parent_request_id: Some("parent-req-1".to_string()),
        caused_by_parent_tool_call_id: None,
        ..base_request()
    };
    assert!(validate_agent_request(&req).is_ok());
}

#[test]
fn validate_rejects_mixed_parent_linkage_tool_call_id_only() {
    let req = AgentRequest {
        subagent_depth: 1,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: Some("parent-tc-1".to_string()),
        ..base_request()
    };
    assert!(validate_agent_request(&req).is_err());
}

#[test]
fn validate_rejects_subagent_depth_zero_with_parent_fields() {
    let req = AgentRequest {
        subagent_depth: 0,
        caused_by_parent_request_id: Some("parent-req-1".to_string()),
        caused_by_parent_tool_call_id: Some("parent-tc-1".to_string()),
        ..base_request()
    };
    assert!(validate_agent_request(&req).is_err());
}

#[test]
fn validate_accepts_top_level_request() {
    let req = AgentRequest {
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
        ..base_request()
    };
    assert!(validate_agent_request(&req).is_ok());
}

#[test]
fn validate_rejects_negative_sampling_seed() {
    let req = AgentRequest {
        seed: Some(-1),
        ..base_request()
    };
    assert_eq!(
        validate_agent_request(&req).unwrap_err().to_string(),
        "agent request seed must be non-negative"
    );
}

#[test]
fn validate_accepts_subagent_request() {
    let req = AgentRequest {
        subagent_depth: 1,
        caused_by_parent_request_id: Some("parent-req-1".to_string()),
        caused_by_parent_tool_call_id: Some("parent-tc-1".to_string()),
        ..base_request()
    };
    assert!(validate_agent_request(&req).is_ok());
}

#[test]
fn agent_request_clone() {
    let req = AgentRequest {
        doc_id: "abc".into(),
        request_id: "req-1".into(),
        agent_did: "did:key:z123".into(),
        requester_did: None,
        behavior_id: Some("general".into()),
        session_id: "sess-1".into(),
        content: "hello".into(),
        temperature: None,
        top_p: None,
        top_k: None,
        seed: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: "2026-03-12T00:00:00Z".into(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    };
    let cloned = req.clone();
    assert_eq!(cloned.doc_id, "abc");
    assert_eq!(cloned.content, "hello");
}

#[test]
fn cooling_down_request_does_not_block_other_pending_sessions() {
    let now = Instant::now();
    let mut processed_request_ids = HashMap::from([("req-1".to_string(), now)]);

    let request = take_next_eligible_pending_request(
        &mut processed_request_ids,
        vec![request("req-1", "sess-1"), request("req-2", "sess-2")],
        now,
    )
    .expect("eligible request");

    assert_eq!(request.request_id, "req-2");
    assert!(processed_request_ids.contains_key("req-1"));
    assert!(processed_request_ids.contains_key("req-2"));
}

#[test]
fn cooled_down_request_becomes_eligible_again() {
    let now = Instant::now();
    let mut processed_request_ids = HashMap::from([("req-1".to_string(), now)]);
    let later = now + PROCESSED_REQUEST_COOLDOWN + Duration::from_millis(1);

    let request = take_next_eligible_pending_request(
        &mut processed_request_ids,
        vec![request("req-1", "sess-1")],
        later,
    )
    .expect("eligible request");

    assert_eq!(request.request_id, "req-1");
    assert_eq!(processed_request_ids.get("req-1").copied(), Some(later));
}

fn base_request() -> AgentRequest {
    request("req-base", "sess-base")
}

fn request(request_id: &str, session_id: &str) -> AgentRequest {
    AgentRequest {
        doc_id: format!("doc-{request_id}"),
        request_id: request_id.to_string(),
        agent_did: "did:key:z123".into(),
        requester_did: None,
        behavior_id: Some("general".into()),
        session_id: session_id.to_string(),
        content: "hello".into(),
        temperature: None,
        top_p: None,
        top_k: None,
        seed: None,
        max_tokens: None,
        metadata: None,
        execution_origin: None,
        created_at: "2026-03-12T00:00:00Z".into(),
        deadline: None,
        subagent_depth: 0,
        caused_by_parent_request_id: None,
        caused_by_parent_tool_call_id: None,
    }
}

// ---------------------------------------------------------------------------
// Integration tests: validate_agent_request wired into
// the query path.  These tests write incoherent AgentRequest rows directly
// into DefraDB and verify that the watcher rejects them at query time.
// ---------------------------------------------------------------------------

async fn test_node() -> Arc<defra_node::EmbeddedNode> {
    Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap())
}

/// Insert an AgentRequest row with an incoherent parent linkage into DefraDB
/// and return its `_docID`.
///
/// `subagent_depth` = 1, `caused_by_parent_request_id` is set, but
/// `caused_by_parent_tool_call_id` is absent — one half of the pair is
/// missing, which the validator must reject.
async fn insert_incoherent_agent_request(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    request_id: &str,
) -> String {
    use crate::graphql::escape_graphql_string;

    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let created_at = chrono::Utc::now().to_rfc3339();

    // subagent_depth = 1 but only caused_by_parent_request_id is set;
    // caused_by_parent_tool_call_id is absent.  This is the coherence
    // violation the validator checks for.
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                session_id: "sess-incoherent",
                content: "test",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                created_at: "{created_at}",
                retry_count: 0,
                max_retries: 0,
                subagent_depth: 1,
                caused_by_parent_request_id: "parent-req-exists"
            }}) {{ _docID }}
        }}"#
    );

    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create_AgentRequest (incoherent) failed: {:?}",
        response.errors
    );

    let lookup = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}, limit: 1) {{
                _docID
            }}
        }}"#
    );
    let response = node.execute(&lookup).await;
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("_docID"))
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .expect("AgentRequest _docID")
}

async fn insert_agent_request_row(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
    request_id: &str,
    session_id: &str,
    status: &str,
    lifecycle_state: &str,
    created_at: &str,
) -> String {
    let request_id = crate::graphql::escape_graphql_string(request_id);
    let agent_did = crate::graphql::escape_graphql_string(agent_did);
    let session_id = crate::graphql::escape_graphql_string(session_id);
    let status = crate::graphql::escape_graphql_string(status);
    let lifecycle_state = crate::graphql::escape_graphql_string(lifecycle_state);
    let created_at = crate::graphql::escape_graphql_string(created_at);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "{agent_did}",
                session_id: "{session_id}",
                retry_parent_request: "",
                retry_root_request: "{request_id}",
                superseded_by_request: "",
                content: "test",
                status: "{status}",
                lifecycle_state: "{lifecycle_state}",
                backend_id: "",
                created_at: "{created_at}",
                retry_count: 0,
                max_retries: 0
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create_AgentRequest failed: {:?}",
        response.errors
    );

    let lookup = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                _docID
            }}
        }}"#
    );
    let response = node.execute(&lookup).await;
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("_docID"))
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .expect("AgentRequest _docID")
}

async fn set_request_terminal_completed(node: &defra_node::EmbeddedNode, doc_id: &str) {
    let doc_id = crate::graphql::escape_graphql_string(doc_id);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{
                    status: "completed",
                    lifecycle_state: "completed"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "update_AgentRequest terminal failed: {:?}",
        response.errors
    );
}

async fn set_request_interrupt_requested_at(
    node: &defra_node::EmbeddedNode,
    doc_id: &str,
    at: &str,
) {
    let doc_id = crate::graphql::escape_graphql_string(doc_id);
    let at = crate::graphql::escape_graphql_string(at);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{ interrupt_requested_at: "{at}" }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "update_AgentRequest interrupt failed: {:?}",
        response.errors
    );
}

async fn mark_as_deprecated_background_completion_wakeup(
    node: &defra_node::EmbeddedNode,
    doc_id: &str,
    session_id: &str,
) {
    let doc_id = crate::graphql::escape_graphql_string(doc_id);
    let metadata = serde_json::json!({
        "queue": {
            "source": "background_completion",
            "policy": "coalesce",
            "key": format!("background_completion:{session_id}"),
            "queued_after_request_id": "legacy-parent"
        }
    })
    .to_string();
    let metadata = crate::graphql::escape_graphql_string(&metadata);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ _docID: {{ _eq: "{doc_id}" }} }},
                input: {{
                    execution_origin: "scheduled",
                    metadata: "{metadata}"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "mark legacy background completion wake failed: {:?}",
        response.errors
    );
}

async fn request_terminal_fields(
    node: &defra_node::EmbeddedNode,
    request_id: &str,
) -> serde_json::Value {
    let request_id = crate::graphql::escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                limit: 1
            ) {{
                status
                lifecycle_state
                failure_reason
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "query AgentRequest terminal fields failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first())
        .cloned()
        .expect("AgentRequest terminal row")
}

#[tokio::test]
async fn pending_requests_skip_queued_same_session_rows_until_claimable() {
    let node = test_node().await;
    crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let agent_did = "did:key:z-watcher-queue";
    let active_doc_id = insert_agent_request_row(
        node.as_ref(),
        agent_did,
        "req-active",
        "sess-queue",
        "processing",
        "processing",
        "2026-03-12T00:00:00Z",
    )
    .await;
    insert_agent_request_row(
        node.as_ref(),
        agent_did,
        "req-queued",
        "sess-queue",
        "pending",
        "pending",
        "2026-03-12T00:00:01Z",
    )
    .await;
    insert_agent_request_row(
        node.as_ref(),
        agent_did,
        "req-other",
        "sess-other",
        "pending",
        "pending",
        "2026-03-12T00:00:02Z",
    )
    .await;

    let watcher = DefraWatcher::new(node.clone(), agent_did);
    let pending = watcher.pending_requests().await.unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|request| request.request_id.as_str())
            .collect::<Vec<_>>(),
        vec!["req-other"]
    );

    set_request_terminal_completed(node.as_ref(), &active_doc_id).await;
    let pending = watcher.pending_requests().await.unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|request| request.request_id.as_str())
            .collect::<Vec<_>>(),
        vec!["req-queued", "req-other"]
    );
}

#[tokio::test]
async fn pending_requests_include_interrupted_queued_rows_for_terminalization() {
    let node = test_node().await;
    crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let agent_did = "did:key:z-watcher-queued-interrupt";
    insert_agent_request_row(
        node.as_ref(),
        agent_did,
        "req-active",
        "sess-queue",
        "processing",
        "processing",
        "2026-03-12T00:00:00Z",
    )
    .await;
    let queued_doc_id = insert_agent_request_row(
        node.as_ref(),
        agent_did,
        "req-queued-interrupt",
        "sess-queue",
        "pending",
        "pending",
        "2026-03-12T00:00:01Z",
    )
    .await;
    set_request_interrupt_requested_at(
        node.as_ref(),
        &queued_doc_id,
        &chrono::Utc::now().to_rfc3339(),
    )
    .await;

    let watcher = DefraWatcher::new(node, agent_did);
    let pending = watcher.pending_requests().await.unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|request| request.request_id.as_str())
            .collect::<Vec<_>>(),
        vec!["req-queued-interrupt"]
    );
}

#[tokio::test]
async fn next_request_ignores_legacy_completion_wake_without_mutating_it() {
    let node = test_node().await;
    crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let agent_did = "did:key:z-watcher-retire-completion-wake";
    let session_id = "sess-retire-completion-wake";
    let wake_doc_id = insert_agent_request_row(
        node.as_ref(),
        agent_did,
        "req-legacy-completion-wake",
        session_id,
        "pending",
        "pending",
        "2026-03-12T00:00:00Z",
    )
    .await;
    mark_as_deprecated_background_completion_wakeup(node.as_ref(), &wake_doc_id, session_id).await;
    insert_agent_request_row(
        node.as_ref(),
        agent_did,
        "req-user",
        session_id,
        "pending",
        "pending",
        "2026-03-12T00:00:01Z",
    )
    .await;

    let mut watcher = DefraWatcher::new(node.clone(), agent_did);
    let request = tokio::time::timeout(Duration::from_secs(2), watcher.next_request())
        .await
        .expect("watcher should not wait on an ignored legacy wake")
        .expect("watcher should remain open")
        .expect("user request should load");
    assert_eq!(request.request_id, "req-user");

    let unchanged = request_terminal_fields(node.as_ref(), "req-legacy-completion-wake").await;
    assert_eq!(unchanged["status"], "pending");
    assert_eq!(unchanged["lifecycle_state"], "pending");
    assert!(unchanged["failure_reason"].is_null());
}

#[tokio::test]
async fn pending_requests_rejects_incoherent_subagent_linkage() {
    let node = test_node().await;
    crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let agent_did = "did:key:z-watcher-coherence-pending";
    insert_incoherent_agent_request(node.as_ref(), agent_did, "req-incoherent-pending").await;

    let watcher = DefraWatcher::new(node.clone(), agent_did);
    let result = watcher.pending_requests().await;
    assert!(
        result.is_err(),
        "pending_requests must fail for incoherent subagent linkage, got: {:?}",
        result
    );
}

#[tokio::test]
async fn try_fetch_request_rejects_incoherent_subagent_linkage() {
    let node = test_node().await;
    crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let agent_did = "did:key:z-watcher-coherence-fetch";
    let doc_id =
        insert_incoherent_agent_request(node.as_ref(), agent_did, "req-incoherent-fetch").await;

    let watcher = DefraWatcher::new(node.clone(), agent_did);
    let result = watcher.try_fetch_request(&doc_id).await;
    assert!(
        result.is_err(),
        "try_fetch_request must fail for incoherent subagent linkage, got: {:?}",
        result
    );
}
