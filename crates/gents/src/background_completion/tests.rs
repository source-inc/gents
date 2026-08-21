use super::*;

const LOCAL_DID: &str = "did:test:local-owner";
const FOREIGN_DID: &str = "did:test:foreign-owner";

#[test]
fn recognizes_only_reserved_background_completion_notification_message_keys() {
    let message_key = background_completion_notification_message_key("child-1", "subagent");
    assert!(is_background_completion_notification_message_key(
        &message_key
    ));
    assert!(!is_background_completion_notification_message_key(
        BACKGROUND_COMPLETION_WAKE_PROMPT
    ));
}

#[test]
fn canonical_wake_prompt_is_a_minimal_generic_control_signal() {
    assert_eq!(
        BACKGROUND_COMPLETION_WAKE_PROMPT,
        "Review the new background completion results and continue the task if needed."
    );
}

async fn test_node() -> Arc<EmbeddedNode> {
    let node = Arc::new(EmbeddedNode::builder().build().await.unwrap());
    crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();
    node
}

async fn exec(node: &EmbeddedNode, statement: &str) {
    let response = node.execute(statement).await;
    assert!(
        !response.has_errors(),
        "GraphQL errors: {:?}",
        response.errors
    );
}

async fn write_parent_request(node: &EmbeddedNode, request_id: &str, agent_did: &str) -> String {
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "{agent_did}",
                behavior_id: "parent",
                session_id: "session-{request_id}",
                content: "parent",
                status: "processing",
                lifecycle_state: "processing",
                created_at: "2026-05-15T00:00:00Z",
                deadline: "2026-05-15T00:05:00Z"
            }}) {{ _docID }}
        }}"#
    );
    exec(node, &mutation).await;
    crate::request_binding::resolve_request_doc_id(node, request_id)
        .await
        .expect("resolve parent request document")
        .expect("created parent request document")
}

async fn write_bridge(
    node: &EmbeddedNode,
    request_id: &str,
    request_doc_id: &str,
    tool_call_id: &str,
    extra_fields: &str,
) {
    let mutation = format!(
        r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "{request_id}:{tool_call_id}",
                request_id: "{request_id}",
                request_doc_id: "{request_doc_id}",
                session_id: "session-{request_id}",
                message_sequence: 1,
                tool_name: "spawn_subagent",
                tool_call_id: "{tool_call_id}",
                args: "{{}}",
                status: "running",
                lifecycle_state: "running",
                started_at: "2026-05-15T00:00:00Z",
                deadline_at: "2026-05-15T00:05:00Z",
                await_mode: "background",
                cancel_policy: "cascade",
                child_request_id: "child-{tool_call_id}"
                {extra_fields}
            }}) {{ _docID }}
        }}"#
    );
    exec(node, &mutation).await;
}

#[derive(Debug, Deserialize)]
struct ToolRow {
    lifecycle_state: Option<String>,
    unclaimed_deadline_at: Option<String>,
    cancel_pending_remote_ack: Option<bool>,
    stuck_since: Option<String>,
}

async fn load_tool(node: &EmbeddedNode, tool_call_id: &str) -> ToolRow {
    let query = format!(
        r#"{{
            AgentToolCall(filter: {{ tool_call_id: {{ _eq: "{tool_call_id}" }} }}, limit: 1) {{
                lifecycle_state
                unclaimed_deadline_at
                cancel_pending_remote_ack
                stuck_since
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "query errors: {:?}",
        response.errors
    );
    first_row(response.data.as_ref(), "AgentToolCall").expect("tool row")
}

#[tokio::test]
async fn unclaimed_reconciler_skips_foreign_parent_bridge() {
    let node = test_node().await;
    let parent_doc_id =
        write_parent_request(node.as_ref(), "parent-foreign-unclaimed", FOREIGN_DID).await;
    write_bridge(
        node.as_ref(),
        "parent-foreign-unclaimed",
        &parent_doc_id,
        "foreign-unclaimed",
        r#", unclaimed_deadline_at: "2020-01-01T00:00:00Z""#,
    )
    .await;

    let outcomes = reconcile_unclaimed_cross_deployment_spawns(node.clone(), LOCAL_DID)
        .await
        .unwrap();
    assert!(outcomes.is_empty());

    let tool = load_tool(node.as_ref(), "foreign-unclaimed").await;
    assert_eq!(tool.lifecycle_state.as_deref(), Some("running"));
    assert!(tool.unclaimed_deadline_at.is_some());
}

#[tokio::test]
async fn cancel_ack_observer_skips_foreign_parent_bridge() {
    let node = test_node().await;
    let parent_doc_id =
        write_parent_request(node.as_ref(), "parent-foreign-cancel", FOREIGN_DID).await;
    write_bridge(
        node.as_ref(),
        "parent-foreign-cancel",
        &parent_doc_id,
        "foreign-cancel",
        r#", cancel_cascade_intent_at: "2020-01-01T00:00:00Z", cancel_pending_remote_ack: true"#,
    )
    .await;

    let outcomes = observe_cancel_cascade_ack(node.clone(), LOCAL_DID)
        .await
        .unwrap();
    assert!(outcomes.is_empty());

    let tool = load_tool(node.as_ref(), "foreign-cancel").await;
    assert_eq!(tool.cancel_pending_remote_ack, Some(true));
    assert!(tool.stuck_since.is_none());
}

#[tokio::test]
async fn unclaimed_reconciler_skips_bridge_whose_parent_is_remote_only() {
    let node = test_node().await;
    write_bridge(
        node.as_ref(),
        "parent-remote-unclaimed",
        "bae-remote-parent-unclaimed",
        "remote-unclaimed",
        r#", unclaimed_deadline_at: "2020-01-01T00:00:00Z""#,
    )
    .await;

    let outcomes = reconcile_unclaimed_cross_deployment_spawns(node.clone(), LOCAL_DID)
        .await
        .expect("remote-only parent should be an ownership-negative result");
    assert!(outcomes.is_empty());

    let tool = load_tool(node.as_ref(), "remote-unclaimed").await;
    assert_eq!(tool.lifecycle_state.as_deref(), Some("running"));
    assert!(tool.unclaimed_deadline_at.is_some());
}

#[tokio::test]
async fn cancel_ack_observer_skips_bridge_whose_parent_is_remote_only() {
    let node = test_node().await;
    write_bridge(
        node.as_ref(),
        "parent-remote-cancel",
        "bae-remote-parent-cancel",
        "remote-cancel",
        r#", cancel_cascade_intent_at: "2020-01-01T00:00:00Z", cancel_pending_remote_ack: true"#,
    )
    .await;

    let outcomes = observe_cancel_cascade_ack(node.clone(), LOCAL_DID)
        .await
        .expect("remote-only parent should be an ownership-negative result");
    assert!(outcomes.is_empty());

    let tool = load_tool(node.as_ref(), "remote-cancel").await;
    assert_eq!(tool.cancel_pending_remote_ack, Some(true));
    assert!(tool.stuck_since.is_none());
}
