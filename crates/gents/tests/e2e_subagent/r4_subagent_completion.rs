use std::time::Duration;

use gents::background_completion::{
    project_background_subagent_completion, BackgroundCompletionOutcome,
};
use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::llm::message::{
    AssistantContent, Message, Text, ToolCall, ToolFunction, ToolResult, ToolResultContent,
    UserContent,
};
use gents::llm::ToolCallHookAction;
use gents::tool_call_lifecycle::{
    create_subagent_request_with_request_id_for_test, AwaitMode, CancelPolicy, ToolCallLifecycle,
};
use gents::{
    fetch_interrupt_requested_at, upsert_agent_behavior, upsert_tool_selection,
    AgentBehaviorDocument, DefraSessionHook, FailurePolicy, ToolSelectionDocument,
};
use serde::Deserialize;
use serde_json::json;

use crate::support::fixtures::spawn_subagent_source;
use crate::support::{first_row, test_db};

const AGENT_DID: &str = "did:test:r4-subagent-completion";
const PARENT_BEHAVIOR_ID: &str = "r4-completion-parent";
const CHILD_BEHAVIOR_ID: &str = "r4-completion-child";
#[derive(Debug, Deserialize)]
struct RequestSessionRow {
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct ToolCallRow {
    result: Option<String>,
    lifecycle_state: Option<String>,
    await_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageRow {
    sequence: u32,
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ChildRequestStateRow {
    status: Option<String>,
    lifecycle_state: Option<String>,
    failure_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResponseStateRow {
    status: Option<String>,
    content: Option<String>,
    error_message: Option<String>,
}

async fn setup_fixture(test_name: &str) -> (crate::support::TestDb, String, String) {
    let db = test_db(test_name).await;
    upsert_tool_selection(
        db.node.as_ref(),
        &ToolSelectionDocument {
            selection_id: format!("{test_name}-tools"),
            agent_did: AGENT_DID.to_string(),
            subagent_targets: Some(vec![gents::subagent_target_entry(
                CHILD_BEHAVIOR_ID,
                AGENT_DID,
                CHILD_BEHAVIOR_ID,
                None,
            )]),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    upsert_agent_behavior(
        db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: PARENT_BEHAVIOR_ID.to_string(),
            agent_did: AGENT_DID.to_string(),
            display_name: Some("R4 completion parent".to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: Some(format!("{test_name}-tools")),
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            enabled: true,
            created_at: Some("2026-05-12T00:00:00Z".to_string()),
        },
    )
    .await
    .unwrap();
    upsert_agent_behavior(
        db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: CHILD_BEHAVIOR_ID.to_string(),
            agent_did: AGENT_DID.to_string(),
            display_name: Some("R4 completion child".to_string()),
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
            created_at: Some("2026-05-12T00:00:01Z".to_string()),
        },
    )
    .await
    .unwrap();

    let session_id = format!("{test_name}-parent-session");
    let request_id = format!("{test_name}-parent-request");
    create_parent_request(db.node.as_ref(), &request_id, &session_id).await;
    (db, session_id, request_id)
}

async fn create_parent_request(node: &EmbeddedNode, request_id: &str, session_id: &str) {
    let request_id = escape_graphql_string(request_id);
    let session_id = escape_graphql_string(session_id);
    let behavior_id = escape_graphql_string(PARENT_BEHAVIOR_ID);
    let agent_did = escape_graphql_string(AGENT_DID);
    let now = chrono::Utc::now().to_rfc3339();
    let deadline = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
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
                created_at: "{now}",
                deadline: "{deadline}",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: 0
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create parent AgentRequest failed: {:?}",
        response.errors
    );
}

async fn create_child_and_bridge(
    node: &std::sync::Arc<EmbeddedNode>,
    parent_request_id: &str,
    parent_session_id: &str,
    tool_call_id: &str,
    await_mode: AwaitMode,
    message_sequence: u32,
) -> (String, String) {
    let child_request_id = format!("{parent_request_id}-{tool_call_id}-child");
    create_subagent_request_with_request_id_for_test(
        node.as_ref(),
        child_request_id.clone(),
        parent_request_id.to_string(),
        tool_call_id.to_string(),
        0,
        AGENT_DID.to_string(),
        CHILD_BEHAVIOR_ID.to_string(),
        format!("prompt for {tool_call_id}"),
        Some(chrono::Utc::now() + chrono::Duration::minutes(4)),
    )
    .await
    .unwrap();
    let child_session_id = child_session_id(node.as_ref(), &child_request_id).await;

    let mut lifecycle = ToolCallLifecycle::new_subagent(
        node.clone(),
        parent_request_id.to_string(),
        parent_session_id.to_string(),
        AGENT_DID.to_string(),
        tool_call_id.to_string(),
        message_sequence,
        "spawn_subagent".to_string(),
        serde_json::json!({
            "name": CHILD_BEHAVIOR_ID,
            "prompt": format!("prompt for {tool_call_id}"),
            "await_mode": await_mode.as_str()
        })
        .to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
        await_mode,
        CancelPolicy::Cascade,
        child_request_id.clone(),
        AGENT_DID.to_string(),
    );
    lifecycle.start_running().await.unwrap();

    (child_request_id, child_session_id)
}

async fn child_session_id(node: &EmbeddedNode, child_request_id: &str) -> String {
    let child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{child_request_id}" }} }},
                limit: 1
            ) {{ session_id }}
        }}"#
    );
    first_row::<RequestSessionRow>(&node.execute(&query).await, "AgentRequest").session_id
}

async fn wait_for_child_for_tool(node: &EmbeddedNode, tool_call_id: &str) -> (String, String) {
    let escaped = escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    caused_by_parent_tool_call_id: {{ _eq: "{escaped}" }}
                }},
                limit: 1
            ) {{ request_id session_id }}
        }}"#
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let response = node.execute(&query).await;
        if let Some(row) =
            crate::support::first_optional_row::<ChildForToolRow>(&response, "AgentRequest")
        {
            return (row.request_id, row.session_id);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for child AgentRequest for tool call {tool_call_id}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(Debug, Deserialize)]
struct ChildForToolRow {
    request_id: String,
    session_id: String,
}

fn skip_reason(action: ToolCallHookAction) -> String {
    let ToolCallHookAction::Skip { reason } = action else {
        panic!("expected Skip action, got {action:?}");
    };
    reason
}

async fn persist_child_completion(
    node: &EmbeddedNode,
    child_request_id: &str,
    child_session_id: &str,
    final_response: &str,
) {
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let update_request = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_child_request_id}" }} }},
                input: {{ status: "completed", lifecycle_state: "completed" }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&update_request).await;
    assert!(
        !response.has_errors(),
        "update child AgentRequest completed failed: {:?}",
        response.errors
    );

    let assistant = Message::Assistant {
        id: None,
        content: vec![AssistantContent::Text(Text {
            text: final_response.to_string(),
        })],
    };
    let escaped_message = escape_graphql_string(&serde_json::to_string(&assistant).unwrap());
    let escaped_child_session_id = escape_graphql_string(child_session_id);
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
        "create child AgentMessage failed: {:?}",
        response.errors
    );

    let escaped_agent_did = escape_graphql_string(AGENT_DID);
    let escaped_behavior_id = escape_graphql_string(CHILD_BEHAVIOR_ID);
    let create_response = format!(
        r#"mutation {{
            create_AgentResponse(input: {{
                response_key: "{escaped_child_request_id}",
                request_id: "{escaped_child_request_id}",
                agent_did: "{escaped_agent_did}",
                behavior_id: "{escaped_behavior_id}",
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
        "create child AgentResponse failed: {:?}",
        response.errors
    );
}

async fn set_request_lifecycle(node: &EmbeddedNode, request_id: &str, state: &str) {
    let request_id = escape_graphql_string(request_id);
    let status = match state {
        "completed" => "completed",
        "processing" => "processing",
        "superseded" => "superseded",
        _ => "error",
    };
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                input: {{ status: "{status}", lifecycle_state: "{state}" }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "set request lifecycle failed: {:?}",
        response.errors
    );
}

async fn set_child_processing_deadline(
    node: &EmbeddedNode,
    request_id: &str,
    deadline: chrono::DateTime<chrono::Utc>,
) {
    let request_id = escape_graphql_string(request_id);
    let deadline = escape_graphql_string(&deadline.to_rfc3339());
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                input: {{
                    status: "processing",
                    lifecycle_state: "processing",
                    deadline: "{deadline}"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "set child processing deadline failed: {:?}",
        response.errors
    );
}

async fn create_streaming_child_response(
    node: &EmbeddedNode,
    child_request_id: &str,
    child_session_id: &str,
    content: &str,
) {
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let escaped_child_session_id = escape_graphql_string(child_session_id);
    let escaped_content = escape_graphql_string(content);
    let escaped_agent_did = escape_graphql_string(AGENT_DID);
    let escaped_behavior_id = escape_graphql_string(CHILD_BEHAVIOR_ID);
    let now = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentResponse(input: {{
                response_key: "{escaped_child_request_id}",
                request_id: "{escaped_child_request_id}",
                agent_did: "{escaped_agent_did}",
                behavior_id: "{escaped_behavior_id}",
                session_id: "{escaped_child_session_id}",
                content: "{escaped_content}",
                reasoning: "",
                status: "streaming",
                error_message: "",
                token_count: 0,
                progress_seq: 1,
                created_at: "{now}"
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create streaming child AgentResponse failed: {:?}",
        response.errors
    );
}

async fn fetch_child_request_state(
    node: &EmbeddedNode,
    child_request_id: &str,
) -> ChildRequestStateRow {
    let child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{child_request_id}" }} }},
                limit: 1
            ) {{
                status
                lifecycle_state
                failure_reason
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentRequest")
}

async fn fetch_response_state(node: &EmbeddedNode, request_id: &str) -> ResponseStateRow {
    let request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                limit: 1
            ) {{
                status
                content
                error_message
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentResponse")
}

async fn fetch_tool_call(node: &EmbeddedNode, session_id: &str, tool_call_id: &str) -> ToolCallRow {
    let session_id = escape_graphql_string(session_id);
    let tool_call_id = escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    tool_call_id: {{ _eq: "{tool_call_id}" }}
                }},
                limit: 1
            ) {{ result lifecycle_state await_mode }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentToolCall")
}

async fn fetch_parent_messages(node: &EmbeddedNode, session_id: &str) -> Vec<MessageRow> {
    let session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{session_id}" }} }},
                order: {{ sequence: ASC }}
            ) {{ sequence role content }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "message query failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
}

async fn fetch_scheduled_wakes(node: &EmbeddedNode, session_id: &str) -> Vec<serde_json::Value> {
    let session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    execution_origin: {{ _eq: "scheduled" }}
                }},
                order: {{ created_at: ASC }}
            ) {{
                request_id
                content
                status
                lifecycle_state
                execution_origin
                metadata
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "wake query failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
}

#[tokio::test]
async fn background_completion_projects_bridge_notifies_and_enqueues_wake() {
    let (db, session_id, parent_request_id) = setup_fixture("background_completion_project").await;
    let (child_request_id, child_session_id) = create_child_and_bridge(
        &db.node,
        &parent_request_id,
        &session_id,
        "spawn-bg-1",
        AwaitMode::Background,
        1,
    )
    .await;
    persist_child_completion(
        db.node.as_ref(),
        &child_request_id,
        &child_session_id,
        "child final answer <ok>",
    )
    .await;

    let outcome =
        project_background_subagent_completion(db.node.clone(), &child_request_id, AGENT_DID)
            .await
            .unwrap();
    assert!(matches!(
        outcome,
        BackgroundCompletionOutcome::Projected { .. }
    ));

    let tool = fetch_tool_call(db.node.as_ref(), &session_id, "spawn-bg-1").await;
    assert_eq!(tool.lifecycle_state.as_deref(), Some("completed"));
    assert_eq!(tool.await_mode.as_deref(), Some("background"));
    assert_eq!(tool.result.as_deref(), Some("child final answer <ok>"));

    let messages = fetch_parent_messages(db.node.as_ref(), &session_id).await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].sequence, 3);
    assert_eq!(messages[0].role, "user");
    assert!(messages[0].content.contains(r#"<subagent-notification"#));
    assert!(messages[0].content.contains(r#"status="completed""#));
    assert!(messages[0]
        .content
        .contains("child final answer &lt;ok&gt;"));

    let wakes = fetch_scheduled_wakes(db.node.as_ref(), &session_id).await;
    assert_eq!(wakes.len(), 1);

    let again =
        project_background_subagent_completion(db.node.clone(), &child_request_id, AGENT_DID)
            .await
            .unwrap();
    assert_eq!(again, BackgroundCompletionOutcome::AlreadyProjected);
    assert_eq!(
        fetch_parent_messages(db.node.as_ref(), &session_id)
            .await
            .len(),
        1
    );
    assert_eq!(
        fetch_scheduled_wakes(db.node.as_ref(), &session_id)
            .await
            .len(),
        1
    );
}

#[tokio::test]
async fn background_completion_recovers_side_effects_after_bridge_already_projected() {
    let (db, session_id, parent_request_id) = setup_fixture("background_completion_recovery").await;
    let (child_request_id, child_session_id) = create_child_and_bridge(
        &db.node,
        &parent_request_id,
        &session_id,
        "spawn-bg-recover",
        AwaitMode::Background,
        1,
    )
    .await;
    persist_child_completion(
        db.node.as_ref(),
        &child_request_id,
        &child_session_id,
        "child completed before observer side effects",
    )
    .await;

    let mut lifecycle = ToolCallLifecycle::load(db.node.clone(), &session_id, "spawn-bg-recover")
        .await
        .unwrap()
        .expect("bridge should exist");
    assert!(lifecycle
        .bridge_complete("child completed before observer side effects".to_string())
        .await
        .unwrap());
    assert!(fetch_parent_messages(db.node.as_ref(), &session_id)
        .await
        .is_empty());
    assert!(fetch_scheduled_wakes(db.node.as_ref(), &session_id)
        .await
        .is_empty());

    let outcome =
        project_background_subagent_completion(db.node.clone(), &child_request_id, AGENT_DID)
            .await
            .unwrap();
    assert!(matches!(
        outcome,
        BackgroundCompletionOutcome::Projected { .. }
    ));
    let messages = fetch_parent_messages(db.node.as_ref(), &session_id).await;
    assert_eq!(messages.len(), 1);
    assert!(messages[0]
        .content
        .contains("child completed before observer side effects"));
    assert_eq!(
        fetch_scheduled_wakes(db.node.as_ref(), &session_id)
            .await
            .len(),
        1
    );

    let again =
        project_background_subagent_completion(db.node.clone(), &child_request_id, AGENT_DID)
            .await
            .unwrap();
    assert_eq!(again, BackgroundCompletionOutcome::AlreadyProjected);
    assert_eq!(
        fetch_parent_messages(db.node.as_ref(), &session_id)
            .await
            .len(),
        1
    );
    assert_eq!(
        fetch_scheduled_wakes(db.node.as_ref(), &session_id)
            .await
            .len(),
        1
    );
}

#[tokio::test]
async fn background_notification_sorts_after_reserved_spawn_tool_result() {
    let (db, session_id, parent_request_id) = setup_fixture("background_completion_order").await;
    let _source = spawn_subagent_source(
        db.node.clone(),
        AGENT_DID,
        PARENT_BEHAVIOR_ID,
        CHILD_BEHAVIOR_ID,
    );
    let hook = DefraSessionHook::resume_or_create_with_identity_policy(
        db.node.clone(),
        &session_id,
        PARENT_BEHAVIOR_ID,
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .unwrap();
    hook.set_active_request_id(Some(parent_request_id.clone()))
        .await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::minutes(5)))
        .await;

    let args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": "background child can complete quickly",
        "await_mode": "background"
    })
    .to_string();
    let action = hook
        .on_tool_call(
            "spawn_subagent",
            Some("model-call-order".to_string()),
            "spawn-bg-order",
            &args,
        )
        .await;
    let receipt = skip_reason(action);
    let (child_request_id, child_session_id) =
        wait_for_child_for_tool(db.node.as_ref(), "spawn-bg-order").await;
    persist_child_completion(
        db.node.as_ref(),
        &child_request_id,
        &child_session_id,
        "fast background child done",
    )
    .await;
    project_background_subagent_completion(db.node.clone(), &child_request_id, AGENT_DID)
        .await
        .unwrap();

    let messages_before_parent_persists =
        fetch_parent_messages(db.node.as_ref(), &session_id).await;
    assert_eq!(messages_before_parent_persists.len(), 1);
    assert_eq!(messages_before_parent_persists[0].sequence, 3);

    hook.persist_message(&Message::Assistant {
        id: None,
        content: vec![AssistantContent::ToolCall(ToolCall {
            id: "spawn-bg-order".to_string(),
            call_id: Some("model-call-order".to_string()),
            function: ToolFunction {
                name: "spawn_subagent".to_string(),
                arguments: serde_json::from_str(&args).unwrap(),
            },
            signature: None,
            additional_params: None,
        })],
    })
    .await
    .unwrap();
    hook.persist_stream_tool_result_message(
        &ToolResult {
            id: "spawn-bg-order".to_string(),
            call_id: Some("model-call-order".to_string()),
            content: vec![ToolResultContent::Text(Text { text: receipt })],
        },
        "spawn-bg-order",
    )
    .await
    .unwrap();

    let messages = fetch_parent_messages(db.node.as_ref(), &session_id).await;
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].sequence, 1);
    assert_eq!(messages[0].role, "assistant");
    assert!(messages[0].content.contains("spawn_subagent"));
    assert_eq!(messages[1].sequence, 2);
    assert_eq!(messages[1].role, "user");
    assert!(messages[1].content.contains("child_request_id"));
    assert_eq!(messages[2].sequence, 3);
    assert!(messages[2].content.contains("<subagent-notification"));
}

#[tokio::test]
async fn background_completion_compacts_multibyte_summary_without_panicking() {
    let (db, session_id, parent_request_id) = setup_fixture("background_completion_unicode").await;
    let (child_request_id, child_session_id) = create_child_and_bridge(
        &db.node,
        &parent_request_id,
        &session_id,
        "spawn-bg-unicode",
        AwaitMode::Background,
        1,
    )
    .await;
    let final_response = "é".repeat(3000);
    persist_child_completion(
        db.node.as_ref(),
        &child_request_id,
        &child_session_id,
        &final_response,
    )
    .await;

    let outcome =
        project_background_subagent_completion(db.node.clone(), &child_request_id, AGENT_DID)
            .await
            .unwrap();
    assert!(matches!(
        outcome,
        BackgroundCompletionOutcome::Projected { .. }
    ));
    let messages = fetch_parent_messages(db.node.as_ref(), &session_id).await;
    assert_eq!(messages.len(), 1);
    assert!(messages[0].content.contains("<subagent-notification"));
    assert!(messages[0].content.contains("..."));
}

#[tokio::test]
async fn multiple_background_completions_append_notifications_and_coalesce_wake() {
    let (db, session_id, parent_request_id) = setup_fixture("background_completion_coalesce").await;
    let (child_a, session_a) = create_child_and_bridge(
        &db.node,
        &parent_request_id,
        &session_id,
        "spawn-bg-a",
        AwaitMode::Background,
        1,
    )
    .await;
    let (child_b, session_b) = create_child_and_bridge(
        &db.node,
        &parent_request_id,
        &session_id,
        "spawn-bg-b",
        AwaitMode::Background,
        1,
    )
    .await;
    persist_child_completion(db.node.as_ref(), &child_a, &session_a, "child A done").await;
    persist_child_completion(db.node.as_ref(), &child_b, &session_b, "child B done").await;

    let first = project_background_subagent_completion(db.node.clone(), &child_a, AGENT_DID)
        .await
        .unwrap();
    let second = project_background_subagent_completion(db.node.clone(), &child_b, AGENT_DID)
        .await
        .unwrap();
    assert!(matches!(
        first,
        BackgroundCompletionOutcome::Projected { .. }
    ));
    assert!(matches!(
        second,
        BackgroundCompletionOutcome::Projected { .. }
    ));
    let messages = fetch_parent_messages(db.node.as_ref(), &session_id).await;
    assert_eq!(messages.len(), 2);
    assert!(messages[0].content.contains("child A done"));
    assert!(messages[1].content.contains("child B done"));
    assert_eq!(
        fetch_scheduled_wakes(db.node.as_ref(), &session_id)
            .await
            .len(),
        1
    );
}

#[tokio::test]
async fn background_completion_does_not_interrupt_active_foreground_parent() {
    let (db, session_id, parent_request_id) =
        setup_fixture("background_completion_interleave").await;
    let (foreground_child, _) = create_child_and_bridge(
        &db.node,
        &parent_request_id,
        &session_id,
        "spawn-fg-a",
        AwaitMode::Foreground,
        1,
    )
    .await;
    set_request_lifecycle(db.node.as_ref(), &foreground_child, "processing").await;

    let (background_child, background_session) = create_child_and_bridge(
        &db.node,
        &parent_request_id,
        &session_id,
        "spawn-bg-b",
        AwaitMode::Background,
        2,
    )
    .await;
    persist_child_completion(
        db.node.as_ref(),
        &background_child,
        &background_session,
        "background child B done",
    )
    .await;

    let outcome =
        project_background_subagent_completion(db.node.clone(), &background_child, AGENT_DID)
            .await
            .unwrap();
    assert!(matches!(
        outcome,
        BackgroundCompletionOutcome::Projected { .. }
    ));
    assert_eq!(
        fetch_parent_messages(db.node.as_ref(), &session_id)
            .await
            .len(),
        1
    );

    let wakes = fetch_scheduled_wakes(db.node.as_ref(), &session_id).await;
    assert_eq!(wakes.len(), 1);
}

#[tokio::test]
async fn recovery_leaves_running_background_bridge_after_clean_parent_completion() {
    let (db, session_id, parent_request_id) =
        setup_fixture("background_completion_recovery_skip").await;
    let (child_request_id, _) = create_child_and_bridge(
        &db.node,
        &parent_request_id,
        &session_id,
        "spawn-bg-recovery-skip",
        AwaitMode::Background,
        1,
    )
    .await;
    set_request_lifecycle(db.node.as_ref(), &child_request_id, "processing").await;
    set_request_lifecycle(db.node.as_ref(), &parent_request_id, "completed").await;

    let report = ToolCallLifecycle::recover_all(db.node.as_ref(), AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.tool_calls_recovered, 0);

    let tool = fetch_tool_call(db.node.as_ref(), &session_id, "spawn-bg-recovery-skip").await;
    assert_eq!(tool.lifecycle_state.as_deref(), Some("running"));
    assert_eq!(tool.await_mode.as_deref(), Some("background"));
    let interrupt = fetch_interrupt_requested_at(db.node.as_ref(), &child_request_id)
        .await
        .unwrap();
    assert!(
        interrupt.is_none(),
        "clean parent completion must not interrupt its linked background child"
    );
}

#[tokio::test]
async fn recovery_terminalizes_expired_background_child_before_projection() {
    let (db, session_id, parent_request_id) =
        setup_fixture("background_completion_expired_child").await;
    let (child_request_id, child_session_id) = create_child_and_bridge(
        &db.node,
        &parent_request_id,
        &session_id,
        "spawn-bg-expired-child",
        AwaitMode::Background,
        1,
    )
    .await;
    let expired_deadline = chrono::Utc::now() - chrono::Duration::seconds(1);
    set_child_processing_deadline(db.node.as_ref(), &child_request_id, expired_deadline).await;
    create_streaming_child_response(
        db.node.as_ref(),
        &child_request_id,
        &child_session_id,
        "partial child output",
    )
    .await;

    let report = ToolCallLifecycle::recover_all(db.node.as_ref(), AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.tool_calls_recovered, 1);

    let child = fetch_child_request_state(db.node.as_ref(), &child_request_id).await;
    assert_eq!(child.status.as_deref(), Some("dead"));
    assert_eq!(child.lifecycle_state.as_deref(), Some("dead"));
    assert!(
        child
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("child request deadline exceeded")),
        "child failure reason should explain deadline expiry: {:?}",
        child.failure_reason
    );

    let response = fetch_response_state(db.node.as_ref(), &child_request_id).await;
    assert_eq!(response.status.as_deref(), Some("error"));
    assert!(
        response
            .content
            .as_deref()
            .is_some_and(|content| content.contains("partial child output")
                && content.contains("Response interrupted")),
        "streaming child response should be finalized with prior content: {:?}",
        response.content
    );
    assert!(
        response
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("child request deadline exceeded")),
        "response error message should explain deadline expiry: {:?}",
        response.error_message
    );

    let tool = fetch_tool_call(db.node.as_ref(), &session_id, "spawn-bg-expired-child").await;
    assert_eq!(tool.lifecycle_state.as_deref(), Some("failed"));
    assert_eq!(tool.await_mode.as_deref(), Some("background"));

    let messages = fetch_parent_messages(db.node.as_ref(), &session_id).await;
    assert_eq!(messages.len(), 1);
    assert!(messages[0].content.contains(r#"<subagent-notification"#));
    assert!(messages[0].content.contains(r#"status="dead""#));
    assert!(messages[0].content.contains(&child_request_id));

    let wakes = fetch_scheduled_wakes(db.node.as_ref(), &session_id).await;
    assert_eq!(wakes.len(), 1);
}

#[tokio::test]
async fn stale_hook_sequence_does_not_overwrite_background_notification() {
    let (db, session_id, parent_request_id) =
        setup_fixture("background_completion_hook_sequence").await;
    let hook = DefraSessionHook::resume_or_create_with_identity_policy(
        db.node.clone(),
        &session_id,
        PARENT_BEHAVIOR_ID,
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .unwrap();

    let (child_request_id, child_session_id) = create_child_and_bridge(
        &db.node,
        &parent_request_id,
        &session_id,
        "spawn-bg-stale-hook",
        AwaitMode::Background,
        1,
    )
    .await;
    persist_child_completion(
        db.node.as_ref(),
        &child_request_id,
        &child_session_id,
        "notification must survive",
    )
    .await;
    project_background_subagent_completion(db.node.clone(), &child_request_id, AGENT_DID)
        .await
        .unwrap();

    hook.persist_message(&Message::User {
        content: vec![UserContent::Text(Text {
            text: "parent hook resumes".to_string(),
        })],
    })
    .await
    .unwrap();

    let messages = fetch_parent_messages(db.node.as_ref(), &session_id).await;
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].sequence, 3);
    assert!(messages[0].content.contains("notification must survive"));
    assert_eq!(messages[1].sequence, 4);
    assert!(messages[1].content.contains("parent hook resumes"));
}
