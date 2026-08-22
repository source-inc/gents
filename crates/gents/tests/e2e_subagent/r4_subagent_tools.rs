use std::time::Duration;

use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::llm::message::{
    AssistantContent, Message, Text, ToolCall, ToolFunction, ToolResult, ToolResultContent,
    UserContent,
};
use gents::llm::ToolCallHookAction;
use gents::tool_call_lifecycle::{
    create_subagent_request_with_request_id, AwaitMode, CancelCause, CancelPolicy, CascadeDispatch,
    ToolCallLifecycle, MAX_SUBAGENT_DEPTH,
};
use gents::{
    fetch_interrupt_requested_at, interrupt_request, load_history, upsert_agent_behavior,
    upsert_tool_selection, AgentBehaviorDocument, DefraSessionHook, FailurePolicy,
    ToolSelectionDocument,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::support::fixtures::{spawn_subagent_source, SubagentSourceGuard};
use crate::support::{first_optional_row, first_row, test_db};

const PARENT_BEHAVIOR_ID: &str = "r4-parent";
const CHILD_BEHAVIOR_ID: &str = "r4-child";

struct SpawnFixture {
    db: crate::support::TestDb,
    hook: DefraSessionHook,
    session_id: String,
    request_id: String,
    parent_deadline: chrono::DateTime<chrono::Utc>,
    agent_did: String,
    _source: SubagentSourceGuard,
}

#[derive(Debug, Deserialize)]
struct ToolCallRow {
    request_id: Option<String>,
    tool_name: Option<String>,
    args: Option<String>,
    result: Option<String>,
    lifecycle_state: Option<String>,
    await_mode: Option<String>,
    cancel_policy: Option<String>,
    cancel_cause: Option<String>,
    child_request_id: Option<String>,
    unclaimed_deadline_at: Option<String>,
    cancel_cascade_intent_at: Option<String>,
    cancel_pending_remote_ack: Option<bool>,
    #[allow(dead_code)]
    stuck_since: Option<String>,
    tool_failure_class: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChildRequestRow {
    request_id: String,
    session_id: String,
    behavior_id: String,
    content: String,
    status: Option<String>,
    lifecycle_state: Option<String>,
    failure_reason: Option<String>,
    subagent_depth: Option<i64>,
    deadline: Option<String>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
    caused_by_trigger_id: Option<String>,
    caused_by_trigger_kind: Option<String>,
}

async fn setup_spawn_fixture(
    test_name: &str,
    targets: Vec<&str>,
    parent_subagent_depth: u32,
    background_enabled: bool,
) -> SpawnFixture {
    setup_spawn_fixture_with_flags(
        test_name,
        targets,
        parent_subagent_depth,
        true,
        background_enabled,
    )
    .await
}

async fn setup_spawn_fixture_with_flags(
    test_name: &str,
    targets: Vec<&str>,
    parent_subagent_depth: u32,
    spawn_enabled: bool,
    background_enabled: bool,
) -> SpawnFixture {
    setup_spawn_fixture_with_flags_and_deadline(
        test_name,
        targets,
        parent_subagent_depth,
        spawn_enabled,
        background_enabled,
        chrono::Utc::now() + chrono::Duration::minutes(5),
    )
    .await
}

async fn setup_spawn_fixture_with_flags_and_deadline(
    test_name: &str,
    targets: Vec<&str>,
    parent_subagent_depth: u32,
    spawn_enabled: bool,
    background_enabled: bool,
    parent_deadline: chrono::DateTime<chrono::Utc>,
) -> SpawnFixture {
    setup_spawn_fixture_with_parent_fields(
        test_name,
        targets,
        parent_subagent_depth,
        spawn_enabled,
        background_enabled,
        parent_deadline,
        "",
    )
    .await
}

async fn setup_spawn_fixture_with_parent_fields(
    test_name: &str,
    targets: Vec<&str>,
    parent_subagent_depth: u32,
    spawn_enabled: bool,
    background_enabled: bool,
    parent_deadline: chrono::DateTime<chrono::Utc>,
    extra_parent_fields: &str,
) -> SpawnFixture {
    let db = test_db(test_name).await;
    let agent_did = format!("did:test:r4-{test_name}");

    upsert_tool_selection(
        db.node.as_ref(),
        &ToolSelectionDocument {
            selection_id: "r4-parent-tools".to_string(),
            agent_did: agent_did.clone(),
            subagent_targets: Some(
                targets
                    .into_iter()
                    .map(|behavior_id| {
                        gents::subagent_target_entry(behavior_id, &agent_did, behavior_id, None)
                    })
                    .collect(),
            ),
            subagent_spawn_enabled: Some(spawn_enabled),
            subagent_background_enabled: Some(background_enabled),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    upsert_agent_behavior(
        db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: PARENT_BEHAVIOR_ID.to_string(),
            agent_did: agent_did.clone(),
            display_name: Some("R4 parent".to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: Some("r4-parent-tools".to_string()),
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
            agent_did: agent_did.clone(),
            display_name: Some("R4 child".to_string()),
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

    let source = spawn_subagent_source(
        db.node.clone(),
        &agent_did,
        PARENT_BEHAVIOR_ID,
        CHILD_BEHAVIOR_ID,
    );

    let session_id = format!("{test_name}-session");
    let request_id = format!("{test_name}-parent");
    create_parent_request_with_extra_fields(
        db.node.as_ref(),
        &agent_did,
        &request_id,
        &session_id,
        parent_subagent_depth,
        parent_deadline,
        extra_parent_fields,
    )
    .await;

    let hook = DefraSessionHook::resume_or_create_with_identity_policy(
        db.node.clone(),
        &session_id,
        PARENT_BEHAVIOR_ID,
        &agent_did,
        FailurePolicy::default(),
    )
    .await
    .unwrap();
    hook.set_active_request_id(Some(request_id.clone())).await;
    hook.set_request_deadline_at(Some(parent_deadline)).await;

    SpawnFixture {
        db,
        hook,
        session_id,
        request_id,
        parent_deadline,
        agent_did,
        _source: source,
    }
}

async fn create_parent_request(
    node: &EmbeddedNode,
    agent_did: &str,
    request_id: &str,
    session_id: &str,
    subagent_depth: u32,
    deadline: chrono::DateTime<chrono::Utc>,
) {
    create_parent_request_with_extra_fields(
        node,
        agent_did,
        request_id,
        session_id,
        subagent_depth,
        deadline,
        "",
    )
    .await;
}

async fn create_parent_request_with_extra_fields(
    node: &EmbeddedNode,
    agent_did: &str,
    request_id: &str,
    session_id: &str,
    subagent_depth: u32,
    deadline: chrono::DateTime<chrono::Utc>,
    extra_fields: &str,
) {
    let request_id = escape_graphql_string(request_id);
    let session_id = escape_graphql_string(session_id);
    let behavior_id = escape_graphql_string(PARENT_BEHAVIOR_ID);
    let agent_did = escape_graphql_string(agent_did);
    let created_at = chrono::Utc::now().to_rfc3339();
    let deadline = deadline.to_rfc3339();
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
                created_at: "{created_at}",
                deadline: "{deadline}",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: {subagent_depth}
                {extra_fields}
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

async fn fetch_tool_call(node: &EmbeddedNode, session_id: &str, tool_call_id: &str) -> ToolCallRow {
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_tool_call_id = escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    tool_call_id: {{ _eq: "{escaped_tool_call_id}" }}
                }},
                limit: 1
            ) {{
                request_id
                tool_name
                args
                result
                lifecycle_state
                await_mode
                cancel_policy
                cancel_cause
                child_request_id
                unclaimed_deadline_at
                cancel_cascade_intent_at
                cancel_pending_remote_ack
                stuck_since
                tool_failure_class
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentToolCall")
}

async fn wait_for_tool_call_await_mode(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
    expected_await_mode: &str,
) -> ToolCallRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = fetch_tool_call(node, session_id, tool_call_id).await;
        if row.await_mode.as_deref() == Some(expected_await_mode) {
            return row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for tool call {tool_call_id} await_mode={expected_await_mode}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn count_tool_calls_by_name(node: &EmbeddedNode, session_id: &str, tool_name: &str) -> usize {
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_tool_name = escape_graphql_string(tool_name);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    tool_name: {{ _eq: "{escaped_tool_name}" }}
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "count AgentToolCall by name failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|rows| rows.as_array())
        .map_or(0, Vec::len)
}

async fn fetch_child_request(node: &EmbeddedNode, child_request_id: &str) -> ChildRequestRow {
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_child_request_id}" }} }},
                limit: 1
            ) {{
                request_id
                session_id
                behavior_id
                content
                status
                lifecycle_state
                failure_reason
                subagent_depth
                deadline
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
                caused_by_trigger_id
                caused_by_trigger_kind
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentRequest")
}

async fn fetch_child_request_optional(
    node: &EmbeddedNode,
    child_request_id: &str,
) -> Option<ChildRequestRow> {
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_child_request_id}" }} }},
                limit: 1
            ) {{
                request_id
                session_id
                behavior_id
                content
                status
                lifecycle_state
                failure_reason
                subagent_depth
                deadline
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
                caused_by_trigger_id
                caused_by_trigger_kind
            }}
        }}"#
    );
    first_optional_row(&node.execute(&query).await, "AgentRequest")
}

async fn child_request_for_tool(
    node: &EmbeddedNode,
    parent_tool_call_id: &str,
) -> Option<ChildRequestRow> {
    let escaped_parent_tool_call_id = escape_graphql_string(parent_tool_call_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    caused_by_parent_tool_call_id: {{ _eq: "{escaped_parent_tool_call_id}" }}
                }},
                limit: 1
            ) {{
                request_id
                session_id
                behavior_id
                content
                status
                lifecycle_state
                failure_reason
                subagent_depth
                deadline
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
                caused_by_trigger_id
                caused_by_trigger_kind
            }}
        }}"#
    );
    first_optional_row(&node.execute(&query).await, "AgentRequest")
}

async fn wait_for_child_request_for_tool(
    node: &EmbeddedNode,
    parent_tool_call_id: &str,
) -> ChildRequestRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(row) = child_request_for_tool(node, parent_tool_call_id).await {
            return row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for child AgentRequest for tool call {parent_tool_call_id}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_child_session_id(node: &EmbeddedNode, child_request_id: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(child) = fetch_child_request_optional(node, child_request_id).await {
            if !child.session_id.is_empty() {
                return child.session_id;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for child AgentRequest {child_request_id} session id"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn persist_child_completion(
    node: &EmbeddedNode,
    agent_did: &str,
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

    let escaped_agent_did = escape_graphql_string(agent_did);
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

async fn persist_child_terminal(
    node: &EmbeddedNode,
    child_request_id: &str,
    lifecycle_state: &str,
    failure_reason: Option<&str>,
) {
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let escaped_lifecycle_state = escape_graphql_string(lifecycle_state);
    let status = match lifecycle_state {
        "completed" => "completed",
        "superseded" => "superseded",
        "failed" | "dead" | "interrupted" => "error",
        other => other,
    };
    let failure_reason_field = failure_reason
        .map(|reason| {
            let escaped = escape_graphql_string(reason);
            format!(r#", failure_reason: "{escaped}""#)
        })
        .unwrap_or_default();
    let update_request = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_child_request_id}" }} }},
                input: {{
                    status: "{status}",
                    lifecycle_state: "{escaped_lifecycle_state}"
                    {failure_reason_field}
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&update_request).await;
    assert!(
        !response.has_errors(),
        "update child AgentRequest {lifecycle_state} failed: {:?}",
        response.errors
    );
}

async fn update_request_state(
    node: &EmbeddedNode,
    request_id: &str,
    status: &str,
    lifecycle_state: &str,
) {
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_status = escape_graphql_string(status);
    let escaped_lifecycle_state = escape_graphql_string(lifecycle_state);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                input: {{
                    status: "{escaped_status}",
                    lifecycle_state: "{escaped_lifecycle_state}"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "update AgentRequest state failed: {:?}",
        response.errors
    );
}

async fn create_child_session_queued_request(
    node: &EmbeddedNode,
    agent_did: &str,
    request_id: &str,
    session_id: &str,
    execution_origin: &str,
    metadata: &str,
) {
    let escaped_request_id = escape_graphql_string(request_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_behavior_id = escape_graphql_string(CHILD_BEHAVIOR_ID);
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_execution_origin = escape_graphql_string(execution_origin);
    let escaped_metadata = escape_graphql_string(metadata);
    let now = chrono::Utc::now();
    let escaped_created_at = escape_graphql_string(&now.to_rfc3339());
    let escaped_deadline =
        escape_graphql_string(&(now + chrono::Duration::minutes(5)).to_rfc3339());
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                behavior_id: "{escaped_behavior_id}",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "queued child session request",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "{escaped_execution_origin}",
                metadata: "{escaped_metadata}",
                failure_reason: "",
                created_at: "{escaped_created_at}",
                deadline: "{escaped_deadline}",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: 1
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create queued child AgentRequest failed: {:?}",
        response.errors
    );
}

fn queue_metadata(
    source: &str,
    policy: &str,
    key: Option<&str>,
    queued_after_request_id: Option<&str>,
) -> String {
    json!({
        "queue": {
            "source": source,
            "policy": policy,
            "key": key,
            "queued_after_request_id": queued_after_request_id
        }
    })
    .to_string()
}

fn skip_reason_json(action: ToolCallHookAction) -> Value {
    let ToolCallHookAction::Skip { reason } = action else {
        panic!("expected Skip action, got {action:?}");
    };
    serde_json::from_str(&reason).expect("skip reason should be JSON")
}

#[path = "r4_subagent_tools_cases/background_cancel.rs"]
mod background_cancel;
#[path = "r4_subagent_tools_cases/cancel_subagent.rs"]
mod cancel_subagent;
#[path = "r4_subagent_tools_cases/foreground_spawn.rs"]
mod foreground_spawn;
#[path = "r4_subagent_tools_cases/spawn_validation.rs"]
mod spawn_validation;
#[path = "r4_subagent_tools_cases/spawn_workspace.rs"]
mod spawn_workspace;
#[path = "r4_subagent_tools_cases/wait_subagent.rs"]
mod wait_subagent;
