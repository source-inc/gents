use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::llm::ToolCallHookAction;
use gents::tool_call_lifecycle::{
    create_subagent_request_with_request_id_for_test, AwaitMode, CancelPolicy, ToolCallLifecycle,
};
use gents::{
    fetch_interrupt_requested_at, upsert_agent_behavior, upsert_tool_selection,
    AgentBehaviorDocument, DefraSessionHook, FailurePolicy, ToolSelectionDocument,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::support::fixtures::spawn_subagent_source;
use crate::support::test_db;

const AGENT_DID: &str = "did:test:r4c-steer";
const PARENT_BEHAVIOR_ID: &str = "r4c-parent";
const CHILD_BEHAVIOR_ID: &str = "r4c-child";

#[derive(Debug, Deserialize)]
struct RequestRow {
    session_id: String,
    behavior_id: Option<String>,
    status: Option<String>,
    lifecycle_state: Option<String>,
    metadata: Option<String>,
    subagent_depth: Option<u32>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageRow {
    role: String,
    content: String,
}

async fn setup_db(
    name: &str,
) -> (
    crate::support::TestDb,
    crate::support::fixtures::SubagentSourceGuard,
) {
    let db = test_db(name).await;
    upsert_tool_selection(
        db.node.as_ref(),
        &ToolSelectionDocument {
            selection_id: "r4c-parent-tools".to_string(),
            agent_did: AGENT_DID.to_string(),
            subagent_targets: Some(vec![gents::subagent_target_entry(
                CHILD_BEHAVIOR_ID,
                AGENT_DID,
                CHILD_BEHAVIOR_ID,
                None,
            )]),
            subagent_spawn_enabled: Some(true),
            orchestration_enabled: Some(false),
            subagent_steering_enabled: Some(true),
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
            display_name: Some("R4c parent".to_string()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: Some("r4c-parent-tools".to_string()),
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            enabled: true,
            created_at: Some("2026-05-14T00:00:00Z".to_string()),
        },
    )
    .await
    .unwrap();
    upsert_agent_behavior(
        db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: CHILD_BEHAVIOR_ID.to_string(),
            agent_did: AGENT_DID.to_string(),
            display_name: Some("R4c child".to_string()),
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
            created_at: Some("2026-05-14T00:00:01Z".to_string()),
        },
    )
    .await
    .unwrap();
    let source = spawn_subagent_source(
        db.node.clone(),
        AGENT_DID,
        PARENT_BEHAVIOR_ID,
        CHILD_BEHAVIOR_ID,
    );
    (db, source)
}

async fn create_parent_hook(
    db: &crate::support::TestDb,
    request_id: &str,
    session_id: &str,
) -> DefraSessionHook {
    let deadline = chrono::Utc::now() + chrono::Duration::minutes(5);
    create_parent_request(db.node.as_ref(), request_id, session_id, deadline).await;
    let hook = DefraSessionHook::resume_or_create_with_identity_policy(
        db.node.clone(),
        session_id,
        PARENT_BEHAVIOR_ID,
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .unwrap();
    hook.set_active_request_id(Some(request_id.to_string()))
        .await;
    hook.set_request_deadline_at(Some(deadline)).await;
    hook
}

async fn create_parent_request(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    deadline: chrono::DateTime<chrono::Utc>,
) {
    let request_id = escape_graphql_string(request_id);
    let session_id = escape_graphql_string(session_id);
    let behavior_id = escape_graphql_string(PARENT_BEHAVIOR_ID);
    let agent_did = escape_graphql_string(AGENT_DID);
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

async fn spawn_background_child(
    node: &EmbeddedNode,
    hook: &DefraSessionHook,
    internal_call_id: &str,
    prompt: &str,
) -> Value {
    let args = json!({
        "name": CHILD_BEHAVIOR_ID,
        "prompt": prompt,
        "await_mode": "background"
    })
    .to_string();
    let action = hook
        .on_tool_call(
            "spawn_subagent",
            Some(format!("model-{internal_call_id}")),
            internal_call_id,
            &args,
        )
        .await;
    let mut receipt = skip_reason_json(action);
    assert_eq!(receipt["ok"], true);
    let child_request_id = receipt["child_request_id"]
        .as_str()
        .expect("child_request_id")
        .to_string();
    let child_session_id = wait_for_child_session_id(node, &child_request_id).await;
    receipt["child_session_id"] = Value::String(child_session_id);
    receipt
}

async fn wait_for_child_session_id(node: &EmbeddedNode, child_request_id: &str) -> String {
    let escaped = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped}" }} }},
                limit: 1
            ) {{ session_id }}
        }}"#
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let response = node.execute(&query).await;
        #[derive(serde::Deserialize)]
        struct Row {
            session_id: String,
        }
        if let Some(row) = crate::support::first_optional_row::<Row>(&response, "AgentRequest") {
            if !row.session_id.is_empty() {
                return row.session_id;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for child AgentRequest {child_request_id} session id"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn steer_subagent(hook: &DefraSessionHook, internal_call_id: &str, args: Value) -> Value {
    let action = hook
        .on_tool_call(
            "steer_subagent",
            Some(format!("model-{internal_call_id}")),
            internal_call_id,
            &args.to_string(),
        )
        .await;
    skip_reason_json(action)
}

fn skip_reason_json(action: ToolCallHookAction) -> Value {
    let ToolCallHookAction::Skip { reason } = action else {
        panic!("expected Skip action, got {action:?}");
    };
    serde_json::from_str(&reason).expect("skip reason should be JSON")
}

async fn fetch_request(node: &EmbeddedNode, request_id: &str) -> RequestRow {
    let request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                session_id
                behavior_id
                status
                lifecycle_state
                metadata
                subagent_depth
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    crate::support::first_row(&response, "AgentRequest")
}

async fn latest_user_message(node: &EmbeddedNode, session_id: &str) -> MessageRow {
    let session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{session_id}" }}, role: {{ _eq: "user" }} }},
                order: {{ sequence: DESC }},
                limit: 1
            ) {{
                role
                content
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    crate::support::first_row(&response, "AgentMessage")
}

async fn update_request_state(
    node: &EmbeddedNode,
    request_id: &str,
    status: &str,
    lifecycle_state: &str,
) {
    let request_id = escape_graphql_string(request_id);
    let status = escape_graphql_string(status);
    let lifecycle_state = escape_graphql_string(lifecycle_state);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                input: {{
                    status: "{status}",
                    lifecycle_state: "{lifecycle_state}"
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
    request_id: &str,
    session_id: &str,
    execution_origin: &str,
    metadata: &str,
) {
    let request_id = escape_graphql_string(request_id);
    let agent_did = escape_graphql_string(AGENT_DID);
    let behavior_id = escape_graphql_string(CHILD_BEHAVIOR_ID);
    let session_id = escape_graphql_string(session_id);
    let execution_origin = escape_graphql_string(execution_origin);
    let metadata = escape_graphql_string(metadata);
    let now = chrono::Utc::now();
    let created_at = escape_graphql_string(&now.to_rfc3339());
    let deadline = escape_graphql_string(&(now + chrono::Duration::minutes(5)).to_rfc3339());
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
                content: "queued child session request",
                status: "pending",
                lifecycle_state: "pending",
                backend_id: "",
                execution_origin: "{execution_origin}",
                metadata: "{metadata}",
                failure_reason: "",
                created_at: "{created_at}",
                deadline: "{deadline}",
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

async fn count_tool_calls_by_name(node: &EmbeddedNode, session_id: &str, tool_name: &str) -> usize {
    let session_id = escape_graphql_string(session_id);
    let tool_name = escape_graphql_string(tool_name);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    tool_name: {{ _eq: "{tool_name}" }}
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

#[tokio::test]
async fn steer_subagent_append_enqueues_with_steering_source() {
    let (db, _source) = setup_db("r4c-steer-append").await;
    let hook = create_parent_hook(&db, "parent-append", "session-append").await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-append", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    let child_session_id = child["child_session_id"].as_str().unwrap();

    let result = steer_subagent(
        &hook,
        "steer-append",
        json!({
            "child_request_id": child_request_id,
            "message": "also check the staging config",
            "interrupt": false
        }),
    )
    .await;
    let queued_request_id = result["queued_request_id"].as_str().unwrap();
    assert_eq!(result["interrupted_active_request_id"], Value::Null);
    assert!(result["drained_wake_up_request_ids"]
        .as_array()
        .unwrap()
        .is_empty());

    let queued = fetch_request(db.node.as_ref(), queued_request_id).await;
    assert_eq!(queued.session_id, child_session_id);
    assert_eq!(queued.behavior_id.as_deref(), Some(CHILD_BEHAVIOR_ID));
    assert_eq!(queued.subagent_depth, Some(1));
    assert_eq!(
        queued.caused_by_parent_request_id.as_deref(),
        Some("parent-append")
    );
    assert_eq!(queued.caused_by_parent_tool_call_id.as_deref(), None);
    assert_eq!(queued.status.as_deref(), Some("pending"));
    assert_eq!(queued.lifecycle_state.as_deref(), Some("pending"));
    let metadata: Value = serde_json::from_str(queued.metadata.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["queue"]["source"], "steering");
    assert_eq!(metadata["queue"]["policy"], "append");
}

#[tokio::test]
async fn steer_subagent_append_writes_user_message() {
    let (db, _source) = setup_db("r4c-steer-message").await;
    let hook = create_parent_hook(&db, "parent-message", "session-message").await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-message", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    let child_session_id = child["child_session_id"].as_str().unwrap();

    let _ = steer_subagent(
        &hook,
        "steer-message",
        json!({
            "child_request_id": child_request_id,
            "message": "also check the staging config"
        }),
    )
    .await;

    let message = latest_user_message(db.node.as_ref(), child_session_id).await;
    assert_eq!(message.role, "user");
    assert!(message.content.contains("also check the staging config"));
}

#[tokio::test]
async fn steer_subagent_rejects_terminal_child() {
    let (db, _source) = setup_db("r4c-steer-terminal").await;
    let hook = create_parent_hook(&db, "parent-terminal", "session-terminal").await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-terminal", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    update_request_state(db.node.as_ref(), child_request_id, "completed", "completed").await;

    let result = steer_subagent(
        &hook,
        "steer-terminal",
        json!({
            "child_request_id": child_request_id,
            "message": "do more"
        }),
    )
    .await;
    assert_eq!(result["ok"].as_bool(), Some(false));
    assert_eq!(
        result["failure_class"].as_str(),
        Some("invalid_tool_arguments")
    );
}

#[tokio::test]
async fn steer_subagent_rejects_unauthorized_child() {
    let (db, _source) = setup_db("r4c-steer-unauthorized").await;
    let hook_1 = create_parent_hook(&db, "parent-one", "session-one").await;
    let hook_2 = create_parent_hook(&db, "parent-two", "session-two").await;
    let child = spawn_background_child(db.node.as_ref(), &hook_2, "spawn-sibling", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();

    let result = steer_subagent(
        &hook_1,
        "steer-unauthorized",
        json!({
            "child_request_id": child_request_id,
            "message": "hi"
        }),
    )
    .await;
    assert_eq!(result["ok"].as_bool(), Some(false));
    assert_eq!(result["failure_class"].as_str(), Some("tool_not_allowed"));
}

#[tokio::test]
async fn steer_subagent_no_parent_tool_call_row_written() {
    let (db, _source) = setup_db("r4c-steer-no-row").await;
    let parent_session_id = "session-no-row";
    let hook = create_parent_hook(&db, "parent-no-row", parent_session_id).await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-no-row", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();

    let _ = steer_subagent(
        &hook,
        "steer-no-row",
        json!({
            "child_request_id": child_request_id,
            "message": "x"
        }),
    )
    .await;

    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), parent_session_id, "steer_subagent").await,
        0
    );
}

#[tokio::test]
async fn steer_subagent_interrupt_latches_active_child_request() {
    let (db, _source) = setup_db("r4c-steer-interrupt").await;
    let hook = create_parent_hook(&db, "parent-interrupt", "session-interrupt").await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-interrupt", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    update_request_state(
        db.node.as_ref(),
        child_request_id,
        "processing",
        "processing",
    )
    .await;

    let result = steer_subagent(
        &hook,
        "steer-interrupt",
        json!({
            "child_request_id": child_request_id,
            "message": "stop, do this instead",
            "interrupt": true
        }),
    )
    .await;

    assert_eq!(
        result["interrupted_active_request_id"].as_str(),
        Some(child_request_id)
    );
    assert!(
        fetch_interrupt_requested_at(db.node.as_ref(), child_request_id)
            .await
            .unwrap()
            .is_some()
    );
    let queued = fetch_request(
        db.node.as_ref(),
        result["queued_request_id"].as_str().unwrap(),
    )
    .await;
    let metadata: Value = serde_json::from_str(queued.metadata.as_deref().unwrap()).unwrap();
    assert_eq!(
        metadata["queue"]["interrupted_request_id"].as_str(),
        Some(child_request_id)
    );
}

#[tokio::test]
async fn steer_subagent_interrupt_drains_automated_wakeups() {
    let (db, _source) = setup_db("r4c-steer-drain").await;
    let hook = create_parent_hook(&db, "parent-drain", "session-drain").await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-drain", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    let child_session_id = child["child_session_id"].as_str().unwrap();
    update_request_state(
        db.node.as_ref(),
        child_request_id,
        "processing",
        "processing",
    )
    .await;
    let wake_request_id = "r4c-steer-drain-wake";
    create_child_session_queued_request(
        db.node.as_ref(),
        wake_request_id,
        child_session_id,
        "scheduled",
        &queue_metadata(
            "background_completion",
            "coalesce",
            Some(&format!("background_completion:{child_session_id}")),
            Some(child_request_id),
        ),
    )
    .await;

    let result = steer_subagent(
        &hook,
        "steer-drain",
        json!({
            "child_request_id": child_request_id,
            "message": "redirect",
            "interrupt": true
        }),
    )
    .await;

    let drained = result["drained_wake_up_request_ids"].as_array().unwrap();
    assert!(drained
        .iter()
        .any(|id| id.as_str() == Some(wake_request_id)));
    let wake = fetch_request(db.node.as_ref(), wake_request_id).await;
    assert_eq!(wake.status.as_deref(), Some("interrupted"));
    assert_eq!(wake.lifecycle_state.as_deref(), Some("interrupted"));
}

#[tokio::test]
async fn steer_subagent_interrupt_cascades_to_grandchild_subagents() {
    let (db, _source) = setup_db("r4c-steer-cascade").await;
    let hook = create_parent_hook(&db, "parent-cascade", "session-cascade").await;
    let parent_deadline = chrono::Utc::now() + chrono::Duration::minutes(5);
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-cascade", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap().to_string();
    let child_session_id = child["child_session_id"].as_str().unwrap().to_string();
    update_request_state(
        db.node.as_ref(),
        &child_request_id,
        "processing",
        "processing",
    )
    .await;

    let grandchild_request_id = "r4c-steer-grandchild";
    let mut descendant_bridge = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        child_request_id.clone(),
        child_session_id.clone(),
        "did:test:test".to_string(),
        "internal-steer-descendant".to_string(),
        1,
        "spawn_subagent".to_string(),
        "{}".to_string(),
        parent_deadline,
        AwaitMode::Background,
        CancelPolicy::Cascade,
        grandchild_request_id.to_string(),
        AGENT_DID.to_string(),
    );
    descendant_bridge.start_running().await.unwrap();
    let _grandchild_session_id = create_subagent_request_with_request_id_for_test(
        db.node.as_ref(),
        grandchild_request_id.to_string(),
        child_request_id.clone(),
        "internal-steer-descendant".to_string(),
        1,
        AGENT_DID.to_string(),
        CHILD_BEHAVIOR_ID.to_string(),
        "grandchild prompt".to_string(),
        Some(parent_deadline - chrono::Duration::minutes(1)),
    )
    .await
    .unwrap();

    let _ = steer_subagent(
        &hook,
        "steer-cascade",
        json!({
            "child_request_id": child_request_id,
            "message": "redirect",
            "interrupt": true
        }),
    )
    .await;

    assert!(
        fetch_interrupt_requested_at(db.node.as_ref(), grandchild_request_id)
            .await
            .unwrap()
            .is_some()
    );
}
