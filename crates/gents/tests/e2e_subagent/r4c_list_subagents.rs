use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::llm::ToolCallHookAction;
use gents::tool_call_lifecycle::ToolCallLifecycle;
use gents::{
    upsert_agent_behavior, upsert_tool_selection, AgentBehaviorDocument, DefraSessionHook,
    FailurePolicy, ToolSelectionDocument,
};
use serde_json::{json, Value};

use crate::support::fixtures::spawn_subagent_source;
use crate::support::test_db;

const AGENT_DID: &str = "did:test:r4c-list-subagents";
const PARENT_BEHAVIOR_ID: &str = "r4c-parent";
const CHILD_BEHAVIOR_ID: &str = "r4c-child";

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
    crate::support::create_agent_session(
        db.node.as_ref(),
        session_id,
        PARENT_BEHAVIOR_ID,
        "2026-05-14T00:00:00Z",
    )
    .await;
    let hook = DefraSessionHook::resume_with_identity_policy(
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

async fn list_subagents(hook: &DefraSessionHook, internal_call_id: &str, args: Value) -> Value {
    let action = hook
        .on_tool_call(
            "list_subagents",
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

async fn bridge_complete(db: &crate::support::TestDb, session_id: &str, tool_call_id: &str) {
    let mut lifecycle = ToolCallLifecycle::load(db.node.clone(), session_id, tool_call_id)
        .await
        .expect("load lifecycle")
        .expect("bridge lifecycle should exist");
    let projected = lifecycle
        .bridge_complete("child final answer".to_string())
        .await
        .expect("bridge complete");
    assert!(projected);
}

async fn create_superseded_child_edge(
    node: &EmbeddedNode,
    parent_request_id: &str,
    session_id: &str,
    tool_call_id: &str,
) {
    let child_request_id = format!("{tool_call_id}-child");
    let child_session_id = format!("{tool_call_id}-child-session");
    let parent_request_id = escape_graphql_string(parent_request_id);
    let session_id = escape_graphql_string(session_id);
    let tool_call_id = escape_graphql_string(tool_call_id);
    let child_request_id = escape_graphql_string(&child_request_id);
    let child_session_id = escape_graphql_string(&child_session_id);
    let agent_did = escape_graphql_string(AGENT_DID);
    let behavior_id = escape_graphql_string(CHILD_BEHAVIOR_ID);
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{child_request_id}",
                agent_did: "{agent_did}",
                behavior_id: "{behavior_id}",
                session_id: "{child_session_id}",
                retry_parent_request: "",
                retry_root_request: "{child_request_id}",
                superseded_by_request: "",
                content: "superseded child",
                status: "superseded",
                lifecycle_state: "superseded",
                backend_id: "",
                execution_origin: "interactive",
                metadata: "",
                failure_reason: "",
                created_at: "2026-05-14T00:01:00Z",
                deadline: "2026-05-14T00:06:00Z",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: 1,
                caused_by_parent_request_id: "{parent_request_id}",
                caused_by_parent_tool_call_id: "{tool_call_id}"
            }}) {{ _docID }}
            create_AgentToolCall(input: {{
                tool_call_key: "{session_id}:{tool_call_id}",
                request_id: "{parent_request_id}",
                session_id: "{session_id}",
                message_sequence: 1,
                tool_name: "spawn_subagent",
                tool_call_id: "{tool_call_id}",
                args: "{{}}",
                result: "",
                status: "superseded",
                lifecycle_state: "superseded",
                started_at: "2026-05-14T00:01:00Z",
                completed_at: "2026-05-14T00:02:00Z",
                deadline_at: "2026-05-14T00:06:00Z",
                await_mode: "background",
                cancel_policy: "propagate",
                child_request_id: "{child_request_id}"
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create superseded child edge failed: {:?}",
        response.errors
    );
}

#[tokio::test]
async fn list_subagents_returns_running_children() {
    let (db, _source) = setup_db("r4c-list-running").await;
    let hook = create_parent_hook(&db, "parent-running", "session-running").await;
    let child_a = spawn_background_child(db.node.as_ref(), &hook, "spawn-a", "do A").await;
    let child_b = spawn_background_child(db.node.as_ref(), &hook, "spawn-b", "do B").await;

    let result = list_subagents(&hook, "list-running", json!({})).await;
    let entries = result["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 2);
    let ids = entries
        .iter()
        .map(|entry| entry["child_request_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(ids.contains(&child_a["child_request_id"].as_str().unwrap()));
    assert!(ids.contains(&child_b["child_request_id"].as_str().unwrap()));
    for entry in entries {
        assert_eq!(entry["deployment_id"].as_str(), Some(AGENT_DID));
        assert_eq!(entry["await_mode"].as_str(), Some("background"));
        assert_eq!(entry["status"].as_str(), Some("running"));
        assert_eq!(entry["behavior_id"].as_str(), Some(CHILD_BEHAVIOR_ID));
    }
}

#[tokio::test]
async fn list_subagents_rejects_sibling_children() {
    let (db, _source) = setup_db("r4c-list-sibling").await;
    let hook_1 = create_parent_hook(&db, "parent-one", "session-one").await;
    let hook_2 = create_parent_hook(&db, "parent-two", "session-two").await;
    spawn_background_child(
        db.node.as_ref(),
        &hook_2,
        "spawn-sibling",
        "do sibling work",
    )
    .await;

    let result = list_subagents(&hook_1, "list-sibling", json!({})).await;
    let entries = result["entries"].as_array().expect("entries");
    assert!(
        entries.is_empty(),
        "parent one must not see parent two's child"
    );
}

#[tokio::test]
async fn list_subagents_status_filter() {
    let (db, _source) = setup_db("r4c-list-status").await;
    let session_id = "session-status";
    let hook = create_parent_hook(&db, "parent-status", session_id).await;
    spawn_background_child(db.node.as_ref(), &hook, "spawn-terminal", "terminal child").await;
    bridge_complete(&db, session_id, "spawn-terminal").await;

    let running = list_subagents(&hook, "list-status-running", json!({"status": "running"})).await;
    assert_eq!(running["entries"].as_array().unwrap().len(), 0);

    let terminal =
        list_subagents(&hook, "list-status-terminal", json!({"status": "terminal"})).await;
    assert_eq!(terminal["entries"].as_array().unwrap().len(), 1);
    assert_eq!(terminal["entries"][0]["status"].as_str(), Some("completed"));

    let all = list_subagents(&hook, "list-status-all", json!({"status": "all"})).await;
    assert_eq!(all["entries"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn list_subagents_terminal_includes_superseded_children() {
    let (db, _source) = setup_db("r4c-list-superseded").await;
    let session_id = "session-superseded";
    let hook = create_parent_hook(&db, "parent-superseded", session_id).await;
    create_superseded_child_edge(
        db.node.as_ref(),
        "parent-superseded",
        session_id,
        "spawn-superseded",
    )
    .await;

    let terminal = list_subagents(&hook, "list-superseded", json!({"status": "terminal"})).await;
    assert_eq!(terminal["entries"].as_array().unwrap().len(), 1);
    assert_eq!(
        terminal["entries"][0]["status"].as_str(),
        Some("superseded")
    );
}

#[tokio::test]
async fn list_subagents_limit_truncates() {
    let (db, _source) = setup_db("r4c-list-limit").await;
    let hook = create_parent_hook(&db, "parent-limit", "session-limit").await;
    for index in 0..5 {
        spawn_background_child(
            db.node.as_ref(),
            &hook,
            &format!("spawn-limit-{index}"),
            "limited child",
        )
        .await;
    }

    let result = list_subagents(&hook, "list-limit", json!({"limit": 3})).await;
    assert_eq!(result["entries"].as_array().unwrap().len(), 3);
    assert_eq!(result["truncated"].as_bool(), Some(true));
}

#[tokio::test]
async fn list_subagents_no_parent_tool_call_row_written() {
    let (db, _source) = setup_db("r4c-list-no-row").await;
    let session_id = "session-no-row";
    let hook = create_parent_hook(&db, "parent-no-row", session_id).await;
    spawn_background_child(db.node.as_ref(), &hook, "spawn-no-row", "child").await;
    let _ = list_subagents(&hook, "list-no-row", json!({})).await;

    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), session_id, "list_subagents").await,
        0
    );
}

#[tokio::test]
async fn list_subagents_lineage_matches_r4c_witness_shape() {
    let (db, _source) = setup_db("r4c-list-witness").await;
    let hook_1 = create_parent_hook(&db, "r4c-w1-caller", "r4c-w1-caller-session").await;
    let hook_2 = create_parent_hook(&db, "r4c-w1-sibling", "r4c-w1-sibling-session").await;
    let sibling_child = spawn_background_child(
        db.node.as_ref(),
        &hook_2,
        "r4c-w1-sibling-tool-call",
        "sibling child",
    )
    .await;
    let sibling_child_id = sibling_child["child_request_id"].as_str().unwrap();

    let result = list_subagents(&hook_1, "r4c-w1-list", json!({})).await;
    let entries = result["entries"].as_array().unwrap();
    assert!(
        !entries
            .iter()
            .any(|entry| entry["child_request_id"].as_str() == Some(sibling_child_id)),
        "caller must not see sibling child"
    );
}
