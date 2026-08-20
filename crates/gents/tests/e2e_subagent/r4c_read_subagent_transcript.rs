use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::llm::message::{AssistantContent, Message, Text, ToolCall, ToolFunction};
use gents::llm::ToolCallHookAction;
use gents::{
    upsert_agent_behavior, upsert_tool_selection, AgentBehaviorDocument, DefraSessionHook,
    FailurePolicy, ToolSelectionDocument,
};
use serde_json::{json, Value};

use crate::support::fixtures::spawn_subagent_source;
use crate::support::test_db;

const AGENT_DID: &str = "did:test:r4c-read-transcript";
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

async fn read_transcript(hook: &DefraSessionHook, internal_call_id: &str, args: Value) -> Value {
    let action = hook
        .on_tool_call(
            "read_subagent",
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

async fn append_message(
    node: &EmbeddedNode,
    session_id: &str,
    sequence: u32,
    role: &str,
    content: &str,
) {
    crate::support::create_agent_message(
        node,
        session_id,
        sequence,
        role,
        content,
        &format!("2026-05-14T00:00:{sequence:02}Z"),
    )
    .await;
}

async fn append_assistant_tool_call_message(
    node: &EmbeddedNode,
    session_id: &str,
    sequence: u32,
    body: &str,
    tool_call_id: &str,
    tool_name: &str,
) {
    let message = Message::Assistant {
        id: None,
        content: vec![
            AssistantContent::Text(Text {
                text: body.to_string(),
            }),
            AssistantContent::ToolCall(ToolCall {
                id: tool_call_id.to_string(),
                call_id: Some(tool_call_id.to_string()),
                function: ToolFunction {
                    name: tool_name.to_string(),
                    arguments: json!({"name": CHILD_BEHAVIOR_ID}),
                },
                signature: None,
                additional_params: None,
            }),
        ],
    };
    append_message(
        node,
        session_id,
        sequence,
        "assistant",
        &serde_json::to_string(&message).unwrap(),
    )
    .await;
}

async fn create_child_bridge_tool_call(
    node: &EmbeddedNode,
    child_request_id: &str,
    child_session_id: &str,
    message_sequence: u32,
    tool_call_id: &str,
) {
    let child_request_id = escape_graphql_string(child_request_id);
    let child_session_id = escape_graphql_string(child_session_id);
    let tool_call_id = escape_graphql_string(tool_call_id);
    let tool_call_key = format!("{child_session_id}:{tool_call_id}");
    let mutation = format!(
        r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "{tool_call_key}",
                request_id: "{child_request_id}",
                session_id: "{child_session_id}",
                message_sequence: {message_sequence},
                tool_name: "spawn_subagent",
                tool_call_id: "{tool_call_id}",
                args: "{{}}",
                result: "",
                status: "called",
                lifecycle_state: "running",
                started_at: "2026-05-14T00:01:00Z",
                deadline_at: "2026-05-14T00:06:00Z",
                await_mode: "background",
                cancel_policy: "propagate",
                child_request_id: "grandchild-request"
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create child bridge AgentToolCall failed: {:?}",
        response.errors
    );
}

async fn create_background_tool_call(
    node: &EmbeddedNode,
    child_request_id: &str,
    child_session_id: &str,
    message_sequence: u32,
    tool_call_id: &str,
) {
    let child_request_id = escape_graphql_string(child_request_id);
    let child_session_id = escape_graphql_string(child_session_id);
    let tool_call_id = escape_graphql_string(tool_call_id);
    let tool_call_key = format!("{child_session_id}:{tool_call_id}");
    let mutation = format!(
        r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "{tool_call_key}",
                request_id: "{child_request_id}",
                session_id: "{child_session_id}",
                message_sequence: {message_sequence},
                tool_name: "bash",
                tool_call_id: "{tool_call_id}",
                args: "{{}}",
                result: "",
                status: "called",
                lifecycle_state: "running",
                started_at: "2026-05-14T00:01:00Z",
                deadline_at: "2026-05-14T00:06:00Z",
                await_mode: "background",
                cancel_policy: "propagate"
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create background AgentToolCall failed: {:?}",
        response.errors
    );
}

async fn mark_child_completed(node: &EmbeddedNode, child_request_id: &str) {
    let request_id = escape_graphql_string(child_request_id);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                input: {{ status: "completed", lifecycle_state: "completed" }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "mark child completed failed: {:?}",
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

#[tokio::test]
async fn read_transcript_assistant_only_default() {
    let (db, _source) = setup_db("r4c-read-default").await;
    let hook = create_parent_hook(&db, "parent-default", "session-default").await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-default", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    let child_session_id = child["child_session_id"].as_str().unwrap();
    append_message(
        db.node.as_ref(),
        child_session_id,
        1,
        "assistant",
        "first thought",
    )
    .await;
    append_message(db.node.as_ref(), child_session_id, 2, "user", "feedback").await;
    append_message(
        db.node.as_ref(),
        child_session_id,
        3,
        "assistant",
        "second thought",
    )
    .await;

    let result = read_transcript(
        &hook,
        "read-default",
        json!({ "child_request_id": child_request_id }),
    )
    .await;
    let transcript = result["transcript"].as_str().unwrap();
    assert!(transcript.contains("first thought"));
    assert!(transcript.contains("second thought"));
    assert!(!transcript.contains("feedback"));
}

#[tokio::test]
async fn read_transcript_includes_user_when_opted_in() {
    let (db, _source) = setup_db("r4c-read-user").await;
    let hook = create_parent_hook(&db, "parent-user", "session-user").await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-user", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    let child_session_id = child["child_session_id"].as_str().unwrap();
    append_message(db.node.as_ref(), child_session_id, 1, "assistant", "a1").await;
    append_message(db.node.as_ref(), child_session_id, 2, "user", "u1").await;

    let result = read_transcript(
        &hook,
        "read-user",
        json!({
            "child_request_id": child_request_id,
            "include_user_messages": true
        }),
    )
    .await;
    let transcript = result["transcript"].as_str().unwrap();
    assert!(transcript.contains("a1"));
    assert!(transcript.contains("u1"));
}

#[tokio::test]
async fn read_transcript_hides_bridge_rows() {
    let (db, _source) = setup_db("r4c-read-bridge").await;
    let hook = create_parent_hook(&db, "parent-bridge", "session-bridge").await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-bridge", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    let child_session_id = child["child_session_id"].as_str().unwrap();
    append_assistant_tool_call_message(
        db.node.as_ref(),
        child_session_id,
        1,
        "plain assistant message",
        "bridge-tc-1",
        "spawn_subagent",
    )
    .await;
    create_child_bridge_tool_call(
        db.node.as_ref(),
        child_request_id,
        child_session_id,
        1,
        "bridge-tc-1",
    )
    .await;

    let result = read_transcript(
        &hook,
        "read-bridge",
        json!({ "child_request_id": child_request_id }),
    )
    .await;
    let transcript = result["transcript"].as_str().unwrap();
    assert!(transcript.contains("plain assistant message"));
    assert!(!transcript.contains("bridge-tc-1"));
    assert!(!transcript.contains("tool_calls="));
}

#[tokio::test]
async fn read_transcript_hides_tool_kind_background_bridge_rows() {
    let (db, _source) = setup_db("r4c-read-tool-bridge").await;
    let hook = create_parent_hook(&db, "parent-tool-bridge", "session-tool-bridge").await;
    let child =
        spawn_background_child(db.node.as_ref(), &hook, "spawn-tool-bridge", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    let child_session_id = child["child_session_id"].as_str().unwrap();
    append_assistant_tool_call_message(
        db.node.as_ref(),
        child_session_id,
        1,
        "checking files",
        "background-tc-1",
        "bash",
    )
    .await;
    create_background_tool_call(
        db.node.as_ref(),
        child_request_id,
        child_session_id,
        1,
        "background-tc-1",
    )
    .await;

    let result = read_transcript(
        &hook,
        "read-tool-bridge",
        json!({ "child_request_id": child_request_id }),
    )
    .await;
    let transcript = result["transcript"].as_str().unwrap();
    assert!(transcript.contains("checking files"));
    assert!(!transcript.contains("background-tc-1"));
    assert!(!transcript.contains("tool_calls="));
}

#[tokio::test]
async fn read_transcript_cursor_advances_cleanly() {
    let (db, _source) = setup_db("r4c-read-cursor").await;
    let hook = create_parent_hook(&db, "parent-cursor", "session-cursor").await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-cursor", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    let child_session_id = child["child_session_id"].as_str().unwrap();
    let pad = "x".repeat(120);
    for sequence in 1..=10 {
        append_message(
            db.node.as_ref(),
            child_session_id,
            sequence,
            "assistant",
            &format!("turn {sequence} {pad}"),
        )
        .await;
    }

    let mut cursor = 0u64;
    let mut pages = 0;
    let mut seen = Vec::new();
    loop {
        let page = read_transcript(
            &hook,
            &format!("read-cursor-{pages}"),
            json!({
                "child_request_id": child_request_id,
                "since_sequence": cursor,
                "max_tokens": 40
            }),
        )
        .await;
        let transcript = page["transcript"].as_str().unwrap();
        for sequence in 1..=10 {
            if transcript.contains(&format!("turn {sequence} ")) {
                seen.push(sequence);
            }
        }
        let next = page["next_sequence"].as_u64().unwrap();
        let has_more = page["has_more"].as_bool().unwrap();
        if !has_more {
            break;
        }
        assert!(
            next > cursor,
            "cursor must advance: next={next} cursor={cursor}"
        );
        cursor = next;
        pages += 1;
        assert!(pages < 50, "paging did not terminate");
    }
    assert!(pages >= 1, "small budget should force more than one page");
    seen.sort_unstable();
    assert_eq!(seen, (1..=10).collect::<Vec<u64>>(), "gap-free coverage");
}

#[tokio::test]
async fn read_transcript_terminal_flag_tracks_child_lifecycle() {
    let (db, _source) = setup_db("r4c-read-terminal").await;
    let hook = create_parent_hook(&db, "parent-terminal", "session-terminal").await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-terminal", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();
    let child_session_id = child["child_session_id"].as_str().unwrap();
    append_message(
        db.node.as_ref(),
        child_session_id,
        1,
        "assistant",
        "working",
    )
    .await;

    let running = read_transcript(
        &hook,
        "read-running",
        json!({ "child_request_id": child_request_id }),
    )
    .await;
    assert_eq!(running["terminal"].as_bool(), Some(false));
    assert_eq!(running["lifecycle_state"].as_str(), Some("pending"));

    mark_child_completed(db.node.as_ref(), child_request_id).await;
    let done = read_transcript(
        &hook,
        "read-done",
        json!({ "child_request_id": child_request_id }),
    )
    .await;
    assert_eq!(done["terminal"].as_bool(), Some(true));
    assert_eq!(done["lifecycle_state"].as_str(), Some("completed"));
    assert_eq!(done["has_more"].as_bool(), Some(false));
}

#[tokio::test]
async fn read_transcript_rejects_unauthorized_child() {
    let (db, _source) = setup_db("r4c-read-unauthorized").await;
    let hook_1 = create_parent_hook(&db, "parent-one", "session-one").await;
    let hook_2 = create_parent_hook(&db, "parent-two", "session-two").await;
    let child = spawn_background_child(db.node.as_ref(), &hook_2, "spawn-sibling", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();

    let result = read_transcript(
        &hook_1,
        "read-unauthorized",
        json!({ "child_request_id": child_request_id }),
    )
    .await;
    assert_eq!(result["ok"].as_bool(), Some(false));
    assert_eq!(result["failure_class"].as_str(), Some("tool_not_allowed"));
}

#[tokio::test]
async fn read_transcript_no_parent_tool_call_row_written() {
    let (db, _source) = setup_db("r4c-read-no-row").await;
    let parent_session_id = "session-no-row";
    let hook = create_parent_hook(&db, "parent-no-row", parent_session_id).await;
    let child = spawn_background_child(db.node.as_ref(), &hook, "spawn-no-row", "do work").await;
    let child_request_id = child["child_request_id"].as_str().unwrap();

    let _ = read_transcript(
        &hook,
        "read-no-row",
        json!({ "child_request_id": child_request_id }),
    )
    .await;
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), parent_session_id, "read_subagent").await,
        0
    );
}
