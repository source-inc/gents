use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::llm::tool::BoxFuture;
use gents::llm::tool::ToolDefinition;
use gents::llm::tool::{ToolDyn, ToolError};
use gents::llm::ToolCallHookAction;
use gents::tool_call_lifecycle::ToolCallLifecycle;
use gents::{BackgroundToolRegistry, DefraSessionHook, FailurePolicy};
use serde_json::{json, Value};

use crate::support::{test_db, AGENT_DID};

struct StaticTool {
    name: &'static str,
    result: &'static str,
}

impl ToolDyn for StaticTool {
    fn name(&self) -> String {
        self.name.to_string()
    }

    fn definition<'a>(&'a self, _prompt: String) -> BoxFuture<'a, ToolDefinition> {
        Box::pin(async move {
            ToolDefinition {
                name: self.name.to_string(),
                description: "test tool".to_string(),
                parameters: json!({"type":"object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        Box::pin(async move { Ok(self.result.to_string()) })
    }
}

struct PendingTool;

impl ToolDyn for PendingTool {
    fn name(&self) -> String {
        "slow_tool".to_string()
    }

    fn definition<'a>(&'a self, _prompt: String) -> BoxFuture<'a, ToolDefinition> {
        Box::pin(async {
            ToolDefinition {
                name: "slow_tool".to_string(),
                description: "test tool".to_string(),
                parameters: json!({"type":"object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        Box::pin(std::future::pending())
    }
}

async fn setup_hook(
    test_name: &str,
    registry: BackgroundToolRegistry,
) -> (crate::support::TestDb, DefraSessionHook, String, String) {
    let db = test_db(test_name).await;
    let session_id = format!("{test_name}-session");
    let request_id = format!("{test_name}-request");
    crate::support::create_request(
        db.node.as_ref(),
        &request_id,
        &session_id,
        "processing",
        "2026-05-14T00:00:00Z",
    )
    .await;
    crate::support::create_agent_session(
        db.node.as_ref(),
        &session_id,
        "r4c-background-tools",
        "2026-05-14T00:00:00Z",
    )
    .await;

    let hook = DefraSessionHook::resume_with_identity_policy(
        db.node.clone(),
        &session_id,
        "r4c-background-tools",
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .unwrap()
    .with_background_tool_registry(registry);
    hook.set_active_request_id(Some(request_id.clone())).await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::seconds(5)))
        .await;
    (db, hook, session_id, request_id)
}

fn registry(tools: Vec<Box<dyn ToolDyn>>, allowlist: &[&str]) -> BackgroundToolRegistry {
    BackgroundToolRegistry::from_tools(
        tools,
        &allowlist
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>(),
    )
}

async fn background_tool(
    hook: &DefraSessionHook,
    internal_call_id: &str,
    tool_name: &str,
) -> Value {
    background_tool_with_args(hook, internal_call_id, tool_name, json!({})).await
}

async fn background_tool_with_args(
    hook: &DefraSessionHook,
    internal_call_id: &str,
    tool_name: &str,
    args: Value,
) -> Value {
    skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            Some(format!("model-{internal_call_id}")),
            internal_call_id,
            &json!({"tool_name": tool_name, "args": args}).to_string(),
        )
        .await,
    )
}

async fn wait_tool(hook: &DefraSessionHook, internal_call_id: &str, tool_call_id: &str) -> Value {
    skip_reason_json(
        hook.on_tool_call(
            "wait_process",
            Some(format!("model-{internal_call_id}")),
            internal_call_id,
            &json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    )
}

async fn list_background_tools(
    hook: &DefraSessionHook,
    internal_call_id: &str,
    args: Value,
) -> Value {
    skip_reason_json(
        hook.on_tool_call(
            "list_processes",
            Some(format!("model-{internal_call_id}")),
            internal_call_id,
            &args.to_string(),
        )
        .await,
    )
}

fn skip_reason_json(action: ToolCallHookAction) -> Value {
    let ToolCallHookAction::Skip { reason } = action else {
        panic!("expected Skip action, got {action:?}");
    };
    serde_json::from_str(&reason).expect("skip reason should be JSON")
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

async fn create_foreground_tool_call(
    db: &crate::support::TestDb,
    request_id: &str,
    session_id: &str,
) {
    let mut lifecycle = ToolCallLifecycle::new(
        db.node.clone(),
        request_id.to_string(),
        session_id.to_string(),
        "did:test:test".to_string(),
        "foreground-call".to_string(),
        99,
        "foreground_tool".to_string(),
        "{}".to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
    );
    lifecycle.start_running().await.unwrap();
    lifecycle.complete("foreground result").await.unwrap();
}

#[tokio::test]
async fn list_background_tools_returns_running_bg_tools() {
    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r4c-list-bg-running",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    let handle_a = background_tool(&hook, "bg-a", "slow_tool").await;
    let handle_b = background_tool(&hook, "bg-b", "slow_tool").await;

    let result = list_background_tools(&hook, "list-running", json!({})).await;
    let entries = result["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 2);
    let ids = entries
        .iter()
        .map(|entry| entry["tool_call_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(ids.contains(&handle_a["tool_call_id"].as_str().unwrap()));
    assert!(ids.contains(&handle_b["tool_call_id"].as_str().unwrap()));
    for entry in entries {
        assert_eq!(entry["tool_name"].as_str(), Some("slow_tool"));
        assert_eq!(entry["await_mode"].as_str(), Some("background"));
        assert_eq!(entry["status"].as_str(), Some("running"));
        assert_eq!(entry["stdout_bytes"].as_u64(), Some(0));
        assert_eq!(entry["stderr_bytes"].as_u64(), Some(0));
    }
}

#[tokio::test]
async fn list_background_tools_reports_running_live_output_bytes() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let tools = gents::ToolSet::builder()
        .bash_unrestricted(tempdir.path())
        .build()
        .build_native_tools()
        .expect("native tools should build");
    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r4c-list-bg-live-output",
        registry(tools, &["bash_unrestricted"]),
    )
    .await;
    let handle = background_tool_with_args(
        &hook,
        "bg-live-output",
        "bash_unrestricted",
        json!({
            "command": "printf live; sleep 2; printf done",
            "args": [],
            "timeout_secs": 5
        }),
    )
    .await;
    let tool_call_id = handle["tool_call_id"].as_str().unwrap();

    let mut entry = json!({});
    for attempt in 0..40 {
        let result =
            list_background_tools(&hook, &format!("list-live-output-{attempt}"), json!({})).await;
        let entries = result["entries"].as_array().expect("entries");
        if let Some(candidate) = entries
            .iter()
            .find(|entry| entry["tool_call_id"].as_str() == Some(tool_call_id))
            .filter(|entry| entry["stdout_bytes"].as_u64().unwrap_or_default() >= 4)
        {
            entry = candidate.clone();
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert_eq!(entry["tool_name"].as_str(), Some("bash_unrestricted"));
    assert_eq!(entry["status"].as_str(), Some("running"));
    assert_eq!(entry["stdout_bytes"].as_u64(), Some(4));
    assert_eq!(entry["stderr_bytes"].as_u64(), Some(0));

    let waited = wait_tool(&hook, "wait-live-output", tool_call_id).await;
    assert_eq!(waited["status"].as_str(), Some("completed"));
}

#[tokio::test]
async fn list_background_tools_rejects_sibling_requests() {
    let db = test_db("r4c-list-bg-sibling").await;
    let (hook_1, _session_1, _request_1) = setup_hook_on_db(
        &db,
        "parent-one",
        "session-one",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    let (hook_2, _session_2, _request_2) = setup_hook_on_db(
        &db,
        "parent-two",
        "session-two",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    background_tool(&hook_2, "sibling-bg", "slow_tool").await;

    let result = list_background_tools(&hook_1, "list-sibling", json!({})).await;
    assert!(result["entries"].as_array().unwrap().is_empty());
}

async fn setup_hook_on_db(
    db: &crate::support::TestDb,
    request_id: &str,
    session_id: &str,
    registry: BackgroundToolRegistry,
) -> (DefraSessionHook, String, String) {
    crate::support::create_request(
        db.node.as_ref(),
        request_id,
        session_id,
        "processing",
        "2026-05-14T00:00:00Z",
    )
    .await;
    crate::support::create_agent_session(
        db.node.as_ref(),
        session_id,
        "r4c-background-tools",
        "2026-05-14T00:00:00Z",
    )
    .await;
    let hook = DefraSessionHook::resume_with_identity_policy(
        db.node.clone(),
        session_id,
        "r4c-background-tools",
        AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .unwrap()
    .with_background_tool_registry(registry);
    hook.set_active_request_id(Some(request_id.to_string()))
        .await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::seconds(5)))
        .await;
    (hook, session_id.to_string(), request_id.to_string())
}

#[tokio::test]
async fn list_background_tools_excludes_foreground_calls() {
    let (db, hook, session_id, request_id) = setup_hook(
        "r4c-list-bg-foreground",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    create_foreground_tool_call(&db, &request_id, &session_id).await;
    background_tool(&hook, "bg-visible", "slow_tool").await;

    let result = list_background_tools(&hook, "list-foreground", json!({})).await;
    let entries = result["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["tool_name"].as_str(), Some("slow_tool"));
}

#[tokio::test]
async fn list_background_tools_status_filter() {
    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r4c-list-bg-status",
        registry(
            vec![Box::new(StaticTool {
                name: "complete_tool",
                result: "terminal output",
            })],
            &["complete_tool"],
        ),
    )
    .await;
    let handle = background_tool(&hook, "bg-terminal", "complete_tool").await;
    let tool_call_id = handle["tool_call_id"].as_str().unwrap();
    let waited = wait_tool(&hook, "wait-terminal", tool_call_id).await;
    assert_eq!(waited["status"].as_str(), Some("completed"));

    let running =
        list_background_tools(&hook, "list-status-running", json!({"status": "running"})).await;
    assert_eq!(running["entries"].as_array().unwrap().len(), 0);

    let terminal =
        list_background_tools(&hook, "list-status-terminal", json!({"status": "terminal"})).await;
    assert_eq!(terminal["entries"].as_array().unwrap().len(), 1);
    assert_eq!(terminal["entries"][0]["status"].as_str(), Some("completed"));
    assert_eq!(
        terminal["entries"][0]["stdout_bytes"].as_u64(),
        Some("terminal output".len() as u64)
    );
}

#[tokio::test]
async fn list_background_tools_limit_truncates() {
    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r4c-list-bg-limit",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    for index in 0..5 {
        background_tool(&hook, &format!("bg-limit-{index}"), "slow_tool").await;
    }

    let result = list_background_tools(&hook, "list-limit", json!({"limit": 3})).await;
    assert_eq!(result["entries"].as_array().unwrap().len(), 3);
    assert_eq!(result["truncated"].as_bool(), Some(true));
}

#[tokio::test]
async fn list_background_tools_no_parent_tool_call_row_written() {
    let (db, hook, session_id, _request_id) = setup_hook(
        "r4c-list-bg-no-row",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    background_tool(&hook, "bg-no-row", "slow_tool").await;
    let _ = list_background_tools(&hook, "list-no-row", json!({})).await;

    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "list_processes").await,
        0
    );
}
