use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::llm::tool::BoxFuture;
use gents::llm::tool::ToolDefinition;
use gents::llm::tool::{ToolDyn, ToolError};
use gents::llm::ToolCallHookAction;
use gents::{
    interrupt_request, BackgroundExecutionRegistry, BackgroundToolRegistry, DefraSessionHook,
    FailurePolicy,
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

use crate::support::{first_row, test_db};

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
                parameters: serde_json::json!({"type":"object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        Box::pin(async move { Ok(self.result.to_string()) })
    }
}

struct LargeOutputTool {
    name: &'static str,
    output: String,
}

impl ToolDyn for LargeOutputTool {
    fn name(&self) -> String {
        self.name.to_string()
    }

    fn definition<'a>(&'a self, _prompt: String) -> BoxFuture<'a, ToolDefinition> {
        Box::pin(async move {
            ToolDefinition {
                name: self.name.to_string(),
                description: "test tool".to_string(),
                parameters: serde_json::json!({"type":"object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        let output = self.output.clone();
        Box::pin(async move { Ok(output) })
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
                parameters: serde_json::json!({"type":"object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        Box::pin(std::future::pending())
    }
}

struct PanickingTool;

impl ToolDyn for PanickingTool {
    fn name(&self) -> String {
        "panicking_tool".to_string()
    }

    fn definition<'a>(&'a self, _prompt: String) -> BoxFuture<'a, ToolDefinition> {
        Box::pin(async {
            ToolDefinition {
                name: "panicking_tool".to_string(),
                description: "test tool that panics".to_string(),
                parameters: serde_json::json!({"type":"object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        Box::pin(async { panic!("intentional background tool panic") })
    }
}

struct ConcurrentGateTool {
    entered: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

impl ToolDyn for ConcurrentGateTool {
    fn name(&self) -> String {
        "concurrent_tool".to_string()
    }

    fn definition<'a>(&'a self, _prompt: String) -> BoxFuture<'a, ToolDefinition> {
        Box::pin(async {
            ToolDefinition {
                name: "concurrent_tool".to_string(),
                description: "test tool".to_string(),
                parameters: serde_json::json!({"type":"object"}),
            }
        })
    }

    fn call<'a>(&'a self, _args: String) -> BoxFuture<'a, Result<String, ToolError>> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            entered.fetch_add(1, Ordering::SeqCst);
            release.notified().await;
            Ok("done".to_string())
        })
    }
}

#[derive(Debug, Deserialize)]
struct ToolCallRow {
    tool_name: Option<String>,
    result: Option<String>,
    lifecycle_state: Option<String>,
    cancel_cause: Option<String>,
    await_mode: Option<String>,
    child_request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageRow {
    content: String,
    request_id: Option<String>,
    request_doc_id: Option<String>,
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
        "r6-background",
        "2026-05-14T00:00:00Z",
    )
    .await;

    let hook = DefraSessionHook::resume_with_identity_policy(
        db.node.clone(),
        &session_id,
        "r6-background",
        crate::support::AGENT_DID,
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

async fn fetch_messages(node: &EmbeddedNode, session_id: &str) -> Vec<MessageRow> {
    let session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentMessage(
                filter: {{ session_id: {{ _eq: "{session_id}" }} }},
                order: {{ sequence: ASC }}
            ) {{ content request_id request_doc_id }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "fetch AgentMessage rows failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentMessage"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
}

async fn wait_for_tool_completion_message(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
) -> MessageRow {
    let marker = format!(r#"<tool-completion tool_call_id="{tool_call_id}""#);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Some(message) = fetch_messages(node, session_id)
            .await
            .into_iter()
            .find(|message| message.content.contains(&marker))
        {
            return message;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("tool completion message for {tool_call_id} was not appended");
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn fetch_background_wakes(node: &EmbeddedNode, session_id: &str) -> Vec<serde_json::Value> {
    let session_id = escape_graphql_string(session_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    execution_origin: {{ _eq: "scheduled" }}
                }}
            ) {{ _docID request_id metadata }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "fetch background wake rows failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
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

fn skip_reason_json(action: ToolCallHookAction) -> Value {
    let ToolCallHookAction::Skip { reason } = action else {
        panic!("expected Skip action, got {action:?}");
    };
    serde_json::from_str(&reason).expect("skip reason should be JSON")
}

async fn load_tool_call(node: &EmbeddedNode, session_id: &str, tool_call_id: &str) -> ToolCallRow {
    let session_id = escape_graphql_string(session_id);
    let tool_call_id = escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    tool_call_id: {{ _eq: "{tool_call_id}" }}
                }}
                limit: 1
            ) {{
                tool_name
                result
                lifecycle_state
                cancel_cause
                await_mode
                child_request_id
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentToolCall")
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
            ) {{
                tool_call_id
            }}
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
        .and_then(|value| value.as_array())
        .map_or(0, Vec::len)
}

#[tokio::test]
async fn background_tool_success_returns_handle_and_wait_tool_returns_terminal_envelope() {
    let (db, hook, session_id, request_id) = setup_hook(
        "r6-background-success",
        registry(
            vec![Box::new(StaticTool {
                name: "test_tool",
                result: "done",
            })],
            &["test_tool"],
        ),
    )
    .await;

    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-1",
            r#"{"tool_name":"test_tool","args":{"x":1}}"#,
        )
        .await,
    );
    assert_eq!(receipt["status"], "running");
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    let waited = skip_reason_json(
        hook.on_tool_call(
            "wait_process",
            None,
            "meta-wait-1",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(waited["status"], "completed");
    assert_eq!(waited["result"], "done");

    let row = load_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert_eq!(row.tool_name.as_deref(), Some("test_tool"));
    assert_eq!(row.lifecycle_state.as_deref(), Some("completed"));
    assert_eq!(row.await_mode.as_deref(), Some("background"));
    assert_eq!(row.child_request_id.as_deref(), None);
    assert_eq!(row.result.as_deref(), Some("done"));
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "wait_process").await,
        0
    );

    let message =
        wait_for_tool_completion_message(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert!(message.content.contains(r#"tool_name="test_tool""#));
    assert!(message.content.contains(r#"status="completed""#));
    assert!(message.content.contains("<result>done</result>"));
    let wakes = fetch_background_wakes(db.node.as_ref(), &session_id).await;
    assert_eq!(
        wakes.len(),
        1,
        "tool completion notification should enqueue one resumable agent turn"
    );
    let wake_request_id = wakes[0]["request_id"].as_str().unwrap();
    let wake_doc_id = wakes[0]["_docID"].as_str().unwrap();
    assert_ne!(wake_request_id, request_id);
    assert_eq!(message.request_id.as_deref(), Some(wake_request_id));
    assert_eq!(message.request_doc_id.as_deref(), Some(wake_doc_id));
}

// #985: a backgrounded bash run's lifetime budget is decoupled from both the
// parent request deadline and the foreground command ceiling — the execution
// must complete (and notify) even though the parent deadline expires while it
// is still running.
#[tokio::test]
async fn background_tool_execution_survives_parent_request_deadline() {
    let bash_tools = gents::ToolSet::builder()
        .bash_read_only_with_policy_and_timeouts(
            gents::CommandExecutionPolicy::read_only(vec!["sleep".to_string()]),
            std::time::Duration::from_secs(120),
            std::time::Duration::from_secs(120),
        )
        .build()
        .build_native_tools()
        .unwrap();
    let (db, hook, session_id, _request_id) = setup_hook(
        "r6-background-outlives-deadline",
        registry(bash_tools, &["bash"]),
    )
    .await;
    hook.set_request_deadline_at(Some(
        chrono::Utc::now() + chrono::Duration::milliseconds(200),
    ))
    .await;

    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-outlive",
            r#"{"tool_name":"bash","args":{"command":"sleep","args":["0.7"]}}"#,
        )
        .await,
    );
    assert_eq!(receipt["status"], "running");
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    // The tool outlives both the parent deadline (200ms) and the shared
    // 500ms wait helper; poll long enough for the 700ms sleep to finish.
    let marker = format!(r#"<tool-completion tool_call_id="{tool_call_id}""#);
    let mut message = None;
    for _ in 0..60 {
        if let Some(found) = fetch_messages(db.node.as_ref(), &session_id)
            .await
            .into_iter()
            .find(|message| message.content.contains(&marker))
        {
            message = Some(found);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let message = message.expect("background tool completion message was not appended");
    assert!(
        message.content.contains(r#"status="completed""#),
        "background tool must outlive the parent request deadline; got: {}",
        message.content
    );

    let row = load_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert_eq!(row.lifecycle_state.as_deref(), Some("completed"));
}

// #985: wait_process is a bounded wait — on timeout it reports the process
// as still running without cancelling it, so a model that waits cannot pin
// the session (or kill the job) until the parent request deadline.
#[tokio::test]
async fn wait_process_bounded_wait_returns_still_running_without_cancelling() {
    let (db, hook, session_id, _request_id) = setup_hook(
        "r6-background-wait-bounded",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;

    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-bounded",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    assert_eq!(receipt["status"], "running");
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    let started = std::time::Instant::now();
    let waited = skip_reason_json(
        hook.on_tool_call(
            "wait_process",
            None,
            "meta-wait-bounded",
            &serde_json::json!({ "tool_call_id": tool_call_id, "timeout_secs": 1 }).to_string(),
        )
        .await,
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(4),
        "bounded wait must return promptly, took {:?}",
        started.elapsed()
    );
    assert_eq!(waited["status"], "running");
    assert_eq!(waited["error"]["reason"], "wait_timeout");

    let row = load_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert_eq!(
        row.lifecycle_state.as_deref(),
        Some("running"),
        "wait timeout must not cancel the background process"
    );
    assert_eq!(row.cancel_cause.as_deref(), None);
}

#[tokio::test]
async fn periodic_recovery_does_not_terminalize_registered_background_worker() {
    let (db, hook, session_id, request_id) = setup_hook(
        "r6-background-periodic-live-owner",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    let executions = BackgroundExecutionRegistry::default();
    let hook = hook.with_background_execution_registry(executions.clone());

    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-periodic-live-owner",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    set_parent_state(db.node.as_ref(), &request_id, "completed", "completed").await;
    let _runs = gents::run_periodic_recovery_sweeps(
        db.node.as_ref(),
        crate::support::AGENT_DID,
        &executions,
    )
    .await
    .unwrap();
    assert_eq!(
        load_tool_call(db.node.as_ref(), &session_id, &tool_call_id)
            .await
            .lifecycle_state
            .as_deref(),
        Some("running")
    );

    let _ = hook
        .on_tool_call(
            "cancel_process",
            None,
            "meta-bg-periodic-live-owner-cleanup",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await;
}

#[tokio::test]
async fn periodic_recovery_preserves_registered_worker_after_parent_interrupt() {
    let (db, hook, session_id, request_id) = setup_hook(
        "r6-background-periodic-interrupted-owner",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    let executions = BackgroundExecutionRegistry::default();
    let hook = hook.with_background_execution_registry(executions.clone());
    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-periodic-interrupted-owner",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    set_parent_state(db.node.as_ref(), &request_id, "interrupted", "interrupted").await;
    gents::run_periodic_recovery_sweeps(db.node.as_ref(), crate::support::AGENT_DID, &executions)
        .await
        .unwrap();
    assert_eq!(
        load_tool_call(db.node.as_ref(), &session_id, &tool_call_id)
            .await
            .lifecycle_state
            .as_deref(),
        Some("running")
    );

    let _ = hook
        .on_tool_call(
            "cancel_process",
            None,
            "meta-bg-periodic-interrupted-owner-cleanup",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await;
}

#[tokio::test]
async fn periodic_recovery_applies_deadline_before_terminal_parent_to_orphan() {
    let (db, _hook, session_id, request_id) = setup_hook(
        "r6-background-periodic-deadline-precedence",
        registry(Vec::new(), &[]),
    )
    .await;
    let tool_call_id = "expired-terminal-parent-orphan";
    let mut lifecycle = gents::tool_call_lifecycle::ToolCallLifecycle::new_background_tool(
        db.node.clone(),
        request_id.clone(),
        session_id.clone(),
        crate::support::AGENT_DID.to_string(),
        tool_call_id.to_string(),
        1,
        "slow_tool".to_string(),
        "{}".to_string(),
        chrono::Utc::now() - chrono::Duration::seconds(1),
    );
    lifecycle.start_running().await.unwrap();
    set_parent_state(db.node.as_ref(), &request_id, "completed", "completed").await;

    gents::run_periodic_recovery_sweeps(
        db.node.as_ref(),
        crate::support::AGENT_DID,
        &BackgroundExecutionRegistry::default(),
    )
    .await
    .unwrap();
    let row = load_tool_call(db.node.as_ref(), &session_id, tool_call_id).await;
    assert_eq!(row.lifecycle_state.as_deref(), Some("timedOut"));
    assert_eq!(row.cancel_cause.as_deref(), Some("deadline"));
}

#[tokio::test]
async fn malformed_running_row_does_not_hide_valid_orphan_recovery() {
    let (db, _hook, session_id, request_id) = setup_hook(
        "r6-background-malformed-recovery-row",
        registry(Vec::new(), &[]),
    )
    .await;
    let tool_call_id = "valid-orphan";
    let mut lifecycle = gents::tool_call_lifecycle::ToolCallLifecycle::new_background_tool(
        db.node.clone(),
        request_id,
        session_id.clone(),
        crate::support::AGENT_DID.to_string(),
        tool_call_id.to_string(),
        1,
        "slow_tool".to_string(),
        "{}".to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
    );
    lifecycle.start_running().await.unwrap();

    let malformed = format!(
        r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "malformed-recovery-row",
                agent_did: "{}",
                lifecycle_state: "running",
                await_mode: "background"
            }}) {{ _docID }}
        }}"#,
        escape_graphql_string(crate::support::AGENT_DID)
    );
    let response = db.node.execute(&malformed).await;
    assert!(
        !response.has_errors(),
        "failed to seed malformed recovery row: {:?}",
        response.errors
    );

    let report =
        gents::tool_call_lifecycle::ToolCallLifecycle::reconcile_orphaned_background_tools(
            db.node.as_ref(),
            crate::support::AGENT_DID,
            &BackgroundExecutionRegistry::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.tool_calls_terminalized, 1);
    assert_eq!(
        load_tool_call(db.node.as_ref(), &session_id, tool_call_id)
            .await
            .lifecycle_state
            .as_deref(),
        Some("cancelled")
    );
}

async fn set_parent_state(
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
                input: {{ status: "{status}", lifecycle_state: "{lifecycle_state}" }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "failed to terminalize spawning request: {:?}",
        response.errors
    );
}

#[tokio::test]
async fn panicking_background_tool_terminalizes_and_notifies() {
    let (db, hook, session_id, _request_id) = setup_hook(
        "r6-background-panic",
        registry(vec![Box::new(PanickingTool)], &["panicking_tool"]),
    )
    .await;

    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-panic",
            r#"{"tool_name":"panicking_tool","args":{}}"#,
        )
        .await,
    );
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let row = loop {
        let row = load_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
        if row.lifecycle_state.as_deref() != Some("running") {
            break row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "panicking background tool remained durably running"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    assert_eq!(row.lifecycle_state.as_deref(), Some("failed"));
    assert!(row
        .result
        .as_deref()
        .is_some_and(|result| { result.contains("intentional background tool panic") }));

    // The notification append is a separate mutation after the terminal row
    // write (the row carries `completionPending:tool_panicked` until the side
    // effects land), so await it rather than reading messages once.
    let notification =
        wait_for_tool_completion_message(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert!(notification.content.contains(r#"status="failed""#));
    assert!(notification
        .content
        .contains("<reason>tool_panicked</reason>"));
}

#[tokio::test]
async fn wait_envelope_bounds_oversized_background_tool_result() {
    let big_line = "x".repeat(200);
    let big_output = std::iter::repeat(big_line)
        .take(5_000)
        .collect::<Vec<_>>()
        .join("\n");
    let full_len = big_output.len();
    let (db, hook, session_id, _request_id) = setup_hook(
        "r6-background-bounded",
        registry(
            vec![Box::new(LargeOutputTool {
                name: "big_tool",
                output: big_output,
            })],
            &["big_tool"],
        ),
    )
    .await;

    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-big",
            r#"{"tool_name":"big_tool","args":{}}"#,
        )
        .await,
    );
    assert_eq!(receipt["status"], "running");
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    let waited = skip_reason_json(
        hook.on_tool_call(
            "wait_process",
            None,
            "meta-wait-big",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(waited["status"], "completed");
    let envelope_result = waited["result"].as_str().expect("envelope result string");
    assert!(
        envelope_result.len() < full_len,
        "wait envelope must bound the model-facing result: envelope={} full={}",
        envelope_result.len(),
        full_len
    );
    assert!(
        !envelope_result.is_empty(),
        "bounded result must be non-empty"
    );

    let row = load_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert_eq!(row.lifecycle_state.as_deref(), Some("completed"));
    assert_eq!(
        row.result.as_deref().map(str::len),
        Some(full_len),
        "the AgentToolCall row must keep the full output"
    );
}

#[tokio::test]
async fn background_tool_rejects_not_allowlisted_target() {
    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r6-background-not-allowed",
        registry(
            vec![Box::new(StaticTool {
                name: "test_tool",
                result: "done",
            })],
            &["test_tool"],
        ),
    )
    .await;

    let error = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-denied",
            r#"{"tool_name":"other_tool","args":{}}"#,
        )
        .await,
    );
    assert_eq!(error["failure_class"], "tool_not_allowed");
    assert_eq!(error["requested_tool_name"], "other_tool");
}

#[tokio::test]
async fn background_tool_rejects_when_parent_budget_is_exhausted() {
    let (db, hook, session_id, _request_id) = setup_hook(
        "r6-background-budget",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;

    for index in 0..8 {
        let receipt = skip_reason_json(
            hook.on_tool_call(
                "spawn_process",
                None,
                &format!("meta-bg-budget-{index}"),
                r#"{"tool_name":"slow_tool","args":{}}"#,
            )
            .await,
        );
        assert_eq!(receipt["status"], "running");
    }

    let denied = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-budget-denied",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    assert_eq!(denied["code"], "background_tool_budget_exceeded");
    assert_eq!(denied["current_backgrounded"], 8);
    assert_eq!(denied["max_backgrounded"], 8);
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "slow_tool").await,
        8
    );
}

#[tokio::test]
async fn wait_tool_caller_deadline_returns_without_cancelling_background_row() {
    let (db, hook, session_id, _request_id) = setup_hook(
        "r6-background-wait-deadline",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;

    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-wait-deadline",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    hook.set_request_deadline_at(Some(chrono::Utc::now() - chrono::Duration::milliseconds(1)))
        .await;

    let waited = skip_reason_json(
        hook.on_tool_call(
            "wait_process",
            None,
            "meta-wait-deadline",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(waited["status"], "running");
    assert_eq!(waited["error"]["reason"], "caller_deadline_exceeded");

    let row = load_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert_eq!(row.lifecycle_state.as_deref(), Some("running"));
    assert_eq!(row.cancel_cause.as_deref(), None);
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "wait_process").await,
        0
    );
}

#[tokio::test]
async fn wait_tool_caller_interrupt_returns_without_cancelling_background_row() {
    let (db, hook, session_id, request_id) = setup_hook(
        "r6-background-wait-interrupt",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;

    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-interrupt",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();
    interrupt_request(db.node.as_ref(), &request_id)
        .await
        .expect("interrupt caller request");

    let waited = skip_reason_json(
        hook.on_tool_call(
            "wait_process",
            None,
            "meta-wait-interrupt",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(waited["status"], "running");
    assert_eq!(waited["error"]["reason"], "caller_interrupted");

    let row = load_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert_eq!(row.lifecycle_state.as_deref(), Some("running"));
    assert_eq!(row.cancel_cause.as_deref(), None);
}

#[tokio::test]
async fn process_controls_manage_same_principal_job_across_request_turns() {
    let (db, hook, session_id, origin_request_id) = setup_hook(
        "r6-background-cross-turn-controls",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    let requester_did = "did:key:same-requester";
    hook.set_active_request_lineage(Some(origin_request_id), Some(requester_did.to_string()))
        .await
        .unwrap();

    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-cross-turn",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    let next_request_id = "r6-background-cross-turn-controls-request-2".to_string();
    crate::support::create_request(
        db.node.as_ref(),
        &next_request_id,
        &session_id,
        "processing",
        "2026-05-14T00:00:01Z",
    )
    .await;
    hook.set_active_request_lineage(Some(next_request_id), Some(requester_did.to_string()))
        .await
        .unwrap();

    let listed = skip_reason_json(
        hook.on_tool_call("list_processes", None, "meta-list-cross-turn", r#"{}"#)
            .await,
    );
    assert!(listed["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["tool_call_id"] == tool_call_id));

    let read = skip_reason_json(
        hook.on_tool_call(
            "read_process",
            None,
            "meta-read-cross-turn",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(read["status"], "running");

    hook.set_request_deadline_at(Some(chrono::Utc::now() - chrono::Duration::milliseconds(1)))
        .await;
    let waited = skip_reason_json(
        hook.on_tool_call(
            "wait_process",
            None,
            "meta-wait-cross-turn",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(waited["status"], "running");
    assert_eq!(waited["error"]["reason"], "caller_deadline_exceeded");

    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::seconds(5)))
        .await;
    let cancelled = skip_reason_json(
        hook.on_tool_call(
            "cancel_process",
            None,
            "meta-cancel-cross-turn",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(cancelled["status"], "cancelled");
}

#[tokio::test]
async fn process_controls_deny_different_requester_in_same_session() {
    let (db, hook, session_id, request_id) = setup_hook(
        "r6-background-cross-requester-denied",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    hook.set_active_request_lineage(Some(request_id.clone()), Some("did:key:owner".to_string()))
        .await
        .unwrap();
    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-cross-requester",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    let other_request_id = format!("{request_id}-other");
    crate::support::create_request(
        db.node.as_ref(),
        &other_request_id,
        &session_id,
        "processing",
        "2026-05-14T00:00:02Z",
    )
    .await;
    hook.set_active_request_lineage(Some(other_request_id), Some("did:key:other".to_string()))
        .await
        .unwrap();
    let listed = skip_reason_json(
        hook.on_tool_call("list_processes", None, "meta-list-cross-requester", r#"{}"#)
            .await,
    );
    assert!(listed["entries"].as_array().unwrap().is_empty());
    let denied = skip_reason_json(
        hook.on_tool_call(
            "read_process",
            None,
            "meta-read-cross-requester",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(denied["ok"], false);

    let wait_denied = skip_reason_json(
        hook.on_tool_call(
            "wait_process",
            None,
            "meta-wait-cross-requester",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(wait_denied["ok"], false);
    let cancel_denied = skip_reason_json(
        hook.on_tool_call(
            "cancel_process",
            None,
            "meta-cancel-cross-requester",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(cancel_denied["ok"], false);
    let row = load_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert_eq!(row.lifecycle_state.as_deref(), Some("running"));
    assert_eq!(row.cancel_cause.as_deref(), None);

    hook.set_active_request_lineage(Some(request_id), Some("did:key:owner".to_string()))
        .await
        .unwrap();
    let _ = hook
        .on_tool_call(
            "cancel_process",
            None,
            "meta-cleanup-cross-requester",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await;
    let row = load_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert_eq!(row.lifecycle_state.as_deref(), Some("cancelled"));
}

#[tokio::test]
async fn list_processes_skips_malformed_legacy_rows_without_hiding_valid_jobs() {
    let (db, hook, session_id, request_id) = setup_hook(
        "r6-background-list-malformed-rows",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-valid-row",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    let valid_tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    let escaped_session_id = escape_graphql_string(&session_id);
    let escaped_request_id = escape_graphql_string(&request_id);
    let escaped_agent_did = escape_graphql_string(crate::support::AGENT_DID);
    let malformed_rows = format!(
        r#"mutation {{
            null_identity: create_AgentToolCall(input: {{
                tool_call_key: "malformed-null-identity-{escaped_session_id}",
                session_id: "{escaped_session_id}",
                await_mode: "background",
                lifecycle_state: "running",
                started_at: "2026-05-14T00:00:02Z"
            }}) {{ _docID }}
            no_start: create_AgentToolCall(input: {{
                tool_call_key: "malformed-no-start-{escaped_session_id}",
                tool_call_id: "malformed-no-start",
                tool_name: "slow_tool",
                request_id: "{escaped_request_id}",
                session_id: "{escaped_session_id}",
                agent_did: "{escaped_agent_did}",
                await_mode: "background",
                lifecycle_state: "running"
            }}) {{ _docID }}
        }}"#,
    );
    let response = db.node.execute(&malformed_rows).await;
    assert!(
        !response.has_errors(),
        "seed malformed AgentToolCall rows failed: {:?}",
        response.errors
    );

    let listed = skip_reason_json(
        hook.on_tool_call("list_processes", None, "meta-list-malformed", r#"{}"#)
            .await,
    );
    let entries = listed["entries"]
        .as_array()
        .expect("list_processes must return entries despite malformed rows");
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0]["tool_call_id"].as_str(),
        Some(valid_tool_call_id.as_str())
    );

    let _ = hook
        .on_tool_call(
            "cancel_process",
            None,
            "meta-cleanup-malformed",
            &serde_json::json!({ "tool_call_id": valid_tool_call_id }).to_string(),
        )
        .await;
}

#[tokio::test]
async fn same_tool_background_calls_execute_concurrently_without_registry_mutex() {
    let entered = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r6-background-concurrent-tool",
        registry(
            vec![Box::new(ConcurrentGateTool {
                entered: entered.clone(),
                release: release.clone(),
            })],
            &["concurrent_tool"],
        ),
    )
    .await;

    let first = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-concurrent-1",
            r#"{"tool_name":"concurrent_tool","args":{}}"#,
        )
        .await,
    );
    let second = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-concurrent-2",
            r#"{"tool_name":"concurrent_tool","args":{}}"#,
        )
        .await,
    );

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while entered.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both calls to the same tool must enter concurrently");
    release.notify_waiters();

    for (index, receipt) in [first, second].into_iter().enumerate() {
        let waited = skip_reason_json(
            hook.on_tool_call(
                "wait_process",
                None,
                &format!("meta-wait-concurrent-{index}"),
                &serde_json::json!({ "tool_call_id": receipt["tool_call_id"] }).to_string(),
            )
            .await,
        );
        assert_eq!(waited["status"], "completed");
    }
}

#[tokio::test]
async fn cancel_tool_cancels_running_background_row_without_persisting_cancel_tool_call() {
    let (db, hook, session_id, _request_id) = setup_hook(
        "r6-background-cancel",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;

    let receipt = skip_reason_json(
        hook.on_tool_call(
            "spawn_process",
            None,
            "meta-bg-slow",
            r#"{"tool_name":"slow_tool","args":{}}"#,
        )
        .await,
    );
    let tool_call_id = receipt["tool_call_id"].as_str().unwrap().to_string();

    let cancelled = skip_reason_json(
        hook.on_tool_call(
            "cancel_process",
            None,
            "meta-cancel-slow",
            &serde_json::json!({ "tool_call_id": tool_call_id }).to_string(),
        )
        .await,
    );
    assert_eq!(cancelled["status"], "cancelled");

    let row = load_tool_call(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert_eq!(row.lifecycle_state.as_deref(), Some("cancelled"));
    assert_eq!(row.cancel_cause.as_deref(), Some("userCancelled"));
    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "cancel_process").await,
        0
    );

    let message =
        wait_for_tool_completion_message(db.node.as_ref(), &session_id, &tool_call_id).await;
    assert!(message.content.contains(r#"status="cancelled""#));
    assert!(message.content.contains("<reason>explicit_cancel</reason>"));

    assert_eq!(
        fetch_background_wakes(db.node.as_ref(), &session_id)
            .await
            .len(),
        1,
        "tool cancellation notification should enqueue one resumable agent turn"
    );
}

#[tokio::test]
async fn cancel_tool_unknown_handle_returns_tool_error_instead_of_failing_turn() {
    let (_db, hook, _session_id, _request_id) =
        setup_hook("r6-background-cancel-missing", registry(Vec::new(), &[])).await;

    let cancelled = skip_reason_json(
        hook.on_tool_call(
            "cancel_process",
            None,
            "meta-cancel-missing",
            r#"{"tool_call_id":"missing-background-handle"}"#,
        )
        .await,
    );

    assert_eq!(cancelled["ok"], false);
    assert_eq!(cancelled["tool_name"], "cancel_process");
    assert!(cancelled["message"]
        .as_str()
        .unwrap()
        .contains("missing-background-handle"));
}

#[tokio::test]
async fn wait_tool_unknown_handle_returns_tool_error_instead_of_failing_turn() {
    let (_db, hook, _session_id, _request_id) =
        setup_hook("r6-background-wait-missing", registry(Vec::new(), &[])).await;

    let waited = skip_reason_json(
        hook.on_tool_call(
            "wait_process",
            None,
            "meta-wait-missing",
            r#"{"tool_call_id":"missing-background-handle"}"#,
        )
        .await,
    );

    assert_eq!(waited["ok"], false);
    assert_eq!(waited["tool_name"], "wait_process");
    assert!(waited["message"]
        .as_str()
        .unwrap()
        .contains("missing-background-handle"));
}
