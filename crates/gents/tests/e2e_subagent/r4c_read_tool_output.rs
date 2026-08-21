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
    result: String,
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
        Box::pin(async move { Ok(self.result.clone()) })
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
        "r4c-read-tool-output",
        "2026-05-14T00:00:00Z",
    )
    .await;

    let hook = DefraSessionHook::resume_with_identity_policy(
        db.node.clone(),
        &session_id,
        "r4c-read-tool-output",
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
        "r4c-read-tool-output",
        "2026-05-14T00:00:00Z",
    )
    .await;
    let hook = DefraSessionHook::resume_with_identity_policy(
        db.node.clone(),
        session_id,
        "r4c-read-tool-output",
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

async fn read_tool_output(hook: &DefraSessionHook, internal_call_id: &str, args: Value) -> Value {
    skip_reason_json(
        hook.on_tool_call(
            "read_process",
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

async fn create_foreground_tool_call(
    db: &crate::support::TestDb,
    request_id: &str,
    session_id: &str,
) -> String {
    let tool_call_id = "foreground-call".to_string();
    let mut lifecycle = ToolCallLifecycle::new(
        db.node.clone(),
        request_id.to_string(),
        session_id.to_string(),
        "did:test:test".to_string(),
        tool_call_id.clone(),
        99,
        "foreground_tool".to_string(),
        "{}".to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
    );
    lifecycle.start_running().await.unwrap();
    lifecycle.complete("foreground result").await.unwrap();
    tool_call_id
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
async fn read_tool_output_running_returns_live_stream_tail() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let tools = gents::ToolSet::builder()
        .bash_unrestricted(tempdir.path())
        .build()
        .build_native_tools()
        .expect("native tools should build");
    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r4c-read-output-running",
        registry(tools, &["bash_unrestricted"]),
    )
    .await;
    let handle = background_tool_with_args(
        &hook,
        "bg-running",
        "bash_unrestricted",
        json!({
            "command": "printf live; sleep 2; printf done",
            "args": [],
            "timeout_secs": 5
        }),
    )
    .await;
    let tool_call_id = handle["tool_call_id"].as_str().unwrap();

    let mut result = json!({});
    for attempt in 0..40 {
        result = read_tool_output(
            &hook,
            &format!("read-running-{attempt}"),
            json!({ "tool_call_id": tool_call_id }),
        )
        .await;
        if result["output"]
            .as_str()
            .is_some_and(|output| output.contains("live"))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert_eq!(result["status"].as_str(), Some("running"));
    assert_eq!(result["tool_name"].as_str(), Some("bash_unrestricted"));
    assert_eq!(result["output"].as_str(), Some("live"));
    assert_eq!(result["next_offset"].as_u64(), Some(4));
    assert_eq!(result["total_bytes"].as_u64(), Some(4));
    assert_eq!(result["has_more"].as_bool(), Some(false));
    assert_eq!(result["exited"].as_bool(), Some(false));
    assert!(result["exit_code"].is_null());

    let waited = wait_tool(&hook, "wait-running-terminal", tool_call_id).await;
    assert_eq!(waited["status"].as_str(), Some("completed"));
    let terminal = read_tool_output(
        &hook,
        "read-running-terminal",
        json!({ "tool_call_id": tool_call_id }),
    )
    .await;
    assert_eq!(terminal["status"].as_str(), Some("completed"));
    assert_eq!(terminal["output"].as_str(), Some("livedone"));
    assert_eq!(terminal["total_bytes"].as_u64(), Some(8));
    assert_eq!(terminal["exited"].as_bool(), Some(true));
}

#[tokio::test]
async fn read_tool_output_terminal_reads_persisted_result() {
    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r4c-read-output-terminal",
        registry(
            vec![Box::new(StaticTool {
                name: "complete_tool",
                result: "done\n".to_string(),
            })],
            &["complete_tool"],
        ),
    )
    .await;
    let handle = background_tool(&hook, "bg-terminal", "complete_tool").await;
    let tool_call_id = handle["tool_call_id"].as_str().unwrap();
    let waited = wait_tool(&hook, "wait-terminal", tool_call_id).await;
    assert_eq!(waited["status"].as_str(), Some("completed"));

    let result = read_tool_output(
        &hook,
        "read-terminal",
        json!({ "tool_call_id": tool_call_id }),
    )
    .await;
    assert_eq!(result["status"].as_str(), Some("completed"));
    assert_eq!(result["output"].as_str(), Some("done\n"));
    assert_eq!(result["total_bytes"].as_u64(), Some(5));
    assert_eq!(result["next_offset"].as_u64(), Some(5));
    assert_eq!(result["has_more"].as_bool(), Some(false));
    assert_eq!(result["exited"].as_bool(), Some(true));
    assert!(result["exit_code"].is_null());
}

#[tokio::test]
async fn read_tool_output_terminal_parses_native_command_streams() {
    let persisted = concat!(
        "gents_exec: {\"ok\":false,\"status\":\"exit_nonzero\",",
        "\"command\":\"grep -P foo README.md\",\"argv\":[\"grep\",\"-P\",\"foo\",\"README.md\"],",
        "\"cwd\":\".\",\"exit_code\":2,\"timed_out\":false,\"duration_ms\":4,",
        "\"timeout_ms\":10000,\"execution_mode\":\"read_only\",",
        "\"network_mode\":\"inherit\",\"sandbox\":\"policy_read_only\",",
        "\"stdout_truncation\":{\"returned_bytes\":7,\"total_bytes\":7,",
        "\"max_bytes\":16000,\"truncated\":false},",
        "\"stderr_truncation\":{\"returned_bytes\":25,\"total_bytes\":25,",
        "\"max_bytes\":16000,\"truncated\":false}}\n",
        "stdout:\n",
        "matches\n",
        "stderr:\n",
        "grep: invalid option -- P"
    );
    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r4c-read-output-native",
        registry(
            vec![Box::new(StaticTool {
                name: "bash",
                result: persisted.to_string(),
            })],
            &["bash"],
        ),
    )
    .await;
    let handle = background_tool(&hook, "bg-native", "bash").await;
    let tool_call_id = handle["tool_call_id"].as_str().unwrap();
    let waited = wait_tool(&hook, "wait-native", tool_call_id).await;
    assert_eq!(waited["status"].as_str(), Some("completed"));

    let result = read_tool_output(
        &hook,
        "read-native",
        json!({ "tool_call_id": tool_call_id }),
    )
    .await;
    let output = result["output"].as_str().unwrap();
    assert_eq!(output, "matches\n--- stderr ---\ngrep: invalid option -- P");
    assert_eq!(result["total_bytes"].as_u64(), Some(output.len() as u64));
    assert_eq!(result["next_offset"].as_u64(), Some(output.len() as u64));
    assert_eq!(result["has_more"].as_bool(), Some(false));
    assert_eq!(result["exited"].as_bool(), Some(true));
    assert_eq!(result["exit_code"].as_i64(), Some(2));
}

#[tokio::test]
async fn read_tool_output_pages_gap_free_across_budget() {
    let large = format!("{}tail", "prefix".repeat(60));
    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r4c-read-output-paging",
        registry(
            vec![Box::new(StaticTool {
                name: "large_tool",
                result: large.clone(),
            })],
            &["large_tool"],
        ),
    )
    .await;
    let handle = background_tool(&hook, "bg-large", "large_tool").await;
    let tool_call_id = handle["tool_call_id"].as_str().unwrap();
    let waited = wait_tool(&hook, "wait-large", tool_call_id).await;
    assert_eq!(waited["status"].as_str(), Some("completed"));

    let first = read_tool_output(
        &hook,
        "read-page-0",
        json!({
            "tool_call_id": tool_call_id,
            "offset": 0,
            "max_tokens": 64
        }),
    )
    .await;
    assert_eq!(first["total_bytes"].as_u64(), Some(large.len() as u64));
    assert_eq!(
        first["has_more"].as_bool(),
        Some(true),
        "first page must signal more"
    );
    assert_eq!(first["exited"].as_bool(), Some(true));
    let first_output = first["output"].as_str().unwrap().to_string();
    assert_eq!(
        first_output.len(),
        256,
        "budget caps the slice at 256 bytes"
    );
    let next_offset = first["next_offset"].as_u64().unwrap();
    assert_eq!(next_offset, 256, "cursor = offset + bytes returned");
    assert_eq!(first_output, &large[..256]);

    let mut reassembled = first_output;
    let mut cursor = next_offset;
    let mut has_more = first["has_more"].as_bool().unwrap();
    let mut pages = 1;
    while has_more {
        let page = read_tool_output(
            &hook,
            &format!("read-page-{pages}"),
            json!({
                "tool_call_id": tool_call_id,
                "offset": cursor,
                "max_tokens": 64
            }),
        )
        .await;
        let out = page["output"].as_str().unwrap();
        assert!(!out.is_empty(), "non-final pages must make progress");
        reassembled.push_str(out);
        let next = page["next_offset"].as_u64().unwrap();
        assert_eq!(next, cursor + out.len() as u64, "no gap, no overlap");
        cursor = next;
        has_more = page["has_more"].as_bool().unwrap();
        pages += 1;
        assert!(pages < 20, "paging did not terminate");
    }
    assert_eq!(cursor, large.len() as u64);
    assert_eq!(reassembled, large);
    assert!(pages >= 2, "should take more than one page");

    let past_end = read_tool_output(
        &hook,
        "read-past-end",
        json!({
            "tool_call_id": tool_call_id,
            "offset": large.len() as u64
        }),
    )
    .await;
    assert_eq!(past_end["output"].as_str(), Some(""));
    assert_eq!(past_end["has_more"].as_bool(), Some(false));
    assert_eq!(past_end["next_offset"].as_u64(), Some(large.len() as u64));
}

#[tokio::test]
async fn read_process_multibyte_utf8_page_boundary_slices_on_char_boundary() {
    let char_3byte: char = '\u{2019}';
    let raw: String = std::iter::repeat(char_3byte).take(64).collect();
    assert_eq!(
        raw.len(),
        192,
        "sanity: each char is exactly 3 bytes in UTF-8"
    );

    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r4c-read-utf8-page-boundary",
        registry(
            vec![Box::new(StaticTool {
                name: "utf8_tool",
                result: raw.clone(),
            })],
            &["utf8_tool"],
        ),
    )
    .await;
    let handle = background_tool(&hook, "bg-utf8", "utf8_tool").await;
    let tool_call_id = handle["tool_call_id"].as_str().unwrap();
    let waited = wait_tool(&hook, "wait-utf8", tool_call_id).await;
    assert_eq!(waited["status"].as_str(), Some("completed"));

    let mut reassembled = String::new();
    let mut cursor = 0u64;
    let mut pages = 0;
    loop {
        let page = read_tool_output(
            &hook,
            &format!("read-utf8-page-{pages}"),
            json!({
                "tool_call_id": tool_call_id,
                "offset": cursor,
                "max_tokens": 64
            }),
        )
        .await;
        let out = page["output"].as_str().expect("output must be a string");
        assert!(
            std::str::from_utf8(out.as_bytes()).is_ok(),
            "page output is not valid UTF-8"
        );
        let next = page["next_offset"].as_u64().unwrap();
        let has_more = page["has_more"].as_bool().unwrap();
        if has_more {
            assert!(!out.is_empty(), "non-final pages must make progress");
            assert!(next > cursor, "cursor must advance");
        }
        reassembled.push_str(out);
        cursor = next;
        pages += 1;
        if !has_more {
            break;
        }
        assert!(pages < 30, "paging did not terminate");
    }
    assert_eq!(
        reassembled, raw,
        "pages must reassemble to the original byte-for-byte"
    );
    assert_eq!(cursor, raw.len() as u64, "final cursor equals total_bytes");
}

#[tokio::test]
async fn read_process_stdout_and_stderr_paging_across_boundary_is_gap_free() {
    let stdout_body = "A".repeat(200);
    let stderr_body = "B".repeat(200);
    let native_result = format!(
        concat!(
            "gents_exec: {{\"ok\":true,\"status\":\"success\",",
            "\"command\":\"echo\",\"argv\":[\"echo\"],",
            "\"cwd\":\".\",\"exit_code\":0,\"timed_out\":false,\"duration_ms\":1,",
            "\"timeout_ms\":10000,\"execution_mode\":\"read_only\",",
            "\"network_mode\":\"inherit\",\"sandbox\":\"policy_read_only\",",
            "\"stdout_truncation\":{{\"returned_bytes\":{stdout_len},\"total_bytes\":{stdout_len},",
            "\"max_bytes\":16000,\"truncated\":false}},",
            "\"stderr_truncation\":{{\"returned_bytes\":{stderr_len},\"total_bytes\":{stderr_len},",
            "\"max_bytes\":16000,\"truncated\":false}}}}\n",
            "stdout:\n",
            "{stdout}\n",
            "stderr:\n",
            "{stderr}"
        ),
        stdout_len = stdout_body.len(),
        stderr_len = stderr_body.len(),
        stdout = stdout_body,
        stderr = stderr_body,
    );

    let (_db, hook, _session_id, _request_id) = setup_hook(
        "r4c-read-stdout-stderr-paging",
        registry(
            vec![Box::new(StaticTool {
                name: "bash",
                result: native_result,
            })],
            &["bash"],
        ),
    )
    .await;
    let handle = background_tool(&hook, "bg-dual", "bash").await;
    let tool_call_id = handle["tool_call_id"].as_str().unwrap();
    let waited = wait_tool(&hook, "wait-dual", tool_call_id).await;
    assert_eq!(waited["status"].as_str(), Some("completed"));

    let first = read_tool_output(
        &hook,
        "read-dual-page-0",
        json!({
            "tool_call_id": tool_call_id,
            "offset": 0,
            "max_tokens": 64
        }),
    )
    .await;
    let total_bytes = first["total_bytes"].as_u64().expect("total_bytes");
    let stderr_boundary_len = "\n--- stderr ---\n".len();
    let expected_combined_len = stdout_body.len() + stderr_boundary_len + stderr_body.len();
    assert_eq!(
        total_bytes, expected_combined_len as u64,
        "total_bytes must cover stdout + boundary + stderr"
    );

    let mut reassembled = first["output"].as_str().unwrap().to_string();
    let mut cursor = first["next_offset"].as_u64().unwrap();
    let mut has_more = first["has_more"].as_bool().unwrap();
    let mut pages = 1usize;
    while has_more {
        let page = read_tool_output(
            &hook,
            &format!("read-dual-page-{pages}"),
            json!({
                "tool_call_id": tool_call_id,
                "offset": cursor,
                "max_tokens": 64
            }),
        )
        .await;
        let out = page["output"].as_str().unwrap();
        let next = page["next_offset"].as_u64().unwrap();
        assert_eq!(
            page["total_bytes"].as_u64().unwrap(),
            total_bytes,
            "total_bytes must be constant across all pages"
        );
        assert_eq!(
            next,
            cursor + out.len() as u64,
            "no gap and no overlap between pages"
        );
        assert!(!out.is_empty(), "non-final pages must make progress");
        reassembled.push_str(out);
        cursor = next;
        has_more = page["has_more"].as_bool().unwrap();
        pages += 1;
        assert!(pages < 30, "paging did not terminate");
    }
    assert_eq!(cursor, total_bytes, "final cursor must equal total_bytes");
    assert!(
        reassembled.contains(&stdout_body),
        "reassembled output must contain stdout"
    );
    assert!(
        reassembled.contains("--- stderr ---"),
        "reassembled output must contain the stderr boundary"
    );
    assert!(
        reassembled.contains(&stderr_body),
        "reassembled output must contain stderr"
    );
    assert_eq!(
        reassembled.len() as u64,
        total_bytes,
        "reassembled length must equal total_bytes"
    );
}

#[tokio::test]
async fn read_tool_output_rejects_non_backgrounded() {
    let (db, hook, session_id, request_id) = setup_hook(
        "r4c-read-output-foreground",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    let foreground_call_id = create_foreground_tool_call(&db, &request_id, &session_id).await;

    let result = read_tool_output(
        &hook,
        "read-foreground",
        json!({ "tool_call_id": foreground_call_id }),
    )
    .await;
    assert_eq!(result["ok"].as_bool(), Some(false));
    assert_eq!(result["failure_class"].as_str(), Some("argument_invalid"));
}

#[tokio::test]
async fn read_tool_output_rejects_unauthorized() {
    let db = test_db("r4c-read-output-unauthorized").await;
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
    let handle = background_tool(&hook_2, "sibling-bg", "slow_tool").await;
    let tool_call_id = handle["tool_call_id"].as_str().unwrap();

    let result = read_tool_output(
        &hook_1,
        "read-unauthorized",
        json!({ "tool_call_id": tool_call_id }),
    )
    .await;
    assert_eq!(result["ok"].as_bool(), Some(false));
    assert_eq!(result["failure_class"].as_str(), Some("tool_not_allowed"));
}

#[tokio::test]
async fn read_tool_output_no_parent_tool_call_row_written() {
    let (db, hook, session_id, _request_id) = setup_hook(
        "r4c-read-output-no-row",
        registry(vec![Box::new(PendingTool)], &["slow_tool"]),
    )
    .await;
    let handle = background_tool(&hook, "bg-no-row", "slow_tool").await;
    let tool_call_id = handle["tool_call_id"].as_str().unwrap();
    let _ = read_tool_output(
        &hook,
        "read-no-row",
        json!({ "tool_call_id": tool_call_id }),
    )
    .await;

    assert_eq!(
        count_tool_calls_by_name(db.node.as_ref(), &session_id, "read_process").await,
        0
    );
}
