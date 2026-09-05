use super::*;

#[derive(Debug, Deserialize)]
struct BackgroundedRow {
    lifecycle_state: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct StoredToolCallResult {
    pub tool_name: String,
    pub result: String,
}

pub(super) async fn count_live_backgrounded_rows(
    node: &defra_node::EmbeddedNode,
    request_id: &str,
) -> anyhow::Result<usize> {
    let escaped_request_id = crate::graphql::escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    request_id: {{ _eq: "{escaped_request_id}" }},
                    await_mode: {{ _eq: "background" }}
                }}
            ) {{
                lifecycle_state
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query live backgrounded tool count for request {request_id} failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<BackgroundedRow> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter(|row| {
            !matches!(
                row.lifecycle_state.as_deref(),
                Some("completed" | "failed" | "timedOut" | "cancelled")
            )
        })
        .count())
}

pub(super) fn background_receipt_payload(
    child_request_id: &str,
    child_session_id: Option<&str>,
    behavior_id: &str,
) -> String {
    json_string(json!({
        "ok": true,
        "child_request_id": child_request_id,
        "child_session_id": child_session_id,
        "behavior_id": behavior_id,
        "await_mode": "background",
        "status": "running"
    }))
}

pub(super) fn backgrounded_receipt_payload(
    child_request_id: &str,
    child_session_id: &str,
    behavior_id: &str,
) -> String {
    json_string(json!({
        "ok": true,
        "child_request_id": child_request_id,
        "child_session_id": child_session_id,
        "behavior_id": behavior_id,
        "await_mode": "background",
        "status": "running",
        "backgrounded": true
    }))
}

pub(super) async fn wait_for_external_lifecycle_owner(
    missing_owner_since: &mut Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
    internal_call_id: &str,
) -> anyhow::Result<()> {
    let first_missing_at = *missing_owner_since.get_or_insert(now);
    if now - first_missing_at >= chrono::Duration::seconds(5) {
        anyhow::bail!(
            "spawn_subagent foreground wait lost lifecycle ownership for tool_call_id={internal_call_id}"
        );
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

pub(super) async fn load_stored_tool_call_result(
    node: &defra_node::EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
) -> anyhow::Result<StoredToolCallResult> {
    let escaped_session_id = crate::graphql::escape_graphql_string(session_id);
    let escaped_tool_call_id = crate::graphql::escape_graphql_string(tool_call_id);
    let tool_call_key = format!("{escaped_session_id}:{escaped_tool_call_id}");
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{ tool_call_key: {{ _eq: "{tool_call_key}" }} }},
                limit: 1
            ) {{
                tool_name
                result
            }}
        }}"#
    );

    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "loading stored tool call result for session_id={session_id} tool_call_id={tool_call_id} failed: {:?}",
            response.errors
        );
    }

    let mut rows: Vec<StoredToolCallResult> = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentToolCall"))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();

    rows.pop().ok_or_else(|| {
        anyhow::anyhow!(
            "loading stored tool call result: no AgentToolCall for session_id={session_id} tool_call_id={tool_call_id}"
        )
    })
}

pub(super) fn truncation_mode_for(tool_name: &str) -> TruncationMode {
    crate::truncation::tool_result_truncation_mode(tool_name)
}

pub(super) fn bounded_tool_result_for_model(
    tool_name: &str,
    raw_result: &str,
    limits: &crate::truncation::TruncationLimits,
) -> String {
    let (bounded, trigger, truncated) =
        truncate_text(raw_result, truncation_mode_for(tool_name), limits);
    if truncated {
        tracing::warn!(
            tool_name = %tool_name,
            truncated_by = ?trigger,
            original_bytes = raw_result.len(),
            max_lines = limits.max_lines,
            max_bytes = limits.max_bytes,
            "bounded hook-managed tool result before returning it to rig"
        );
    }
    bounded
}

pub(super) fn model_observation_for_tool_result(tool_name: &str, raw_result: &str) -> String {
    if !is_read_file_tool(tool_name) {
        return raw_result.to_string();
    }

    project_read_file_observation(raw_result).unwrap_or_else(|| raw_result.to_string())
}

fn is_read_file_tool(tool_name: &str) -> bool {
    matches!(tool_name, "read_file" | "read" | "cat")
}

fn project_read_file_observation(raw_result: &str) -> Option<String> {
    project_read_file_json_observation(raw_result)
        .or_else(|| project_read_file_compact_observation(raw_result))
}

fn project_read_file_compact_observation(raw_result: &str) -> Option<String> {
    let (first_line, body) = raw_result.split_once('\n')?;
    let metadata = first_line.strip_prefix("gents_fs: ")?;
    let metadata: serde_json::Value = serde_json::from_str(metadata).ok()?;
    if metadata.get("tool").and_then(|value| value.as_str()) != Some("read_file") {
        return None;
    }
    let content = body.strip_prefix("content:\n").unwrap_or(body);
    Some(render_read_file_observation(&metadata, content))
}

fn project_read_file_json_observation(raw_result: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(raw_result.trim()).ok()?;
    let content = value.get("content").and_then(|value| value.as_str())?;
    if !looks_like_read_file_wrapper(&value) {
        return None;
    }
    Some(render_read_file_observation(&value, content))
}

fn looks_like_read_file_wrapper(value: &serde_json::Value) -> bool {
    if value.get("tool").and_then(|value| value.as_str()) == Some("read_file") {
        return true;
    }

    value.get("ok").is_some()
        && value.get("status").is_some()
        && value.get("path").is_some()
        && value.get("start_line").is_some()
        && value.get("end_line").is_some()
}

fn render_read_file_observation(metadata: &serde_json::Value, content: &str) -> String {
    let path = metadata
        .get("path")
        .and_then(|value| value.as_str())
        .filter(|path| !path.trim().is_empty())
        .unwrap_or("file");
    let truncated = metadata
        .get("truncated")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);

    let mut provenance = format!("Read {path}");
    if let (Some(start), Some(end)) = (
        json_usize(metadata, "start_line"),
        json_usize(metadata, "end_line"),
    ) {
        provenance.push_str(&format!(" (lines {start}-{end}"));
        if let Some(total) = json_usize(metadata, "total_count") {
            provenance.push_str(&format!(" of {total}"));
        }
        if truncated {
            provenance.push_str(", truncated");
        }
        provenance.push(')');
    } else if truncated {
        provenance.push_str(" (truncated)");
    }
    provenance.push(':');

    let content = content.trim_end();
    if content.is_empty() {
        provenance
    } else {
        format!("{provenance}\n{content}")
    }
}

fn json_usize(value: &serde_json::Value, key: &str) -> Option<usize> {
    value
        .get(key)
        .and_then(|value| value.as_u64())
        .and_then(|value| usize::try_from(value).ok())
}

pub(super) fn render_tool_result_text(tool_result: &ToolResult) -> String {
    tool_result
        .content
        .iter()
        .filter_map(|content| match content {
            ToolResultContent::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) fn tool_result_message_key(
    session_id: &str,
    message: &Message,
) -> anyhow::Result<Option<String>> {
    let Message::User { content } = message else {
        return Ok(None);
    };
    if content.len() != 1 {
        return Ok(None);
    }
    let Some(UserContent::ToolResult(tool_result)) = content.first() else {
        return Ok(None);
    };

    let Some(logical_id) = non_empty(Some(tool_result.id.as_str()))
        .or_else(|| non_empty(tool_result.call_id.as_deref()))
    else {
        return Ok(None);
    };
    let content_json = serde_json::to_string(&tool_result.content)?;
    Ok(Some(format!(
        "{session_id}:tool-result:{:016x}:{:016x}",
        stable_hash(logical_id.as_bytes()),
        stable_hash(content_json.as_bytes())
    )))
}

pub(super) fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub(super) fn is_subagent_tool_result_payload(raw: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return false;
    };
    value
        .get("service_id")
        .and_then(|value| value.as_str())
        .is_some_and(|service_id| service_id == "subagent")
        || (value.get("child_request_id").is_some() && value.get("await_mode").is_some())
}

pub(super) fn json_string(value: serde_json::Value) -> String {
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
}

/// Build a model-facing JSON envelope that embeds a potentially oversized
/// result while staying inside the hook's truncation limits.
///
/// Skip reasons are bounded by `skip_tool_result` with no JSON awareness: if an
/// embedded result pushes the envelope past the limits, the outer truncation
/// slices the envelope mid-structure and the model receives corrupt JSON. So
/// bound the embedded copy first at half the byte budget (headroom for JSON
/// string escaping), and if escaping inflation still overflows the limits,
/// fall back to a stub — the envelope must always survive the outer bound
/// intact. The full output stays on the durable AgentToolCall row.
pub(super) fn json_envelope_with_bounded_result(
    mut envelope: serde_json::Value,
    result_key: &str,
    result: &str,
    tool_name: &str,
    limits: &crate::truncation::TruncationLimits,
) -> String {
    let inner_limits = crate::truncation::TruncationLimits {
        max_lines: limits.max_lines,
        max_bytes: limits.max_bytes / 2,
    };
    let (bounded, _, _) = crate::truncation::truncate_text(
        result,
        crate::truncation::tool_result_truncation_mode(tool_name),
        &inner_limits,
    );
    envelope[result_key] = serde_json::Value::String(bounded);
    let rendered = serde_json::to_string_pretty(&envelope).unwrap_or_else(|_| envelope.to_string());
    if rendered.len() <= limits.max_bytes {
        return rendered;
    }
    envelope[result_key] = serde_json::Value::String(format!(
        "[{} bytes omitted from envelope; full output stored on the tool-call row]",
        result.len()
    ));
    json_string(envelope)
}

pub(super) fn foreground_terminal_failure_payload(
    child_request_id: &str,
    child_session_id: &str,
    status: &str,
    reason: impl Into<String>,
    failure_class: FailureClass,
) -> String {
    json_string(json!({
        "ok": false,
        "child_request_id": child_request_id,
        "child_session_id": child_session_id,
        "await_mode": "foreground",
        "status": status,
        "final_response": null,
        "error": {
            "reason": reason.into(),
            "failure_class": failure_class.as_str()
        }
    }))
}

pub(super) fn invalid_tool_arguments_payload(
    tool_name: &str,
    path: &str,
    message: impl Into<String>,
) -> String {
    json_string(json!({
        "ok": false,
        "failure_class": "invalid_tool_arguments",
        "path": path,
        "message": message.into(),
        "retryable": false,
        "service_id": "subagent",
        "tool_name": tool_name
    }))
}

pub(super) fn background_invalid_tool_arguments_payload(
    tool_name: &str,
    path: &str,
    message: impl Into<String>,
) -> String {
    json_string(json!({
        "ok": false,
        "failure_class": "argument_invalid",
        "path": path,
        "message": message.into(),
        "retryable": false,
        "service_id": "process",
        "tool_name": tool_name
    }))
}

pub(super) fn background_tool_not_allowed_payload(
    tool_name: &str,
    path: &str,
    requested: &str,
    message: impl Into<String>,
    allowed_targets: Vec<String>,
) -> String {
    json_string(json!({
        "ok": false,
        "failure_class": "tool_not_allowed",
        "path": path,
        "message": message.into(),
        "retryable": false,
        "service_id": "process",
        "tool_name": tool_name,
        "requested_tool_name": requested,
        "allowed_backgroundable_tool_names": allowed_targets
    }))
}

pub(super) fn background_budget_exceeded_payload(current_backgrounded: usize) -> String {
    json_string(json!({
        "ok": false,
        "failure_class": "argument_invalid",
        "code": "background_tool_budget_exceeded",
        "path": "/",
        "message": format!(
            "parent request has reached the concurrent backgrounded tool ceiling ({MAX_BACKGROUNDED_TOOLS_PER_PARENT})"
        ),
        "retryable": false,
        "service_id": "process",
        "tool_name": SPAWN_PROCESS_TOOL_NAME,
        "current_backgrounded": current_backgrounded,
        "max_backgrounded": MAX_BACKGROUNDED_TOOLS_PER_PARENT
    }))
}

pub(super) fn depth_exceeded_payload(parent_subagent_depth: u32) -> String {
    json_string(json!({
        "ok": false,
        "failure_class": "invalid_tool_arguments",
        "code": "subagent_depth_exceeded",
        "path": "/behavior_id",
        "message": "subagent depth ceiling would be exceeded",
        "retryable": false,
        "service_id": "subagent",
        "tool_name": SPAWN_SUBAGENT_TOOL_NAME,
        "parent_subagent_depth": parent_subagent_depth,
        "max_subagent_depth": MAX_SUBAGENT_DEPTH
    }))
}

pub(super) fn tool_not_allowed_payload(
    tool_name: &str,
    path: &str,
    requested: &str,
    message: impl Into<String>,
    allowed_targets: Vec<String>,
) -> String {
    json_string(json!({
        "ok": false,
        "failure_class": "tool_not_allowed",
        "path": path,
        "message": message.into(),
        "retryable": false,
        "service_id": "subagent",
        "tool_name": tool_name,
        "requested_tool_name": requested,
        "allowed_subagent_targets": allowed_targets
    }))
}

pub(super) fn service_unavailable_payload(
    tool_name: &str,
    path: &str,
    message: impl Into<String>,
    retryable: bool,
) -> String {
    json_string(json!({
        "ok": false,
        "failure_class": "service_unavailable",
        "path": path,
        "message": message.into(),
        "retryable": retryable,
        "service_id": "subagent",
        "tool_name": tool_name
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_envelope_bounds_oversized_result_and_stays_valid_json() {
        let limits = crate::truncation::TruncationLimits::default();
        let big = "x".repeat(2 * limits.max_bytes);
        let rendered = json_envelope_with_bounded_result(
            serde_json::json!({"ok": true, "status": "completed", "result": null}),
            "result",
            &big,
            "wait_process",
            &limits,
        );
        assert!(
            rendered.len() <= limits.max_bytes,
            "envelope must fit the outer bound: {} > {}",
            rendered.len(),
            limits.max_bytes
        );
        let parsed: serde_json::Value =
            serde_json::from_str(&rendered).expect("envelope must remain valid JSON");
        let result = parsed["result"].as_str().expect("result string");
        assert!(!result.is_empty(), "bounded result must be non-empty");
        assert!(result.len() < big.len(), "result must be bounded");
    }

    #[test]
    fn json_envelope_falls_back_to_stub_when_escaping_inflates_past_limits() {
        // Control characters escape 6x (0x01 -> \\u0001), defeating the half-budget
        // headroom; the helper must fall back to a stub rather than let the
        // outer truncation slice the envelope.
        let limits = crate::truncation::TruncationLimits::default();
        // Many short control-char lines: small enough to survive the inner
        // half-budget bound, but escaping inflates ~6x past the full budget.
        let pathological = vec!["\u{1}".repeat(10); 1500].join("\n");
        let rendered = json_envelope_with_bounded_result(
            serde_json::json!({"ok": true, "result": null}),
            "result",
            &pathological,
            "wait_process",
            &limits,
        );
        assert!(rendered.len() <= limits.max_bytes);
        let parsed: serde_json::Value =
            serde_json::from_str(&rendered).expect("stub envelope must be valid JSON");
        assert!(
            parsed["result"]
                .as_str()
                .is_some_and(|s| s.contains("omitted")),
            "expected the stub fallback, got {parsed}"
        );
    }

    #[test]
    fn json_envelope_passes_small_results_through_untruncated() {
        let limits = crate::truncation::TruncationLimits::default();
        let rendered = json_envelope_with_bounded_result(
            serde_json::json!({"ok": true, "result": null}),
            "result",
            "done",
            "wait_process",
            &limits,
        );
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed["result"], "done");
    }

    #[test]
    fn read_file_compact_output_projects_to_model_observation() {
        let raw = concat!(
            r#"gents_fs: {"ok":true,"status":"success","tool":"read_file","path":"notes.txt","returned_count":2,"total_count":3,"truncated":false,"start_line":2,"end_line":3}"#,
            "\ncontent:\nL2: beta\nL3: gamma\n"
        );

        assert_eq!(
            model_observation_for_tool_result("read_file", raw),
            "Read notes.txt (lines 2-3 of 3):\nL2: beta\nL3: gamma"
        );
    }

    #[test]
    fn read_file_raw_json_output_projects_to_model_observation() {
        let raw = r#"{"ok":true,"status":"success","tool":"read_file","path":"src/main.rs","returned_count":1,"total_count":9,"truncated":true,"start_line":4,"end_line":4,"content":"L4: fn main() {}"}"#;

        assert_eq!(
            model_observation_for_tool_result("read_file", raw),
            "Read src/main.rs (lines 4-4 of 9, truncated):\nL4: fn main() {}"
        );
    }

    #[test]
    fn read_file_projection_does_not_parse_plain_file_json() {
        let raw = r#"{"path":"data.json","content":"not a wrapper"}"#;

        assert_eq!(model_observation_for_tool_result("read_file", raw), raw);
    }

    #[test]
    fn non_read_tool_result_is_unchanged_for_model() {
        let raw = r#"{"ok":true,"content":"still structured"}"#;

        assert_eq!(model_observation_for_tool_result("grep", raw), raw);
    }

    #[test]
    fn hook_managed_tool_result_is_bounded_before_model_loop() {
        let limits = crate::truncation::TruncationLimits {
            max_lines: 2,
            max_bytes: 80,
        };
        let raw = "line 1\nline 2\nline 3\nline 4";

        let bounded = bounded_tool_result_for_model("wait_subagent", raw, &limits);

        assert!(bounded.contains("line 1\nline 2"));
        assert!(!bounded.contains("line 3"));
        assert!(bounded.contains("[Showing lines 1-2 of 4"));
    }
}
