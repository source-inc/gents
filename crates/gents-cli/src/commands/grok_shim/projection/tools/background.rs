//! Native task cards derived exclusively from persisted background Bash calls.
//! There is no process registry here: IDs, lifecycle and output remain runtime-owned.

use super::{observed_status, ToolCallRow, ToolCallStatus};
use serde_json::{json, Value};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BackgroundTaskUpdate {
    pub method: &'static str,
    pub kind: &'static str,
    pub key: String,
    pub payload: Value,
    /// Runtime stream offset, carried internally rather than on the ACP wire.
    pub output_start: Option<u64>,
}

fn update(kind: &'static str, id: &str, payload: Value) -> BackgroundTaskUpdate {
    BackgroundTaskUpdate {
        method: match kind {
            "task_backgrounded" => "x.ai/task_backgrounded",
            _ => "x.ai/task_completed",
        },
        kind,
        key: format!("{kind}:{id}"),
        payload,
        output_start: None,
    }
}

/// The shell's TaskSnapshot uses serde's SystemTime representation, not an
/// RFC3339 string. Missing durable timestamps must not become wall-clock state.
fn system_time(timestamp: Option<&str>) -> Option<Value> {
    let time = chrono::DateTime::parse_from_rfc3339(timestamp?).ok()?;
    let seconds = u64::try_from(time.timestamp()).ok()?;
    serde_json::to_value(std::time::UNIX_EPOCH.checked_add(std::time::Duration::new(
        seconds,
        time.timestamp_subsec_nanos(),
    ))?)
    .ok()
}

fn output(result: &str) -> (Value, String) {
    if let Ok(value) = serde_json::from_str::<Value>(result) {
        let text = [value["stdout"].as_str(), value["stderr"].as_str()]
            .into_iter()
            .flatten()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        return (value, if text.is_empty() { result.into() } else { text });
    }
    if let Some((metadata, body)) = result
        .strip_prefix("gents_exec: ")
        .and_then(|value| value.split_once('\n'))
    {
        if let Ok(value) = serde_json::from_str::<Value>(metadata) {
            let text = body.strip_prefix("stdout:\n").unwrap_or(body);
            let text = if let Some((stdout, stderr)) = text.split_once("\nstderr:\n") {
                [stdout, stderr]
                    .into_iter()
                    .filter(|value| value.trim() != "(empty)" && !value.is_empty())
                    .collect::<Vec<_>>()
                    .join("\n")
            } else {
                text.to_string()
            };
            return (value, text);
        }
    }
    (Value::Null, result.to_string())
}

fn truncated(metadata: &Value) -> bool {
    ["stdout_truncation", "stderr_truncation"]
        .iter()
        .any(|field| metadata[field]["truncated"].as_bool() == Some(true))
}

pub(super) fn raw_output(result: &str) -> Value {
    let (metadata, output) = output(result);
    json!({"type":"Bash", "output_for_prompt":output, "truncated":truncated(&metadata)})
}

pub(super) fn project(
    row: &ToolCallRow,
    id: &str,
    result: &str,
) -> Option<(BackgroundTaskUpdate, Option<BackgroundTaskUpdate>)> {
    if row.await_mode.as_deref() != Some("background")
        || !matches!(
            row.tool_name.as_deref(),
            Some("bash" | "bash_unrestricted" | "Bash")
        )
    {
        return None;
    }
    let args: Value = serde_json::from_str(row.args.as_deref().unwrap_or("")).ok()?;
    let command = args["command"].as_str()?;
    // Without a durable start, retain the standard ACP lifecycle instead
    // of creating a native task that cannot later be completed faithfully.
    let start = system_time(row.started_at.as_deref())?;
    let (metadata, output) = output(result);
    let cwd = args["cwd"]
        .as_str()
        .or_else(|| metadata["cwd"].as_str())
        .unwrap_or("");
    let started = update(
        "task_backgrounded",
        id,
        json!({
            "sessionUpdate":"task_backgrounded", "tool_call_id":id, "task_id":id,
            "command":command, "cwd":cwd, "output_file":""
        }),
    );
    let status = observed_status(row);
    let completed = if status.is_completed() {
        // Native end_time is optional; a legacy missing end must not leave
        // an otherwise terminal background card permanently running.
        let end = system_time(row.completed_at.as_deref());
        Some({
            let cancelled = row.lifecycle_state.as_deref() == Some("cancelled");
            let signal = if let Some(signal) = metadata["signal"].as_str().filter(|s| !s.is_empty())
            {
                Some(signal)
            } else if cancelled {
                Some("cancelled")
            } else if metadata["timed_out"].as_bool() == Some(true) {
                Some("timeout")
            } else if status == ToolCallStatus::Failed {
                Some("failed")
            } else {
                None
            };
            let total = ["stdout_truncation", "stderr_truncation"]
                .iter()
                .filter_map(|field| metadata[field]["total_bytes"].as_u64())
                .fold(0_u64, u64::saturating_add);
            // Native pager considers exit 0 successful even with a signal.
            // A stale success output must not override terminal failure.
            let exit_code = if signal.is_some() && metadata["exit_code"].as_i64() == Some(0) {
                Value::Null
            } else {
                metadata["exit_code"].clone()
            };
            update(
                "task_completed",
                id,
                json!({
                    "sessionUpdate":"task_completed", "will_wake":false,
                    "task_snapshot":{
                        "task_id":id, "command":command, "cwd":cwd,
                        "start_time":start, "end_time":end, "output":output,
                        "output_file":"", "truncated":truncated(&metadata),
                        "output_total_bytes":total, "exit_code":exit_code,
                        "signal":signal, "completed":true
                    }
                }),
            )
        })
    } else {
        None
    };
    Some((started, completed))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(state: &str) -> ToolCallRow {
        serde_json::from_value(json!({
            "_docID":"doc", "tool_call_key":"s:call", "tool_call_id":"call",
            "tool_name":"bash_unrestricted", "args":r#"{"command":"false","cwd":"/tmp"}"#,
            "await_mode":"background", "lifecycle_state":state,
            "started_at":"2026-09-04T00:00:00Z", "completed_at":"2026-09-04T00:00:01Z"
        }))
        .unwrap()
    }

    #[test]
    fn native_background_lifecycle_and_output_are_durable() {
        let row = row("completed");
        let result = "gents_exec: {\"exit_code\":0}\nstdout:\nhello\nstderr:\n(empty)";
        let (started, done) = project(&row, "call", result).unwrap();
        assert_eq!(started.payload["task_id"], "call");
        let done = done.unwrap();
        assert_eq!(done.payload["task_snapshot"]["output"], "hello");
        assert_eq!(done.payload["task_snapshot"]["exit_code"], 0);
        let start: std::time::SystemTime =
            serde_json::from_value(done.payload["task_snapshot"]["start_time"].clone()).unwrap();
        assert!(start > std::time::UNIX_EPOCH);
        assert_eq!(raw_output(result)["type"], "Bash");
    }

    #[test]
    fn foreground_is_not_a_background_task_and_failure_is_not_green() {
        let mut foreground = row("completed");
        foreground.await_mode = None;
        assert!(project(&foreground, "call", "").is_none());
        for state in ["failed", "cancelled"] {
            let (_, done) = project(&row(state), "call", r#"{"exit_code":0}"#).unwrap();
            let snapshot = &done.unwrap().payload["task_snapshot"];
            assert!(snapshot["exit_code"].is_null());
            assert!(snapshot["signal"].as_str().is_some());
        }
    }

    #[test]
    fn missing_end_completes_but_missing_start_stays_on_acp() {
        let mut missing = row("completed");
        missing.completed_at = None;
        let (_, done) = project(&missing, "call", "done").unwrap();
        assert!(done.unwrap().payload["task_snapshot"]["end_time"].is_null());
        missing.started_at = None;
        assert!(project(&missing, "call", "done").is_none());
    }
}
