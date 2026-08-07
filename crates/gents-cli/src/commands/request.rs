use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{Context, Result};
use gents::{graphql::escape_graphql_string, tool_call_lifecycle::CancelCause};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::cli::args::{
    RequestCommand, RequestInterruptArgs, RequestResendArgs, RequestShowArgs, RequestSubmitArgs,
};
use crate::cli::output_format::OutputFormat;
use crate::request_helpers::{
    fetch_request_view, is_terminal_lifecycle_state, parse_duration_suffix, parse_valid_until_flag,
};
use crate::{
    create_agent_request, post_graphql, print_json, resolve_agent_did, resolve_graphql_endpoint,
    resolve_request_content, resolve_request_id, response_query, wait_for_terminal_response,
    write_json_output_file, RequestSubmitOptions,
};

pub(crate) async fn dispatch(command: RequestCommand) -> Result<()> {
    match command {
        RequestCommand::Submit(args) => request_submit(args).await,
        RequestCommand::Show(args) => request_show(args).await,
        RequestCommand::Interrupt(args) => request_interrupt(args).await,
        RequestCommand::Resend(args) => request_resend(args).await,
    }
}

async fn request_submit(args: RequestSubmitArgs) -> Result<()> {
    let graphql = resolve_graphql_endpoint(args.graphql.as_deref(), args.home.as_deref())?;
    let agent_did = resolve_agent_did(args.home.as_deref(), args.agent_did.as_deref())?;
    let content = resolve_request_content(args.content.as_deref(), args.content_file.as_deref())?;
    let valid_until = parse_valid_until_flag(args.valid_until.as_deref())?;
    let submitted = create_agent_request(
        &graphql,
        &agent_did,
        &content,
        args.session_id.as_deref(),
        args.behavior_id.as_deref(),
        RequestSubmitOptions {
            temperature: args.temperature,
            top_p: args.top_p,
            top_k: args.top_k,
            seed: args.seed,
            max_tokens: args.max_tokens,
            metadata: args.metadata.clone(),
            valid_until,
            retry_parent_request: None,
            retry_root_request: None,
        },
    )
    .await?;
    let request_summary = json!({
        "request_id": submitted.request_id,
        "session_id": submitted.session_id,
        "agent_did": submitted.agent_did,
        "behavior_id": submitted.behavior_id,
        "temperature": submitted.temperature,
        "top_p": submitted.top_p,
        "top_k": submitted.top_k,
        "seed": submitted.seed,
        "max_tokens": submitted.max_tokens,
        "metadata": submitted.metadata,
    });
    if args.no_wait {
        print_json(&request_summary)?;
        if let Some(path) = args.output_file.as_deref() {
            write_json_output_file(path, &request_summary)?;
        }
        return Ok(());
    }

    let response = wait_for_terminal_response(
        &graphql,
        &submitted.request_id,
        args.timeout_secs,
        args.poll_secs,
    )
    .await
    .with_context(|| format!("waiting for AgentResponse {}", submitted.request_id))?;
    let mut output = request_summary
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("request summary was not a JSON object"))?;
    output.insert("response".to_string(), response);
    let output = serde_json::Value::Object(output);
    print_json(&output)?;
    if let Some(path) = args.output_file.as_deref() {
        write_json_output_file(path, &output)?;
    }
    Ok(())
}

pub(crate) async fn request_show(args: RequestShowArgs) -> Result<()> {
    let graphql = resolve_graphql_endpoint(args.graphql.as_deref(), args.home.as_deref())?;
    let request_id =
        resolve_request_id(args.request_id.as_deref(), args.request_id_flag.as_deref())?;
    let snapshot = load_request_show_snapshot(&graphql, &request_id).await?;
    match args
        .output
        .ensure_supported("request show", &[OutputFormat::Text, OutputFormat::Json])?
    {
        OutputFormat::Json => {
            let value = serde_json::to_value(&snapshot)?;
            print_json(&value)?;
        }
        OutputFormat::Text => {
            print!("{}", render_request_show_text(&snapshot));
        }
        _ => unreachable!("ensure_supported restricts request show output formats"),
    }
    Ok(())
}

#[derive(Debug, Clone, Default)]
struct RequestShowSchema {
    agent_request: BTreeSet<String>,
    agent_tool_call: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RequestShowSnapshot {
    request: RequestShowHeader,
    cancel_cause: Option<RequestCancelCauseView>,
    tool_calls: Vec<RequestToolCallView>,
    backgrounded_tools: Vec<BackgroundedToolView>,
    native_executors_available: bool,
    native_executors: Vec<NativeExecutorView>,
    child_requests: Vec<ChildRequestView>,
}

#[derive(Debug, Clone, Serialize)]
struct RequestShowHeader {
    request_id: String,
    agent_did: String,
    behavior_id: String,
    session_id: String,
    status: String,
    lifecycle_state: String,
    backend_id: Option<String>,
    execution_origin: Option<String>,
    failure_reason: Option<String>,
    terminal_cause: Option<String>,
    transition_history: Vec<RequestTransitionView>,
    created_at: Option<String>,
    claimed_at: Option<String>,
    deadline: Option<String>,
    valid_until: Option<String>,
    interrupt_requested_at: Option<String>,
    retry_count: Option<i64>,
    max_retries: Option<i64>,
    seed: Option<i64>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
    caused_by_trigger_id: Option<String>,
    caused_by_trigger_kind: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RequestTransitionView {
    action: String,
    from: Option<String>,
    to: String,
    at: Option<String>,
    source: String,
    inferred: bool,
    note: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RequestCancelCauseView {
    cause: String,
    cancel_initiated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct RequestToolCallView {
    tool_call_key: String,
    request_id: String,
    session_id: String,
    message_sequence: Option<i64>,
    tool_name: String,
    tool_call_id: String,
    status: String,
    state: String,
    await_mode: String,
    cancel_policy: String,
    child_terminal: String,
    cancel_cause: String,
    cancel_initiated_at: Option<String>,
    child_request_id: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    deadline_at: Option<String>,
    active_tool_call: bool,
    active_native_executor_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct BackgroundedToolView {
    tool_call_id: String,
    tool_name: String,
    state: String,
    await_mode: String,
    cancel_policy: String,
    child_request_id: Option<String>,
    started_at: Option<String>,
    active_tool_call: bool,
    active_native_executor_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct NativeExecutorView {
    id: Option<u64>,
    pid: Option<i64>,
    argv0: Option<String>,
    tool_name: Option<String>,
    started_at: Option<String>,
    kill_signaled_at: Option<String>,
    reaped_at: Option<String>,
    exit_code: Option<i64>,
    age_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
struct ChildRequestView {
    request_id: String,
    state: String,
    status: String,
    behavior_id: String,
    created_at: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
    caused_by_trigger_kind: Option<String>,
}

async fn load_request_show_snapshot(
    graphql: &str,
    request_id: &str,
) -> Result<RequestShowSnapshot> {
    let schema = load_request_show_schema(graphql).await;
    let request_response = post_graphql(graphql, &request_show_request_query(request_id, &schema))
        .await
        .with_context(|| format!("loading AgentRequest {request_id}"))?;
    let request_row = request_response
        .pointer("/data/AgentRequest")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("request {request_id} not found"))?;

    let response_response = post_graphql(graphql, &response_query(request_id))
        .await
        .with_context(|| format!("loading latest AgentResponse for {request_id}"))?;
    let response_row = response_response
        .pointer("/data/AgentResponse")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .cloned();

    let tool_response = post_graphql(graphql, &request_show_tool_calls_query(request_id, &schema))
        .await
        .with_context(|| format!("loading AgentToolCall rows for {request_id}"))?;
    let tool_rows = value_array(&tool_response, "/data/AgentToolCall");

    let child_response = post_graphql(graphql, &request_show_child_requests_query(request_id))
        .await
        .with_context(|| format!("loading child AgentRequest rows for {request_id}"))?;
    let child_rows = value_array(&child_response, "/data/AgentRequest");

    let request_agent_did = string_field(&request_row, "agent_did").unwrap_or_default();
    let liveness = crate::commands::status::load_liveness_value(graphql, &request_agent_did).await;
    let active_tool_calls = active_tool_call_keys(&liveness);
    let native_executors_available = liveness
        .get("active_native_executors_available")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let native_executor_values = value_array(&liveness, "/active_native_executors");
    let request_terminal = is_terminal_lifecycle_state(
        string_field(&request_row, "lifecycle_state")
            .unwrap_or_default()
            .as_str(),
    );

    let tool_names = tool_rows
        .iter()
        .filter_map(|row| string_field(row, "tool_name"))
        .collect::<BTreeSet<_>>();
    let tool_call_ids = tool_rows
        .iter()
        .filter_map(|row| string_field(row, "tool_call_id"))
        .collect::<BTreeSet<_>>();
    let tool_call_keys = tool_rows
        .iter()
        .filter_map(|row| string_field(row, "tool_call_key"))
        .collect::<BTreeSet<_>>();
    let relevant_native_executor_values = native_executor_values
        .iter()
        .filter(|executor| {
            native_executor_matches_request(
                executor,
                request_id,
                &tool_names,
                &tool_call_ids,
                &tool_call_keys,
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    let active_executor_counts =
        active_native_executor_counts_by_tool(&tool_rows, &relevant_native_executor_values);

    let tool_calls = tool_rows
        .iter()
        .map(|row| tool_call_view(row, &active_tool_calls, &active_executor_counts))
        .collect::<Vec<_>>();
    let backgrounded_tools = tool_calls
        .iter()
        .filter(|tool| {
            tool.await_mode == "background"
                || tool.active_tool_call
                || tool.active_native_executor_count > 0
        })
        .map(backgrounded_tool_view)
        .collect::<Vec<_>>();
    let native_executors = if request_terminal {
        Vec::new()
    } else {
        relevant_native_executor_values
            .iter()
            .map(native_executor_view)
            .collect()
    };
    let cancel_cause = request_cancel_cause_view(&request_row, &tool_calls);
    let terminal_cause = terminal_cause(&request_row, response_row.as_ref(), cancel_cause.as_ref());
    let transition_history = transition_history(
        &request_row,
        response_row.as_ref(),
        terminal_cause.as_deref(),
    );
    let request = request_header_view(&request_row, terminal_cause, transition_history);
    let child_requests = child_rows
        .iter()
        .map(child_request_view)
        .collect::<Vec<_>>();

    Ok(RequestShowSnapshot {
        request,
        cancel_cause,
        tool_calls,
        backgrounded_tools,
        native_executors_available,
        native_executors,
        child_requests,
    })
}

async fn load_request_show_schema(graphql: &str) -> RequestShowSchema {
    RequestShowSchema {
        agent_request: load_graphql_type_fields(graphql, "AgentRequest").await,
        agent_tool_call: load_graphql_type_fields(graphql, "AgentToolCall").await,
    }
}

async fn load_graphql_type_fields(graphql: &str, type_name: &str) -> BTreeSet<String> {
    let query = format!(
        r#"{{
            __type(name: "{type_name}") {{
                fields {{ name }}
            }}
        }}"#
    );
    let Ok(response) = post_graphql(graphql, &query).await else {
        return BTreeSet::new();
    };
    response
        .pointer("/data/__type/fields")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|field| field.get("name").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect()
}

fn request_show_request_query(request_id: &str, schema: &RequestShowSchema) -> String {
    let mut fields = vec![
        "request_id",
        "agent_did",
        "behavior_id",
        "session_id",
        "status",
        "lifecycle_state",
        "backend_id",
        "execution_origin",
        "failure_reason",
        "retry_count",
        "max_retries",
        "temperature",
        "top_p",
        "top_k",
        "seed",
        "max_tokens",
        "metadata",
        "created_at",
        "claimed_at",
        "deadline",
        "valid_until",
        "interrupt_requested_at",
        "retry_parent_request",
        "retry_root_request",
        "superseded_by_request",
        "caused_by_parent_request_id",
        "caused_by_parent_tool_call_id",
        "caused_by_trigger_id",
        "caused_by_trigger_kind",
        "subagent_depth",
    ];
    append_optional_fields(
        &mut fields,
        &schema.agent_request,
        &["cancel_cause", "cancel_initiated_at"],
    );
    let fields = fields.join("\n                ");
    format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }}
            ) {{
                {fields}
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    )
}

fn request_show_tool_calls_query(request_id: &str, schema: &RequestShowSchema) -> String {
    let mut fields = vec![
        "tool_call_key",
        "request_id",
        "session_id",
        "message_sequence",
        "tool_name",
        "tool_call_id",
        "status",
        "lifecycle_state",
        "started_at",
        "deadline_at",
        "completed_at",
        "await_mode",
        "cancel_policy",
        "child_request_id",
    ];
    append_optional_fields(
        &mut fields,
        &schema.agent_tool_call,
        &["child_terminal", "cancel_cause", "cancel_initiated_at"],
    );
    let fields = fields.join("\n                ");
    format!(
        r#"{{
            AgentToolCall(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                order: {{ started_at: ASC }}
            ) {{
                {fields}
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    )
}

fn request_show_child_requests_query(request_id: &str) -> String {
    format!(
        r#"{{
            AgentRequest(
                filter: {{ caused_by_parent_request_id: {{ _eq: "{request_id}" }} }},
                order: {{ created_at: ASC }}
            ) {{
                request_id
                behavior_id
                status
                lifecycle_state
                created_at
                caused_by_parent_tool_call_id
                caused_by_trigger_kind
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    )
}

fn append_optional_fields(
    fields: &mut Vec<&'static str>,
    available_fields: &BTreeSet<String>,
    optional_fields: &[&'static str],
) {
    for field in optional_fields {
        if available_fields.contains(*field) {
            fields.push(field);
        }
    }
}

fn request_header_view(
    row: &Value,
    terminal_cause: Option<String>,
    transition_history: Vec<RequestTransitionView>,
) -> RequestShowHeader {
    RequestShowHeader {
        request_id: string_field_or_unknown(row, "request_id"),
        agent_did: string_field_or_unknown(row, "agent_did"),
        behavior_id: string_field_or_unknown(row, "behavior_id"),
        session_id: string_field_or_unknown(row, "session_id"),
        status: string_field_or_unknown(row, "status"),
        lifecycle_state: string_field_or_unknown(row, "lifecycle_state"),
        backend_id: string_field(row, "backend_id"),
        execution_origin: string_field(row, "execution_origin"),
        failure_reason: string_field(row, "failure_reason"),
        terminal_cause,
        transition_history,
        created_at: string_field(row, "created_at"),
        claimed_at: string_field(row, "claimed_at"),
        deadline: string_field(row, "deadline"),
        valid_until: string_field(row, "valid_until"),
        interrupt_requested_at: string_field(row, "interrupt_requested_at"),
        retry_count: integer_field(row, "retry_count"),
        max_retries: integer_field(row, "max_retries"),
        seed: integer_field(row, "seed"),
        caused_by_parent_request_id: string_field(row, "caused_by_parent_request_id"),
        caused_by_parent_tool_call_id: string_field(row, "caused_by_parent_tool_call_id"),
        caused_by_trigger_id: string_field(row, "caused_by_trigger_id"),
        caused_by_trigger_kind: string_field(row, "caused_by_trigger_kind"),
    }
}

fn transition_history(
    request: &Value,
    response: Option<&Value>,
    terminal_cause: Option<&str>,
) -> Vec<RequestTransitionView> {
    let state = string_field(request, "lifecycle_state").unwrap_or_else(|| "unknown".to_string());
    let mut transitions = Vec::new();
    if let Some(claimed_at) = string_field(request, "claimed_at") {
        transitions.push(RequestTransitionView {
            action: "claim".to_string(),
            from: Some("pending".to_string()),
            to: "claimed".to_string(),
            at: Some(claimed_at),
            source: "AgentRequest.claimed_at".to_string(),
            inferred: false,
            note: None,
        });
    }
    if lifecycle_has_begun_inference(&state) {
        transitions.push(RequestTransitionView {
            action: "begin_inference".to_string(),
            from: Some("claimed".to_string()),
            to: "processing".to_string(),
            at: string_field(request, "claimed_at"),
            source: "AgentRequest.lifecycle_state".to_string(),
            inferred: true,
            note: Some("no dedicated begin timestamp is persisted".to_string()),
        });
    }
    if state != "interrupted" {
        if let Some(interrupt_at) = string_field(request, "interrupt_requested_at") {
            let note = if terminal_action_for_state(&state).is_some() {
                "interrupt was requested before this terminal snapshot"
            } else {
                "interrupt requested; terminal transition not yet observed"
            };
            transitions.push(RequestTransitionView {
                action: "interrupt_requested".to_string(),
                from: None,
                to: state.clone(),
                at: Some(interrupt_at),
                source: "AgentRequest.interrupt_requested_at".to_string(),
                inferred: false,
                note: Some(note.to_string()),
            });
        }
    }
    if let Some(action) = terminal_action_for_state(&state) {
        transitions.push(RequestTransitionView {
            action: action.to_string(),
            from: None,
            to: state,
            at: terminal_timestamp(request, response, action),
            source: terminal_source(action).to_string(),
            inferred: true,
            note: terminal_cause.map(ToOwned::to_owned),
        });
    }
    transitions
}

fn lifecycle_has_begun_inference(state: &str) -> bool {
    matches!(
        state,
        "processing" | "inputRequired" | "completed" | "superseded"
    )
}

fn terminal_action_for_state(state: &str) -> Option<&'static str> {
    match state {
        "completed" => Some("finish"),
        "failed" => Some("fail"),
        "dead" => Some("expire"),
        "interrupted" => Some("interrupt"),
        "superseded" => Some("supersede"),
        _ => None,
    }
}

fn terminal_timestamp(request: &Value, response: Option<&Value>, action: &str) -> Option<String> {
    match action {
        "finish" | "fail" => response
            .and_then(|row| string_field(row, "completed_at"))
            .or_else(|| string_field(request, "deadline")),
        "expire" => {
            string_field(request, "valid_until").or_else(|| string_field(request, "deadline"))
        }
        "interrupt" => string_field(request, "interrupt_requested_at")
            .or_else(|| response.and_then(|row| string_field(row, "interrupted_at"))),
        _ => None,
    }
}

fn terminal_source(action: &str) -> &'static str {
    match action {
        "finish" | "fail" => "AgentResponse.completed_at",
        "expire" => "AgentRequest.valid_until",
        "interrupt" => "AgentRequest.interrupt_requested_at",
        _ => "AgentRequest.lifecycle_state",
    }
}

fn terminal_cause(
    request: &Value,
    response: Option<&Value>,
    cancel_cause: Option<&RequestCancelCauseView>,
) -> Option<String> {
    let state = string_field(request, "lifecycle_state")?;
    match state.as_str() {
        "completed" => Some("finish".to_string()),
        "failed" => Some(format!(
            "fail{}",
            cause_suffix(
                string_field(request, "failure_reason")
                    .or_else(|| response.and_then(|row| string_field(row, "error_message")))
                    .as_deref()
            )
        )),
        "dead" => Some(format!(
            "expire{}",
            cause_suffix(string_field(request, "failure_reason").as_deref())
        )),
        "interrupted" => {
            let cause = cancel_cause
                .map(|cause| cause.cause.as_str())
                .filter(|cause| !cause.trim().is_empty())
                .unwrap_or("unknown");
            Some(format!("interrupt: {cause}"))
        }
        "superseded" => Some(format!(
            "supersede{}",
            cause_suffix(string_field(request, "superseded_by_request").as_deref())
        )),
        _ => None,
    }
}

fn cause_suffix(cause: Option<&str>) -> String {
    cause
        .map(str::trim)
        .filter(|cause| !cause.is_empty())
        .map(|cause| format!(": {cause}"))
        .unwrap_or_default()
}

fn request_cancel_cause_view(
    request: &Value,
    tool_calls: &[RequestToolCallView],
) -> Option<RequestCancelCauseView> {
    let request_cause = string_field(request, "cancel_cause");
    let request_cancel_at = string_field(request, "cancel_initiated_at");
    let interrupt_at = string_field(request, "interrupt_requested_at");
    let lifecycle_state = string_field(request, "lifecycle_state").unwrap_or_default();
    let cascade_tool_cause = (lifecycle_state == "interrupted")
        .then(|| {
            tool_calls
                .iter()
                .find(|tool| tool.cancel_policy == "cascade" && tool.cancel_cause != "unknown")
                .map(|tool| tool.cancel_cause.clone())
        })
        .flatten();
    let cascade_tool_cancel_at = (lifecycle_state == "interrupted")
        .then(|| {
            tool_calls
                .iter()
                .find(|tool| tool.cancel_policy == "cascade")
                .and_then(|tool| tool.cancel_initiated_at.clone())
        })
        .flatten();
    let was_cancelled = lifecycle_state == "interrupted"
        || request_cause.is_some()
        || request_cancel_at.is_some()
        || interrupt_at.is_some();
    if !was_cancelled {
        return None;
    }
    Some(RequestCancelCauseView {
        cause: request_cause
            .or(cascade_tool_cause)
            .unwrap_or_else(|| "unknown".to_string()),
        cancel_initiated_at: request_cancel_at
            .or(interrupt_at)
            .or(cascade_tool_cancel_at),
    })
}

fn tool_call_view(
    row: &Value,
    active_tool_calls: &BTreeSet<(String, String)>,
    active_executor_counts: &BTreeMap<String, usize>,
) -> RequestToolCallView {
    let request_id = string_field_or_unknown(row, "request_id");
    let tool_call_id = string_field_or_unknown(row, "tool_call_id");
    let active_tool_call = active_tool_calls.contains(&(request_id.clone(), tool_call_id.clone()));
    let active_native_executor_count = string_field(row, "tool_call_key")
        .as_ref()
        .and_then(|key| active_executor_counts.get(key).copied())
        .unwrap_or(0);
    RequestToolCallView {
        tool_call_key: string_field_or_unknown(row, "tool_call_key"),
        request_id,
        session_id: string_field_or_unknown(row, "session_id"),
        message_sequence: integer_field(row, "message_sequence"),
        tool_name: string_field_or_unknown(row, "tool_name"),
        tool_call_id,
        status: string_field_or_unknown(row, "status"),
        state: string_field(row, "lifecycle_state")
            .or_else(|| string_field(row, "status"))
            .unwrap_or_else(|| "unknown".to_string()),
        await_mode: string_field_or_unknown(row, "await_mode"),
        cancel_policy: string_field_or_unknown(row, "cancel_policy"),
        child_terminal: string_field_or_unknown(row, "child_terminal"),
        cancel_cause: string_field_or_unknown(row, "cancel_cause"),
        cancel_initiated_at: string_field(row, "cancel_initiated_at"),
        child_request_id: string_field(row, "child_request_id"),
        started_at: string_field(row, "started_at"),
        completed_at: string_field(row, "completed_at"),
        deadline_at: string_field(row, "deadline_at"),
        active_tool_call,
        active_native_executor_count,
    }
}

fn backgrounded_tool_view(tool: &RequestToolCallView) -> BackgroundedToolView {
    BackgroundedToolView {
        tool_call_id: tool.tool_call_id.clone(),
        tool_name: tool.tool_name.clone(),
        state: tool.state.clone(),
        await_mode: tool.await_mode.clone(),
        cancel_policy: tool.cancel_policy.clone(),
        child_request_id: tool.child_request_id.clone(),
        started_at: tool.started_at.clone(),
        active_tool_call: tool.active_tool_call,
        active_native_executor_count: tool.active_native_executor_count,
    }
}

fn child_request_view(row: &Value) -> ChildRequestView {
    ChildRequestView {
        request_id: string_field_or_unknown(row, "request_id"),
        state: string_field(row, "lifecycle_state")
            .or_else(|| string_field(row, "status"))
            .unwrap_or_else(|| "unknown".to_string()),
        status: string_field_or_unknown(row, "status"),
        behavior_id: string_field_or_unknown(row, "behavior_id"),
        created_at: string_field(row, "created_at"),
        caused_by_parent_tool_call_id: string_field(row, "caused_by_parent_tool_call_id"),
        caused_by_trigger_kind: string_field(row, "caused_by_trigger_kind"),
    }
}

fn native_executor_view(row: &Value) -> NativeExecutorView {
    NativeExecutorView {
        id: unsigned_field(row, "id"),
        pid: integer_field(row, "pid"),
        argv0: string_field(row, "argv0"),
        tool_name: string_field(row, "tool_name"),
        started_at: string_field(row, "started_at"),
        kill_signaled_at: string_field(row, "kill_signaled_at"),
        reaped_at: string_field(row, "reaped_at"),
        exit_code: integer_field(row, "exit_code"),
        age_ms: integer_field(row, "age_ms"),
    }
}

fn active_tool_call_keys(liveness: &Value) -> BTreeSet<(String, String)> {
    value_array(liveness, "/active_tool_calls")
        .iter()
        .filter_map(|row| {
            Some((
                string_field(row, "request_id")?,
                string_field(row, "tool_call_id")?,
            ))
        })
        .collect()
}

fn native_executor_matches_request(
    executor: &Value,
    request_id: &str,
    tool_names: &BTreeSet<String>,
    tool_call_ids: &BTreeSet<String>,
    tool_call_keys: &BTreeSet<String>,
) -> bool {
    if string_field(executor, "request_id").as_deref() == Some(request_id) {
        return true;
    }
    if string_field(executor, "tool_call_id")
        .as_ref()
        .is_some_and(|id| tool_call_ids.contains(id))
    {
        return true;
    }
    if string_field(executor, "tool_call_key")
        .as_ref()
        .is_some_and(|key| tool_call_keys.contains(key))
    {
        return true;
    }
    string_field(executor, "tool_name")
        .as_ref()
        .is_some_and(|name| tool_names.contains(name))
}

fn active_native_executor_counts_by_tool(
    tool_rows: &[Value],
    native_executors: &[Value],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for tool in tool_rows {
        let Some(tool_call_key) = string_field(tool, "tool_call_key") else {
            continue;
        };
        let tool_call_id = string_field(tool, "tool_call_id");
        let tool_name = string_field(tool, "tool_name");
        let count = native_executors
            .iter()
            .filter(|executor| {
                (tool_call_id.is_some() && string_field(executor, "tool_call_id") == tool_call_id)
                    || string_field(executor, "tool_call_key").as_deref()
                        == Some(tool_call_key.as_str())
                    || (tool_name.is_some() && string_field(executor, "tool_name") == tool_name)
            })
            .count();
        counts.insert(tool_call_key, count);
    }
    counts
}

fn value_array(value: &Value, pointer: &str) -> Vec<Value> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn string_field(row: &Value, field: &str) -> Option<String> {
    row.get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn string_field_or_unknown(row: &Value, field: &str) -> String {
    string_field(row, field).unwrap_or_else(|| "unknown".to_string())
}

fn integer_field(row: &Value, field: &str) -> Option<i64> {
    row.get(field).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
    })
}

fn unsigned_field(row: &Value, field: &str) -> Option<u64> {
    row.get(field).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|v| u64::try_from(v).ok()))
    })
}

fn render_request_show_text(snapshot: &RequestShowSnapshot) -> String {
    let mut lines = Vec::new();
    let request = &snapshot.request;
    lines.push(format!("Request {}", request.request_id));
    lines.push(format!(
        "state: {} (status: {})",
        request.lifecycle_state, request.status
    ));
    lines.push(format!("agent_did: {}", request.agent_did));
    lines.push(format!("behavior_id: {}", request.behavior_id));
    lines.push(format!("session_id: {}", request.session_id));
    push_option_line(&mut lines, "backend_id", request.backend_id.as_deref());
    push_option_line(
        &mut lines,
        "execution_origin",
        request.execution_origin.as_deref(),
    );
    push_option_line(&mut lines, "created_at", request.created_at.as_deref());
    push_option_line(&mut lines, "claimed_at", request.claimed_at.as_deref());
    push_option_line(&mut lines, "deadline", request.deadline.as_deref());
    push_option_line(&mut lines, "valid_until", request.valid_until.as_deref());
    push_option_line(
        &mut lines,
        "failure_reason",
        request.failure_reason.as_deref(),
    );
    push_option_line(
        &mut lines,
        "terminal_cause",
        request.terminal_cause.as_deref(),
    );

    lines.push(String::new());
    lines.push("Transition history:".to_string());
    if request.transition_history.is_empty() {
        lines.push("  none observed".to_string());
    } else {
        for transition in &request.transition_history {
            let at = transition.at.as_deref().unwrap_or("unknown");
            let state_change = transition
                .from
                .as_deref()
                .map(|from| format!("{from} -> {}", transition.to))
                .unwrap_or_else(|| transition.to.clone());
            let inferred = if transition.inferred { " inferred" } else { "" };
            let note = transition
                .note
                .as_deref()
                .map(|note| format!("; {note}"))
                .unwrap_or_default();
            lines.push(format!(
                "  - {}: {} at {} (source={}{}{})",
                transition.action, state_change, at, transition.source, inferred, note
            ));
        }
    }

    if let Some(cancel) = &snapshot.cancel_cause {
        lines.push(String::new());
        lines.push("CancelCause:".to_string());
        lines.push(format!("  cause: {}", cancel.cause));
        lines.push(format!(
            "  cancel_initiated_at: {}",
            cancel.cancel_initiated_at.as_deref().unwrap_or("unknown")
        ));
    }

    lines.push(String::new());
    lines.push("Tool calls:".to_string());
    if snapshot.tool_calls.is_empty() {
        lines.push("  none".to_string());
    } else {
        for tool in &snapshot.tool_calls {
            lines.push(format!("  - {} ({})", tool.tool_call_id, tool.tool_name));
            lines.push(format!(
                "    state: {} (status: {})",
                tool.state, tool.status
            ));
            lines.push(format!("    await_mode: {}", tool.await_mode));
            lines.push(format!("    cancel_policy: {}", tool.cancel_policy));
            lines.push(format!("    child_terminal: {}", tool.child_terminal));
            lines.push(format!("    cancel_cause: {}", tool.cancel_cause));
            lines.push(format!(
                "    started_at: {}",
                tool.started_at.as_deref().unwrap_or("unknown")
            ));
            lines.push(format!(
                "    completed_at: {}",
                tool.completed_at.as_deref().unwrap_or("unknown")
            ));
            lines.push(format!(
                "    active_tool_call: {}",
                yes_no(tool.active_tool_call)
            ));
            lines.push(format!(
                "    active_native_executors: {}",
                tool.active_native_executor_count
            ));
        }
    }

    lines.push(String::new());
    lines.push("Backgrounded tools:".to_string());
    if snapshot.backgrounded_tools.is_empty() {
        lines.push("  none".to_string());
    } else {
        for tool in &snapshot.backgrounded_tools {
            lines.push(format!(
                "  - {} ({}) state={} await_mode={} cancel_policy={} active_tool_call={} active_native_executors={}",
                tool.tool_call_id,
                tool.tool_name,
                tool.state,
                tool.await_mode,
                tool.cancel_policy,
                yes_no(tool.active_tool_call),
                tool.active_native_executor_count
            ));
            push_option_line(
                &mut lines,
                "    child_request_id",
                tool.child_request_id.as_deref(),
            );
        }
    }

    if snapshot.native_executors_available && !snapshot.native_executors.is_empty() {
        lines.push(String::new());
        lines.push("Native executors:".to_string());
        for executor in &snapshot.native_executors {
            lines.push(format!(
                "  - pid={} tool_name={} started_at={} kill_signaled_at={} reaped_at={} exit_code={}",
                display_i64(executor.pid),
                executor.tool_name.as_deref().unwrap_or("unknown"),
                executor.started_at.as_deref().unwrap_or("unknown"),
                executor.kill_signaled_at.as_deref().unwrap_or("unknown"),
                executor.reaped_at.as_deref().unwrap_or("unknown"),
                display_i64(executor.exit_code)
            ));
        }
    }

    lines.push(String::new());
    lines.push("Child requests:".to_string());
    if snapshot.child_requests.is_empty() {
        lines.push("  none".to_string());
    } else {
        for child in &snapshot.child_requests {
            lines.push(format!(
                "  - {} state={} status={} behavior_id={}",
                child.request_id, child.state, child.status, child.behavior_id
            ));
            push_option_line(
                &mut lines,
                "    caused_by_parent_tool_call_id",
                child.caused_by_parent_tool_call_id.as_deref(),
            );
        }
    }

    lines.push(String::new());
    lines.join("\n")
}

fn push_option_line(lines: &mut Vec<String>, label: &str, value: Option<&str>) {
    if let Some(value) = value {
        lines.push(format!("{label}: {value}"));
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn display_i64(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

async fn request_interrupt(args: RequestInterruptArgs) -> Result<()> {
    let graphql = resolve_graphql_endpoint(args.graphql.as_deref(), args.home.as_deref())?;
    let request_id =
        resolve_request_id(args.request_id.as_deref(), args.request_id_flag.as_deref())?;
    let cancel_cause: CancelCause = args.cause.into();

    let before = fetch_interrupt_request_row(&graphql, &request_id).await?;
    let already_interrupted = request_row_string(&before, "interrupt_requested_at").is_some();
    let already_terminal = request_row_is_terminal(&before);

    if !already_interrupted && !already_terminal {
        let now = chrono::Utc::now().to_rfc3339();
        let mutation = format!(
            r#"mutation {{
                update_AgentRequest(
                    filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                    input: {{ interrupt_requested_at: "{now_escaped}" }}
                ) {{ _docID }}
            }}"#,
            request_id = escape_graphql_string(&request_id),
            now_escaped = escape_graphql_string(&now),
        );
        post_graphql(&graphql, &mutation).await?;
    }

    let mut row = fetch_interrupt_request_row(&graphql, &request_id).await?;
    let interrupt_landed_at = request_row_string(&row, "interrupt_requested_at");
    if !already_terminal && interrupt_landed_at.is_none() {
        anyhow::bail!("request {request_id} did not persist interrupt_requested_at");
    }

    if args.wait && !request_row_is_terminal(&row) {
        let timeout = parse_duration_suffix(&args.timeout)?;
        row = wait_for_terminal_request_state(&graphql, &request_id, timeout, row).await?;
    }

    let summary = request_interrupt_summary(
        &row,
        cancel_cause.as_str(),
        interrupt_landed_at.as_deref(),
        already_interrupted,
        already_terminal,
    );
    match args.output.ensure_supported(
        "request interrupt",
        &[OutputFormat::Text, OutputFormat::Json],
    )? {
        OutputFormat::Json => print_json(&summary)?,
        OutputFormat::Text => print_interrupt_text(&summary)?,
        _ => unreachable!("ensure_supported restricts request interrupt output formats"),
    }
    Ok(())
}

async fn fetch_interrupt_request_row(graphql: &str, request_id: &str) -> Result<Value> {
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{request_id}" }} }},
                order: {{ created_at: DESC }},
                limit: 1
            ) {{
                request_id
                agent_did
                behavior_id
                session_id
                status
                lifecycle_state
                failure_reason
                retry_count
                max_retries
                created_at
                claimed_at
                deadline
                valid_until
                interrupt_requested_at
            }}
        }}"#,
        request_id = escape_graphql_string(request_id),
    );
    let response = post_graphql(graphql, &query).await?;
    response
        .pointer("/data/AgentRequest")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("request {request_id} not found"))
}

async fn wait_for_terminal_request_state(
    graphql: &str,
    request_id: &str,
    timeout: Duration,
    mut last_row: Value,
) -> Result<Value> {
    let deadline = Instant::now() + timeout;
    loop {
        if request_row_is_terminal(&last_row) {
            return Ok(last_row);
        }
        if Instant::now() >= deadline {
            let state = request_row_string(&last_row, "lifecycle_state")
                .unwrap_or_else(|| "<missing>".to_string());
            anyhow::bail!(
                "timed out waiting for request {request_id} to reach a terminal state after {}s (last lifecycle_state={state})",
                timeout.as_secs()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        last_row = fetch_interrupt_request_row(graphql, request_id).await?;
    }
}

fn request_row_is_terminal(row: &Value) -> bool {
    row.get("lifecycle_state")
        .and_then(Value::as_str)
        .is_some_and(is_terminal_lifecycle_state)
}

fn request_row_string(row: &Value, key: &str) -> Option<String> {
    row.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn request_interrupt_summary(
    row: &Value,
    cause: &str,
    interrupt_landed_at: Option<&str>,
    already_interrupted: bool,
    already_terminal: bool,
) -> Value {
    let lifecycle_state = request_row_string(row, "lifecycle_state").unwrap_or_default();
    json!({
        "request_id": request_row_string(row, "request_id").unwrap_or_default(),
        "agent_did": request_row_string(row, "agent_did"),
        "behavior_id": request_row_string(row, "behavior_id"),
        "session_id": request_row_string(row, "session_id"),
        "status": request_row_string(row, "status"),
        "lifecycle_state": lifecycle_state,
        "failure_reason": request_row_string(row, "failure_reason"),
        "interrupt_requested_at": request_row_string(row, "interrupt_requested_at"),
        "interrupt_landed_at": interrupt_landed_at,
        "cause": cause,
        "already_interrupted": already_interrupted,
        "already_terminal": already_terminal,
        "terminal": is_terminal_lifecycle_state(&lifecycle_state),
        "created_at": request_row_string(row, "created_at"),
        "claimed_at": request_row_string(row, "claimed_at"),
        "deadline": request_row_string(row, "deadline"),
        "valid_until": request_row_string(row, "valid_until"),
    })
}

fn print_interrupt_text(summary: &Value) -> Result<()> {
    let text = |key: &str| {
        summary
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or("-")
            .to_string()
    };
    println!("request_id: {}", text("request_id"));
    println!("state: {}", text("lifecycle_state"));
    println!("status: {}", text("status"));
    println!("interrupt_landed_at: {}", text("interrupt_landed_at"));
    println!("cause: {}", text("cause"));
    println!(
        "already_interrupted: {}",
        summary
            .get("already_interrupted")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    );
    println!(
        "already_terminal: {}",
        summary
            .get("already_terminal")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    );
    println!(
        "terminal: {}",
        summary
            .get("terminal")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    );
    if summary
        .get("already_terminal")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && summary
            .get("interrupt_landed_at")
            .is_none_or(Value::is_null)
    {
        println!("note: request was already terminal; interrupt was not latched");
    }
    if let Some(reason) = summary
        .get("failure_reason")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        println!("failure_reason: {reason}");
    }
    Ok(())
}

async fn request_resend(args: RequestResendArgs) -> Result<()> {
    let graphql = resolve_graphql_endpoint(args.graphql.as_deref(), args.home.as_deref())?;
    let stale_id = resolve_request_id(args.request_id.as_deref(), args.request_id_flag.as_deref())?;
    let stale = fetch_request_view(&graphql, &stale_id).await?;
    if stale.lifecycle_state != "dead" || stale.failure_reason != "Stale" {
        anyhow::bail!(
            "request {stale_id} is not a stale terminal (lifecycle_state={}, failure_reason={}); resend is only valid for stale-dead requests",
            stale.lifecycle_state,
            stale.failure_reason
        );
    }
    let valid_until = Some(chrono::Utc::now() + chrono::Duration::minutes(5));
    let submitted = create_agent_request(
        &graphql,
        &stale.agent_did,
        &stale.content,
        None,
        stale.behavior_id.as_deref(),
        RequestSubmitOptions {
            temperature: stale.temperature,
            top_p: stale.top_p,
            top_k: stale.top_k,
            seed: stale.seed,
            max_tokens: stale.max_tokens,
            metadata: stale.metadata.clone(),
            valid_until,
            retry_parent_request: Some(stale_id.clone()),
            retry_root_request: stale.retry_root_request.clone(),
        },
    )
    .await?;
    let request_summary = json!({
        "request_id": submitted.request_id,
        "session_id": submitted.session_id,
        "agent_did": submitted.agent_did,
        "behavior_id": submitted.behavior_id,
        "retry_parent_request": stale_id,
        "retry_root_request": stale.retry_root_request,
    });
    if args.no_wait {
        print_json(&request_summary)?;
        if let Some(path) = args.output_file.as_deref() {
            write_json_output_file(path, &request_summary)?;
        }
        return Ok(());
    }
    let response = wait_for_terminal_response(
        &graphql,
        &submitted.request_id,
        args.timeout_secs,
        args.poll_secs,
    )
    .await
    .with_context(|| format!("waiting for AgentResponse {}", submitted.request_id))?;
    let mut output = request_summary
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("request summary was not a JSON object"))?;
    output.insert("response".to_string(), response);
    let output = serde_json::Value::Object(output);
    print_json(&output)?;
    if let Some(path) = args.output_file.as_deref() {
        write_json_output_file(path, &output)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_show_json_retains_persisted_seed() {
        let request = json!({
            "request_id": "request-one",
            "agent_did": "did:key:agent",
            "behavior_id": "default",
            "session_id": "session-one",
            "status": "pending",
            "lifecycle_state": "pending",
            "seed": 1234,
        });
        let header = request_header_view(&request, None, Vec::new());
        let value = serde_json::to_value(header).unwrap();

        assert_eq!(value["seed"], 1234);
    }

    #[test]
    fn transition_history_does_not_emit_processing_self_loop() {
        let request = json!({
            "lifecycle_state": "processing",
            "claimed_at": "2026-05-20T10:00:01Z",
        });

        let transitions = transition_history(&request, None, None);

        assert!(transitions
            .iter()
            .any(|transition| transition.action == "claim"));
        assert!(transitions
            .iter()
            .any(|transition| transition.action == "begin_inference"));
        assert!(!transitions
            .iter()
            .any(|transition| transition.action == "advance"));
        assert!(!transitions.iter().any(|transition| transition
            .from
            .as_deref()
            .is_some_and(|from| from == transition.to)));
    }

    #[test]
    fn transition_history_keeps_interrupt_attempt_on_terminal_completion() {
        let request = json!({
            "lifecycle_state": "completed",
            "claimed_at": "2026-05-20T10:00:01Z",
            "interrupt_requested_at": "2026-05-20T10:00:02Z",
        });
        let response = json!({
            "completed_at": "2026-05-20T10:00:03Z",
        });

        let transitions = transition_history(&request, Some(&response), Some("finish"));
        let interrupt = transitions
            .iter()
            .find(|transition| transition.action == "interrupt_requested")
            .expect("interrupt request transition should be present");
        assert_eq!(interrupt.from, None);
        assert_eq!(interrupt.to, "completed");
        assert_eq!(interrupt.at.as_deref(), Some("2026-05-20T10:00:02Z"));
        assert!(interrupt
            .note
            .as_deref()
            .is_some_and(|note| note.contains("terminal snapshot")));

        let finish = transitions
            .iter()
            .find(|transition| transition.action == "finish")
            .expect("finish transition should be present");
        assert_eq!(finish.from, None);
        assert_eq!(finish.at.as_deref(), Some("2026-05-20T10:00:03Z"));
    }

    #[test]
    fn terminal_transitions_do_not_guess_prior_state() {
        let cases = [
            ("completed", "finish"),
            ("failed", "fail"),
            ("dead", "expire"),
            ("interrupted", "interrupt"),
            ("superseded", "supersede"),
        ];

        for (state, action) in cases {
            let request = json!({
                "lifecycle_state": state,
                "claimed_at": "2026-05-20T10:00:01Z",
                "interrupt_requested_at": "2026-05-20T10:00:02Z",
                "valid_until": "2026-05-20T10:00:03Z",
                "deadline": "2026-05-20T10:00:04Z",
            });
            let transitions = transition_history(&request, None, Some(action));
            let terminal = transitions
                .iter()
                .find(|transition| transition.action == action)
                .expect("terminal transition should be present");

            assert_eq!(
                terminal.from, None,
                "{state} should not infer a prior state"
            );
        }
    }

    #[test]
    fn request_cancel_cause_ignores_tool_causes_for_non_interrupted_requests() {
        let request = json!({
            "lifecycle_state": "processing",
        });
        let tool_calls = vec![request_tool_call(
            "cascade",
            "operator_interrupt",
            Some("2026-05-20T10:00:02Z"),
        )];

        assert!(request_cancel_cause_view(&request, &tool_calls).is_none());
    }

    #[test]
    fn request_cancel_cause_only_falls_back_to_cascade_tool_on_interrupted_requests() {
        let request = json!({
            "lifecycle_state": "interrupted",
        });

        let independent_only = vec![request_tool_call(
            "independent",
            "independent_tool_timeout",
            Some("2026-05-20T10:00:02Z"),
        )];
        let cancel = request_cancel_cause_view(&request, &independent_only)
            .expect("interrupted requests should render CancelCause");
        assert_eq!(cancel.cause, "unknown");
        assert_eq!(cancel.cancel_initiated_at, None);

        let cascade_time_only = vec![request_tool_call(
            "cascade",
            "unknown",
            Some("2026-05-20T10:00:03Z"),
        )];
        let cancel = request_cancel_cause_view(&request, &cascade_time_only)
            .expect("interrupted requests should render CancelCause");
        assert_eq!(cancel.cause, "unknown");
        assert_eq!(
            cancel.cancel_initiated_at.as_deref(),
            Some("2026-05-20T10:00:03Z")
        );

        let cascade_tool = vec![
            request_tool_call(
                "independent",
                "independent_tool_timeout",
                Some("2026-05-20T10:00:02Z"),
            ),
            request_tool_call(
                "cascade",
                "operator_interrupt",
                Some("2026-05-20T10:00:03Z"),
            ),
        ];
        let cancel = request_cancel_cause_view(&request, &cascade_tool)
            .expect("interrupted requests should render CancelCause");
        assert_eq!(cancel.cause, "operator_interrupt");
        assert_eq!(
            cancel.cancel_initiated_at.as_deref(),
            Some("2026-05-20T10:00:03Z")
        );
    }

    fn request_tool_call(
        cancel_policy: &str,
        cancel_cause: &str,
        cancel_initiated_at: Option<&str>,
    ) -> RequestToolCallView {
        RequestToolCallView {
            tool_call_key: "session:tool".to_string(),
            request_id: "request".to_string(),
            session_id: "session".to_string(),
            message_sequence: Some(1),
            tool_name: "spawn_subagent".to_string(),
            tool_call_id: "tool".to_string(),
            status: "called".to_string(),
            state: "running".to_string(),
            await_mode: "background".to_string(),
            cancel_policy: cancel_policy.to_string(),
            child_terminal: "unknown".to_string(),
            cancel_cause: cancel_cause.to_string(),
            cancel_initiated_at: cancel_initiated_at.map(ToOwned::to_owned),
            child_request_id: None,
            started_at: Some("2026-05-20T10:00:01Z".to_string()),
            completed_at: None,
            deadline_at: None,
            active_tool_call: false,
            active_native_executor_count: 0,
        }
    }
}
