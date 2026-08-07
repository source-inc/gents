use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::time::Duration;

use anyhow::{Context, Result};
use gents::adapter_projection::{
    adapter_projection_eval_jsonl_record_schema, adapter_projection_eval_jsonl_records,
    adapter_projection_json_schema, adapter_projection_jsonl_record_schema,
    adapter_projection_jsonl_records, adapter_projection_native_json,
    adapter_projection_native_json_schema, build_adapter_projection,
    validate_adapter_projection_contract, AdapterProjectionKind, ProjectionContext,
    ProjectionRedactionMode,
};
use gents::graphql::escape_graphql_string;
use gents::run_timeline::{
    build_run_timeline, RunTimeline, RunTimelineEvent, RunTimelineRows, TimelineRequestEvent,
    TimelineRequestRow, TimelineToolCallEvent,
};
#[cfg(test)]
use gents::run_timeline::{
    TimelineConversationRow, TimelineInferenceCallRow, TimelineMessageRow, TimelineResponseRow,
    TimelineSessionRow, TimelineToolCallRow,
};
use gents::run_timeline_fetch::{load_run_timeline, load_run_timeline_rows};
use gents::trace_export::{
    analyze_request_failure, analyze_tool_call, extract_raw_tool_call_json, latency_ms,
    raw_message_json, AmyToolCallTraceRecord,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::cli::args::{
    TraceCaptureArgs, TraceCommand, TraceExportArgs, TraceProjectArgs, TraceProjectSchemaArgs,
    TraceProjectionArg, TraceProjectionFormatArg, TraceProjectionRedactionArg, TraceTimelineArgs,
};
use crate::config_writes::ConfigAccess;
use crate::{
    graphql_rows_or_empty_if_collection_missing, graphql_string_list_literal, print_json,
    write_json_output_file,
};

pub(crate) async fn dispatch(command: TraceCommand) -> Result<()> {
    match command {
        TraceCommand::Export(args) => trace_export(args).await,
        TraceCommand::Timeline(args) => trace_timeline(args).await,
        TraceCommand::Project(args) => trace_project(args).await,
        TraceCommand::ProjectSchema(args) => trace_project_schema(args),
        TraceCommand::Capture(args) => trace_capture(args).await,
    }
}

/// Fetch rendered-request capture metadata — and, for exactly one match, its
/// `request_json` field-commit CID. This is the one deliberate body read in
/// the system: `--include-body` selects `request_json` and the raw provenance
/// manifest; without it neither is even queried, and the default output is the
/// same metadata surface the timeline exposes.
async fn trace_capture(args: TraceCaptureArgs) -> Result<()> {
    let (access, _home_dir) =
        crate::resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;

    let mut clauses = Vec::new();
    if let Some(capture_key) = args.capture_key.as_deref() {
        clauses.push(format!(
            r#"capture_key: {{ _eq: "{}" }}"#,
            escape_graphql_string(capture_key)
        ));
    }
    if let Some(request_id) = args.request_id.as_deref() {
        clauses.push(format!(
            r#"request_id: {{ _eq: "{}" }}"#,
            escape_graphql_string(request_id)
        ));
    }
    if clauses.is_empty() {
        anyhow::bail!("pass --capture-key or --request-id");
    }
    if let Some(scope) = args.scope.as_deref() {
        clauses.push(format!(
            r#"capture_scope: {{ _eq: "{}" }}"#,
            escape_graphql_string(scope)
        ));
    }
    if let Some(turn) = args.turn {
        clauses.push(format!("turn_index: {{ _eq: {turn} }}"));
    }
    if let Some(attempt) = args.attempt {
        clauses.push(format!("attempt: {{ _eq: {attempt} }}"));
    }

    let body_fields = if args.include_body {
        "\n                request_json"
    } else {
        ""
    };
    let query = format!(
        r#"{{
            RenderedRequest(
                filter: {{ {filter} }},
                order: {{ created_at: ASC }}
            ) {{
                _docID
                capture_key
                request_doc_id
                request_id
                session_id
                capture_scope
                turn_index
                attempt
                capture_version
                model_name
                source
                prompt_hash
                tools_hash
                provenance_json
                created_at{body_fields}
            }}
        }}"#,
        filter = clauses.join(", "),
    );
    let raw_rows =
        graphql_rows_or_empty_if_collection_missing(&access, "RenderedRequest", &query).await?;

    let mut entries = raw_rows
        .into_iter()
        .map(|raw| {
            let row: gents::run_timeline::TimelineRenderedRequestRow =
                serde_json::from_value(raw.clone()).context("decoding RenderedRequest row")?;
            Ok((row, raw))
        })
        .collect::<Result<Vec<_>>>()?;
    if entries.is_empty() {
        anyhow::bail!("no capture rows matched");
    }
    // Identity order: parsed numeric order key first, unparseable rows last by
    // capture key — deterministic either way, never a lexical seq sort.
    entries.sort_by(|(left, _), (right, _)| {
        let left_key = capture_order_padded(left);
        let right_key = capture_order_padded(right);
        left_key
            .cmp(&right_key)
            .then_with(|| left.capture_key.cmp(&right.capture_key))
    });

    if args.list {
        let captures = entries
            .iter()
            .map(|(row, raw)| capture_metadata_value(row, raw, args.include_body))
            .collect::<Vec<_>>();
        let value = json!({ "captures": captures });
        return write_or_print(args.output_file.as_deref(), &value);
    }

    if entries.len() > 1 {
        let keys = entries
            .iter()
            .map(|(row, _)| {
                format!(
                    "  {} ({} turn {} attempt {})",
                    row.capture_key,
                    row.capture_scope.as_deref().unwrap_or("?"),
                    row.turn_index.map_or("?".to_string(), |t| t.to_string()),
                    row.attempt.map_or("?".to_string(), |a| a.to_string()),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        anyhow::bail!(
            "{count} capture rows matched; narrow with --scope/--turn/--attempt or pass --list:\n{keys}",
            count = entries.len(),
        );
    }

    let (row, raw) = &entries[0];
    let mut value = capture_metadata_value(row, raw, args.include_body);
    let commit = match row.doc_id.as_deref() {
        Some(doc_id) => gents::rendered_request::commits::request_json_commit(&access, doc_id)
            .await?
            .map(|commit| json!({ "cid": commit.cid, "height": commit.height })),
        None => None,
    };
    value["request_json_commit"] = commit.unwrap_or_else(|| json!("unavailable"));
    write_or_print(args.output_file.as_deref(), &value)
}

/// The metadata object for one capture row: the timeline's event derivation
/// plus the document id, with the body fields attached only on request.
fn capture_metadata_value(
    row: &gents::run_timeline::TimelineRenderedRequestRow,
    raw: &Value,
    include_body: bool,
) -> Value {
    let event = gents::run_timeline::rendered_request_event(row);
    let mut value = serde_json::to_value(&event).unwrap_or_else(|_| json!({}));
    value["doc_id"] = json!(row.doc_id);
    if include_body {
        value["request_json"] = raw.get("request_json").cloned().unwrap_or(Value::Null);
        value["provenance_json"] = json!(row.provenance_json);
    }
    value
}

fn capture_order_padded(row: &gents::run_timeline::TimelineRenderedRequestRow) -> String {
    use gents_protocol::rendered_request::{CaptureOrderKey, CaptureScope};

    let scope = row
        .capture_scope
        .as_deref()
        .and_then(|scope| scope.parse::<CaptureScope>().ok());
    match (scope, row.turn_index, row.attempt) {
        (Some(scope), Some(turn_index), Some(attempt)) => CaptureOrderKey {
            scope,
            turn_index,
            attempt,
        }
        .padded(),
        // '~' sorts after every padded key's alphabet, pushing unparseable
        // rows to the end.
        _ => format!("~{}", row.capture_key),
    }
}

fn write_or_print(output_file: Option<&std::path::Path>, value: &Value) -> Result<()> {
    if let Some(path) = output_file {
        write_json_output_file(path, value)?;
    } else {
        print_json(value)?;
    }
    Ok(())
}

async fn trace_timeline(args: TraceTimelineArgs) -> Result<()> {
    let (access, _home_dir) =
        crate::resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;
    let timeline = load_run_timeline(&access, &args.request_id).await?;
    let value = serde_json::to_value(&timeline)?;
    if let Some(path) = args.output_file.as_deref() {
        write_json_output_file(path, &value)?;
    } else {
        print_json(&value)?;
    }
    Ok(())
}

async fn trace_project(args: TraceProjectArgs) -> Result<()> {
    let (access, _home_dir) =
        crate::resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;
    let actor_did = args.actor_did;
    let projection_kind = adapter_projection_kind(args.projection);
    let scope = ProjectionDocumentScope {
        agent_did: optional_scope_arg("scope-agent-did", args.scope_agent_did)?,
        behavior_id: optional_scope_arg("scope-behavior-id", args.scope_behavior_id)?,
        session_id: optional_scope_arg("scope-session-id", args.scope_session_id)?,
    };
    let rows = load_run_timeline_rows(&access, &args.request_id).await?;
    let acp_scope = projection_acp_read_scope(
        &access,
        args.acp_policy_id.as_deref(),
        actor_did.as_deref(),
        projection_kind,
        &rows.request,
    )
    .await?;
    let rows = match acp_scope.as_ref() {
        Some(acp_scope) => apply_projection_acp_read_filter(rows, acp_scope).await?,
        None => rows,
    };
    let timeline = apply_projection_document_scope(build_run_timeline(rows), &scope)?;
    let context = ProjectionContext {
        actor_did,
        redaction_mode: projection_redaction_mode(args.redaction),
    };
    let projection = build_adapter_projection(projection_kind, &timeline, &context);
    validate_adapter_projection_contract(&projection)?;
    match args.format {
        TraceProjectionFormatArg::Json => {
            let value = serde_json::to_value(&projection)?;
            if let Some(path) = args.output_file.as_deref() {
                write_json_output_file(path, &value)?;
            } else {
                print_json(&value)?;
            }
        }
        TraceProjectionFormatArg::NativeJson => {
            let value = adapter_projection_native_json(&projection);
            if let Some(path) = args.output_file.as_deref() {
                write_json_output_file(path, &value)?;
            } else {
                print_json(&value)?;
            }
        }
        TraceProjectionFormatArg::Jsonl => {
            let records = adapter_projection_jsonl_records(&projection);
            write_jsonl(args.output_file.as_deref(), &records)?;
        }
        TraceProjectionFormatArg::EvalJsonl => {
            let records = adapter_projection_eval_jsonl_records(&projection);
            write_jsonl(args.output_file.as_deref(), &records)?;
        }
    }
    Ok(())
}

fn trace_project_schema(args: TraceProjectSchemaArgs) -> Result<()> {
    let kind = adapter_projection_kind(args.projection);
    let schema = match args.format {
        TraceProjectionFormatArg::Json => adapter_projection_json_schema(kind),
        TraceProjectionFormatArg::NativeJson => adapter_projection_native_json_schema(kind),
        TraceProjectionFormatArg::Jsonl => adapter_projection_jsonl_record_schema(kind),
        TraceProjectionFormatArg::EvalJsonl => adapter_projection_eval_jsonl_record_schema(kind),
    };
    if let Some(path) = args.output_file.as_deref() {
        write_json_output_file(path, &schema)?;
    } else {
        print_json(&schema)?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ProjectionAcpReadScope {
    actor_did: String,
    policy_id: String,
    api_base: String,
    resource_names: BTreeMap<String, String>,
}

impl ProjectionAcpReadScope {
    fn resource_name<'a>(&'a self, collection: &'a str) -> &'a str {
        self.resource_names
            .get(collection)
            .map(String::as_str)
            .unwrap_or(collection)
    }
}

#[derive(Debug, Deserialize)]
struct ProjectionAcpDecisionResponse {
    allowed: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ProjectionAcpBindingRow {
    #[serde(default)]
    binding_id: String,
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(default)]
    behavior_id: Option<String>,
    #[serde(default)]
    projection_id: Option<String>,
    #[serde(default)]
    policy_id: String,
    #[serde(default)]
    staged_policy_id: Option<String>,
    #[serde(default)]
    previous_policy_id: Option<String>,
    #[serde(default)]
    resource_map_json: Option<String>,
    #[serde(default)]
    publication_status: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
}

const PROJECTION_ACP_RUNTIME_COLLECTIONS: &[&str] = &[
    "AgentRequest",
    "AgentMessage",
    "AgentToolCall",
    "AgentResponse",
    "InferenceCall",
    "AgentSession",
    "AgentConversation",
];

async fn projection_acp_read_scope(
    access: &ConfigAccess,
    policy_id: Option<&str>,
    actor_did: Option<&str>,
    projection_kind: AdapterProjectionKind,
    request: &TimelineRequestRow,
) -> Result<Option<ProjectionAcpReadScope>> {
    let (policy_id, resource_names) =
        match policy_id.map(str::trim).filter(|value| !value.is_empty()) {
            Some(policy_id) => (policy_id.to_string(), BTreeMap::new()),
            None => {
                let Some(binding) =
                    discover_projection_acp_binding(access, projection_kind, request).await?
                else {
                    return Ok(None);
                };
                (
                    binding.policy_id.trim().to_string(),
                    parse_projection_resource_map(binding.resource_map_json.as_deref())?,
                )
            }
        };
    let actor_did = actor_did
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("projection ACP enforcement requires --actor-did"))?;
    let ConfigAccess::Graphql(graphql) = access else {
        anyhow::bail!(
            "projection ACP enforcement requires --graphql so DefraDB ACP can decide documents"
        );
    };
    Ok(Some(ProjectionAcpReadScope {
        actor_did: actor_did.to_string(),
        policy_id,
        api_base: crate::graphql_access::graphql_api_base(graphql)?,
        resource_names,
    }))
}

async fn discover_projection_acp_binding(
    access: &ConfigAccess,
    projection_kind: AdapterProjectionKind,
    request: &TimelineRequestRow,
) -> Result<Option<ProjectionAcpBindingRow>> {
    let Some(agent_did) = normalize_projection_binding_field(request.agent_did.as_deref()) else {
        return Ok(None);
    };
    let query = format!(
        r#"{{
            ProjectionAcpBinding(
                filter: {{
                    enabled: {{ _eq: true }}
                    agent_did: {{ _eq: "{agent_did}" }}
                }}
            ) {{
                binding_id
                agent_did
                behavior_id
                projection_id
                policy_id
                staged_policy_id
                previous_policy_id
                resource_map_json
                publication_status
                enabled
            }}
        }}"#,
        agent_did = escape_graphql_string(agent_did),
    );
    let rows = load_rows::<ProjectionAcpBindingRow>(access, "ProjectionAcpBinding", &query).await?;
    select_projection_acp_binding(rows, projection_kind, request)
}

fn select_projection_acp_binding(
    rows: Vec<ProjectionAcpBindingRow>,
    projection_kind: AdapterProjectionKind,
    request: &TimelineRequestRow,
) -> Result<Option<ProjectionAcpBindingRow>> {
    let mut best = None::<(u8, ProjectionAcpBindingRow)>;
    let projection_id = projection_kind.id();
    for row in rows {
        if row.enabled == Some(false) {
            continue;
        }
        if row.policy_id.trim().is_empty() {
            continue;
        }
        let Some(scope_mask) = projection_binding_scope_mask(&row, projection_id, request) else {
            continue;
        };
        validate_projection_binding_operational_state(&row)?;
        match &best {
            None => best = Some((scope_mask, row)),
            Some((best_mask, _)) if projection_binding_scope_dominates(scope_mask, *best_mask) => {
                best = Some((scope_mask, row));
            }
            Some((best_mask, _)) if projection_binding_scope_dominates(*best_mask, scope_mask) => {}
            Some((_, best_row)) => {
                anyhow::bail!(
                    "ambiguous ProjectionAcpBinding rows for projection {} request {}: {} and {}",
                    projection_id,
                    request.request_id,
                    projection_binding_label(best_row),
                    projection_binding_label(&row)
                );
            }
        }
    }
    Ok(best.map(|(_, row)| row))
}

const PROJECTION_ACP_BINDING_PROJECTION_SCOPE: u8 = 0b100;
const PROJECTION_ACP_BINDING_AGENT_SCOPE: u8 = 0b010;
const PROJECTION_ACP_BINDING_BEHAVIOR_SCOPE: u8 = 0b001;

fn projection_binding_scope_mask(
    row: &ProjectionAcpBindingRow,
    projection_id: &str,
    request: &TimelineRequestRow,
) -> Option<u8> {
    let row_agent_did = normalize_projection_binding_field(row.agent_did.as_deref())?;
    if request.agent_did.as_deref() != Some(row_agent_did) {
        return None;
    }
    let mut scope_mask = PROJECTION_ACP_BINDING_AGENT_SCOPE;
    if let Some(row_projection_id) =
        normalize_projection_binding_field(row.projection_id.as_deref())
    {
        if row_projection_id != projection_id {
            return None;
        }
        scope_mask |= PROJECTION_ACP_BINDING_PROJECTION_SCOPE;
    }
    if let Some(row_behavior_id) = normalize_projection_binding_field(row.behavior_id.as_deref()) {
        if request.behavior_id.as_deref() != Some(row_behavior_id) {
            return None;
        }
        scope_mask |= PROJECTION_ACP_BINDING_BEHAVIOR_SCOPE;
    }
    Some(scope_mask)
}

fn validate_projection_binding_operational_state(row: &ProjectionAcpBindingRow) -> Result<()> {
    let status = normalize_projection_binding_field(row.publication_status.as_deref())
        .unwrap_or("published");
    let staged_policy_id = normalize_projection_binding_field(row.staged_policy_id.as_deref());
    let previous_policy_id = normalize_projection_binding_field(row.previous_policy_id.as_deref());
    let active_policy_id = row.policy_id.trim();
    match status {
        "published" => {
            if staged_policy_id.is_some() {
                anyhow::bail!(
                    "enabled ProjectionAcpBinding {} is published but still has staged_policy_id",
                    projection_binding_label(row)
                );
            }
        }
        "rotating" => {
            let Some(staged_policy_id) = staged_policy_id else {
                anyhow::bail!(
                    "enabled ProjectionAcpBinding {} is rotating but has no staged_policy_id",
                    projection_binding_label(row)
                );
            };
            if staged_policy_id == active_policy_id {
                anyhow::bail!(
                    "enabled ProjectionAcpBinding {} staged_policy_id must differ from policy_id",
                    projection_binding_label(row)
                );
            }
        }
        "draft" | "staged" | "retired" => {
            anyhow::bail!(
                "enabled ProjectionAcpBinding {} has non-operational publication_status {}; disable it or publish it before projection enforcement",
                projection_binding_label(row),
                status
            );
        }
        _ => {
            anyhow::bail!(
                "enabled ProjectionAcpBinding {} has invalid publication_status {}",
                projection_binding_label(row),
                status
            );
        }
    }
    if previous_policy_id == Some(active_policy_id) {
        anyhow::bail!(
            "enabled ProjectionAcpBinding {} previous_policy_id must differ from policy_id",
            projection_binding_label(row)
        );
    }
    if previous_policy_id == staged_policy_id && previous_policy_id.is_some() {
        anyhow::bail!(
            "enabled ProjectionAcpBinding {} previous_policy_id must differ from staged_policy_id",
            projection_binding_label(row)
        );
    }
    Ok(())
}

fn projection_binding_scope_dominates(candidate: u8, current: u8) -> bool {
    candidate != current && (candidate & current) == current
}

fn normalize_projection_binding_field(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn projection_binding_label(row: &ProjectionAcpBindingRow) -> &str {
    normalize_projection_binding_field(Some(&row.binding_id)).unwrap_or("<unnamed>")
}

fn parse_projection_resource_map(
    resource_map_json: Option<&str>,
) -> Result<BTreeMap<String, String>> {
    let Some(raw) = resource_map_json
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(BTreeMap::new());
    };
    let raw_map = serde_json::from_str::<BTreeMap<String, String>>(raw)
        .context("parsing ProjectionAcpBinding.resource_map_json")?;
    let mut map = BTreeMap::new();
    for (collection, resource_name) in raw_map {
        let collection = collection.trim();
        let resource_name = resource_name.trim();
        if collection.is_empty() || resource_name.is_empty() {
            anyhow::bail!(
                "ProjectionAcpBinding.resource_map_json must map non-empty collection names to non-empty ACP resource names"
            );
        }
        if !PROJECTION_ACP_RUNTIME_COLLECTIONS.contains(&collection) {
            anyhow::bail!(
                "ProjectionAcpBinding.resource_map_json contains unknown runtime collection {collection}; expected one of {}",
                PROJECTION_ACP_RUNTIME_COLLECTIONS.join(", ")
            );
        }
        map.insert(collection.to_string(), resource_name.to_string());
    }
    Ok(map)
}

async fn apply_projection_acp_read_filter(
    rows: RunTimelineRows,
    scope: &ProjectionAcpReadScope,
) -> Result<RunTimelineRows> {
    let mut decider = ProjectionAcpReadDecider::new(scope)?;
    let request_doc_id = required_doc_id(
        "AgentRequest",
        rows.request.request_id.as_str(),
        &rows.request.doc_id,
    )?;
    if !decider
        .read_allowed(scope.resource_name("AgentRequest"), request_doc_id)
        .await?
    {
        anyhow::bail!(
            "DefraDB ACP denied read access to root request {}",
            rows.request.request_id
        );
    }

    let mut filtered_requests = Vec::new();
    for request in rows.requests {
        let doc_id = required_doc_id("AgentRequest", request.request_id.as_str(), &request.doc_id)?;
        if decider
            .read_allowed(scope.resource_name("AgentRequest"), doc_id)
            .await?
        {
            filtered_requests.push(request);
        }
    }

    let mut filtered_messages = Vec::new();
    for message in rows.messages {
        let label = format!("{}:{}", message.session_id, message.sequence);
        let doc_id = required_doc_id("AgentMessage", &label, &message.doc_id)?;
        if decider
            .read_allowed(scope.resource_name("AgentMessage"), doc_id)
            .await?
        {
            filtered_messages.push(message);
        }
    }

    let mut filtered_tool_calls = Vec::new();
    for tool_call in rows.tool_calls {
        let doc_id = required_doc_id(
            "AgentToolCall",
            tool_call.tool_call_id.as_str(),
            &tool_call.doc_id,
        )?;
        if decider
            .read_allowed(scope.resource_name("AgentToolCall"), doc_id)
            .await?
        {
            filtered_tool_calls.push(tool_call);
        }
    }

    let mut filtered_responses = Vec::new();
    for response in rows.responses {
        let doc_id = required_doc_id(
            "AgentResponse",
            response.request_id.as_str(),
            &response.doc_id,
        )?;
        if decider
            .read_allowed(scope.resource_name("AgentResponse"), doc_id)
            .await?
        {
            filtered_responses.push(response);
        }
    }

    let mut filtered_inference_calls = Vec::new();
    for call in rows.inference_calls {
        let label = format!("{}:{}", call.request_id, call.call_seq);
        let doc_id = required_doc_id("InferenceCall", &label, &call.doc_id)?;
        if decider
            .read_allowed(scope.resource_name("InferenceCall"), doc_id)
            .await?
        {
            filtered_inference_calls.push(call);
        }
    }

    let session = match rows.session {
        Some(session) => {
            let doc_id =
                required_doc_id("AgentSession", session.session_id.as_str(), &session.doc_id)?;
            if decider
                .read_allowed(scope.resource_name("AgentSession"), doc_id)
                .await?
            {
                Some(session)
            } else {
                None
            }
        }
        None => None,
    };
    let conversation = match rows.conversation {
        Some(conversation) => {
            let doc_id = required_doc_id(
                "AgentConversation",
                conversation.session_id.as_str(),
                &conversation.doc_id,
            )?;
            if decider
                .read_allowed(scope.resource_name("AgentConversation"), doc_id)
                .await?
            {
                Some(conversation)
            } else {
                None
            }
        }
        None => None,
    };

    Ok(RunTimelineRows {
        request: rows.request,
        session,
        conversation,
        requests: filtered_requests,
        messages: filtered_messages,
        tool_calls: filtered_tool_calls,
        inference_calls: filtered_inference_calls,
        responses: filtered_responses,
        // Passed through unfiltered: ACP enforcement is DefraDB's, not this
        // filter's. Reads executed under a requester identity return only the
        // rows that identity may see, and an unpoliced collection is public by
        // DefraDB's own rules. When `RenderedRequest` gains its `@policy`,
        // rows are excluded at the database read — with nothing to change
        // here. (This per-row decider exists only for the actor-on-behalf
        // GraphQL path the seven families above already use.)
        rendered_requests: rows.rendered_requests,
    })
}

struct ProjectionAcpReadDecider<'a> {
    scope: &'a ProjectionAcpReadScope,
    client: reqwest::Client,
    cache: BTreeMap<(String, String), bool>,
}

impl<'a> ProjectionAcpReadDecider<'a> {
    fn new(scope: &'a ProjectionAcpReadScope) -> Result<Self> {
        Ok(Self {
            scope,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .context("building ACP decision client")?,
            cache: BTreeMap::new(),
        })
    }

    async fn read_allowed(&mut self, resource_name: &str, doc_id: &str) -> Result<bool> {
        let key = (resource_name.to_string(), doc_id.to_string());
        if let Some(allowed) = self.cache.get(&key) {
            return Ok(*allowed);
        }
        let url = format!(
            "{}/acp/document/decide",
            self.scope.api_base.trim_end_matches('/')
        );
        let response = self
            .client
            .post(url)
            .json(&json!({
                "actor": self.scope.actor_did,
                "permission": "read",
                "policyID": self.scope.policy_id,
                "resourceName": resource_name,
                "docID": doc_id,
            }))
            .send()
            .await
            .with_context(|| {
                format!("requesting DefraDB ACP read decision for {resource_name}/{doc_id}")
            })?;
        let status = response.status();
        let text = response
            .text()
            .await
            .context("reading DefraDB ACP decision response")?;
        if !status.is_success() {
            anyhow::bail!(
                "DefraDB ACP decision endpoint returned {status} for {resource_name}/{doc_id}: {text}"
            );
        }
        let decision =
            serde_json::from_str::<ProjectionAcpDecisionResponse>(&text).with_context(|| {
                format!("parsing DefraDB ACP decision response for {resource_name}/{doc_id}")
            })?;
        self.cache.insert(key, decision.allowed);
        Ok(decision.allowed)
    }
}

fn required_doc_id<'a>(
    resource_name: &str,
    label: &str,
    doc_id: &'a Option<String>,
) -> Result<&'a str> {
    doc_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "DefraDB ACP projection decisions require _docID for {resource_name} {label}"
            )
        })
}

#[derive(Debug, Default)]
struct ProjectionDocumentScope {
    agent_did: Option<String>,
    behavior_id: Option<String>,
    session_id: Option<String>,
}

impl ProjectionDocumentScope {
    fn has_filters(&self) -> bool {
        self.agent_did.is_some() || self.behavior_id.is_some() || self.session_id.is_some()
    }

    fn description(&self) -> String {
        let mut parts = Vec::new();
        if let Some(agent_did) = self.agent_did.as_deref() {
            parts.push(format!("agent_did={agent_did}"));
        }
        if let Some(behavior_id) = self.behavior_id.as_deref() {
            parts.push(format!("behavior_id={behavior_id}"));
        }
        if let Some(session_id) = self.session_id.as_deref() {
            parts.push(format!("session_id={session_id}"));
        }
        parts.join(", ")
    }
}

fn apply_projection_document_scope(
    mut timeline: RunTimeline,
    scope: &ProjectionDocumentScope,
) -> Result<RunTimeline> {
    if !scope.has_filters() {
        return Ok(timeline);
    }

    if !timeline_root_matches_scope(&timeline, scope) {
        anyhow::bail!(
            "projection scope denied request {} for {}",
            timeline.request_id,
            scope.description()
        );
    }

    let allowed_request_ids = scoped_request_ids(&timeline, scope);
    timeline.events.retain(|event| {
        should_keep_scoped_timeline_event(event, &timeline.request_id, &allowed_request_ids, scope)
    });
    Ok(timeline)
}

fn timeline_root_matches_scope(timeline: &RunTimeline, scope: &ProjectionDocumentScope) -> bool {
    scope_value_matches(
        scope.agent_did.as_deref(),
        [
            timeline.request.agent_did.as_deref(),
            timeline.agent_did.as_deref(),
        ],
    ) && scope_value_matches(
        scope.behavior_id.as_deref(),
        [
            timeline.request.behavior_id.as_deref(),
            timeline.behavior_id.as_deref(),
            timeline
                .session
                .as_ref()
                .and_then(|session| session.behavior_id.as_deref()),
        ],
    ) && scope_value_matches(
        scope.session_id.as_deref(),
        [
            timeline.request.session_id.as_deref(),
            timeline.session_id.as_deref(),
        ],
    )
}

fn scoped_request_ids(timeline: &RunTimeline, scope: &ProjectionDocumentScope) -> BTreeSet<String> {
    let mut allowed = BTreeSet::from([timeline.request_id.clone()]);
    for event in &timeline.events {
        if let RunTimelineEvent::Request(request) = event {
            if request_event_matches_scope(request, scope) {
                allowed.insert(request.request_id.clone());
            }
        }
    }
    allowed
}

fn request_event_matches_scope(
    request: &TimelineRequestEvent,
    scope: &ProjectionDocumentScope,
) -> bool {
    scope_value_matches(scope.agent_did.as_deref(), [request.agent_did.as_deref()])
        && scope_value_matches(
            scope.behavior_id.as_deref(),
            [request.behavior_id.as_deref()],
        )
        && scope_value_matches(scope.session_id.as_deref(), [request.session_id.as_deref()])
}

fn should_keep_scoped_timeline_event(
    event: &RunTimelineEvent,
    root_request_id: &str,
    allowed_request_ids: &BTreeSet<String>,
    scope: &ProjectionDocumentScope,
) -> bool {
    match event {
        RunTimelineEvent::Request(request) => {
            request.request_id == root_request_id
                || allowed_request_ids.contains(&request.request_id)
                || request
                    .parent_request_id
                    .as_deref()
                    .is_some_and(|parent_request_id| {
                        allowed_request_ids.contains(parent_request_id)
                    })
        }
        RunTimelineEvent::InferenceCall(call) => allowed_request_ids.contains(&call.request_id),
        RunTimelineEvent::RenderedRequest(rendered) => rendered
            .request_id
            .as_deref()
            .is_some_and(|request_id| allowed_request_ids.contains(request_id)),
        RunTimelineEvent::Message(message) => scoped_request_id_allowed(
            message.request_id.as_deref(),
            Some(message.session_id.as_str()),
            allowed_request_ids,
            scope,
        ),
        RunTimelineEvent::ToolCall(tool_call) => {
            scoped_tool_call_allowed(tool_call, allowed_request_ids, scope)
        }
        RunTimelineEvent::Response(response) => allowed_request_ids.contains(&response.request_id),
    }
}

fn scoped_tool_call_allowed(
    tool_call: &TimelineToolCallEvent,
    allowed_request_ids: &BTreeSet<String>,
    scope: &ProjectionDocumentScope,
) -> bool {
    scoped_request_id_allowed(
        tool_call.request_id.as_deref(),
        Some(tool_call.session_id.as_str()),
        allowed_request_ids,
        scope,
    )
}

fn scoped_request_id_allowed(
    request_id: Option<&str>,
    session_id: Option<&str>,
    allowed_request_ids: &BTreeSet<String>,
    scope: &ProjectionDocumentScope,
) -> bool {
    request_id
        .map(|request_id| allowed_request_ids.contains(request_id))
        .unwrap_or_else(|| {
            scope.agent_did.is_none()
                && scope.behavior_id.is_none()
                && scope_value_matches(scope.session_id.as_deref(), [session_id])
        })
}

fn scope_value_matches<'a>(
    expected: Option<&str>,
    actual_values: impl IntoIterator<Item = Option<&'a str>>,
) -> bool {
    let Some(expected) = expected.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    actual_values
        .into_iter()
        .flatten()
        .any(|actual| actual.trim() == expected)
}

fn optional_scope_arg(field: &str, value: Option<String>) -> Result<Option<String>> {
    value
        .map(|value| crate::require_non_empty(field, &value).map(ToOwned::to_owned))
        .transpose()
}

fn adapter_projection_kind(arg: TraceProjectionArg) -> AdapterProjectionKind {
    match arg {
        TraceProjectionArg::Atif => AdapterProjectionKind::AtifTrajectory,
        TraceProjectionArg::OpenaiCodex => AdapterProjectionKind::OpenAiCodexRunTrace,
        TraceProjectionArg::Langgraph => AdapterProjectionKind::LangGraphStateHistory,
        TraceProjectionArg::MultiAgent => AdapterProjectionKind::MultiAgentTask,
    }
}

fn projection_redaction_mode(arg: TraceProjectionRedactionArg) -> ProjectionRedactionMode {
    match arg {
        TraceProjectionRedactionArg::Full => ProjectionRedactionMode::Full,
        TraceProjectionRedactionArg::TrainingSafe => ProjectionRedactionMode::TrainingSafe,
        TraceProjectionRedactionArg::Public => ProjectionRedactionMode::Public,
    }
}

async fn trace_export(args: TraceExportArgs) -> Result<()> {
    let (access, _home_dir) =
        crate::resolve_config_access(args.home.as_deref(), args.graphql.as_deref()).await?;
    let requested_request = match args.request_id.as_deref() {
        Some(request_id) => Some(load_request_by_id(&access, request_id).await?),
        None => None,
    };
    if let (Some(session_id), Some(request)) = (args.session_id.as_deref(), &requested_request) {
        if request.session_id.as_deref() != Some(session_id) {
            anyhow::bail!(
                "--session-id {session_id} does not match request {} session_id={}",
                request.request_id,
                request.session_id.as_deref().unwrap_or("")
            );
        }
    }

    let session_filter = args.session_id.as_deref().or_else(|| {
        requested_request
            .as_ref()
            .and_then(|request| request.session_id.as_deref())
    });
    let tool_calls = load_tool_calls(&access, args.limit, session_filter).await?;
    if tool_calls.is_empty() {
        write_jsonl::<AmyToolCallTraceRecord>(args.output_file.as_deref(), &[])?;
        return Ok(());
    }

    let session_ids = unique_tool_call_session_ids(&tool_calls);
    let messages = load_messages_for_tool_calls(&access, &tool_calls).await?;
    let mut requests = load_requests_for_sessions(&access, &session_ids).await?;
    if let Some(request) = requested_request {
        if !requests
            .iter()
            .any(|row| row.request_id == request.request_id)
        {
            requests.push(request);
        }
    }
    let responses = load_responses_for_sessions(&access, &session_ids).await?;
    let sessions = load_sessions(&access, &session_ids).await?;
    let conversations = load_conversations(&access, &session_ids).await?;
    let behaviors = load_behaviors(&access, &requests, &sessions, &conversations).await?;

    let records = build_records(
        &tool_calls,
        &messages,
        &requests,
        &responses,
        &sessions,
        &conversations,
        &behaviors,
        &args,
    );
    let records = match args.request_id.as_deref() {
        Some(request_id) => records
            .into_iter()
            .filter(|record| record.request_id.as_deref() == Some(request_id))
            .collect::<Vec<_>>(),
        None => records,
    };

    write_jsonl(args.output_file.as_deref(), &records)
}

fn build_records(
    tool_calls: &[ToolCallRow],
    messages: &HashMap<(String, i64), MessageRow>,
    requests: &[RequestRow],
    responses: &[ResponseRow],
    sessions: &HashMap<String, SessionRow>,
    conversations: &HashMap<String, ConversationRow>,
    behaviors: &HashMap<String, BehaviorRow>,
    args: &TraceExportArgs,
) -> Vec<AmyToolCallTraceRecord> {
    let requests_by_session = rows_by_session(requests);
    let responses_by_session = rows_by_session(responses);
    let responses_by_request = responses
        .iter()
        .map(|row| (row.request_id.clone(), row))
        .collect::<HashMap<_, _>>();

    tool_calls
        .iter()
        .map(|tool_call| {
            let session_requests = requests_by_session
                .get(tool_call.session_id.as_str())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let session_responses = responses_by_session
                .get(tool_call.session_id.as_str())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let request =
                infer_request_for_tool_call(tool_call, session_requests, session_responses);
            let response = request.and_then(|request| {
                responses_by_request
                    .get(request.request_id.as_str())
                    .copied()
            });
            let request_failure = combined_request_failure_text(request, response);
            let request_failure_class = analyze_request_failure(request_failure.as_deref());
            let analysis = analyze_tool_call(
                &tool_call.tool_name,
                &tool_call.args,
                &tool_call.result,
                &tool_call.status,
            );
            let message = tool_call
                .message_sequence
                .and_then(|sequence| messages.get(&(tool_call.session_id.clone(), sequence)));
            let raw_assistant_message = message.map(|message| raw_message_json(&message.content));
            let raw_tool_call_json = message.and_then(|message| {
                extract_raw_tool_call_json(
                    &message.role,
                    &message.content,
                    &tool_call.tool_call_id,
                    &tool_call.tool_name,
                )
            });
            let session = sessions.get(tool_call.session_id.as_str());
            let conversation = conversations.get(tool_call.session_id.as_str());
            let behavior_id = first_nonempty([
                request.and_then(|request| request.behavior_id.as_deref()),
                conversation.and_then(|conversation| conversation.behavior_id.as_deref()),
                session.and_then(|session| session.behavior_id.as_deref()),
            ]);
            let behavior = behavior_id.and_then(|behavior_id| behaviors.get(behavior_id));
            let metadata = request.and_then(parse_request_metadata);
            let run_id = args
                .run_id
                .clone()
                .or_else(|| metadata_string(metadata.as_ref(), "run_id"))
                .or_else(|| metadata_string(metadata.as_ref(), "runId"));
            let case_id = args
                .case_id
                .clone()
                .or_else(|| metadata_string(metadata.as_ref(), "case_id"))
                .or_else(|| metadata_string(metadata.as_ref(), "caseId"));
            let backend_id = first_nonempty([
                request.and_then(|request| request.backend_id.as_deref()),
                behavior.and_then(|behavior| behavior.backend_id.as_deref()),
            ]);
            let agent_did = first_nonempty([
                request.and_then(|request| request.agent_did.as_deref()),
                conversation.and_then(|conversation| conversation.agent_did.as_deref()),
                behavior.and_then(|behavior| behavior.agent_did.as_deref()),
            ]);

            AmyToolCallTraceRecord {
                run_id,
                case_id,
                prompt: request.and_then(|request| request.content.clone()),
                agent_did: agent_did.map(ToOwned::to_owned),
                behavior_id: behavior_id.map(ToOwned::to_owned),
                session_id: tool_call.session_id.clone(),
                request_id: request.map(|request| request.request_id.clone()),
                request_status: request.and_then(|request| request.status.clone()),
                request_lifecycle_state: request
                    .and_then(|request| request.lifecycle_state.clone()),
                request_failure_reason: request.and_then(|request| request.failure_reason.clone()),
                response_status: response.and_then(|response| response.status.clone()),
                response_error_message: response
                    .and_then(|response| response.error_message.clone()),
                request_failure_class,
                backend_id: backend_id.map(ToOwned::to_owned),
                model_name: behavior.and_then(|behavior| behavior.model_name.clone()),
                inference_profile_id: behavior
                    .and_then(|behavior| behavior.inference_profile_id.clone()),
                raw_assistant_message,
                raw_tool_call_json,
                tool_call_id: tool_call.tool_call_id.clone(),
                native_or_meta_tool: tool_call.tool_name.clone(),
                selected_service_id: analysis.selected_service_id,
                selected_tool_name: analysis.selected_tool_name,
                raw_arguments: tool_call.args.clone(),
                argument_parse_result: analysis.argument_parse_result,
                schema_validation_result: analysis.schema_validation_result,
                validation_errors: analysis.validation_errors,
                repair_attempt: None,
                final_arguments_sent: analysis.final_arguments_sent,
                tool_result: tool_call.result.clone(),
                native_tool_output: analysis.native_tool_output,
                tool_result_ok: analysis.tool_result_ok,
                tool_call_completed: tool_call.status.eq_ignore_ascii_case("completed"),
                tool_status: tool_call.status.clone(),
                task_outcome: None,
                tool_failure_class: analysis.tool_failure_class,
                tool_error: analysis.tool_error,
                failure_class: analysis.tool_failure_class,
                started_at: tool_call.started_at.clone(),
                completed_at: tool_call.completed_at.clone(),
                latency_ms: latency_ms(
                    tool_call.started_at.as_deref(),
                    tool_call.completed_at.as_deref(),
                ),
                retry_count: request.and_then(|request| request.retry_count),
            }
        })
        .collect()
}

async fn load_tool_calls(
    access: &ConfigAccess,
    limit: usize,
    session_id: Option<&str>,
) -> Result<Vec<ToolCallRow>> {
    let filter = session_id
        .map(|session_id| {
            format!(
                r#"filter: {{ session_id: {{ _eq: "{}" }} }}, "#,
                escape_graphql_string(session_id)
            )
        })
        .unwrap_or_default();
    let query = format!(
        r#"{{
            AgentToolCall(
                {filter}
                order: {{ started_at: DESC }},
                limit: {limit}
            ) {{
                session_id
                message_sequence
                tool_name
                tool_call_id
                args
                result
                status
                started_at
                completed_at
            }}
        }}"#
    );
    load_rows(access, "AgentToolCall", &query).await
}

async fn load_request_by_id(access: &ConfigAccess, request_id: &str) -> Result<RequestRow> {
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{}" }} }},
                order: {{ created_at: DESC }},
                limit: 1
            ) {{
                request_id
                agent_did
                behavior_id
                session_id
                content
                metadata
                status
                lifecycle_state
                backend_id
                failure_reason
                created_at
                retry_count
            }}
        }}"#,
        escape_graphql_string(request_id)
    );
    load_rows::<RequestRow>(access, "AgentRequest", &query)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("request {request_id} not found"))
}

async fn load_requests_for_sessions(
    access: &ConfigAccess,
    session_ids: &[String],
) -> Result<Vec<RequestRow>> {
    if session_ids.is_empty() {
        return Ok(Vec::new());
    }
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ session_id: {{ _in: {} }} }},
                order: {{ created_at: ASC }}
            ) {{
                request_id
                agent_did
                behavior_id
                session_id
                content
                metadata
                status
                lifecycle_state
                backend_id
                failure_reason
                created_at
                retry_count
            }}
        }}"#,
        graphql_string_list_literal(session_ids)
    );
    load_rows(access, "AgentRequest", &query).await
}

async fn load_responses_for_sessions(
    access: &ConfigAccess,
    session_ids: &[String],
) -> Result<Vec<ResponseRow>> {
    if session_ids.is_empty() {
        return Ok(Vec::new());
    }
    let query = format!(
        r#"{{
            AgentResponse(
                filter: {{ session_id: {{ _in: {} }} }},
                order: {{ created_at: ASC }}
            ) {{
                request_id
                session_id
                status
                error_message
                materialized_message_sequence
            }}
        }}"#,
        graphql_string_list_literal(session_ids)
    );
    load_rows(access, "AgentResponse", &query).await
}

async fn load_sessions(
    access: &ConfigAccess,
    session_ids: &[String],
) -> Result<HashMap<String, SessionRow>> {
    if session_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let query = format!(
        r#"{{
            AgentSession(
                filter: {{ session_id: {{ _in: {} }} }}
            ) {{
                session_id
                behavior_id
            }}
        }}"#,
        graphql_string_list_literal(session_ids)
    );
    Ok(load_rows::<SessionRow>(access, "AgentSession", &query)
        .await?
        .into_iter()
        .map(|row| (row.session_id.clone(), row))
        .collect())
}

async fn load_conversations(
    access: &ConfigAccess,
    session_ids: &[String],
) -> Result<HashMap<String, ConversationRow>> {
    if session_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let query = format!(
        r#"{{
            AgentConversation(
                filter: {{ session_id: {{ _in: {} }} }}
            ) {{
                session_id
                agent_did
                behavior_id
            }}
        }}"#,
        graphql_string_list_literal(session_ids)
    );
    Ok(
        load_rows::<ConversationRow>(access, "AgentConversation", &query)
            .await?
            .into_iter()
            .map(|row| (row.session_id.clone(), row))
            .collect(),
    )
}

async fn load_messages_for_tool_calls(
    access: &ConfigAccess,
    tool_calls: &[ToolCallRow],
) -> Result<HashMap<(String, i64), MessageRow>> {
    let mut sequences_by_session: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
    for tool_call in tool_calls {
        if let Some(sequence) = tool_call.message_sequence {
            sequences_by_session
                .entry(tool_call.session_id.clone())
                .or_default()
                .insert(sequence);
        }
    }

    let mut out = HashMap::new();
    for (session_id, sequences) in sequences_by_session {
        let sequence_values = sequences.into_iter().collect::<Vec<_>>();
        let query = format!(
            r#"{{
                AgentMessage(
                    filter: {{
                        _and: [
                            {{ session_id: {{ _eq: "{}" }} }},
                            {{ sequence: {{ _in: {} }} }}
                        ]
                    }}
                ) {{
                    session_id
                    sequence
                    role
                    content
                }}
            }}"#,
            escape_graphql_string(&session_id),
            graphql_int_list_literal(&sequence_values)
        );
        for row in load_rows::<MessageRow>(access, "AgentMessage", &query).await? {
            out.insert((row.session_id.clone(), row.sequence), row);
        }
    }
    Ok(out)
}

async fn load_behaviors(
    access: &ConfigAccess,
    requests: &[RequestRow],
    sessions: &HashMap<String, SessionRow>,
    conversations: &HashMap<String, ConversationRow>,
) -> Result<HashMap<String, BehaviorRow>> {
    let mut behavior_ids = BTreeSet::new();
    for request in requests {
        if let Some(behavior_id) = request
            .behavior_id
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            behavior_ids.insert(behavior_id.to_string());
        }
    }
    for session in sessions.values() {
        if let Some(behavior_id) = session
            .behavior_id
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            behavior_ids.insert(behavior_id.to_string());
        }
    }
    for conversation in conversations.values() {
        if let Some(behavior_id) = conversation
            .behavior_id
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            behavior_ids.insert(behavior_id.to_string());
        }
    }
    if behavior_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let behavior_ids = behavior_ids.into_iter().collect::<Vec<_>>();
    let query = format!(
        r#"{{
            AgentBehavior(
                filter: {{ behavior_id: {{ _in: {} }} }}
            ) {{
                behavior_id
                agent_did
                backend_id
                model_name
                inference_profile_id
            }}
        }}"#,
        graphql_string_list_literal(&behavior_ids)
    );
    Ok(load_rows::<BehaviorRow>(access, "AgentBehavior", &query)
        .await?
        .into_iter()
        .map(|row| (row.behavior_id.clone(), row))
        .collect())
}

async fn load_rows<T>(access: &ConfigAccess, collection: &str, query: &str) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    graphql_rows_or_empty_if_collection_missing(access, collection, query)
        .await?
        .into_iter()
        .map(serde_json::from_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("decoding {collection} rows"))
}

fn infer_request_for_tool_call<'a>(
    tool_call: &ToolCallRow,
    requests: &[&'a RequestRow],
    responses: &[&ResponseRow],
) -> Option<&'a RequestRow> {
    if let Some(sequence) = tool_call.message_sequence {
        if let Some(response) = responses
            .iter()
            .filter_map(|response| {
                let materialized = response.materialized_message_sequence?;
                (materialized >= sequence).then_some((*response, materialized))
            })
            .min_by_key(|(_, materialized)| *materialized)
            .map(|(response, _)| response)
        {
            if let Some(request) = requests
                .iter()
                .copied()
                .find(|request| request.request_id == response.request_id)
            {
                return Some(request);
            }
        }
    }

    if let Some(started_at) = tool_call
        .started_at
        .as_deref()
        .and_then(parse_rfc3339_millis)
    {
        if let Some(request) = requests
            .iter()
            .copied()
            .filter_map(|request| {
                let created_at = request
                    .created_at
                    .as_deref()
                    .and_then(parse_rfc3339_millis)?;
                (created_at <= started_at).then_some((request, created_at))
            })
            .max_by_key(|(_, created_at)| *created_at)
            .map(|(request, _)| request)
        {
            return Some(request);
        }
    }

    if requests.len() == 1 {
        return requests.first().copied();
    }

    None
}

fn rows_by_session<T: HasSessionId>(rows: &[T]) -> HashMap<&str, Vec<&T>> {
    let mut out: HashMap<&str, Vec<&T>> = HashMap::new();
    for row in rows {
        if let Some(session_id) = row.session_id() {
            out.entry(session_id).or_default().push(row);
        }
    }
    out
}

fn combined_request_failure_text(
    request: Option<&RequestRow>,
    response: Option<&ResponseRow>,
) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(request) = request {
        push_nonempty(&mut parts, request.status.as_deref());
        push_nonempty(&mut parts, request.lifecycle_state.as_deref());
        push_nonempty(&mut parts, request.failure_reason.as_deref());
    }
    if let Some(response) = response {
        push_nonempty(&mut parts, response.status.as_deref());
        push_nonempty(&mut parts, response.error_message.as_deref());
    }

    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn parse_request_metadata(request: &RequestRow) -> Option<Value> {
    let metadata = request.metadata.as_deref()?.trim();
    if metadata.is_empty() {
        return None;
    }
    serde_json::from_str(metadata).ok()
}

fn metadata_string(metadata: Option<&Value>, key: &str) -> Option<String> {
    metadata?
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn first_nonempty<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> Option<&'a str> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

fn push_nonempty(parts: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        parts.push(value.to_string());
    }
}

fn parse_rfc3339_millis(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn unique_tool_call_session_ids(tool_calls: &[ToolCallRow]) -> Vec<String> {
    tool_calls
        .iter()
        .filter_map(|row| {
            let session_id = row.session_id.trim();
            (!session_id.is_empty()).then_some(session_id.to_string())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn graphql_int_list_literal(values: &[i64]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn write_jsonl<T: Serialize>(path: Option<&std::path::Path>, records: &[T]) -> Result<()> {
    let mut output = String::new();
    for record in records {
        output.push_str(&serde_json::to_string(record)?);
        output.push('\n');
    }

    if let Some(path) = path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating output directory {}", parent.display()))?;
        }
        fs::write(path, output).with_context(|| format!("writing JSONL {}", path.display()))?;
    } else {
        print!("{output}");
    }
    Ok(())
}

trait HasSessionId {
    fn session_id(&self) -> Option<&str>;
}

#[derive(Debug, Clone, Deserialize)]
struct ToolCallRow {
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    message_sequence: Option<i64>,
    #[serde(default)]
    tool_name: String,
    #[serde(default)]
    tool_call_id: String,
    #[serde(default)]
    args: String,
    #[serde(default)]
    result: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    completed_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RequestRow {
    #[serde(default)]
    request_id: String,
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(default)]
    behavior_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    metadata: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    lifecycle_state: Option<String>,
    #[serde(default)]
    backend_id: Option<String>,
    #[serde(default)]
    failure_reason: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    retry_count: Option<i64>,
}

impl HasSessionId for RequestRow {
    fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ResponseRow {
    #[serde(default)]
    request_id: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    error_message: Option<String>,
    #[serde(default)]
    materialized_message_sequence: Option<i64>,
}

impl HasSessionId for ResponseRow {
    fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }
}

#[derive(Debug, Clone, Deserialize)]
struct MessageRow {
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    sequence: i64,
    #[serde(default)]
    role: String,
    #[serde(default)]
    content: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SessionRow {
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    behavior_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConversationRow {
    #[serde(default)]
    session_id: String,
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(default)]
    behavior_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct BehaviorRow {
    #[serde(default)]
    behavior_id: String,
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(default)]
    backend_id: Option<String>,
    #[serde(default)]
    model_name: Option<String>,
    #[serde(default)]
    inference_profile_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};

    use super::*;

    #[derive(Debug, Deserialize)]
    struct MockAcpDecisionRequest {
        actor: String,
        permission: String,
        #[serde(rename = "policyID")]
        policy_id: String,
        #[serde(rename = "resourceName")]
        resource_name: String,
        #[serde(rename = "docID")]
        doc_id: String,
    }

    async fn mock_acp_decide(
        State(allowed): State<Arc<BTreeMap<(String, String), bool>>>,
        Json(body): Json<MockAcpDecisionRequest>,
    ) -> (StatusCode, Json<Value>) {
        if body.actor != "did:test:projection-reader"
            || body.permission != "read"
            || body.policy_id != "projection-policy"
        {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "unexpected ACP decision request" })),
            );
        }
        let allowed = allowed
            .get(&(body.resource_name, body.doc_id))
            .copied()
            .unwrap_or(false);
        (StatusCode::OK, Json(json!({ "allowed": allowed })))
    }

    async fn spawn_mock_acp(
        allowed: BTreeMap<(String, String), bool>,
    ) -> Result<ProjectionAcpReadScope> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let router = Router::new()
            .route("/api/v0/acp/document/decide", post(mock_acp_decide))
            .with_state(Arc::new(allowed));
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok(ProjectionAcpReadScope {
            actor_did: "did:test:projection-reader".to_string(),
            policy_id: "projection-policy".to_string(),
            api_base: format!("http://{addr}/api/v0"),
            resource_names: BTreeMap::new(),
        })
    }

    #[test]
    fn request_metadata_hydrates_run_and_case_ids() {
        let request = RequestRow {
            request_id: "req-1".to_string(),
            metadata: Some(r#"{"run_id":"run-1","case_id":"case-1"}"#.to_string()),
            ..empty_request()
        };
        let metadata = parse_request_metadata(&request);

        assert_eq!(
            metadata_string(metadata.as_ref(), "run_id").as_deref(),
            Some("run-1")
        );
        assert_eq!(
            metadata_string(metadata.as_ref(), "case_id").as_deref(),
            Some("case-1")
        );
    }

    #[test]
    fn infers_request_by_materialized_message_sequence() {
        let requests = vec![
            RequestRow {
                request_id: "req-1".to_string(),
                session_id: Some("session-1".to_string()),
                ..empty_request()
            },
            RequestRow {
                request_id: "req-2".to_string(),
                session_id: Some("session-1".to_string()),
                ..empty_request()
            },
        ];
        let request_refs = requests.iter().collect::<Vec<_>>();
        let responses = [
            ResponseRow {
                request_id: "req-1".to_string(),
                session_id: Some("session-1".to_string()),
                materialized_message_sequence: Some(4),
                ..empty_response()
            },
            ResponseRow {
                request_id: "req-2".to_string(),
                session_id: Some("session-1".to_string()),
                materialized_message_sequence: Some(8),
                ..empty_response()
            },
        ];
        let response_refs = responses.iter().collect::<Vec<_>>();
        let tool_call = ToolCallRow {
            session_id: "session-1".to_string(),
            message_sequence: Some(3),
            ..empty_tool_call()
        };

        let request = infer_request_for_tool_call(&tool_call, &request_refs, &response_refs)
            .expect("request");

        assert_eq!(request.request_id, "req-1");
    }

    #[test]
    fn projection_acp_binding_selects_most_specific_matching_row() -> Result<()> {
        let request = TimelineRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            behavior_id: Some("amy:default".to_string()),
            ..TimelineRequestRow::default()
        };
        let selected = select_projection_acp_binding(
            vec![
                projection_binding("global", None, None, None),
                projection_binding("agent", Some("did:test:amy"), None, None),
                projection_binding(
                    "exact",
                    Some("did:test:amy"),
                    Some("amy:default"),
                    Some("openai_codex_run_trace"),
                ),
                projection_binding(
                    "other-projection",
                    Some("did:test:amy"),
                    Some("amy:default"),
                    Some("langgraph_state_history"),
                ),
            ],
            AdapterProjectionKind::OpenAiCodexRunTrace,
            &request,
        )?
        .expect("binding");

        assert_eq!(selected.binding_id, "exact");
        Ok(())
    }

    #[test]
    fn projection_acp_binding_rejects_ambiguous_rows() {
        let request = TimelineRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            ..TimelineRequestRow::default()
        };
        let error = select_projection_acp_binding(
            vec![
                projection_binding("first", Some("did:test:amy"), None, None),
                projection_binding("second", Some("did:test:amy"), None, None),
            ],
            AdapterProjectionKind::OpenAiCodexRunTrace,
            &request,
        )
        .expect_err("ambiguous rows should fail");

        assert!(
            error
                .to_string()
                .contains("ambiguous ProjectionAcpBinding rows"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn projection_acp_binding_rejects_incomparable_matching_scopes() {
        let request = TimelineRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            behavior_id: Some("amy:default".to_string()),
            ..TimelineRequestRow::default()
        };
        let error = select_projection_acp_binding(
            vec![
                projection_binding("behavior", Some("did:test:amy"), Some("amy:default"), None),
                projection_binding(
                    "projection",
                    Some("did:test:amy"),
                    None,
                    Some("openai_codex_run_trace"),
                ),
            ],
            AdapterProjectionKind::OpenAiCodexRunTrace,
            &request,
        )
        .expect_err("overlapping incomparable scopes should fail closed");

        assert!(
            error
                .to_string()
                .contains("ambiguous ProjectionAcpBinding rows"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn projection_acp_binding_ignores_unscoped_rows() -> Result<()> {
        let request = TimelineRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            ..TimelineRequestRow::default()
        };
        let selected = select_projection_acp_binding(
            vec![
                projection_binding("global", None, None, Some("openai_codex_run_trace")),
                projection_binding("agent", Some("did:test:amy"), None, None),
            ],
            AdapterProjectionKind::OpenAiCodexRunTrace,
            &request,
        )?
        .expect("agent-scoped binding");

        assert_eq!(selected.binding_id, "agent");
        Ok(())
    }

    #[test]
    fn projection_acp_binding_rejects_enabled_non_operational_status() {
        let request = TimelineRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            ..TimelineRequestRow::default()
        };
        let mut binding = projection_binding("draft", Some("did:test:amy"), None, None);
        binding.publication_status = Some("draft".to_string());

        let error = select_projection_acp_binding(
            vec![binding],
            AdapterProjectionKind::OpenAiCodexRunTrace,
            &request,
        )
        .expect_err("enabled draft binding should fail closed");

        assert!(
            error
                .to_string()
                .contains("non-operational publication_status draft"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn projection_resource_map_parses_nonempty_collection_resource_pairs() -> Result<()> {
        let map = parse_projection_resource_map(Some(
            r#"{"AgentRequest":"runtime_request"," AgentToolCall ":" runtime_tool_call "}"#,
        ))?;

        assert_eq!(
            map.get("AgentRequest").map(String::as_str),
            Some("runtime_request")
        );
        assert_eq!(
            map.get("AgentToolCall").map(String::as_str),
            Some("runtime_tool_call")
        );
        Ok(())
    }

    #[test]
    fn projection_resource_map_rejects_empty_resource_names() {
        let error = parse_projection_resource_map(Some(r#"{"AgentRequest":""}"#))
            .expect_err("empty resource names should fail");

        assert!(
            error
                .to_string()
                .contains("must map non-empty collection names"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn projection_resource_map_rejects_unknown_collection_names() {
        let error = parse_projection_resource_map(Some(r#"{"AgentMesage":"messages"}"#))
            .expect_err("unknown collection names should fail");

        assert!(
            error
                .to_string()
                .contains("unknown runtime collection AgentMesage"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn projection_acp_filter_omits_rows_denied_by_defradb_acp() -> Result<()> {
        let mut allowed = BTreeMap::new();
        for (resource_name, doc_id) in [
            ("AgentRequest", "doc-request-root"),
            ("AgentMessage", "doc-message-allowed"),
            ("AgentToolCall", "doc-tool-allowed"),
            ("AgentResponse", "doc-response-allowed"),
            ("InferenceCall", "doc-inference-allowed"),
            ("AgentConversation", "doc-conversation"),
        ] {
            allowed.insert((resource_name.to_string(), doc_id.to_string()), true);
        }
        let scope = spawn_mock_acp(allowed).await?;

        let filtered = apply_projection_acp_read_filter(acp_fixture_rows(), &scope).await?;

        assert_eq!(
            filtered
                .requests
                .iter()
                .map(|request| request.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["req-root"]
        );
        assert_eq!(
            filtered
                .messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            filtered
                .tool_calls
                .iter()
                .map(|tool_call| tool_call.tool_call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["call-allowed"]
        );
        assert_eq!(
            filtered
                .responses
                .iter()
                .map(|response| response.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["req-root"]
        );
        assert_eq!(
            filtered
                .inference_calls
                .iter()
                .map(|call| call.call_id.as_str())
                .collect::<Vec<_>>(),
            vec!["inference-allowed"]
        );
        assert!(
            filtered.session.is_none(),
            "session row should be omitted when ACP denies it"
        );
        assert!(
            filtered.conversation.is_some(),
            "conversation row should remain when ACP allows it"
        );
        Ok(())
    }

    #[tokio::test]
    async fn projection_acp_filter_uses_configured_resource_names() -> Result<()> {
        let mut allowed = BTreeMap::new();
        for (resource_name, doc_id) in [
            ("runtime_request", "doc-request-root"),
            ("runtime_message", "doc-message-allowed"),
            ("runtime_tool_call", "doc-tool-allowed"),
            ("runtime_response", "doc-response-allowed"),
            ("runtime_inference_call", "doc-inference-allowed"),
            ("runtime_conversation", "doc-conversation"),
        ] {
            allowed.insert((resource_name.to_string(), doc_id.to_string()), true);
        }
        let mut scope = spawn_mock_acp(allowed).await?;
        scope.resource_names = BTreeMap::from([
            ("AgentRequest".to_string(), "runtime_request".to_string()),
            ("AgentMessage".to_string(), "runtime_message".to_string()),
            ("AgentToolCall".to_string(), "runtime_tool_call".to_string()),
            ("AgentResponse".to_string(), "runtime_response".to_string()),
            (
                "InferenceCall".to_string(),
                "runtime_inference_call".to_string(),
            ),
            (
                "AgentConversation".to_string(),
                "runtime_conversation".to_string(),
            ),
        ]);

        let filtered = apply_projection_acp_read_filter(acp_fixture_rows(), &scope).await?;

        assert_eq!(filtered.requests.len(), 1);
        assert_eq!(filtered.messages.len(), 1);
        assert_eq!(filtered.tool_calls.len(), 1);
        assert_eq!(filtered.inference_calls.len(), 1);
        assert_eq!(filtered.responses.len(), 1);
        assert!(filtered.conversation.is_some());
        assert!(filtered.session.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn projection_acp_filter_denies_root_request_fail_closed() -> Result<()> {
        let scope = spawn_mock_acp(BTreeMap::new()).await?;
        let error = apply_projection_acp_read_filter(acp_fixture_rows(), &scope)
            .await
            .expect_err("root request should be denied");

        assert!(
            error
                .to_string()
                .contains("DefraDB ACP denied read access to root request req-root"),
            "unexpected error: {error:#}"
        );
        Ok(())
    }

    fn acp_fixture_rows() -> RunTimelineRows {
        RunTimelineRows {
            request: TimelineRequestRow {
                doc_id: Some("doc-request-root".to_string()),
                request_id: "req-root".to_string(),
                session_id: Some("session-acp".to_string()),
                ..TimelineRequestRow::default()
            },
            session: Some(TimelineSessionRow {
                doc_id: Some("doc-session".to_string()),
                session_id: "session-acp".to_string(),
                ..TimelineSessionRow::default()
            }),
            conversation: Some(TimelineConversationRow {
                doc_id: Some("doc-conversation".to_string()),
                session_id: "session-acp".to_string(),
                ..TimelineConversationRow::default()
            }),
            requests: vec![
                TimelineRequestRow {
                    doc_id: Some("doc-request-root".to_string()),
                    request_id: "req-root".to_string(),
                    session_id: Some("session-acp".to_string()),
                    ..TimelineRequestRow::default()
                },
                TimelineRequestRow {
                    doc_id: Some("doc-request-child".to_string()),
                    request_id: "req-child".to_string(),
                    session_id: Some("session-acp".to_string()),
                    caused_by_parent_request_id: Some("req-root".to_string()),
                    ..TimelineRequestRow::default()
                },
            ],
            messages: vec![
                TimelineMessageRow {
                    doc_id: Some("doc-message-allowed".to_string()),
                    session_id: "session-acp".to_string(),
                    request_id: Some("req-root".to_string()),
                    sequence: 1,
                    role: "user".to_string(),
                    content: "allowed".to_string(),
                    timestamp: None,
                },
                TimelineMessageRow {
                    doc_id: Some("doc-message-denied".to_string()),
                    session_id: "session-acp".to_string(),
                    request_id: Some("req-child".to_string()),
                    sequence: 2,
                    role: "assistant".to_string(),
                    content: "denied".to_string(),
                    timestamp: None,
                },
            ],
            tool_calls: vec![
                TimelineToolCallRow {
                    doc_id: Some("doc-tool-allowed".to_string()),
                    request_id: Some("req-root".to_string()),
                    session_id: "session-acp".to_string(),
                    tool_call_id: "call-allowed".to_string(),
                    tool_name: "handoff".to_string(),
                    status: "completed".to_string(),
                    ..TimelineToolCallRow::default()
                },
                TimelineToolCallRow {
                    doc_id: Some("doc-tool-denied".to_string()),
                    request_id: Some("req-child".to_string()),
                    session_id: "session-acp".to_string(),
                    tool_call_id: "call-denied".to_string(),
                    tool_name: "review".to_string(),
                    status: "completed".to_string(),
                    ..TimelineToolCallRow::default()
                },
            ],
            responses: vec![
                TimelineResponseRow {
                    doc_id: Some("doc-response-allowed".to_string()),
                    request_id: "req-root".to_string(),
                    session_id: Some("session-acp".to_string()),
                    status: Some("completed".to_string()),
                    ..TimelineResponseRow::default()
                },
                TimelineResponseRow {
                    doc_id: Some("doc-response-denied".to_string()),
                    request_id: "req-child".to_string(),
                    session_id: Some("session-acp".to_string()),
                    status: Some("completed".to_string()),
                    ..TimelineResponseRow::default()
                },
            ],
            inference_calls: vec![
                TimelineInferenceCallRow {
                    doc_id: Some("doc-inference-allowed".to_string()),
                    call_id: "inference-allowed".to_string(),
                    request_id: "req-root".to_string(),
                    call_seq: 1,
                    attempt: 1,
                    call_state: "failed".to_string(),
                    failure_reason: Some("sensitive transient".to_string()),
                    call_kind: "inference".to_string(),
                    ..TimelineInferenceCallRow::default()
                },
                TimelineInferenceCallRow {
                    doc_id: Some("doc-inference-denied".to_string()),
                    call_id: "inference-denied".to_string(),
                    request_id: "req-child".to_string(),
                    call_seq: 1,
                    attempt: 1,
                    call_state: "completed".to_string(),
                    call_kind: "inference".to_string(),
                    ..TimelineInferenceCallRow::default()
                },
            ],
            rendered_requests: Vec::new(),
        }
    }

    fn projection_binding(
        binding_id: &str,
        agent_did: Option<&str>,
        behavior_id: Option<&str>,
        projection_id: Option<&str>,
    ) -> ProjectionAcpBindingRow {
        ProjectionAcpBindingRow {
            binding_id: binding_id.to_string(),
            agent_did: agent_did.map(ToOwned::to_owned),
            behavior_id: behavior_id.map(ToOwned::to_owned),
            projection_id: projection_id.map(ToOwned::to_owned),
            policy_id: "projection-policy".to_string(),
            staged_policy_id: None,
            previous_policy_id: None,
            resource_map_json: None,
            publication_status: None,
            enabled: Some(true),
        }
    }

    fn empty_tool_call() -> ToolCallRow {
        ToolCallRow {
            session_id: String::new(),
            message_sequence: None,
            tool_name: String::new(),
            tool_call_id: String::new(),
            args: String::new(),
            result: String::new(),
            status: String::new(),
            started_at: None,
            completed_at: None,
        }
    }

    fn empty_request() -> RequestRow {
        RequestRow {
            request_id: String::new(),
            agent_did: None,
            behavior_id: None,
            session_id: None,
            content: None,
            metadata: None,
            status: None,
            lifecycle_state: None,
            backend_id: None,
            failure_reason: None,
            created_at: None,
            retry_count: None,
        }
    }

    fn empty_response() -> ResponseRow {
        ResponseRow {
            request_id: String::new(),
            session_id: None,
            status: None,
            error_message: None,
            materialized_message_sequence: None,
        }
    }
}
