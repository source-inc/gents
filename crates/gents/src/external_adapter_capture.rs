use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::adapter_projection::{
    validate_adapter_projection_contract, AdapterProjectionEnvelope, AdapterProjectionKind,
};
use crate::run_timeline::{
    RunTimelineRows, TimelineConversationRow, TimelineMessageRow, TimelineRequestRow,
    TimelineResponseRow, TimelineSessionRow, TimelineToolCallRow,
};

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalAdapterCapture {
    pub source: ExternalAdapterSource,
    #[serde(default)]
    pub native: Value,
    #[serde(default)]
    pub mapping: Option<ExternalAdapterMapping>,
    #[serde(default)]
    pub envelope: Option<AdapterProjectionEnvelope>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalAdapterSource {
    pub system: String,
    #[serde(default)]
    pub package: Option<String>,
    #[serde(default)]
    pub package_version: Option<String>,
    #[serde(default)]
    pub generator: Option<String>,
    #[serde(default)]
    pub capture: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalAdapterMapping {
    #[serde(alias = "projection_id")]
    pub projection: AdapterProjectionKind,
    #[serde(default)]
    pub scenario_id: Option<String>,
    pub request_id: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub behavior_id: Option<String>,
    #[serde(default)]
    pub actor_did: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub participants: Vec<ExternalParticipantMapping>,
    #[serde(default)]
    pub delegations: Vec<ExternalDelegationMapping>,
    #[serde(default)]
    pub tool_events: Vec<ExternalToolEventMapping>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExternalParticipantMapping {
    #[serde(default)]
    pub native_name: Option<String>,
    pub role: String,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub behavior_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalDelegationMapping {
    pub parent_request_id: String,
    pub child_request_id: String,
    #[serde(default)]
    pub parent_tool_call_id: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub agent_did: Option<String>,
    #[serde(default)]
    pub behavior_id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalToolEventMapping {
    pub id: String,
    pub request_id: String,
    pub tool_name: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub child_request_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExternalAdapterImport {
    pub projection: AdapterProjectionKind,
    pub rows: RunTimelineRows,
    pub actor_did: Option<String>,
    pub source_system: String,
    pub scenario_id: String,
}

#[derive(Debug, Clone)]
struct ImportedMessage {
    role: String,
    content: String,
    request_id: Option<String>,
}

pub fn import_external_adapter_capture_to_timeline_rows(
    capture: &ExternalAdapterCapture,
) -> Result<ExternalAdapterImport> {
    let mapping = capture.mapping.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "external adapter capture {} is missing mapping metadata",
            capture.source.system
        )
    })?;
    validate_external_adapter_capture_mapping(capture, mapping)?;
    match mapping.projection {
        AdapterProjectionKind::MultiAgentTask => import_multi_agent_capture(capture, mapping),
        AdapterProjectionKind::LangGraphStateHistory => import_langgraph_capture(capture, mapping),
        AdapterProjectionKind::AtifTrajectory | AdapterProjectionKind::OpenAiCodexRunTrace => {
            bail!(
                "external adapter import for projection {} is not implemented",
                mapping.projection.id()
            )
        }
    }
}

fn validate_external_adapter_capture_mapping(
    capture: &ExternalAdapterCapture,
    mapping: &ExternalAdapterMapping,
) -> Result<()> {
    require_nonempty_field("source.system", &capture.source.system)?;
    require_nonempty_field("mapping.request_id", &mapping.request_id)?;
    if let Some(envelope) = capture.envelope.as_ref() {
        validate_adapter_projection_contract(envelope).map_err(|error| {
            anyhow::anyhow!("external adapter capture envelope is invalid: {error}")
        })?;
        if envelope.output.kind() != mapping.projection {
            bail!(
                "external adapter capture envelope projection {} does not match mapping projection {}",
                envelope.output.kind().id(),
                mapping.projection.id()
            );
        }
    }
    match mapping.projection {
        AdapterProjectionKind::MultiAgentTask => {
            validate_supported_source_system(
                capture,
                &["autogen-agentchat", "crewai", "microsoft-agent-framework"],
            )?;
            validate_multi_agent_mapping(mapping)
        }
        AdapterProjectionKind::LangGraphStateHistory => {
            validate_supported_source_system(capture, &["langgraph"])?;
            validate_langgraph_mapping(capture)
        }
        AdapterProjectionKind::AtifTrajectory | AdapterProjectionKind::OpenAiCodexRunTrace => {
            Ok(())
        }
    }
}

fn validate_supported_source_system(
    capture: &ExternalAdapterCapture,
    allowed: &[&str],
) -> Result<()> {
    if allowed.contains(&capture.source.system.as_str()) {
        return Ok(());
    }
    bail!(
        "external adapter capture source.system {:?} is not supported for mapped import; expected one of {}",
        capture.source.system,
        allowed.join(", ")
    );
}

fn validate_multi_agent_mapping(mapping: &ExternalAdapterMapping) -> Result<()> {
    if mapping.participants.is_empty() {
        bail!("multi-agent external adapter mapping must include at least one participant");
    }

    let mut request_ids = BTreeSet::from([mapping.request_id.as_str()]);
    let mut has_root_participant = false;
    for participant in &mapping.participants {
        require_nonempty_field("mapping.participants[].role", &participant.role)?;
        match participant.request_id.as_deref() {
            Some(request_id) => {
                require_nonempty_field("mapping.participants[].request_id", request_id)?;
                if request_id == mapping.request_id {
                    has_root_participant = true;
                }
                request_ids.insert(request_id);
            }
            None => has_root_participant = true,
        }
    }
    if !has_root_participant {
        bail!(
            "multi-agent external adapter mapping must include a root participant for request_id {:?}",
            mapping.request_id
        );
    }

    for delegation in &mapping.delegations {
        require_nonempty_field(
            "mapping.delegations[].parent_request_id",
            &delegation.parent_request_id,
        )?;
        require_nonempty_field(
            "mapping.delegations[].child_request_id",
            &delegation.child_request_id,
        )?;
        if !request_ids.contains(delegation.parent_request_id.as_str()) {
            bail!(
                "multi-agent delegation parent_request_id {:?} does not reference a declared participant/root request",
                delegation.parent_request_id
            );
        }
        if !request_ids.contains(delegation.child_request_id.as_str()) {
            bail!(
                "multi-agent delegation child_request_id {:?} does not reference a declared child participant",
                delegation.child_request_id
            );
        }
    }

    for event in &mapping.tool_events {
        require_nonempty_field("mapping.tool_events[].id", &event.id)?;
        require_nonempty_field("mapping.tool_events[].request_id", &event.request_id)?;
        require_nonempty_field("mapping.tool_events[].tool_name", &event.tool_name)?;
        if !request_ids.contains(event.request_id.as_str()) {
            bail!(
                "multi-agent tool event {:?} request_id {:?} does not reference a declared participant/root request",
                event.id,
                event.request_id
            );
        }
        if let Some(child_request_id) = event.child_request_id.as_deref() {
            require_nonempty_field("mapping.tool_events[].child_request_id", child_request_id)?;
            if !request_ids.contains(child_request_id) {
                bail!(
                    "multi-agent tool event {:?} child_request_id {:?} does not reference a declared child participant",
                    event.id,
                    child_request_id
                );
            }
        }
    }
    Ok(())
}

fn validate_langgraph_mapping(capture: &ExternalAdapterCapture) -> Result<()> {
    match capture.native.get("history").and_then(Value::as_array) {
        Some(history) if !history.is_empty() => Ok(()),
        _ => bail!("LangGraph external adapter mapping requires non-empty native.history"),
    }
}

fn require_nonempty_field(path: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{path} must not be empty");
    }
    Ok(())
}

fn import_langgraph_capture(
    capture: &ExternalAdapterCapture,
    mapping: &ExternalAdapterMapping,
) -> Result<ExternalAdapterImport> {
    let session_id = mapping
        .session_id
        .clone()
        .or_else(|| {
            capture
                .native
                .get("thread_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| format!("external-session:{}", mapping.request_id));
    let status = mapping
        .status
        .clone()
        .or_else(|| {
            latest_langgraph_values(&capture.native)
                .and_then(|values| values.get("status"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "completed".to_string());
    let started_at = "2026-06-05T00:00:00Z";
    let hint = langgraph_state_history_hint(capture, mapping, &session_id)
        .context("building LangGraph state/history projection hint")?;
    let latest_values = latest_langgraph_values(&capture.native);
    let child_request_id = langgraph_child_request_id(latest_values, mapping);
    let child_tool_call_id = latest_values
        .and_then(|values| values.get("tool_call_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            mapping
                .tool_events
                .iter()
                .find_map(|event| event.child_request_id.as_ref().map(|_| event.id.clone()))
        })
        .or_else(|| {
            child_request_id
                .as_ref()
                .map(|_| format!("langgraph:child:{}", mapping.request_id))
        });

    let root_metadata = serde_json::to_string(&json!({
        "adapter_projection": {
            "source_system": capture.source.system,
            "source_package": capture.source.package,
            "source_package_version": capture.source.package_version,
            "scenario_id": mapping.scenario_id,
            "langgraph_state_history": hint,
        }
    }))
    .context("serializing LangGraph root metadata")?;
    let mut requests = vec![TimelineRequestRow {
        request_id: mapping.request_id.clone(),
        agent_did: mapping.agent_did.clone(),
        behavior_id: mapping.behavior_id.clone(),
        session_id: Some(session_id.clone()),
        content: latest_values
            .and_then(|values| values.get("topic"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        metadata: Some(root_metadata),
        status: Some(status.clone()),
        lifecycle_state: Some(status.clone()),
        backend_id: capture.source.package.clone(),
        created_at: Some(started_at.to_string()),
        retry_count: Some(0),
        ..Default::default()
    }];
    if let Some(child_request_id) = child_request_id.as_ref() {
        requests.push(TimelineRequestRow {
            request_id: child_request_id.clone(),
            session_id: Some(session_id.clone()),
            content: Some("Imported LangGraph child request boundary".to_string()),
            status: Some(status.clone()),
            lifecycle_state: Some(status.clone()),
            backend_id: capture.source.package.clone(),
            created_at: Some(started_at.to_string()),
            retry_count: Some(0),
            caused_by_parent_request_id: Some(mapping.request_id.clone()),
            caused_by_parent_tool_call_id: child_tool_call_id.clone(),
            ..Default::default()
        });
    }

    let messages = langgraph_messages(&capture.native)
        .into_iter()
        .enumerate()
        .map(|(index, message)| TimelineMessageRow {
            session_id: session_id.clone(),
            request_id: Some(
                message
                    .request_id
                    .unwrap_or_else(|| mapping.request_id.clone()),
            ),
            sequence: (index as i64) + 1,
            role: message.role,
            content: message.content,
            timestamp: Some(timestamp_for_index(index + 1)),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    let tool_calls = child_request_id
        .as_ref()
        .map(|child_request_id| TimelineToolCallRow {
            request_id: Some(mapping.request_id.clone()),
            session_id: session_id.clone(),
            message_sequence: Some(messages.len().max(1) as i64),
            tool_name: "langgraph_child_boundary".to_string(),
            tool_call_id: child_tool_call_id
                .clone()
                .unwrap_or_else(|| format!("langgraph:child:{}", mapping.request_id)),
            args: json!({
                "source_system": capture.source.system,
                "child_request_id": child_request_id,
            })
            .to_string(),
            result: "external LangGraph child boundary imported".to_string(),
            status: status.clone(),
            started_at: Some(timestamp_for_index(messages.len().max(1))),
            completed_at: Some(timestamp_for_index(messages.len().max(1))),
            child_request_id: Some(child_request_id.clone()),
            ..Default::default()
        })
        .into_iter()
        .collect::<Vec<_>>();
    let responses = requests
        .iter()
        .map(|request| TimelineResponseRow {
            request_id: request.request_id.clone(),
            session_id: Some(session_id.clone()),
            content: Some(
                latest_values
                    .and_then(|values| values.get("final_output"))
                    .and_then(Value::as_str)
                    .unwrap_or("external LangGraph state imported")
                    .to_string(),
            ),
            status: Some(status.clone()),
            materialized_message_sequence: (request.request_id == mapping.request_id)
                .then_some(messages.len() as i64),
            created_at: Some(started_at.to_string()),
            completed_at: Some(timestamp_for_index(messages.len() + 1)),
            ..Default::default()
        })
        .collect::<Vec<_>>();

    let root = requests
        .iter()
        .find(|request| request.request_id == mapping.request_id)
        .cloned()
        .context("imported LangGraph rows missing root request")?;
    let scenario_id = mapping
        .scenario_id
        .clone()
        .unwrap_or_else(|| format!("{}:{}", capture.source.system, mapping.request_id));
    Ok(ExternalAdapterImport {
        projection: mapping.projection,
        actor_did: mapping.actor_did.clone(),
        source_system: capture.source.system.clone(),
        scenario_id,
        rows: RunTimelineRows {
            request: root,
            session: Some(TimelineSessionRow {
                session_id: session_id.clone(),
                agent_name: mapping
                    .agent_did
                    .clone()
                    .or_else(|| mapping.behavior_id.clone()),
                behavior_id: mapping.behavior_id.clone(),
                started: Some(started_at.to_string()),
                status: Some(status.clone()),
                ..Default::default()
            }),
            conversation: Some(TimelineConversationRow {
                session_id,
                agent_name: mapping
                    .agent_did
                    .clone()
                    .or_else(|| mapping.behavior_id.clone()),
                agent_did: mapping.agent_did.clone(),
                behavior_id: mapping.behavior_id.clone(),
                title: Some("Imported LangGraph state history".to_string()),
                title_source: Some("external_adapter_capture".to_string()),
                preview_text: latest_values
                    .and_then(|values| values.get("topic"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                status: Some(status),
                created_at: Some(started_at.to_string()),
                updated_at: Some(timestamp_for_index(messages.len() + 1)),
                latest_request_id: Some(mapping.request_id.clone()),
                ..Default::default()
            }),
            requests,
            messages,
            tool_calls,
            inference_calls: Vec::new(),
            responses,
            rendered_requests: Vec::new(),
        },
    })
}

fn langgraph_state_history_hint(
    capture: &ExternalAdapterCapture,
    mapping: &ExternalAdapterMapping,
    session_id: &str,
) -> Result<Value> {
    let latest_snapshot = capture
        .native
        .get("history")
        .and_then(Value::as_array)
        .and_then(|history| history.first())
        .context("LangGraph capture missing native.history[0]")?;
    let mut values = latest_snapshot
        .get("values")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    values.insert(
        "history_checkpoint_count".to_string(),
        json!(capture
            .native
            .get("history")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or_default()),
    );
    if let Some(package_version) = capture.source.package_version.as_deref() {
        values.insert(
            "langgraph_package_version".to_string(),
            json!(package_version),
        );
    }
    if let Some(provider) = capture.native.get("provider") {
        values.insert("provider".to_string(), provider.clone());
    }

    Ok(json!({
        "thread_id": capture
            .native
            .get("thread_id")
            .and_then(Value::as_str)
            .unwrap_or(session_id),
        "checkpoint_id": latest_snapshot
            .pointer("/config/configurable/checkpoint_id")
            .and_then(Value::as_str)
            .unwrap_or("langgraph:checkpoint:missing"),
        "root_request_id": mapping.request_id,
        "values": Value::Object(values),
        "nodes": langgraph_projection_nodes(&capture.native, mapping),
        "edges": capture
            .native
            .pointer("/graph/edges")
            .cloned()
            .unwrap_or_else(|| json!([])),
        "tasks": langgraph_projection_tasks(&capture.native, mapping),
    }))
}

fn latest_langgraph_values(native: &Value) -> Option<&serde_json::Map<String, Value>> {
    native
        .get("history")?
        .as_array()?
        .first()?
        .get("values")?
        .as_object()
}

fn langgraph_projection_nodes(native: &Value, mapping: &ExternalAdapterMapping) -> Vec<Value> {
    let values = latest_langgraph_values(native);
    let status = values
        .and_then(|values| values.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("completed");
    let child_request_id = langgraph_child_request_id(values, mapping);
    let mut nodes = vec![json!({
        "id": "langgraph:start",
        "kind": "start",
        "status": "completed",
    })];
    for name in native
        .pointer("/graph/nodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        let (id, kind, request_id) = if name.ends_with("_subgraph") {
            (
                format!("langgraph:subgraph:{}", name.trim_end_matches("_subgraph")),
                "subgraph",
                child_request_id
                    .as_deref()
                    .unwrap_or(mapping.request_id.as_str()),
            )
        } else {
            (
                format!("langgraph:node:{name}"),
                if name == "provider_model" {
                    "provider_node"
                } else {
                    "node"
                },
                if name == "provider_tool" {
                    child_request_id
                        .as_deref()
                        .unwrap_or(mapping.request_id.as_str())
                } else {
                    mapping.request_id.as_str()
                },
            )
        };
        nodes.push(json!({
            "id": id,
            "kind": kind,
            "request_id": request_id,
            "status": status,
        }));
    }
    if let Some(subgraphs) = native
        .pointer("/graph/subgraphs")
        .and_then(Value::as_object)
    {
        for (subgraph_name, subgraph) in subgraphs {
            let prefix = subgraph_name.trim_end_matches("_subgraph");
            nodes.push(json!({
                "id": format!("langgraph:subgraph:{prefix}:start"),
                "kind": "subgraph_start",
                "request_id": child_request_id.as_deref().unwrap_or(mapping.request_id.as_str()),
                "status": "completed",
            }));
            for name in subgraph
                .get("nodes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                nodes.push(json!({
                    "id": format!("langgraph:subgraph:{prefix}:{name}"),
                    "kind": "subgraph_node",
                    "request_id": child_request_id.as_deref().unwrap_or(mapping.request_id.as_str()),
                    "status": status,
                }));
            }
            nodes.push(json!({
                "id": format!("langgraph:subgraph:{prefix}:end"),
                "kind": "subgraph_end",
                "request_id": child_request_id.as_deref().unwrap_or(mapping.request_id.as_str()),
                "status": status,
            }));
        }
    }
    nodes.push(json!({
        "id": "langgraph:end",
        "kind": "end",
        "status": status,
    }));
    nodes
}

fn langgraph_projection_tasks(native: &Value, mapping: &ExternalAdapterMapping) -> Vec<Value> {
    let values = latest_langgraph_values(native);
    let child_request_id = langgraph_child_request_id(values, mapping);
    let native_tasks = collect_langgraph_native_tasks(native);
    let mut tasks = Vec::new();
    for name in native
        .pointer("/graph/nodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        tasks.push(langgraph_task_value(
            name,
            &native_tasks,
            if name == "provider_tool" {
                child_request_id
                    .as_deref()
                    .unwrap_or(mapping.request_id.as_str())
            } else {
                mapping.request_id.as_str()
            },
            langgraph_task_child_request_id(name, child_request_id.as_deref()),
        ));
    }
    if let Some(subgraphs) = native
        .pointer("/graph/subgraphs")
        .and_then(Value::as_object)
    {
        for subgraph in subgraphs.values() {
            for name in subgraph
                .get("nodes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                tasks.push(langgraph_task_value(
                    name,
                    &native_tasks,
                    child_request_id
                        .as_deref()
                        .unwrap_or(mapping.request_id.as_str()),
                    None,
                ));
            }
        }
    }
    tasks
}

fn langgraph_task_value(
    name: &str,
    native_tasks: &BTreeMap<String, Value>,
    request_id: &str,
    task_child_request_id: Option<&str>,
) -> Value {
    let task = native_tasks.get(name);
    let status = if task
        .and_then(|task| task.get("error"))
        .is_some_and(|error| !error.is_null())
    {
        "failed"
    } else if task
        .and_then(|task| task.get("result"))
        .is_some_and(Value::is_null)
    {
        "pending"
    } else {
        "completed"
    };
    let mut value = json!({
        "id": task
            .and_then(|task| task.get("id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("langgraph:task:{name}")),
        "request_id": request_id,
        "name": name,
        "status": status,
    });
    if let Some(task_child_request_id) = task_child_request_id {
        if let Some(object) = value.as_object_mut() {
            object.insert("child_request_id".to_string(), json!(task_child_request_id));
        }
    }
    value
}

fn langgraph_task_child_request_id<'a>(
    name: &str,
    child_request_id: Option<&'a str>,
) -> Option<&'a str> {
    match name {
        "delegate" | "review_subgraph" | "provider_model" => child_request_id,
        _ => None,
    }
}

fn langgraph_child_request_id(
    values: Option<&serde_json::Map<String, Value>>,
    mapping: &ExternalAdapterMapping,
) -> Option<String> {
    values
        .and_then(|values| values.get("child_request_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            mapping
                .tool_events
                .iter()
                .find_map(|event| event.child_request_id.clone())
        })
        .or_else(|| {
            values
                .and_then(|values| values.get("tool_call_id"))
                .and_then(Value::as_str)
                .map(|_| format!("{}:tool", mapping.request_id))
        })
}

fn collect_langgraph_native_tasks(native: &Value) -> BTreeMap<String, Value> {
    let mut tasks = BTreeMap::new();
    for snapshot in native
        .get("history")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .rev()
    {
        for task in snapshot
            .get("tasks")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(name) = task.get("name").and_then(Value::as_str) {
                tasks.insert(name.to_string(), task.clone());
            }
        }
    }
    tasks
}

fn langgraph_messages(native: &Value) -> Vec<ImportedMessage> {
    latest_langgraph_values(native)
        .and_then(|values| values.get("messages"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|message| match message {
            Value::String(content) => Some(ImportedMessage {
                role: "state".to_string(),
                content: content.clone(),
                request_id: None,
            }),
            Value::Object(object) => object.get("content").map(|content| ImportedMessage {
                role: object
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("message")
                    .to_string(),
                content: value_to_text(content),
                request_id: None,
            }),
            value => Some(ImportedMessage {
                role: "state".to_string(),
                content: value_to_text(value),
                request_id: None,
            }),
        })
        .collect()
}

fn import_multi_agent_capture(
    capture: &ExternalAdapterCapture,
    mapping: &ExternalAdapterMapping,
) -> Result<ExternalAdapterImport> {
    let session_id = mapping
        .session_id
        .clone()
        .unwrap_or_else(|| format!("external-session:{}", mapping.request_id));
    let status = mapping
        .status
        .clone()
        .unwrap_or_else(|| "completed".to_string());
    let started_at = "2026-06-05T00:00:00Z";
    let root_participant = mapping.participants.iter().find(|participant| {
        participant
            .request_id
            .as_deref()
            .is_none_or(|request_id| request_id == mapping.request_id)
    });
    let root_agent_did = first_owned([
        mapping.agent_did.as_deref(),
        root_participant.and_then(|participant| participant.agent_did.as_deref()),
    ]);
    let root_behavior_id = first_owned([
        mapping.behavior_id.as_deref(),
        root_participant.and_then(|participant| participant.behavior_id.as_deref()),
    ]);
    let participant_by_request = mapping
        .participants
        .iter()
        .filter_map(|participant| {
            let request_id = participant.request_id.as_ref()?;
            Some((request_id.as_str(), participant))
        })
        .collect::<BTreeMap<_, _>>();
    let delegation_by_child = mapping
        .delegations
        .iter()
        .map(|delegation| (delegation.child_request_id.as_str(), delegation))
        .collect::<BTreeMap<_, _>>();
    let child_request_ids = child_request_ids(mapping);
    let mut requests = Vec::new();
    requests.push(TimelineRequestRow {
        request_id: mapping.request_id.clone(),
        agent_did: root_agent_did.clone(),
        behavior_id: root_behavior_id.clone(),
        session_id: Some(session_id.clone()),
        content: external_task_text(&capture.native),
        metadata: Some(root_metadata(capture, mapping)?),
        status: Some(status.clone()),
        lifecycle_state: Some(status.clone()),
        backend_id: capture.source.package.clone(),
        created_at: Some(started_at.to_string()),
        retry_count: Some(0),
        ..Default::default()
    });

    for child_request_id in child_request_ids {
        let participant = participant_by_request
            .get(child_request_id.as_str())
            .copied();
        let delegation = delegation_by_child.get(child_request_id.as_str()).copied();
        requests.push(TimelineRequestRow {
            request_id: child_request_id.clone(),
            agent_did: first_owned([
                participant.and_then(|participant| participant.agent_did.as_deref()),
                delegation.and_then(|delegation| delegation.agent_did.as_deref()),
            ]),
            behavior_id: first_owned([
                participant.and_then(|participant| participant.behavior_id.as_deref()),
                delegation.and_then(|delegation| delegation.behavior_id.as_deref()),
            ]),
            session_id: Some(session_id.clone()),
            content: participant
                .and_then(|participant| participant.native_name.as_deref())
                .map(|name| format!("Imported external participant {name}")),
            metadata: participant
                .map(participant_metadata)
                .transpose()
                .context("serializing child request participant metadata")?,
            status: Some(status.clone()),
            lifecycle_state: Some(status.clone()),
            backend_id: capture.source.package.clone(),
            created_at: Some(started_at.to_string()),
            retry_count: Some(0),
            caused_by_parent_request_id: delegation
                .map(|delegation| delegation.parent_request_id.clone()),
            caused_by_parent_tool_call_id: delegation.and_then(|delegation| {
                delegation
                    .parent_tool_call_id
                    .clone()
                    .or_else(|| Some(default_delegation_tool_call_id(delegation)))
            }),
            ..Default::default()
        });
    }

    let messages = native_messages(&capture.source.system, &capture.native, mapping)
        .into_iter()
        .enumerate()
        .map(|(index, message)| TimelineMessageRow {
            session_id: session_id.clone(),
            request_id: Some(
                message
                    .request_id
                    .unwrap_or_else(|| mapping.request_id.clone()),
            ),
            sequence: (index as i64) + 1,
            role: message.role,
            content: message.content,
            timestamp: Some(timestamp_for_index(index + 1)),
            ..Default::default()
        })
        .collect::<Vec<_>>();

    let mut seen_tool_call_ids = BTreeSet::new();
    let mut tool_calls = Vec::new();
    for (index, delegation) in mapping.delegations.iter().enumerate() {
        let tool_call_id = delegation
            .parent_tool_call_id
            .clone()
            .unwrap_or_else(|| default_delegation_tool_call_id(delegation));
        seen_tool_call_ids.insert(tool_call_id.clone());
        tool_calls.push(TimelineToolCallRow {
            request_id: Some(delegation.parent_request_id.clone()),
            session_id: session_id.clone(),
            message_sequence: Some((index as i64) + 1),
            tool_name: delegation
                .tool_name
                .clone()
                .unwrap_or_else(|| "handoff".to_string()),
            tool_call_id,
            args: json!({
                "source_system": capture.source.system,
                "child_request_id": delegation.child_request_id,
            })
            .to_string(),
            result: "external framework delegation imported".to_string(),
            status: delegation
                .status
                .clone()
                .unwrap_or_else(|| "completed".to_string()),
            started_at: Some(timestamp_for_index(index + 1)),
            completed_at: Some(timestamp_for_index(index + 1)),
            child_request_id: Some(delegation.child_request_id.clone()),
            ..Default::default()
        });
    }
    for (index, event) in mapping.tool_events.iter().enumerate() {
        if !seen_tool_call_ids.insert(event.id.clone()) {
            continue;
        }
        tool_calls.push(TimelineToolCallRow {
            request_id: Some(event.request_id.clone()),
            session_id: session_id.clone(),
            message_sequence: Some((index as i64) + 1),
            tool_name: event.tool_name.clone(),
            tool_call_id: event.id.clone(),
            args: json!({ "source_system": capture.source.system }).to_string(),
            result: "external framework tool event imported".to_string(),
            status: event
                .status
                .clone()
                .unwrap_or_else(|| "completed".to_string()),
            started_at: Some(timestamp_for_index(index + 1)),
            completed_at: Some(timestamp_for_index(index + 1)),
            child_request_id: event.child_request_id.clone(),
            ..Default::default()
        });
    }

    let mut responses = requests
        .iter()
        .map(|request| TimelineResponseRow {
            request_id: request.request_id.clone(),
            agent_did: request.agent_did.clone(),
            behavior_id: request.behavior_id.clone(),
            session_id: Some(session_id.clone()),
            content: Some(response_content_for_request(request, &messages)),
            status: Some(status.clone()),
            materialized_message_sequence: (request.request_id == mapping.request_id)
                .then_some(messages.len() as i64),
            created_at: Some(started_at.to_string()),
            completed_at: Some(timestamp_for_index(messages.len() + 1)),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    if responses.is_empty() {
        responses.push(TimelineResponseRow {
            request_id: mapping.request_id.clone(),
            session_id: Some(session_id.clone()),
            status: Some(status.clone()),
            created_at: Some(started_at.to_string()),
            completed_at: Some(timestamp_for_index(messages.len() + 1)),
            ..Default::default()
        });
    }

    let root = requests
        .iter()
        .find(|request| request.request_id == mapping.request_id)
        .cloned()
        .context("imported rows missing root request")?;
    let scenario_id = mapping
        .scenario_id
        .clone()
        .unwrap_or_else(|| format!("{}:{}", capture.source.system, mapping.request_id));
    Ok(ExternalAdapterImport {
        projection: mapping.projection,
        actor_did: mapping.actor_did.clone(),
        source_system: capture.source.system.clone(),
        scenario_id,
        rows: RunTimelineRows {
            request: root,
            session: Some(TimelineSessionRow {
                session_id: session_id.clone(),
                agent_name: root_agent_did
                    .as_deref()
                    .or(root_behavior_id.as_deref())
                    .map(ToOwned::to_owned),
                behavior_id: root_behavior_id.clone(),
                started: Some(started_at.to_string()),
                status: Some(status.clone()),
                ..Default::default()
            }),
            conversation: Some(TimelineConversationRow {
                session_id,
                agent_name: root_agent_did
                    .as_deref()
                    .or(root_behavior_id.as_deref())
                    .map(ToOwned::to_owned),
                agent_did: root_agent_did,
                behavior_id: root_behavior_id,
                title: Some(format!("Imported {}", capture.source.system)),
                title_source: Some("external_adapter_capture".to_string()),
                preview_text: external_task_text(&capture.native),
                status: Some(status),
                created_at: Some(started_at.to_string()),
                updated_at: Some(timestamp_for_index(messages.len() + 1)),
                latest_request_id: Some(mapping.request_id.clone()),
                ..Default::default()
            }),
            requests,
            messages,
            tool_calls,
            inference_calls: Vec::new(),
            responses,
            rendered_requests: Vec::new(),
        },
    })
}

fn child_request_ids(mapping: &ExternalAdapterMapping) -> Vec<String> {
    let mut ids = BTreeSet::new();
    for participant in &mapping.participants {
        if let Some(request_id) = participant.request_id.as_deref() {
            if request_id != mapping.request_id {
                ids.insert(request_id.to_string());
            }
        }
    }
    for delegation in &mapping.delegations {
        if delegation.child_request_id != mapping.request_id {
            ids.insert(delegation.child_request_id.clone());
        }
    }
    ids.into_iter().collect()
}

fn external_task_text(native: &Value) -> Option<String> {
    native
        .get("task")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn root_metadata(
    capture: &ExternalAdapterCapture,
    mapping: &ExternalAdapterMapping,
) -> Result<String> {
    serde_json::to_string(&json!({
        "adapter_projection": {
            "source_system": capture.source.system,
            "source_package": capture.source.package,
            "source_package_version": capture.source.package_version,
            "scenario_id": mapping.scenario_id,
            "role": mapping
                .participants
                .iter()
                .find(|participant| {
                    participant
                        .request_id
                        .as_deref()
                        .is_none_or(|request_id| request_id == mapping.request_id)
                })
                .map(|participant| participant.role.as_str())
                .unwrap_or("owner"),
            "participants": mapping.participants,
        }
    }))
    .context("serializing external adapter root metadata")
}

fn participant_metadata(participant: &ExternalParticipantMapping) -> Result<String> {
    serde_json::to_string(&json!({
        "adapter_projection": {
            "role": participant.role,
            "native_name": participant.native_name,
        }
    }))
    .context("serializing external adapter participant metadata")
}

fn native_messages(
    source_system: &str,
    native: &Value,
    mapping: &ExternalAdapterMapping,
) -> Vec<ImportedMessage> {
    match source_system {
        "autogen-agentchat" => autogen_messages(native, mapping),
        "crewai" => crewai_messages(native, mapping),
        "microsoft-agent-framework" => microsoft_agent_framework_messages(native, mapping),
        _ => Vec::new(),
    }
}

fn autogen_messages(native: &Value, mapping: &ExternalAdapterMapping) -> Vec<ImportedMessage> {
    native
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|message| {
            let source = message.get("source")?.as_str()?;
            let participant = participant_for_native_name(mapping, source);
            Some(ImportedMessage {
                role: participant
                    .map(|participant| participant.role.clone())
                    .unwrap_or_else(|| source.to_string()),
                content: value_to_text(message.get("content")?),
                request_id: participant
                    .and_then(|participant| participant.request_id.clone())
                    .or_else(|| Some(mapping.request_id.clone())),
            })
        })
        .collect()
}

fn crewai_messages(native: &Value, mapping: &ExternalAdapterMapping) -> Vec<ImportedMessage> {
    let manager_responses = native
        .get("manager_responses")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    if !manager_responses.is_empty() {
        return crewai_hierarchical_messages(native, mapping, manager_responses);
    }

    native
        .get("tasks")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|task| crewai_task_message(task, mapping))
        .collect()
}

fn crewai_hierarchical_messages(
    native: &Value,
    mapping: &ExternalAdapterMapping,
    manager_responses: &[Value],
) -> Vec<ImportedMessage> {
    let mut messages = Vec::new();
    let manager = participant_for_role(mapping, "manager");
    let manager_role = manager
        .map(|participant| participant.role.clone())
        .unwrap_or_else(|| "manager".to_string());
    let manager_request_id = manager
        .and_then(|participant| participant.request_id.clone())
        .unwrap_or_else(|| mapping.request_id.clone());
    let tasks = native
        .get("tasks")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    if let Some(response) = manager_responses.first() {
        messages.push(ImportedMessage {
            role: manager_role.clone(),
            content: value_to_text(response),
            request_id: Some(manager_request_id.clone()),
        });
    }
    push_crewai_llm_message(&mut messages, native, mapping, "researcher");
    if let Some(task) = tasks.first() {
        if let Some(mut message) = crewai_task_message(task, mapping) {
            message.role = manager_role.clone();
            message.request_id = Some(manager_request_id.clone());
            messages.push(message);
        }
    }
    if let Some(response) = manager_responses.get(2) {
        messages.push(ImportedMessage {
            role: manager_role.clone(),
            content: value_to_text(response),
            request_id: Some(manager_request_id.clone()),
        });
    }
    push_crewai_llm_message(&mut messages, native, mapping, "reviewer");
    if let Some(task) = tasks.get(1) {
        if let Some(mut message) = crewai_task_message(task, mapping) {
            message.role = manager_role;
            message.request_id = Some(manager_request_id);
            messages.push(message);
        }
    }
    messages
}

fn crewai_task_message(task: &Value, mapping: &ExternalAdapterMapping) -> Option<ImportedMessage> {
    let native_name = task
        .pointer("/agent/role")
        .and_then(Value::as_str)
        .unwrap_or("agent");
    let participant = participant_for_native_name(mapping, native_name)
        .or_else(|| participant_for_role(mapping, native_name));
    let output = task.pointer("/output/raw").or_else(|| task.get("output"))?;
    Some(ImportedMessage {
        role: participant
            .map(|participant| participant.role.clone())
            .unwrap_or_else(|| native_name.to_string()),
        content: value_to_text(output),
        request_id: participant
            .and_then(|participant| participant.request_id.clone())
            .or_else(|| Some(mapping.request_id.clone())),
    })
}

fn push_crewai_llm_message(
    messages: &mut Vec<ImportedMessage>,
    native: &Value,
    mapping: &ExternalAdapterMapping,
    role: &str,
) {
    let Some(content) = native
        .pointer(&format!("/llm_calls/{role}"))
        .and_then(Value::as_array)
        .and_then(|calls| calls.last())
        .and_then(|call| call.get("response"))
        .map(value_to_text)
    else {
        return;
    };
    let participant = participant_for_role(mapping, role);
    messages.push(ImportedMessage {
        role: participant
            .map(|participant| participant.role.clone())
            .unwrap_or_else(|| role.to_string()),
        content: strip_final_answer_prefix(&content).to_string(),
        request_id: participant
            .and_then(|participant| participant.request_id.clone())
            .or_else(|| Some(mapping.request_id.clone())),
    });
}

fn microsoft_agent_framework_messages(
    native: &Value,
    mapping: &ExternalAdapterMapping,
) -> Vec<ImportedMessage> {
    let mut messages = Vec::new();
    if let Some(task) = external_task_text(native) {
        messages.push(ImportedMessage {
            role: "user".to_string(),
            content: task,
            request_id: Some(mapping.request_id.clone()),
        });
    }
    if let Some(outputs) = native.get("agent_outputs").and_then(Value::as_object) {
        let mut keys = outputs.keys().collect::<Vec<_>>();
        keys.sort();
        for key in keys {
            let Some(items) = outputs.get(key).and_then(Value::as_array) else {
                continue;
            };
            for item in items {
                let role = item.get("role").and_then(Value::as_str).unwrap_or(key);
                let participant = participant_for_role(mapping, role)
                    .or_else(|| participant_for_native_name(mapping, key));
                messages.push(ImportedMessage {
                    role: participant
                        .map(|participant| participant.role.clone())
                        .unwrap_or_else(|| role.to_string()),
                    content: item
                        .get("text")
                        .map(value_to_text)
                        .unwrap_or_else(|| item.to_string()),
                    request_id: item
                        .get("request_id")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                        .or_else(|| {
                            participant.and_then(|participant| participant.request_id.clone())
                        })
                        .or_else(|| Some(mapping.request_id.clone())),
                });
            }
        }
    }
    if let Some(output) = microsoft_agent_framework_final_output(native) {
        messages.push(ImportedMessage {
            role: "orchestrator".to_string(),
            content: output,
            request_id: Some(mapping.request_id.clone()),
        });
    }
    messages
}

fn microsoft_agent_framework_final_output(native: &Value) -> Option<String> {
    native
        .get("events")?
        .as_array()?
        .iter()
        .rev()
        .find_map(|event| {
            if event.get("type").and_then(Value::as_str) != Some("output") {
                return None;
            }
            event
                .pointer("/data/text")
                .and_then(Value::as_str)
                .or_else(|| {
                    event
                        .pointer("/data/contents")
                        .and_then(Value::as_array)?
                        .iter()
                        .find_map(|content| content.get("text").and_then(Value::as_str))
                })
                .filter(|text| !text.trim().is_empty())
                .map(ToOwned::to_owned)
        })
}

fn participant_for_native_name<'a>(
    mapping: &'a ExternalAdapterMapping,
    native_name: &str,
) -> Option<&'a ExternalParticipantMapping> {
    mapping.participants.iter().find(|participant| {
        participant.native_name.as_deref() == Some(native_name) || participant.role == native_name
    })
}

fn participant_for_role<'a>(
    mapping: &'a ExternalAdapterMapping,
    role: &str,
) -> Option<&'a ExternalParticipantMapping> {
    mapping
        .participants
        .iter()
        .find(|participant| participant.role == role)
}

fn strip_final_answer_prefix(value: &str) -> &str {
    value.strip_prefix("Final Answer: ").unwrap_or(value)
}

fn response_content_for_request(
    request: &TimelineRequestRow,
    messages: &[TimelineMessageRow],
) -> String {
    let Some(metadata) = request.metadata.as_deref() else {
        return "external framework request imported".to_string();
    };
    let native_name = serde_json::from_str::<Value>(metadata)
        .ok()
        .and_then(|value| {
            value
                .pointer("/adapter_projection/native_name")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        });
    if let Some(native_name) = native_name {
        if let Some(message) = messages
            .iter()
            .rev()
            .find(|message| message.role == native_name)
        {
            return message.content.clone();
        }
    }
    messages
        .last()
        .map(|message| message.content.clone())
        .unwrap_or_else(|| "external framework request imported".to_string())
}

fn default_delegation_tool_call_id(delegation: &ExternalDelegationMapping) -> String {
    format!(
        "external:{}:{}",
        delegation.parent_request_id, delegation.child_request_id
    )
}

fn timestamp_for_index(index: usize) -> String {
    format!("2026-06-05T00:00:{:02}Z", index.min(59))
}

fn value_to_text(value: &Value) -> String {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn first_owned<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> Option<String> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}
