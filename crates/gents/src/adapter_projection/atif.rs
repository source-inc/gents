use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use gents_protocol::transcript::{
    normalize_markdown_text, present_persisted_message, PresentedMessageRole,
};

use crate::run_timeline::{
    RunTimeline, RunTimelineEvent, TimelineMessageEvent, TimelineResponseEvent,
    TimelineToolCallEvent,
};

use super::{redact_json_value, redact_option, redact_str, ProjectionContext};

pub const ATIF_SCHEMA_VERSION: &str = "ATIF-v1.7";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AtifTrajectory {
    pub schema_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trajectory_id: Option<String>,
    pub agent: AtifAgent,
    pub steps: Vec<AtifStep>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_metrics: Option<AtifFinalMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AtifAgent {
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AtifStepSource {
    System,
    User,
    Agent,
}

impl AtifStepSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Agent => "agent",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AtifStep {
    pub step_id: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    pub source: AtifStepSource,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<AtifToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observation: Option<AtifObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_call_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AtifToolCall {
    pub tool_call_id: String,
    pub function_name: String,
    pub arguments: BTreeMap<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AtifObservation {
    pub results: Vec<AtifObservationResult>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AtifObservationResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AtifFinalMetrics {
    pub total_steps: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<BTreeMap<String, Value>>,
}

pub(super) fn build_atif_trajectory(
    timeline: &RunTimeline,
    context: &ProjectionContext,
) -> AtifTrajectory {
    let mut steps = Vec::new();
    let mut consumed_tool_call_ids = BTreeSet::new();
    let mut emitted_request_input = false;

    if let Some(content) = timeline
        .request
        .content
        .as_deref()
        .filter(|content| !content.trim().is_empty())
    {
        steps.push(AtifStep {
            step_id: 0,
            timestamp: valid_timestamp(timeline.request.created_at.as_deref()),
            source: AtifStepSource::User,
            message: redact_str(content, context),
            reasoning_content: None,
            tool_calls: None,
            observation: None,
            llm_call_count: None,
            extra: optional_extra([
                ("request_id", string_value(&timeline.request_id)),
                (
                    "session_id",
                    optional_string_value(timeline.session_id.as_deref()),
                ),
                (
                    "status",
                    optional_string_value(timeline.request.status.as_deref()),
                ),
                (
                    "lifecycle_state",
                    optional_string_value(timeline.request.lifecycle_state.as_deref()),
                ),
            ]),
        });
        emitted_request_input = true;
    }

    for event in &timeline.events {
        match event {
            RunTimelineEvent::Message(message) if root_message(timeline, message) => {
                let Some((source, content, reasoning)) = projected_message(message) else {
                    continue;
                };
                if emitted_request_input
                    && source == AtifStepSource::User
                    && timeline
                        .request
                        .content
                        .as_deref()
                        .is_some_and(|request_content| {
                            normalize_markdown_text(request_content) == content
                        })
                {
                    emitted_request_input = false;
                    continue;
                }

                let paired_tools = if source == AtifStepSource::Agent {
                    paired_tools_for_message(timeline, message)
                } else {
                    Vec::new()
                };
                consumed_tool_call_ids.extend(
                    paired_tools
                        .iter()
                        .map(|tool| tool.tool_call_id.as_str().to_string()),
                );
                steps.push(step_for_message(
                    message,
                    source,
                    &content,
                    reasoning.as_deref(),
                    paired_tools,
                    context,
                ));
            }
            RunTimelineEvent::ToolCall(tool)
                if root_tool(timeline, tool)
                    && !consumed_tool_call_ids.contains(&tool.tool_call_id)
                    && !tool_has_paired_message(timeline, tool) =>
            {
                consumed_tool_call_ids.insert(tool.tool_call_id.clone());
                steps.push(step_for_unpaired_tool(tool, context));
            }
            RunTimelineEvent::Response(response) if response.request_id == timeline.request_id => {
                apply_root_response(&mut steps, response, context);
            }
            RunTimelineEvent::Request(_)
            | RunTimelineEvent::RenderedRequest(_)
            | RunTimelineEvent::InferenceCall(_)
            | RunTimelineEvent::Message(_)
            | RunTimelineEvent::ToolCall(_)
            | RunTimelineEvent::Response(_) => {}
        }
    }

    if steps.is_empty() {
        steps.push(AtifStep {
            step_id: 1,
            timestamp: valid_timestamp(timeline.request.created_at.as_deref()),
            source: AtifStepSource::User,
            message: String::new(),
            reasoning_content: None,
            tool_calls: None,
            observation: None,
            llm_call_count: None,
            extra: optional_extra([("request_id", string_value(&timeline.request_id))]),
        });
    }
    for (index, step) in steps.iter_mut().enumerate() {
        step.step_id = index + 1;
    }

    let inference_call_count = timeline
        .inference_calls
        .iter()
        .filter(|call| call.request_id == timeline.request_id && call.call_kind == "inference")
        .count();
    let tool_call_count = timeline
        .events
        .iter()
        .filter(
            |event| matches!(event, RunTimelineEvent::ToolCall(tool) if root_tool(timeline, tool)),
        )
        .count();

    AtifTrajectory {
        schema_version: ATIF_SCHEMA_VERSION.to_string(),
        session_id: timeline
            .session_id
            .clone()
            .or_else(|| Some(timeline.request_id.clone())),
        trajectory_id: Some(timeline.request_id.clone()),
        agent: AtifAgent {
            name: timeline
                .session
                .as_ref()
                .and_then(|session| session.agent_name.clone())
                .or_else(|| {
                    timeline
                        .conversation
                        .as_ref()
                        .and_then(|conversation| conversation.agent_name.clone())
                })
                .or_else(|| timeline.behavior_id.clone())
                .unwrap_or_else(|| "gents".to_string()),
            version: env!("CARGO_PKG_VERSION").to_string(),
            model_name: super::adapter_projection_metadata_string(
                timeline.request.metadata.as_deref(),
                "model_name",
            ),
            extra: optional_extra([
                ("runtime", string_value("gents")),
                ("agent_did", optional_string_value(timeline.agent_did.as_deref())),
                (
                    "behavior_id",
                    optional_string_value(timeline.behavior_id.as_deref()),
                ),
                (
                    "backend_id",
                    optional_string_value(timeline.request.backend_id.as_deref()),
                ),
            ]),
        },
        final_metrics: Some(AtifFinalMetrics {
            total_steps: steps.len(),
            extra: optional_extra([
                ("request_id", string_value(&timeline.request_id)),
                ("status", optional_string_value(timeline.request.status.as_deref())),
                (
                    "lifecycle_state",
                    optional_string_value(timeline.request.lifecycle_state.as_deref()),
                ),
                ("inference_call_count", Some(json!(inference_call_count))),
                ("tool_call_count", Some(json!(tool_call_count))),
                ("child_request_ids", Some(json!(timeline.child_request_ids))),
            ]),
        }),
        steps,
        notes: Some(
            "Projected from Gents' persisted run timeline; token metrics are omitted when the durable trace does not distinguish prompt and completion usage."
                .to_string(),
        ),
        extra: optional_extra([
            ("source_projection_id", string_value("run_timeline")),
            ("source_request_id", string_value(&timeline.request_id)),
            // Capture metadata only (keys, hashes, ordering, admission join).
            // ATIF `extra` bypasses redaction, which is safe here precisely
            // because bodies never enter the timeline this is derived from.
            (
                "rendered_captures",
                super::rendered_captures_json(timeline),
            ),
        ]),
    }
}

pub(super) fn validate_atif_trajectory(violations: &mut Vec<String>, trajectory: &AtifTrajectory) {
    if trajectory.schema_version != ATIF_SCHEMA_VERSION {
        violations.push(format!(
            "output.projection.schema_version expected {ATIF_SCHEMA_VERSION:?}, got {:?}",
            trajectory.schema_version
        ));
    }
    require_nonempty(
        violations,
        "output.projection.agent.name",
        &trajectory.agent.name,
    );
    require_nonempty(
        violations,
        "output.projection.agent.version",
        &trajectory.agent.version,
    );
    if trajectory.steps.is_empty() {
        violations.push("output.projection.steps must not be empty".to_string());
    }
    for (index, step) in trajectory.steps.iter().enumerate() {
        let expected_step_id = index + 1;
        if step.step_id != expected_step_id {
            violations.push(format!(
                "output.projection.steps[{index}].step_id expected {expected_step_id}, got {}",
                step.step_id
            ));
        }
        if let Some(timestamp) = step.timestamp.as_deref() {
            if chrono::DateTime::parse_from_rfc3339(timestamp).is_err() {
                violations.push(format!(
                    "output.projection.steps[{index}].timestamp must be ISO 8601"
                ));
            }
        }
        let tool_call_ids = step
            .tool_calls
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|tool| tool.tool_call_id.as_str())
            .collect::<BTreeSet<_>>();
        for (tool_index, tool) in step
            .tool_calls
            .as_deref()
            .unwrap_or_default()
            .iter()
            .enumerate()
        {
            require_nonempty(
                violations,
                &format!("output.projection.steps[{index}].tool_calls[{tool_index}].tool_call_id"),
                &tool.tool_call_id,
            );
            require_nonempty(
                violations,
                &format!("output.projection.steps[{index}].tool_calls[{tool_index}].function_name"),
                &tool.function_name,
            );
        }
        if let Some(observation) = step.observation.as_ref() {
            for (result_index, result) in observation.results.iter().enumerate() {
                if let Some(source_call_id) = result.source_call_id.as_deref() {
                    if !tool_call_ids.contains(source_call_id) {
                        violations.push(format!(
                            "output.projection.steps[{index}].observation.results[{result_index}].source_call_id {:?} does not reference a tool call in the same step",
                            source_call_id
                        ));
                    }
                }
            }
        }
    }
}

pub(super) fn atif_projection_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["schema_version", "agent", "steps"],
        "properties": {
            "schema_version": { "const": ATIF_SCHEMA_VERSION },
            "session_id": optional_string_schema(),
            "trajectory_id": optional_string_schema(),
            "agent": { "$ref": "#/$defs/agent" },
            "steps": {
                "type": "array",
                "minItems": 1,
                "items": { "$ref": "#/$defs/step" }
            },
            "notes": optional_string_schema(),
            "final_metrics": {
                "anyOf": [
                    { "$ref": "#/$defs/final_metrics" },
                    { "type": "null" }
                ]
            },
            "extra": optional_object_schema()
        },
        "$defs": {
            "agent": {
                "type": "object",
                "additionalProperties": false,
                "required": ["name", "version"],
                "properties": {
                    "name": { "type": "string" },
                    "version": { "type": "string" },
                    "model_name": optional_string_schema(),
                    "extra": optional_object_schema()
                }
            },
            "step": {
                "type": "object",
                "additionalProperties": false,
                "required": ["step_id", "source", "message"],
                "properties": {
                    "step_id": { "type": "integer", "minimum": 1 },
                    "timestamp": optional_string_schema(),
                    "source": { "enum": ["system", "user", "agent"] },
                    "message": { "type": "string" },
                    "reasoning_content": optional_string_schema(),
                    "tool_calls": {
                        "anyOf": [
                            {
                                "type": "array",
                                "items": { "$ref": "#/$defs/tool_call" }
                            },
                            { "type": "null" }
                        ]
                    },
                    "observation": {
                        "anyOf": [
                            { "$ref": "#/$defs/observation" },
                            { "type": "null" }
                        ]
                    },
                    "llm_call_count": {
                        "anyOf": [
                            { "type": "integer", "minimum": 0 },
                            { "type": "null" }
                        ]
                    },
                    "extra": optional_object_schema()
                }
            },
            "tool_call": {
                "type": "object",
                "additionalProperties": false,
                "required": ["tool_call_id", "function_name", "arguments"],
                "properties": {
                    "tool_call_id": { "type": "string" },
                    "function_name": { "type": "string" },
                    "arguments": { "type": "object" },
                    "extra": optional_object_schema()
                }
            },
            "observation": {
                "type": "object",
                "additionalProperties": false,
                "required": ["results"],
                "properties": {
                    "results": {
                        "type": "array",
                        "items": { "$ref": "#/$defs/observation_result" }
                    }
                }
            },
            "observation_result": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "source_call_id": optional_string_schema(),
                    "content": optional_string_schema(),
                    "extra": optional_object_schema()
                }
            },
            "final_metrics": {
                "type": "object",
                "additionalProperties": false,
                "required": ["total_steps"],
                "properties": {
                    "total_steps": { "type": "integer", "minimum": 0 },
                    "extra": optional_object_schema()
                }
            }
        }
    })
}

fn step_for_message(
    message: &TimelineMessageEvent,
    source: AtifStepSource,
    content: &str,
    reasoning: Option<&str>,
    paired_tools: Vec<&TimelineToolCallEvent>,
    context: &ProjectionContext,
) -> AtifStep {
    let (tool_calls, observation) = tool_payload(paired_tools, context);
    AtifStep {
        step_id: 0,
        timestamp: valid_timestamp(message.timestamp.as_deref()),
        source,
        message: redact_str(content, context),
        reasoning_content: redact_option(reasoning, context),
        tool_calls,
        observation,
        llm_call_count: None,
        extra: optional_extra([
            (
                "request_id",
                optional_string_value(message.request_id.as_deref()),
            ),
            ("session_id", string_value(&message.session_id)),
            ("message_sequence", Some(json!(message.sequence))),
            ("gents_role", string_value(&message.role)),
        ]),
    }
}

fn step_for_unpaired_tool(tool: &TimelineToolCallEvent, context: &ProjectionContext) -> AtifStep {
    let (tool_calls, observation) = tool_payload(vec![tool], context);
    AtifStep {
        step_id: 0,
        timestamp: valid_timestamp(tool.started_at.as_deref()),
        source: AtifStepSource::Agent,
        message: String::new(),
        reasoning_content: None,
        tool_calls,
        observation,
        llm_call_count: None,
        extra: optional_extra([
            (
                "request_id",
                optional_string_value(tool.request_id.as_deref()),
            ),
            ("session_id", string_value(&tool.session_id)),
            (
                "message_sequence",
                tool.message_sequence.map(|sequence| json!(sequence)),
            ),
            ("synthetic_tool_step", Some(Value::Bool(true))),
        ]),
    }
}

fn tool_payload(
    tools: Vec<&TimelineToolCallEvent>,
    context: &ProjectionContext,
) -> (Option<Vec<AtifToolCall>>, Option<AtifObservation>) {
    if tools.is_empty() {
        return (None, None);
    }
    let tool_calls = tools
        .iter()
        .map(|tool| AtifToolCall {
            tool_call_id: tool.tool_call_id.clone(),
            function_name: tool.tool_name.clone(),
            arguments: atif_arguments(&tool.args, context),
            extra: optional_extra([
                ("status", string_value(&tool.status)),
                (
                    "lifecycle_state",
                    optional_string_value(tool.lifecycle_state.as_deref()),
                ),
                ("started_at", timestamp_value(tool.started_at.as_deref())),
                (
                    "completed_at",
                    timestamp_value(tool.completed_at.as_deref()),
                ),
                (
                    "selected_service_id",
                    optional_string_value(tool.selected_service_id.as_deref()),
                ),
                (
                    "selected_tool_name",
                    optional_string_value(tool.selected_tool_name.as_deref()),
                ),
                (
                    "child_request_id",
                    optional_string_value(tool.child_request_id.as_deref()),
                ),
            ]),
        })
        .collect::<Vec<_>>();
    let results = tools
        .iter()
        .map(|tool| AtifObservationResult {
            source_call_id: Some(tool.tool_call_id.clone()),
            content: Some(redact_str(&tool.result, context)),
            extra: optional_extra([
                ("status", string_value(&tool.status)),
                (
                    "tool_failure_class",
                    optional_string_value(tool.tool_failure_class.as_deref()),
                ),
                (
                    "denial_reason",
                    redact_option(tool.denial_reason.as_deref(), context).map(Value::String),
                ),
                ("latency_ms", tool.latency_ms.map(|latency| json!(latency))),
                (
                    "await_mode",
                    optional_string_value(tool.await_mode.as_deref()),
                ),
                (
                    "cancel_policy",
                    optional_string_value(tool.cancel_policy.as_deref()),
                ),
                (
                    "cancel_cause",
                    redact_option(tool.cancel_cause.as_deref(), context).map(Value::String),
                ),
                (
                    "child_request_id",
                    optional_string_value(tool.child_request_id.as_deref()),
                ),
            ]),
        })
        .collect::<Vec<_>>();
    (Some(tool_calls), Some(AtifObservation { results }))
}

fn apply_root_response(
    steps: &mut Vec<AtifStep>,
    response: &TimelineResponseEvent,
    context: &ProjectionContext,
) {
    let content = redact_option(response.content.as_deref(), context);
    let reasoning = redact_option(response.reasoning.as_deref(), context);
    let matching_agent_step = content.as_deref().and_then(|content| {
        steps
            .iter_mut()
            .rev()
            .find(|step| step.source == AtifStepSource::Agent && step.message == content)
    });
    if let Some(step) = matching_agent_step {
        if step.reasoning_content.is_none() {
            step.reasoning_content = reasoning;
        }
        merge_extra(
            &mut step.extra,
            [
                (
                    "response_status",
                    optional_string_value(response.status.as_deref()),
                ),
                (
                    "response_error",
                    redact_option(response.error_message.as_deref(), context).map(Value::String),
                ),
            ],
        );
        return;
    }

    if content.is_none() && reasoning.is_none() && response.error_message.is_none() {
        return;
    }
    steps.push(AtifStep {
        step_id: 0,
        timestamp: valid_timestamp(response.timestamp.as_deref()),
        source: AtifStepSource::Agent,
        message: content
            .or_else(|| redact_option(response.error_message.as_deref(), context))
            .unwrap_or_default(),
        reasoning_content: reasoning,
        tool_calls: None,
        observation: None,
        llm_call_count: None,
        extra: optional_extra([
            ("request_id", string_value(&response.request_id)),
            (
                "response_status",
                optional_string_value(response.status.as_deref()),
            ),
            (
                "response_error",
                redact_option(response.error_message.as_deref(), context).map(Value::String),
            ),
            (
                "materialized_message_sequence",
                response
                    .materialized_message_sequence
                    .map(|sequence| json!(sequence)),
            ),
        ]),
    });
}

fn paired_tools_for_message<'a>(
    timeline: &'a RunTimeline,
    message: &TimelineMessageEvent,
) -> Vec<&'a TimelineToolCallEvent> {
    timeline
        .events
        .iter()
        .filter_map(|event| match event {
            RunTimelineEvent::ToolCall(tool)
                if root_tool(timeline, tool)
                    && tool.session_id == message.session_id
                    && tool.message_sequence == Some(message.sequence) =>
            {
                Some(tool)
            }
            _ => None,
        })
        .collect()
}

fn tool_has_paired_message(timeline: &RunTimeline, tool: &TimelineToolCallEvent) -> bool {
    timeline.events.iter().any(|event| match event {
        RunTimelineEvent::Message(message)
            if root_message(timeline, message)
                && message.session_id == tool.session_id
                && Some(message.sequence) == tool.message_sequence =>
        {
            projected_message(message).is_some_and(|(source, _, _)| source == AtifStepSource::Agent)
        }
        _ => false,
    })
}

fn projected_message(
    message: &TimelineMessageEvent,
) -> Option<(AtifStepSource, String, Option<String>)> {
    if message.role.eq_ignore_ascii_case("tool") {
        return None;
    }

    let decode_role = if message.role.eq_ignore_ascii_case("agent") {
        "assistant"
    } else {
        message.role.as_str()
    };
    let presented = present_persisted_message(decode_role, &message.content);
    let source = match presented.role {
        PresentedMessageRole::Tool => return None,
        _ if message.role.eq_ignore_ascii_case("system") => AtifStepSource::System,
        PresentedMessageRole::User => AtifStepSource::User,
        PresentedMessageRole::Assistant => AtifStepSource::Agent,
    };
    Some((
        source,
        presented.body_markdown,
        presented.reasoning_markdown,
    ))
}

fn root_message(timeline: &RunTimeline, message: &TimelineMessageEvent) -> bool {
    event_belongs_to_root(
        timeline,
        message.request_id.as_deref(),
        Some(&message.session_id),
    )
}

fn root_tool(timeline: &RunTimeline, tool: &TimelineToolCallEvent) -> bool {
    event_belongs_to_root(timeline, tool.request_id.as_deref(), Some(&tool.session_id))
}

fn event_belongs_to_root(
    timeline: &RunTimeline,
    request_id: Option<&str>,
    session_id: Option<&str>,
) -> bool {
    request_id == Some(timeline.request_id.as_str())
        || (request_id.is_none() && session_id == timeline.session_id.as_deref())
}

fn atif_arguments(raw: &str, context: &ProjectionContext) -> BTreeMap<String, Value> {
    match serde_json::from_str::<Value>(raw) {
        Ok(Value::Object(arguments)) => redact_json_value(Value::Object(arguments), context)
            .as_object()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect(),
        Ok(value) => BTreeMap::from([("_value".to_string(), redact_json_value(value, context))]),
        Err(_) => BTreeMap::from([("_raw".to_string(), Value::String(redact_str(raw, context)))]),
    }
}

fn valid_timestamp(timestamp: Option<&str>) -> Option<String> {
    timestamp
        .filter(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).is_ok())
        .map(ToOwned::to_owned)
}

fn timestamp_value(timestamp: Option<&str>) -> Option<Value> {
    valid_timestamp(timestamp).map(Value::String)
}

fn string_value(value: &str) -> Option<Value> {
    Some(Value::String(value.to_string()))
}

fn optional_string_value(value: Option<&str>) -> Option<Value> {
    value.map(|value| Value::String(value.to_string()))
}

fn optional_extra<const N: usize>(
    entries: [(&str, Option<Value>); N],
) -> Option<BTreeMap<String, Value>> {
    let extra = entries
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key.to_string(), value)))
        .collect::<BTreeMap<_, _>>();
    (!extra.is_empty()).then_some(extra)
}

fn merge_extra<const N: usize>(
    extra: &mut Option<BTreeMap<String, Value>>,
    entries: [(&str, Option<Value>); N],
) {
    let map = extra.get_or_insert_with(BTreeMap::new);
    map.extend(
        entries
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| (key.to_string(), value))),
    );
}

fn optional_string_schema() -> Value {
    json!({
        "anyOf": [
            { "type": "string" },
            { "type": "null" }
        ]
    })
}

fn optional_object_schema() -> Value {
    json!({
        "anyOf": [
            { "type": "object" },
            { "type": "null" }
        ]
    })
}

fn require_nonempty(violations: &mut Vec<String>, path: &str, value: &str) {
    if value.trim().is_empty() {
        violations.push(format!("{path} is required"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter_projection::{ProjectionRedactionMode, ATIF_SCHEMA_VERSION};
    use crate::run_timeline::{
        build_run_timeline, RunTimelineRows, TimelineMessageRow, TimelineRequestRow,
        TimelineResponseRow, TimelineToolCallRow,
    };
    use gents_protocol::message::{
        AssistantContent, Message, Reasoning, ToolCall, ToolFunction, ToolResultContent,
        UserContent,
    };

    fn tool_timeline() -> RunTimeline {
        build_run_timeline(RunTimelineRows {
            request: TimelineRequestRow {
                request_id: "req-atif".to_string(),
                agent_did: Some("did:test:gents".to_string()),
                behavior_id: Some("terminal-bench".to_string()),
                session_id: Some("session-atif".to_string()),
                content: Some("Fix the project.".to_string()),
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                backend_id: Some("d4f".to_string()),
                created_at: Some("2026-07-31T20:00:00Z".to_string()),
                ..TimelineRequestRow::default()
            },
            messages: vec![TimelineMessageRow {
                session_id: "session-atif".to_string(),
                request_id: Some("req-atif".to_string()),
                sequence: 1,
                role: "assistant".to_string(),
                content: "I will inspect it.".to_string(),
                timestamp: Some("2026-07-31T20:00:01Z".to_string()),
                ..TimelineMessageRow::default()
            }],
            tool_calls: vec![TimelineToolCallRow {
                request_id: Some("req-atif".to_string()),
                session_id: "session-atif".to_string(),
                message_sequence: Some(1),
                tool_name: "bash".to_string(),
                tool_call_id: "call-1".to_string(),
                args: r#"{"command":"cargo test"}"#.to_string(),
                result: "ok".to_string(),
                status: "completed".to_string(),
                started_at: Some("2026-07-31T20:00:02Z".to_string()),
                completed_at: Some("2026-07-31T20:00:03Z".to_string()),
                ..TimelineToolCallRow::default()
            }],
            responses: vec![TimelineResponseRow {
                request_id: "req-atif".to_string(),
                session_id: Some("session-atif".to_string()),
                content: Some("Done.".to_string()),
                reasoning: Some("The tests passed.".to_string()),
                status: Some("completed".to_string()),
                completed_at: Some("2026-07-31T20:00:04Z".to_string()),
                ..TimelineResponseRow::default()
            }],
            ..RunTimelineRows::default()
        })
    }

    #[test]
    fn builds_harbor_valid_atif_shape() {
        let timeline = tool_timeline();
        let trajectory = build_atif_trajectory(&timeline, &ProjectionContext::default());

        assert_eq!(trajectory.schema_version, ATIF_SCHEMA_VERSION);
        assert_eq!(trajectory.trajectory_id.as_deref(), Some("req-atif"));
        assert_eq!(trajectory.steps.len(), 3);
        assert_eq!(trajectory.steps[0].source, AtifStepSource::User);
        assert_eq!(trajectory.steps[1].source, AtifStepSource::Agent);
        assert_eq!(
            trajectory.steps[1]
                .tool_calls
                .as_ref()
                .and_then(|calls| calls.first())
                .map(|call| call.function_name.as_str()),
            Some("bash")
        );
        assert_eq!(
            trajectory.steps[1]
                .observation
                .as_ref()
                .and_then(|observation| observation.results.first())
                .and_then(|result| result.source_call_id.as_deref()),
            Some("call-1")
        );
        assert_eq!(
            trajectory.steps[2].reasoning_content.as_deref(),
            Some("The tests passed.")
        );

        let mut violations = Vec::new();
        validate_atif_trajectory(&mut violations, &trajectory);
        assert!(violations.is_empty(), "{violations:#?}");
    }

    #[test]
    fn redaction_keeps_atif_tool_arguments_object_shaped() {
        let timeline = tool_timeline();
        let trajectory = build_atif_trajectory(
            &timeline,
            &ProjectionContext {
                actor_did: None,
                redaction_mode: ProjectionRedactionMode::Public,
            },
        );
        let serialized = serde_json::to_string(&trajectory).unwrap();

        assert!(!serialized.contains("cargo test"));
        assert_eq!(
            trajectory.steps[1]
                .tool_calls
                .as_ref()
                .and_then(|calls| calls.first())
                .and_then(|call| call.arguments.get("command"))
                .and_then(Value::as_str),
            Some("[redacted]")
        );
    }

    #[test]
    fn decodes_persisted_messages_without_duplicate_prompt_tool_or_synthetic_steps() {
        let assistant = Message::Assistant {
            id: None,
            content: vec![
                AssistantContent::Reasoning(Reasoning::new("Inspect before changing it.")),
                AssistantContent::text("I will inspect it."),
                AssistantContent::ToolCall(ToolCall::new(
                    "call-1".to_string(),
                    ToolFunction::new("bash".to_string(), json!({"command": "cargo test"})),
                )),
            ],
        };
        let tool_result = Message::User {
            content: vec![UserContent::tool_result(
                "call-1",
                vec![ToolResultContent::text("ok")],
            )],
        };
        let timeline = build_run_timeline(RunTimelineRows {
            request: TimelineRequestRow {
                request_id: "req-decoded".to_string(),
                session_id: Some("session-decoded".to_string()),
                content: Some("Fix the project.".to_string()),
                status: Some("completed".to_string()),
                lifecycle_state: Some("completed".to_string()),
                created_at: Some("2026-07-31T20:00:00Z".to_string()),
                ..TimelineRequestRow::default()
            },
            messages: vec![
                TimelineMessageRow {
                    session_id: "session-decoded".to_string(),
                    request_id: Some("req-decoded".to_string()),
                    sequence: 0,
                    role: "user".to_string(),
                    content: serde_json::to_string(&Message::user("Fix the project.")).unwrap(),
                    timestamp: Some("2026-07-31T20:00:01Z".to_string()),
                    ..TimelineMessageRow::default()
                },
                TimelineMessageRow {
                    session_id: "session-decoded".to_string(),
                    request_id: Some("req-decoded".to_string()),
                    sequence: 1,
                    role: "assistant".to_string(),
                    content: serde_json::to_string(&assistant).unwrap(),
                    // Persisted message timestamps can trail tool start time. The
                    // projection must still pair the call instead of emitting a
                    // synthetic tool step first.
                    timestamp: Some("2026-07-31T20:00:04Z".to_string()),
                    ..TimelineMessageRow::default()
                },
                TimelineMessageRow {
                    session_id: "session-decoded".to_string(),
                    request_id: Some("req-decoded".to_string()),
                    sequence: 2,
                    role: "user".to_string(),
                    content: serde_json::to_string(&tool_result).unwrap(),
                    timestamp: Some("2026-07-31T20:00:05Z".to_string()),
                    ..TimelineMessageRow::default()
                },
            ],
            tool_calls: vec![TimelineToolCallRow {
                request_id: Some("req-decoded".to_string()),
                session_id: "session-decoded".to_string(),
                message_sequence: Some(1),
                tool_name: "bash".to_string(),
                tool_call_id: "call-1".to_string(),
                args: r#"{"command":"cargo test"}"#.to_string(),
                result: "ok".to_string(),
                status: "completed".to_string(),
                started_at: Some("2026-07-31T20:00:02Z".to_string()),
                completed_at: Some("2026-07-31T20:00:03Z".to_string()),
                ..TimelineToolCallRow::default()
            }],
            responses: vec![TimelineResponseRow {
                request_id: "req-decoded".to_string(),
                session_id: Some("session-decoded".to_string()),
                content: Some("Done.".to_string()),
                status: Some("completed".to_string()),
                completed_at: Some("2026-07-31T20:00:06Z".to_string()),
                ..TimelineResponseRow::default()
            }],
            ..RunTimelineRows::default()
        });

        let trajectory = build_atif_trajectory(&timeline, &ProjectionContext::default());

        assert_eq!(trajectory.steps.len(), 3);
        assert_eq!(trajectory.steps[0].message, "Fix the project.");
        assert_eq!(trajectory.steps[1].message, "I will inspect it.");
        assert_eq!(
            trajectory.steps[1].reasoning_content.as_deref(),
            Some("Inspect before changing it.")
        );
        assert_eq!(
            trajectory.steps[1]
                .tool_calls
                .as_deref()
                .map(|calls| calls.len()),
            Some(1)
        );
        assert_eq!(trajectory.steps[2].message, "Done.");
        assert!(trajectory.steps.iter().all(|step| {
            step.extra
                .as_ref()
                .and_then(|extra| extra.get("synthetic_tool_step"))
                .is_none()
        }));
    }
}
