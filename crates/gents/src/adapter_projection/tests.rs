use super::*;
use std::collections::BTreeSet;

use crate::run_timeline::{
    build_run_timeline, RunTimelineRows, TimelineInferenceCallRow, TimelineMessageRow,
    TimelineRenderedRequestRow, TimelineRequestRow, TimelineResponseRow, TimelineToolCallRow,
};

const BODY_SENTINEL: &str = "SENTINEL_RENDERED_BODY_9f3a";

/// A timeline whose capture rows carry the sentinel in the one place a body
/// realistically travels — `assembly_trace.effective_messages` inside
/// `provenance_json`. Built through `build_run_timeline`, not hand-crafted
/// events, so the test fences the whole rows→events→projection pipeline.
fn timeline_with_captures() -> crate::run_timeline::RunTimeline {
    let provenance = serde_json::json!({
        "manifest_version": 3,
        "status": "captured_only",
        "status_reason": "fixture",
        "capture_seam": "transport_body",
        "capture_scope": "inference.1",
        "admission": { "call_id": "call-1", "call_seq": 1 },
        "assembly_trace": {
            "trace_version": 2,
            "build_path": "budgeted",
            "effective_message_count": 1,
            "effective_messages": [
                { "role": "user", "content": [{ "type": "text", "text": BODY_SENTINEL }] }
            ],
            "assistant_message_ids": [],
            "threaded_tool_results": []
        }
    })
    .to_string();

    let capture = |capture_key: &str, attempt: i64, created_at: &str, provenance: String| {
        TimelineRenderedRequestRow {
            capture_key: capture_key.to_string(),
            request_doc_id: Some("doc-req-1".to_string()),
            request_id: Some("req-1".to_string()),
            session_id: Some("session-1".to_string()),
            capture_scope: Some("inference.1".to_string()),
            turn_index: Some(0),
            attempt: Some(attempt),
            capture_version: Some(1),
            model_name: Some("test-model".to_string()),
            source: Some("openai_chat_completions".to_string()),
            prompt_hash: Some("aa".to_string()),
            tools_hash: Some("bb".to_string()),
            provenance_json: Some(provenance),
            created_at: Some(created_at.to_string()),
            ..Default::default()
        }
    };

    build_run_timeline(RunTimelineRows {
        request: TimelineRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:amy".to_string()),
            behavior_id: Some("amy".to_string()),
            session_id: Some("session-1".to_string()),
            content: Some("hello".to_string()),
            status: Some("completed".to_string()),
            created_at: Some("2026-08-07T12:00:00Z".to_string()),
            ..Default::default()
        },
        inference_calls: vec![TimelineInferenceCallRow {
            call_id: "call-1".to_string(),
            request_id: "req-1".to_string(),
            call_seq: 1,
            attempt: 1,
            call_state: "completed".to_string(),
            call_kind: "inference".to_string(),
            queued_at: Some("2026-08-07T12:00:01Z".to_string()),
            ..Default::default()
        }],
        rendered_requests: vec![
            capture("rendered:v1:one", 0, "2026-08-07T12:00:02Z", provenance),
            capture(
                "rendered:v1:two",
                1,
                "2026-08-07T12:00:03Z",
                format!(r#"{{"manifest_version":99,"body":{body:?}}}"#, body = BODY_SENTINEL),
            ),
        ],
        ..Default::default()
    })
}

/// Capture metadata surfaces in every projection's envelope; captured bodies
/// never appear in ANY serialized output, in any redaction mode. This is the
/// positive default #1066 demands — Harbor invokes `trace project` with
/// neither `--redaction` nor `--actor-did`, so the exclusion cannot be a
/// caller responsibility.
#[test]
fn rendered_captures_surface_as_metadata_and_bodies_never_leak() {
    let timeline = timeline_with_captures();
    for kind in [
        AdapterProjectionKind::AtifTrajectory,
        AdapterProjectionKind::OpenAiCodexRunTrace,
        AdapterProjectionKind::LangGraphStateHistory,
        AdapterProjectionKind::MultiAgentTask,
    ] {
        for mode in [
            ProjectionRedactionMode::Full,
            ProjectionRedactionMode::TrainingSafe,
            ProjectionRedactionMode::Public,
        ] {
            let envelope = build_adapter_projection(
                kind,
                &timeline,
                &ProjectionContext {
                    actor_did: None,
                    redaction_mode: mode,
                },
            );
            let serialized = serde_json::to_string(&envelope).unwrap();
            assert!(
                !serialized.contains(BODY_SENTINEL),
                "{kind:?}/{mode:?} leaked a captured body"
            );
            assert_eq!(
                envelope.rendered_captures.len(),
                2,
                "{kind:?}/{mode:?} lost capture metadata"
            );
            assert_eq!(
                envelope.rendered_captures[0].call_seq,
                Some(1),
                "{kind:?}/{mode:?} lost the admission join"
            );
            assert_eq!(
                envelope.rendered_captures[1].provenance_status,
                "unsupported_manifest"
            );
            assert_adapter_projection_matches_json_schema(&envelope);
        }
    }
}

/// The open extension surfaces carry the same metadata: ATIF at trajectory
/// `extra.rendered_captures`, LangGraph in `values.rendered_captures`.
#[test]
fn open_extension_surfaces_carry_capture_metadata() {
    let timeline = timeline_with_captures();
    let context = ProjectionContext::default();

    let atif = build_adapter_projection(
        AdapterProjectionKind::AtifTrajectory,
        &timeline,
        &context,
    );
    let AdapterProjection::AtifTrajectory(trajectory) = &atif.output else {
        panic!("expected ATIF output");
    };
    let captures = trajectory
        .extra
        .as_ref()
        .and_then(|extra| extra.get("rendered_captures"))
        .and_then(Value::as_array)
        .expect("ATIF trajectory extra.rendered_captures");
    assert_eq!(captures.len(), 2);
    assert_eq!(captures[0]["capture_key"], "rendered:v1:one");

    let langgraph = build_adapter_projection(
        AdapterProjectionKind::LangGraphStateHistory,
        &timeline,
        &context,
    );
    let AdapterProjection::LangGraphStateHistory(projection) = &langgraph.output else {
        panic!("expected LangGraph output");
    };
    let captures = projection
        .values
        .get("rendered_captures")
        .and_then(Value::as_array)
        .expect("LangGraph values.rendered_captures");
    assert_eq!(captures.len(), 2);
    assert_eq!(captures[1]["provenance_status"], "unsupported_manifest");
}

fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .map(std::path::Path::to_path_buf)
        .expect("workspace root")
}

fn read_adapter_projection_fixture(fixture_name: &str) -> (AdapterProjectionEnvelope, Value) {
    let path = workspace_root().join(format!(
        "crates/gents/tests/fixtures/adapter_projections/envelopes/{fixture_name}.envelope.json"
    ));
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()));
    let value = serde_json::from_str::<Value>(&raw)
        .unwrap_or_else(|error| panic!("parsing {} as JSON: {error}", path.display()));
    let envelope = serde_json::from_value::<AdapterProjectionEnvelope>(value.clone())
        .unwrap_or_else(|error| panic!("deserializing {}: {error}", path.display()));
    (envelope, value)
}

fn assert_json_schema_valid(schema: &Value, instance: &Value, label: &str) {
    let validator = jsonschema::validator_for(schema)
        .unwrap_or_else(|error| panic!("{label} schema failed to compile: {error}"));
    let errors = validator
        .iter_errors(instance)
        .map(|error| format!("{}: {error}", error.instance_path()))
        .collect::<Vec<_>>();
    assert!(
        errors.is_empty(),
        "{label} failed JSON Schema validation:\n{}",
        errors.join("\n")
    );
}

fn assert_adapter_projection_matches_json_schema(envelope: &AdapterProjectionEnvelope) {
    let kind = envelope.output.kind();
    let envelope_value = serde_json::to_value(envelope).unwrap();
    assert_json_schema_valid(
        &adapter_projection_json_schema(kind),
        &envelope_value,
        kind.id(),
    );

    let jsonl_record_schema = adapter_projection_jsonl_record_schema(kind);
    for record in adapter_projection_jsonl_records(envelope) {
        let record_value = serde_json::to_value(&record).unwrap();
        assert_json_schema_valid(
            &jsonl_record_schema,
            &record_value,
            &format!("{} JSONL record {}", kind.id(), record.record_id),
        );
    }

    let eval_jsonl_record_schema = adapter_projection_eval_jsonl_record_schema(kind);
    for record in adapter_projection_eval_jsonl_records(envelope) {
        let record_value = serde_json::to_value(&record).unwrap();
        assert_json_schema_valid(
            &eval_jsonl_record_schema,
            &record_value,
            &format!("{} eval JSONL record {}", kind.id(), record.record_id),
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProjectionParticipant {
    agent_did: Option<String>,
    behavior_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProjectionDelegation {
    parent_request_id: String,
    child_request_id: String,
    parent_tool_call_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProjectionToolCall {
    tool_call_id: String,
    tool_name: String,
    status: String,
}

fn delegated_coherence_timeline() -> RunTimeline {
    build_run_timeline(RunTimelineRows {
        request: TimelineRequestRow {
            request_id: "req-root".to_string(),
            agent_did: Some("did:test:coordinator".to_string()),
            behavior_id: Some("coordinator".to_string()),
            session_id: Some("session-root".to_string()),
            content: Some("root private objective".to_string()),
            status: Some("completed".to_string()),
            lifecycle_state: Some("completed".to_string()),
            created_at: Some("2026-06-05T00:00:00Z".to_string()),
            ..TimelineRequestRow::default()
        },
        requests: vec![TimelineRequestRow {
            request_id: "req-review".to_string(),
            agent_did: Some("did:test:reviewer".to_string()),
            behavior_id: Some("reviewer".to_string()),
            session_id: Some("session-review".to_string()),
            status: Some("completed".to_string()),
            lifecycle_state: Some("completed".to_string()),
            caused_by_parent_request_id: Some("req-root".to_string()),
            caused_by_parent_tool_call_id: Some("call-delegate".to_string()),
            created_at: Some("2026-06-05T00:00:03Z".to_string()),
            ..TimelineRequestRow::default()
        }],
        messages: vec![
            TimelineMessageRow {
                doc_id: None,
                session_id: "session-root".to_string(),
                request_id: Some("req-root".to_string()),
                sequence: 1,
                role: "assistant".to_string(),
                content: "root private assistant note".to_string(),
                timestamp: Some("2026-06-05T00:00:01Z".to_string()),
            },
            TimelineMessageRow {
                doc_id: None,
                session_id: "session-review".to_string(),
                request_id: Some("req-review".to_string()),
                sequence: 1,
                role: "assistant".to_string(),
                content: "child private assistant note".to_string(),
                timestamp: Some("2026-06-05T00:00:03.100Z".to_string()),
            },
        ],
        tool_calls: vec![
            TimelineToolCallRow {
                request_id: Some("req-root".to_string()),
                session_id: "session-root".to_string(),
                message_sequence: Some(1),
                tool_name: "delegate".to_string(),
                tool_call_id: "call-delegate".to_string(),
                args: r#"{"prompt":"delegate private args"}"#.to_string(),
                result: r#"{"summary":"delegate private result"}"#.to_string(),
                status: "completed".to_string(),
                child_request_id: Some("req-review".to_string()),
                started_at: Some("2026-06-05T00:00:02Z".to_string()),
                completed_at: Some("2026-06-05T00:00:03Z".to_string()),
                ..TimelineToolCallRow::default()
            },
            TimelineToolCallRow {
                request_id: Some("req-review".to_string()),
                session_id: "session-review".to_string(),
                message_sequence: Some(1),
                tool_name: "bash".to_string(),
                tool_call_id: "call-review-check".to_string(),
                args: r#"{"cmd":"child private args"}"#.to_string(),
                result: "child private result".to_string(),
                status: "denied".to_string(),
                denial_reason: Some("child private denial reason".to_string()),
                selected_service_id: Some("native-shell".to_string()),
                selected_tool_name: Some("bash".to_string()),
                started_at: Some("2026-06-05T00:00:03.200Z".to_string()),
                completed_at: Some("2026-06-05T00:00:03.300Z".to_string()),
                ..TimelineToolCallRow::default()
            },
        ],
        responses: vec![
            TimelineResponseRow {
                request_id: "req-review".to_string(),
                session_id: Some("session-review".to_string()),
                content: Some("child private final".to_string()),
                reasoning: Some("child private reasoning".to_string()),
                status: Some("completed".to_string()),
                completed_at: Some("2026-06-05T00:00:03.500Z".to_string()),
                ..TimelineResponseRow::default()
            },
            TimelineResponseRow {
                request_id: "req-root".to_string(),
                session_id: Some("session-root".to_string()),
                content: Some("root private final".to_string()),
                reasoning: Some("root private reasoning".to_string()),
                status: Some("completed".to_string()),
                completed_at: Some("2026-06-05T00:00:04Z".to_string()),
                ..TimelineResponseRow::default()
            },
        ],
        ..RunTimelineRows::default()
    })
}

fn build_all_adapter_projections(
    timeline: &RunTimeline,
    redaction_mode: ProjectionRedactionMode,
) -> Vec<AdapterProjectionEnvelope> {
    let context = ProjectionContext {
        actor_did: Some("did:test:projection-reader".to_string()),
        redaction_mode,
    };
    [
        AdapterProjectionKind::OpenAiCodexRunTrace,
        AdapterProjectionKind::LangGraphStateHistory,
        AdapterProjectionKind::MultiAgentTask,
    ]
    .into_iter()
    .map(|kind| build_adapter_projection(kind, timeline, &context))
    .collect()
}

fn projection_participants(
    envelope: &AdapterProjectionEnvelope,
) -> BTreeSet<ProjectionParticipant> {
    match &envelope.output {
        AdapterProjection::AtifTrajectory(projection) => participant(
            projection
                .agent
                .extra
                .as_ref()
                .and_then(|extra| extra.get("agent_did"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            projection
                .agent
                .extra
                .as_ref()
                .and_then(|extra| extra.get("behavior_id"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        )
        .into_iter()
        .collect(),
        AdapterProjection::OpenAiCodexRunTrace(projection) => projection
            .items
            .iter()
            .filter_map(|item| match item {
                OpenAiCodexTraceItem::Request {
                    agent_did,
                    behavior_id,
                    ..
                } => participant(agent_did.clone(), behavior_id.clone()),
                _ => None,
            })
            .collect(),
        AdapterProjection::LangGraphStateHistory(projection) => projection
            .nodes
            .iter()
            .filter(|node| node.kind == "request")
            .filter_map(|node| participant(node.agent_did.clone(), node.behavior_id.clone()))
            .collect(),
        AdapterProjection::MultiAgentTask(projection) => projection
            .participants
            .iter()
            .filter_map(|participant| {
                self::participant(
                    participant.agent_did.clone(),
                    participant.behavior_id.clone(),
                )
            })
            .collect(),
    }
}

fn participant(
    agent_did: Option<String>,
    behavior_id: Option<String>,
) -> Option<ProjectionParticipant> {
    if agent_did.is_none() && behavior_id.is_none() {
        return None;
    }
    Some(ProjectionParticipant {
        agent_did,
        behavior_id,
    })
}

fn projection_delegations(envelope: &AdapterProjectionEnvelope) -> BTreeSet<ProjectionDelegation> {
    match &envelope.output {
        AdapterProjection::AtifTrajectory(projection) => projection
            .steps
            .iter()
            .flat_map(|step| {
                step.tool_calls
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(move |tool| (step, tool))
            })
            .filter_map(|(step, tool)| {
                let extra = tool.extra.as_ref()?;
                Some(ProjectionDelegation {
                    parent_request_id: step
                        .extra
                        .as_ref()
                        .and_then(|extra| extra.get("request_id"))
                        .and_then(Value::as_str)?
                        .to_string(),
                    child_request_id: extra.get("child_request_id")?.as_str()?.to_string(),
                    parent_tool_call_id: Some(tool.tool_call_id.clone()),
                })
            })
            .collect(),
        AdapterProjection::OpenAiCodexRunTrace(projection) => projection
            .items
            .iter()
            .filter_map(|item| match item {
                OpenAiCodexTraceItem::ToolCall {
                    id,
                    request_id,
                    child_run_id,
                    ..
                } => Some(ProjectionDelegation {
                    parent_request_id: request_id.clone()?,
                    child_request_id: child_run_id.clone()?,
                    parent_tool_call_id: Some(id.clone()),
                }),
                _ => None,
            })
            .collect(),
        AdapterProjection::LangGraphStateHistory(projection) => projection
            .tasks
            .iter()
            .filter_map(|task| {
                Some(ProjectionDelegation {
                    parent_request_id: task.request_id.clone()?,
                    child_request_id: task.child_request_id.clone()?,
                    parent_tool_call_id: Some(task.id.clone()),
                })
            })
            .collect(),
        AdapterProjection::MultiAgentTask(projection) => projection
            .delegations
            .iter()
            .map(|delegation| ProjectionDelegation {
                parent_request_id: delegation.parent_request_id.clone(),
                child_request_id: delegation.child_request_id.clone(),
                parent_tool_call_id: delegation.parent_tool_call_id.clone(),
            })
            .collect(),
    }
}

fn projection_tool_calls(envelope: &AdapterProjectionEnvelope) -> BTreeSet<ProjectionToolCall> {
    match &envelope.output {
        AdapterProjection::AtifTrajectory(projection) => projection
            .steps
            .iter()
            .flat_map(|step| step.tool_calls.as_deref().unwrap_or_default())
            .map(|tool| ProjectionToolCall {
                tool_call_id: tool.tool_call_id.clone(),
                tool_name: tool.function_name.clone(),
                status: tool
                    .extra
                    .as_ref()
                    .and_then(|extra| extra.get("status"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            })
            .collect(),
        AdapterProjection::OpenAiCodexRunTrace(projection) => projection
            .items
            .iter()
            .filter_map(|item| match item {
                OpenAiCodexTraceItem::ToolCall {
                    id, name, status, ..
                } => Some(ProjectionToolCall {
                    tool_call_id: id.clone(),
                    tool_name: name.clone(),
                    status: status.clone(),
                }),
                _ => None,
            })
            .collect(),
        AdapterProjection::LangGraphStateHistory(projection) => projection
            .tasks
            .iter()
            .map(|task| ProjectionToolCall {
                tool_call_id: task.id.clone(),
                tool_name: task.name.clone(),
                status: task.status.clone(),
            })
            .collect(),
        AdapterProjection::MultiAgentTask(projection) => projection
            .tool_events
            .iter()
            .map(|event| ProjectionToolCall {
                tool_call_id: event.id.clone(),
                tool_name: event.tool_name.clone(),
                status: event.status.clone(),
            })
            .collect(),
    }
}

fn projection_terminal_status(envelope: &AdapterProjectionEnvelope) -> Option<String> {
    match &envelope.output {
        AdapterProjection::AtifTrajectory(projection) => projection
            .final_metrics
            .as_ref()
            .and_then(|metrics| metrics.extra.as_ref())
            .and_then(|extra| extra.get("lifecycle_state").or_else(|| extra.get("status")))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        AdapterProjection::OpenAiCodexRunTrace(projection) => projection.status.clone(),
        AdapterProjection::LangGraphStateHistory(projection) => projection
            .values
            .get("lifecycle_state")
            .and_then(Value::as_str)
            .or_else(|| projection.values.get("status").and_then(Value::as_str))
            .map(ToOwned::to_owned),
        AdapterProjection::MultiAgentTask(projection) => projection.status.clone(),
    }
}

#[test]
fn adapter_projections_are_coherent_for_delegated_timeline() {
    let timeline = delegated_coherence_timeline();
    let full = build_all_adapter_projections(&timeline, ProjectionRedactionMode::Full);
    let expected_participants = BTreeSet::from([
        ProjectionParticipant {
            agent_did: Some("did:test:coordinator".to_string()),
            behavior_id: Some("coordinator".to_string()),
        },
        ProjectionParticipant {
            agent_did: Some("did:test:reviewer".to_string()),
            behavior_id: Some("reviewer".to_string()),
        },
    ]);
    let expected_delegations = BTreeSet::from([ProjectionDelegation {
        parent_request_id: "req-root".to_string(),
        child_request_id: "req-review".to_string(),
        parent_tool_call_id: Some("call-delegate".to_string()),
    }]);
    let expected_tool_calls = BTreeSet::from([
        ProjectionToolCall {
            tool_call_id: "call-delegate".to_string(),
            tool_name: "delegate".to_string(),
            status: "completed".to_string(),
        },
        ProjectionToolCall {
            tool_call_id: "call-review-check".to_string(),
            tool_name: "bash".to_string(),
            status: "denied".to_string(),
        },
    ]);

    for envelope in &full {
        validate_adapter_projection_contract(envelope).unwrap();
        assert_adapter_projection_matches_json_schema(envelope);
        assert_eq!(
            projection_participants(envelope),
            expected_participants,
            "{} participant identities drifted from the shared timeline",
            envelope.projection_id
        );
        assert_eq!(
            projection_delegations(envelope),
            expected_delegations,
            "{} delegation shape drifted from the shared timeline",
            envelope.projection_id
        );
        assert_eq!(
            projection_tool_calls(envelope),
            expected_tool_calls,
            "{} tool calls drifted from the shared timeline",
            envelope.projection_id
        );
        assert_eq!(
            projection_terminal_status(envelope).as_deref(),
            Some("completed"),
            "{} terminal status drifted from the shared timeline",
            envelope.projection_id
        );
    }

    let sensitive_literals = [
        "root private objective",
        "root private assistant note",
        "delegate private args",
        "delegate private result",
        "child private assistant note",
        "child private args",
        "child private result",
        "child private denial reason",
        "child private final",
        "child private reasoning",
        "root private final",
        "root private reasoning",
    ];
    let full_serialized = serde_json::to_string(&full).unwrap();
    for literal in sensitive_literals {
        assert!(
            full_serialized.contains(literal),
            "full projections should retain sensitive literal {literal:?}"
        );
    }

    for (mode, marker) in [
        (
            ProjectionRedactionMode::TrainingSafe,
            "[training_safe_redacted]",
        ),
        (ProjectionRedactionMode::Public, "[redacted]"),
    ] {
        let redacted = build_all_adapter_projections(&timeline, mode);
        for envelope in &redacted {
            validate_adapter_projection_contract(envelope).unwrap();
            assert_adapter_projection_matches_json_schema(envelope);
            assert_eq!(
                projection_participants(envelope),
                expected_participants,
                "{} participant identities changed under {mode:?} redaction",
                envelope.projection_id
            );
            assert_eq!(
                projection_delegations(envelope),
                expected_delegations,
                "{} delegation shape changed under {mode:?} redaction",
                envelope.projection_id
            );
            assert_eq!(
                projection_tool_calls(envelope),
                expected_tool_calls,
                "{} tool calls changed under {mode:?} redaction",
                envelope.projection_id
            );
            assert_eq!(
                projection_terminal_status(envelope).as_deref(),
                Some("completed"),
                "{} terminal status changed under {mode:?} redaction",
                envelope.projection_id
            );
        }

        let serialized = serde_json::to_string(&redacted).unwrap();
        assert!(
            serialized.contains(marker),
            "{mode:?} projections should carry redaction markers"
        );
        for literal in sensitive_literals {
            assert!(
                !serialized.contains(literal),
                "{mode:?} projections leaked sensitive literal {literal:?}"
            );
        }
    }
}

#[test]
fn atif_projection_emits_a_schema_valid_native_harbor_document() {
    let timeline = delegated_coherence_timeline();
    let envelope = build_adapter_projection(
        AdapterProjectionKind::AtifTrajectory,
        &timeline,
        &ProjectionContext::default(),
    );

    validate_adapter_projection_contract(&envelope).unwrap();
    assert_adapter_projection_matches_json_schema(&envelope);

    let native = adapter_projection_native_json(&envelope);
    assert_eq!(
        native.get("schema_version").and_then(Value::as_str),
        Some(ATIF_SCHEMA_VERSION)
    );
    assert!(native.get("projection_id").is_none());
    assert_json_schema_valid(
        &adapter_projection_native_json_schema(AdapterProjectionKind::AtifTrajectory),
        &native,
        "ATIF native JSON",
    );
}

#[test]
fn builds_three_adapter_shapes_from_one_timeline_with_redaction() {
    let timeline = build_run_timeline(RunTimelineRows {
        request: TimelineRequestRow {
            request_id: "req-1".to_string(),
            agent_did: Some("did:test:root".to_string()),
            behavior_id: Some("root".to_string()),
            session_id: Some("session-1".to_string()),
            content: Some("sensitive prompt".to_string()),
            status: Some("completed".to_string()),
            lifecycle_state: Some("completed".to_string()),
            created_at: Some("2026-06-05T00:00:00Z".to_string()),
            ..TimelineRequestRow::default()
        },
        requests: vec![TimelineRequestRow {
            request_id: "child-1".to_string(),
            agent_did: Some("did:test:child".to_string()),
            behavior_id: Some("child".to_string()),
            session_id: Some("session-1".to_string()),
            status: Some("completed".to_string()),
            lifecycle_state: Some("completed".to_string()),
            caused_by_parent_request_id: Some("req-1".to_string()),
            caused_by_parent_tool_call_id: Some("call-child".to_string()),
            created_at: Some("2026-06-05T00:00:03Z".to_string()),
            ..TimelineRequestRow::default()
        }],
        messages: vec![TimelineMessageRow {
            doc_id: None,
            session_id: "session-1".to_string(),
            request_id: Some("req-1".to_string()),
            sequence: 1,
            role: "assistant".to_string(),
            content: "sensitive assistant text".to_string(),
            timestamp: Some("2026-06-05T00:00:01Z".to_string()),
        }],
        tool_calls: vec![TimelineToolCallRow {
            request_id: Some("req-1".to_string()),
            session_id: "session-1".to_string(),
            message_sequence: Some(1),
            tool_name: "delegate".to_string(),
            tool_call_id: "call-child".to_string(),
            args: "{\"prompt\":\"secret\"}".to_string(),
            result: "{\"ok\":true}".to_string(),
            status: "completed".to_string(),
            child_request_id: Some("child-1".to_string()),
            started_at: Some("2026-06-05T00:00:02Z".to_string()),
            completed_at: Some("2026-06-05T00:00:03Z".to_string()),
            ..TimelineToolCallRow::default()
        }],
        responses: vec![TimelineResponseRow {
            request_id: "req-1".to_string(),
            session_id: Some("session-1".to_string()),
            content: Some("sensitive final".to_string()),
            status: Some("completed".to_string()),
            completed_at: Some("2026-06-05T00:00:04Z".to_string()),
            ..TimelineResponseRow::default()
        }],
        ..RunTimelineRows::default()
    });
    let context = ProjectionContext {
        actor_did: Some("did:test:viewer".to_string()),
        redaction_mode: ProjectionRedactionMode::Public,
    };

    let codex = build_adapter_projection(
        AdapterProjectionKind::OpenAiCodexRunTrace,
        &timeline,
        &context,
    );
    let langgraph = build_adapter_projection(
        AdapterProjectionKind::LangGraphStateHistory,
        &timeline,
        &context,
    );
    let multi =
        build_adapter_projection(AdapterProjectionKind::MultiAgentTask, &timeline, &context);

    for kind in [
        AdapterProjectionKind::OpenAiCodexRunTrace,
        AdapterProjectionKind::LangGraphStateHistory,
        AdapterProjectionKind::MultiAgentTask,
    ] {
        let envelope_schema = adapter_projection_json_schema(kind);
        assert_eq!(
            envelope_schema
                .pointer("/properties/projection_id/const")
                .and_then(Value::as_str),
            Some(kind.id())
        );
        assert_eq!(
            envelope_schema
                .pointer("/properties/output/properties/adapter/const")
                .and_then(Value::as_str),
            Some(kind.id())
        );

        let jsonl_schema = adapter_projection_jsonl_record_schema(kind);
        assert_eq!(
            jsonl_schema
                .pointer("/properties/projection_id/const")
                .and_then(Value::as_str),
            Some(kind.id())
        );
        assert!(jsonl_schema.pointer("/properties/record_kind").is_some());
    }
    let schema_index = adapter_projection_schema_index();
    assert_eq!(
        schema_index
            .get("schemas")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(4)
    );

    assert_eq!(codex.projection_id, "openai_codex_run_trace");
    assert_eq!(langgraph.projection_id, "langgraph_state_history");
    assert_eq!(multi.projection_id, "multi_agent_task");
    validate_adapter_projection_contract(&codex).unwrap();
    validate_adapter_projection_contract(&langgraph).unwrap();
    validate_adapter_projection_contract(&multi).unwrap();
    assert_adapter_projection_matches_json_schema(&codex);
    assert_adapter_projection_matches_json_schema(&langgraph);
    assert_adapter_projection_matches_json_schema(&multi);

    let codex_records = adapter_projection_jsonl_records(&codex);
    assert!(!codex_records.is_empty());
    assert_eq!(codex_records[0].projection_id, "openai_codex_run_trace");
    assert_eq!(codex_records[0].source_request_id, "req-1");
    assert_eq!(codex_records[0].record_kind, "openai_codex_trace_item");

    let mut invalid = multi.clone();
    invalid.projection_id.clear();
    let error = validate_adapter_projection_contract(&invalid).unwrap_err();
    assert!(error
        .violations
        .iter()
        .any(|violation| violation == "projection_id is required"));

    assert!(!serde_json::to_string(&codex)
        .unwrap()
        .contains("sensitive prompt"));
    assert!(serde_json::to_string(&langgraph)
        .unwrap()
        .contains("child_request"));
    assert!(serde_json::to_string(&multi)
        .unwrap()
        .contains("\"role\":\"delegate\""));
}

/// Drift guard, not a behavior test: the checked-in envelope fixtures
/// were generated by this serializer, so the round-trip equality below is
/// tautological today. Its job is to fail loudly when a DTO/serde change
/// would silently alter the wire format external consumers parse.
#[test]
fn external_contract_fixtures_validate_without_runtime_dependencies() {
    let cases: &[(AdapterProjectionKind, &str, &[&str])] = &[
        (
            AdapterProjectionKind::AtifTrajectory,
            "atif_trajectory",
            &["atif_agent", "atif_step", "atif_final_metrics"],
        ),
        (
            AdapterProjectionKind::OpenAiCodexRunTrace,
            "openai_codex_run_trace",
            &["openai_codex_trace_item"],
        ),
        (
            AdapterProjectionKind::LangGraphStateHistory,
            "langgraph_state_history",
            &[
                "langgraph_values",
                "langgraph_node",
                "langgraph_edge",
                "langgraph_task",
            ],
        ),
        (
            AdapterProjectionKind::MultiAgentTask,
            "multi_agent_task",
            &[
                "multi_agent_participant",
                "multi_agent_message",
                "multi_agent_delegation",
                "multi_agent_tool_event",
            ],
        ),
    ];

    for (kind, fixture_name, expected_record_kinds) in cases {
        let (envelope, fixture_value) = read_adapter_projection_fixture(fixture_name);
        assert_eq!(envelope.projection_id, kind.id());
        assert_eq!(envelope.output.kind(), *kind);
        validate_adapter_projection_contract(&envelope)
            .unwrap_or_else(|error| panic!("{fixture_name} failed contract: {error}"));
        assert_json_schema_valid(
            &adapter_projection_json_schema(*kind),
            &fixture_value,
            fixture_name,
        );

        let round_trip = serde_json::to_value(&envelope).unwrap();
        assert_eq!(
            round_trip, fixture_value,
            "{fixture_name} fixture drifted from adapter DTO serialization"
        );

        let allowed_record_kinds = adapter_projection_jsonl_record_schema(*kind)
            .pointer("/properties/record_kind/enum")
            .and_then(Value::as_array)
            .expect("JSONL record kind enum")
            .iter()
            .map(|value| value.as_str().expect("string record kind").to_string())
            .collect::<std::collections::BTreeSet<_>>();
        let records = adapter_projection_jsonl_records(&envelope);
        assert!(
            !records.is_empty(),
            "{fixture_name} fixture should produce JSONL records"
        );

        let mut observed_record_kinds = std::collections::BTreeSet::new();
        for (index, record) in records.iter().enumerate() {
            assert_eq!(record.projection_id, kind.id());
            assert_eq!(record.projection_version, ADAPTER_PROJECTION_VERSION);
            assert_eq!(record.source_request_id, envelope.source_request_id);
            assert_eq!(record.record_index, index);
            assert!(
                record.value.is_object(),
                "{fixture_name} JSONL value must be an object: {record:#?}"
            );
            assert!(
                allowed_record_kinds.contains(&record.record_kind),
                "{fixture_name} produced unsupported JSONL record kind {}",
                record.record_kind
            );
            assert_json_schema_valid(
                &adapter_projection_jsonl_record_schema(*kind),
                &serde_json::to_value(record).unwrap(),
                &format!("{fixture_name} JSONL record {}", record.record_id),
            );
            observed_record_kinds.insert(record.record_kind.clone());
        }
        for expected in *expected_record_kinds {
            assert!(
                observed_record_kinds.contains(*expected),
                "{fixture_name} missing expected JSONL record kind {expected}"
            );
        }

        let eval_records = adapter_projection_eval_jsonl_records(&envelope);
        assert!(
            !eval_records.is_empty(),
            "{fixture_name} fixture should produce eval JSONL records"
        );
        for (index, record) in eval_records.iter().enumerate() {
            assert_eq!(record.projection_id, kind.id());
            assert_eq!(record.record_index, index);
            assert_json_schema_valid(
                &adapter_projection_eval_jsonl_record_schema(*kind),
                &serde_json::to_value(record).unwrap(),
                &format!("{fixture_name} eval JSONL record {}", record.record_id),
            );
        }
    }
}
