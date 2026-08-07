mod support;
use support::*;

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use gents::defra_node::{EmbeddedNode, StorageBackend};
use gents::ensure_runtime_schemas;
use gents::llm::message::{AssistantContent, Message, ToolCall, ToolFunction};
use serde::Deserialize;
use serde_json::{json, Value};

fn adapter_schema_snapshot(snapshot_name: &str, suffix: &str) -> Result<Value> {
    read_workspace_json(&format!(
        "crates/gents/tests/fixtures/adapter_projections/contracts/{snapshot_name}.{suffix}.json"
    ))
}

fn assert_projection_json_matches_schema(snapshot_name: &str, projection: &Value) -> Result<()> {
    assert_json_schema_valid(
        &adapter_schema_snapshot(snapshot_name, "schema")?,
        projection,
        &format!("{snapshot_name} JSON projection"),
    )
}

fn assert_projection_records_match_schema(
    snapshot_name: &str,
    suffix: &str,
    records: &[Value],
) -> Result<()> {
    let schema = adapter_schema_snapshot(snapshot_name, suffix)?;
    anyhow::ensure!(
        !records.is_empty(),
        "{snapshot_name} {suffix} should produce records"
    );
    for (index, record) in records.iter().enumerate() {
        assert_json_schema_valid(
            &schema,
            record,
            &format!("{snapshot_name} {suffix} record {index}"),
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn trace_export_emits_amy_style_jsonl_and_classifies_completed_failures() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let agent_home = tempdir.path().join("agent-home");
    let data_dir = agent_home.join("data");

    {
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await
            .context("opening embedded node")?;
        ensure_runtime_schemas(&node).await?;
        seed_trace_export_rows(&node).await?;
    }

    let output = run_cli_text(
        tempdir.path(),
        &[
            "trace",
            "export",
            "--home",
            agent_home.to_str().context("agent home utf8")?,
            "--run-id",
            "run-cli",
            "--case-id",
            "case-cli",
            "--limit",
            "10",
        ],
    )?;
    let mut records = output
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).context("parsing JSONL record"))
        .collect::<Result<Vec<_>>>()?;
    records.sort_by_key(|record| {
        record
            .get("tool_call_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    });

    assert_eq!(records.len(), 4, "export output:\n{output}");
    let find_record = |tool_call_id: &str| {
        records
            .iter()
            .find(|record| record.get("tool_call_id").and_then(Value::as_str) == Some(tool_call_id))
            .unwrap_or_else(|| panic!("missing record {tool_call_id}; output:\n{output}"))
    };
    let deadline = find_record("call-deadline");
    let failed = find_record("call-fail");
    let missing_tool = find_record("call-missing-tool");
    let succeeded = find_record("call-success");

    assert_eq!(
        failed.get("tool_call_id").and_then(Value::as_str),
        Some("call-fail")
    );
    assert_eq!(
        failed.get("tool_status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        failed.get("tool_result_ok").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        failed.get("tool_failure_class").and_then(Value::as_str),
        Some("toolReturnedError")
    );
    assert_eq!(
        failed
            .get("tool_error")
            .and_then(|value| value.get("retryable"))
            .and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        failed.get("failure_class").and_then(Value::as_str),
        Some("toolReturnedError")
    );
    assert!(failed
        .get("request_failure_class")
        .is_some_and(Value::is_null));
    assert_eq!(
        failed.get("request_id").and_then(Value::as_str),
        Some("req-1")
    );
    assert_eq!(
        failed.get("backend_id").and_then(Value::as_str),
        Some("studios-cluster")
    );
    assert_eq!(
        failed.get("model_name").and_then(Value::as_str),
        Some("baa-ai/GLM-5.1-RAM-420GB-MLX")
    );
    assert_eq!(
        failed.get("inference_profile_id").and_then(Value::as_str),
        Some("amy")
    );
    assert_eq!(
        failed
            .get("raw_tool_call_json")
            .and_then(|value| value.get("function"))
            .and_then(|function| function.get("name"))
            .and_then(Value::as_str),
        Some("bash")
    );
    assert_eq!(failed.get("latency_ms").and_then(Value::as_i64), Some(1500));
    let native_output = failed
        .get("native_tool_output")
        .unwrap_or_else(|| panic!("missing native_tool_output in {failed:#}"));
    assert_eq!(
        native_output.get("ok").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        native_output.get("status").and_then(Value::as_str),
        Some("exit_nonzero")
    );
    assert_eq!(
        native_output.get("command").and_then(Value::as_str),
        Some("grep -P amy README.md")
    );
    assert_eq!(
        native_output.get("exit_code").and_then(Value::as_i64),
        Some(2)
    );
    assert_eq!(
        native_output.get("execution_mode").and_then(Value::as_str),
        Some("read_only")
    );
    assert_eq!(
        native_output.get("sandbox").and_then(Value::as_str),
        Some("policy_read_only")
    );

    assert_eq!(
        missing_tool.get("tool_result_ok").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        missing_tool
            .get("tool_failure_class")
            .and_then(Value::as_str),
        Some("serviceUnavailable")
    );
    assert_eq!(
        missing_tool
            .get("tool_error")
            .and_then(|value| value.get("available_tools")),
        Some(&json!(["search_posts"]))
    );
    assert_eq!(
        missing_tool
            .get("tool_error")
            .and_then(|value| value.get("requested_tool_name"))
            .and_then(Value::as_str),
        Some("search_post")
    );

    assert_eq!(
        succeeded.get("tool_call_id").and_then(Value::as_str),
        Some("call-success")
    );
    assert_eq!(
        succeeded.get("tool_status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        succeeded.get("tool_result_ok").and_then(Value::as_bool),
        Some(true)
    );
    assert!(succeeded
        .get("tool_failure_class")
        .is_some_and(Value::is_null));
    assert!(succeeded.get("failure_class").is_some_and(Value::is_null));
    assert_eq!(
        succeeded.get("run_id").and_then(Value::as_str),
        Some("run-cli")
    );
    assert_eq!(
        succeeded.get("case_id").and_then(Value::as_str),
        Some("case-cli")
    );
    assert_eq!(
        succeeded.get("prompt").and_then(Value::as_str),
        Some("Inspect the repo and show README.md")
    );

    assert_eq!(
        deadline.get("tool_call_id").and_then(Value::as_str),
        Some("call-deadline")
    );
    assert_eq!(
        deadline.get("tool_result_ok").and_then(Value::as_bool),
        Some(true)
    );
    assert!(deadline
        .get("tool_failure_class")
        .is_some_and(Value::is_null));
    assert_eq!(
        deadline
            .get("request_failure_class")
            .and_then(Value::as_str),
        Some("external")
    );
    assert_eq!(
        deadline.get("request_status").and_then(Value::as_str),
        Some("error")
    );
    assert_eq!(
        deadline
            .get("request_lifecycle_state")
            .and_then(Value::as_str),
        Some("failed")
    );
    assert_eq!(
        deadline.get("response_status").and_then(Value::as_str),
        Some("error")
    );

    Ok(())
}

#[test]
fn trace_project_schema_prints_adapter_contracts_without_runtime() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let schema_cases = [
        ("openai-codex", "openai_codex_run_trace"),
        ("langgraph", "langgraph_state_history"),
        ("multi-agent", "multi_agent_task"),
    ];

    for (cli_projection, snapshot_name) in schema_cases {
        let json_schema_output = run_cli_text(
            tempdir.path(),
            &["trace", "project-schema", "--projection", cli_projection],
        )?;
        let json_schema =
            serde_json::from_str::<Value>(&json_schema_output).context("parsing JSON schema")?;
        let expected_json_schema = read_workspace_json(&format!(
            "crates/gents/tests/fixtures/adapter_projections/contracts/{snapshot_name}.schema.json"
        ))?;
        assert_eq!(
            json_schema, expected_json_schema,
            "{cli_projection} JSON schema drifted from checked-in snapshot"
        );

        let jsonl_schema_output = run_cli_text(
            tempdir.path(),
            &[
                "trace",
                "project-schema",
                "--projection",
                cli_projection,
                "--format",
                "jsonl",
            ],
        )?;
        let jsonl_schema =
            serde_json::from_str::<Value>(&jsonl_schema_output).context("parsing JSONL schema")?;
        let expected_jsonl_schema = read_workspace_json(&format!(
            "crates/gents/tests/fixtures/adapter_projections/contracts/{snapshot_name}.jsonl-record.schema.json"
        ))?;
        assert_eq!(
            jsonl_schema, expected_jsonl_schema,
            "{cli_projection} JSONL schema drifted from checked-in snapshot"
        );

        let eval_jsonl_schema_output = run_cli_text(
            tempdir.path(),
            &[
                "trace",
                "project-schema",
                "--projection",
                cli_projection,
                "--format",
                "eval-jsonl",
            ],
        )?;
        let eval_jsonl_schema = serde_json::from_str::<Value>(&eval_jsonl_schema_output)
            .context("parsing eval JSONL schema")?;
        let expected_eval_jsonl_schema = read_workspace_json(&format!(
            "crates/gents/tests/fixtures/adapter_projections/contracts/{snapshot_name}.eval-jsonl-record.schema.json"
        ))?;
        assert_eq!(
            eval_jsonl_schema, expected_eval_jsonl_schema,
            "{cli_projection} eval JSONL schema drifted from checked-in snapshot"
        );
    }

    let atif_native_schema_output = run_cli_text(
        tempdir.path(),
        &[
            "trace",
            "project-schema",
            "--projection",
            "atif",
            "--format",
            "native-json",
        ],
    )?;
    let atif_native_schema = serde_json::from_str::<Value>(&atif_native_schema_output)
        .context("parsing ATIF native JSON schema")?;
    assert_eq!(
        atif_native_schema
            .pointer("/properties/schema_version/const")
            .and_then(Value::as_str),
        Some("ATIF-v1.7")
    );
    assert_eq!(
        atif_native_schema.get("$id").and_then(Value::as_str),
        Some(
            "https://schemas.defra.ai/gents/adapter-projection/atif_trajectory/v1-native.schema.json"
        )
    );

    Ok(())
}

#[tokio::test]
async fn trace_timeline_reconstructs_request_events_from_persisted_rows() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let agent_home = tempdir.path().join("agent-home");
    let data_dir = agent_home.join("data");

    {
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await
            .context("opening embedded node")?;
        ensure_runtime_schemas(&node).await?;
        seed_trace_export_rows(&node).await?;
    }

    let output = run_cli_text(
        tempdir.path(),
        &[
            "trace",
            "timeline",
            "--home",
            agent_home.to_str().context("agent home utf8")?,
            "--request-id",
            "req-1",
        ],
    )?;
    let timeline = serde_json::from_str::<Value>(&output).context("parsing timeline JSON")?;

    assert_eq!(
        timeline.get("request_id").and_then(Value::as_str),
        Some("req-1")
    );
    assert_eq!(
        timeline.get("session_id").and_then(Value::as_str),
        Some("session-1")
    );
    assert_eq!(
        timeline
            .get("conversation")
            .and_then(|conversation| conversation.get("title"))
            .and_then(Value::as_str),
        Some("Trace export test")
    );
    let events = timeline
        .get("events")
        .and_then(Value::as_array)
        .context("timeline events array")?;
    assert!(
        events
            .iter()
            .any(|event| event.get("kind").and_then(Value::as_str) == Some("request")),
        "timeline missing request event: {timeline:#}"
    );
    assert!(
        events.iter().any(|event| {
            event.get("kind").and_then(Value::as_str) == Some("message")
                && event.get("sequence").and_then(Value::as_i64) == Some(2)
        }),
        "timeline missing assistant message event: {timeline:#}"
    );
    let failed_tool = events
        .iter()
        .find(|event| {
            event.get("kind").and_then(Value::as_str) == Some("tool_call")
                && event.get("tool_call_id").and_then(Value::as_str) == Some("call-fail")
        })
        .unwrap_or_else(|| panic!("missing call-fail tool event: {timeline:#}"));
    assert_eq!(
        failed_tool.get("request_id").and_then(Value::as_str),
        Some("req-1")
    );
    assert_eq!(
        failed_tool.get("tool_name").and_then(Value::as_str),
        Some("bash")
    );
    assert!(
        events.iter().any(|event| {
            event.get("kind").and_then(Value::as_str) == Some("response")
                && event.get("request_id").and_then(Value::as_str) == Some("req-1")
        }),
        "timeline missing response event: {timeline:#}"
    );

    Ok(())
}

/// Seed one request with two rendered-request captures (turn 0 attempts 0 and
/// 1) carrying v3 provenance manifests with the admission join.
async fn seed_rendered_request_rows(node: &EmbeddedNode) -> Result<()> {
    exec(
        node,
        r#"mutation {
            create_AgentRequest(input: {
                request_id: "req-cap",
                agent_did: "did:test:amy",
                behavior_id: "amy",
                session_id: "session-cap",
                content: "capture me",
                metadata: "",
                status: "completed",
                lifecycle_state: "complete",
                backend_id: "studios-cluster",
                failure_reason: "",
                created_at: "2026-08-07T12:00:00Z",
                retry_count: 0
            }) { _docID }
        }"#,
    )
    .await?;
    for (suffix, attempt, call_id, call_seq, created_at) in [
        ("a0", 0, "call-cap-1", 1, "2026-08-07T12:00:02Z"),
        ("a1", 1, "call-cap-2", 2, "2026-08-07T12:00:04Z"),
    ] {
        let provenance = json!({
            "manifest_version": 3,
            "status": "captured_only",
            "status_reason": "seeded",
            "capture_seam": "transport_body",
            "capture_scope": "inference.1",
            "admission": { "call_id": call_id, "call_seq": call_seq },
            "assembly_trace": {
                "trace_version": 2,
                "build_path": "budgeted",
                "effective_message_count": 0,
                "assistant_message_ids": [],
                "threaded_tool_results": []
            }
        })
        .to_string();
        let request_json = json!({
            "model": "test-model",
            "messages": [{ "role": "user", "content": "capture me" }]
        })
        .to_string();
        exec(
            node,
            &format!(
                r#"mutation {{
                    create_RenderedRequest(input: {{
                        capture_key: "rendered:v1:seeded-{suffix}",
                        request_doc_id: "doc-req-cap",
                        request_id: "req-cap",
                        session_id: "session-cap",
                        agent_did: "did:test:amy",
                        requester_did: "",
                        behavior_id: "amy",
                        capture_scope: "inference.1",
                        turn_index: 0,
                        attempt: {attempt},
                        capture_version: 1,
                        model_name: "test-model",
                        source: "openai_chat_completions",
                        request_json: "{request_json}",
                        prompt_hash: "aa",
                        tools_hash: "bb",
                        provenance_json: "{provenance}",
                        created_at: "{created_at}"
                    }}) {{ _docID }}
                }}"#,
                request_json = gents::graphql::escape_graphql_string(&request_json),
                provenance = gents::graphql::escape_graphql_string(&provenance),
            ),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn trace_capture_fetches_metadata_with_field_commit_cid() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let agent_home = tempdir.path().join("agent-home");
    let data_dir = agent_home.join("data");

    {
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await
            .context("opening embedded node")?;
        ensure_runtime_schemas(&node).await?;
        seed_rendered_request_rows(&node).await?;
    }
    let home = agent_home.to_str().context("agent home utf8")?;

    // --list: both captures, in numeric identity order, metadata only.
    let output = run_cli_text(
        tempdir.path(),
        &[
            "trace", "capture", "--home", home, "--request-id", "req-cap", "--list",
        ],
    )?;
    let listing = serde_json::from_str::<Value>(&output).context("parsing capture list")?;
    let captures = listing
        .get("captures")
        .and_then(Value::as_array)
        .context("captures array")?;
    assert_eq!(captures.len(), 2, "{listing:#}");
    assert_eq!(
        captures[0].get("attempt").and_then(Value::as_i64),
        Some(0),
        "identity order: attempt 0 first"
    );
    assert!(
        captures.iter().all(|capture| capture.get("request_json").is_none()),
        "list output must not carry bodies: {listing:#}"
    );
    assert_eq!(
        captures[0].get("call_seq").and_then(Value::as_i64),
        Some(1),
        "admission join surfaces: {listing:#}"
    );

    // Exactly one match: metadata plus the request_json field-commit CID.
    let output = run_cli_text(
        tempdir.path(),
        &[
            "trace",
            "capture",
            "--home",
            home,
            "--request-id",
            "req-cap",
            "--scope",
            "inference.1",
            "--turn",
            "0",
            "--attempt",
            "0",
        ],
    )?;
    let capture = serde_json::from_str::<Value>(&output).context("parsing single capture")?;
    assert_eq!(
        capture.get("capture_key").and_then(Value::as_str),
        Some("rendered:v1:seeded-a0")
    );
    assert_eq!(
        capture.get("provenance_status").and_then(Value::as_str),
        Some("captured_only")
    );
    let cid = capture
        .pointer("/request_json_commit/cid")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("expected field-commit cid: {capture:#}"));
    assert!(!cid.is_empty());
    assert!(
        capture.get("request_json").is_none(),
        "default output must not carry the body: {capture:#}"
    );

    // --include-body is the one deliberate body read.
    let output = run_cli_text(
        tempdir.path(),
        &[
            "trace",
            "capture",
            "--home",
            home,
            "--capture-key",
            "rendered:v1:seeded-a1",
            "--include-body",
        ],
    )?;
    let capture = serde_json::from_str::<Value>(&output).context("parsing bodied capture")?;
    let body = capture
        .get("request_json")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("expected request_json with --include-body: {capture:#}"));
    assert!(body.contains("capture me"));
    assert!(capture.get("provenance_json").is_some());

    // Ambiguity without --list fails with a narrowing hint.
    let stderr = run_cli_failure_stderr(
        tempdir.path(),
        &["trace", "capture", "--home", home, "--request-id", "req-cap"],
    )?;
    assert!(
        stderr.contains("narrow with --scope/--turn/--attempt"),
        "{stderr}"
    );

    // The run timeline surfaces the same captures as events.
    let output = run_cli_text(
        tempdir.path(),
        &[
            "trace", "timeline", "--home", home, "--request-id", "req-cap",
        ],
    )?;
    let timeline = serde_json::from_str::<Value>(&output).context("parsing timeline")?;
    let rendered_events = timeline
        .get("events")
        .and_then(Value::as_array)
        .context("timeline events")?
        .iter()
        .filter(|event| event.get("kind").and_then(Value::as_str) == Some("rendered_request"))
        .collect::<Vec<_>>();
    assert_eq!(rendered_events.len(), 2, "{timeline:#}");
    assert_eq!(
        rendered_events[0]
            .get("provenance_status")
            .and_then(Value::as_str),
        Some("captured_only")
    );
    assert!(
        rendered_events
            .iter()
            .all(|event| event.get("request_json").is_none()
                && event.get("provenance_json").is_none()),
        "timeline events must stay metadata-only: {timeline:#}"
    );

    Ok(())
}

#[tokio::test]
async fn trace_project_exports_first_adapter_shapes_from_persisted_rows() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let agent_home = tempdir.path().join("agent-home");
    let data_dir = agent_home.join("data");

    {
        let node = EmbeddedNode::builder()
            .data_path(&data_dir)
            .with_storage_backend(StorageBackend::RocksDb)
            .build()
            .await
            .context("opening embedded node")?;
        ensure_runtime_schemas(&node).await?;
        seed_trace_export_rows(&node).await?;
    }

    let home = agent_home.to_str().context("agent home utf8")?;
    let atif = trace_project_json_with_extra_args(
        tempdir.path(),
        home,
        "atif",
        "full",
        &["--format", "native-json"],
    )?;
    assert_eq!(
        atif.get("schema_version").and_then(Value::as_str),
        Some("ATIF-v1.7")
    );
    assert!(atif.get("projection_id").is_none());
    assert!(
        atif.get("steps")
            .and_then(Value::as_array)
            .is_some_and(|steps| !steps.is_empty()),
        "ATIF projection should contain trajectory steps: {atif:#}"
    );
    assert!(
        atif.pointer("/agent/name")
            .and_then(Value::as_str)
            .is_some(),
        "ATIF projection should identify the Gents behavior: {atif:#}"
    );

    let openai = trace_project_json(tempdir.path(), home, "openai-codex", "public")?;
    assert_projection_json_matches_schema("openai_codex_run_trace", &openai)?;
    assert_eq!(
        openai.get("projection_id").and_then(Value::as_str),
        Some("openai_codex_run_trace")
    );
    assert_eq!(
        openai.get("redaction_mode").and_then(Value::as_str),
        Some("public")
    );
    assert_eq!(
        openai.pointer("/output/adapter").and_then(Value::as_str),
        Some("openai_codex_run_trace")
    );
    let serialized_openai = serde_json::to_string(&openai)?;
    assert!(
        !serialized_openai.contains("Inspect the repo and show README.md"),
        "public adapter projection leaked request content: {openai:#}"
    );
    assert!(
        serialized_openai.contains("[redacted]"),
        "public adapter projection should show redaction markers: {openai:#}"
    );
    let openai_jsonl = trace_project_jsonl_lines(tempdir.path(), home, "openai-codex", "public")?;
    assert_projection_records_match_schema(
        "openai_codex_run_trace",
        "jsonl-record.schema",
        &openai_jsonl,
    )?;
    assert!(
        !openai_jsonl.is_empty(),
        "expected openai-codex JSONL projection records"
    );
    assert!(openai_jsonl.iter().all(|record| {
        record.get("projection_id").and_then(Value::as_str) == Some("openai_codex_run_trace")
            && record.get("source_request_id").and_then(Value::as_str) == Some("req-1")
            && record.get("record_kind").and_then(Value::as_str) == Some("openai_codex_trace_item")
    }));
    let serialized_openai_jsonl = serde_json::to_string(&openai_jsonl)?;
    assert!(
        !serialized_openai_jsonl.contains("Inspect the repo and show README.md"),
        "public JSONL adapter projection leaked request content: {openai_jsonl:#?}"
    );
    let openai_eval_jsonl =
        trace_project_eval_jsonl_lines(tempdir.path(), home, "openai-codex", "public")?;
    assert_projection_records_match_schema(
        "openai_codex_run_trace",
        "eval-jsonl-record.schema",
        &openai_eval_jsonl,
    )?;
    assert!(
        !openai_eval_jsonl.is_empty(),
        "expected openai-codex eval JSONL records"
    );
    assert!(openai_eval_jsonl.iter().all(|record| {
        record.get("projection_id").and_then(Value::as_str) == Some("openai_codex_run_trace")
            && record.get("source_request_id").and_then(Value::as_str) == Some("req-1")
            && record.get("adapter_record_kind").and_then(Value::as_str)
                == Some("openai_codex_trace_item")
    }));
    assert!(
        openai_eval_jsonl.iter().any(|record| {
            record.get("sample_kind").and_then(Value::as_str) == Some("tool_call")
                && record.get("tool_name").and_then(Value::as_str) == Some("bash")
        }),
        "eval JSONL should retain tool-call evidence: {openai_eval_jsonl:#?}"
    );
    let serialized_openai_eval_jsonl = serde_json::to_string(&openai_eval_jsonl)?;
    assert!(
        !serialized_openai_eval_jsonl.contains("Inspect the repo and show README.md"),
        "public eval JSONL adapter projection leaked request content: {openai_eval_jsonl:#?}"
    );
    for projection in ["atif", "openai-codex", "langgraph", "multi-agent"] {
        let training_safe = trace_project_json(tempdir.path(), home, projection, "training-safe")?;
        let serialized_training_safe = serde_json::to_string(&training_safe)?;
        assert_eq!(
            training_safe.get("redaction_mode").and_then(Value::as_str),
            Some("training_safe"),
            "{projection} should record training-safe redaction mode"
        );
        assert!(
            serialized_training_safe.contains("[training_safe_redacted]"),
            "{projection} training-safe projection should include training-safe redaction markers: {training_safe:#}"
        );
        for sensitive_text in [
            "Inspect the repo and show README.md",
            "reviewer private child response",
            "reviewer private child message",
        ] {
            assert!(
                !serialized_training_safe.contains(sensitive_text),
                "{projection} training-safe projection leaked sensitive text {sensitive_text:?}: {training_safe:#}"
            );
        }
    }
    let openai_full = trace_project_json(tempdir.path(), home, "openai-codex", "full")?;
    assert_projection_json_matches_schema("openai_codex_run_trace", &openai_full)?;
    let openai_full_serialized = serde_json::to_string(&openai_full)?;
    for expected_child_text in [
        "reviewer private child response",
        "reviewer private child message",
    ] {
        assert!(
            openai_full_serialized.contains(expected_child_text),
            "unscoped full projection should include child-agent content {expected_child_text:?}: {openai_full:#}"
        );
    }
    let scoped_openai = trace_project_json_with_extra_args(
        tempdir.path(),
        home,
        "openai-codex",
        "full",
        &["--scope-agent-did", "did:test:amy"],
    )?;
    let scoped_openai_serialized = serde_json::to_string(&scoped_openai)?;
    assert!(
        scoped_openai_serialized.contains("req-child"),
        "scoped projection should retain child delegation metadata: {scoped_openai:#}"
    );
    assert!(
        !scoped_openai_serialized.contains("reviewer private child response"),
        "scoped projection leaked child-agent response content: {scoped_openai:#}"
    );
    assert!(
        !scoped_openai_serialized.contains("reviewer private child message"),
        "scoped projection leaked child-agent message content: {scoped_openai:#}"
    );
    let denied_scope = run_cli_failure_stderr(
        tempdir.path(),
        &[
            "trace",
            "project",
            "--home",
            home,
            "--request-id",
            "req-1",
            "--projection",
            "openai-codex",
            "--scope-agent-did",
            "did:test:reviewer",
        ],
    )?;
    assert!(
        denied_scope.contains("projection scope denied request req-1"),
        "expected projection scope denial, got:\n{denied_scope}"
    );

    let langgraph = trace_project_json(tempdir.path(), home, "langgraph", "full")?;
    assert_projection_json_matches_schema("langgraph_state_history", &langgraph)?;
    assert_eq!(
        langgraph.get("projection_id").and_then(Value::as_str),
        Some("langgraph_state_history")
    );
    assert_eq!(
        langgraph.pointer("/output/adapter").and_then(Value::as_str),
        Some("langgraph_state_history")
    );
    let edges = langgraph
        .pointer("/output/projection/edges")
        .and_then(Value::as_array)
        .context("langgraph edges")?;
    assert!(
        edges.iter().any(|edge| {
            edge.get("kind").and_then(Value::as_str) == Some("child_request")
                && edge.get("to").and_then(Value::as_str) == Some("request:req-child")
        }),
        "langgraph projection missing child request edge: {langgraph:#}"
    );
    let langgraph_jsonl = trace_project_jsonl_lines(tempdir.path(), home, "langgraph", "full")?;
    assert_projection_records_match_schema(
        "langgraph_state_history",
        "jsonl-record.schema",
        &langgraph_jsonl,
    )?;
    assert!(
        langgraph_jsonl.iter().any(|record| {
            record.get("record_kind").and_then(Value::as_str) == Some("langgraph_edge")
                && record.pointer("/value/kind").and_then(Value::as_str) == Some("child_request")
                && record.pointer("/value/to").and_then(Value::as_str) == Some("request:req-child")
        }),
        "langgraph JSONL projection missing child request edge record: {langgraph_jsonl:#?}"
    );
    let langgraph_eval_jsonl =
        trace_project_eval_jsonl_lines(tempdir.path(), home, "langgraph", "full")?;
    assert_projection_records_match_schema(
        "langgraph_state_history",
        "eval-jsonl-record.schema",
        &langgraph_eval_jsonl,
    )?;
    assert!(
        langgraph_eval_jsonl.iter().any(|record| {
            record.get("sample_kind").and_then(Value::as_str) == Some("task")
                && record.get("tool_name").and_then(Value::as_str) == Some("bash")
        }),
        "langgraph eval JSONL projection missing task sample: {langgraph_eval_jsonl:#?}"
    );
    assert!(
        langgraph_eval_jsonl.iter().any(|record| {
            record.get("sample_kind").and_then(Value::as_str) == Some("state_transition")
                && record
                    .pointer("/metadata/kind")
                    .and_then(Value::as_str)
                    == Some("child_request")
        }),
        "langgraph eval JSONL projection missing child transition sample: {langgraph_eval_jsonl:#?}"
    );

    let multi_agent = trace_project_json(tempdir.path(), home, "multi-agent", "full")?;
    assert_projection_json_matches_schema("multi_agent_task", &multi_agent)?;
    assert_eq!(
        multi_agent.get("projection_id").and_then(Value::as_str),
        Some("multi_agent_task")
    );
    assert_eq!(
        multi_agent
            .pointer("/output/adapter")
            .and_then(Value::as_str),
        Some("multi_agent_task")
    );
    let delegations = multi_agent
        .pointer("/output/projection/delegations")
        .and_then(Value::as_array)
        .context("multi-agent delegations")?;
    assert!(
        delegations.iter().any(|delegation| {
            delegation.get("parent_request_id").and_then(Value::as_str) == Some("req-1")
                && delegation.get("child_request_id").and_then(Value::as_str) == Some("req-child")
        }),
        "multi-agent projection missing child delegation: {multi_agent:#}"
    );
    let multi_agent_jsonl = trace_project_jsonl_lines(tempdir.path(), home, "multi-agent", "full")?;
    assert_projection_records_match_schema(
        "multi_agent_task",
        "jsonl-record.schema",
        &multi_agent_jsonl,
    )?;
    assert!(
        multi_agent_jsonl.iter().any(|record| {
            record.get("record_kind").and_then(Value::as_str) == Some("multi_agent_delegation")
                && record
                    .pointer("/value/parent_request_id")
                    .and_then(Value::as_str)
                    == Some("req-1")
                && record
                    .pointer("/value/child_request_id")
                    .and_then(Value::as_str)
                    == Some("req-child")
        }),
        "multi-agent JSONL projection missing delegation record: {multi_agent_jsonl:#?}"
    );
    let multi_agent_eval_jsonl =
        trace_project_eval_jsonl_lines(tempdir.path(), home, "multi-agent", "full")?;
    assert_projection_records_match_schema(
        "multi_agent_task",
        "eval-jsonl-record.schema",
        &multi_agent_eval_jsonl,
    )?;
    assert!(
        multi_agent_eval_jsonl.iter().any(|record| {
            record.get("sample_kind").and_then(Value::as_str) == Some("delegation")
                && record.get("parent_request_id").and_then(Value::as_str) == Some("req-1")
                && record.get("child_request_id").and_then(Value::as_str) == Some("req-child")
        }),
        "multi-agent eval JSONL projection missing delegation sample: {multi_agent_eval_jsonl:#?}"
    );

    Ok(())
}

#[tokio::test]
async fn trace_project_graphql_enforces_acp_read_filter_with_real_cli() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let endpoint = spawn_projection_graphql_acp_mock().await?;

    let cwd = tempdir.path().to_path_buf();
    let graphql = endpoint.graphql.clone();
    let output = tokio::task::spawn_blocking(move || {
        run_cli_text(
            &cwd,
            &[
                "trace",
                "project",
                "--graphql",
                &graphql,
                "--request-id",
                "req-acp",
                "--projection",
                "multi-agent",
                "--redaction",
                "full",
                "--actor-did",
                "did:test:projection-reader",
                "--acp-policy-id",
                "projection-policy",
            ],
        )
    })
    .await
    .context("joining trace project CLI task")??;
    let projection =
        serde_json::from_str::<Value>(&output).context("parsing ACP-filtered projection JSON")?;

    assert_eq!(
        projection.get("projection_id").and_then(Value::as_str),
        Some("multi_agent_task")
    );
    assert_eq!(
        projection
            .pointer("/provenance/actor_did")
            .and_then(Value::as_str),
        Some("did:test:projection-reader")
    );

    let serialized = serde_json::to_string(&projection)?;
    assert!(
        serialized.contains("req-acp-child"),
        "allowed parent delegation metadata should retain child request id: {projection:#}"
    );
    for denied_text in [
        "child private request",
        "child private response",
        "child private message",
    ] {
        assert!(
            !serialized.contains(denied_text),
            "ACP-filtered projection leaked denied row content {denied_text:?}: {projection:#}"
        );
    }

    let delegations = projection
        .pointer("/output/projection/delegations")
        .and_then(Value::as_array)
        .context("multi-agent delegations")?;
    assert!(
        delegations.is_empty(),
        "ACP-filtered projection should omit delegation rows for unreadable child requests: {projection:#}"
    );
    let tool_events = projection
        .pointer("/output/projection/tool_events")
        .and_then(Value::as_array)
        .context("multi-agent tool events")?;
    assert!(
        tool_events.iter().any(|tool_event| {
            tool_event.get("request_id").and_then(Value::as_str) == Some("req-acp")
                && tool_event.get("child_request_id").and_then(Value::as_str)
                    == Some("req-acp-child")
        }),
        "ACP-filtered projection should retain allowed parent tool event child edge: {projection:#}"
    );

    let messages = projection
        .pointer("/output/projection/messages")
        .and_then(Value::as_array)
        .context("multi-agent messages")?;
    assert!(
        messages.iter().all(|message| {
            message
                .get("content")
                .and_then(Value::as_str)
                .is_none_or(|content| !content.contains("child private"))
        }),
        "ACP-filtered projection leaked denied child message rows: {projection:#}"
    );

    Ok(())
}

fn trace_project_json(
    cwd: &std::path::Path,
    home: &str,
    projection: &str,
    redaction: &str,
) -> Result<Value> {
    trace_project_json_with_extra_args(cwd, home, projection, redaction, &[])
}

fn trace_project_json_with_extra_args(
    cwd: &std::path::Path,
    home: &str,
    projection: &str,
    redaction: &str,
    extra_args: &[&str],
) -> Result<Value> {
    let mut args = vec![
        "trace",
        "project",
        "--home",
        home,
        "--request-id",
        "req-1",
        "--projection",
        projection,
        "--redaction",
        redaction,
        "--actor-did",
        "did:test:test-viewer",
    ];
    args.extend_from_slice(extra_args);
    let output = run_cli_text(cwd, &args)?;
    serde_json::from_str::<Value>(&output).context("parsing adapter projection JSON")
}

fn trace_project_jsonl_lines(
    cwd: &std::path::Path,
    home: &str,
    projection: &str,
    redaction: &str,
) -> Result<Vec<Value>> {
    let output = run_cli_text(
        cwd,
        &[
            "trace",
            "project",
            "--home",
            home,
            "--request-id",
            "req-1",
            "--projection",
            projection,
            "--redaction",
            redaction,
            "--format",
            "jsonl",
            "--actor-did",
            "did:test:test-viewer",
        ],
    )?;
    output
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).context("parsing adapter projection JSONL"))
        .collect::<Result<Vec<_>>>()
}

fn trace_project_eval_jsonl_lines(
    cwd: &std::path::Path,
    home: &str,
    projection: &str,
    redaction: &str,
) -> Result<Vec<Value>> {
    let output = run_cli_text(
        cwd,
        &[
            "trace",
            "project",
            "--home",
            home,
            "--request-id",
            "req-1",
            "--projection",
            projection,
            "--redaction",
            redaction,
            "--format",
            "eval-jsonl",
            "--actor-did",
            "did:test:test-viewer",
        ],
    )?;
    output
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).context("parsing adapter projection eval JSONL")
        })
        .collect::<Result<Vec<_>>>()
}

async fn seed_trace_export_rows(node: &EmbeddedNode) -> Result<()> {
    exec(
        node,
        r#"mutation {
            create_AgentBehavior(input: {
                behavior_id: "amy",
                agent_did: "did:test:amy",
                display_name: "Amy",
                system_prompt: "baseline",
                backend_id: "studios-cluster",
                model_name: "baa-ai/GLM-5.1-RAM-420GB-MLX",
                tool_selection_id: "default-tools",
                inference_profile_id: "amy",
                enabled: true,
                created_at: "2026-05-04T12:00:00Z"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentSession(input: {
                session_id: "session-1",
                agent_name: "Amy",
                behavior_id: "amy",
                started: "2026-05-04T12:00:00Z",
                status: "active"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentConversation(input: {
                session_id: "session-1",
                agent_name: "Amy",
                agent_did: "did:test:amy",
                behavior_id: "amy",
                title: "Trace export test",
                title_source: "test",
                preview_text: "Inspect the repo",
                status: "active",
                created_at: "2026-05-04T12:00:00Z",
                updated_at: "2026-05-04T12:00:05Z",
                latest_request_id: "req-1"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentRequest(input: {
                request_id: "req-1",
                agent_did: "did:test:amy",
                behavior_id: "amy",
                session_id: "session-1",
                content: "Inspect the repo and show README.md",
                metadata: "{\"run_id\":\"run-metadata\",\"case_id\":\"case-metadata\"}",
                status: "completed",
                lifecycle_state: "complete",
                backend_id: "studios-cluster",
                failure_reason: "",
                created_at: "2026-05-04T12:00:01Z",
                retry_count: 0
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentRequest(input: {
                request_id: "req-child",
                agent_did: "did:test:reviewer",
                behavior_id: "reviewer",
                session_id: "session-child",
                content: "Review the README finding",
                metadata: "",
                status: "completed",
                lifecycle_state: "complete",
                backend_id: "studios-cluster",
                failure_reason: "",
                created_at: "2026-05-04T12:00:04Z",
                retry_count: 0,
                caused_by_parent_request_id: "req-1",
                caused_by_parent_tool_call_id: "call-fail"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentSession(input: {
                session_id: "session-child",
                agent_name: "Reviewer",
                behavior_id: "reviewer",
                started: "2026-05-04T12:00:04Z",
                status: "active"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentConversation(input: {
                session_id: "session-child",
                agent_name: "Reviewer",
                agent_did: "did:test:reviewer",
                behavior_id: "reviewer",
                title: "Child trace export test",
                title_source: "test",
                preview_text: "Review the README finding",
                status: "active",
                created_at: "2026-05-04T12:00:04Z",
                updated_at: "2026-05-04T12:00:07Z",
                latest_request_id: "req-child"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentResponse(input: {
                response_key: "req-1",
                request_id: "req-1",
                agent_did: "did:test:amy",
                behavior_id: "amy",
                session_id: "session-1",
                content: "done",
                reasoning: "",
                status: "completed",
                error_message: "",
                token_count: 12,
                progress_seq: 3,
                materialized_message_sequence: 4,
                materialized_at: "2026-05-04T12:00:06Z",
                created_at: "2026-05-04T12:00:01Z",
                completed_at: "2026-05-04T12:00:06Z"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentResponse(input: {
                response_key: "req-child",
                request_id: "req-child",
                agent_did: "did:test:reviewer",
                behavior_id: "reviewer",
                session_id: "session-child",
                content: "reviewer private child response",
                reasoning: "child reasoning",
                status: "completed",
                error_message: "",
                token_count: 8,
                progress_seq: 1,
                materialized_message_sequence: 1,
                materialized_at: "2026-05-04T12:00:07Z",
                created_at: "2026-05-04T12:00:04Z",
                completed_at: "2026-05-04T12:00:07Z"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentMessage(input: {
                message_key: "session-child:1",
                session_id: "session-child",
                request_id: "req-child",
                sequence: 1,
                role: "assistant",
                content: "reviewer private child message",
                timestamp: "2026-05-04T12:00:06Z"
            }) { _docID }
        }"#,
    )
    .await?;

    let success_message =
        assistant_tool_message("call-success", "read", json!({"path":"README.md"}))?;
    let failed_message = assistant_tool_message(
        "call-fail",
        "bash",
        json!({"command":"grep","args":["-P","amy","README.md"]}),
    )?;
    let failed_result = format!(
        "gents_exec: {}\nstdout:\n(empty)\nstderr:\ngrep: invalid option -- P",
        json!({
            "ok": false,
            "status": "exit_nonzero",
            "command": "grep -P amy README.md",
            "argv": ["grep", "-P", "amy", "README.md"],
            "cwd": "/repo",
            "exit_code": 2,
            "timed_out": false,
            "duration_ms": 1500,
            "timeout_ms": 10000,
            "execution_mode": "read_only",
            "network_mode": "inherit",
            "sandbox": "policy_read_only",
            "stdout_truncation": {
                "returned_bytes": 0,
                "total_bytes": 0,
                "max_bytes": 16000,
                "truncated": false
            },
            "stderr_truncation": {
                "returned_bytes": 24,
                "total_bytes": 24,
                "max_bytes": 16000,
                "truncated": false
            }
        })
    );
    let missing_tool_message = assistant_tool_message(
        "call-missing-tool",
        "describe_tool",
        json!({"service_id":"x-data","tool_name":"search_post"}),
    )?;
    exec(
        node,
        &format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "session-1:2",
                    session_id: "session-1",
                    sequence: 2,
                    role: "assistant",
                    content: "{}",
                    timestamp: "2026-05-04T12:00:02Z"
                }}) {{ _docID }}
            }}"#,
            escape_graphql_string(&success_message)
        ),
    )
    .await?;
    exec(
        node,
        &format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "session-1:3",
                    session_id: "session-1",
                    sequence: 3,
                    role: "assistant",
                    content: "{}",
                    timestamp: "2026-05-04T12:00:03Z"
                }}) {{ _docID }}
            }}"#,
            escape_graphql_string(&failed_message)
        ),
    )
    .await?;
    exec(
        node,
        &format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "session-1:4",
                    session_id: "session-1",
                    sequence: 4,
                    role: "assistant",
                    content: "{}",
                    timestamp: "2026-05-04T12:00:04Z"
                }}) {{ _docID }}
            }}"#,
            escape_graphql_string(&missing_tool_message)
        ),
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentToolCall(input: {
                tool_call_key: "session-1:call-success",
                session_id: "session-1",
                message_sequence: 2,
                tool_name: "read",
                tool_call_id: "call-success",
                args: "{\"path\":\"README.md\"}",
                result: "README contents",
                status: "completed",
                started_at: "2026-05-04T12:00:02Z",
                completed_at: "2026-05-04T12:00:03Z"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        &format!(
            r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "session-1:call-fail",
                session_id: "session-1",
                message_sequence: 3,
                tool_name: "bash",
                tool_call_id: "call-fail",
                args: "{{\"command\":\"grep\",\"args\":[\"-P\",\"amy\",\"README.md\"]}}",
                result: "{}",
                status: "completed",
                started_at: "2026-05-04T12:00:03Z",
                completed_at: "2026-05-04T12:00:04.500Z"
            }}) {{ _docID }}
        }}"#,
            escape_graphql_string(&failed_result)
        ),
    )
    .await?;

    let missing_tool_result = json!({
        "ok": false,
        "failure_class": "tool_not_found",
        "path": "/tool_name",
        "message": "tool 'search_post' was not found on service 'x-data'; available tools: search_posts",
        "retryable": true,
        "service_id": "x-data",
        "tool_name": "search_post",
        "requested_tool_name": "search_post",
        "available_tools": ["search_posts"]
    })
    .to_string();
    exec(
        node,
        &format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "session-1:call-missing-tool",
                    session_id: "session-1",
                    message_sequence: 4,
                    tool_name: "describe_tool",
                    tool_call_id: "call-missing-tool",
                    args: "{{\"service_id\":\"x-data\",\"tool_name\":\"search_post\"}}",
                    result: "{}",
                    status: "completed",
                    started_at: "2026-05-04T12:00:04Z",
                    completed_at: "2026-05-04T12:00:04.250Z"
                }}) {{ _docID }}
            }}"#,
            escape_graphql_string(&missing_tool_result)
        ),
    )
    .await?;

    exec(
        node,
        r#"mutation {
            create_AgentSession(input: {
                session_id: "session-2",
                agent_name: "Amy",
                behavior_id: "amy",
                started: "2026-05-04T13:00:00Z",
                status: "active"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentConversation(input: {
                session_id: "session-2",
                agent_name: "Amy",
                agent_did: "did:test:amy",
                behavior_id: "amy",
                title: "Trace export deadline test",
                title_source: "test",
                preview_text: "Read a file then deadline",
                status: "active",
                created_at: "2026-05-04T13:00:00Z",
                updated_at: "2026-05-04T13:00:10Z",
                latest_request_id: "req-deadline"
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentRequest(input: {
                request_id: "req-deadline",
                agent_did: "did:test:amy",
                behavior_id: "amy",
                session_id: "session-2",
                content: "Read README.md but the request later times out",
                metadata: "",
                status: "error",
                lifecycle_state: "failed",
                backend_id: "studios-cluster",
                failure_reason: "request deadline exceeded while waiting for inference stream item",
                created_at: "2026-05-04T13:00:01Z",
                retry_count: 0
            }) { _docID }
        }"#,
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentResponse(input: {
                response_key: "req-deadline",
                request_id: "req-deadline",
                agent_did: "did:test:amy",
                behavior_id: "amy",
                session_id: "session-2",
                content: "",
                reasoning: "",
                status: "error",
                error_message: "request deadline exceeded while waiting for inference stream item",
                token_count: 0,
                progress_seq: 3,
                materialized_message_sequence: 4,
                materialized_at: "2026-05-04T13:00:10Z",
                created_at: "2026-05-04T13:00:01Z",
                completed_at: "2026-05-04T13:00:10Z"
            }) { _docID }
        }"#,
    )
    .await?;
    let deadline_message =
        assistant_tool_message("call-deadline", "read", json!({"path":"README.md"}))?;
    exec(
        node,
        &format!(
            r#"mutation {{
                create_AgentMessage(input: {{
                    message_key: "session-2:2",
                    session_id: "session-2",
                    sequence: 2,
                    role: "assistant",
                    content: "{}",
                    timestamp: "2026-05-04T13:00:02Z"
                }}) {{ _docID }}
            }}"#,
            escape_graphql_string(&deadline_message)
        ),
    )
    .await?;
    exec(
        node,
        r#"mutation {
            create_AgentToolCall(input: {
                tool_call_key: "session-2:call-deadline",
                session_id: "session-2",
                message_sequence: 2,
                tool_name: "read",
                tool_call_id: "call-deadline",
                args: "{\"path\":\"README.md\"}",
                result: "README contents",
                status: "completed",
                started_at: "2026-05-04T13:00:02Z",
                completed_at: "2026-05-04T13:00:03Z"
            }) { _docID }
        }"#,
    )
    .await?;
    Ok(())
}

fn assistant_tool_message(call_id: &str, name: &str, arguments: Value) -> Result<String> {
    serde_json::to_string(&Message::Assistant {
        id: None,
        content: vec![AssistantContent::ToolCall(ToolCall {
            id: call_id.to_string(),
            call_id: Some(call_id.to_string()),
            function: ToolFunction {
                name: name.to_string(),
                arguments,
            },
            signature: None,
            additional_params: None,
        })],
    })
    .context("serializing assistant tool message")
}

struct ProjectionGraphqlAcpMock {
    graphql: String,
}

#[derive(Clone)]
struct ProjectionGraphqlAcpState {
    allowed: Arc<BTreeMap<(String, String), bool>>,
}

#[derive(Debug, Deserialize)]
struct ProjectionGraphqlRequest {
    query: String,
}

#[derive(Debug, Deserialize)]
struct ProjectionAcpDecisionRequest {
    actor: String,
    permission: String,
    #[serde(rename = "policyID")]
    policy_id: String,
    #[serde(rename = "resourceName")]
    resource_name: String,
    #[serde(rename = "docID")]
    doc_id: String,
}

async fn spawn_projection_graphql_acp_mock() -> Result<ProjectionGraphqlAcpMock> {
    let mut allowed = BTreeMap::new();
    for (resource_name, doc_id) in [
        ("AgentRequest", "doc-request-root"),
        ("AgentToolCall", "doc-tool-delegate"),
        ("AgentResponse", "doc-response-root"),
        ("AgentSession", "doc-session"),
        ("AgentConversation", "doc-conversation"),
    ] {
        allowed.insert((resource_name.to_string(), doc_id.to_string()), true);
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("binding projection GraphQL/ACP mock")?;
    let addr = listener.local_addr()?;
    let state = ProjectionGraphqlAcpState {
        allowed: Arc::new(allowed),
    };
    let router = Router::new()
        .route("/api/v0/graphql", post(projection_graphql_mock))
        .route("/api/v0/acp/document/decide", post(projection_acp_mock))
        .with_state(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    Ok(ProjectionGraphqlAcpMock {
        graphql: format!("http://{addr}/api/v0/graphql"),
    })
}

async fn projection_graphql_mock(
    Json(body): Json<ProjectionGraphqlRequest>,
) -> (StatusCode, Json<Value>) {
    let query = body.query.as_str();
    let response = if query.contains("AgentRequest(") {
        json!({ "data": { "AgentRequest": projection_mock_agent_requests(query) } })
    } else if query.contains("AgentMessage(") {
        json!({ "data": { "AgentMessage": projection_mock_agent_messages() } })
    } else if query.contains("AgentToolCall(") {
        json!({ "data": { "AgentToolCall": projection_mock_tool_calls() } })
    } else if query.contains("AgentResponse(") {
        json!({ "data": { "AgentResponse": projection_mock_agent_responses() } })
    } else if query.contains("AgentSession(") {
        json!({ "data": { "AgentSession": [projection_mock_session()] } })
    } else if query.contains("AgentConversation(") {
        json!({ "data": { "AgentConversation": [projection_mock_conversation()] } })
    } else if query.contains("InferenceCall(") {
        json!({ "data": { "InferenceCall": [] } })
    } else if query.contains("RenderedRequest(") {
        json!({ "data": { "RenderedRequest": [] } })
    } else {
        json!({
            "errors": [{
                "message": format!("unexpected projection GraphQL query: {query}")
            }]
        })
    };
    (StatusCode::OK, Json(response))
}

async fn projection_acp_mock(
    State(state): State<ProjectionGraphqlAcpState>,
    Json(body): Json<ProjectionAcpDecisionRequest>,
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
    let allowed = state
        .allowed
        .get(&(body.resource_name, body.doc_id))
        .copied()
        .unwrap_or(false);
    (StatusCode::OK, Json(json!({ "allowed": allowed })))
}

fn projection_mock_agent_requests(query: &str) -> Value {
    if query.contains("filter: { caused_by_parent_request_id") {
        json!([projection_mock_child_request()])
    } else if query.contains("filter: { session_id") {
        json!([
            projection_mock_root_request(),
            projection_mock_child_request()
        ])
    } else {
        json!([projection_mock_root_request()])
    }
}

fn projection_mock_root_request() -> Value {
    json!({
        "_docID": "doc-request-root",
        "request_id": "req-acp",
        "agent_did": "did:test:amy",
        "behavior_id": "amy",
        "session_id": "session-acp",
        "content": "root visible request",
        "metadata": "",
        "status": "completed",
        "lifecycle_state": "complete",
        "backend_id": "mock-backend",
        "failure_reason": "",
        "created_at": "2026-06-05T18:00:00Z",
        "retry_count": 0,
        "interrupt_requested_at": null,
        "caused_by_parent_request_id": null,
        "caused_by_parent_tool_call_id": null
    })
}

fn projection_mock_child_request() -> Value {
    json!({
        "_docID": "doc-request-child",
        "request_id": "req-acp-child",
        "agent_did": "did:test:reviewer",
        "behavior_id": "reviewer",
        "session_id": "session-acp",
        "content": "child private request",
        "metadata": "",
        "status": "completed",
        "lifecycle_state": "complete",
        "backend_id": "mock-backend",
        "failure_reason": "",
        "created_at": "2026-06-05T18:00:02Z",
        "retry_count": 0,
        "interrupt_requested_at": null,
        "caused_by_parent_request_id": "req-acp",
        "caused_by_parent_tool_call_id": "call-delegate"
    })
}

fn projection_mock_agent_messages() -> Value {
    json!([
        {
            "_docID": "doc-message-root",
            "session_id": "session-acp",
            "sequence": 1,
            "role": "assistant",
            "content": "root visible message",
            "timestamp": "2026-06-05T18:00:01Z"
        },
        {
            "_docID": "doc-message-child",
            "session_id": "session-acp",
            "sequence": 2,
            "role": "assistant",
            "content": "child private message",
            "timestamp": "2026-06-05T18:00:03Z"
        }
    ])
}

fn projection_mock_tool_calls() -> Value {
    json!([
        {
            "_docID": "doc-tool-delegate",
            "request_id": "req-acp",
            "session_id": "session-acp",
            "message_sequence": 1,
            "tool_name": "spawn_subagent",
            "tool_call_id": "call-delegate",
            "args": "{\"task\":\"review visible request\"}",
            "result": "spawned reviewer",
            "status": "completed",
            "lifecycle_state": "completed",
            "started_at": "2026-06-05T18:00:01Z",
            "deadline_at": null,
            "completed_at": "2026-06-05T18:00:02Z",
            "selected_service_id": "gents",
            "selected_tool_name": "spawn_subagent",
            "tool_failure_class": null,
            "denial_reason": null,
            "denied_argv": [],
            "denied_command": null,
            "denied_argument": null,
            "denied_subcommand": null,
            "denied_prefix": [],
            "policy_mode": null,
            "policy_network": null,
            "latency_ms": 1000,
            "await_mode": "background",
            "cancel_policy": null,
            "cancel_cause": null,
            "child_request_id": "req-acp-child"
        }
    ])
}

fn projection_mock_agent_responses() -> Value {
    json!([
        {
            "_docID": "doc-response-root",
            "request_id": "req-acp",
            "agent_did": "did:test:amy",
            "behavior_id": "amy",
            "session_id": "session-acp",
            "content": "root visible response",
            "reasoning": "root visible reasoning",
            "status": "completed",
            "error_message": "",
            "token_count": 7,
            "progress_seq": 2,
            "materialized_message_sequence": 1,
            "materialized_at": "2026-06-05T18:00:04Z",
            "created_at": "2026-06-05T18:00:00Z",
            "completed_at": "2026-06-05T18:00:04Z",
            "interrupted_at": null
        },
        {
            "_docID": "doc-response-child",
            "request_id": "req-acp-child",
            "agent_did": "did:test:reviewer",
            "behavior_id": "reviewer",
            "session_id": "session-acp",
            "content": "child private response",
            "reasoning": "child private reasoning",
            "status": "completed",
            "error_message": "",
            "token_count": 5,
            "progress_seq": 1,
            "materialized_message_sequence": 2,
            "materialized_at": "2026-06-05T18:00:05Z",
            "created_at": "2026-06-05T18:00:02Z",
            "completed_at": "2026-06-05T18:00:05Z",
            "interrupted_at": null
        }
    ])
}

fn projection_mock_session() -> Value {
    json!({
        "_docID": "doc-session",
        "session_id": "session-acp",
        "agent_name": "Amy",
        "behavior_id": "amy",
        "started": "2026-06-05T18:00:00Z",
        "ended": null,
        "status": "active"
    })
}

fn projection_mock_conversation() -> Value {
    json!({
        "_docID": "doc-conversation",
        "session_id": "session-acp",
        "agent_name": "Amy",
        "agent_did": "did:test:amy",
        "behavior_id": "amy",
        "title": "ACP projection test",
        "title_source": "test",
        "preview_text": "root visible preview",
        "status": "active",
        "created_at": "2026-06-05T18:00:00Z",
        "updated_at": "2026-06-05T18:00:05Z",
        "latest_request_id": "req-acp",
        "forked_from_session_id": null
    })
}
