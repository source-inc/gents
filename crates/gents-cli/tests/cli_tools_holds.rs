mod support;
use support::*;

use std::fs;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::Value;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tools_holds_lists_and_approve_writes_decision() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-holds-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;
    let port = allocate_port()?;
    let agent_name = format!("cli-holds-{}", Uuid::new_v4().simple());
    let graphql = graphql_url(port);

    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let mut serve = spawn_server(&home_dir, port)?;
    wait_for_port(port, &mut serve)?;
    wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;

    let empty = run_cli_json(
        &home_dir,
        &[
            "tools",
            "holds",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
        ],
    )?;
    assert_eq!(empty.get("count").and_then(Value::as_u64), Some(0));

    let deadline = (chrono::Utc::now() + chrono::Duration::seconds(300)).to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "sess-holds:call-held-1",
                request_id: "req-holds-1",
                session_id: "sess-holds",
                agent_did: "{agent_did}",
                message_sequence: 1,
                tool_name: "bash_unrestricted",
                tool_call_id: "call-held-1",
                args: "{{\"command\":\"rm -rf build\"}}",
                result: "",
                status: "called",
                lifecycle_state: "awaitingApproval",
                started_at: null,
                deadline_at: "{deadline}"
            }}) {{ _docID }}
        }}"#
    );
    graphql_query(&graphql, &mutation).await?;

    let held = run_cli_json(
        &home_dir,
        &[
            "tools",
            "holds",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
        ],
    )?;
    assert_eq!(held.get("count").and_then(Value::as_u64), Some(1));
    assert_eq!(
        held.pointer("/held/0/tool_call_id").and_then(Value::as_str),
        Some("call-held-1")
    );
    assert_eq!(
        held.pointer("/held/0/tool_name").and_then(Value::as_str),
        Some("bash_unrestricted")
    );

    let denied = run_cli_json(
        &home_dir,
        &[
            "tools",
            "approve",
            "call-held-1",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--deny",
            "--reason",
            "destructive command",
        ],
    )?;
    assert_eq!(
        denied.get("decision").and_then(Value::as_str),
        Some("denied")
    );
    let approval_id = denied
        .get("approval_id")
        .and_then(Value::as_str)
        .context("approve output missing approval_id")?;
    let held_doc_id = held
        .pointer("/held/0/_docID")
        .and_then(Value::as_str)
        .context("held call missing physical _docID")?;
    assert_eq!(approval_id, format!("approval-{held_doc_id}"));

    let response = graphql_query(
        &graphql,
        r#"{
            AgentToolApproval(filter: { tool_call_id: { _eq: "call-held-1" } }) {
                decision
                reason
                approver_did
                agent_did
            }
        }"#,
    )
    .await?;
    let row = first_graphql_row(&response, "AgentToolApproval")?;
    assert_eq!(row.get("decision").and_then(Value::as_str), Some("denied"));
    assert_eq!(
        row.get("reason").and_then(Value::as_str),
        Some("destructive command")
    );
    assert_eq!(
        row.get("agent_did").and_then(Value::as_str),
        Some(agent_did.as_str())
    );
    assert_eq!(
        row.get("approver_did").and_then(Value::as_str),
        Some(agent_did.as_str())
    );

    let stderr = run_cli_failure_stderr(
        &home_dir,
        &[
            "tools",
            "approve",
            "no-such-call",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
        ],
    )?;
    assert!(
        stderr.contains("not awaiting approval"),
        "unexpected stderr: {stderr}"
    );

    serve.child.kill().ok();
    Ok(())
}
