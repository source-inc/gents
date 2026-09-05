mod support;
use support::*;

use std::fs;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use gents::{default_behavior_id_for_agent, default_tool_selection_id_for_behavior};
use serde_json::Value;
use uuid::Uuid;

fn generated_backend_id_for_agent(agent_did: &str) -> String {
    format!("{agent_did}:backend")
}

fn generated_tool_selection_id_for_agent(agent_did: &str) -> String {
    let default_behavior_id = default_behavior_id_for_agent(agent_did);
    default_tool_selection_id_for_behavior(&default_behavior_id)
}

fn find_snapshot_row<'a>(
    snapshot: &'a Value,
    collection: &str,
    key: &str,
    expected: &str,
) -> Result<&'a Value> {
    snapshot
        .get(collection)
        .and_then(Value::as_array)
        .and_then(|rows| {
            rows.iter()
                .find(|row| row.get(key).and_then(Value::as_str) == Some(expected))
        })
        .ok_or_else(|| anyhow!("missing {collection} row with {key}={expected}: {snapshot}"))
}

async fn wait_for_inference_call_state(
    graphql: &str,
    request_id: &str,
    expected_state: &str,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = graphql_query(
            graphql,
            &format!(
                r#"{{
                    InferenceCall(
                        filter: {{ request_id: {{ _eq: "{}" }} }},
                        order: {{ call_seq: ASC }}
                    ) {{
                        request_id
                        backend_id
                        behavior_id
                        call_state
                    }}
                }}"#,
                escape_graphql_string(request_id),
            ),
        )
        .await?;
        let rows = response
            .pointer("/data/InferenceCall")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if let Some(row) = rows
            .iter()
            .find(|row| row.get("call_state").and_then(Value::as_str) == Some(expected_state))
        {
            return Ok(row.clone());
        }

        if Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out waiting for InferenceCall request_id={request_id} call_state={expected_state}; last rows={}",
                Value::Array(rows)
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn active_inference_calls_for_backend(graphql: &str, backend_id: &str) -> Result<Vec<Value>> {
    let response = graphql_query(
        graphql,
        &format!(
            r#"{{
                InferenceCall(
                    filter: {{ backend_id: {{ _eq: "{}" }} }},
                    order: {{ call_seq: ASC }}
                ) {{
                    request_id
                    backend_id
                    behavior_id
                    call_state
                }}
            }}"#,
            escape_graphql_string(backend_id),
        ),
    )
    .await?;
    Ok(response
        .pointer("/data/InferenceCall")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|row| {
            matches!(
                row.get("call_state").and_then(Value::as_str),
                Some("running" | "queued")
            )
        })
        .collect())
}

fn count_inference_calls(rows: &[Value], behavior_id: Option<&str>, call_state: &str) -> i64 {
    rows.iter()
        .filter(|row| {
            row.get("call_state").and_then(Value::as_str) == Some(call_state)
                && behavior_id.is_none_or(|expected| {
                    row.get("behavior_id").and_then(Value::as_str) == Some(expected)
                })
        })
        .count() as i64
}

fn wait_for_server_exit(
    serve: &mut ServeProcess,
    timeout: Duration,
) -> Result<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = serve.child.try_wait().context("checking server exit")? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let (stdout, stderr) = serve.captured_output()?;
            return Err(anyhow!(
                "server did not exit within {timeout:?}\nstdout:\n{stdout}\nstderr:\n{stderr}"
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rejects_ephemeral_http_port_before_publishing_readiness() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let mut serve = spawn_server(&home_dir, 0)?;
    let status = wait_for_server_exit(&mut serve, Duration::from_secs(5))?;
    let (stdout, stderr) = serve.captured_output()?;

    assert!(!status.success(), "server unexpectedly exited successfully");
    assert!(
        !stdout.contains("\"status\": \"serving\""),
        "server published readiness for an unknowable ephemeral port:\n{stdout}"
    );
    assert!(
        stderr.contains("--http-port 0 is not supported")
            && stderr.contains("choose an explicit non-zero port"),
        "missing actionable ephemeral-port diagnostic:\n{stderr}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_fails_closed_when_http_port_is_occupied() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-bind-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;
    let agent_name = format!("cli-bind-{}", Uuid::new_v4().simple());
    run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--inference-url",
            mock_endpoint.endpoint(),
        ],
    )?;

    let listener =
        std::net::TcpListener::bind(("127.0.0.1", 0)).context("reserving occupied HTTP port")?;
    let port = listener.local_addr()?.port();
    let mut serve = spawn_server(&home_dir, port)?;
    let status = wait_for_server_exit(&mut serve, Duration::from_secs(10))?;
    let (stdout, stderr) = serve.captured_output()?;

    assert!(!status.success(), "server unexpectedly exited successfully");
    assert!(
        !stdout.contains("\"status\": \"serving\""),
        "server published readiness after bind failure:\n{stdout}"
    );
    assert!(
        stderr.contains("embedded HTTP listener cannot bind")
            && stderr.contains(&format!("127.0.0.1:{port}")),
        "missing actionable bind diagnostic:\n{stderr}"
    );
    drop(listener);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_home_apply_root_precedes_grok_behavior_binding() -> Result<()> {
    const GROK_BEHAVIOR: &str = "port-live";
    const APPLIED_BACKEND: &str = "fresh-applied-grok-backend";

    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    let root = tempdir.path().join("pack");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-grok-apply-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;
    let agent_name = format!("cli-grok-apply-{}", Uuid::new_v4().simple());
    run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;

    // Export the freshly initialized home as a self-contained pack, then add
    // the behavior that only --apply-root can make available to this server
    // invocation. The home itself deliberately still has no port-live row.
    run_cli_text(
        &home_dir,
        &["config", "export", "--root", &root.to_string_lossy()],
    )?;

    // The applied behavior uses a backend that does not exist when the
    // runtime's recurring prober takes its immediate startup tick. Its
    // exported runtime-owned health fields are deliberately absent, so the
    // post-apply path must probe and promote it before readiness can publish.
    let backends_dir = root.join("inference-backends");
    let existing_backend = fs::read_dir(&backends_dir)
        .context("reading exported backend directory")?
        .next()
        .ok_or_else(|| anyhow!("exported pack has no backend"))??;
    let applied_backend_dir = backends_dir.join(APPLIED_BACKEND);
    fs::create_dir_all(&applied_backend_dir)?;
    for entry in fs::read_dir(existing_backend.path()).context("reading exported backend")? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::copy(entry.path(), applied_backend_dir.join(entry.file_name()))?;
        }
    }
    let backend_path = applied_backend_dir.join("object.json");
    let mut backend = read_json_file(&backend_path)?;
    backend["backend_id"] = Value::String(APPLIED_BACKEND.to_string());
    if let Some(object) = backend.as_object_mut() {
        object.remove("probe_status");
        object.remove("last_probe");
    }
    write_json_file(&backend_path, &backend)?;

    let behaviors_dir = root.join("agent-behaviors");
    let existing = fs::read_dir(&behaviors_dir)
        .context("reading exported behavior directory")?
        .next()
        .ok_or_else(|| anyhow!("exported pack has no behavior"))??;
    let grok_behavior_dir = behaviors_dir.join(GROK_BEHAVIOR);
    fs::create_dir_all(&grok_behavior_dir)?;
    for entry in fs::read_dir(existing.path()).context("reading exported behavior")? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::copy(entry.path(), grok_behavior_dir.join(entry.file_name()))?;
        }
    }
    let behavior_path = grok_behavior_dir.join("object.json");
    let mut behavior = read_json_file(&behavior_path)?;
    behavior["behavior_id"] = Value::String(GROK_BEHAVIOR.to_string());
    behavior["backend_id"] = Value::String(APPLIED_BACKEND.to_string());
    write_json_file(&behavior_path, &behavior)?;

    let port = allocate_port()?;
    let socket_path = tempdir.path().join("grok.sock");
    let (mut serve, readiness) = spawn_server_with_ready_json(
        &home_dir,
        port,
        &[
            "--p2p-transport",
            "none",
            "--apply-root",
            root.to_str().context("pack root path is not UTF-8")?,
            "--grok-shim",
            "--grok-shim-socket-path",
            socket_path.to_str().context("socket path is not UTF-8")?,
            "--grok-shim-behavior-id",
            GROK_BEHAVIOR,
        ],
        &[],
    )?;
    wait_for_port(port, &mut serve)?;
    let (_stdout, stderr) = serve.captured_output()?;

    assert_eq!(
        readiness.pointer("/apply_root/ok").and_then(Value::as_bool),
        Some(true),
        "the pack must apply before readiness: {readiness}"
    );
    assert_eq!(
        readiness
            .pointer("/grok_shim/bound")
            .and_then(Value::as_bool),
        Some(true),
        "the behavior supplied by --apply-root must bind in the same invocation: {readiness}; stderr: {stderr}"
    );
    assert_eq!(
        readiness
            .pointer("/grok_shim/socket")
            .and_then(Value::as_str),
        socket_path.to_str()
    );
    assert!(
        socket_path.exists(),
        "the bound Grok leader must publish its socket"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_exposes_prometheus_metrics_endpoint() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-metrics-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;
    let port = allocate_port()?;
    let agent_name = format!("cli-metrics-{}", Uuid::new_v4().simple());
    let graphql = graphql_url(port);

    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--inference-url",
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let default_behavior_id = default_behavior_id_for_agent(&agent_did);

    let mut serve = spawn_server(&home_dir, port)?;
    wait_for_port(port, &mut serve)?;
    wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;

    let client = reqwest::Client::new();

    let version_response = client
        .get(format!("http://127.0.0.1:{port}/version"))
        .send()
        .await
        .context("fetching /version")?;
    assert!(
        version_response.status().is_success(),
        "unexpected /version status: {version_response:?}"
    );
    let version: Value = version_response
        .json()
        .await
        .context("reading /version body")?;
    assert_eq!(
        version.get("service").and_then(Value::as_str),
        Some("gents")
    );
    assert_eq!(version.get("binary").and_then(Value::as_str), Some("gents"));
    assert_eq!(
        version.get("package").and_then(Value::as_str),
        Some("gents-cli")
    );
    assert_eq!(
        version.get("version").and_then(Value::as_str),
        Some(env!("CARGO_PKG_VERSION"))
    );
    assert!(
        version.get("build").and_then(Value::as_object).is_some(),
        "expected build metadata in /version body: {version}"
    );

    let health_response = client
        .get(format!("http://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .context("fetching /healthz")?;
    assert!(
        health_response.status().is_success(),
        "unexpected /healthz status: {health_response:?}"
    );
    let health: Value = health_response
        .json()
        .await
        .context("reading /healthz body")?;
    assert_eq!(health.get("ok").and_then(Value::as_bool), Some(true));
    assert_eq!(health.get("status").and_then(Value::as_str), Some("ok"));
    assert_eq!(health.get("service").and_then(Value::as_str), Some("gents"));
    assert_eq!(
        health
            .pointer("/checks/runtime/ready")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert!(
        health
            .get("runtimes")
            .and_then(Value::as_array)
            .is_some_and(|runtimes| runtimes.iter().any(|runtime| {
                runtime.get("agent_did").and_then(Value::as_str) == Some(agent_did.as_str())
            })),
        "expected runtime row for {agent_did} in /healthz body: {health}"
    );

    let status_response = client
        .get(format!("http://127.0.0.1:{port}/status"))
        .send()
        .await
        .context("fetching /status")?;
    assert!(
        status_response.status().is_success(),
        "unexpected /status response: {status_response:?}"
    );
    let status: Value = status_response
        .json()
        .await
        .context("reading /status body")?;
    assert_eq!(
        status.get("agent_name").and_then(Value::as_str),
        Some(agent_name.as_str())
    );
    assert_eq!(
        status.get("agent_did").and_then(Value::as_str),
        Some(agent_did.as_str())
    );
    assert_eq!(
        status.get("graphql").and_then(Value::as_str),
        Some(graphql.as_str())
    );
    assert!(
        status
            .get("p2p_listen_addresses")
            .and_then(Value::as_array)
            .is_some_and(|rows| !rows.is_empty()),
        "expected /status to include P2P listen addresses: {status}"
    );
    assert!(
        status
            .pointer("/liveness/active_native_executors")
            .and_then(Value::as_array)
            .is_some(),
        "expected /status liveness to include active_native_executors: {status}"
    );
    assert_eq!(
        status
            .pointer("/liveness/active_native_executors_available")
            .and_then(Value::as_bool),
        Some(true)
    );

    for mutation in [
        format!(
            r#"mutation {{ create_AgentSession(input: {{ session_id: "self-budget-session", agent_name: "{}", behavior_id: "{}", started: "2026-06-02T09:59:00Z", status: "active" }}) {{ _docID }} }}"#,
            escape_graphql_string(&agent_name),
            escape_graphql_string(&default_behavior_id),
        ),
        format!(
            r#"mutation {{ create_AgentRequest(input: {{ request_id: "self-budget-req", agent_did: "{agent_did}", session_id: "self-budget-session", lifecycle_state: "completed", created_at: "2026-06-02T10:00:00Z" }}) {{ _docID }} }}"#
        ),
        r#"mutation { create_AgentMessage(input: { message_key: "self-budget-session:1", session_id: "self-budget-session", sequence: 1, role: "user", content: "hello", timestamp: "2026-06-02T10:01:00Z" }) { _docID } }"#.to_string(),
        r#"mutation { create_CompactionEntry(input: { compaction_key: "self-budget-ce", session_id: "self-budget-session", sequence: 1, original_tokens: 1234, compacted_tokens: 567, created_at: "2026-06-02T10:00:00Z" }) { _docID } }"#.to_string(),
    ] {
        graphql_query(&graphql, &mutation)
            .await
            .context("seeding self-view fixtures")?;
    }

    let status_response = client
        .get(format!("http://127.0.0.1:{port}/status"))
        .send()
        .await
        .context("fetching /status after seeding context fixtures")?;
    assert!(
        status_response.status().is_success(),
        "unexpected /status response: {status_response:?}"
    );
    let status: Value = status_response
        .json()
        .await
        .context("reading /status body")?;
    let behaviors = status
        .get("behaviors")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty())
        .unwrap_or_else(|| panic!("expected /status to include behaviors: {status}"));
    assert!(
        behaviors.iter().any(|behavior| {
            behavior.get("model_name").and_then(Value::as_str) == Some(model_name.as_str())
                && behavior
                    .get("endpoint")
                    .and_then(Value::as_str)
                    .is_some_and(|endpoint| !endpoint.is_empty())
        }),
        "expected /status behavior joined with backend endpoint for model {model_name}: {status}"
    );
    let budget = status
        .get("context_budget")
        .unwrap_or_else(|| panic!("expected /status to include context_budget: {status}"));
    assert_eq!(
        budget.get("compaction_count").and_then(Value::as_i64),
        Some(1),
        "expected agent-scoped context_budget to count exactly the seeded compaction: {status}"
    );
    assert_eq!(
        budget.get("latest_original_tokens").and_then(Value::as_i64),
        Some(1234),
        "expected context_budget latest tokens from the seeded compaction: {status}"
    );
    let context = status
        .get("context")
        .unwrap_or_else(|| panic!("expected /status to include context indicator: {status}"));
    assert_eq!(
        context.get("compaction_count").and_then(Value::as_i64),
        Some(1),
        "expected /status context to mirror compaction count: {status}"
    );
    assert_eq!(
        context.get("current_estimate").and_then(Value::as_i64),
        Some(567),
        "expected /status context current_estimate from latest compacted tokens: {status}"
    );

    let sessions_response = client
        .get(format!("http://127.0.0.1:{port}/sessions?limit=1"))
        .send()
        .await
        .context("fetching /sessions")?;
    assert!(
        sessions_response.status().is_success(),
        "unexpected /sessions response: {sessions_response:?}"
    );
    let sessions: Value = sessions_response
        .json()
        .await
        .context("reading /sessions body")?;
    assert_eq!(
        sessions.get("agent_did").and_then(Value::as_str),
        Some(agent_did.as_str())
    );
    let session = sessions
        .get("sessions")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .unwrap_or_else(|| panic!("expected /sessions to include the seeded row: {sessions}"));
    assert_eq!(
        session.get("session_id").and_then(Value::as_str),
        Some("self-budget-session")
    );
    assert_eq!(
        session.get("request_count").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        session.get("message_count").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        session.get("compaction_count").and_then(Value::as_i64),
        Some(1)
    );

    let fleet_response = client
        .get(format!("http://127.0.0.1:{port}/fleet"))
        .send()
        .await
        .context("fetching /fleet")?;
    assert!(
        fleet_response.status().is_success(),
        "unexpected /fleet response: {fleet_response:?}"
    );
    let fleet: Value = fleet_response.json().await.context("reading /fleet body")?;
    assert!(
        fleet
            .get("agents")
            .and_then(Value::as_array)
            .is_some_and(|agents| agents.iter().any(|agent| {
                agent.get("agent_did").and_then(Value::as_str) == Some(agent_did.as_str())
                    && agent.get("process_state").and_then(Value::as_str) == Some("ready")
            })),
        "expected /fleet to list this agent in ready state: {fleet}"
    );

    let escaped_agent_did = escape_graphql_string(&agent_did);
    for mutation in [
        r#"mutation {
            create_ToolServiceRegistry(input: {
                service_id: "runtime-mcp-pool-obs",
                display_name: "Runtime Observability",
                description: "Runtime endpoint fixture",
                hostname: "studio-1",
                tailscale_ip: "100.64.0.10",
                lan_ip: "192.168.1.10",
                mcp_port: 9201,
                mcp_path: "/mcp",
                send_agent_did: true,
                status: "online",
                version: "test",
                updated_at: "2026-06-05T00:00:00Z"
            }) { _docID }
        }"#
        .to_string(),
        format!(
            r#"mutation {{
                create_ToolServiceHealthState(input: {{
                    service_id: "runtime-mcp-pool-obs",
                    agent_did: "{escaped_agent_did}",
                    endpoint: "http://100.64.0.10:9201/mcp",
                    status: "healthy",
                    tool_count: 3,
                    failure_count: 0,
                    k_max: 3,
                    last_probe_at: "2026-06-05T00:00:00Z",
                    last_seen: "2026-06-05T00:00:00Z",
                    updated_at: "2026-06-05T00:00:00Z"
                }}) {{ _docID }}
            }}"#
        ),
    ] {
        graphql_query(&graphql, &mutation)
            .await
            .context("seeding MCP pool fixtures")?;
    }

    let mcp_pool_response = client
        .get(format!("http://127.0.0.1:{port}/mcp/pool"))
        .send()
        .await
        .context("fetching /mcp/pool")?;
    assert!(
        mcp_pool_response.status().is_success(),
        "unexpected /mcp/pool response: {mcp_pool_response:?}"
    );
    let mcp_pool: Value = mcp_pool_response
        .json()
        .await
        .context("reading /mcp/pool body")?;
    assert_eq!(
        mcp_pool.get("agent_did").and_then(Value::as_str),
        Some(agent_did.as_str())
    );
    assert_eq!(
        mcp_pool.pointer("/totals/online").and_then(Value::as_i64),
        Some(1),
        "expected /mcp/pool totals to count the seeded online service: {mcp_pool}"
    );
    assert_eq!(
        mcp_pool.pointer("/totals/healthy").and_then(Value::as_i64),
        Some(1),
        "expected /mcp/pool totals to count the seeded healthy service: {mcp_pool}"
    );
    assert!(
        mcp_pool
            .get("services")
            .and_then(Value::as_array)
            .is_some_and(|services| services.iter().any(|service| {
                service.get("service_id").and_then(Value::as_str) == Some("runtime-mcp-pool-obs")
                    && service.get("tool_count").and_then(Value::as_i64) == Some(3)
                    && service.get("health_status").and_then(Value::as_str) == Some("healthy")
            })),
        "expected /mcp/pool to include the seeded service and tool count: {mcp_pool}"
    );

    let mcp_off = client
        .get(format!("http://127.0.0.1:{port}/mcp"))
        .send()
        .await
        .context("probing /mcp")?;
    assert_eq!(
        mcp_off.status(),
        reqwest::StatusCode::NOT_FOUND,
        "expected /mcp to be absent without --enable-mcp"
    );

    let response = client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()
        .await
        .context("fetching /metrics")?;
    assert!(
        response.status().is_success(),
        "unexpected status: {response:?}"
    );
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("text/plain"),
        "unexpected content-type: {content_type}"
    );
    let body = response.text().await.context("reading /metrics body")?;
    assert!(
        body.contains("# HELP gents_up"),
        "expected gents_up help text in metrics body:\n{body}"
    );
    assert!(
        body.contains(r#"gents_up 1"#),
        "expected gents_up sample in metrics body:\n{body}"
    );
    assert!(
        body.contains(&format!(
            r#"gents_runtime_process_state{{agent_did="{agent_did}",state="ready"}} 1"#
        )),
        "expected ready process-state metric in metrics body:\n{body}"
    );
    assert!(
        body.contains(&format!(
            r#"gents_runtime_active_generation{{agent_did="{agent_did}"}}"#
        )),
        "expected active-generation metric in metrics body:\n{body}"
    );
    assert!(
        body.contains("gents_backend_enabled"),
        "expected backend metrics in metrics body:\n{body}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_exposes_fleet_slot_snapshot_endpoint() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-fleet-slots-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start_hanging(&model_name)?;
    let port = allocate_port()?;
    let agent_name = format!("cli-fleet-slots-{}", Uuid::new_v4().simple());
    let graphql = graphql_url(port);
    let request_content = format!("fleet slots live request {}", Uuid::new_v4());

    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--max-concurrent",
            "1",
            "--max-queue-depth",
            "2",
            "--inference-url",
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let backend_id = init
        .pointer("/init/backend_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("init output missing backend_id: {init}"))?
        .to_string();
    let default_behavior_id = init
        .pointer("/init/default_behavior_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| default_behavior_id_for_agent(&agent_did));

    let mut serve = spawn_server(&home_dir, port)?;
    wait_for_port(port, &mut serve)?;
    wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;

    let submitted = run_cli_json(
        &home_dir,
        &[
            "request",
            "submit",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--content",
            &request_content,
            "--no-wait",
        ],
    )?;
    let request_id = submitted
        .get("request_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("request submit output missing request_id: {submitted}"))?
        .to_string();
    wait_for_request_lifecycle_state(
        &graphql,
        &request_id,
        &["processing"],
        Duration::from_secs(30),
    )
    .await?;
    wait_for_inference_call_state(&graphql, &request_id, "running").await?;

    let client = reqwest::Client::new();
    let stable_deadline = Instant::now() + Duration::from_secs(5);
    let (
        snapshot,
        active_calls,
        expected_backend_running,
        expected_backend_queued,
        expected_behavior_running,
        expected_behavior_queued,
    ) = loop {
        let response = client
            .get(format!("http://127.0.0.1:{port}/fleet/slots"))
            .send()
            .await
            .context("fetching /fleet/slots")?;
        assert!(
            response.status().is_success(),
            "unexpected /fleet/slots response: {response:?}"
        );
        let snapshot: Value = response.json().await.context("reading /fleet/slots body")?;
        let active_calls = active_inference_calls_for_backend(&graphql, &backend_id).await?;
        let backend_running = count_inference_calls(&active_calls, None, "running");
        let backend_queued = count_inference_calls(&active_calls, None, "queued");
        let snapshot_running = snapshot.pointer("/totals/assigned").and_then(Value::as_i64);
        let snapshot_queued = snapshot.pointer("/totals/queued").and_then(Value::as_i64);
        if snapshot_running == Some(backend_running) && snapshot_queued == Some(backend_queued) {
            break (
                snapshot,
                active_calls.clone(),
                backend_running,
                backend_queued,
                count_inference_calls(&active_calls, Some(&default_behavior_id), "running"),
                count_inference_calls(&active_calls, Some(&default_behavior_id), "queued"),
            );
        }
        if Instant::now() >= stable_deadline {
            return Err(anyhow!(
                "fleet slot snapshot did not stabilize with active inference calls; snapshot={snapshot}; active_calls={}",
                Value::Array(active_calls)
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let expected_available = 1_i64.saturating_sub(expected_backend_running);
    assert!(
        expected_backend_running >= 1,
        "test setup should hold at least one running call; active calls={}",
        Value::Array(active_calls)
    );

    assert_eq!(
        snapshot.pointer("/source").and_then(Value::as_str),
        Some("graphql.derived_admission_state")
    );
    assert_eq!(
        snapshot.pointer("/totals/assigned").and_then(Value::as_i64),
        Some(expected_backend_running)
    );
    assert_eq!(
        snapshot
            .pointer("/totals/available")
            .and_then(Value::as_i64),
        Some(expected_available)
    );
    assert_eq!(
        snapshot.pointer("/totals/max").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        snapshot.pointer("/totals/queued").and_then(Value::as_i64),
        Some(expected_backend_queued)
    );
    assert_eq!(
        snapshot
            .pointer("/expired/processing_requests")
            .and_then(Value::as_i64),
        Some(0)
    );

    let backend = find_snapshot_row(&snapshot, "backends", "backend_id", &backend_id)?;
    assert_eq!(
        backend.get("running").and_then(Value::as_i64),
        Some(expected_backend_running)
    );
    assert_eq!(
        backend.get("queued").and_then(Value::as_i64),
        Some(expected_backend_queued)
    );
    assert_eq!(
        backend.get("available").and_then(Value::as_i64),
        Some(expected_available)
    );
    assert_eq!(
        backend.get("max_concurrent").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        backend.get("max_queue_depth").and_then(Value::as_i64),
        Some(2)
    );
    assert_eq!(
        backend.get("accepting_admission").and_then(Value::as_bool),
        Some(true)
    );

    let behavior = find_snapshot_row(&snapshot, "behaviors", "behavior_id", &default_behavior_id)?;
    assert_eq!(
        behavior.get("backend_id").and_then(Value::as_str),
        Some(backend_id.as_str())
    );
    assert_eq!(
        behavior.get("assigned").and_then(Value::as_i64),
        Some(expected_behavior_running)
    );
    assert_eq!(
        behavior.get("available").and_then(Value::as_i64),
        Some(expected_available)
    );
    assert_eq!(behavior.get("max").and_then(Value::as_i64), Some(1));
    assert_eq!(
        behavior.get("queued").and_then(Value::as_i64),
        Some(expected_behavior_queued)
    );

    let cli_snapshot = run_cli_json(&home_dir, &["fleet", "slots", "--graphql", &graphql])?;
    assert_eq!(
        cli_snapshot.pointer("/totals/assigned"),
        snapshot.pointer("/totals/assigned")
    );
    assert_eq!(
        cli_snapshot.pointer("/backends/0/backend_id"),
        snapshot.pointer("/backends/0/backend_id")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rejects_real_initialized_did_without_key_path() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_env = tempdir.path().join("home-env");
    let agent_home = home_env.join(".gents");
    fs::create_dir_all(&agent_home)?;

    let agent_did = format!("did:key:z{}", Uuid::new_v4().simple());
    write_json_file(
        &agent_home.join("init.json"),
        &serde_json::json!({
            "home": agent_home.to_string_lossy(),
            "agent_name": "mini-1-steward",
            "agent_did": agent_did,
            "key_path": null,
            "tool_ceiling": "Readonly",
            "tool_root": tempdir.path().to_string_lossy()
        }),
    )?;

    let port = allocate_port()?;
    let stderr = run_cli_failure_stderr(
        &home_env,
        &[
            "server",
            "--home",
            agent_home.to_str().expect("utf-8 home"),
            "--http-port",
            &port.to_string(),
        ],
    )?;
    assert!(
        stderr.contains("has no key_path and unsupported identity_backend"),
        "expected no-key-path/backend error, got:\n{stderr}"
    );
    assert!(
        !agent_home.join("keys").exists(),
        "server must not create a fallback file-key identity for a no-key initialized home"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rejects_macos_keychain_identity_without_label() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_env = tempdir.path().join("home-env");
    let agent_home = home_env.join(".gents");
    fs::create_dir_all(&agent_home)?;

    let agent_did = format!("did:key:z{}", Uuid::new_v4().simple());
    write_json_file(
        &agent_home.join("init.json"),
        &serde_json::json!({
            "home": agent_home.to_string_lossy(),
            "agent_name": "mini-1-steward",
            "agent_did": agent_did,
            "key_path": null,
            "identity_backend": "macos-keychain",
            "tool_ceiling": "Readonly",
            "tool_root": tempdir.path().to_string_lossy()
        }),
    )?;

    let port = allocate_port()?;
    let stderr = run_cli_failure_stderr(
        &home_env,
        &[
            "server",
            "--home",
            agent_home.to_str().expect("utf-8 home"),
            "--http-port",
            &port.to_string(),
        ],
    )?;
    assert!(
        stderr.contains("macos-keychain"),
        "expected macos-keychain error, got:\n{stderr}"
    );
    assert!(
        stderr.contains("no keychain_label"),
        "expected missing keychain label error, got:\n{stderr}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rejects_real_initialized_did_with_missing_key_file_without_creating_it(
) -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_env = tempdir.path().join("home-env");
    let agent_home = home_env.join(".gents");
    let key_path = agent_home.join("keys").join("missing.key");
    fs::create_dir_all(&agent_home)?;

    let agent_did = format!("did:key:z{}", Uuid::new_v4().simple());
    write_json_file(
        &agent_home.join("init.json"),
        &serde_json::json!({
            "home": agent_home.to_string_lossy(),
            "agent_name": "mini-1-steward",
            "agent_did": agent_did,
            "key_path": key_path.to_string_lossy(),
            "tool_ceiling": "Readonly",
            "tool_root": tempdir.path().to_string_lossy()
        }),
    )?;

    let port = allocate_port()?;
    let stderr = run_cli_failure_stderr(
        &home_env,
        &[
            "server",
            "--home",
            agent_home.to_str().expect("utf-8 home"),
            "--http-port",
            &port.to_string(),
        ],
    )?;
    assert!(
        stderr.contains("requires identity key"),
        "expected missing-key error, got:\n{stderr}"
    );
    assert!(
        stderr.contains("to already exist"),
        "expected no-create hint, got:\n{stderr}"
    );
    assert!(
        !key_path.exists(),
        "server must not create a new key for a real initialized DID with missing key file"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_startup_with_iroh_p2p_reports_runtime_connectivity() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-p2p-ready-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;

    let port = allocate_port()?;
    let agent_name = format!("cli-p2p-ready-{}", Uuid::new_v4().simple());
    let graphql = graphql_url(port);

    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--inference-url",
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let default_behavior_id = default_behavior_id_for_agent(&agent_did);
    let (mut serve, readiness) = spawn_server_with_ready_json(
        &home_dir,
        port,
        &[
            "--p2p-bind-addr",
            "127.0.0.1",
            "--p2p-port",
            "0",
            "--p2p-relay-mode",
            "disabled",
            "--p2p-discovery",
            "disabled",
        ],
        &[],
    )?;
    wait_for_port(port, &mut serve)?;
    wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;

    assert_eq!(
        readiness.get("agent_did").and_then(Value::as_str),
        Some(agent_did.as_str())
    );
    assert_eq!(
        readiness.get("graphql").and_then(Value::as_str),
        Some(graphql.as_str())
    );
    assert_eq!(
        readiness.get("default_behavior_id").and_then(Value::as_str),
        Some(default_behavior_id.as_str())
    );
    assert_eq!(
        readiness.get("p2p_transport").and_then(Value::as_str),
        Some("iroh")
    );
    assert!(readiness
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty()));
    assert!(readiness
        .get("p2p_listen_addresses")
        .and_then(Value::as_array)
        .is_some_and(|rows| !rows.is_empty()));

    let status_response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/status"))
        .send()
        .await
        .context("fetching /status")?;
    assert!(
        status_response.status().is_success(),
        "unexpected /status response: {status_response:?}"
    );
    let status: Value = status_response
        .json()
        .await
        .context("reading /status body")?;
    assert_eq!(
        status.get("agent_did").and_then(Value::as_str),
        Some(agent_did.as_str())
    );
    assert_eq!(
        status.get("agent_name").and_then(Value::as_str),
        Some(agent_name.as_str())
    );
    assert_eq!(
        status.get("p2p_transport").and_then(Value::as_str),
        Some("iroh")
    );
    assert!(status
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty()));
    assert!(status
        .get("p2p_listen_addresses")
        .and_then(Value::as_array)
        .is_some_and(|rows| !rows.is_empty()));

    let runtime_state = read_runtime_state_json(&home_dir)?;
    assert_eq!(
        runtime_state.get("p2p_transport").and_then(Value::as_str),
        Some("iroh")
    );
    assert_eq!(
        runtime_state.get("p2p_peer_id"),
        readiness.get("p2p_peer_id")
    );
    assert_eq!(
        runtime_state.get("p2p_listen_addresses"),
        readiness.get("p2p_listen_addresses")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_startup_defaults_to_iroh_p2p_for_desktop_pairing() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-default-iroh-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;

    let port = allocate_port()?;
    let agent_name = format!("cli-default-iroh-{}", Uuid::new_v4().simple());
    let graphql = graphql_url(port);

    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--inference-url",
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let (mut serve, readiness) = spawn_server_with_ready_json(&home_dir, port, &[], &[])?;
    wait_for_port(port, &mut serve)?;
    wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;

    assert_eq!(
        readiness.get("p2p_transport").and_then(Value::as_str),
        Some("iroh")
    );
    assert!(readiness
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty()));
    assert!(readiness
        .get("p2p_listen_addresses")
        .and_then(Value::as_array)
        .is_some_and(|rows| !rows.is_empty()));

    let runtime_state = read_runtime_state_json(&home_dir)?;
    assert_eq!(
        runtime_state.get("p2p_transport").and_then(Value::as_str),
        Some("iroh")
    );
    assert_eq!(
        runtime_state.get("p2p_peer_id"),
        readiness.get("p2p_peer_id")
    );
    assert_eq!(
        runtime_state.get("p2p_listen_addresses"),
        readiness.get("p2p_listen_addresses")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_starts_in_degraded_mode_when_backend_is_unavailable() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-degraded-model-{}", Uuid::new_v4().simple());
    let warm_port = allocate_port()?;
    let port = allocate_port()?;
    let agent_name = format!("cli-degraded-{}", Uuid::new_v4().simple());
    let graphql = graphql_url(port);

    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--inference-url",
            "http://127.0.0.1:9/v1",
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let backend_id = init
        .pointer("/init/backend_id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("init output missing backend_id: {init}"))?
        .to_string();

    let mut warm_server = spawn_server(&home_dir, warm_port)?;
    wait_for_port(warm_port, &mut warm_server)?;
    wait_for_runtime_ready(&graphql_url(warm_port), &agent_did, Duration::from_secs(30)).await?;
    run_cli_json(
        &home_dir,
        &[
            "config",
            "backend",
            "set",
            "--graphql",
            &graphql_url(warm_port),
            "--backend-id",
            &backend_id,
            "--name",
            &backend_id,
            "--provider-kind",
            "OpenAiCompatible",
            "--endpoint",
            "http://127.0.0.1:9/v1",
            "--max-concurrent",
            "1",
            "--probe-status",
            "unknown",
        ],
    )?;
    warm_server
        .child
        .kill()
        .context("stopping warm server after backend downgrade")?;
    warm_server
        .child
        .wait()
        .context("waiting for warm server shutdown")?;

    let (mut serve, readiness) = spawn_server_with_ready_json(&home_dir, port, &[], &[])?;
    wait_for_port(port, &mut serve)?;
    wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;

    assert_eq!(
        readiness.get("status").and_then(Value::as_str),
        Some("serving")
    );
    assert_eq!(
        readiness.get("readiness_status").and_then(Value::as_str),
        Some("degraded")
    );
    assert_eq!(
        readiness
            .get("runnable_behaviors")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );
    let unavailable = readiness
        .get("unavailable_behaviors")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("readiness missing unavailable_behaviors: {readiness}"))?;
    assert_eq!(unavailable.len(), 1);
    let reason = unavailable
        .first()
        .and_then(|entry| entry.get("message"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    assert!(
        reason.contains("inference backend is temporarily unavailable"),
        "unexpected unavailable reason: {reason}"
    );
    assert_eq!(
        readiness.get("graphql").and_then(Value::as_str),
        Some(graphql.as_str())
    );

    let status = run_cli_json(&home_dir, &["status"])?;
    assert_eq!(
        status.get("process_state").and_then(Value::as_str),
        Some("ready")
    );
    assert_eq!(
        status.get("readiness_status").and_then(Value::as_str),
        Some("degraded")
    );
    assert_eq!(
        status
            .get("runnable_behavior_count")
            .and_then(Value::as_i64),
        Some(0)
    );
    let status_unavailable = status
        .get("unavailable_behaviors")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("status output missing unavailable_behaviors: {status}"))?;
    assert_eq!(status_unavailable.len(), 1);
    let status_reason = status_unavailable
        .values()
        .next()
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    assert!(
        status_reason.contains("inference backend is temporarily unavailable"),
        "unexpected status unavailable reason: {status_reason}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_and_server_use_backend_specific_api_key_env_var() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-auth-model-{}", Uuid::new_v4().simple());
    let expected_reply = "AUTH_BACKEND_OK";
    let mock_endpoint = MockChatEndpoint::start_with_required_bearer(
        &model_name,
        expected_reply,
        Some("backend-key"),
    )?;

    let port = allocate_port()?;
    let agent_name = format!("cli-auth-{}", Uuid::new_v4().simple());
    let graphql = graphql_url(port);

    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--api-key-env-var",
            "GENTS_TEST_CLI_BACKEND_KEY",
            "--inference-url",
            mock_endpoint.endpoint(),
        ],
    )?;
    assert_eq!(
        init.pointer("/init/api_key_env_var")
            .and_then(Value::as_str),
        Some("GENTS_TEST_CLI_BACKEND_KEY")
    );
    let agent_did = agent_did_from_init(&init)?;
    let backend_id = generated_backend_id_for_agent(&agent_did);
    let tool_selection_id = generated_tool_selection_id_for_agent(&agent_did);

    let (_serve, readiness) = spawn_server_with_ready_json(
        &home_dir,
        port,
        &[],
        &[("GENTS_TEST_CLI_BACKEND_KEY", "backend-key")],
    )?;
    assert_eq!(
        readiness.get("graphql").and_then(Value::as_str),
        Some(graphql.as_str()),
        "serving payload must identify the allocated GraphQL endpoint"
    );

    assert_runtime_init_state(
        &graphql,
        &agent_did,
        &backend_id,
        mock_endpoint.endpoint(),
        "OpenAiCompatible",
        None,
        Some("GENTS_TEST_CLI_BACKEND_KEY"),
        &model_name,
        &tool_selection_id,
        "ReadOnly",
        "ReadOnly",
        "read-only operating mode",
    )
    .await?;

    let output = run_cli_text(
        &home_dir,
        &[
            "chat",
            "backend auth should flow through the configured env var",
        ],
    )?;
    assert!(
        output.contains(expected_reply),
        "expected chat output to contain {expected_reply}, got:\n{output}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_command_reconstructs_a_trace() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-query-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;
    let port = allocate_port()?;
    let agent_name = format!("cli-query-{}", Uuid::new_v4().simple());
    let graphql = graphql_url(port);

    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--inference-url",
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;

    let mut serve = spawn_server(&home_dir, port)?;
    wait_for_port(port, &mut serve)?;
    wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;

    let mutations = [
        format!(
            r#"mutation {{ create_AgentRequest(input: {{ request_id: "trace-req", agent_did: "{agent_did}", session_id: "trace-session", lifecycle_state: "completed", content: "hi", created_at: "2026-06-03T10:00:00Z" }}) {{ _docID }} }}"#
        ),
        r#"mutation { create_AgentResponse(input: { response_key: "trace-resp", request_id: "trace-req", session_id: "trace-session", content: "hello", status: "completed", token_count: 7 }) { _docID } }"#.to_string(),
        r#"mutation { create_AgentMessage(input: { message_key: "trace-msg", session_id: "trace-session", sequence: 1, role: "assistant", content: "encoded-blob" }) { _docID } }"#.to_string(),
        r#"mutation { create_AgentToolCall(input: { tool_call_key: "trace-tc", request_id: "trace-req", session_id: "trace-session", tool_name: "defra_query", args: "{\"collection\":\"AgentRequest\"}", result: "{\"ok\":true}", status: "completed" }) { _docID } }"#.to_string(),
    ];
    for mutation in mutations {
        graphql_query(&graphql, &mutation)
            .await
            .context("seeding trace")?;
    }

    let request = run_cli_json(
        &home_dir,
        &[
            "query",
            "--graphql",
            &graphql,
            "--collection",
            "AgentRequest",
            "--field",
            "request_id",
            "--field",
            "session_id",
            "--field",
            "lifecycle_state",
            "--filter",
            r#"{"request_id":{"_eq":"trace-req"}}"#,
        ],
    )?;
    assert_eq!(
        request.get("count").and_then(Value::as_i64),
        Some(1),
        "{request}"
    );
    let req_row = &request["results"][0];
    assert_eq!(req_row["session_id"].as_str(), Some("trace-session"));
    assert_eq!(req_row["lifecycle_state"].as_str(), Some("completed"));

    let tool_calls = run_cli_json(
        &home_dir,
        &[
            "query",
            "--graphql",
            &graphql,
            "--collection",
            "AgentToolCall",
            "--field",
            "request_id",
            "--field",
            "tool_name",
            "--field",
            "args",
            "--field",
            "result",
            "--field",
            "status",
            "--filter",
            r#"{"request_id":{"_eq":"trace-req"}}"#,
        ],
    )?;
    assert_eq!(
        tool_calls.get("count").and_then(Value::as_i64),
        Some(1),
        "{tool_calls}"
    );
    let tc = &tool_calls["results"][0];
    assert_eq!(tc["tool_name"].as_str(), Some("defra_query"));
    let tc_args: Value = serde_json::from_str(tc["args"].as_str().unwrap())
        .context("tool call args parse as JSON")?;
    assert_eq!(tc_args["collection"].as_str(), Some("AgentRequest"));

    let responses = run_cli_json(
        &home_dir,
        &[
            "query",
            "--graphql",
            &graphql,
            "--collection",
            "AgentResponse",
            "--field",
            "request_id",
            "--field",
            "status",
            "--field",
            "token_count",
            "--filter",
            r#"{"request_id":{"_eq":"trace-req"}}"#,
        ],
    )?;
    let resp_row = &responses["results"][0];
    assert_eq!(resp_row["token_count"].as_i64(), Some(7));

    let messages = run_cli_json(
        &home_dir,
        &[
            "query",
            "--graphql",
            &graphql,
            "--collection",
            "AgentMessage",
            "--field",
            "session_id",
            "--field",
            "role",
            "--field",
            "sequence",
            "--filter",
            r#"{"session_id":{"_eq":"trace-session"}}"#,
        ],
    )?;
    let msg_row = &messages["results"][0];
    assert_eq!(msg_row["role"].as_str(), Some("assistant"));

    assert_eq!(
        req_row["session_id"].as_str(),
        msg_row["session_id"].as_str()
    );
    assert_eq!(tc["request_id"].as_str(), resp_row["request_id"].as_str());

    let denied = run_cli_failure_stderr(
        &home_dir,
        &[
            "query",
            "--graphql",
            &graphql,
            "--collection",
            "InferenceBackend",
            "--field",
            "api_key",
        ],
    )?;
    assert!(
        denied.contains("restricted"),
        "expected secret guard to fire: {denied}"
    );

    let diagnostic = run_cli_failure_stderr(
        &home_dir,
        &[
            "query",
            "--graphql",
            &graphql,
            "--collection",
            "AgentToolCall",
            "--field",
            "created_at",
        ],
    )?;
    assert!(diagnostic.contains("created_at"), "{diagnostic}");
    assert!(
        diagnostic.contains("started_at") && diagnostic.contains("completed_at"),
        "suggestions missing: {diagnostic}"
    );
    assert!(
        diagnostic.contains("tool_call_key"),
        "field inventory missing: {diagnostic}"
    );

    let inventory = run_cli_json(
        &home_dir,
        &[
            "query",
            "--graphql",
            &graphql,
            "--collection",
            "InferenceBackend",
            "--field",
            "*",
        ],
    )?;
    assert_eq!(inventory["discovery"], Value::Bool(true), "{inventory}");
    let field_names: Vec<&str> = inventory["fields"]
        .as_array()
        .context("discovery fields array")?
        .iter()
        .filter_map(|f| f["name"].as_str())
        .collect();
    assert!(field_names.contains(&"backend_id"), "{field_names:?}");
    assert!(
        !field_names.contains(&"api_key") && !field_names.contains(&"api_key_env_var"),
        "secret leaked into discovery inventory: {field_names:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_endpoint_serves_defra_query() -> Result<()> {
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use rmcp::ServiceExt;

    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-mcp-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockModelEndpoint::start(&model_name)?;
    let port = allocate_port()?;
    let agent_name = format!("cli-mcp-{}", Uuid::new_v4().simple());
    let graphql = graphql_url(port);

    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--inference-url",
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;

    let mut serve = spawn_server_with_env(&home_dir, port, &["--enable-mcp"], &[])?;
    wait_for_port(port, &mut serve)?;
    wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;

    for mutation in [
        format!(
            r#"mutation {{ create_AgentRequest(input: {{ request_id: "mcp-req", agent_did: "{agent_did}", session_id: "mcp-session", lifecycle_state: "completed", created_at: "2026-06-03T10:00:00Z" }}) {{ _docID }} }}"#
        ),
        r#"mutation { create_AgentToolCall(input: { tool_call_key: "mcp-tc", request_id: "mcp-req", session_id: "mcp-session", tool_name: "defra_query", args: "{\"collection\":\"AgentRequest\"}", result: "{\"ok\":true}", status: "completed" }) { _docID } }"#.to_string(),
    ] {
        graphql_query(&graphql, &mutation)
            .await
            .context("seeding mcp trace")?;
    }

    let config =
        StreamableHttpClientTransportConfig::with_uri(format!("http://127.0.0.1:{port}/mcp"));
    let transport = rmcp::transport::StreamableHttpClientTransport::from_config(config);
    let mcp = ().serve(transport).await.context("MCP client handshake with /mcp")?;

    let tools = mcp.peer().list_tools(None).await.context("list_tools")?;
    assert!(
        tools
            .tools
            .iter()
            .any(|tool| tool.name.as_ref() == "defra_query"),
        "expected defra_query in advertised tools: {:?}",
        tools
            .tools
            .iter()
            .map(|t| t.name.as_ref())
            .collect::<Vec<_>>()
    );

    let args = serde_json::json!({
        "collection": "AgentToolCall",
        "fields": ["request_id", "tool_name", "args", "result", "status"],
        "filter": { "request_id": { "_eq": "mcp-req" } }
    });
    let params =
        CallToolRequestParams::new("defra_query").with_arguments(args.as_object().unwrap().clone());
    let result = mcp
        .peer()
        .call_tool(params)
        .await
        .context("call_tool defra_query")?;
    let text = result
        .content
        .iter()
        .filter_map(|content| content.raw.as_text().map(|t| t.text.clone()))
        .collect::<Vec<_>>()
        .join("");
    let payload: Value = serde_json::from_str(&text).context("MCP tool result is JSON")?;
    assert_eq!(payload["count"].as_i64(), Some(1), "{payload}");
    let tc = &payload["results"][0];
    assert_eq!(tc["tool_name"].as_str(), Some("defra_query"));
    assert_eq!(tc["request_id"].as_str(), Some("mcp-req"));

    let denied_args =
        serde_json::json!({ "collection": "InferenceBackend", "fields": ["api_key"] });
    let denied_params = CallToolRequestParams::new("defra_query")
        .with_arguments(denied_args.as_object().unwrap().clone());
    let denied = mcp.peer().call_tool(denied_params).await;
    let blocked = match denied {
        Err(_) => true,
        Ok(result) => {
            result.is_error == Some(true)
                || result.content.iter().any(|content| {
                    content
                        .raw
                        .as_text()
                        .map(|t| t.text.contains("restricted"))
                        .unwrap_or(false)
                })
        }
    };
    assert!(
        blocked,
        "expected MCP defra_query to block api_key selection"
    );

    let _ = mcp.cancel().await;
    Ok(())
}
