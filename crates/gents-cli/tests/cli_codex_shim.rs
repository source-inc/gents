mod support;
use support::*;

#[path = "cli_codex_shim/helpers.rs"]
mod shim_helpers;
use shim_helpers::workspace_root;
use shim_helpers::*;

use std::fs;
use std::process::Command;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use gents::subagent_target_entry;
use gents_codex_protocol as codex;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

type ShimWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
const LIVE_CODEX_SHIM_TIMEOUT_SECS: &str = "900";

fn gents_model_selection_id(backend_id: &str, model_name: &str) -> String {
    format!("{backend_id}::{model_name}")
}

fn default_backend_id(agent_did: &str) -> String {
    format!("{agent_did}:backend")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_handshake_requires_configured_bearer_token() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;
    let model_name = format!("mock-auth-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "unused")?;
    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            "authenticated-shim",
            "--model-name",
            &model_name,
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let server_port = allocate_port()?;
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-auth-token-env",
            "GENTS_SHIM_TEST_TOKEN",
        ],
        &[("GENTS_SHIM_TEST_TOKEN", "correct-secret")],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    let url = format!("ws://127.0.0.1:{shim_port}/");
    let health = reqwest::get(format!("http://127.0.0.1:{server_port}/healthz"))
        .await?
        .json::<Value>()
        .await?;
    assert_eq!(
        health
            .pointer("/checks/codex_shim/auth_required")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        health
            .pointer("/checks/codex_shim/bound_agent_did")
            .and_then(Value::as_str),
        Some(agent_did.as_str())
    );
    assert!(!health.to_string().contains("correct-secret"));

    let unauthenticated = connect_async(&url)
        .await
        .expect_err("missing token rejected");
    assert_http_status(unauthenticated, StatusCode::UNAUTHORIZED)?;

    let mut wrong_request = url.clone().into_client_request()?;
    wrong_request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer wrong-secret"),
    );
    let wrong = connect_async(wrong_request)
        .await
        .expect_err("wrong token rejected");
    assert_http_status(wrong, StatusCode::UNAUTHORIZED)?;

    let mut request = url.into_client_request()?;
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer correct-secret"),
    );
    let (mut websocket, _) = connect_async(request).await?;
    websocket.close(None).await?;
    Ok(())
}

fn assert_http_status(
    error: tokio_tungstenite::tungstenite::Error,
    expected: StatusCode,
) -> Result<()> {
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        bail!("expected HTTP {expected}, got {error}");
    };
    if response.status() != expected {
        bail!("expected HTTP {expected}, got {}", response.status());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_goal_round_trip_survives_shim_restart() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;
    let model_name = format!("mock-goal-restart-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "unused")?;
    let server_port = allocate_port()?;
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-goal-restart-{}", Uuid::new_v4().simple());
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
    let server_args = [
        "--codex-shim",
        "--codex-shim-port",
        shim_port_string.as_str(),
        "--codex-shim-poll-ms",
        "50",
    ];

    let mut serve = spawn_server_with_env(&home_dir, server_port, &server_args, &[])?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{shim_port}/")).await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;
    let thread_id = start_thread(&mut ws, &home_dir).await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(119),
            params: codex::TurnStartParams {
                thread_id: thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: "materialize the canonical session".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let _: codex::TurnStartResponse = read_typed_response(&mut ws, request_id(119)).await?;
    let _ = read_turn_capture(&mut ws).await?;
    let objective = format!("survive restart {}", Uuid::new_v4().simple());
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadGoalSet {
            request_id: request_id(120),
            params: codex::ThreadGoalSetParams {
                thread_id: thread_id.clone(),
                objective: Some(objective.clone()),
                status: Some(codex::ThreadGoalStatus::Active),
                token_budget: Some(Some(12_345)),
            },
        },
    )
    .await?;
    let set: codex::ThreadGoalSetResponse = read_typed_response(&mut ws, request_id(120)).await?;
    assert_eq!(set.goal.objective, objective);
    ws.close(None).await?;
    drop(serve);

    let mut restarted = spawn_server_with_env(&home_dir, server_port, &server_args, &[])?;
    wait_for_port(server_port, &mut restarted)?;
    wait_for_port(shim_port, &mut restarted)?;
    restarted
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{shim_port}/")).await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadGoalGet {
            request_id: request_id(121),
            params: codex::ThreadGoalGetParams {
                thread_id: thread_id.clone(),
            },
        },
    )
    .await?;
    let get: codex::ThreadGoalGetResponse = read_typed_response(&mut ws, request_id(121)).await?;
    let goal = get.goal.context("goal disappeared across shim restart")?;
    assert_eq!(goal.thread_id, thread_id);
    assert_eq!(goal.objective, objective);
    assert_eq!(goal.status, codex::ThreadGoalStatus::Active);
    assert_eq!(goal.token_budget, Some(12_345));

    let foreign_thread_id = format!("foreign-goal-{}", Uuid::new_v4().simple());
    restarted
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"mutation {{
                create_Goal(input: {{
                    goal_id: "foreign-goal",
                    session_id: "{}",
                    agent_did: "{}",
                    objective: "belongs to another surface",
                    status: "active",
                    created_at: "2026-07-16T00:00:00Z"
                }}) {{ _docID }}
            }}"#,
                escape_graphql_string(&foreign_thread_id),
                escape_graphql_string(&agent_did),
            ),
        ))
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadGoalGet {
            request_id: request_id(122),
            params: codex::ThreadGoalGetParams {
                thread_id: foreign_thread_id.clone(),
            },
        },
    )
    .await?;
    let foreign_get: codex::ThreadGoalGetResponse =
        read_typed_response(&mut ws, request_id(122)).await?;
    assert!(foreign_get.goal.is_none());
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadGoalClear {
            request_id: request_id(123),
            params: codex::ThreadGoalClearParams {
                thread_id: foreign_thread_id.clone(),
            },
        },
    )
    .await?;
    let foreign_clear: codex::ThreadGoalClearResponse =
        read_typed_response(&mut ws, request_id(123)).await?;
    assert!(!foreign_clear.cleared);
    let foreign_rows = restarted
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"{{ Goal(filter: {{ session_id: {{ _eq: "{}" }} }}) {{ goal_id }} }}"#,
                escape_graphql_string(&foreign_thread_id)
            ),
        ))
        .await?;
    assert_eq!(
        foreign_rows
            .pointer("/data/Goal")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );

    restarted
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"mutation {{
                create_Goal(input: {{
                    goal_id: "duplicate-goal",
                    session_id: "{}",
                    agent_did: "{}",
                    objective: "replicated twin",
                    status: "paused",
                    created_at: "2026-07-16T00:00:01Z"
                }}) {{ _docID }}
            }}"#,
                escape_graphql_string(&thread_id),
                escape_graphql_string(&agent_did),
            ),
        ))
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadGoalClear {
            request_id: request_id(124),
            params: codex::ThreadGoalClearParams {
                thread_id: thread_id.clone(),
            },
        },
    )
    .await?;
    let cleared: codex::ThreadGoalClearResponse =
        read_typed_response(&mut ws, request_id(124)).await?;
    assert!(cleared.cleared);
    let cleared_rows = restarted
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"{{ Goal(filter: {{ session_id: {{ _eq: "{}" }} }}) {{ goal_id }} }}"#,
                escape_graphql_string(&thread_id)
            ),
        ))
        .await?;
    assert_eq!(
        cleared_rows
            .pointer("/data/Goal")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_keeps_running_when_codex_shim_port_is_taken() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-shim-degrade-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "unused")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-shim-degrade-{}", Uuid::new_v4().simple());
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

    let occupied = std::net::TcpListener::bind("127.0.0.1:0").context("occupying a port")?;
    let shim_port = occupied.local_addr()?.port();
    let shim_port_string = shim_port.to_string();
    let (mut serve, readiness) = spawn_server_with_ready_json(
        &home_dir,
        server_port,
        &["--codex-shim", "--codex-shim-port", &shim_port_string],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    assert_eq!(
        readiness
            .pointer("/codex_shim/disabled")
            .and_then(Value::as_bool),
        Some(true),
        "server readiness must report the shim as disabled: {readiness}"
    );

    let (_stdout, stderr) = serve.captured_output()?;
    assert!(
        stderr.contains("Codex endpoint disabled"),
        "server should report the degraded Codex endpoint; stderr:\n{stderr}"
    );
    drop(occupied);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_protocol_turn_streams_gents_response() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let expected_reply = format!("codex-shim-ok-{}", Uuid::new_v4().simple());
    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, &expected_reply)?;

    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());
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
    let default_backend_id = default_backend_id(&agent_did);
    let default_model_selection = gents_model_selection_id(&default_backend_id, &model_name);
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let initialize: codex::InitializeResponse = read_typed_response(&mut ws, request_id(1)).await?;
    assert!(
        initialize.user_agent.starts_with("gents-codex-shim/"),
        "unexpected initialize response: {initialize:?}"
    );

    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ConfigRead {
            request_id: request_id(2),
            params: codex::ConfigReadParams {
                include_layers: false,
                cwd: None,
            },
        },
    )
    .await?;
    let config: codex::ConfigReadResponse = read_typed_response(&mut ws, request_id(2)).await?;
    assert_eq!(
        config.config.model.as_deref(),
        Some(default_model_selection.as_str()),
        "ConfigRead.model should be the bound behavior's backend-qualified model selection"
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadStart {
            request_id: request_id(3),
            params: codex::ThreadStartParams {
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let thread_start: codex::ThreadStartResponse =
        read_typed_response(&mut ws, request_id(3)).await?;
    let thread_id = thread_start.thread.id.clone();
    Uuid::parse_str(&thread_id)
        .with_context(|| format!("Codex TUI requires UUID thread ids, got {thread_id}"))?;

    let prompt = format!("Reply with exactly {}.", Uuid::new_v4().simple());
    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(4),
            params: codex::TurnStartParams {
                thread_id: thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: prompt.clone(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let turn_start: codex::TurnStartResponse = read_typed_response(&mut ws, request_id(4)).await?;
    assert_eq!(turn_start.turn.status, codex::TurnStatus::InProgress);

    let turn_capture = read_turn_capture(&mut ws).await?;
    let final_text = turn_capture.text.clone();
    let completed_turn = turn_capture.turn.clone();
    assert_eq!(
        completed_turn.status,
        codex::TurnStatus::Completed,
        "completed_turn={completed_turn:?}; final_text={final_text}"
    );
    assert!(
        final_text.contains(&expected_reply),
        "expected streamed Codex text to contain {expected_reply}, got:\n{final_text}"
    );

    let turn_usage = turn_capture
        .token_usage
        .as_ref()
        .expect("turn completion should emit a ThreadTokenUsageUpdated notification");
    assert!(
        turn_usage.total.total_tokens > 0,
        "expected non-zero cumulative token usage on turn completion, got {turn_usage:?}"
    );
    assert!(
        turn_usage.last.total_tokens > 0,
        "expected non-zero last-turn token usage on turn completion, got {turn_usage:?}"
    );
    assert_eq!(
        turn_usage.model_context_window,
        Some(gents::DEFAULT_CONTEXT_WINDOW as i64),
        "context capacity should come from the bound GENTS inference profile"
    );

    let session_response = serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"{{
                AgentSession(filter: {{ session_id: {{ _eq: "{}" }} }}, limit: 1) {{
                    session_id
                    agent_did
                    behavior_id
                    status
                    started
                }}
            }}"#,
                escape_graphql_string(&thread_id),
            ),
        ))
        .await?;
    let session = first_graphql_row(&session_response, "AgentSession")?;
    assert_eq!(
        session.get("session_id").and_then(Value::as_str),
        Some(thread_id.as_str())
    );
    assert_eq!(
        session.get("agent_did").and_then(Value::as_str),
        Some(agent_did.as_str())
    );
    let expected_behavior_id = format!("{agent_did}:default");
    assert_eq!(
        session.get("behavior_id").and_then(Value::as_str),
        Some(expected_behavior_id.as_str())
    );
    assert_eq!(
        session.get("status").and_then(Value::as_str),
        Some("active")
    );
    assert!(
        session
            .get("started")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "AgentSession.started should be populated: {session}"
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadList {
            request_id: request_id(30),
            params: codex::ThreadListParams {
                cursor: None,
                limit: None,
                sort_key: None,
                sort_direction: None,
                model_providers: None,
                source_kinds: None,
                archived: None,
                cwd: None,
                use_state_db_only: true,
                search_term: None,
            },
        },
    )
    .await?;
    let thread_list: codex::ThreadListResponse =
        read_typed_response(&mut ws, request_id(30)).await?;
    assert!(
        thread_list.data.iter().any(|thread| thread.id == thread_id),
        "GENTS-backed thread list did not include {thread_id}: {thread_list:?}"
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadLoadedList {
            request_id: request_id(31),
            params: codex::ThreadLoadedListParams::default(),
        },
    )
    .await?;
    let loaded_threads: codex::ThreadLoadedListResponse =
        read_typed_response(&mut ws, request_id(31)).await?;
    assert!(loaded_threads.data.contains(&thread_id));

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: request_id(32),
            params: codex::ThreadReadParams {
                thread_id: thread_id.clone(),
                include_turns: true,
            },
        },
    )
    .await?;
    let thread_read: codex::ThreadReadResponse =
        read_typed_response(&mut ws, request_id(32)).await?;
    assert_eq!(thread_read.thread.id, thread_id);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadResume {
            request_id: request_id(33),
            params: codex::ThreadResumeParams {
                thread_id: thread_id.clone(),
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let thread_resume: codex::ThreadResumeResponse = read_typed_response(&mut ws, request_id(33))
        .await
        .context("reading thread/resume response")?;
    assert_eq!(thread_resume.thread.id, thread_id);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadSetName {
            request_id: request_id(34),
            params: codex::ThreadSetNameParams {
                thread_id: thread_id.clone(),
                name: "GENTS-backed Codex thread".to_string(),
            },
        },
    )
    .await?;
    let _: codex::ThreadSetNameResponse = read_typed_response(&mut ws, request_id(34))
        .await
        .context("reading thread/name/set response")?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadMemoryModeSet {
            request_id: request_id(35),
            params: codex::ThreadMemoryModeSetParams {
                thread_id: thread_id.clone(),
                mode: codex::ThreadMemoryMode::Disabled,
            },
        },
    )
    .await?;
    let _: codex::ThreadMemoryModeSetResponse = read_typed_response(&mut ws, request_id(35))
        .await
        .context("reading thread/memoryMode/set response")?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadSettingsUpdate {
            request_id: request_id(36),
            params: codex::ThreadSettingsUpdateParams {
                thread_id: thread_id.clone(),
                cwd: Some(home_dir.clone()),
                ..Default::default()
            },
        },
    )
    .await?;
    let _: codex::ThreadSettingsUpdateResponse = read_typed_response(&mut ws, request_id(36))
        .await
        .context("reading thread/settings/update response")?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadGoalSet {
            request_id: request_id(37),
            params: codex::ThreadGoalSetParams {
                thread_id: thread_id.clone(),
                objective: Some("exercise GENTS-backed Codex goal state".to_string()),
                status: Some(codex::ThreadGoalStatus::Active),
                token_budget: Some(Some(123)),
            },
        },
    )
    .await?;
    let goal_set: codex::ThreadGoalSetResponse = read_typed_response(&mut ws, request_id(37))
        .await
        .context("reading thread/goal/set response")?;
    assert_eq!(goal_set.goal.thread_id, thread_id);
    assert_eq!(
        goal_set.goal.objective,
        "exercise GENTS-backed Codex goal state"
    );
    assert_eq!(goal_set.goal.token_budget, Some(123));

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadGoalGet {
            request_id: request_id(38),
            params: codex::ThreadGoalGetParams {
                thread_id: thread_id.clone(),
            },
        },
    )
    .await?;
    let goal_get: codex::ThreadGoalGetResponse = read_typed_response(&mut ws, request_id(38))
        .await
        .context("reading thread/goal/get response")?;
    assert_eq!(
        goal_get.goal.as_ref().map(|goal| &goal.thread_id),
        Some(&thread_id)
    );

    let expected_git_sha = init_test_git_repo(&home_dir, "main")?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadMetadataUpdate {
            request_id: request_id(39),
            params: codex::ThreadMetadataUpdateParams {
                thread_id: thread_id.clone(),
                git_info: Some(codex::ThreadMetadataGitInfoUpdateParams {
                    sha: Some(Some("abc123".to_string())),
                    branch: Some(Some("main".to_string())),
                    origin_url: None,
                }),
            },
        },
    )
    .await?;
    let metadata_update: codex::ThreadMetadataUpdateResponse =
        read_typed_response(&mut ws, request_id(39))
            .await
            .context("reading thread/metadata/update response")?;
    assert_eq!(
        metadata_update
            .thread
            .git_info
            .as_ref()
            .and_then(|git| git.sha.as_deref()),
        Some(expected_git_sha.as_str())
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadArchive {
            request_id: request_id(40),
            params: codex::ThreadArchiveParams {
                thread_id: thread_id.clone(),
            },
        },
    )
    .await?;
    let _: codex::ThreadArchiveResponse = read_typed_response(&mut ws, request_id(40))
        .await
        .context("reading thread/archive response")?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadList {
            request_id: request_id(48),
            params: codex::ThreadListParams {
                cursor: None,
                limit: Some(1),
                sort_key: None,
                sort_direction: None,
                model_providers: Some(vec!["gents".to_string()]),
                source_kinds: Some(vec![codex::ThreadSourceKind::Cli]),
                archived: Some(true),
                cwd: Some(codex::ThreadListCwdFilter::One(
                    home_dir.display().to_string(),
                )),
                use_state_db_only: true,
                search_term: Some("GENTS-backed Codex thread".to_string()),
            },
        },
    )
    .await?;
    let archived_threads: codex::ThreadListResponse = read_typed_response(&mut ws, request_id(48))
        .await
        .context("reading archived thread/list response")?;
    assert_eq!(archived_threads.data.len(), 1);
    assert_eq!(archived_threads.data[0].id, thread_id);
    assert_eq!(
        archived_threads.backwards_cursor.as_deref(),
        Some(thread_id.as_str())
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadList {
            request_id: request_id(49),
            params: codex::ThreadListParams {
                cursor: None,
                limit: None,
                sort_key: None,
                sort_direction: None,
                model_providers: Some(vec!["openai".to_string()]),
                source_kinds: Some(vec![codex::ThreadSourceKind::Cli]),
                archived: Some(true),
                cwd: None,
                use_state_db_only: true,
                search_term: None,
            },
        },
    )
    .await?;
    let wrong_provider_threads: codex::ThreadListResponse =
        read_typed_response(&mut ws, request_id(49))
            .await
            .context("reading provider-filtered thread/list response")?;
    assert!(wrong_provider_threads.data.is_empty());

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadUnarchive {
            request_id: request_id(41),
            params: codex::ThreadUnarchiveParams {
                thread_id: thread_id.clone(),
            },
        },
    )
    .await?;
    let thread_unarchive: codex::ThreadUnarchiveResponse =
        read_typed_response(&mut ws, request_id(41))
            .await
            .context("reading thread/unarchive response")?;
    assert_eq!(thread_unarchive.thread.id, thread_id);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadGoalGet {
            request_id: request_id(50),
            params: codex::ThreadGoalGetParams {
                thread_id: thread_id.clone(),
            },
        },
    )
    .await?;
    let goal_after_turn: codex::ThreadGoalGetResponse =
        read_typed_response(&mut ws, request_id(50))
            .await
            .context("reading post-turn thread/goal/get response")?;
    let goal_after_turn = goal_after_turn
        .goal
        .expect("goal should still exist after the turn");
    assert!(
        goal_after_turn.tokens_used > 0,
        "goal.tokens_used should reflect real session usage after a turn, got {}",
        goal_after_turn.tokens_used
    );

    let (_request_id, session_id, _behavior_id) =
        wait_for_request(&graphql, &agent_did, &prompt).await?;
    assert_eq!(session_id, thread_id);
    let captured_requests = mock_endpoint.captured_chat_requests();
    assert!(
        captured_requests
            .iter()
            .any(|request| request_contains_role_text(request, "user", &prompt)),
        "mock endpoint did not receive the Codex prompt; captured={captured_requests:?}"
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: request_id(42),
            params: codex::ThreadReadParams {
                thread_id: thread_id.clone(),
                include_turns: true,
            },
        },
    )
    .await?;
    let thread_history: codex::ThreadReadResponse =
        read_typed_response(&mut ws, request_id(42)).await?;
    assert_eq!(thread_history.thread.id, thread_id);
    assert_eq!(thread_history.thread.turns.len(), 1);
    let history_turn = &thread_history.thread.turns[0];
    assert_eq!(history_turn.id, completed_turn.id);
    assert_eq!(history_turn.items_view, codex::TurnItemsView::Full);
    assert_eq!(history_turn.status, codex::TurnStatus::Completed);
    assert_turn_has_user_text(history_turn, &prompt);
    assert_turn_has_agent_text(history_turn, &expected_reply);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadResume {
            request_id: request_id(46),
            params: codex::ThreadResumeParams {
                thread_id: thread_id.clone(),
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let resumed_history: codex::ThreadResumeResponse = read_typed_response(&mut ws, request_id(46))
        .await
        .context("reading history-bearing thread/resume response")?;
    assert_eq!(resumed_history.thread.id, thread_id);
    assert_eq!(resumed_history.thread.turns.len(), 1);
    let resumed_turn = &resumed_history.thread.turns[0];
    assert_eq!(resumed_turn.id, completed_turn.id);
    assert_eq!(resumed_turn.items_view, codex::TurnItemsView::Full);
    assert_eq!(resumed_turn.status, codex::TurnStatus::Completed);
    assert_turn_has_user_text(resumed_turn, &prompt);
    assert_turn_has_agent_text(resumed_turn, &expected_reply);

    let replay_usage = read_token_usage_notification(&mut ws)
        .await
        .context("reading token-usage replay after thread/resume")?;
    assert!(
        replay_usage.total.total_tokens > 0,
        "thread/resume should replay non-zero session token usage, got {replay_usage:?}"
    );
    assert_eq!(
        replay_usage.last.total_tokens, turn_usage.last.total_tokens,
        "thread/resume should restore the latest inference context, not cumulative usage"
    );
    assert_eq!(
        replay_usage.model_context_window, turn_usage.model_context_window,
        "thread/resume should restore the effective context capacity"
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadResume {
            request_id: request_id(47),
            params: codex::ThreadResumeParams {
                thread_id: thread_id.clone(),
                cwd: Some(home_dir.display().to_string()),
                exclude_turns: true,
                ..Default::default()
            },
        },
    )
    .await?;
    let metadata_resume: codex::ThreadResumeResponse = read_typed_response(&mut ws, request_id(47))
        .await
        .context("reading metadata-only thread/resume response")?;
    assert_eq!(metadata_resume.thread.id, thread_id);
    assert!(
        metadata_resume.thread.turns.is_empty(),
        "excludeTurns resume should not load persisted turns"
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadTurnsList {
            request_id: request_id(43),
            params: codex::ThreadTurnsListParams {
                thread_id: thread_id.clone(),
                cursor: None,
                limit: None,
                sort_direction: None,
                items_view: None,
            },
        },
    )
    .await?;
    let turns_list: codex::ThreadTurnsListResponse =
        read_typed_response(&mut ws, request_id(43)).await?;
    assert_eq!(turns_list.data.len(), 1);
    assert_eq!(turns_list.data[0].id, completed_turn.id);
    assert_eq!(turns_list.data[0].items_view, codex::TurnItemsView::Summary);
    assert_turn_has_user_text(&turns_list.data[0], &prompt);
    assert_turn_has_agent_text(&turns_list.data[0], &expected_reply);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadTurnsItemsList {
            request_id: request_id(44),
            params: codex::ThreadTurnsItemsListParams {
                thread_id: thread_id.clone(),
                turn_id: completed_turn.id.clone(),
                cursor: None,
                limit: None,
                sort_direction: None,
            },
        },
    )
    .await?;
    let items_list: codex::ThreadTurnsItemsListResponse =
        read_typed_response(&mut ws, request_id(44)).await?;
    assert!(
        items_list.data.len() >= 2,
        "expected persisted turn items, got {:?}",
        items_list.data
    );

    send_raw_client_request(
        &mut ws,
        request_id(45),
        "getConversationSummary",
        json!({ "conversationId": thread_id.clone() }),
    )
    .await?;
    let summary: codex::GetConversationSummaryResponse =
        read_typed_response(&mut ws, request_id(45)).await?;
    assert_eq!(summary.summary.conversation_id.to_string(), thread_id);
    assert_eq!(summary.summary.model_provider, "gents");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_streams_claimed_background_completion_and_replays_it_once() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let expected_reply = format!("background-wake-ok-{}", Uuid::new_v4().simple());
    let model_name = format!("mock-background-wake-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, &expected_reply)?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-background-wake-{}", Uuid::new_v4().simple());
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
    let behavior_id = format!("{agent_did}:default");
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "50",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;
    let thread_id = start_thread(&mut ws, &home_dir).await?;
    let wake_request_id =
        seed_background_completion_wake(&graphql, &agent_did, &behavior_id, &thread_id).await?;

    let started = tokio::time::timeout(Duration::from_secs(30), read_turn_started(&mut ws))
        .await
        .context("background completion wake was not projected as a new turn")??;
    assert_eq!(started.thread_id, thread_id);
    assert_eq!(started.turn.id, wake_request_id);
    let capture = tokio::time::timeout(Duration::from_secs(30), read_turn_capture(&mut ws))
        .await
        .context("background completion wake did not finish in the connected client")??;
    assert_eq!(capture.turn.id, wake_request_id);
    assert_eq!(capture.turn.status, codex::TurnStatus::Completed);
    assert!(
        capture.text.contains(&expected_reply),
        "missing background continuation output: {}",
        capture.text
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadResume {
            request_id: request_id(105),
            params: codex::ThreadResumeParams {
                thread_id: thread_id.clone(),
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let resumed: codex::ThreadResumeResponse =
        read_typed_response(&mut ws, request_id(105)).await?;
    let wake_turns = resumed
        .thread
        .turns
        .iter()
        .filter(|turn| turn.id == wake_request_id)
        .collect::<Vec<_>>();
    assert_eq!(wake_turns.len(), 1, "wake replay must not duplicate turns");
    assert_turn_has_agent_text(wake_turns[0], &expected_reply);
    assert!(
        !wake_turns[0]
            .items
            .iter()
            .any(|item| matches!(item, codex::ThreadItem::UserMessage { .. })),
        "internal background wake prompt must not replay as user input"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_resume_finishes_an_in_progress_background_completion() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let expected_reply = format!("resumed-background-wake-ok-{}", Uuid::new_v4().simple());
    let model_name = format!("mock-resumed-background-wake-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start_routed_delayed(
        &model_name,
        Vec::new(),
        expected_reply.clone(),
        Duration::from_secs(5),
    )?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-resumed-background-wake-{}", Uuid::new_v4().simple());
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
    let behavior_id = format!("{agent_did}:default");
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "50",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;
    let thread_id = start_thread(&mut ws, &home_dir).await?;
    let wake_request_id =
        seed_background_completion_wake(&graphql, &agent_did, &behavior_id, &thread_id).await?;

    let started = tokio::time::timeout(Duration::from_secs(30), read_turn_started(&mut ws))
        .await
        .context("background completion wake was not projected as a new turn")??;
    assert_eq!(started.turn.id, wake_request_id);
    assert_eq!(started.turn.status, codex::TurnStatus::InProgress);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadResume {
            request_id: request_id(106),
            params: codex::ThreadResumeParams {
                thread_id: thread_id.clone(),
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let resumed: codex::ThreadResumeResponse =
        read_typed_response(&mut ws, request_id(106)).await?;
    let baseline = resumed
        .thread
        .turns
        .iter()
        .find(|turn| turn.id == wake_request_id)
        .context("resume omitted the in-progress background wake")?;
    assert_eq!(baseline.status, codex::TurnStatus::InProgress);

    let capture = tokio::time::timeout(Duration::from_secs(30), read_turn_capture(&mut ws))
        .await
        .context("resumed background wake did not finish in the connected client")??;
    assert_eq!(capture.turn.id, wake_request_id);
    assert_eq!(capture.turn.status, codex::TurnStatus::Completed);
    assert!(
        capture.text.contains(&expected_reply),
        "missing resumed background continuation output: {}",
        capture.text
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_thread_list_reconstructs_turned_threads_from_durable_data() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "unused")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());
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
    let behavior_id = format!("{agent_did}:default");
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let turned_session_id = Uuid::new_v4().to_string();
    for mutation in [
        format!(
            r#"mutation {{ create_AgentSession(input: {{
                session_id: "{s}", agent_name: "{behavior_id}", agent_did: "{agent_did}",
                behavior_id: "{behavior_id}", started: "2026-01-01T00:00:00Z", status: "active"
            }}) {{ _docID }} }}"#,
            s = escape_graphql_string(&turned_session_id),
            behavior_id = escape_graphql_string(&behavior_id),
            agent_did = escape_graphql_string(&agent_did),
        ),
        format!(
            r#"mutation {{ create_AgentRequest(input: {{
                request_id: "{r}", agent_did: "{agent_did}", behavior_id: "{behavior_id}",
                session_id: "{s}", metadata: "{{\"codex_shim\":{{}}}}",
                execution_origin: "interactive", created_at: "2026-01-01T00:00:00Z"
            }}) {{ _docID }} }}"#,
            r = escape_graphql_string(&Uuid::new_v4().to_string()),
            agent_did = escape_graphql_string(&agent_did),
            behavior_id = escape_graphql_string(&behavior_id),
            s = escape_graphql_string(&turned_session_id),
        ),
        format!(
            r#"mutation {{ create_AgentConversation(input: {{
                session_id: "{s}", agent_name: "{behavior_id}", agent_did: "{agent_did}",
                behavior_id: "{behavior_id}",
                title: "Earlier Codex thread", title_source: "user",
                status: "active", created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z"
            }}) {{ _docID }} }}"#,
            s = escape_graphql_string(&turned_session_id),
            behavior_id = escape_graphql_string(&behavior_id),
            agent_did = escape_graphql_string(&agent_did),
        ),
    ] {
        serve.capturing(graphql_query(&graphql, &mutation)).await?;
    }

    let zero_turn_session_id = Uuid::new_v4().to_string();
    serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"mutation {{ create_AgentSession(input: {{
                session_id: "{s}", agent_name: "{behavior_id}", agent_did: "{agent_did}",
                behavior_id: "{behavior_id}", started: "2026-01-01T00:00:00Z", status: "active"
            }}) {{ _docID }} }}"#,
                s = escape_graphql_string(&zero_turn_session_id),
                behavior_id = escape_graphql_string(&behavior_id),
                agent_did = escape_graphql_string(&agent_did),
            ),
        ))
        .await?;

    let pending_session_id = Uuid::new_v4().to_string();
    serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"mutation {{ create_AgentRequest(input: {{
                    request_id: "{request}", agent_did: "{agent_did}",
                    behavior_id: "{behavior_id}", session_id: "{session}",
                    content: "pending projection", status: "pending",
                    lifecycle_state: "pending", execution_origin: "interactive",
                    created_at: "2026-01-01T00:00:01Z"
                }}) {{ _docID }} }}"#,
                request = escape_graphql_string(&Uuid::new_v4().to_string()),
                agent_did = escape_graphql_string(&agent_did),
                behavior_id = escape_graphql_string(&behavior_id),
                session = escape_graphql_string(&pending_session_id),
            ),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _: codex::InitializeResponse = read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadList {
            request_id: request_id(2),
            params: codex::ThreadListParams {
                cursor: None,
                limit: None,
                sort_key: None,
                sort_direction: None,
                model_providers: None,
                source_kinds: None,
                archived: None,
                cwd: None,
                use_state_db_only: true,
                search_term: None,
            },
        },
    )
    .await?;
    let listed: codex::ThreadListResponse = read_typed_response(&mut ws, request_id(2)).await?;
    assert!(
        listed
            .data
            .iter()
            .any(|thread| thread.id == turned_session_id),
        "a turned Codex thread must be reconstructed from its durable codex_shim request, \
         with no in-process marker: {listed:?}"
    );
    assert!(
        !listed
            .data
            .iter()
            .any(|thread| thread.id == zero_turn_session_id),
        "a never-turned session holds no durable Codex data and must not be surfaced \
         after restart: {listed:?}"
    );
    assert!(
        listed
            .data
            .iter()
            .any(|thread| thread.id == pending_session_id),
        "a durable request must remain visible while claim has not projected its conversation: {listed:?}"
    );

    let turned = listed
        .data
        .iter()
        .find(|thread| thread.id == turned_session_id)
        .expect("turned thread present");
    assert_eq!(
        turned.name.as_deref(),
        Some("Earlier Codex thread"),
        "reconstructed thread name should come from the durable conversation title"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_derives_git_info_and_keeps_empty_thread_ephemeral() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "unused")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());
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
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _: codex::InitializeResponse = read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    let git_dir = tempdir.path().join("repo");
    fs::create_dir_all(&git_dir)?;
    let expected_sha = init_test_git_repo(&git_dir, "main")?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadStart {
            request_id: request_id(2),
            params: codex::ThreadStartParams {
                cwd: Some(git_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let git_thread: codex::ThreadStartResponse =
        read_typed_response(&mut ws, request_id(2)).await?;
    assert_eq!(
        git_thread
            .thread
            .git_info
            .as_ref()
            .and_then(|git| git.sha.as_deref()),
        Some(expected_sha.as_str()),
        "git sha should be derived from the thread cwd at ThreadStart: {:?}",
        git_thread.thread.git_info
    );
    assert_eq!(
        git_thread
            .thread
            .git_info
            .as_ref()
            .and_then(|git| git.branch.as_deref()),
        Some("main"),
        "git branch should be derived from the thread cwd"
    );

    let git_thread_id = git_thread.thread.id.clone();
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadSetName {
            request_id: request_id(3),
            params: codex::ThreadSetNameParams {
                thread_id: git_thread_id.clone(),
                name: "Named before first turn".to_string(),
            },
        },
    )
    .await?;
    let _: codex::ThreadSetNameResponse = read_typed_response(&mut ws, request_id(3))
        .await
        .context("reading early thread/name/set response")?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: request_id(4),
            params: codex::ThreadReadParams {
                thread_id: git_thread_id.clone(),
                include_turns: false,
            },
        },
    )
    .await?;
    let renamed: codex::ThreadReadResponse = read_typed_response(&mut ws, request_id(4))
        .await
        .context("reading thread/read after early rename")?;
    assert_eq!(
        renamed.thread.name.as_deref(),
        Some("Named before first turn"),
        "the adapter should retain an empty thread's presentation state"
    );
    let empty_projection = serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"{{
                    AgentSession(filter: {{ session_id: {{ _eq: "{}" }} }}) {{ _docID }}
                    AgentConversation(filter: {{ session_id: {{ _eq: "{}" }} }}) {{ _docID }}
                }}"#,
                escape_graphql_string(&git_thread_id),
                escape_graphql_string(&git_thread_id),
            ),
        ))
        .await?;
    assert_eq!(empty_projection.pointer("/data/AgentSession/0"), None);
    assert_eq!(empty_projection.pointer("/data/AgentConversation/0"), None);

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(4),
            params: codex::TurnStartParams {
                thread_id: git_thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: "materialize this session".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let _: codex::TurnStartResponse = read_typed_response(&mut ws, request_id(4)).await?;
    let _ = read_turn_capture(&mut ws).await?;
    let canonical = serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"{{ AgentConversation(filter: {{ session_id: {{ _eq: "{}" }} }}) {{ title }} }}"#,
                escape_graphql_string(&git_thread_id),
            ),
        ))
        .await?;
    assert_eq!(
        canonical
            .pointer("/data/AgentConversation/0/title")
            .and_then(Value::as_str),
        Some("Named before first turn")
    );

    let plain_dir = tempdir.path().join("plain");
    fs::create_dir_all(&plain_dir)?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadStart {
            request_id: request_id(5),
            params: codex::ThreadStartParams {
                cwd: Some(plain_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let plain_thread: codex::ThreadStartResponse =
        read_typed_response(&mut ws, request_id(5)).await?;
    assert!(
        plain_thread.thread.git_info.is_none(),
        "non-git cwd should yield no gitInfo, got {:?}",
        plain_thread.thread.git_info
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_thread_list_projects_canonical_gents_sessions() -> Result<()> {
    // Codex is a view over canonical Gents sessions. The source of the request
    // does not create a second class of persisted conversation.
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "unused")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());
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
    let behavior_id = format!("{agent_did}:default");
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let foreign_session_id = Uuid::new_v4().to_string();
    let foreign_request_id = Uuid::new_v4().to_string();
    serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"mutation {{
                create_AgentSession(input: {{
                    session_id: "{session}",
                    agent_name: "{agent_name}",
                    agent_did: "{agent_did}",
                    behavior_id: "{behavior_id}",
                    started: "2026-01-01T00:00:00Z",
                    status: "active"
                }}) {{ _docID }}
            }}"#,
                session = escape_graphql_string(&foreign_session_id),
                agent_name = escape_graphql_string(&agent_name),
                agent_did = escape_graphql_string(&agent_did),
                behavior_id = escape_graphql_string(&behavior_id),
            ),
        ))
        .await?;
    serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{request}",
                    agent_did: "{agent_did}",
                    behavior_id: "{behavior_id}",
                    session_id: "{session}",
                    metadata: "{{}}",
                    execution_origin: "cli",
                    created_at: "2026-01-01T00:00:00Z"
                }}) {{ _docID }}
            }}"#,
                request = escape_graphql_string(&foreign_request_id),
                agent_did = escape_graphql_string(&agent_did),
                behavior_id = escape_graphql_string(&behavior_id),
                session = escape_graphql_string(&foreign_session_id),
            ),
        ))
        .await?;
    serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"mutation {{
                create_AgentConversation(input: {{
                    session_id: "{session}",
                    agent_name: "{agent_name}",
                    agent_did: "{agent_did}",
                    behavior_id: "{behavior_id}",
                    title: "Shared Gents session",
                    title_source: "user",
                    preview_text: "shared",
                    status: "active",
                    created_at: "2026-01-01T00:00:00Z",
                    updated_at: "2026-01-01T00:00:00Z",
                    latest_request_id: "{request}"
                }}) {{ _docID }}
            }}"#,
                session = escape_graphql_string(&foreign_session_id),
                request = escape_graphql_string(&foreign_request_id),
                agent_name = escape_graphql_string(&agent_name),
                agent_did = escape_graphql_string(&agent_did),
                behavior_id = escape_graphql_string(&behavior_id),
            ),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _: codex::InitializeResponse = read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    // An empty shim-created thread is visible from the process-local adapter.
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadStart {
            request_id: request_id(2),
            params: codex::ThreadStartParams {
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let codex_thread: codex::ThreadStartResponse =
        read_typed_response(&mut ws, request_id(2)).await?;
    let codex_thread_id = codex_thread.thread.id.clone();

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadList {
            request_id: request_id(3),
            params: codex::ThreadListParams {
                cursor: None,
                limit: None,
                sort_key: None,
                sort_direction: None,
                model_providers: None,
                source_kinds: None,
                archived: None,
                cwd: None,
                use_state_db_only: true,
                search_term: None,
            },
        },
    )
    .await?;
    let listed: codex::ThreadListResponse = read_typed_response(&mut ws, request_id(3)).await?;
    assert!(
        listed
            .data
            .iter()
            .any(|thread| thread.id == codex_thread_id),
        "the shim-created Codex thread should be listed: {listed:?}"
    );
    assert!(
        listed
            .data
            .iter()
            .any(|thread| thread.id == foreign_session_id),
        "a canonical Gents session must be visible through the Codex projection: {listed:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_completes_blank_materialized_terminal_message() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-blank-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start_hanging(&model_name)?;

    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-blank-{}", Uuid::new_v4().simple());
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
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "50",
            "--codex-shim-timeout-secs",
            "5",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;
    let thread_id = start_thread(&mut ws, &home_dir).await?;

    let prompt = "Read notes.txt, then finish without visible final text.";
    send_turn(&mut ws, &thread_id, prompt).await?;
    let (request_id, session_id, behavior_id) =
        wait_for_request(&graphql, &agent_did, prompt).await?;
    assert_eq!(session_id, thread_id);
    seed_blank_materialized_completion(&graphql, &request_id, &agent_did, &behavior_id, &thread_id)
        .await?;

    let capture = tokio::time::timeout(Duration::from_secs(15), read_turn_capture(&mut ws))
        .await
        .context("timed out waiting for Codex shim turn completion")??;

    assert_eq!(capture.turn.status, codex::TurnStatus::Completed);
    assert!(
        capture.text.trim().is_empty(),
        "mock final response is intentionally blank; got:\n{}",
        capture.text
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_thread_fork_and_search_project_gents_sessions() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let expected_reply = format!("fork-search-reply-{}", Uuid::new_v4().simple());
    let model_name = format!("mock-codex-shim-fork-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, &expected_reply)?;

    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-fork-{}", Uuid::new_v4().simple());
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
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;
    let thread_id = start_thread(&mut ws, &home_dir).await?;

    let search_token = format!("FORKSEARCH{}", Uuid::new_v4().simple());
    let prompt = format!("Reply with exactly {search_token} and no extra words.");
    send_turn(&mut ws, &thread_id, &prompt).await?;
    let (_final_text, completed_turn) = read_turn_to_completion(&mut ws).await?;
    assert_eq!(completed_turn.status, codex::TurnStatus::Completed);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadFork {
            request_id: request_id(120),
            params: codex::ThreadForkParams {
                thread_id: thread_id.clone(),
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let forked: codex::ThreadForkResponse = read_typed_response(&mut ws, request_id(120)).await?;
    let forked_id = forked.thread.id.clone();
    assert_ne!(forked_id, thread_id);
    assert_eq!(forked.thread.session_id, forked_id);
    assert_eq!(
        forked.thread.forked_from_id.as_deref(),
        Some(thread_id.as_str())
    );
    assert_eq!(forked.thread.status, codex::ThreadStatus::Idle);
    assert_eq!(forked.thread.turns.len(), 1);
    assert_turn_has_user_text(&forked.thread.turns[0], &prompt);
    assert_turn_has_agent_text(&forked.thread.turns[0], &expected_reply);

    let forked_conversation = serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"{{
                AgentConversation(filter: {{ session_id: {{ _eq: "{}" }} }}, limit: 1) {{
                    session_id
                    forked_from_session_id
                    fork_at_user_turn
                }}
            }}"#,
                escape_graphql_string(&forked_id),
            ),
        ))
        .await?;
    let child = first_graphql_row(&forked_conversation, "AgentConversation")?;
    assert_eq!(
        child.get("forked_from_session_id").and_then(Value::as_str),
        Some(thread_id.as_str())
    );
    assert_eq!(
        child.get("fork_at_user_turn").and_then(Value::as_i64),
        Some(1)
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: request_id(121),
            params: codex::ThreadReadParams {
                thread_id: forked_id.clone(),
                include_turns: true,
            },
        },
    )
    .await?;
    let forked_read: codex::ThreadReadResponse =
        read_typed_response(&mut ws, request_id(121)).await?;
    assert_eq!(forked_read.thread.id, forked_id);
    assert_eq!(
        forked_read.thread.forked_from_id.as_deref(),
        Some(thread_id.as_str())
    );
    assert_eq!(forked_read.thread.turns.len(), 1);
    assert_turn_has_user_text(&forked_read.thread.turns[0], &prompt);
    assert_turn_has_agent_text(&forked_read.thread.turns[0], &expected_reply);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadSearch {
            request_id: request_id(122),
            params: codex::ThreadSearchParams {
                cursor: None,
                limit: None,
                sort_key: None,
                sort_direction: None,
                source_kinds: None,
                archived: None,
                search_term: search_token.clone(),
            },
        },
    )
    .await?;
    let search: codex::ThreadSearchResponse = read_typed_response(&mut ws, request_id(122)).await?;
    let result_ids = search
        .data
        .iter()
        .map(|result| result.thread.id.as_str())
        .collect::<Vec<_>>();
    assert!(
        result_ids.contains(&thread_id.as_str()),
        "thread/search did not include source thread {thread_id}: {search:?}"
    );
    assert!(
        result_ids.contains(&forked_id.as_str()),
        "thread/search did not include forked thread {forked_id}: {search:?}"
    );
    assert!(
        search
            .data
            .iter()
            .any(|result| result.snippet.contains(&search_token)),
        "thread/search snippets did not include token {search_token}: {search:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_fs_routes_are_unsupported() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let expected_reply = format!("unused-fs-unsupported-reply-{}", Uuid::new_v4().simple());
    let model_name = format!("mock-codex-shim-fs-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, &expected_reply)?;

    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-fs-{}", Uuid::new_v4().simple());
    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--write-tools",
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;

    for (idx, method, params) in [
        (
            0,
            "fs/readFile",
            json!({ "path": home_dir.join("file.txt").display().to_string() }),
        ),
        (
            1,
            "fs/writeFile",
            json!({
                "path": home_dir.join("file.txt").display().to_string(),
                "dataBase64": "ZGVmcmE=",
            }),
        ),
        (
            2,
            "fs/createDirectory",
            json!({
                "path": home_dir.join("dir").display().to_string(),
                "recursive": true,
            }),
        ),
        (
            3,
            "fs/getMetadata",
            json!({ "path": home_dir.display().to_string() }),
        ),
        (
            4,
            "fs/readDirectory",
            json!({ "path": home_dir.display().to_string() }),
        ),
        (
            5,
            "fs/remove",
            json!({
                "path": home_dir.join("file.txt").display().to_string(),
                "recursive": true,
                "force": true,
            }),
        ),
        (
            6,
            "fs/copy",
            json!({
                "sourcePath": home_dir.join("file.txt").display().to_string(),
                "destinationPath": home_dir.join("copy.txt").display().to_string(),
                "recursive": false,
            }),
        ),
        (
            7,
            "fs/watch",
            json!({
                "watchId": "watch-unsupported",
                "path": home_dir.display().to_string(),
            }),
        ),
        (8, "fs/unwatch", json!({ "watchId": "watch-unsupported" })),
    ] {
        let id = request_id(501 + idx);
        send_raw_client_request(&mut ws, id.clone(), method, params).await?;
        let error = read_error_response(&mut ws, id).await?;
        assert_eq!(error.code, -32601);
        assert!(
            error.message.contains("unsupported Codex shim method"),
            "unexpected fs/* unsupported message for {method}: {error:?}"
        );
        assert!(
            error
                .message
                .contains("model filesystem activity must run through GENTS"),
            "fs/* error should describe the GENTS tool-call boundary for {method}: {error:?}"
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_host_runtime_routes_cover_low_risk_paths() -> Result<()> {
    require_command("git")?;
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let expected_reply = format!("unused-host-runtime-reply-{}", Uuid::new_v4().simple());
    let model_name = format!("mock-codex-shim-host-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, &expected_reply)?;

    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-host-{}", Uuid::new_v4().simple());
    let init = run_init_json(
        &home_dir,
        &[
            "--agent-name",
            &agent_name,
            "--model-name",
            &model_name,
            "--write-tools",
            mock_endpoint.endpoint(),
        ],
    )?;
    let agent_did = agent_did_from_init(&init)?;
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;

    send_raw_client_request(
        &mut ws,
        request_id(551),
        "command/exec",
        json!({
            "command": ["/bin/sh", "-lc", "printf gents-host-exec"],
            "cwd": home_dir.display().to_string(),
            "timeoutMs": 5000,
        }),
    )
    .await?;
    let exec_error = read_error_response(&mut ws, request_id(551)).await?;
    assert_eq!(exec_error.code, -32601);
    assert!(exec_error.message.contains("GENTS tool-call"));

    send_raw_client_request(
        &mut ws,
        request_id(581),
        "process/spawn",
        json!({
            "command": ["/bin/sh", "-lc", "printf gents-process-spawn"],
            "processHandle": format!("process-{}", Uuid::new_v4().simple()),
            "cwd": home_dir.display().to_string(),
            "streamStdoutStderr": true,
            "timeoutMs": 5000,
        }),
    )
    .await?;
    let process_error = read_error_response(&mut ws, request_id(581)).await?;
    assert_eq!(process_error.code, -32601);
    assert!(process_error
        .message
        .contains("managed-exec state machines"));

    fs::write(home_dir.join("alpha_notes.txt"), "alpha")?;
    fs::create_dir_all(home_dir.join("nested"))?;
    fs::write(home_dir.join("nested/beta_alpha.md"), "alpha")?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::FuzzyFileSearch {
            request_id: request_id(552),
            params: codex::FuzzyFileSearchParams {
                query: "alpha".to_string(),
                roots: vec![home_dir.display().to_string()],
                cancellation_token: None,
            },
        },
    )
    .await?;
    let fuzzy: codex::FuzzyFileSearchResponse =
        read_typed_response(&mut ws, request_id(552)).await?;
    assert!(
        fuzzy
            .files
            .iter()
            .any(|file| file.path == "alpha_notes.txt" && file.file_name == "alpha_notes.txt"),
        "fuzzy search did not include alpha_notes.txt: {fuzzy:?}"
    );

    let session_id = format!("fuzzy-{}", Uuid::new_v4().simple());
    send_client_request(
        &mut ws,
        codex::ClientRequest::FuzzyFileSearchSessionStart {
            request_id: request_id(553),
            params: codex::FuzzyFileSearchSessionStartParams {
                session_id: session_id.clone(),
                roots: vec![home_dir.display().to_string()],
            },
        },
    )
    .await?;
    let _: codex::FuzzyFileSearchSessionStartResponse =
        read_typed_response(&mut ws, request_id(553)).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::FuzzyFileSearchSessionUpdate {
            request_id: request_id(554),
            params: codex::FuzzyFileSearchSessionUpdateParams {
                session_id: session_id.clone(),
                query: "beta".to_string(),
            },
        },
    )
    .await?;
    let _: codex::FuzzyFileSearchSessionUpdateResponse =
        read_typed_response(&mut ws, request_id(554)).await?;
    let fuzzy_update = read_fuzzy_file_search_update(&mut ws).await?;
    assert_eq!(fuzzy_update.session_id, session_id);
    assert_eq!(fuzzy_update.query, "beta");
    assert!(
        fuzzy_update
            .files
            .iter()
            .any(|file| file.path == "nested/beta_alpha.md"),
        "fuzzy search session update did not include nested/beta_alpha.md: {fuzzy_update:?}"
    );
    let fuzzy_completed = read_fuzzy_file_search_completed(&mut ws).await?;
    assert_eq!(fuzzy_completed.session_id, session_id);

    send_client_request(
        &mut ws,
        codex::ClientRequest::FuzzyFileSearchSessionStop {
            request_id: request_id(555),
            params: codex::FuzzyFileSearchSessionStopParams {
                session_id: session_id.clone(),
            },
        },
    )
    .await?;
    let _: codex::FuzzyFileSearchSessionStopResponse =
        read_typed_response(&mut ws, request_id(555)).await?;

    let repo = home_dir.join("git-repo");
    fs::create_dir_all(&repo)?;
    run_git_command(&repo, &["init"])?;
    fs::write(repo.join("tracked.txt"), "base\n")?;
    run_git_command(&repo, &["add", "tracked.txt"])?;
    run_git_command(
        &repo,
        &[
            "-c",
            "user.name=Gents Test",
            "-c",
            "user.email=gents-test@example.invalid",
            "commit",
            "-m",
            "base",
        ],
    )?;
    fs::write(repo.join("tracked.txt"), "base\nchanged\n")?;
    fs::write(repo.join("untracked.txt"), "new\n")?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::GitDiffToRemote {
            request_id: request_id(556),
            params: codex::GitDiffToRemoteParams { cwd: repo },
        },
    )
    .await?;
    let diff: codex::GitDiffToRemoteResponse =
        read_typed_response(&mut ws, request_id(556)).await?;
    assert!(
        diff.diff.contains("+changed"),
        "git diff did not include tracked change: {diff:?}"
    );
    assert!(
        diff.diff.contains("untracked.txt"),
        "git diff did not include untracked file: {diff:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_turn_steer_queues_gents_request_on_active_turn() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-steer-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start_hanging(&model_name)?;

    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-steer-{}", Uuid::new_v4().simple());
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
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;
    let thread_id = start_thread(&mut ws, &home_dir).await?;

    let initial_prompt = format!("hold the turn open {}", Uuid::new_v4().simple());
    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(201),
            params: codex::TurnStartParams {
                thread_id: thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: initial_prompt.clone(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let turn_start: codex::TurnStartResponse =
        read_typed_response(&mut ws, request_id(201)).await?;
    let started = read_turn_started(&mut ws).await?;
    assert_eq!(started.turn.id, turn_start.turn.id);

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnSteer {
            request_id: request_id(202),
            params: codex::TurnSteerParams {
                thread_id: thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: "wrong expected turn".to_string(),
                    text_elements: Vec::new(),
                }],
                responsesapi_client_metadata: None,
                expected_turn_id: "stale-turn".to_string(),
            },
        },
    )
    .await?;
    let error = read_error_response(&mut ws, request_id(202)).await?;
    assert_eq!(
        error.message,
        format!(
            "expected active turn id `stale-turn` but found `{}`",
            turn_start.turn.id
        )
    );

    let steer_prompt = format!("steer while active {}", Uuid::new_v4().simple());
    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnSteer {
            request_id: request_id(203),
            params: codex::TurnSteerParams {
                thread_id: thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: steer_prompt.clone(),
                    text_elements: Vec::new(),
                }],
                responsesapi_client_metadata: None,
                expected_turn_id: turn_start.turn.id.clone(),
            },
        },
    )
    .await?;
    let steer: codex::TurnSteerResponse = read_typed_response(&mut ws, request_id(203)).await?;
    assert_eq!(steer.turn_id, turn_start.turn.id);

    let (steering_request_id, session_id, metadata) =
        wait_for_request_metadata(&graphql, &agent_did, &steer_prompt).await?;
    assert_eq!(session_id, thread_id);
    assert_eq!(
        metadata.pointer("/queue/source").and_then(Value::as_str),
        Some("steering")
    );
    assert_eq!(
        metadata.pointer("/queue/policy").and_then(Value::as_str),
        Some("append")
    );
    assert_eq!(
        metadata
            .pointer("/queue/queued_after_request_id")
            .and_then(Value::as_str),
        Some(turn_start.turn.id.as_str())
    );
    assert_ne!(steering_request_id, turn_start.turn.id);

    let second_steer_prompt = format!("second steer while active {}", Uuid::new_v4().simple());
    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnSteer {
            request_id: request_id(205),
            params: codex::TurnSteerParams {
                thread_id: thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: second_steer_prompt.clone(),
                    text_elements: Vec::new(),
                }],
                responsesapi_client_metadata: None,
                expected_turn_id: turn_start.turn.id.clone(),
            },
        },
    )
    .await?;
    let second_steer: codex::TurnSteerResponse =
        read_typed_response(&mut ws, request_id(205)).await?;
    assert_eq!(second_steer.turn_id, turn_start.turn.id);

    let (second_steering_request_id, second_session_id, second_metadata) =
        wait_for_request_metadata(&graphql, &agent_did, &second_steer_prompt).await?;
    assert_eq!(second_session_id, thread_id);
    assert_eq!(
        second_metadata
            .pointer("/queue/queued_after_request_id")
            .and_then(Value::as_str),
        Some(steering_request_id.as_str()),
        "second steering request should queue after the current GENTS tail, not after the root turn"
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnInterrupt {
            request_id: request_id(204),
            params: codex::TurnInterruptParams {
                thread_id,
                turn_id: turn_start.turn.id,
            },
        },
    )
    .await?;
    let _: codex::TurnInterruptResponse = read_typed_response(&mut ws, request_id(204)).await?;
    wait_for_request_lifecycle_state(
        &graphql,
        &steering_request_id,
        &["interrupted"],
        Duration::from_secs(15),
    )
    .await?;
    wait_for_request_lifecycle_state(
        &graphql,
        &second_steering_request_id,
        &["interrupted"],
        Duration::from_secs(15),
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_interrupt_completes_with_running_background_tool() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-bg-interrupt-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start_hanging(&model_name)?;

    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-bg-interrupt-{}", Uuid::new_v4().simple());
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
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "50",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;
    let thread_id = start_thread(&mut ws, &home_dir).await?;

    let prompt = format!(
        "start background interrupt repro {}",
        Uuid::new_v4().simple()
    );
    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(220),
            params: codex::TurnStartParams {
                thread_id: thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: prompt.clone(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let turn_start: codex::TurnStartResponse =
        read_typed_response(&mut ws, request_id(220)).await?;
    let started = read_turn_started(&mut ws).await?;
    assert_eq!(started.turn.id, turn_start.turn.id);

    let (gents_request_id, session_id, _behavior_id) =
        wait_for_request(&graphql, &agent_did, &prompt).await?;
    assert_eq!(session_id, thread_id);
    let tool_call_key = format!("{session_id}:codex-bg-interrupt");
    seed_running_background_tool(&graphql, &gents_request_id, &session_id, &tool_call_key).await?;

    let started_process = tokio::time::timeout(
        Duration::from_secs(15),
        read_background_command_started(&mut ws, &tool_call_key),
    )
    .await
    .context("timed out waiting for shim to project running background tool")??;
    assert_eq!(started_process, tool_call_key);

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnInterrupt {
            request_id: request_id(221),
            params: codex::TurnInterruptParams {
                thread_id: thread_id.clone(),
                turn_id: turn_start.turn.id.clone(),
            },
        },
    )
    .await?;
    let interrupted_turn = tokio::time::timeout(
        Duration::from_secs(15),
        read_interrupt_response_and_completed_turn(&mut ws, request_id(221)),
    )
    .await
    .context("timed out waiting for interrupted turn with running background tool")??;
    assert_eq!(interrupted_turn.status, codex::TurnStatus::Interrupted);

    wait_for_request_lifecycle_state(
        &graphql,
        &gents_request_id,
        &["interrupted"],
        Duration::from_secs(15),
    )
    .await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadList {
            request_id: request_id(222),
            params: codex::ThreadListParams {
                cursor: None,
                limit: None,
                sort_key: None,
                sort_direction: None,
                model_providers: None,
                source_kinds: None,
                archived: None,
                cwd: None,
                use_state_db_only: true,
                search_term: None,
            },
        },
    )
    .await?;
    let _: codex::ThreadListResponse = tokio::time::timeout(
        Duration::from_secs(15),
        read_typed_response(&mut ws, request_id(222)),
    )
    .await
    .context("shim stopped answering after interrupting background-tool turn")??;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_projects_authorized_subagent_and_enforces_read_only_child_thread() -> Result<()>
{
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-subagent-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start_hanging(&model_name)?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-subagent-{}", Uuid::new_v4().simple());
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
    let behavior_id = format!("{agent_did}:default");
    let child_behavior_id = format!("{behavior_id}:reviewer");
    let child_backend_id = "child-projection-backend";
    let child_model_name = "child-projection-model";
    let child_model_selection = format!("{child_backend_id}::{child_model_name}");
    let root_model_selection =
        gents_model_selection_id(&default_backend_id(&agent_did), &model_name);
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "50",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;
    let parent_thread_id = start_thread(&mut ws, &home_dir).await?;
    let prompt = format!("hold subagent projection {}", Uuid::new_v4().simple());
    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(230),
            params: codex::TurnStartParams {
                thread_id: parent_thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: prompt.clone(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let turn_start: codex::TurnStartResponse = serve
        .capturing(read_typed_response(&mut ws, request_id(230)))
        .await?;
    let started = read_turn_started(&mut ws).await?;
    assert_eq!(started.turn.id, turn_start.turn.id);
    let parent_active = read_thread_status_changed(&mut ws, &parent_thread_id).await?;
    assert!(matches!(parent_active, codex::ThreadStatus::Active { .. }));
    let (parent_request_id, session_id, _) =
        wait_for_request(&graphql, &agent_did, &prompt).await?;
    assert_eq!(session_id, parent_thread_id);

    let child_thread_id = Uuid::new_v4().to_string();
    let child_request_id = Uuid::new_v4().to_string();
    let tool_call_id = format!("spawn-{}", Uuid::new_v4().simple());
    let tool_call_key = format!("{parent_thread_id}:{tool_call_id}");
    seed_authorized_subagent_link(
        &graphql,
        &agent_did,
        &child_behavior_id,
        &parent_request_id,
        &parent_thread_id,
        &child_request_id,
        &child_thread_id,
        &tool_call_id,
        &tool_call_key,
        child_backend_id,
        child_model_name,
    )
    .await?;

    let (running, projected_model, reasoning_effort_absent) = tokio::time::timeout(
        Duration::from_secs(15),
        read_collab_agent_status(&mut ws, &tool_call_key, &child_thread_id),
    )
    .await
    .context("timed out waiting for native subagent projection")??;
    assert_eq!(running, codex::CollabAgentStatus::Running);
    assert_eq!(
        projected_model.as_deref(),
        Some(child_model_selection.as_str())
    );
    assert!(reasoning_effort_absent);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadList {
            request_id: request_id(239),
            params: codex::ThreadListParams {
                cursor: None,
                limit: None,
                sort_key: None,
                sort_direction: None,
                model_providers: None,
                source_kinds: Some(vec![codex::ThreadSourceKind::SubAgentThreadSpawn]),
                archived: None,
                cwd: None,
                use_state_db_only: true,
                search_term: None,
            },
        },
    )
    .await?;
    let subagent_list: codex::ThreadListResponse = serve
        .capturing(read_typed_response(&mut ws, request_id(239)))
        .await?;
    assert_eq!(subagent_list.data.len(), 1, "{subagent_list:?}");
    assert_eq!(subagent_list.data[0].id, child_thread_id);
    assert!(matches!(
        subagent_list.data[0].source,
        codex::SessionSource::SubAgent(_)
    ));

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: request_id(231),
            params: codex::ThreadReadParams {
                thread_id: child_thread_id.clone(),
                include_turns: true,
            },
        },
    )
    .await?;
    let child_read: codex::ThreadReadResponse = serve
        .capturing(read_typed_response(&mut ws, request_id(231)))
        .await?;
    assert_eq!(child_read.thread.id, child_thread_id);
    assert!(matches!(
        child_read.thread.status,
        codex::ThreadStatus::Active { .. }
    ));
    assert_eq!(child_read.thread.turns.len(), 1);
    assert_eq!(
        child_read.thread.turns[0].status,
        codex::TurnStatus::InProgress
    );
    let child_json = serde_json::to_value(&child_read.thread)?;
    assert_eq!(
        child_json.pointer("/source/subAgent/thread_spawn/parent_thread_id"),
        Some(&Value::String(parent_thread_id.clone()))
    );
    assert!(child_json.get("parentThreadId").is_none());

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadResume {
            request_id: request_id(232),
            params: codex::ThreadResumeParams {
                thread_id: child_thread_id.clone(),
                cwd: None,
                ..Default::default()
            },
        },
    )
    .await?;
    let child_resume: codex::ThreadResumeResponse = serve
        .capturing(read_typed_response(&mut ws, request_id(232)))
        .await?;
    assert_eq!(child_resume.thread.id, child_thread_id);
    assert_eq!(child_resume.model, child_model_selection);

    delete_agent_behavior(&graphql, &child_behavior_id).await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadResume {
            request_id: request_id(241),
            params: codex::ThreadResumeParams {
                thread_id: child_thread_id.clone(),
                cwd: None,
                ..Default::default()
            },
        },
    )
    .await?;
    let child_resume_without_behavior: codex::ThreadResumeResponse = serve
        .capturing(read_typed_response(&mut ws, request_id(241)))
        .await?;
    assert_eq!(child_resume_without_behavior.thread.id, child_thread_id);
    assert_eq!(child_resume_without_behavior.model, root_model_selection);

    let live_child_text = format!("live child output {}", Uuid::new_v4().simple());
    let live_child_reasoning = format!("child reasoning {}", Uuid::new_v4().simple());
    let child_started_at_ms = seed_child_streaming_response(
        &graphql,
        &agent_did,
        &child_behavior_id,
        &child_request_id,
        &child_thread_id,
        &live_child_text,
        &live_child_reasoning,
    )
    .await?;
    let (projected_text, projected_reasoning, projected_started_at_ms) = match tokio::time::timeout(
        Duration::from_secs(15),
        read_child_agent_and_reasoning_deltas(&mut ws, &child_thread_id, &child_request_id),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            let (stdout, stderr) = serve.captured_output()?;
            bail!(
                "timed out waiting for live loaded-child delta\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
        }
    };
    assert_eq!(projected_text, live_child_text);
    assert_eq!(projected_reasoning, live_child_reasoning);
    assert_eq!(projected_started_at_ms, child_started_at_ms);

    // End the parent turn while the child response is still streaming. From
    // this point onward the connection-scoped root and loaded-child watchers,
    // rather than the parent turn stream, own the native card and child deltas.
    let unresolved_tool_call_key = format!("{parent_thread_id}:unresolved-spawn");
    let unresolved_completed_at_ms = seed_unresolved_completed_subagent_tool(
        &graphql,
        &agent_did,
        &parent_request_id,
        &parent_thread_id,
        &unresolved_tool_call_key,
    )
    .await?;
    update_request_lifecycle(&graphql, &parent_request_id, "failed").await?;
    tokio::time::timeout(
        Duration::from_secs(15),
        read_mcp_tool_completion(
            &mut ws,
            &unresolved_tool_call_key,
            unresolved_completed_at_ms,
        ),
    )
    .await
    .context("timed out waiting for terminal unresolved subagent MCP fallback")??;

    let parent_failed = tokio::time::timeout(
        Duration::from_secs(15),
        read_thread_status_changed(&mut ws, &parent_thread_id),
    )
    .await
    .context("timed out waiting for failed root thread status")??;
    assert_eq!(parent_failed, codex::ThreadStatus::SystemError);

    let reasoning_tail = " then checks the durable result";
    update_streaming_response_reasoning(
        &graphql,
        &child_request_id,
        &format!("{live_child_reasoning}{reasoning_tail}"),
        2,
    )
    .await?;
    let appended_reasoning = tokio::time::timeout(
        Duration::from_secs(15),
        read_child_reasoning_delta(&mut ws, &child_thread_id, &child_request_id),
    )
    .await
    .context("timed out waiting for appended child reasoning delta")??;
    assert_eq!(appended_reasoning, reasoning_tail);

    let durable_reasoning = format!("{live_child_reasoning}{reasoning_tail}");
    let child_materialized_at_ms = materialize_child_response_before_terminal(
        &graphql,
        &agent_did,
        &child_request_id,
        &child_thread_id,
        &durable_reasoning,
    )
    .await?;
    let (completed_reasoning, projected_materialized_at_ms) = tokio::time::timeout(
        Duration::from_secs(15),
        read_child_reasoning_completion(&mut ws, &child_thread_id, &child_request_id),
    )
    .await
    .context("timed out waiting for reasoning completion at the final reset-tail window")??;
    assert_eq!(completed_reasoning, durable_reasoning);
    assert_eq!(projected_materialized_at_ms, child_materialized_at_ms);

    let child_completed_at_ms =
        finalize_child_response_after_materialization(&graphql, &child_request_id).await?;
    let (completed, child_thread_status, projected_completed_at_ms) = tokio::time::timeout(
        Duration::from_secs(15),
        read_terminal_child_without_reasoning_replay(
            &mut ws,
            &child_thread_id,
            &child_request_id,
            &tool_call_key,
        ),
    )
    .await
    .context("timed out waiting for terminal child state after final reset-tail window")??;
    assert_eq!(completed, codex::CollabAgentStatus::Completed);
    assert_eq!(child_thread_status, codex::ThreadStatus::Idle);
    assert_eq!(projected_completed_at_ms, child_completed_at_ms);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: request_id(240),
            params: codex::ThreadReadParams {
                thread_id: child_thread_id.clone(),
                include_turns: true,
            },
        },
    )
    .await?;
    let completed_child: codex::ThreadReadResponse = serve
        .capturing(read_typed_response(&mut ws, request_id(240)))
        .await?;
    assert!(completed_child.thread.turns.iter().any(|turn| {
        turn.items.iter().any(|item| {
            matches!(
                item,
                codex::ThreadItem::Reasoning { summary, content, .. }
                    if summary.is_empty()
                        && content.len() == 1
                        && content.first().is_some_and(|text| text == &durable_reasoning)
            )
        })
    }));

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(233),
            params: codex::TurnStartParams {
                thread_id: child_thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: "must be rejected".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let start_error = read_error_response(&mut ws, request_id(233)).await?;
    assert!(start_error.message.contains("read-only"));

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnSteer {
            request_id: request_id(234),
            params: codex::TurnSteerParams {
                thread_id: child_thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: "must also be rejected".to_string(),
                    text_elements: Vec::new(),
                }],
                responsesapi_client_metadata: None,
                expected_turn_id: child_request_id.clone(),
            },
        },
    )
    .await?;
    let steer_error = read_error_response(&mut ws, request_id(234)).await?;
    assert!(steer_error.message.contains("read-only"));

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnInterrupt {
            request_id: request_id(235),
            params: codex::TurnInterruptParams {
                thread_id: child_thread_id,
                turn_id: child_request_id,
            },
        },
    )
    .await?;
    let interrupt_error = read_error_response(&mut ws, request_id(235)).await?;
    assert!(interrupt_error.message.contains("read-only"));

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: request_id(242),
            params: codex::ThreadReadParams {
                thread_id: parent_thread_id.clone(),
                include_turns: false,
            },
        },
    )
    .await?;
    let failed_parent: codex::ThreadReadResponse = serve
        .capturing(read_typed_response(&mut ws, request_id(242)))
        .await?;
    assert_eq!(
        failed_parent.thread.status,
        codex::ThreadStatus::SystemError
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_turn_steer_drains_queued_request_before_completing_turn() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-steer-drain-{}", Uuid::new_v4().simple());
    let initial_prompt = format!("first active turn {}", Uuid::new_v4().simple());
    let steer_prompt = format!("queued steering {}", Uuid::new_v4().simple());
    let first_reply = format!("first-drain-{}", Uuid::new_v4().simple());
    let second_reply = format!("second-drain-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start_routed_delayed(
        &model_name,
        vec![
            (steer_prompt.clone(), second_reply.clone()),
            (initial_prompt.clone(), first_reply.clone()),
        ],
        "steer-drain-title".to_string(),
        Duration::from_millis(750),
    )?;

    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-steer-drain-{}", Uuid::new_v4().simple());
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
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "50",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    initialize_config_and_thread(&mut ws, &home_dir).await?;
    let thread_id = start_thread(&mut ws, &home_dir).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(301),
            params: codex::TurnStartParams {
                thread_id: thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: initial_prompt.clone(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let turn_start: codex::TurnStartResponse =
        read_typed_response(&mut ws, request_id(301)).await?;
    let started = read_turn_started(&mut ws).await?;
    assert_eq!(started.turn.id, turn_start.turn.id);

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnSteer {
            request_id: request_id(302),
            params: codex::TurnSteerParams {
                thread_id: thread_id.clone(),
                input: vec![codex::UserInput::Text {
                    text: steer_prompt.clone(),
                    text_elements: Vec::new(),
                }],
                responsesapi_client_metadata: None,
                expected_turn_id: turn_start.turn.id.clone(),
            },
        },
    )
    .await?;
    let steer: codex::TurnSteerResponse = read_typed_response(&mut ws, request_id(302)).await?;
    assert_eq!(steer.turn_id, turn_start.turn.id);

    let capture = read_turn_capture(&mut ws).await?;
    assert_eq!(capture.turn.status, codex::TurnStatus::Completed);
    assert!(
        capture.text.contains(&first_reply),
        "turn completed before streaming first reply {first_reply}; text:\n{}",
        capture.text
    );
    assert!(
        capture.text.contains(&second_reply),
        "turn completed before draining steering reply {second_reply}; text:\n{}",
        capture.text
    );

    let (_initial_request_id, initial_session_id, _behavior_id) =
        wait_for_request(&graphql, &agent_did, &initial_prompt).await?;
    assert_eq!(initial_session_id, thread_id);
    let (steering_request_id, steering_session_id, metadata) =
        wait_for_request_metadata(&graphql, &agent_did, &steer_prompt).await?;
    assert_eq!(steering_session_id, thread_id);
    assert_ne!(steering_request_id, turn_start.turn.id);
    assert_eq!(
        metadata.pointer("/queue/source").and_then(Value::as_str),
        Some("steering")
    );
    assert_eq!(
        metadata
            .pointer("/queue/queued_after_request_id")
            .and_then(Value::as_str),
        Some(turn_start.turn.id.as_str())
    );

    let captured_requests = mock_endpoint.captured_chat_requests();
    assert!(
        captured_requests
            .iter()
            .any(|request| request_contains_role_text(request, "user", &initial_prompt)),
        "mock endpoint did not receive the initial prompt; captured={captured_requests:?}"
    );
    assert!(
        captured_requests
            .iter()
            .any(|request| request_contains_role_text(request, "user", &steer_prompt)),
        "mock endpoint did not receive the steering prompt; captured={captured_requests:?}"
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: request_id(303),
            params: codex::ThreadReadParams {
                thread_id: thread_id.clone(),
                include_turns: true,
            },
        },
    )
    .await?;
    let thread_history: codex::ThreadReadResponse =
        read_typed_response(&mut ws, request_id(303)).await?;
    assert_eq!(
        thread_history.thread.turns.len(),
        1,
        "queued steering should reload as one Codex turn"
    );
    let history_turn = &thread_history.thread.turns[0];
    assert_eq!(history_turn.id, turn_start.turn.id);
    assert_turn_has_user_text(history_turn, &initial_prompt);
    assert_turn_has_agent_text(history_turn, &first_reply);
    assert_turn_has_user_text(history_turn, &steer_prompt);
    assert_turn_has_agent_text(history_turn, &second_reply);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the configured real OpenAI-compatible backend"]
async fn codex_shim_live_protocol_uses_real_backend() -> Result<()> {
    let prompt_token = "PONGLIVE";
    let smoke = start_live_codex_shim().await?;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", smoke.shim_port))
        .await
        .context("connecting to live codex-shim websocket")?;

    initialize_config_and_thread(&mut ws, &smoke.home_dir).await?;
    let thread_id = start_thread(&mut ws, &smoke.home_dir).await?;
    let prompt = format!("Reply with exactly this token and no extra words: {prompt_token}");
    send_turn(&mut ws, &thread_id, &prompt).await?;
    let (final_text, completed_turn) = read_turn_to_completion(&mut ws).await?;

    assert_eq!(completed_turn.status, codex::TurnStatus::Completed);
    assert!(
        final_text.contains(prompt_token),
        "expected live Codex protocol stream to contain {prompt_token}, got:\n{final_text}"
    );
    let (_request_id, session_id, _behavior_id) =
        wait_for_request(&smoke.graphql, &smoke.agent_did, &prompt).await?;
    assert_eq!(session_id, thread_id);
    assert_shim_trace_methods(
        &smoke.shim_trace,
        &["initialize", "config/read", "thread/start", "turn/start"],
    )?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the configured real OpenAI-compatible backend"]
async fn codex_shim_live_runtime_spawn_projects_real_subagent() -> Result<()> {
    let suffix = Uuid::new_v4().simple().to_string();
    let child_token = format!("CHILDLIVE-{}", &suffix[..8]);
    let smoke = start_live_codex_shim().await?;
    let child_behavior_id = configure_live_local_subagent(&smoke).await?;
    let expected_child_model = gents_model_selection_id(&smoke.backend_id, &smoke.model_name);

    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", smoke.shim_port))
        .await
        .context("connecting to live codex-shim websocket")?;
    initialize_config_and_thread(&mut ws, &smoke.home_dir).await?;
    let thread_id = start_thread(&mut ws, &smoke.home_dir).await?;
    let prompt = format!(
        "Call spawn_subagent exactly once using the target named `codex-live-child`, \
         prompt `Reply with exactly {child_token} and no extra words`, and await_mode \
         `foreground`. Do not call any other tool. After the child returns, reply with \
         exactly {child_token} and no extra words."
    );
    send_turn(&mut ws, &thread_id, &prompt).await?;
    let capture = read_turn_capture(&mut ws).await?;

    assert_eq!(capture.turn.status, codex::TurnStatus::Completed);
    assert!(
        capture.text.contains(&child_token),
        "parent did not return the real child result token {child_token}: {}",
        capture.text
    );
    let (parent_request_id, parent_session_id, parent_behavior_id) =
        wait_for_request(&smoke.graphql, &smoke.agent_did, &prompt).await?;
    assert_eq!(parent_session_id, thread_id);
    assert_eq!(parent_behavior_id, smoke.behavior_id);

    let spawned = wait_for_real_spawn_projection(
        &smoke.graphql,
        &parent_request_id,
        &smoke.agent_did,
        &child_behavior_id,
        &child_token,
    )
    .await?;
    assert_eq!(spawned.parent_session_id, thread_id);

    let completed_spawn = capture
        .completed_collab_items
        .iter()
        .rev()
        .find(|item| {
            item.tool == codex::CollabAgentTool::SpawnAgent
                && item.receiver_thread_ids == vec![spawned.child_session_id.clone()]
                && item.child_status == Some(codex::CollabAgentStatus::Completed)
        })
        .ok_or_else(|| {
            anyhow!(
                "live turn did not project the completed runtime spawn as a native collab item: {:?}",
                capture.completed_collab_items
            )
        })?;
    assert_eq!(
        completed_spawn.status,
        codex::CollabAgentToolCallStatus::Completed
    );
    assert_eq!(
        completed_spawn.model.as_deref(),
        Some(expected_child_model.as_str())
    );

    wait_for_completed_inference_behaviors(
        &smoke.graphql,
        &smoke.backend_id,
        &[&smoke.behavior_id, &child_behavior_id],
    )
    .await?;
    assert_shim_trace_methods(
        &smoke.shim_trace,
        &[
            "initialize",
            "thread/start",
            "turn/start",
            "collabAgentToolCall",
        ],
    )?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the configured real OpenAI-compatible backend"]
async fn codex_shim_live_gents_filesystem_tools_project_to_codex_items() -> Result<()> {
    let suffix = Uuid::new_v4().simple().to_string();
    let token = format!("FSLIVE-{}", &suffix[..8]);
    let smoke = start_live_codex_shim_with_write_tools(true, None).await?;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", smoke.shim_port))
        .await
        .context("connecting to live codex-shim websocket")?;

    initialize_config_and_thread(&mut ws, &smoke.home_dir).await?;
    let thread_id = start_thread(&mut ws, &smoke.home_dir).await?;

    let fixture_dir = smoke.home_dir.join("live-fs-route");
    let fixture_file = fixture_dir.join("fixture.txt");
    let relative_fixture = "live-fs-route/fixture.txt";
    fs::create_dir_all(&fixture_dir)?;
    fs::write(&fixture_file, &token)?;

    let prompt = format!(
        "Use the read_file tool to read `{relative_fixture}` from the current working directory. Reply with exactly the file contents and no extra words."
    );
    send_turn(&mut ws, &thread_id, &prompt).await?;
    let capture = read_turn_capture(&mut ws).await?;

    assert_eq!(capture.turn.status, codex::TurnStatus::Completed);
    assert!(
        capture.text.contains(&token),
        "expected live backend to read fs route fixture token {token}, got:\n{}",
        capture.text
    );
    assert!(
        capture
            .completed_tools
            .iter()
            .any(|tool| tool.contains("read_file")),
        "live backend did not complete read_file; completed tools: {:?}\ntext:\n{}",
        capture.completed_tools,
        capture.text
    );
    let (_request_id, session_id, _behavior_id) =
        wait_for_request(&smoke.graphql, &smoke.agent_did, &prompt).await?;
    assert_eq!(session_id, thread_id);
    assert_shim_trace_methods(
        &smoke.shim_trace,
        &["initialize", "config/read", "thread/start", "turn/start"],
    )?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the configured real OpenAI-compatible backend"]
async fn codex_shim_live_thread_projection_survives_real_backend_turn() -> Result<()> {
    let prompt_token = "PROJLIVE";
    let thread_name = format!("GENTS live projection {}", Uuid::new_v4().simple());
    let goal_objective = format!("exercise live projection {}", Uuid::new_v4().simple());
    let git_branch = "codex-shim-live-projection".to_string();
    let smoke = start_live_codex_shim().await?;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", smoke.shim_port))
        .await
        .context("connecting to live codex-shim websocket")?;

    initialize_config_and_thread(&mut ws, &smoke.home_dir).await?;
    let thread_id = start_thread(&mut ws, &smoke.home_dir).await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadSetName {
            request_id: request_id(401),
            params: codex::ThreadSetNameParams {
                thread_id: thread_id.clone(),
                name: thread_name.clone(),
            },
        },
    )
    .await?;
    let _: codex::ThreadSetNameResponse = read_typed_response(&mut ws, request_id(401)).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadMemoryModeSet {
            request_id: request_id(402),
            params: codex::ThreadMemoryModeSetParams {
                thread_id: thread_id.clone(),
                mode: codex::ThreadMemoryMode::Disabled,
            },
        },
    )
    .await?;
    let _: codex::ThreadMemoryModeSetResponse =
        read_typed_response(&mut ws, request_id(402)).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadSettingsUpdate {
            request_id: request_id(403),
            params: codex::ThreadSettingsUpdateParams {
                thread_id: thread_id.clone(),
                cwd: Some(smoke.home_dir.clone()),
                ..Default::default()
            },
        },
    )
    .await?;
    let _: codex::ThreadSettingsUpdateResponse =
        read_typed_response(&mut ws, request_id(403)).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadGoalSet {
            request_id: request_id(404),
            params: codex::ThreadGoalSetParams {
                thread_id: thread_id.clone(),
                objective: Some(goal_objective.clone()),
                status: Some(codex::ThreadGoalStatus::Active),
                token_budget: Some(Some(321)),
            },
        },
    )
    .await?;
    let goal_set: codex::ThreadGoalSetResponse =
        read_typed_response(&mut ws, request_id(404)).await?;
    assert_eq!(goal_set.goal.thread_id, thread_id);
    assert_eq!(goal_set.goal.objective, goal_objective);
    assert_eq!(goal_set.goal.token_budget, Some(321));

    let expected_git_sha = init_test_git_repo(&smoke.home_dir, &git_branch)?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadMetadataUpdate {
            request_id: request_id(405),
            params: codex::ThreadMetadataUpdateParams {
                thread_id: thread_id.clone(),
                git_info: Some(codex::ThreadMetadataGitInfoUpdateParams {
                    sha: Some(Some(format!("ignored-{}", Uuid::new_v4().simple()))),
                    branch: Some(Some("ignored-client-branch".to_string())),
                    origin_url: None,
                }),
            },
        },
    )
    .await?;
    let metadata_update: codex::ThreadMetadataUpdateResponse =
        read_typed_response(&mut ws, request_id(405)).await?;
    assert_eq!(
        metadata_update.thread.name.as_deref(),
        Some(thread_name.as_str())
    );
    assert_eq!(
        metadata_update
            .thread
            .git_info
            .as_ref()
            .and_then(|git| git.sha.as_deref()),
        Some(expected_git_sha.as_str())
    );

    let prompt = format!("Reply with exactly this token and no extra words: {prompt_token}");
    send_turn(&mut ws, &thread_id, &prompt).await?;
    let (final_text, completed_turn) = read_turn_to_completion(&mut ws).await?;
    assert_eq!(completed_turn.status, codex::TurnStatus::Completed);
    assert!(
        final_text.contains(prompt_token),
        "expected live Codex protocol stream to contain {prompt_token}, got:\n{final_text}"
    );
    let (_request_id, session_id, _behavior_id) =
        wait_for_request(&smoke.graphql, &smoke.agent_did, &prompt).await?;
    assert_eq!(session_id, thread_id);

    let durable_response = graphql_query(
        &smoke.graphql,
        &format!(
            r#"{{
                AgentSession(filter: {{ session_id: {{ _eq: "{}" }} }}, limit: 1) {{
                    session_id
                    agent_did
                    behavior_id
                    status
                    started
                }}
                AgentConversation(filter: {{ session_id: {{ _eq: "{}" }} }}, limit: 1) {{
                    session_id
                    agent_did
                    behavior_id
                    title
                    title_source
                }}
            }}"#,
            escape_graphql_string(&thread_id),
            escape_graphql_string(&thread_id),
        ),
    )
    .await?;
    let session = first_graphql_row(&durable_response, "AgentSession")?;
    assert_eq!(
        session.get("session_id").and_then(Value::as_str),
        Some(thread_id.as_str())
    );
    assert_eq!(
        session.get("agent_did").and_then(Value::as_str),
        Some(smoke.agent_did.as_str())
    );
    let expected_behavior_id = format!("{}:default", smoke.agent_did);
    assert_eq!(
        session.get("behavior_id").and_then(Value::as_str),
        Some(expected_behavior_id.as_str())
    );
    assert_eq!(
        session.get("status").and_then(Value::as_str),
        Some("active")
    );
    assert!(
        session
            .get("started")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "AgentSession.started should be populated: {session}"
    );
    let conversation = first_graphql_row(&durable_response, "AgentConversation")?;
    assert_eq!(
        conversation.get("session_id").and_then(Value::as_str),
        Some(thread_id.as_str())
    );
    assert_eq!(
        conversation.get("agent_did").and_then(Value::as_str),
        Some(smoke.agent_did.as_str())
    );
    assert_eq!(
        conversation.get("behavior_id").and_then(Value::as_str),
        Some(expected_behavior_id.as_str())
    );
    assert_eq!(
        conversation.get("title").and_then(Value::as_str),
        Some(thread_name.as_str())
    );
    assert_eq!(
        conversation.get("title_source").and_then(Value::as_str),
        Some("user")
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: request_id(406),
            params: codex::ThreadReadParams {
                thread_id: thread_id.clone(),
                include_turns: false,
            },
        },
    )
    .await?;
    let thread_read: codex::ThreadReadResponse =
        read_typed_response(&mut ws, request_id(406)).await?;
    assert_eq!(thread_read.thread.id, thread_id);
    assert_eq!(
        thread_read.thread.name.as_deref(),
        Some(thread_name.as_str())
    );
    assert_eq!(
        thread_read
            .thread
            .git_info
            .as_ref()
            .and_then(|git| git.branch.as_deref()),
        Some(git_branch.as_str())
    );
    let history_turn = thread_read
        .thread
        .turns
        .iter()
        .find(|turn| turn.id == completed_turn.id)
        .ok_or_else(|| {
            anyhow!(
                "live thread/read did not include turn {}",
                completed_turn.id
            )
        })?;
    assert_turn_has_user_text(history_turn, &prompt);
    assert_turn_has_agent_text(history_turn, prompt_token);

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadList {
            request_id: request_id(407),
            params: codex::ThreadListParams {
                cursor: None,
                limit: None,
                sort_key: None,
                sort_direction: None,
                model_providers: None,
                source_kinds: None,
                archived: None,
                cwd: None,
                use_state_db_only: true,
                search_term: None,
            },
        },
    )
    .await?;
    let thread_list: codex::ThreadListResponse =
        read_typed_response(&mut ws, request_id(407)).await?;
    let listed = thread_list
        .data
        .iter()
        .find(|thread| thread.id == thread_id)
        .ok_or_else(|| anyhow!("live GENTS-backed thread list did not include {thread_id}"))?;
    assert_eq!(listed.name.as_deref(), Some(thread_name.as_str()));

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadGoalGet {
            request_id: request_id(408),
            params: codex::ThreadGoalGetParams {
                thread_id: thread_id.clone(),
            },
        },
    )
    .await?;
    let goal_get: codex::ThreadGoalGetResponse =
        read_typed_response(&mut ws, request_id(408)).await?;
    assert_eq!(
        goal_get.goal.as_ref().map(|goal| goal.objective.as_str()),
        Some(goal_objective.as_str())
    );

    assert_shim_trace_methods(
        &smoke.shim_trace,
        &[
            "initialize",
            "config/read",
            "thread/start",
            "thread/name/set",
            "thread/memoryMode/set",
            "thread/settings/update",
            "thread/goal/set",
            "thread/metadata/update",
            "turn/start",
            "thread/read",
            "thread/list",
            "thread/goal/get",
        ],
    )?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the configured real OpenAI-compatible backend"]
async fn codex_shim_live_protocol_supports_multiturn_memory() -> Result<()> {
    let memory_token = "LIME7";
    let smoke = start_live_codex_shim().await?;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", smoke.shim_port))
        .await
        .context("connecting to live codex-shim websocket")?;

    initialize_config_and_thread(&mut ws, &smoke.home_dir).await?;
    let thread_id = start_thread(&mut ws, &smoke.home_dir).await?;

    let first_prompt = multiturn_first_prompt(memory_token);
    send_turn(&mut ws, &thread_id, &first_prompt).await?;
    let (_first_text, first_turn) = read_turn_to_completion(&mut ws).await?;
    assert_eq!(first_turn.status, codex::TurnStatus::Completed);
    let (_request_id, session_id, _behavior_id) =
        wait_for_request(&smoke.graphql, &smoke.agent_did, &first_prompt).await?;
    assert_eq!(session_id, thread_id);

    let second_prompt = "What project codeword did I give earlier in this conversation? Reply with exactly the codeword and no extra words.";
    send_turn(&mut ws, &thread_id, second_prompt).await?;
    let (second_text, second_turn) = read_turn_to_completion(&mut ws).await?;

    assert_eq!(second_turn.status, codex::TurnStatus::Completed);
    assert!(
        second_text.contains(memory_token),
        "expected second live Codex protocol turn to remember {memory_token}, got:\n{second_text}"
    );
    let (_request_id, session_id, _behavior_id) =
        wait_for_request(&smoke.graphql, &smoke.agent_did, second_prompt).await?;
    assert_eq!(session_id, thread_id);
    assert_shim_trace_methods(
        &smoke.shim_trace,
        &["initialize", "config/read", "thread/start"],
    )?;
    assert_shim_trace_method_count_at_least(&smoke.shim_trace, "turn/start", 2)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires authenticated gh and the configured real OpenAI-compatible backend"]
async fn codex_shim_live_three_prompt_regression_writes_codex_home_trace() -> Result<()> {
    require_command("gh")?;
    if !gh_is_authenticated() {
        eprintln!("skipping three-prompt live regression: gh is not authenticated");
        return Ok(());
    }
    let repo_root = workspace_root()?;
    let home_root = std::env::var_os("HOME").map(std::path::PathBuf::from);
    let broad_tool_root = home_root
        .as_deref()
        .filter(|home| repo_root.starts_with(home))
        .unwrap_or_else(|| repo_root.parent().unwrap_or(repo_root.as_path()));
    let smoke = start_live_codex_shim_with_write_tools(true, Some(broad_tool_root)).await?;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", smoke.shim_port))
        .await
        .context("connecting to live codex-shim websocket")?;

    initialize_config_and_thread(&mut ws, &smoke.home_dir).await?;
    let thread_id = start_thread(&mut ws, &repo_root).await?;

    let cases: &[(&str, &str, &[&str], &str)] = &[
        (
            "repo overview",
            "hey codex! tell mea bout this repo",
            &["gents"],
            "read_file",
        ),
        (
            "github issues and prs",
            "amazing can you use gh to tell me about open issues and prs",
            &["issue", "pr"],
            "gh",
        ),
        (
            "lean state machines",
            "i'd like you to do a deep dive on the lean code and tell me how the state machines defined there interlock and interact",
            &["lean", "state"],
            "read_file",
        ),
    ];
    let mut captures = Vec::new();

    for &(label, prompt, expected_text, expected_tool) in cases {
        send_turn(&mut ws, &thread_id, prompt).await?;
        let capture = read_turn_capture(&mut ws).await?;

        assert_eq!(
            capture.turn.status,
            codex::TurnStatus::Completed,
            "{label} turn did not complete: {:?}",
            capture.turn
        );
        assert_text_contains_all_case_insensitive(&capture.text, label, expected_text);
        assert!(
            capture
                .completed_tools
                .iter()
                .any(|tool| tool.contains(expected_tool)),
            "{label} did not complete expected tool {expected_tool}; completed tools: {:?}\ntext:\n{}",
            capture.completed_tools,
            capture.text
        );
        assert!(
            !capture.started_tools.is_empty(),
            "{label} did not stream any started tool items; events: {:?}\ntext:\n{}",
            capture.event_order,
            capture.text
        );
        assert!(
            turn_had_tool_before_later_agent_text(&capture),
            "{label} did not stream a tool item before later assistant text; events: {:?}\ntext:\n{}",
            capture.event_order,
            capture.text
        );
        assert!(
            !turn_had_tool_after_final_agent_text(&capture),
            "{label} streamed tool items after the final assistant text; events: {:?}\ntext:\n{}",
            capture.event_order,
            capture.text
        );
        assert!(
            capture
                .turn_completed_tool_ids
                .iter()
                .all(|id| capture.completed_tool_ids.contains(id)),
            "{label} turn/completed introduced tool ids that were not streamed first; completed ids: {:?}; turn/completed ids: {:?}",
            capture.completed_tool_ids,
            capture.turn_completed_tool_ids
        );
        assert_eq!(
            capture.turn.items_view,
            codex::TurnItemsView::NotLoaded,
            "{label} turn/completed should not send a replayable full item snapshot"
        );
        assert!(
            capture.turn.items.is_empty(),
            "{label} turn/completed should not repeat streamed items: {:?}",
            capture.turn.items
        );
        let (_request_id, session_id, _behavior_id) =
            wait_for_request(&smoke.graphql, &smoke.agent_did, prompt).await?;
        assert_eq!(session_id, thread_id);
        captures.push(capture);
    }

    let default_trace = smoke.codex_home.join("log").join("codex-shim-events.jsonl");
    assert_eq!(smoke.shim_trace, default_trace);
    assert!(
        smoke.codex_home.is_dir(),
        "expected Codex home to exist at {}",
        smoke.codex_home.display()
    );
    assert_shim_trace_methods(
        &smoke.shim_trace,
        &[
            "initialize",
            "config/read",
            "thread/start",
            "agent_message/delta",
            "item/started",
            "item/completed",
            "turn/completed",
            "mcpToolCall",
            "commandExecution",
            "read_file",
        ],
    )?;
    assert_shim_trace_method_count_at_least(&smoke.shim_trace, "turn/start", cases.len())?;
    assert_shim_trace_method_count_at_least(&smoke.shim_trace, "turn/completed", cases.len())?;

    assert_eq!(captures.len(), cases.len());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_remote_frontend_keeps_client_codex_home_separate() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    let client_codex_home = tempdir.path().join("existing-client-codex-home");
    fs::create_dir_all(&home_dir)?;
    fs::create_dir_all(&client_codex_home)?;
    fs::write(
        client_codex_home.join("config.toml"),
        "# Existing user Codex config should remain client-side.\n",
    )?;

    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "unused")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());
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
    let expected_model_selection =
        gents_model_selection_id(&default_backend_id(&agent_did), &model_name);
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let expected_shim_home = home_dir.join(".gents").join("codex-ui");
    let (_stdout, stderr) = serve.captured_output()?;
    assert!(
        stderr.contains("Chat from another terminal with: gents codex"),
        "server guidance should point at the embedded codex subcommand; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains(&format!("--remote ws://127.0.0.1:{shim_port}/")),
        "non-default shim addresses should include the --remote hint; stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("CODEX_HOME="),
        "server guidance should not instruct users to replace their existing Codex home; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains(&expected_shim_home.to_string_lossy().to_string()),
        "server guidance should still identify the shim state dir; stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains(&client_codex_home.to_string_lossy().to_string()),
        "server guidance must not depend on or rewrite a user's local Codex home; stderr:\n{stderr}"
    );

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let initialize: codex::InitializeResponse = read_typed_response(&mut ws, request_id(1)).await?;
    assert_eq!(
        initialize.codex_home.as_path(),
        expected_shim_home.as_path(),
        "initialize codexHome is shim state, not the user's local Codex home"
    );
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadStart {
            request_id: request_id(2),
            params: codex::ThreadStartParams {
                model: Some("client-local-model-from-existing-codex-config".to_string()),
                model_provider: Some("openai".to_string()),
                approval_policy: Some(codex::AskForApproval::OnRequest),
                sandbox: Some(codex::SandboxMode::ReadOnly),
                cwd: None,
                ..Default::default()
            },
        },
    )
    .await?;
    let thread_start: codex::ThreadStartResponse =
        read_typed_response(&mut ws, request_id(2)).await?;
    assert_eq!(
        thread_start.model, expected_model_selection,
        "Gents remote runtime should use the bound behavior model, not the client Codex model"
    );
    assert_eq!(thread_start.model_provider, "gents");
    assert_eq!(thread_start.approval_policy, codex::AskForApproval::Never);
    let expected_server_cwd = home_dir
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", home_dir.display()))?;
    assert_eq!(
        thread_start.cwd.as_path(),
        expected_server_cwd.as_path(),
        "without a remote --cd override, the shim should keep its server-side cwd"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires stock codex CLI, expect, and the configured real OpenAI-compatible backend"]
async fn stock_codex_remote_pty_smoke_uses_existing_client_codex_home_with_real_backend(
) -> Result<()> {
    require_command("codex")?;
    require_command("expect")?;
    let prompt_token = "PONGPTY";
    let smoke = start_live_codex_shim().await?;
    let client_codex_home = create_existing_client_codex_home(&smoke, "pty")?;
    assert_ne!(client_codex_home, smoke.codex_home);

    let transcript = smoke.tempdir.path().join("codex-pty.log");
    let expect_script = smoke.tempdir.path().join("codex-pty-smoke.expect");
    write_expect_smoke(
        &expect_script,
        &transcript,
        &client_codex_home,
        smoke.shim_port,
        prompt_token,
    )?;

    let output = Command::new("expect")
        .arg(&expect_script)
        .output()
        .context("running codex --remote PTY smoke through expect")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let transcript = fs::read_to_string(&transcript).unwrap_or_default();
    if !output.status.success() {
        let (server_stdout, server_stderr) = smoke._server.captured_output()?;
        let shim_trace = fs::read_to_string(&smoke.shim_trace).unwrap_or_default();
        bail!(
            "codex --remote PTY smoke failed\nstdout:\n{stdout}\nstderr:\n{stderr}\ntranscript:\n{transcript}\nserver stdout:\n{server_stdout}\nserver stderr:\n{server_stderr}\nshim trace:\n{shim_trace}"
        );
    }
    let token_search_text = terminal_token_search_text(&transcript);
    assert!(
        token_occurrences(&token_search_text, prompt_token) >= 2,
        "expected PTY transcript to contain an echoed prompt and assistant response for {prompt_token}\nstdout:\n{stdout}\nstderr:\n{stderr}\ntranscript:\n{transcript}"
    );
    let prompt = smoke_prompt(prompt_token);
    let (_request_id, _session_id, _behavior_id) =
        wait_for_request(&smoke.graphql, &smoke.agent_did, &prompt).await?;
    assert_shim_trace_methods(
        &smoke.shim_trace,
        &["initialize", "thread/start", "turn/start"],
    )?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires tmux, stock codex CLI, and the configured real OpenAI-compatible backend"]
async fn stock_codex_remote_tmux_smoke_uses_existing_client_codex_home_with_real_backend(
) -> Result<()> {
    require_command("codex")?;
    if which("tmux").is_none() {
        eprintln!("skipping tmux smoke: tmux is not installed");
        return Ok(());
    }
    let prompt_token = "PONGTMUX";
    let smoke = start_live_codex_shim().await?;
    let client_codex_home = create_existing_client_codex_home(&smoke, "tmux")?;
    assert_ne!(client_codex_home, smoke.codex_home);
    let session = format!("gents-codex-smoke-{}", Uuid::new_v4().simple());
    let command = format!(
        "CODEX_HOME={} codex --no-alt-screen --dangerously-bypass-approvals-and-sandbox --remote ws://127.0.0.1:{} {}",
        shell_quote_path(&client_codex_home),
        smoke.shim_port,
        shell_quote(&format!(
            "Reply with exactly this token and no extra words: {prompt_token}"
        )),
    );

    let new_status = Command::new("tmux")
        .args(["new-session", "-d", "-s", &session, &command])
        .status()
        .context("starting tmux codex smoke session")?;
    if !new_status.success() {
        bail!("tmux new-session failed");
    }
    let transcript =
        wait_for_tmux_token_occurrences(&session, prompt_token, 2, Duration::from_secs(180))?;
    let _ = Command::new("tmux")
        .args(["kill-session", "-t", &session])
        .status();
    let token_search_text = terminal_token_search_text(&transcript);
    assert!(
        token_occurrences(&token_search_text, prompt_token) >= 2,
        "expected tmux transcript to contain an echoed prompt and assistant response for {prompt_token}, got:\n{transcript}"
    );
    let prompt = smoke_prompt(prompt_token);
    let (_request_id, _session_id, _behavior_id) =
        wait_for_request(&smoke.graphql, &smoke.agent_did, &prompt).await?;
    assert_shim_trace_methods(
        &smoke.shim_trace,
        &["initialize", "thread/start", "turn/start"],
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires tmux, stock codex CLI, and the configured real OpenAI-compatible backend"]
async fn stock_codex_remote_tmux_multiturn_uses_existing_client_codex_home_with_real_backend(
) -> Result<()> {
    require_command("codex")?;
    if which("tmux").is_none() {
        eprintln!("skipping tmux multi-turn smoke: tmux is not installed");
        return Ok(());
    }
    let memory_token = "LIME7";
    let transformed_token = "MINT7";
    let first_prompt = multiturn_first_prompt(memory_token);
    let second_prompt = multiturn_second_prompt();
    let smoke = start_live_codex_shim().await?;
    let client_codex_home = create_existing_client_codex_home(&smoke, "tmux-multiturn")?;
    assert_ne!(client_codex_home, smoke.codex_home);
    let session = format!("gents-codex-multiturn-{}", Uuid::new_v4().simple());
    let command = format!(
        "CODEX_HOME={} codex --no-alt-screen --dangerously-bypass-approvals-and-sandbox --remote ws://127.0.0.1:{} {}",
        shell_quote_path(&client_codex_home),
        smoke.shim_port,
        shell_quote(&first_prompt),
    );

    let new_status = Command::new("tmux")
        .args(["new-session", "-d", "-s", &session, &command])
        .status()
        .context("starting tmux codex multi-turn smoke session")?;
    if !new_status.success() {
        bail!("tmux new-session failed");
    }

    let result: Result<()> = async {
        wait_for_tmux_token_occurrences(&session, "READY", 2, Duration::from_secs(180))?;
        let literal_status = Command::new("tmux")
            .args(["send-keys", "-t", &session, "-l", second_prompt])
            .status()
            .context("sending second prompt to tmux codex session")?;
        if !literal_status.success() {
            bail!("tmux send-keys second prompt failed");
        }
        std::thread::sleep(Duration::from_millis(1500));
        let enter_status = Command::new("tmux")
            .args(["send-keys", "-t", &session, "Enter"])
            .status()
            .context("submitting second prompt in tmux codex session")?;
        if !enter_status.success() {
            bail!("tmux send-keys Enter failed");
        }

        let transcript = wait_for_tmux_token_occurrences(
            &session,
            transformed_token,
            1,
            Duration::from_secs(180),
        )?;
        let token_search_text = terminal_token_search_text(&transcript);
        assert!(
            token_occurrences(&token_search_text, transformed_token) >= 1,
            "expected tmux transcript to contain transformed multi-turn response {transformed_token}, got:\n{transcript}"
        );
        let (_request_id, first_session_id, _behavior_id) =
            wait_for_request(&smoke.graphql, &smoke.agent_did, &first_prompt).await?;
        let (_request_id, second_session_id, _behavior_id) =
            wait_for_request(&smoke.graphql, &smoke.agent_did, second_prompt).await?;
        assert_eq!(first_session_id, second_session_id);
        assert_shim_trace_methods(&smoke.shim_trace, &["initialize", "thread/start"])?;
        assert_shim_trace_method_count_at_least(&smoke.shim_trace, "turn/start", 2)?;
        Ok(())
    }
    .await;
    let _ = Command::new("tmux")
        .args(["kill-session", "-t", &session])
        .status();
    result?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_waits_for_a_missing_bound_behavior_instead_of_disabling() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "irrelevant")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());

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

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let (mut serve, readiness) = spawn_server_with_ready_json(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-behavior-id",
            "behavior-that-does-not-exist",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    assert_eq!(
        readiness
            .pointer("/codex_shim/pending")
            .and_then(Value::as_bool),
        Some(true),
        "server readiness must report the shim as pending: {readiness}"
    );
    assert_eq!(
        readiness
            .pointer("/codex_shim/bound_behavior_id")
            .and_then(Value::as_str),
        Some("behavior-that-does-not-exist"),
        "server readiness must name the missing bound behavior: {readiness}"
    );

    let (_stdout, stderr) = serve.captured_output()?;
    assert!(
        stderr.contains("Codex endpoint pending"),
        "a missing bound behavior is suppliable, so the shim must wait rather than \
         disable itself; got:\n{stderr}"
    );
    assert!(
        !stderr.contains("Codex endpoint disabled"),
        "a missing behavior must not be reported as a terminal disable (#699); got:\n{stderr}"
    );
    assert!(
        stderr.contains("behavior-that-does-not-exist"),
        "expected stderr to name the behavior it is waiting for; got:\n{stderr}"
    );
    assert!(
        stderr.contains("no restart needed"),
        "the operator must be told the shim converges on its own; got:\n{stderr}"
    );
    assert!(
        std::net::TcpStream::connect(("127.0.0.1", shim_port)).is_err(),
        "the shim port must stay closed while its bound behavior does not exist"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_model_list_enumerates_backend_models() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "irrelevant")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());

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
    let default_backend_id = default_backend_id(&agent_did);
    let default_model_selection = gents_model_selection_id(&default_backend_id, &model_name);
    let extra_model_name = format!("mock-codex-shim-extra-model-{}", Uuid::new_v4().simple());
    let extra_endpoint = MockChatEndpoint::start(&extra_model_name, "irrelevant")?;
    let extra_backend_id = format!("extra-backend-{}", Uuid::new_v4().simple());
    let extra_model_selection = gents_model_selection_id(&extra_backend_id, &extra_model_name);
    let duplicate_backend_id = format!("duplicate-backend-{}", Uuid::new_v4().simple());
    let duplicate_model_selection = gents_model_selection_id(&duplicate_backend_id, &model_name);

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &["--codex-shim", "--codex-shim-port", &shim_port_string],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    let create_extra_backend = format!(
        r#"mutation {{
            create_InferenceBackend(input: {{
                backend_id: "{extra_backend_id}",
                name: "Extra Backend",
                provider_kind: "OpenAiCompatible",
                endpoint: "{}",
                max_concurrent: 1,
                max_queue_depth: 100,
                enabled: true,
                models: ["{extra_model_name}"],
                probe_status: "healthy"
            }}) {{ _docID }}
            create_duplicate: create_InferenceBackend(input: {{
                backend_id: "{duplicate_backend_id}",
                name: "Duplicate Backend",
                provider_kind: "OpenAiCompatible",
                endpoint: "{}",
                max_concurrent: 1,
                max_queue_depth: 100,
                enabled: true,
                models: ["{model_name}"],
                probe_status: "healthy"
            }}) {{ _docID }}
        }}"#,
        escape_graphql_string(extra_endpoint.endpoint()),
        escape_graphql_string(extra_endpoint.endpoint())
    );
    serve
        .capturing(graphql_query(&graphql, &create_extra_backend))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _initialize: codex::InitializeResponse =
        read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ModelList {
            request_id: request_id(2),
            params: codex::ModelListParams::default(),
        },
    )
    .await?;
    let model_list: codex::ModelListResponse = read_typed_response(&mut ws, request_id(2)).await?;

    let ids: Vec<&str> = model_list
        .data
        .iter()
        .map(|entry| entry.id.as_str())
        .collect();
    assert!(
        ids.contains(&default_model_selection.as_str()),
        "expected default model selection {default_model_selection} in model list; got {ids:?}"
    );
    assert!(
        ids.contains(&extra_model_selection.as_str()),
        "expected extra model selection {extra_model_selection} in model list; got {ids:?}"
    );
    assert!(
        ids.contains(&duplicate_model_selection.as_str()),
        "expected duplicate model selection {duplicate_model_selection} in model list; got {ids:?}"
    );
    let default_entry = model_list
        .data
        .iter()
        .find(|entry| entry.id == default_model_selection)
        .expect("default model present");
    assert_eq!(default_entry.model, default_model_selection);
    assert_eq!(default_entry.display_name, model_name);
    assert!(
        default_entry.is_default,
        "default model should be flagged as isDefault"
    );
    let extra_entry = model_list
        .data
        .iter()
        .find(|entry| entry.id == extra_model_selection)
        .expect("extra model present");
    assert!(
        !extra_entry.is_default,
        "non-default model must not be flagged isDefault"
    );
    let duplicate_entry = model_list
        .data
        .iter()
        .find(|entry| entry.id == duplicate_model_selection)
        .expect("duplicate backend model present");
    assert_eq!(duplicate_entry.display_name, model_name);
    assert!(
        !duplicate_entry.is_default,
        "duplicate backend model must not be flagged isDefault"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_config_read_reflects_doc_mutation() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;
    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "irrelevant")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());

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
    let default_behavior_id = format!("{agent_did}:default");
    let default_backend_id = default_backend_id(&agent_did);
    let alt_model_name = format!("alt-model-{}", Uuid::new_v4().simple());
    let alt_model_selection = gents_model_selection_id(&default_backend_id, &alt_model_name);

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &["--codex-shim", "--codex-shim-port", &shim_port_string],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    let switch_behavior = format!(
        r#"mutation {{
            update_AgentBehavior(
                filter: {{ behavior_id: {{ _eq: "{default_behavior_id}" }} }},
                input: {{ model_name: "{alt_model_name}" }}
            ) {{ _docID }}
        }}"#
    );
    serve
        .capturing(graphql_query(&graphql, &switch_behavior))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _initialize: codex::InitializeResponse =
        read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ConfigRead {
            request_id: request_id(2),
            params: codex::ConfigReadParams {
                include_layers: false,
                cwd: None,
            },
        },
    )
    .await?;
    let config: codex::ConfigReadResponse = read_typed_response(&mut ws, request_id(2)).await?;
    assert_eq!(
        config.config.model.as_deref(),
        Some(alt_model_selection.as_str()),
        "ConfigRead should reflect the doc-mutated backend-qualified model selection"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_config_value_write_model_mutates_behavior() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;
    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "irrelevant")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());

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
    let default_behavior_id = format!("{agent_did}:default");
    let original_profile_id = format!("{agent_did}:default-profile");
    let alt_model_name = format!("mock-codex-shim-alt-model-{}", Uuid::new_v4().simple());
    let alt_endpoint = MockChatEndpoint::start(&alt_model_name, "irrelevant")?;
    let alt_backend_id = format!("alt-backend-{}", Uuid::new_v4().simple());
    let alt_model_selection = gents_model_selection_id(&alt_backend_id, &alt_model_name);

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &["--codex-shim", "--codex-shim-port", &shim_port_string],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    let create_alt_backend = format!(
        r#"mutation {{
            create_InferenceBackend(input: {{
                backend_id: "{alt_backend_id}",
                name: "Alt Backend",
                provider_kind: "OpenAiCompatible",
                endpoint: "{}",
                max_concurrent: 1,
                max_queue_depth: 100,
                enabled: true,
                models: ["{alt_model_name}"],
                probe_status: "healthy"
            }}) {{ _docID }}
        }}"#,
        escape_graphql_string(alt_endpoint.endpoint())
    );
    serve
        .capturing(graphql_query(&graphql, &create_alt_backend))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _initialize: codex::InitializeResponse = serve
        .capturing(read_typed_response(&mut ws, request_id(1)))
        .await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ConfigValueWrite {
            request_id: request_id(2),
            params: codex::ConfigValueWriteParams {
                key_path: "model".to_string(),
                value: serde_json::Value::String(alt_model_selection),
                merge_strategy: codex::MergeStrategy::Replace,
                file_path: None,
                expected_version: None,
            },
        },
    )
    .await?;
    let _write: codex::ConfigWriteResponse = serve
        .capturing(read_typed_response(&mut ws, request_id(2)))
        .await?;

    let resp = serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"{{
                AgentBehavior(
                    filter: {{ behavior_id: {{ _eq: "{default_behavior_id}" }} }},
                    limit: 1
                ) {{ backend_id model_name inference_profile_id }}
            }}"#
            ),
        ))
        .await?;
    let stored_backend = resp
        .pointer("/data/AgentBehavior/0/backend_id")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .unwrap_or_default();
    assert_eq!(
        stored_backend, alt_backend_id,
        "AgentBehavior.backend_id should reflect ConfigValueWrite"
    );
    let stored_model = resp
        .pointer("/data/AgentBehavior/0/model_name")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .unwrap_or_default();
    assert_eq!(
        stored_model, alt_model_name,
        "AgentBehavior.model_name should reflect ConfigValueWrite"
    );
    let stored_profile = resp
        .pointer("/data/AgentBehavior/0/inference_profile_id")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .unwrap_or_default();
    assert_eq!(
        stored_profile, original_profile_id,
        "AgentBehavior.inference_profile_id should remain unchanged by model selection"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_config_value_write_rejects_unknown_model() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;
    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "irrelevant")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());

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
    let default_behavior_id = format!("{agent_did}:default");
    let original_backend_id = format!("{agent_did}:backend");
    let original_profile_id = format!("{agent_did}:default-profile");

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &["--codex-shim", "--codex-shim-port", &shim_port_string],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _initialize: codex::InitializeResponse =
        read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ConfigValueWrite {
            request_id: request_id(2),
            params: codex::ConfigValueWriteParams {
                key_path: "model".to_string(),
                value: serde_json::Value::String("definitely-not-real".to_string()),
                merge_strategy: codex::MergeStrategy::Replace,
                file_path: None,
                expected_version: None,
            },
        },
    )
    .await?;
    let error = read_error_response(&mut ws, request_id(2)).await?;
    assert!(
        error.message.contains("model") && error.message.contains("not found"),
        "expected error to mention missing model; got: {}",
        error.message
    );

    let resp = serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"{{
                AgentBehavior(
                    filter: {{ behavior_id: {{ _eq: "{default_behavior_id}" }} }},
                    limit: 1
                ) {{ backend_id model_name inference_profile_id }}
            }}"#
            ),
        ))
        .await?;
    let stored_backend = resp
        .pointer("/data/AgentBehavior/0/backend_id")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .unwrap_or_default();
    assert_eq!(
        stored_backend, original_backend_id,
        "behavior backend_id must remain unchanged after rejected write"
    );
    let stored_model = resp
        .pointer("/data/AgentBehavior/0/model_name")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .unwrap_or_default();
    assert_eq!(
        stored_model, model_name,
        "behavior model_name must remain unchanged after rejected write"
    );
    let stored_profile = resp
        .pointer("/data/AgentBehavior/0/inference_profile_id")
        .and_then(|v| v.as_str())
        .map(ToOwned::to_owned)
        .unwrap_or_default();
    assert_eq!(
        stored_profile, original_profile_id,
        "behavior inference_profile_id must remain unchanged after rejected write"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_does_not_clobber_session_behavior_id() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;
    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "irrelevant")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());

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
    let default_behavior_id = format!("{agent_did}:default");
    let session_id = format!("test-session-{}", Uuid::new_v4().simple());

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &["--codex-shim", "--codex-shim-port", &shim_port_string],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"mutation {{
                create_AgentSession(input: {{
                    session_id: "{session_id}",
                    agent_name: "preexisting",
                    behavior_id: "{default_behavior_id}",
                    status: "active"
                }}) {{ _docID }}
            }}"#
            ),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _initialize: codex::InitializeResponse = serve
        .capturing(read_typed_response(&mut ws, request_id(1)))
        .await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadResume {
            request_id: request_id(2),
            params: codex::ThreadResumeParams {
                thread_id: session_id.clone(),
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let _ = serve.capturing(read_jsonrpc(&mut ws)).await?;

    let resp = serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"{{
                AgentSession(
                    filter: {{ session_id: {{ _eq: "{session_id}" }} }},
                    limit: 1
                ) {{ agent_name behavior_id }}
            }}"#
            ),
        ))
        .await?;
    let preserved_agent_name = resp
        .pointer("/data/AgentSession/0/agent_name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let preserved_behavior_id = resp
        .pointer("/data/AgentSession/0/behavior_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        preserved_agent_name, "preexisting",
        "agent_name must not be clobbered by the shim's session upsert"
    );
    assert_eq!(
        preserved_behavior_id, default_behavior_id,
        "behavior_id must remain pinned to its create-time value"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_does_not_adopt_a_session_from_another_behavior() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;
    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "irrelevant")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());

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
    let session_id = format!("test-session-{}", Uuid::new_v4().simple());
    let foreign_behavior_id = "some-other-behavior".to_string();

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &["--codex-shim", "--codex-shim-port", &shim_port_string],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;

    serve
        .capturing(graphql_query(
            &graphql,
            &format!(
                r#"mutation {{
                create_AgentSession(input: {{
                    session_id: "{session_id}",
                    agent_name: "foreign",
                    behavior_id: "{foreign_behavior_id}",
                    status: "active"
                }}) {{ _docID }}
            }}"#
            ),
        ))
        .await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _initialize: codex::InitializeResponse =
        read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadResume {
            request_id: request_id(2),
            params: codex::ThreadResumeParams {
                thread_id: session_id.clone(),
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let error = read_error_response(&mut ws, request_id(2)).await?;
    assert!(
        error.message.contains("unknown Codex thread"),
        "a session outside the bound behavior must not enter the projection: {}",
        error.message
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadArchive {
            request_id: request_id(3),
            params: codex::ThreadArchiveParams {
                thread_id: session_id,
            },
        },
    )
    .await?;
    let error = read_error_response(&mut ws, request_id(3)).await?;
    assert!(
        error.message.contains("unknown Codex thread"),
        "archiving a session outside the bound behavior must fail explicitly: {}",
        error.message
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_lists_and_toggles_skills() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-skill-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "ok")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-skill-{}", Uuid::new_v4().simple());
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

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    let added = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "add",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--skill-id",
            "research",
            "--scope",
            "principal",
            "--name",
            "Research",
            "--description",
            "Find and cite sources",
            "--instructions",
            "Always cite your sources.",
        ],
    )?;
    assert_eq!(
        added.get("skill_id").and_then(Value::as_str),
        Some("research")
    );

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _: codex::InitializeResponse = read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::SkillsList {
            request_id: request_id(2),
            params: codex::SkillsListParams::default(),
        },
    )
    .await?;
    let list: codex::SkillsListResponse = read_typed_response(&mut ws, request_id(2)).await?;
    let research = list
        .data
        .iter()
        .flat_map(|entry| entry.skills.iter())
        .find(|skill| skill.name == "Research")
        .expect("Research skill should be listed");
    assert!(research.enabled, "newly added skill should be enabled");
    assert_eq!(research.scope, codex::SkillScope::System);

    send_client_request(
        &mut ws,
        codex::ClientRequest::SkillsConfigWrite {
            request_id: request_id(3),
            params: codex::SkillsConfigWriteParams {
                path: None,
                name: Some("Research".to_string()),
                enabled: false,
            },
        },
    )
    .await?;
    let write: codex::SkillsConfigWriteResponse =
        read_typed_response(&mut ws, request_id(3)).await?;
    assert!(
        !write.effective_enabled,
        "config write should report disabled"
    );

    send_client_request(
        &mut ws,
        codex::ClientRequest::SkillsList {
            request_id: request_id(4),
            params: codex::SkillsListParams::default(),
        },
    )
    .await?;
    let list: codex::SkillsListResponse = read_typed_response(&mut ws, request_id(4)).await?;
    let research = list
        .data
        .iter()
        .flat_map(|entry| entry.skills.iter())
        .find(|skill| skill.name == "Research")
        .expect("Research skill should still be listed");
    assert!(
        !research.enabled,
        "skill should be disabled after skills/config/write"
    );

    let _ = ws.close(None).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_live_skill_add_reaches_model_in_conversation() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let expected_reply = format!("skill-live-ok-{}", Uuid::new_v4().simple());
    let model_name = format!("mock-skill-live-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, &expected_reply)?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-skill-live-{}", Uuid::new_v4().simple());
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

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    let gen0 = wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    let catalog_phrase = format!("cite-sources-{}", Uuid::new_v4().simple());
    run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "add",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--skill-id",
            "live-skill",
            "--scope",
            "principal",
            "--name",
            &catalog_phrase,
            "--description",
            "find and cite sources",
            "--instructions",
            "Always cite your sources.",
        ],
    )?;
    wait_for_runtime_quiescence(&graphql, &agent_did, gen0 + 1, Duration::from_secs(2)).await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _: codex::InitializeResponse = read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadStart {
            request_id: request_id(2),
            params: codex::ThreadStartParams {
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let thread_start: codex::ThreadStartResponse =
        read_typed_response(&mut ws, request_id(2)).await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(3),
            params: codex::TurnStartParams {
                thread_id: thread_start.thread.id.clone(),
                input: vec![codex::UserInput::Text {
                    text: "hello".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let _: codex::TurnStartResponse = read_typed_response(&mut ws, request_id(3)).await?;
    let (_text, completed) = read_turn_to_completion(&mut ws).await?;
    assert_eq!(completed.status, codex::TurnStatus::Completed);

    let captured = mock_endpoint.captured_chat_requests();
    assert!(
        captured.iter().any(|request| {
            let text = request.to_string();
            text.contains(&catalog_phrase) && text.contains("load_skill")
        }),
        "live-added skill's catalog entry did not reach the model; captured={captured:?}"
    );

    let _ = ws.close(None).await;
    Ok(())
}

/// End-to-end proof that an EXPLICIT Codex skill selection (`UserInput::Skill`,
/// the skill "pill") deterministically activates the skill (#340). The shim
/// forwards only the id; the RUNTIME resolves it against the behavior's
/// effective set and injects the body as a per-turn system reminder (rather than
/// relying on the model to pull it). A skill-only turn (no text) must (a) not be
/// rejected as empty and (b) carry the skill body to the model.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_explicit_skill_selection_injects_body_into_turn() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let expected_reply = format!("skill-inject-ok-{}", Uuid::new_v4().simple());
    let model_name = format!("mock-skill-inject-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, &expected_reply)?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-skill-inject-{}", Uuid::new_v4().simple());
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

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    let gen0 = wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    let body_phrase = format!("INJECTED-BODY-{}", Uuid::new_v4().simple());
    run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "add",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--skill-id",
            "inject-skill",
            "--scope",
            "principal",
            "--name",
            "Injectable",
            "--description",
            "a skill to inject",
            "--instructions",
            &body_phrase,
        ],
    )?;
    wait_for_runtime_quiescence(&graphql, &agent_did, gen0 + 1, Duration::from_secs(2)).await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _: codex::InitializeResponse = read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadStart {
            request_id: request_id(2),
            params: codex::ThreadStartParams {
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let thread_start: codex::ThreadStartResponse =
        read_typed_response(&mut ws, request_id(2)).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(3),
            params: codex::TurnStartParams {
                thread_id: thread_start.thread.id.clone(),
                input: vec![codex::UserInput::Skill {
                    name: "Injectable".to_string(),
                    path: std::path::PathBuf::from("/gents/skills/inject-skill"),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let _: codex::TurnStartResponse = read_typed_response(&mut ws, request_id(3)).await?;
    let (_text, completed) = read_turn_to_completion(&mut ws).await?;
    assert_eq!(completed.status, codex::TurnStatus::Completed);

    let captured = mock_endpoint.captured_chat_requests();
    assert!(
        captured.iter().any(|request| {
            let text = request.to_string();
            text.contains(&body_phrase) && text.contains("system-reminder")
        }),
        "explicit skill selection did not inject the body; captured={captured:?}"
    );

    let _ = ws.close(None).await;
    Ok(())
}

/// An explicit Codex skill selection must still respect the bound behavior's
/// effective set (D5): a behavior-scoped skill NOT opted into the bound behavior
/// (empty skill_refs) cannot be force-activated via the pill (#340). Privilege
/// scoping — the Codex UI lists all the agent's skills, but selecting one the
/// behavior didn't opt into must not inject it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_explicit_selection_respects_effective_set() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let expected_reply = format!("scope-ok-{}", Uuid::new_v4().simple());
    let model_name = format!("mock-skill-scope-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, &expected_reply)?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-skill-scope-{}", Uuid::new_v4().simple());
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

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    let body_phrase = format!("UNSCOPED-BODY-{}", Uuid::new_v4().simple());
    run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "add",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--skill-id",
            "unscoped-skill",
            "--scope",
            "behavior",
            "--name",
            "Unscoped",
            "--description",
            "not opted in",
            "--instructions",
            &body_phrase,
        ],
    )?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _: codex::InitializeResponse = read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadStart {
            request_id: request_id(2),
            params: codex::ThreadStartParams {
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let thread_start: codex::ThreadStartResponse =
        read_typed_response(&mut ws, request_id(2)).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(3),
            params: codex::TurnStartParams {
                thread_id: thread_start.thread.id.clone(),
                input: vec![
                    codex::UserInput::Text {
                        text: "hello".to_string(),
                        text_elements: Vec::new(),
                    },
                    codex::UserInput::Skill {
                        name: "Unscoped".to_string(),
                        path: std::path::PathBuf::from("/gents/skills/unscoped-skill"),
                    },
                ],
                ..Default::default()
            },
        },
    )
    .await?;
    let _: codex::TurnStartResponse = read_typed_response(&mut ws, request_id(3)).await?;
    let (_text, completed) = read_turn_to_completion(&mut ws).await?;
    assert_eq!(completed.status, codex::TurnStatus::Completed);

    let captured = mock_endpoint.captured_chat_requests();
    assert!(
        captured
            .iter()
            .all(|request| !request.to_string().contains(&body_phrase)),
        "a behavior-scoped skill not in the effective set must not be injected; captured={captured:?}"
    );

    let _ = ws.close(None).await;
    Ok(())
}

/// End-to-end proof that a Codex-shim-driven `skills/config/write` disable
/// reconciles a RUNNING agent without a restart (#340): the shim commits the
/// toggle in a transaction, the COMMIT wakes the control watcher, the runtime
/// fingerprint changes (skills now contribute to `AgentBehavior`'s Debug), the
/// generation bumps, and the disabled skill's catalog entry stops reaching the
/// model on the next turn. This closes the gap where the shim's enable/disable
/// used an auto-committed mutation (no `Update` event) and only took effect on
/// restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_live_skill_toggle_reaches_model_in_conversation() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let expected_reply = format!("skill-toggle-ok-{}", Uuid::new_v4().simple());
    let model_name = format!("mock-skill-toggle-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, &expected_reply)?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-skill-toggle-{}", Uuid::new_v4().simple());
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

    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-poll-ms",
            "100",
            "--codex-shim-timeout-secs",
            "60",
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    wait_for_port(shim_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    let gen0 = wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    let catalog_phrase = format!("toggle-cite-{}", Uuid::new_v4().simple());
    run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "add",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--skill-id",
            "toggle-skill",
            "--scope",
            "principal",
            "--name",
            &catalog_phrase,
            "--description",
            "find and cite sources",
            "--instructions",
            "Always cite your sources.",
        ],
    )?;
    let gen1 =
        wait_for_runtime_quiescence(&graphql, &agent_did, gen0 + 1, Duration::from_secs(2)).await?;

    let (mut ws, _) = serve
        .capturing(async {
            connect_async(format!("ws://127.0.0.1:{shim_port}/"))
                .await
                .context("connecting to codex-shim websocket")
        })
        .await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _: codex::InitializeResponse = read_typed_response(&mut ws, request_id(1)).await?;
    send_client_notification(&mut ws, codex::ClientNotification::Initialized).await?;
    send_client_request(
        &mut ws,
        codex::ClientRequest::ThreadStart {
            request_id: request_id(2),
            params: codex::ThreadStartParams {
                cwd: Some(home_dir.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let thread_start: codex::ThreadStartResponse =
        read_typed_response(&mut ws, request_id(2)).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(3),
            params: codex::TurnStartParams {
                thread_id: thread_start.thread.id.clone(),
                input: vec![codex::UserInput::Text {
                    text: "hello".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let _: codex::TurnStartResponse = read_typed_response(&mut ws, request_id(3)).await?;
    let (_text, completed) = read_turn_to_completion(&mut ws).await?;
    assert_eq!(completed.status, codex::TurnStatus::Completed);
    assert!(
        mock_endpoint
            .captured_chat_requests()
            .iter()
            .any(|request| {
                let text = request.to_string();
                text.contains(&catalog_phrase) && text.contains("load_skill")
            }),
        "enabled skill's catalog entry should reach the model before the disable"
    );

    let captured_before_toggle = mock_endpoint.captured_chat_requests().len();
    send_client_request(
        &mut ws,
        codex::ClientRequest::SkillsConfigWrite {
            request_id: request_id(4),
            params: codex::SkillsConfigWriteParams {
                path: None,
                name: Some(catalog_phrase.clone()),
                enabled: false,
            },
        },
    )
    .await?;
    let write: codex::SkillsConfigWriteResponse =
        read_typed_response(&mut ws, request_id(4)).await?;
    assert!(
        !write.effective_enabled,
        "shim should report the skill disabled"
    );

    wait_for_runtime_quiescence(&graphql, &agent_did, gen1 + 1, Duration::from_secs(2)).await?;

    send_client_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: request_id(5),
            params: codex::TurnStartParams {
                thread_id: thread_start.thread.id.clone(),
                input: vec![codex::UserInput::Text {
                    text: "hello again".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let _: codex::TurnStartResponse = read_typed_response(&mut ws, request_id(5)).await?;
    let (_text, completed) = read_turn_to_completion(&mut ws).await?;
    assert_eq!(completed.status, codex::TurnStatus::Completed);

    let captured = mock_endpoint.captured_chat_requests();
    assert!(
        captured.len() > captured_before_toggle,
        "turn 2 should have produced at least one new captured request"
    );
    assert!(
        captured[captured_before_toggle..]
            .iter()
            .all(|request| !request.to_string().contains(&catalog_phrase)),
        "disabled skill's catalog entry must NOT reach the model after the shim toggle reconciled; \
         captured tail={:?}",
        &captured[captured_before_toggle..]
    );

    let _ = ws.close(None).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_skill_cli_disable_enable_and_rm_round_trip() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-skill-crud-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "ok")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-skill-crud-{}", Uuid::new_v4().simple());
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

    let mut serve = spawn_server_with_env(&home_dir, server_port, &[], &[])?;
    wait_for_port(server_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "add",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--skill-id",
            "research",
            "--scope",
            "principal",
            "--name",
            "Research",
            "--description",
            "Find and cite sources",
            "--instructions",
            "Always cite your sources.",
            "--tool-ref",
            "web_search",
        ],
    )?;

    let show = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "show",
            "--graphql",
            &graphql,
            "--skill-id",
            "research",
        ],
    )?;
    assert_eq!(show.get("enabled").and_then(Value::as_bool), Some(true));
    assert_eq!(
        show.get("tool_refs")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1),
        "tool_ref should be stored on add"
    );

    run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "add",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--skill-id",
            "research",
            "--scope",
            "principal",
            "--name",
            "Research",
            "--description",
            "Find and cite sources",
            "--instructions",
            "Always cite your sources.",
        ],
    )?;
    let show = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "show",
            "--graphql",
            &graphql,
            "--skill-id",
            "research",
        ],
    )?;
    let tool_refs_empty = match show.get("tool_refs") {
        None | Some(Value::Null) => true,
        Some(Value::Array(items)) => items.is_empty(),
        _ => false,
    };
    assert!(
        tool_refs_empty,
        "re-add without --tool-ref must clear tool_refs; got {:?}",
        show.get("tool_refs")
    );

    let disabled = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "disable",
            "--graphql",
            &graphql,
            "--skill-id",
            "research",
        ],
    )?;
    assert_eq!(disabled.get("updated").and_then(Value::as_u64), Some(1));
    assert_eq!(
        disabled.get("enabled").and_then(Value::as_bool),
        Some(false)
    );
    let show = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "show",
            "--graphql",
            &graphql,
            "--skill-id",
            "research",
        ],
    )?;
    assert_eq!(show.get("enabled").and_then(Value::as_bool), Some(false));

    let enabled = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "enable",
            "--graphql",
            &graphql,
            "--skill-id",
            "research",
        ],
    )?;
    assert_eq!(enabled.get("enabled").and_then(Value::as_bool), Some(true));

    let removed = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "rm",
            "--graphql",
            &graphql,
            "--skill-id",
            "research",
        ],
    )?;
    assert_eq!(removed.get("deleted").and_then(Value::as_u64), Some(1));
    let list = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "list",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
        ],
    )?;
    assert_eq!(list.get("count").and_then(Value::as_u64), Some(0));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "external fixture: set HERMES_SKILLS_DIR and pass --ignored"]
async fn config_skill_import_export_roundtrip_hermes() -> Result<()> {
    let hermes_dir = std::env::var("HERMES_SKILLS_DIR").context(
        "set HERMES_SKILLS_DIR to the NousResearch/hermes-agent skills directory and pass --ignored",
    )?;
    anyhow::ensure!(
        std::path::Path::new(&hermes_dir).is_dir(),
        "HERMES_SKILLS_DIR must point to an existing Hermes skills directory: {hermes_dir}"
    );

    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    fs::create_dir_all(&home_dir)?;
    let model_name = format!("mock-skill-import-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "ok")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-skill-import-{}", Uuid::new_v4().simple());
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

    let mut serve = spawn_server_with_env(&home_dir, server_port, &[], &[])?;
    wait_for_port(server_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    let imported = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "import",
            &hermes_dir,
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--scope",
            "behavior",
        ],
    )?;
    let imported_count = imported
        .get("imported_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    assert!(
        imported_count >= 50,
        "expected to import many hermes skills, got {imported_count}: {imported}"
    );

    let listed = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "list",
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
        ],
    )?;
    let listed_count = listed.get("count").and_then(Value::as_u64).unwrap_or(0);
    assert!(
        listed_count >= 50 && listed_count <= imported_count,
        "list count {listed_count}"
    );

    let out_dir = tempdir.path().join("export");
    let exported = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "export",
            out_dir.to_str().unwrap(),
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
        ],
    )?;
    let exported_count = exported
        .get("exported_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    assert_eq!(
        exported_count, listed_count,
        "export count must match distinct skills"
    );
    assert!(
        out_dir.join("notion").join("SKILL.md").is_file(),
        "exported notion/SKILL.md should exist"
    );

    let reimported = run_cli_json(
        &home_dir,
        &[
            "config",
            "skill",
            "import",
            out_dir.to_str().unwrap(),
            "--graphql",
            &graphql,
            "--agent-did",
            &agent_did,
            "--scope",
            "behavior",
        ],
    )?;
    let reimported_count = reimported
        .get("imported_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    assert_eq!(
        reimported_count, exported_count,
        "re-import of export must round-trip"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_shim_binds_when_config_apply_supplies_its_behavior() -> Result<()> {
    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let home_dir = tempdir.path().join("home");
    let root = tempdir.path().join("infra").join("agents").join("default");
    fs::create_dir_all(&home_dir)?;

    let model_name = format!("mock-codex-shim-model-{}", Uuid::new_v4().simple());
    let mock_endpoint = MockChatEndpoint::start(&model_name, "irrelevant")?;
    let server_port = allocate_port()?;
    let graphql = graphql_url(server_port);
    let agent_name = format!("cli-codex-shim-{}", Uuid::new_v4().simple());

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

    let root_str = root.to_str().expect("utf-8 root");
    run_cli_text(&home_dir, &["config", "export", "--root", root_str])?;

    const LATE_BEHAVIOR: &str = "late-arriving-behavior";
    let shim_port = allocate_port()?;
    let shim_port_string = shim_port.to_string();
    let mut serve = spawn_server_with_env(
        &home_dir,
        server_port,
        &[
            "--codex-shim",
            "--codex-shim-port",
            &shim_port_string,
            "--codex-shim-behavior-id",
            LATE_BEHAVIOR,
        ],
        &[],
    )?;
    wait_for_port(server_port, &mut serve)?;
    serve
        .capturing(wait_for_runtime_ready(
            &graphql,
            &agent_did,
            Duration::from_secs(30),
        ))
        .await?;
    wait_for_runtime_state_graphql(&home_dir, &graphql, Duration::from_secs(30)).await?;
    wait_for_runtime_quiescence(&graphql, &agent_did, 1, Duration::from_secs(2)).await?;

    assert!(
        std::net::TcpStream::connect(("127.0.0.1", shim_port)).is_err(),
        "the shim must not listen before its bound behavior exists"
    );

    let behaviors_dir = root.join("agent-behaviors");
    let existing = fs::read_dir(&behaviors_dir)
        .context("reading agent-behaviors dir after export")?
        .next()
        .ok_or_else(|| anyhow!("no agent-behavior subdirs after export"))??;
    let late_dir = behaviors_dir.join(LATE_BEHAVIOR);
    fs::create_dir_all(&late_dir)?;
    for entry in fs::read_dir(existing.path()).context("reading exported behavior dir")? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::copy(entry.path(), late_dir.join(entry.file_name()))?;
        }
    }
    let mut behavior = read_json_file(&late_dir.join("object.json"))?;
    behavior["behavior_id"] = Value::String(LATE_BEHAVIOR.to_string());
    write_json_file(&late_dir.join("object.json"), &behavior)?;

    let applied = run_cli_json(&home_dir, &["config", "apply", "--root", root_str])?;
    assert_eq!(
        applied.get("ok").and_then(Value::as_bool),
        Some(true),
        "config apply must succeed: {applied}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", shim_port)).is_ok() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let (_stdout, stderr) = serve.captured_output()?;
            panic!(
                "the shim never bound after `config apply` supplied behavior {LATE_BEHAVIOR:?} \
                 — this is #699: the port stays closed until the process restarts.\nstderr:\n{stderr}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let (_stdout, stderr) = serve.captured_output()?;
    assert!(
        stderr.contains("Codex endpoint bound"),
        "the operator must see the shim converge; got:\n{stderr}"
    );
    assert!(
        stderr.contains("no restart was needed"),
        "the fix is that no restart is needed; got:\n{stderr}"
    );
    Ok(())
}
