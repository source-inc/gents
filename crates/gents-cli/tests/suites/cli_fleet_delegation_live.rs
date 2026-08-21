//! Multi-process fleet e2e for filtered conversation pairing and
//! cross-deployment live subagent delegation, driven entirely by the daemon
//! reconcilers (no direct REST replicator install). Each coordinator<->subagent
//! edge is an independent two-node reconcile with two documents: (1) a v5
//! network-control join (P2P mesh + control-plane document gossip) and (2) an
//! operator conversation `DataPlanePairingDesired` row (the doc sync delegation
//! needs). Convergence is layer-specific: control proves `AgentNetwork` in the
//! Replicate subscription collections, while conversation proves the local DID
//! in the Push replicator's `AgentRequest` filter. See `establish_control_plane`
//! and `establish_conversation_data_plane`.
//!
//! Requires the defradb iroh fixes in sourcenetwork/defradb.rs#1045 (addr
//! hygiene + observed-addr reverse-dial fallback + spawning the dial off the
//! command loop). The load-bearing one is the spawn: defradb's iroh command
//! loop awaited the blocking `endpoint.connect()` inline, starving `accept()`,
//! so two peers dialing each other in-window deadlocked — the #511 wall. With
//! #1045 this converges reliably (2-node 8/8, 5-node substrate 5/5, full
//! delegation 5/5). Those fixes are now in the pinned defradb rev, so this runs
//! against the workspace pin directly. The convergence checkpoint still dumps
//! doc-state + full daemon logs on timeout (`dump_fleet_doc_state` /
//! `persist_fleet_logs`) for future triage.
//!
//! Normal test runs compile this file but skip the live test. To run:
//!
//! ```bash
//! GENTS_LIVE_OPENAI=1 \
//! GENTS_LIVE_OPENAI_ENDPOINT="http://host:8000/v1" \
//! GENTS_LIVE_OPENAI_MODEL="model-name" \
//!   cargo test -p gents-cli --test cli_fleet_delegation_live -- --ignored --nocapture
//! ```
//!
//! The release acceptance composes the same primitives into a 19-fresh-store
//! genesis mesh, a coordinator restart, and a 15-target delegation/readback
//! sweep. Its control-plane fence reads DefraDB's effective replicator scope.
//! It is intentionally a product E2E across both model instruction
//! following and the mesh; the hermetic two-process demo test isolates the pure
//! transport/materialization contract. It is separately gated because it is
//! intentionally expensive:
//!
//! ```bash
//! GENTS_RELEASE_ACCEPTANCE=1 \
//! GENTS_LIVE_OPENAI_ENDPOINT="http://host:8000/v1" \
//! GENTS_LIVE_OPENAI_MODEL="model-name" \
//!   cargo test -p gents-cli --test cli_fleet_delegation_live \
//!     nineteen_process_release_acceptance_live -- --ignored --nocapture
//! ```
//!
//! The fresh-store pending-DAG failure and release exception were tracked in
//! #798.

use crate::support::*;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use gents::{
    subagent_target_entry, JsonP2pSyncStatusAdapter, P2pSyncStatusAdapter, P2pSyncStatusSnapshot,
};
use gents_codex_protocol as codex;
use gents_protocol::message::{
    AssistantContent, Message as ProtocolMessage, ToolResultContent, UserContent,
};
use gents_protocol::transcript::decode_persisted_message;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

type FleetShimWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn require_live_gate(gate: &str) -> Result<()> {
    anyhow::ensure!(
        std::env::var(gate).as_deref() == Ok("1"),
        "set {gate}=1 and pass --ignored to run this fleet live qualification"
    );
    Ok(())
}

const P2P_LOOPBACK_ARGS: &[&str] = &[
    "--p2p-bind-addr",
    "127.0.0.1",
    "--p2p-port",
    "0",
    "--p2p-relay-mode",
    "disabled",
    "--p2p-discovery",
    "disabled",
];

const FAST_RECONCILE_ENVS: &[(&str, &str)] = &[
    ("GENTS_REGISTRY_HEARTBEAT_MS", "1000"),
    ("GENTS_PAIRING_SWEEP_MS", "1000"),
    ("GENTS_REGISTRY_STALE_MS", "300000"),
    ("GENTS_ENDPOINT_HEARTBEAT_MS", "1000"),
    (
        "RUST_LOG",
        "warn,gents::agent::p2p_reconcile=debug,gents::graphql=debug",
    ),
];

// Cover DefraDB's 30s, 1m, 2m durable retry ladder plus three quiet samples.
const RELEASE_P2P_QUIET_TIMEOUT: Duration = Duration::from_secs(240);

const CONVERSATION_COLLECTIONS: &[&str] = &[
    "AgentRequest",
    "AgentResponse",
    "AgentMessage",
    "AgentToolCall",
    "AgentToolResult",
    "AgentSession",
    "AgentConversation",
    "CompactionEntry",
];

const COORDINATOR_SYSTEM_PROMPT: &str = r#"You are a fleet coordinator. You have four remote research subagents named researcher-1, researcher-2, researcher-3, and researcher-4. For any user request asking you to use the fleet, call the spawn_subagent tool exactly once for each of those four researcher names, and each call must set await_mode to "background". Do not use foreground. Do not call spawn_subagent more than four total times. Do not call any other tool. After the four background calls are issued, reply briefly that all four researchers were delegated."#;

const SUBAGENT_SYSTEM_PROMPT: &str = r#"You are a remote research subagent. Answer the assigned question directly in at least five factual paragraphs totaling roughly 500 words. Do not delegate to other subagents. The detail is intentional: this live test observes the response while it is streaming across deployments."#;

const RELEASE_FLEET_SIZE: usize = 19;
const RELEASE_DELEGATE_COUNT: usize = 15;
const RELEASE_MAX_RETAINED_BACKGROUND_TASKS: usize = 128;
const RELEASE_BAD_LOG_SIGNATURES: &[&str] = &[
    "Dropping GossipSub message outside accepted replication direction",
    "Collection-commit push failed; document retry ledger cannot replay CID-scoped work",
    "CAR handler: no exact blocks",
    "skipping unparseable block in replicator push",
];

const MAX_CONTROL_LEASE_WRITES_PER_NODE: usize = 16;

struct FleetNode {
    home: PathBuf,
    graphql: String,
    agent_did: String,
    peer_id: String,
    #[allow(dead_code)]
    address: String,
    shareable: String,
    behavior_id: String,
    tool_selection_id: String,
    backend_id: String,
    inference_profile_id: String,
    model_name: String,
    codex_shim_port: Option<u16>,
    archived_logs: Vec<FleetLogCapture>,
    #[allow(dead_code)]
    serve: ServeProcess,
}

#[derive(Debug, Clone)]
struct FleetLogCapture {
    phase: String,
    stdout: String,
    stderr: String,
}

#[derive(Debug)]
struct FatalFleetInvariant(String);

impl std::fmt::Display for FatalFleetInvariant {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for FatalFleetInvariant {}

fn fatal_fleet_invariant(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(FatalFleetInvariant(message.into()))
}

#[derive(Debug, Clone)]
struct BridgeRow {
    tool_call_id: String,
    lifecycle_state: String,
    child_request_id: String,
    await_mode: Option<String>,
    target_name: String,
}

#[derive(Debug, Clone)]
struct ChildRow {
    request_id: String,
    session_id: String,
    agent_did: String,
    behavior_id: String,
    lifecycle_state: Option<String>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
    caused_by_trigger_kind: Option<String>,
}

#[derive(Debug, Clone)]
struct CompletedChild {
    tool_call_id: String,
    child_request_id: String,
    child_session_id: String,
    owner_agent_did: String,
    owner_behavior_id: String,
    owner_answer: String,
    coordinator_answer: String,
}

#[derive(Debug, Clone)]
struct TranscriptToolExchange {
    id: String,
    call_id: Option<String>,
    name: String,
    args: Value,
    result: Option<String>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live: set GENTS_LIVE_OPENAI=1 and pass --ignored; currently blocked on defra.rs #1033 filtered replication (replicator_filter installs null) — see #1147"]
async fn five_process_filtered_conversation_delegation_live() -> Result<()> {
    require_live_gate("GENTS_LIVE_OPENAI")?;

    let endpoint = std::env::var("GENTS_LIVE_OPENAI_ENDPOINT")
        .or_else(|_| std::env::var("GENTS_CLI_E2E_MODEL_ENDPOINT"))
        .unwrap_or_else(|_| DEFAULT_MODEL_ENDPOINT.to_string());
    let model = std::env::var("GENTS_LIVE_OPENAI_MODEL")
        .or_else(|_| std::env::var("GENTS_CLI_E2E_MODEL_NAME"))
        .unwrap_or_else(|_| DEFAULT_MODEL_NAME.to_string());
    assert_endpoint_reachable(&endpoint).await?;

    let fleet_size: usize = std::env::var("GENTS_FLEET_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let substrate_only = std::env::var("GENTS_FLEET_SUBSTRATE_ONLY").as_deref() == Ok("1");

    let tempdir = tempfile::tempdir().context("creating tempdir")?;
    let fleet = bring_up_fleet(tempdir.path(), fleet_size, &endpoint, &model, true).await?;
    let (coord, subagents) = fleet
        .split_first()
        .ok_or_else(|| anyhow!("fleet should contain a coordinator"))?;

    establish_reconciler_pairing(coord, subagents).await?;

    if let Err(error) = wait_for_fleet_pairing(coord, subagents).await {
        dump_fleet_doc_state(&fleet).await;
        persist_fleet_logs(&fleet, "fail");
        dump_fleet_logs(&fleet);
        return Err(error);
    }
    assert_no_subagent_data_plane_edges(subagents).await?;

    if substrate_only {
        persist_fleet_logs(&fleet, "pass");
        tracing::info!(
            fleet_size,
            "reconciler-driven pairing converged on all edges; \
             GENTS_FLEET_SUBSTRATE_ONLY=1 set, skipping delegation"
        );
        drop(fleet);
        return Ok(());
    }

    configure_fleet_behaviors(tempdir.path(), coord, subagents).await?;
    wait_for_runtime_quiescence(&coord.graphql, &coord.agent_did, 2, Duration::from_secs(6))
        .await?;
    for subagent in subagents {
        wait_for_runtime_quiescence(
            &subagent.graphql,
            &subagent.agent_did,
            2,
            Duration::from_secs(6),
        )
        .await?;
    }

    let parent_prompt = "Use all four research subagents in parallel with background spawns only. Ask researcher-1 for a detailed five-paragraph report about Mercury, researcher-2 for a detailed five-paragraph report about Venus, researcher-3 for a detailed five-paragraph report about Earth, and researcher-4 for a detailed five-paragraph report about Mars. Make exactly four spawn_subagent calls total, one per researcher, then stop and reply that all four background researchers were delegated.";
    let shim_port = coord
        .codex_shim_port
        .context("coordinator must expose the Codex shim")?;
    let mut parent_ws = fleet_connect_and_initialize_codex(shim_port).await?;
    let parent_session_id = fleet_start_codex_thread(&mut parent_ws, &coord.home).await?;
    let parent_request_id =
        fleet_start_codex_turn(&mut parent_ws, &parent_session_id, parent_prompt).await?;

    let observation = tokio::try_join!(
        fleet_capture_parent_turn(&mut parent_ws),
        fleet_observe_live_child(shim_port, &parent_session_id),
    );
    let (parent_capture, live_child) = match observation {
        Ok(observation) => observation,
        Err(error) => {
            dump_fleet_doc_state(&fleet).await;
            persist_fleet_logs(&fleet, "delegation-fail");
            dump_fleet_logs(&fleet);
            return Err(error);
        }
    };
    assert_eq!(parent_capture.turn.status, codex::TurnStatus::Completed);

    let completed_children = wait_for_all_subagent_children_completed(
        &coord.graphql,
        subagents,
        &parent_session_id,
        &parent_request_id,
        None,
        Duration::from_secs(300),
    )
    .await?;

    let expected_child_threads = completed_children
        .values()
        .map(|child| child.child_session_id.clone())
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        expected_child_threads.contains(&live_child.thread_id),
        "live Codex child {} was not one of the runtime-spawned children: {expected_child_threads:?}",
        live_child.thread_id
    );
    anyhow::ensure!(
        !live_child.delta.trim().is_empty(),
        "loaded child {} emitted an empty live delta",
        live_child.thread_id
    );
    assert_fleet_parent_collab_projection(&parent_capture, &expected_child_threads)?;
    assert_fleet_completed_collab_history(shim_port, &parent_session_id, &expected_child_threads)
        .await?;
    assert_fleet_child_thread_is_read_only(shim_port, &live_child.thread_id).await?;

    let parent_terminal =
        wait_for_request_terminal(&coord.graphql, &parent_request_id, Duration::from_secs(240))
            .await?;
    assert_eq!(
        parent_terminal, "completed",
        "parent request must complete successfully"
    );
    let parent_answer =
        wait_for_assistant_answer(&coord.graphql, &parent_request_id, Duration::from_secs(60))
            .await?;
    anyhow::ensure!(
        !parent_answer.trim().is_empty(),
        "parent request completed with an empty response"
    );

    assert_subagent_store_scopes(coord, subagents, &completed_children).await?;
    assert_subagents_have_no_spawn_targets(subagents).await?;

    drop(fleet);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "live: set GENTS_RELEASE_ACCEPTANCE=1 and pass --ignored"]
async fn nineteen_process_release_acceptance_live() -> Result<()> {
    require_live_gate("GENTS_RELEASE_ACCEPTANCE")?;

    let control_only = std::env::var("GENTS_RELEASE_CONTROL_ONLY").as_deref() == Ok("1");
    let control_model_name = format!("release-control-only-{}", Uuid::new_v4().simple());
    let control_model = control_only
        .then(|| MockModelEndpoint::start(&control_model_name))
        .transpose()?;
    let endpoint = control_model
        .as_ref()
        .map(|mock| mock.endpoint().to_string())
        .unwrap_or_else(|| {
            std::env::var("GENTS_LIVE_OPENAI_ENDPOINT")
                .or_else(|_| std::env::var("GENTS_CLI_E2E_MODEL_ENDPOINT"))
                .unwrap_or_else(|_| DEFAULT_MODEL_ENDPOINT.to_string())
        });
    let model = control_model
        .as_ref()
        .map(|_| control_model_name)
        .unwrap_or_else(|| {
            std::env::var("GENTS_LIVE_OPENAI_MODEL")
                .or_else(|_| std::env::var("GENTS_CLI_E2E_MODEL_NAME"))
                .unwrap_or_else(|_| DEFAULT_MODEL_NAME.to_string())
        });
    assert_endpoint_reachable(&endpoint).await?;

    let tempdir = tempfile::tempdir().context("creating release acceptance tempdir")?;
    let mut fleet =
        bring_up_fleet(tempdir.path(), RELEASE_FLEET_SIZE, &endpoint, &model, false).await?;

    let result = run_release_acceptance(tempdir.path(), &mut fleet, control_only).await;
    if result.is_err() {
        dump_fleet_doc_state(&fleet).await;
        persist_fleet_logs(&fleet, "release-acceptance-fail");
        dump_fleet_logs(&fleet);
    }
    if result.is_err() && std::env::var("GENTS_RELEASE_PRESERVE_STORES").as_deref() == Ok("1") {
        let path = tempdir.keep();
        tracing::warn!(path = %path.display(), "preserved failed release-acceptance stores");
    }
    result
}

async fn run_release_acceptance(
    root: &Path,
    fleet: &mut [FleetNode],
    control_only: bool,
) -> Result<()> {
    anyhow::ensure!(
        fleet.len() == RELEASE_FLEET_SIZE,
        "release acceptance requires exactly {RELEASE_FLEET_SIZE} fresh stores"
    );

    {
        let (coord, spokes) = fleet
            .split_first()
            .context("release fleet requires a coordinator")?;
        establish_control_plane(coord, spokes).await?;
        wait_for_fleet_control_plane(coord, spokes).await?;
    }
    wait_for_p2p_fleet_quiet(fleet, RELEASE_P2P_QUIET_TIMEOUT).await?;
    assert_no_fleet_log_signatures(fleet)?;
    assert_bounded_control_lease_writes(fleet)?;

    restart_fleet_node(&mut fleet[0]).await?;
    {
        let (coord, spokes) = fleet
            .split_first()
            .context("release fleet requires a coordinator")?;
        wait_for_fleet_control_plane(coord, spokes).await?;
        wait_for_fleet_hub_remesh(coord, spokes, Duration::from_secs(120)).await?;
    }
    wait_for_p2p_fleet_quiet(fleet, RELEASE_P2P_QUIET_TIMEOUT).await?;
    assert_no_fleet_log_signatures(fleet)?;
    assert_bounded_control_lease_writes(fleet)?;

    if control_only {
        tracing::info!(
            fleet_size = fleet.len(),
            "release control mesh converged across coordinator restart before model/delegation phases"
        );
        return Ok(());
    }

    let (coord, spokes) = fleet
        .split_first()
        .context("release fleet requires a coordinator")?;
    let delegates = spokes
        .get(..RELEASE_DELEGATE_COUNT)
        .context("release fleet does not contain all 15 delegates")?;

    establish_conversation_data_plane(coord, spokes).await?;
    wait_for_fleet_pairing(coord, spokes).await?;
    assert_no_subagent_data_plane_edges(spokes).await?;

    let coordinator_prompt = release_coordinator_system_prompt(RELEASE_DELEGATE_COUNT);
    configure_fleet_behaviors_with_coordinator_prompt(
        root,
        coord,
        delegates,
        &coordinator_prompt,
        true,
    )
    .await?;
    futures_util::future::try_join_all(std::iter::once(coord).chain(delegates.iter()).map(
        |node| {
            wait_for_runtime_quiescence(&node.graphql, &node.agent_did, 2, Duration::from_secs(6))
        },
    ))
    .await?;

    let submit = run_cli_json(
        &coord.home,
        &[
            "request",
            "submit",
            "--graphql",
            &coord.graphql,
            "--agent-did",
            &coord.agent_did,
            "--behavior-id",
            &coord.behavior_id,
            "--content",
            &release_user_prompt(RELEASE_DELEGATE_COUNT),
            "--no-wait",
        ],
    )?;
    let parent_request_id = required_output_string(&submit, "request_id")?;
    let parent_session_id = required_output_string(&submit, "session_id")?;

    let completed_children = wait_for_release_sweep(
        fleet,
        coord,
        delegates,
        &parent_session_id,
        &parent_request_id,
        Duration::from_secs(600),
    )
    .await?;
    assert_release_subagent_inspection(
        &coord.graphql,
        &parent_request_id,
        &parent_session_id,
        RELEASE_DELEGATE_COUNT,
    )
    .await?;

    let parent_answer =
        wait_for_assistant_answer(&coord.graphql, &parent_request_id, Duration::from_secs(60))
            .await?;
    anyhow::ensure!(
        !parent_answer.trim().is_empty(),
        "release sweep parent completed with an empty response"
    );
    assert_subagent_store_scopes(coord, delegates, &completed_children).await?;
    assert_subagents_have_no_spawn_targets(delegates).await?;
    assert_no_subagent_data_plane_edges(spokes).await?;

    wait_for_p2p_fleet_quiet(fleet, RELEASE_P2P_QUIET_TIMEOUT).await?;
    assert_no_fleet_log_signatures(fleet)?;
    Ok(())
}

fn release_coordinator_system_prompt(delegate_count: usize) -> String {
    let names = (1..=delegate_count)
        .map(|index| format!("researcher-{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "You are the release acceptance fleet coordinator. Your allowed remote targets are: {names}. When the user asks for the release sweep, follow this exact sequence: (1) call spawn_subagent exactly once for every allowed target, with await_mode=background; (2) after all {delegate_count} spawn calls return, call list_subagents exactly once with status=all and limit=50; (3) call wait_subagent for researcher-{delegate_count}'s returned child_request_id; if and only if wait_subagent returns retryable=true, retry wait_subagent on that same child until one call succeeds with status=completed; (4) after the successful wait, call read_subagent exactly once for researcher-1's returned child_request_id with include_user_messages=true; (5) reply briefly that the sweep completed. The wait and read calls must target those two different children. Do not omit a target, do not create duplicate spawns, do not call steer_subagent, and do not call any other tools."
    )
}

fn release_user_prompt(delegate_count: usize) -> String {
    format!(
        "Run the release sweep across researcher-1 through researcher-{delegate_count}. Ask each researcher for its detailed five-paragraph report on a distinct numbered fleet reliability scenario. Use background spawns and perform the required list, wait, and read checks before replying."
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct P2pProgressSignature {
    failed_pushes: u64,
    rejected_items: u64,
    rejected_bytes: u64,
    peer_capacity_parks: u64,
    missing_link_retries: u64,
    provider_rotations: u64,
    car_filtered_cids: u64,
    pending_dag_registered: u64,
    pending_dag_expired: u64,
    pending_dag_capacity_shed: u64,
    pending_dag_retry_dispatched: u64,
    pending_dag_fetch_deferred_contention: u64,
    pending_dag_fetch_exhausted: u64,
    pending_dag_terminal_merged: u64,
    non_authoritative_broadcast_rejected: u64,
}

impl From<&P2pSyncStatusSnapshot> for P2pProgressSignature {
    fn from(snapshot: &P2pSyncStatusSnapshot) -> Self {
        Self {
            failed_pushes: snapshot.push_backlog.failed_total,
            rejected_items: snapshot.push_backlog.rejected_items_total,
            rejected_bytes: snapshot.push_backlog.rejected_bytes_total,
            peer_capacity_parks: snapshot.push_backlog.peer_capacity_parks_total,
            missing_link_retries: snapshot.missing_link_retries,
            provider_rotations: snapshot.provider_rotations,
            car_filtered_cids: snapshot.car_filtered_cids,
            pending_dag_registered: snapshot.pending_dag_registered,
            pending_dag_expired: snapshot.pending_dag_expired,
            pending_dag_capacity_shed: snapshot.pending_dag_capacity_shed,
            pending_dag_retry_dispatched: snapshot.pending_dag_retry_dispatched,
            pending_dag_fetch_deferred_contention: snapshot.pending_dag_fetch_deferred_contention,
            pending_dag_fetch_exhausted: snapshot.pending_dag_fetch_exhausted,
            pending_dag_terminal_merged: snapshot.pending_dag_terminal_merged,
            non_authoritative_broadcast_rejected: snapshot
                .non_authoritative_broadcast_rejected_total,
        }
    }
}

static P2P_HTTP_CLIENT: OnceLock<std::result::Result<reqwest::Client, String>> = OnceLock::new();

fn p2p_http_client() -> Result<&'static reqwest::Client> {
    match P2P_HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|error| error.to_string())
    }) {
        Ok(client) => Ok(client),
        Err(error) => bail!("building shared P2P diagnostics client: {error}"),
    }
}

async fn fetch_p2p_sync_status(graphql: &str) -> Result<P2pSyncStatusSnapshot> {
    let api_base = graphql
        .strip_suffix("/graphql")
        .with_context(|| format!("unexpected GraphQL endpoint shape: {graphql}"))?;
    let url = format!("{api_base}/p2p/sync/status");
    let value = p2p_http_client()?
        .get(&url)
        .send()
        .await
        .with_context(|| format!("fetching P2P diagnostics from {url}"))?
        .error_for_status()
        .with_context(|| format!("P2P diagnostics returned an error from {url}"))?
        .json::<Value>()
        .await
        .with_context(|| format!("decoding P2P diagnostics from {url}"))?;
    JsonP2pSyncStatusAdapter
        .adapt(&value)
        .with_context(|| format!("adapting typed P2P diagnostics from {url}"))
}

async fn fetch_connected_peer_ids(graphql: &str) -> Result<HashSet<String>> {
    let api_base = graphql
        .strip_suffix("/graphql")
        .with_context(|| format!("unexpected GraphQL endpoint shape: {graphql}"))?;
    let url = format!("{api_base}/p2p/peers");
    let rows = p2p_http_client()?
        .get(&url)
        .send()
        .await
        .with_context(|| format!("fetching connected P2P peers from {url}"))?
        .error_for_status()
        .with_context(|| format!("P2P peer endpoint returned an error from {url}"))?
        .json::<Vec<Value>>()
        .await
        .with_context(|| format!("decoding connected P2P peers from {url}"))?;
    rows.into_iter()
        .map(|row| {
            row.get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.trim().is_empty())
                .map(|id| {
                    p2p::iroh::parse_public_peer_addr(id)
                        .map(|(peer_id, _)| peer_id.to_string())
                        .unwrap_or_else(|_| id.to_owned())
                })
                .with_context(|| format!("connected P2P peer row has no id: {row}"))
        })
        .collect()
}

async fn wait_for_fleet_hub_remesh(
    coord: &FleetNode,
    spokes: &[FleetNode],
    timeout: Duration,
) -> Result<()> {
    let expected = spokes
        .iter()
        .map(|node| node.peer_id.clone())
        .collect::<HashSet<_>>();
    wait_until_value(timeout, || async {
        let connected = fetch_connected_peer_ids(&coord.graphql).await?;
        anyhow::ensure!(
            connected == expected,
            "restarted hub has not live-remeshed with every spoke: connected={connected:?} expected={expected:?}"
        );
        Ok(())
    })
    .await
}

async fn fetch_p2p_fleet(fleet: &[FleetNode]) -> Result<Vec<P2pSyncStatusSnapshot>> {
    futures_util::future::try_join_all(
        fleet
            .iter()
            .map(|node| fetch_p2p_sync_status(&node.graphql)),
    )
    .await
}

fn assert_p2p_fleet_bounded(
    fleet: &[FleetNode],
    snapshots: &[P2pSyncStatusSnapshot],
) -> Result<()> {
    for (node, snapshot) in fleet.iter().zip(snapshots) {
        assert_p2p_snapshot_bounded(&node.agent_did, snapshot)?;
    }
    Ok(())
}

fn assert_p2p_snapshot_bounded(label: &str, snapshot: &P2pSyncStatusSnapshot) -> Result<()> {
    let backlog = &snapshot.push_backlog;
    anyhow::ensure!(
        backlog.queue_item_capacity > 0
            && backlog.queue_byte_capacity > 0
            && backlog.worker_count > 0
            && backlog.per_peer_active_cap > 0,
        "{label} reported an invalid zero P2P admission limit: {snapshot:?}"
    );
    anyhow::ensure!(
        backlog.queued_items <= backlog.queue_item_capacity,
        "{label} P2P item queue exceeded its cap: {snapshot:?}"
    );
    anyhow::ensure!(
        backlog.queued_bytes <= backlog.queue_byte_capacity,
        "{label} P2P byte queue exceeded its cap: {snapshot:?}"
    );
    anyhow::ensure!(
        backlog.active_jobs <= backlog.worker_count,
        "{label} P2P active jobs exceeded worker count: {snapshot:?}"
    );
    anyhow::ensure!(
        snapshot.pending_dag_capacity > 0 && snapshot.pending_dags <= snapshot.pending_dag_capacity,
        "{label} pending-DAG occupancy exceeded its cap: {snapshot:?}"
    );
    anyhow::ensure!(
        snapshot.persisted_pending_dag_capacity > 0
            && snapshot.persisted_pending_dags <= snapshot.persisted_pending_dag_capacity,
        "{label} persisted pending-DAG occupancy exceeded its cap: {snapshot:?}"
    );
    anyhow::ensure!(
        snapshot.retained_background_tasks <= RELEASE_MAX_RETAINED_BACKGROUND_TASKS,
        "{label} retained {} P2P background tasks (release max {})",
        snapshot.retained_background_tasks,
        RELEASE_MAX_RETAINED_BACKGROUND_TASKS
    );
    anyhow::ensure!(
        snapshot.gossip_direction_filtered_total == 0,
        "{label} dropped gossip outside the accepted replication direction: {snapshot:?}"
    );
    anyhow::ensure!(
        snapshot.pending_dag_terminal_quarantined == 0 && snapshot.quarantined_pending_dags == 0,
        "{label} quarantined a deterministic pending-DAG failure: {snapshot:?}"
    );
    anyhow::ensure!(
        snapshot.pending_dag_capacity_shed == 0,
        "{label} shed a pending DAG at its admission capacity: {snapshot:?}"
    );
    anyhow::ensure!(
        snapshot.pending_dag_fetch_exhausted == 0,
        "{label} exhausted a bounded pending-DAG fetch: {snapshot:?}"
    );
    for peer in &backlog.per_peer {
        anyhow::ensure!(
            peer.active_jobs <= backlog.per_peer_active_cap,
            "{label} peer {} exceeded its active-job cap: {peer:?}",
            peer.peer_id
        );
        anyhow::ensure!(
            peer.queued_items <= backlog.queue_item_capacity
                && peer.queued_bytes <= backlog.queue_byte_capacity,
            "{label} peer {} exceeded a global queue cap: {peer:?}",
            peer.peer_id
        );
    }
    Ok(())
}

fn p2p_snapshot_is_quiet(snapshot: &P2pSyncStatusSnapshot) -> bool {
    let backlog = &snapshot.push_backlog;
    backlog.queued_items == 0
        && backlog.queued_bytes == 0
        && backlog.active_jobs == 0
        // Healed failures may remain cumulative; the progress fence must stabilize.
        && backlog.rejected_items_total == 0
        && backlog.rejected_bytes_total == 0
        && backlog.per_peer.iter().all(|peer| {
            peer.queued_items == 0
                && peer.queued_bytes == 0
                && peer.active_jobs == 0
                && peer.consecutive_failures == 0
                && peer.cooldown_remaining_ms == 0
        })
        && snapshot.push_retry_markers.document_markers == 0
        && snapshot.push_retry_markers.collection_markers == 0
        && snapshot.push_retry_markers.scheduled_peers == 0
        && snapshot
            .push_retry_markers
            .oldest_scheduled_retry_unix
            .is_none()
        && snapshot.pending_dags == 0
        && snapshot.persisted_pending_dags == 0
        && snapshot.non_authoritative_broadcast_tasks == 0
        && !snapshot.pending_resync_in_flight
        && snapshot.next_pending_retry_in_ms.is_none()
        && snapshot.quarantined_pending_dags == 0
}

async fn wait_for_p2p_fleet_quiet(fleet: &[FleetNode], timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut previous = None::<Vec<P2pProgressSignature>>;
    let mut stable_quiet_samples = 0usize;
    loop {
        let snapshots = match fetch_p2p_fleet(fleet).await {
            Ok(snapshots) => snapshots,
            Err(error) if Instant::now() < deadline => {
                tracing::warn!(
                    error = %error,
                    "transient P2P diagnostics fetch failed while waiting for fleet quiet; retrying"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(error) => {
                bail!(
                    "P2P diagnostics remained unavailable while waiting for fleet quiet: {error:#}"
                );
            }
        };
        assert_p2p_fleet_bounded(fleet, &snapshots)?;
        let signatures = snapshots
            .iter()
            .map(P2pProgressSignature::from)
            .collect::<Vec<_>>();
        let quiet = snapshots.iter().all(p2p_snapshot_is_quiet);
        stable_quiet_samples = if quiet && previous.as_ref() == Some(&signatures) {
            stable_quiet_samples + 1
        } else if quiet {
            1
        } else {
            0
        };
        previous = Some(signatures);
        if stable_quiet_samples >= 3 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let summary = fleet
                .iter()
                .zip(&snapshots)
                .map(|(node, snapshot)| {
                    format!(
                        "{}: queued={}/{} active={} pending={}/{} persisted={}/{} retained={} non_authoritative={} missing_retries={} provider_rotations={} car_filtered={} fetch_exhausted={} failed_pushes={} rejected_items={} rejected_bytes={}",
                        node.agent_did,
                        snapshot.push_backlog.queued_items,
                        snapshot.push_backlog.queued_bytes,
                        snapshot.push_backlog.active_jobs,
                        snapshot.pending_dags,
                        snapshot.pending_dag_capacity,
                        snapshot.persisted_pending_dags,
                        snapshot.persisted_pending_dag_capacity,
                        snapshot.retained_background_tasks,
                        snapshot.non_authoritative_broadcast_tasks,
                        snapshot.missing_link_retries,
                        snapshot.provider_rotations,
                        snapshot.car_filtered_cids,
                        snapshot.pending_dag_fetch_exhausted,
                        snapshot.push_backlog.failed_total,
                        snapshot.push_backlog.rejected_items_total,
                        snapshot.push_backlog.rejected_bytes_total,
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            bail!("P2P fleet did not reach three stable quiet samples: {summary}");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn assert_no_fleet_log_signatures(fleet: &[FleetNode]) -> Result<()> {
    for node in fleet {
        let (stdout, stderr) = fleet_node_captured_output(node)?;
        let combined = format!("{stdout}\n{stderr}");
        for signature in RELEASE_BAD_LOG_SIGNATURES {
            anyhow::ensure!(
                !combined.contains(signature),
                "fleet node {} emitted release-blocking P2P log signature {signature:?}",
                node.agent_did
            );
        }
    }
    Ok(())
}

fn assert_bounded_control_lease_writes(fleet: &[FleetNode]) -> Result<()> {
    let mut endpoint_writes = 0;
    let mut registry_writes = 0;
    for node in fleet {
        let (stdout, stderr) = fleet_node_captured_output(node)?;
        let combined = format!("{stdout}\n{stderr}");
        let node_endpoint_writes = combined
            .matches("PeerEndpoint heartbeat: signed endpoint written")
            .count();
        let node_registry_writes = combined
            .matches("registry heartbeat: self-registration written")
            .count();
        anyhow::ensure!(
            node_endpoint_writes <= MAX_CONTROL_LEASE_WRITES_PER_NODE,
            "fleet node {} emitted {node_endpoint_writes} PeerEndpoint writes; bounded lease ceiling is {MAX_CONTROL_LEASE_WRITES_PER_NODE}",
            node.agent_did
        );
        anyhow::ensure!(
            node_registry_writes <= MAX_CONTROL_LEASE_WRITES_PER_NODE,
            "fleet node {} emitted {node_registry_writes} PeerRegistry writes; bounded lease ceiling is {MAX_CONTROL_LEASE_WRITES_PER_NODE}",
            node.agent_did
        );
        endpoint_writes += node_endpoint_writes;
        registry_writes += node_registry_writes;
    }
    tracing::info!(
        fleet_size = fleet.len(),
        endpoint_writes,
        registry_writes,
        per_node_ceiling = MAX_CONTROL_LEASE_WRITES_PER_NODE,
        "release control-plane lease-write budget satisfied"
    );
    Ok(())
}

fn fleet_node_captured_output(node: &FleetNode) -> Result<(String, String)> {
    let (current_stdout, current_stderr) = node.serve.captured_output()?;
    let mut stdout = String::new();
    let mut stderr = String::new();
    for capture in &node.archived_logs {
        stdout.push_str(&format!(
            "\n===== {} stdout =====\n{}",
            capture.phase, capture.stdout
        ));
        stderr.push_str(&format!(
            "\n===== {} stderr =====\n{}",
            capture.phase, capture.stderr
        ));
    }
    stdout.push_str(&format!("\n===== current stdout =====\n{current_stdout}"));
    stderr.push_str(&format!("\n===== current stderr =====\n{current_stderr}"));
    Ok((stdout, stderr))
}

async fn wait_for_release_sweep(
    fleet: &[FleetNode],
    coord: &FleetNode,
    delegates: &[FleetNode],
    parent_session_id: &str,
    parent_request_id: &str,
    timeout: Duration,
) -> Result<HashMap<String, CompletedChild>> {
    let deadline = Instant::now() + timeout;
    let allowed_foreground_target = format!("researcher-{}", delegates.len());
    let completion = async {
        let (parent_terminal, completed_children) = tokio::try_join!(
            wait_for_request_terminal(&coord.graphql, parent_request_id, timeout),
            wait_for_all_subagent_children_completed(
                &coord.graphql,
                delegates,
                parent_session_id,
                parent_request_id,
                Some(allowed_foreground_target.as_str()),
                timeout,
            ),
        )?;
        anyhow::ensure!(
            parent_terminal == "completed",
            "release sweep parent terminalized as {parent_terminal}"
        );
        Ok::<_, anyhow::Error>(completed_children)
    };
    tokio::pin!(completion);
    let mut sample_interval = tokio::time::interval(Duration::from_secs(1));
    sample_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut consecutive_diagnostics_failures = 0usize;
    loop {
        tokio::select! {
            result = &mut completion => return result,
            _ = sample_interval.tick() => {
                match fetch_p2p_fleet(fleet).await {
                    Ok(snapshots) => {
                        consecutive_diagnostics_failures = 0;
                        assert_p2p_fleet_bounded(fleet, &snapshots)?;
                    }
                    Err(error) if Instant::now() < deadline => {
                        consecutive_diagnostics_failures += 1;
                        tracing::warn!(
                            error = %error,
                            consecutive_failures = consecutive_diagnostics_failures,
                            "transient P2P diagnostics fetch failed during release sweep; retrying"
                        );
                    }
                    Err(error) => {
                        consecutive_diagnostics_failures += 1;
                        bail!(
                            "P2P diagnostics fetch failed at or after the release sweep deadline after {consecutive_diagnostics_failures} consecutive failure(s); latest error: {error:#}"
                        );
                    }
                }
            }
        }
    }
}

async fn assert_release_subagent_inspection(
    coord_graphql: &str,
    parent_request_id: &str,
    parent_session_id: &str,
    expected_count: usize,
) -> Result<()> {
    let escaped_request_id = escape_graphql_string(parent_request_id);
    let expected_wait_target_name = format!("researcher-{expected_count}");
    let response = graphql_query(
        coord_graphql,
        &format!(
            r#"{{
                AgentToolCall(
                    filter: {{
                        request_id: {{ _eq: "{escaped_request_id}" }},
                        tool_name: {{ _eq: "spawn_subagent" }}
                    }},
                    order: {{ started_at: ASC }}
                ) {{
                    tool_name
                    args
                    result
                    lifecycle_state
                    child_request_id
                    await_mode
                }}
            }}"#
        ),
    )
    .await?;
    let rows = response
        .pointer("/data/AgentToolCall")
        .and_then(Value::as_array)
        .context("release sweep has no persisted spawn bridge rows")?;

    anyhow::ensure!(
        rows.len() == expected_count,
        "release sweep persisted {} spawn calls, expected exactly {expected_count}",
        rows.len()
    );
    let mut child_ids = HashSet::new();
    let mut child_ids_by_target = HashMap::new();
    for row in rows {
        anyhow::ensure!(
            row.get("lifecycle_state").and_then(Value::as_str) == Some("completed"),
            "release spawn bridge was not completed: {row}"
        );
        let child_id = row
            .get("child_request_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .with_context(|| format!("release spawn bridge has no child id: {row}"))?;
        anyhow::ensure!(
            child_ids.insert(child_id.to_string()),
            "duplicate release child request id {child_id}"
        );
        let args = parse_tool_result_json(row, "args")?;
        let name = args
            .get("name")
            .and_then(Value::as_str)
            .with_context(|| format!("release spawn args have no target name: {args}"))?;
        match row.get("await_mode").and_then(Value::as_str) {
            Some("background") => {}
            Some("foreground") if name == expected_wait_target_name => {}
            mode => bail!(
                "release spawn bridge for {name} has invalid final await mode {mode:?}; only {expected_wait_target_name} may be foreground"
            ),
        }
        anyhow::ensure!(
            child_ids_by_target
                .insert(name.to_string(), child_id.to_string())
                .is_none(),
            "duplicate release spawn target {name}"
        );
    }
    let expected_names = (1..=expected_count)
        .map(|index| format!("researcher-{index}"))
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        child_ids_by_target.keys().cloned().collect::<HashSet<_>>() == expected_names,
        "release spawn target set mismatch: actual={:?} expected={expected_names:?}",
        child_ids_by_target.keys().collect::<Vec<_>>()
    );

    let exchanges = fetch_transcript_tool_exchanges(coord_graphql, parent_session_id).await?;
    let spawn_exchanges = transcript_exchanges_named(&exchanges, "spawn_subagent");
    anyhow::ensure!(
        spawn_exchanges.len() == expected_count,
        "release transcript contains {} spawn calls, expected exactly {expected_count}",
        spawn_exchanges.len()
    );
    let transcript_targets = spawn_exchanges
        .iter()
        .filter_map(|exchange| exchange.args.get("name").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        transcript_targets == expected_names,
        "release transcript spawn target mismatch: actual={transcript_targets:?} expected={expected_names:?}"
    );
    anyhow::ensure!(
        spawn_exchanges.iter().all(|exchange| {
            exchange.args.get("await_mode").and_then(Value::as_str) == Some("background")
        }),
        "release transcript contains a spawn that did not request background mode: {spawn_exchanges:?}"
    );

    anyhow::ensure!(
        transcript_exchanges_named(&exchanges, "steer_subagent").is_empty(),
        "release parent called steer_subagent even though the sweep forbids steering"
    );
    let allowed_tool_names = [
        "spawn_subagent",
        "list_subagents",
        "wait_subagent",
        "read_subagent",
    ];
    let unexpected = exchanges
        .iter()
        .filter(|exchange| !allowed_tool_names.contains(&exchange.name.as_str()))
        .map(|exchange| exchange.name.as_str())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        unexpected.is_empty(),
        "release parent called tools outside the sweep contract: {unexpected:?}"
    );

    let list_rows = transcript_exchanges_named(&exchanges, "list_subagents");
    anyhow::ensure!(
        list_rows.len() == 1,
        "release parent must call list_subagents exactly once; saw {}",
        list_rows.len()
    );
    anyhow::ensure!(
        list_rows[0].args.get("status").and_then(Value::as_str) == Some("all")
            && list_rows[0]
                .args
                .get("limit")
                .and_then(Value::as_u64)
                .is_some_and(|limit| limit >= expected_count as u64),
        "list_subagents must explicitly request status=all with enough capacity: {:?}",
        list_rows[0].args
    );
    let list_result = parse_transcript_tool_result(list_rows[0])?;
    anyhow::ensure!(
        list_result.get("truncated").and_then(Value::as_bool) == Some(false),
        "list_subagents truncated the release bridge set: {list_result}"
    );
    let list_entries = list_result
        .get("entries")
        .and_then(Value::as_array)
        .with_context(|| format!("list_subagents result has no entries: {list_result}"))?;
    anyhow::ensure!(
        list_entries.len() == expected_count,
        "list_subagents returned {} entries, expected {expected_count}: {list_result}",
        list_entries.len()
    );
    let listed_ids = list_entries
        .iter()
        .filter_map(|entry| entry.get("child_request_id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<HashSet<_>>();
    anyhow::ensure!(
        listed_ids == child_ids,
        "list_subagents did not expose the exact owned bridge set: listed={listed_ids:?} spawned={child_ids:?}"
    );
    anyhow::ensure!(
        list_entries
            .iter()
            .all(|entry| { entry.get("await_mode").and_then(Value::as_str) == Some("background") }),
        "list_subagents did not preserve the background spawn mode: {list_result}"
    );

    let wait_rows = transcript_exchanges_named(&exchanges, "wait_subagent");
    let read_rows = transcript_exchanges_named(&exchanges, "read_subagent");
    anyhow::ensure!(
        !wait_rows.is_empty() && !read_rows.is_empty(),
        "release parent must successfully wait and read a subagent; wait={} read={} session={parent_session_id}",
        wait_rows.len(),
        read_rows.len()
    );
    let expected_waited_child_id = child_ids_by_target
        .get(&expected_wait_target_name)
        .context("release sweep has no last researcher child id")?;
    let mut successful_wait_exchange_ids = HashSet::new();
    for exchange in &wait_rows {
        let waited_child_id = exchange
            .args
            .get("child_request_id")
            .and_then(Value::as_str)
            .context("wait_subagent args omitted child_request_id")?;
        anyhow::ensure!(
            waited_child_id == expected_waited_child_id,
            "wait_subagent targeted {waited_child_id}, expected researcher-{expected_count} child {expected_waited_child_id}"
        );
        let wait_result = parse_transcript_tool_result(exchange)?;
        if wait_result.get("ok").and_then(Value::as_bool) == Some(true) {
            anyhow::ensure!(
                wait_result.get("child_request_id").and_then(Value::as_str)
                    == Some(waited_child_id)
                    && wait_result.get("status").and_then(Value::as_str) == Some("completed"),
                "successful wait_subagent did not observe researcher-{expected_count} completed: {wait_result}"
            );
            successful_wait_exchange_ids.insert(exchange.id.as_str());
        } else {
            anyhow::ensure!(
                wait_result.get("ok").and_then(Value::as_bool) == Some(false)
                    && wait_result.get("retryable").and_then(Value::as_bool) == Some(true)
                    && wait_result.get("failure_class").and_then(Value::as_str)
                        == Some("service_unavailable"),
                "wait_subagent may retry only after retryable materialization failures: {wait_result}"
            );
        }
    }
    anyhow::ensure!(
        !successful_wait_exchange_ids.is_empty(),
        "wait_subagent never successfully observed researcher-{expected_count} completed"
    );
    let waited_child_id = expected_waited_child_id.as_str();

    let expected_read_child_id = child_ids_by_target
        .get("researcher-1")
        .context("release sweep has no researcher-1 child id")?;
    anyhow::ensure!(
        expected_read_child_id != waited_child_id,
        "read_subagent must inspect a different background child than wait_subagent"
    );
    let mut successful_read_exchange_ids = HashSet::new();
    for exchange in &read_rows {
        let read_child_id = exchange
            .args
            .get("child_request_id")
            .and_then(Value::as_str)
            .context("read_subagent args omitted child_request_id")?;
        anyhow::ensure!(
            read_child_id == expected_read_child_id,
            "read_subagent targeted {read_child_id}, expected researcher-1 child {expected_read_child_id}"
        );
        anyhow::ensure!(
            exchange
                .args
                .get("include_user_messages")
                .and_then(Value::as_bool)
                == Some(true),
            "read_subagent must include user messages so a materialized live child has an observable transcript: {:?}",
            exchange.args
        );
        let read_result = parse_transcript_tool_result(exchange)?;
        if read_result.get("child_request_id").and_then(Value::as_str) == Some(read_child_id)
            && read_result
                .get("child_session_id")
                .and_then(Value::as_str)
                .is_some_and(|session_id| !session_id.trim().is_empty())
            && read_result
                .get("lifecycle_state")
                .and_then(Value::as_str)
                .is_some_and(|state| {
                    !state.trim().is_empty() && state != "awaiting_child_materialization"
                })
            && read_result
                .get("transcript")
                .and_then(Value::as_str)
                .is_some_and(|transcript| !transcript.trim().is_empty())
        {
            successful_read_exchange_ids.insert(exchange.id.as_str());
        }
    }
    anyhow::ensure!(
        !successful_read_exchange_ids.is_empty(),
        "read_subagent never returned a materialized researcher-1 child with observable transcript content"
    );

    let last_spawn_index = exchanges
        .iter()
        .rposition(|exchange| exchange.name == "spawn_subagent")
        .context("release transcript contains no spawn_subagent call")?;
    let list_index = exchanges
        .iter()
        .position(|exchange| exchange.name == "list_subagents")
        .context("release transcript contains no list_subagents call")?;
    let first_wait_index = exchanges
        .iter()
        .position(|exchange| exchange.name == "wait_subagent")
        .context("release transcript contains no wait_subagent call")?;
    let successful_wait_index = exchanges
        .iter()
        .position(|exchange| successful_wait_exchange_ids.contains(exchange.id.as_str()))
        .context("release transcript contains no successful wait_subagent call")?;
    let successful_read_index = exchanges
        .iter()
        .position(|exchange| successful_read_exchange_ids.contains(exchange.id.as_str()))
        .context("release transcript contains no successful read_subagent call")?;
    anyhow::ensure!(
        last_spawn_index < list_index
            && list_index < first_wait_index
            && successful_wait_index < successful_read_index,
        "release inspection tools ran out of order: last_spawn={last_spawn_index} list={list_index} first_wait={first_wait_index} successful_wait={successful_wait_index} successful_read={successful_read_index}"
    );
    Ok(())
}

async fn fetch_transcript_tool_exchanges(
    coord_graphql: &str,
    parent_session_id: &str,
) -> Result<Vec<TranscriptToolExchange>> {
    let escaped_session_id = escape_graphql_string(parent_session_id);
    let response = graphql_query(
        coord_graphql,
        &format!(
            r#"{{
                AgentMessage(
                    filter: {{ session_id: {{ _eq: "{escaped_session_id}" }} }},
                    order: {{ sequence: ASC }}
                ) {{ role content sequence }}
            }}"#
        ),
    )
    .await?;
    let rows = response
        .pointer("/data/AgentMessage")
        .and_then(Value::as_array)
        .context("release parent transcript has no AgentMessage rows")?;
    let mut exchanges = Vec::<TranscriptToolExchange>::new();
    for row in rows {
        let role = row
            .get("role")
            .and_then(Value::as_str)
            .context("release transcript row has no role")?;
        let content = row
            .get("content")
            .and_then(Value::as_str)
            .context("release transcript row has no content")?;
        match decode_persisted_message(role, content) {
            ProtocolMessage::Assistant { content, .. } => {
                for item in content {
                    if let AssistantContent::ToolCall(tool_call) = item {
                        exchanges.push(TranscriptToolExchange {
                            id: tool_call.id,
                            call_id: tool_call.call_id,
                            name: tool_call.function.name,
                            args: tool_call.function.arguments,
                            result: None,
                        });
                    }
                }
            }
            ProtocolMessage::User { content } => {
                for item in content {
                    let UserContent::ToolResult(tool_result) = item else {
                        continue;
                    };
                    let result = tool_result
                        .content
                        .iter()
                        .filter_map(|content| match content {
                            ToolResultContent::Text(text) => Some(text.text.as_str()),
                            ToolResultContent::Image(_) => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    let exchange = exchanges
                        .iter_mut()
                        .rev()
                        .find(|exchange| {
                            exchange.result.is_none()
                                && transcript_tool_result_matches(exchange, &tool_result)
                        })
                        .with_context(|| {
                            format!(
                                "release transcript tool result has no matching call: id={} call_id={:?}",
                                tool_result.id, tool_result.call_id
                            )
                        })?;
                    exchange.result = Some(result);
                }
            }
            ProtocolMessage::System { .. } => {}
        }
    }
    Ok(exchanges)
}

fn transcript_tool_result_matches(
    exchange: &TranscriptToolExchange,
    result: &gents_protocol::message::ToolResult,
) -> bool {
    exchange.id == result.id
        || exchange.call_id.as_deref() == Some(result.id.as_str())
        || result.call_id.as_deref() == Some(exchange.id.as_str())
        || (exchange.call_id.is_some() && exchange.call_id == result.call_id)
}

fn transcript_exchanges_named<'a>(
    exchanges: &'a [TranscriptToolExchange],
    name: &str,
) -> Vec<&'a TranscriptToolExchange> {
    exchanges
        .iter()
        .filter(|exchange| exchange.name == name)
        .collect()
}

fn parse_transcript_tool_result(exchange: &TranscriptToolExchange) -> Result<Value> {
    let result = exchange
        .result
        .as_deref()
        .with_context(|| format!("{} has no paired transcript result", exchange.name))?;
    serde_json::from_str(result)
        .with_context(|| format!("decoding {} transcript result: {result}", exchange.name))
}

fn parse_tool_result_json(row: &Value, field: &str) -> Result<Value> {
    let raw = row
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("tool call has no string {field}: {row}"))?;
    serde_json::from_str(raw).with_context(|| format!("decoding tool call {field}: {raw}"))
}

#[derive(Debug)]
struct FleetParentTurnCapture {
    turn: codex::Turn,
    collab_items: Vec<codex::ThreadItem>,
}

#[derive(Debug)]
struct FleetLiveChildObservation {
    thread_id: String,
    delta: String,
}

fn fleet_request_id(value: i64) -> codex::RequestId {
    codex::RequestId::Integer(value)
}

async fn fleet_connect_and_initialize_codex(port: u16) -> Result<FleetShimWebSocket> {
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{port}/"))
        .await
        .with_context(|| format!("connecting to fleet Codex shim on port {port}"))?;
    fleet_send_codex_request(
        &mut ws,
        codex::ClientRequest::Initialize {
            request_id: fleet_request_id(1),
            params: codex::InitializeParams {
                client_info: codex::ClientInfo {
                    name: "gents-fleet-live-test".to_string(),
                    title: None,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                capabilities: None,
            },
        },
    )
    .await?;
    let _: codex::InitializeResponse =
        fleet_read_typed_response(&mut ws, fleet_request_id(1)).await?;
    fleet_send_codex_notification(&mut ws, codex::ClientNotification::Initialized).await?;
    Ok(ws)
}

async fn fleet_start_codex_thread(ws: &mut FleetShimWebSocket, cwd: &Path) -> Result<String> {
    fleet_send_codex_request(
        ws,
        codex::ClientRequest::ThreadStart {
            request_id: fleet_request_id(2),
            params: codex::ThreadStartParams {
                cwd: Some(cwd.display().to_string()),
                ..Default::default()
            },
        },
    )
    .await?;
    let response: codex::ThreadStartResponse =
        fleet_read_typed_response(ws, fleet_request_id(2)).await?;
    Ok(response.thread.id)
}

async fn fleet_start_codex_turn(
    ws: &mut FleetShimWebSocket,
    thread_id: &str,
    prompt: &str,
) -> Result<String> {
    fleet_send_codex_request(
        ws,
        codex::ClientRequest::TurnStart {
            request_id: fleet_request_id(3),
            params: codex::TurnStartParams {
                thread_id: thread_id.to_string(),
                input: vec![codex::UserInput::Text {
                    text: prompt.to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    let response: codex::TurnStartResponse =
        fleet_read_typed_response(ws, fleet_request_id(3)).await?;
    Ok(response.turn.id)
}

async fn fleet_capture_parent_turn(ws: &mut FleetShimWebSocket) -> Result<FleetParentTurnCapture> {
    let mut collab_items = Vec::new();
    loop {
        match fleet_read_jsonrpc(ws).await? {
            codex::JSONRPCMessage::Notification(notification) => {
                match fleet_server_notification(notification)? {
                    codex::ServerNotification::ItemCompleted(completed)
                        if matches!(
                            completed.item,
                            codex::ThreadItem::CollabAgentToolCall { .. }
                        ) =>
                    {
                        collab_items.push(completed.item);
                    }
                    codex::ServerNotification::TurnCompleted(completed) => {
                        return Ok(FleetParentTurnCapture {
                            turn: completed.turn,
                            collab_items,
                        });
                    }
                    _ => {}
                }
            }
            codex::JSONRPCMessage::Error(error) => {
                bail!("fleet Codex shim emitted an error: {}", error.error.message);
            }
            codex::JSONRPCMessage::Request(request) => {
                bail!("fleet Codex shim sent an unexpected request: {request:?}");
            }
            codex::JSONRPCMessage::Response(_) => {}
        }
    }
}

async fn fleet_observe_live_child(
    shim_port: u16,
    parent_thread_id: &str,
) -> Result<FleetLiveChildObservation> {
    let mut list_ws = fleet_connect_and_initialize_codex(shim_port).await?;
    let mut seen = HashSet::new();
    let mut observing = HashSet::new();
    let mut retry_after = HashMap::<String, Instant>::new();
    let mut observers = tokio::task::JoinSet::new();
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut request_sequence = 100_i64;

    loop {
        while let Some(joined) = observers.try_join_next() {
            let (thread_id, result) = joined.context("fleet child observer task panicked")?;
            observing.remove(&thread_id);
            match result? {
                Some(observation) => {
                    observers.abort_all();
                    return Ok(observation);
                }
                None => {
                    retry_after.insert(thread_id, Instant::now() + Duration::from_millis(500));
                }
            }
        }

        if Instant::now() >= deadline {
            observers.abort_all();
            bail!(
                "no live delta was observed from {} navigable runtime-spawned Codex child threads",
                seen.len()
            );
        }

        let request_id = fleet_request_id(request_sequence);
        request_sequence += 1;
        fleet_send_codex_request(
            &mut list_ws,
            codex::ClientRequest::ThreadList {
                request_id: request_id.clone(),
                params: codex::ThreadListParams {
                    cursor: None,
                    limit: Some(200),
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
        let response: codex::ThreadListResponse =
            fleet_read_typed_response(&mut list_ws, request_id).await?;
        for thread in response.data {
            seen.insert(thread.id.clone());
            if observing.contains(&thread.id)
                || retry_after
                    .get(&thread.id)
                    .is_some_and(|retry_at| *retry_at > Instant::now())
            {
                continue;
            }
            let thread_id = thread.id;
            observing.insert(thread_id.clone());
            retry_after.remove(&thread_id);
            let parent_thread_id = parent_thread_id.to_string();
            let observer_thread_id = thread_id.clone();
            observers.spawn(async move {
                let result =
                    fleet_observe_child_thread(shim_port, observer_thread_id, parent_thread_id)
                        .await;
                (thread_id, result)
            });
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn fleet_observe_child_thread(
    shim_port: u16,
    thread_id: String,
    parent_thread_id: String,
) -> Result<Option<FleetLiveChildObservation>> {
    let mut ws = fleet_connect_and_initialize_codex(shim_port).await?;
    fleet_send_codex_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: fleet_request_id(10),
            params: codex::ThreadReadParams {
                thread_id: thread_id.clone(),
                include_turns: true,
            },
        },
    )
    .await?;
    let read: codex::ThreadReadResponse =
        fleet_read_typed_response(&mut ws, fleet_request_id(10)).await?;
    let read_json = serde_json::to_value(&read.thread)?;
    anyhow::ensure!(
        read_json
            .pointer("/source/subAgent/thread_spawn/parent_thread_id")
            .and_then(Value::as_str)
            == Some(parent_thread_id.as_str()),
        "child thread {thread_id} did not expose native Codex ancestry: {read_json}"
    );
    let Some(turn_id) = read
        .thread
        .turns
        .iter()
        .rev()
        .find(|turn| turn.status == codex::TurnStatus::InProgress)
        .map(|turn| turn.id.clone())
    else {
        return Ok(None);
    };

    fleet_send_codex_request(
        &mut ws,
        codex::ClientRequest::ThreadResume {
            request_id: fleet_request_id(11),
            params: codex::ThreadResumeParams {
                thread_id: thread_id.clone(),
                cwd: None,
                ..Default::default()
            },
        },
    )
    .await?;

    let observation = tokio::time::timeout(Duration::from_secs(60), async {
        let mut resumed = false;
        let mut terminal = false;
        let mut delta = None::<String>;
        loop {
            match fleet_read_jsonrpc(&mut ws).await? {
                codex::JSONRPCMessage::Response(response)
                    if response.id == fleet_request_id(11) =>
                {
                    let resume: codex::ThreadResumeResponse =
                        serde_json::from_value(response.result)
                            .context("decoding fleet child thread/resume response")?;
                    resumed = true;
                    terminal = !resume.thread.turns.iter().any(|turn| {
                        turn.id == turn_id && turn.status == codex::TurnStatus::InProgress
                    });
                }
                codex::JSONRPCMessage::Notification(notification) => {
                    match fleet_server_notification(notification)? {
                        codex::ServerNotification::AgentMessageDelta(update)
                            if update.thread_id == thread_id
                                && update.turn_id == turn_id
                                && !update.delta.is_empty() =>
                        {
                            delta = Some(update.delta);
                        }
                        codex::ServerNotification::TurnCompleted(completed)
                            if completed.thread_id == thread_id && completed.turn.id == turn_id =>
                        {
                            terminal = true;
                        }
                        _ => {}
                    }
                }
                codex::JSONRPCMessage::Error(error) => {
                    bail!(
                        "fleet child {thread_id} emitted an error while resuming: {}",
                        error.error.message
                    );
                }
                codex::JSONRPCMessage::Request(request) => {
                    bail!("fleet child {thread_id} sent an unexpected request: {request:?}");
                }
                codex::JSONRPCMessage::Response(response) => {
                    bail!(
                        "unexpected response while resuming fleet child {thread_id}: {response:?}"
                    );
                }
            }

            if resumed {
                if let Some(delta) = delta.take() {
                    return Ok(Some(FleetLiveChildObservation { thread_id, delta }));
                }
                if terminal {
                    return Ok(None);
                }
            }
        }
    })
    .await;

    match observation {
        Ok(result) => result,
        Err(_) => Ok(None),
    }
}

fn assert_fleet_parent_collab_projection(
    capture: &FleetParentTurnCapture,
    expected_child_threads: &HashSet<String>,
) -> Result<()> {
    let mut projected = HashSet::new();
    for item in &capture.collab_items {
        let codex::ThreadItem::CollabAgentToolCall {
            tool,
            status,
            receiver_thread_ids,
            model,
            agents_states,
            ..
        } = item
        else {
            continue;
        };
        if *tool != codex::CollabAgentTool::SpawnAgent {
            continue;
        }
        anyhow::ensure!(
            *status == codex::CollabAgentToolCallStatus::Completed,
            "spawn projection was not terminal-completed: {item:?}"
        );
        anyhow::ensure!(
            model
                .as_deref()
                .is_none_or(|value| !value.trim().is_empty()),
            "spawn projection exposed a blank child model: {item:?}"
        );
        for thread_id in receiver_thread_ids {
            anyhow::ensure!(
                agents_states.contains_key(thread_id),
                "spawn projection omitted agentsStates for {thread_id}: {item:?}"
            );
            projected.insert(thread_id.clone());
        }
    }
    anyhow::ensure!(
        projected == *expected_child_threads,
        "native parent collab projection did not match real runtime children; projected={projected:?} expected={expected_child_threads:?} items={:?}",
        capture.collab_items
    );
    Ok(())
}

async fn assert_fleet_completed_collab_history(
    shim_port: u16,
    parent_thread_id: &str,
    expected_child_threads: &HashSet<String>,
) -> Result<()> {
    let mut ws = fleet_connect_and_initialize_codex(shim_port).await?;
    fleet_send_codex_request(
        &mut ws,
        codex::ClientRequest::ThreadRead {
            request_id: fleet_request_id(20),
            params: codex::ThreadReadParams {
                thread_id: parent_thread_id.to_string(),
                include_turns: true,
            },
        },
    )
    .await?;
    let read: codex::ThreadReadResponse =
        fleet_read_typed_response(&mut ws, fleet_request_id(20)).await?;
    let mut completed = HashSet::new();
    for item in read.thread.turns.iter().flat_map(|turn| &turn.items) {
        if let codex::ThreadItem::CollabAgentToolCall {
            tool: codex::CollabAgentTool::SpawnAgent,
            receiver_thread_ids,
            agents_states,
            ..
        } = item
        {
            for thread_id in receiver_thread_ids {
                if agents_states
                    .get(thread_id)
                    .is_some_and(|state| state.status == codex::CollabAgentStatus::Completed)
                {
                    completed.insert(thread_id.clone());
                }
            }
        }
    }
    anyhow::ensure!(
        completed == *expected_child_threads,
        "completed parent history did not refresh all native agentsStates; completed={completed:?} expected={expected_child_threads:?}"
    );
    Ok(())
}

async fn assert_fleet_child_thread_is_read_only(
    shim_port: u16,
    child_thread_id: &str,
) -> Result<()> {
    let mut ws = fleet_connect_and_initialize_codex(shim_port).await?;
    fleet_send_codex_request(
        &mut ws,
        codex::ClientRequest::TurnStart {
            request_id: fleet_request_id(30),
            params: codex::TurnStartParams {
                thread_id: child_thread_id.to_string(),
                input: vec![codex::UserInput::Text {
                    text: "this write must be rejected".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    loop {
        match fleet_read_jsonrpc(&mut ws).await? {
            codex::JSONRPCMessage::Error(error) if error.id == fleet_request_id(30) => {
                anyhow::ensure!(
                    error.error.message.contains("read-only"),
                    "unexpected child turn/start rejection: {}",
                    error.error.message
                );
                return Ok(());
            }
            codex::JSONRPCMessage::Response(response) if response.id == fleet_request_id(30) => {
                bail!("read-only fleet child accepted turn/start: {response:?}");
            }
            codex::JSONRPCMessage::Notification(_) => {}
            other => bail!("unexpected message awaiting child read-only rejection: {other:?}"),
        }
    }
}

async fn fleet_send_codex_request(
    ws: &mut FleetShimWebSocket,
    request: codex::ClientRequest,
) -> Result<()> {
    let request: codex::JSONRPCRequest = serde_json::from_value(serde_json::to_value(request)?)
        .context("building fleet Codex JSON-RPC request")?;
    fleet_write_jsonrpc(ws, codex::JSONRPCMessage::Request(request)).await
}

async fn fleet_send_codex_notification(
    ws: &mut FleetShimWebSocket,
    notification: codex::ClientNotification,
) -> Result<()> {
    let notification: codex::JSONRPCNotification =
        serde_json::from_value(serde_json::to_value(notification)?)
            .context("building fleet Codex JSON-RPC notification")?;
    fleet_write_jsonrpc(ws, codex::JSONRPCMessage::Notification(notification)).await
}

async fn fleet_write_jsonrpc(
    ws: &mut FleetShimWebSocket,
    message: codex::JSONRPCMessage,
) -> Result<()> {
    let text = serde_json::to_string(&message).context("encoding fleet Codex JSON-RPC")?;
    ws.send(WsMessage::Text(text.into()))
        .await
        .context("sending fleet Codex JSON-RPC websocket frame")
}

async fn fleet_read_typed_response<T>(
    ws: &mut FleetShimWebSocket,
    expected_id: codex::RequestId,
) -> Result<T>
where
    T: DeserializeOwned,
{
    loop {
        match fleet_read_jsonrpc(ws).await? {
            codex::JSONRPCMessage::Response(response) if response.id == expected_id => {
                return serde_json::from_value(response.result)
                    .context("decoding fleet Codex response");
            }
            codex::JSONRPCMessage::Error(error) if error.id == expected_id => {
                bail!(
                    "fleet Codex shim returned an error for {expected_id}: {}",
                    error.error.message
                );
            }
            codex::JSONRPCMessage::Notification(_) => {}
            other => {
                bail!("unexpected fleet Codex message while waiting for {expected_id}: {other:?}")
            }
        }
    }
}

async fn fleet_read_jsonrpc(ws: &mut FleetShimWebSocket) -> Result<codex::JSONRPCMessage> {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(90), ws.next())
            .await
            .context("timed out waiting for fleet Codex shim websocket message")?
            .ok_or_else(|| anyhow!("fleet Codex shim websocket closed"))?
            .context("reading fleet Codex shim websocket frame")?;
        let text = match frame {
            WsMessage::Text(text) => text,
            WsMessage::Binary(bytes) => String::from_utf8(bytes.to_vec())
                .context("decoding fleet Codex binary websocket payload")?
                .into(),
            WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
            WsMessage::Close(close) => bail!("fleet Codex shim websocket closed: {close:?}"),
            WsMessage::Frame(_) => bail!("unexpected raw fleet Codex websocket frame"),
        };
        return serde_json::from_str(&text)
            .with_context(|| format!("decoding fleet Codex JSON-RPC message: {text}"));
    }
}

fn fleet_server_notification(
    notification: codex::JSONRPCNotification,
) -> Result<codex::ServerNotification> {
    serde_json::from_value(serde_json::to_value(notification)?)
        .context("decoding fleet Codex server notification")
}

async fn bring_up_fleet(
    root: &Path,
    count: usize,
    model_endpoint: &str,
    model_name: &str,
    coordinator_codex_shim: bool,
) -> Result<Vec<FleetNode>> {
    let mut nodes = Vec::with_capacity(count);
    for index in 0..count {
        let label = if index == 0 {
            "coordinator".to_string()
        } else {
            format!("subagent-{index}")
        };
        let home = root.join(&label);
        fs::create_dir_all(&home)?;
        let port = allocate_port()?;
        let graphql = graphql_url(port);
        let agent_name = format!("fleet-{label}-{}", Uuid::new_v4().simple());

        let init = run_init_json(
            &home,
            &[
                "--agent-name",
                &agent_name,
                "--model-name",
                model_name,
                "--max-concurrent",
                "4",
                "--max-queue-depth",
                "16",
                model_endpoint,
            ],
        )?;
        let agent_did = agent_did_from_init(&init)?;
        let behavior_id = init_string(&init, "default_behavior_id")?;
        let tool_selection_id = init_string(&init, "tool_selection_id")?;
        let backend_id = init_string(&init, "backend_id")?;
        let inference_profile_id = init_string(&init, "inference_profile_id")?;
        let model_name = init_string(&init, "model_name")?;

        let codex_shim_port = if index == 0 && coordinator_codex_shim {
            Some(allocate_port()?)
        } else {
            None
        };
        let serve_args = fleet_server_args(codex_shim_port);
        let serve_arg_refs = serve_args.iter().map(String::as_str).collect::<Vec<_>>();
        let (mut serve, readiness) =
            spawn_server_with_ready_json(&home, port, &serve_arg_refs, FAST_RECONCILE_ENVS)?;
        wait_for_port(port, &mut serve)?;
        if let Some(shim_port) = codex_shim_port {
            wait_for_port(shim_port, &mut serve)?;
        }
        wait_for_runtime_ready(&graphql, &agent_did, Duration::from_secs(30)).await?;
        let peer_id = readiness
            .get("p2p_peer_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("{label} readiness missing p2p_peer_id: {readiness}"))?;
        let address = shareable_address_from_readiness(&readiness, &peer_id)
            .with_context(|| format!("{label} readiness missing P2P address: {readiness}"))?;
        let shareable = fetch_shareable_address(&graphql)
            .await
            .with_context(|| format!("{label} fetching shareable P2P address"))?;

        nodes.push(FleetNode {
            home,
            graphql,
            agent_did,
            peer_id,
            address,
            shareable,
            behavior_id,
            tool_selection_id,
            backend_id,
            inference_profile_id,
            model_name,
            codex_shim_port,
            archived_logs: Vec::new(),
            serve,
        });
    }
    Ok(nodes)
}

fn fleet_server_args(codex_shim_port: Option<u16>) -> Vec<String> {
    let mut args = P2P_LOOPBACK_ARGS
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    if let Some(shim_port) = codex_shim_port {
        args.extend([
            "--codex-shim".to_string(),
            "--codex-shim-port".to_string(),
            shim_port.to_string(),
            "--codex-shim-poll-ms".to_string(),
            "100".to_string(),
            "--codex-shim-timeout-secs".to_string(),
            "900".to_string(),
        ]);
    }
    args
}

async fn restart_fleet_node(node: &mut FleetNode) -> Result<()> {
    if node
        .serve
        .child
        .try_wait()
        .context("checking fleet daemon before restart")?
        .is_none()
    {
        node.serve
            .child
            .kill()
            .context("stopping fleet daemon for restart")?;
    }
    node.serve
        .child
        .wait()
        .context("waiting for stopped fleet daemon")?;
    let (archived_stdout, archived_stderr) = node
        .serve
        .captured_output()
        .context("capturing stopped fleet daemon logs before restart")?;

    let http_port = reqwest::Url::parse(&node.graphql)
        .with_context(|| format!("parsing fleet GraphQL URL {}", node.graphql))?
        .port()
        .with_context(|| format!("fleet GraphQL URL has no port: {}", node.graphql))?;
    let serve_args = fleet_server_args(node.codex_shim_port);
    let serve_arg_refs = serve_args.iter().map(String::as_str).collect::<Vec<_>>();
    let (mut serve, readiness) =
        spawn_server_with_ready_json(&node.home, http_port, &serve_arg_refs, FAST_RECONCILE_ENVS)?;
    wait_for_port(http_port, &mut serve)?;
    if let Some(shim_port) = node.codex_shim_port {
        wait_for_port(shim_port, &mut serve)?;
    }
    wait_for_runtime_ready(&node.graphql, &node.agent_did, Duration::from_secs(30)).await?;

    let peer_id = readiness
        .get("p2p_peer_id")
        .and_then(Value::as_str)
        .context("restarted fleet daemon readiness missing p2p_peer_id")?;
    anyhow::ensure!(
        peer_id == node.peer_id,
        "fleet restart changed peer identity: before={} after={peer_id}",
        node.peer_id
    );
    node.address = shareable_address_from_readiness(&readiness, peer_id)
        .context("restarted fleet daemon readiness missing P2P address")?;
    node.shareable = fetch_shareable_address(&node.graphql)
        .await
        .context("fetching restarted fleet daemon shareable address")?;
    node.archived_logs.push(FleetLogCapture {
        phase: format!("before-restart-{}", node.archived_logs.len() + 1),
        stdout: archived_stdout,
        stderr: archived_stderr,
    });
    node.serve = serve;
    Ok(())
}

fn init_string(init: &Value, key: &str) -> Result<String> {
    let nested = format!("/init/{key}");
    init.get(key)
        .or_else(|| init.pointer(&nested))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("init output missing {key}: {init}"))
}

fn shareable_address_from_readiness(readiness: &Value, peer_id: &str) -> Option<String> {
    let raw = readiness
        .get("p2p_shareable_address")
        .and_then(Value::as_str)
        .or_else(|| {
            readiness
                .get("p2p_listen_addresses")
                .and_then(Value::as_array)
                .and_then(|rows| rows.iter().find_map(Value::as_str))
        })?
        .trim();
    if raw.is_empty() {
        None
    } else if raw.contains("/p2p/") {
        Some(raw.to_string())
    } else {
        Some(format!("{raw}/p2p/{peer_id}"))
    }
}

fn required_output_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("output missing {key}: {value}"))
}

async fn fetch_shareable_address(graphql: &str) -> Result<String> {
    let api_base = graphql
        .strip_suffix("/graphql")
        .with_context(|| format!("unexpected GraphQL endpoint shape: {graphql}"))?;
    let url = format!("{api_base}/p2p/shareable-address");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("building shareable-address client")?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(resp) = client.get(&url).send().await {
            if let Ok(value) = resp.json::<Value>().await {
                if let Some(addr) = value
                    .get("address")
                    .and_then(Value::as_str)
                    .filter(|s| !s.trim().is_empty())
                {
                    return Ok(addr.to_string());
                }
            }
        }
        if Instant::now() >= deadline {
            bail!("timed out fetching shareable address from {url}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn establish_reconciler_pairing(coord: &FleetNode, subagents: &[FleetNode]) -> Result<()> {
    establish_control_plane(coord, subagents).await?;
    establish_conversation_data_plane(coord, subagents).await
}

async fn establish_control_plane(coord: &FleetNode, subagents: &[FleetNode]) -> Result<()> {
    run_cli_json(
        &coord.home,
        &[
            "p2p",
            "network",
            "create",
            "--name",
            "Fleet One",
            "--output",
            "json",
        ],
    )
    .context("coordinator network create")?;
    for subagent in subagents {
        run_cli_json(
            &coord.home,
            &[
                "p2p",
                "network",
                "grant",
                &subagent.agent_did,
                "--output",
                "json",
            ],
        )
        .with_context(|| format!("granting membership to {}", subagent.agent_did))?;
    }
    for subagent in subagents {
        let invite = run_cli_json(
            &coord.home,
            &[
                "p2p",
                "pairings",
                "invite",
                "--member-did",
                &subagent.agent_did,
                "--template",
                "network-control",
            ],
        )
        .with_context(|| format!("minting v5 invite for {}", subagent.agent_did))?;
        let token = invite
            .get("token")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("invite for {} missing token: {invite}", subagent.agent_did))?;
        let joined = run_cli_json(&subagent.home, &["p2p", "pairings", "join", token])
            .with_context(|| format!("{} joining fleet", subagent.agent_did))?;
        let status = joined.get("status").and_then(Value::as_str);
        anyhow::ensure!(
            matches!(status, Some("pairing_joined") | Some("pairing_exists")),
            "unexpected join status for {}: {joined}",
            subagent.agent_did
        );
    }

    Ok(())
}

async fn establish_conversation_data_plane(
    coord: &FleetNode,
    subagents: &[FleetNode],
) -> Result<()> {
    for subagent in subagents {
        upsert_conversation_data_plane(
            &coord.graphql,
            &subagent.peer_id,
            &coord.agent_did,
            &subagent.shareable,
        )
        .await?;
        upsert_conversation_data_plane(
            &subagent.graphql,
            &coord.peer_id,
            &subagent.agent_did,
            &coord.shareable,
        )
        .await?;
    }
    Ok(())
}

async fn upsert_conversation_data_plane(
    graphql: &str,
    peer_id: &str,
    agent_did: &str,
    address: &str,
) -> Result<()> {
    let peer_id = escape_graphql_string(peer_id);
    let agent_did = escape_graphql_string(agent_did);
    let address = escape_graphql_string(address);
    let now = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
    let collections = CONVERSATION_COLLECTIONS
        .iter()
        .map(|collection| format!("\"{collection}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let mutation = format!(
        r#"mutation {{
            upsert_DataPlanePairingDesired(
                filter: {{ peer_id: {{ _eq: "{peer_id}" }} }},
                add: {{
                    peer_id: "{peer_id}",
                    agent_did: "{agent_did}",
                    collections: [{collections}],
                    replicator_addresses: ["{address}"],
                    template: "conversation",
                    created_at: "{now}",
                    updated_at: "{now}"
                }},
                update: {{
                    agent_did: "{agent_did}",
                    collections: [{collections}],
                    replicator_addresses: ["{address}"],
                    template: "conversation",
                    updated_at: "{now}"
                }}
            ) {{ _docID }}
        }}"#
    );
    graphql_query(graphql, &mutation).await?;
    Ok(())
}

async fn wait_for_fleet_pairing(coord: &FleetNode, subagents: &[FleetNode]) -> Result<()> {
    for subagent in subagents {
        wait_for_conversation_replicator_installed(
            &subagent.graphql,
            &coord.peer_id,
            &subagent.agent_did,
            Duration::from_secs(120),
        )
        .await
        .with_context(|| {
            format!(
                "{} -> coordinator conversation replicator filtered to {}",
                subagent.agent_did, subagent.agent_did
            )
        })?;
        wait_for_conversation_replicator_installed(
            &coord.graphql,
            &subagent.peer_id,
            &coord.agent_did,
            Duration::from_secs(120),
        )
        .await
        .with_context(|| {
            format!(
                "coordinator -> {} conversation replicator filtered to {}",
                subagent.agent_did, coord.agent_did
            )
        })?;
    }
    Ok(())
}

async fn wait_for_fleet_control_plane(coord: &FleetNode, subagents: &[FleetNode]) -> Result<()> {
    wait_for_fleet_control_plane_collection(coord, subagents, "AgentNetwork").await
}

async fn wait_for_fleet_control_plane_collection(
    coord: &FleetNode,
    subagents: &[FleetNode],
    required_collection: &str,
) -> Result<()> {
    for subagent in subagents {
        wait_for_subscription_replicator_installed(
            &subagent.graphql,
            &coord.peer_id,
            required_collection,
            Duration::from_secs(120),
        )
        .await
        .with_context(|| {
            format!(
                "{} -> coordinator replicator containing {required_collection}",
                subagent.agent_did
            )
        })?;
        wait_for_subscription_replicator_installed(
            &coord.graphql,
            &subagent.peer_id,
            required_collection,
            Duration::from_secs(120),
        )
        .await
        .with_context(|| {
            format!(
                "coordinator -> {} replicator containing {required_collection}",
                subagent.agent_did
            )
        })?;
    }
    Ok(())
}

async fn dump_fleet_doc_state(fleet: &[FleetNode]) {
    let query = r#"{
        AgentNetwork { _docID network_id admin_did }
        NetworkMembership { _docID membership_key member_did status }
        PeerEndpoint { _docID did }
        PeerPairingDesired { _docID peer_id source template replicator_addresses }
        DataPlanePairingDesired { _docID peer_id template replicator_addresses }
        PeerPairingApplied { _docID peer_id collections replicator_addresses replicator_filter }
    }"#;
    for node in fleet {
        match graphql_query(&node.graphql, query).await {
            Ok(response) => {
                let data = response.get("data").unwrap_or(&response);
                eprintln!(
                    "\n##### DOC STATE {} (peer={}) #####\n{}",
                    node.agent_did,
                    node.peer_id,
                    serde_json::to_string_pretty(data).unwrap_or_default()
                );
            }
            Err(error) => eprintln!("(doc-state query failed for {}: {error})", node.agent_did),
        }
    }
}

async fn wait_for_subscription_replicator_installed(
    graphql: &str,
    peer_id: &str,
    required_collection: &str,
    timeout: Duration,
) -> Result<()> {
    let api_base = graphql
        .strip_suffix("/graphql")
        .with_context(|| format!("unexpected GraphQL endpoint shape: {graphql}"))?;
    let collection_versions_url = format!("{api_base}/collections/versions");
    let replicators_url = format!("{api_base}/p2p/replicators");
    let deadline = Instant::now() + timeout;
    let mut last_collection_versions = Value::Null;
    let mut last_replicators = Value::Null;
    loop {
        let fetched = async {
            let collection_versions = p2p_http_client()?
                .get(&collection_versions_url)
                .send()
                .await
                .with_context(|| {
                    format!("fetching collection versions from {collection_versions_url}")
                })?
                .error_for_status()
                .with_context(|| {
                    format!("collection versions returned an error from {collection_versions_url}")
                })?
                .json::<Vec<Value>>()
                .await
                .with_context(|| {
                    format!("decoding collection versions from {collection_versions_url}")
                })?;
            let replicators = p2p_http_client()?
                .get(&replicators_url)
                .send()
                .await
                .with_context(|| format!("fetching effective replicators from {replicators_url}"))?
                .error_for_status()
                .with_context(|| {
                    format!("effective replicators returned an error from {replicators_url}")
                })?
                .json::<Vec<Value>>()
                .await
                .with_context(|| {
                    format!("decoding effective replicators from {replicators_url}")
                })?;
            Ok::<_, anyhow::Error>((collection_versions, replicators))
        }
        .await;
        let last_error = match fetched {
            Ok((collection_versions, replicators)) => {
                last_collection_versions = Value::Array(collection_versions.clone());
                last_replicators = Value::Array(replicators.clone());
                if let Some(collection_id) =
                    collection_id_from_versions(&collection_versions, required_collection)
                {
                    if effective_replicator_has_collection(&replicators, peer_id, collection_id) {
                        return Ok(());
                    }
                }
                "effective replicator did not yet contain the required collection".to_string()
            }
            Err(error) => error.to_string(),
        };
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for effective replicator scope containing {required_collection} on edge peer={peer_id} (graphql={graphql}); last error: {last_error}; last collection versions: {last_collection_versions}; last replicators: {last_replicators}"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn collection_id_from_versions<'a>(versions: &'a [Value], collection: &str) -> Option<&'a str> {
    versions.iter().find_map(|version| {
        let name = version
            .get("Name")
            .or_else(|| version.get("name"))
            .and_then(Value::as_str)?;
        (name == collection).then(|| {
            version
                .get("CollectionID")
                .or_else(|| version.get("collection_id"))
                .and_then(Value::as_str)
        })?
    })
}

fn effective_replicator_has_collection(
    replicators: &[Value],
    peer_id: &str,
    required_collection_id: &str,
) -> bool {
    replicators.iter().any(|row| {
        row.get("ID").and_then(Value::as_str) == Some(peer_id)
            && row
                .get("CollectionIDs")
                .and_then(Value::as_array)
                .is_some_and(|ids| {
                    ids.iter()
                        .any(|id| id.as_str() == Some(required_collection_id))
                })
    })
}

async fn wait_for_conversation_replicator_installed(
    graphql: &str,
    peer_id: &str,
    local_did: &str,
    timeout: Duration,
) -> Result<()> {
    let escaped = escape_graphql_string(peer_id);
    let query = format!(
        r#"{{ PeerPairingApplied(filter: {{ peer_id: {{ _eq: "{escaped}" }} }}) {{ peer_id replicator_addresses replicator_filter }} }}"#
    );
    let deadline = Instant::now() + timeout;
    let mut last = Value::Null;
    loop {
        let response = graphql_query(graphql, &query).await?;
        if let Some(rows) = response
            .pointer("/data/PeerPairingApplied")
            .and_then(Value::as_array)
        {
            last = Value::Array(rows.clone());
            if rows.iter().any(|row| {
                let has_address = row
                    .get("replicator_addresses")
                    .and_then(Value::as_array)
                    .is_some_and(|addresses| {
                        addresses
                            .iter()
                            .any(|address| address.as_str().is_some_and(|value| !value.is_empty()))
                    });
                let has_conversation_filter = row
                    .get("replicator_filter")
                    .and_then(Value::as_str)
                    .is_some_and(|filter| conversation_filter_matches(filter, local_did));
                has_address && has_conversation_filter
            }) {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for conversation replicator filtered to AgentRequest={local_did} on edge peer={peer_id} (graphql={graphql}); last PeerPairingApplied rows: {last}"
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn conversation_filter_matches(filter: &str, local_did: &str) -> bool {
    serde_json::from_str::<Value>(filter)
        .ok()
        .and_then(|value| {
            value
                .get("AgentRequest")
                .and_then(|entry| entry.get("value"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .as_deref()
        == Some(local_did)
}

#[test]
fn conversation_pairing_fence_requires_the_local_agent_request_scope() {
    let filter = r#"{"AgentRequest":{"operator":"_eq","value":"did:key:local"}}"#;
    assert!(conversation_filter_matches(filter, "did:key:local"));
    assert!(!conversation_filter_matches(filter, "did:key:other"));
    assert!(!conversation_filter_matches("not-json", "did:key:local"));
}

#[test]
fn control_plane_fence_reads_effective_replicator_scope() {
    let versions = vec![serde_json::json!({
        "CollectionID": "bafy-agent-network",
        "Name": "AgentNetwork"
    })];
    let replicators = vec![serde_json::json!({
        "ID": "peer-b",
        "Addresses": ["endpoint-b"],
        "CollectionIDs": ["bafy-agent-network"]
    })];

    let collection_id = collection_id_from_versions(&versions, "AgentNetwork").unwrap();
    assert!(effective_replicator_has_collection(
        &replicators,
        "peer-b",
        collection_id
    ));
    assert!(!effective_replicator_has_collection(
        &replicators,
        "peer-c",
        collection_id
    ));
}

fn persist_fleet_logs(fleet: &[FleetNode], suffix: &str) {
    for (idx, node) in fleet.iter().enumerate() {
        if let Ok((stdout, stderr)) = fleet_node_captured_output(node) {
            let label = if idx == 0 {
                "coordinator".to_string()
            } else {
                format!("subagent-{idx}")
            };
            let path = format!("/tmp/fleet_{label}_{suffix}.log");
            let _ = std::fs::write(
                &path,
                format!(
                    "# {} peer={}\n=== STDOUT ===\n{stdout}\n=== STDERR ===\n{stderr}\n",
                    node.agent_did, node.peer_id
                ),
            );
            eprintln!("wrote {path} ({} stderr bytes)", stderr.len());
        }
    }
}

fn dump_fleet_logs(fleet: &[FleetNode]) {
    let tail = |text: &str| {
        let lines: Vec<&str> = text.lines().collect();
        let start = lines.len().saturating_sub(120);
        lines[start..].join("\n")
    };
    for node in fleet {
        match fleet_node_captured_output(node) {
            Ok((stdout, stderr)) => {
                eprintln!(
                    "\n===== {} ({}) stderr tail =====\n{}",
                    node.agent_did,
                    node.graphql,
                    tail(&stderr)
                );
                if !stdout.trim().is_empty() {
                    eprintln!(
                        "----- {} stdout tail -----\n{}",
                        node.agent_did,
                        tail(&stdout)
                    );
                }
            }
            Err(error) => eprintln!("(could not read logs for {}: {error})", node.agent_did),
        }
    }
}

async fn assert_no_subagent_data_plane_edges(subagents: &[FleetNode]) -> Result<()> {
    let sub_peer_ids = subagents
        .iter()
        .map(|node| node.peer_id.as_str())
        .collect::<HashSet<_>>();
    for node in subagents {
        let response = graphql_query(
            &node.graphql,
            r#"{ DataPlanePairingDesired { peer_id agent_did template } }"#,
        )
        .await?;
        let rows = response
            .pointer("/data/DataPlanePairingDesired")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for row in rows {
            let peer_id = row.get("peer_id").and_then(Value::as_str).unwrap_or("");
            anyhow::ensure!(
                !sub_peer_ids.contains(peer_id),
                "subagent {} unexpectedly has a data-plane edge to another subagent: {row}",
                node.agent_did
            );
        }
    }
    Ok(())
}

async fn configure_fleet_behaviors(
    root: &Path,
    coord: &FleetNode,
    subagents: &[FleetNode],
) -> Result<()> {
    configure_fleet_behaviors_with_coordinator_prompt(
        root,
        coord,
        subagents,
        COORDINATOR_SYSTEM_PROMPT,
        false,
    )
    .await
}

async fn configure_fleet_behaviors_with_coordinator_prompt(
    root: &Path,
    coord: &FleetNode,
    subagents: &[FleetNode],
    coordinator_prompt: &str,
    steering_enabled: bool,
) -> Result<()> {
    let coord_prompt = root.join("coordinator-system-prompt.txt");
    fs::write(&coord_prompt, coordinator_prompt)?;
    configure_behavior_prompt(coord, &coord_prompt, "Fleet Coordinator")?;

    let sub_prompt = root.join("subagent-system-prompt.txt");
    fs::write(&sub_prompt, SUBAGENT_SYSTEM_PROMPT)?;
    for (index, subagent) in subagents.iter().enumerate() {
        configure_behavior_prompt(
            subagent,
            &sub_prompt,
            &format!("Fleet Researcher {}", index + 1),
        )?;
        configure_subagent_target_gate(subagent)?;
    }
    configure_coordinator_targets_with_steering(coord, subagents, steering_enabled)?;
    Ok(())
}

fn configure_behavior_prompt(
    node: &FleetNode,
    prompt_path: &Path,
    display_name: &str,
) -> Result<()> {
    configure_behavior_prompt_with_model(node, prompt_path, display_name, &node.model_name)
}

fn configure_behavior_prompt_with_model(
    node: &FleetNode,
    prompt_path: &Path,
    display_name: &str,
    model_name: &str,
) -> Result<()> {
    run_cli_json(
        &node.home,
        &[
            "config",
            "behavior",
            "set",
            "--graphql",
            &node.graphql,
            "--agent-did",
            &node.agent_did,
            "--behavior-id",
            &node.behavior_id,
            "--display-name",
            display_name,
            "--system-prompt-file",
            prompt_path
                .to_str()
                .ok_or_else(|| anyhow!("system prompt path is not UTF-8"))?,
            "--backend-id",
            &node.backend_id,
            "--model-name",
            model_name,
            "--tool-selection-id",
            &node.tool_selection_id,
            "--inference-profile-id",
            &node.inference_profile_id,
        ],
    )?;
    Ok(())
}

fn configure_subagent_target_gate(node: &FleetNode) -> Result<()> {
    run_cli_json(
        &node.home,
        &[
            "config",
            "tools",
            "set",
            "--graphql",
            &node.graphql,
            "--agent-did",
            &node.agent_did,
            "--selection-id",
            &node.tool_selection_id,
            "--display-name",
            "Fleet Researcher Tools",
            "--clear-subagent-targets",
            "--subagent-spawn-enabled",
            "false",
            "--subagent-background-enabled",
            "false",
            "--subagent-allow-cross-deployment",
            "true",
            "--enable-meta-tools",
            "false",
            "--enable-defra-query",
            "false",
        ],
    )?;
    Ok(())
}

fn configure_coordinator_targets_with_steering(
    coord: &FleetNode,
    subagents: &[FleetNode],
    steering_enabled: bool,
) -> Result<()> {
    let mut args = vec![
        "config".to_string(),
        "tools".to_string(),
        "set".to_string(),
        "--graphql".to_string(),
        coord.graphql.clone(),
        "--agent-did".to_string(),
        coord.agent_did.clone(),
        "--selection-id".to_string(),
        coord.tool_selection_id.clone(),
        "--display-name".to_string(),
        "Fleet Coordinator Tools".to_string(),
        "--subagent-spawn-enabled".to_string(),
        "true".to_string(),
        "--subagent-background-enabled".to_string(),
        "true".to_string(),
        "--subagent-steering-enabled".to_string(),
        steering_enabled.to_string(),
        "--subagent-allow-cross-deployment".to_string(),
        "true".to_string(),
        "--cross-deployment-spawn-timeout-seconds".to_string(),
        "180".to_string(),
        "--enable-meta-tools".to_string(),
        "false".to_string(),
        "--enable-defra-query".to_string(),
        "false".to_string(),
    ];
    for (index, subagent) in subagents.iter().enumerate() {
        args.push("--subagent-target".to_string());
        args.push(subagent_target_entry(
            &format!("researcher-{}", index + 1),
            &subagent.agent_did,
            &subagent.behavior_id,
            Some(format!("Remote fleet researcher {}", index + 1)),
        ));
    }
    let refs = args.iter().map(String::as_str).collect::<Vec<_>>();
    run_cli_json(&coord.home, &refs)?;
    Ok(())
}

async fn wait_for_all_subagent_children_completed(
    coord_graphql: &str,
    subagents: &[FleetNode],
    parent_session_id: &str,
    parent_request_id: &str,
    allowed_foreground_target: Option<&str>,
    timeout: Duration,
) -> Result<HashMap<String, CompletedChild>> {
    wait_until_value(timeout, || async {
        let bridges = fetch_spawn_bridges(coord_graphql, parent_session_id).await?;
        anyhow::ensure!(
            bridges.len() >= subagents.len(),
            "saw {} spawn bridges, expected at least {}",
            bridges.len(),
            subagents.len()
        );

        let mut completed_by_owner = HashMap::new();
        let mut pending = Vec::new();
        for bridge in &bridges {
            match bridge.await_mode.as_deref() {
                Some("background") => {}
                Some("foreground")
                    if allowed_foreground_target == Some(bridge.target_name.as_str()) => {}
                Some("foreground") => {
                    return Err(fatal_fleet_invariant(format!(
                        "bridge {} for target {:?} was unexpectedly foregrounded; only {allowed_foreground_target:?} may foreground: {bridge:?}",
                        bridge.tool_call_id, bridge.target_name,
                    )));
                }
                _ => {
                    return Err(fatal_fleet_invariant(format!(
                        "bridge {} has an invalid await mode: {bridge:?}",
                        bridge.tool_call_id
                    )));
                }
            }
            if bridge.lifecycle_state == "failed" {
                return Err(fatal_fleet_invariant(format!(
                    "bridge {} failed before child completion: {bridge:?}",
                    bridge.tool_call_id
                )));
            }

            let Some((owner, child)) =
                find_child_on_any_subagent(subagents, &bridge.child_request_id).await?
            else {
                pending.push(format!(
                    "child {} from bridge {} not materialized",
                    bridge.child_request_id, bridge.tool_call_id
                ));
                continue;
            };

            assert_child_lineage(&child, owner, bridge, parent_request_id).map_err(|error| {
                fatal_fleet_invariant(format!(
                    "child {} violated release lineage: {error:#}",
                    child.request_id
                ))
            })?;

            let child_state = child
                .lifecycle_state
                .clone()
                .or(fetch_request_lifecycle(&owner.graphql, &child.request_id).await?)
                .unwrap_or_else(|| "unknown".to_string());
            if child_state != "completed" {
                pending.push(format!(
                    "child {} on {} not completed yet: {child_state}",
                    child.request_id, owner.agent_did
                ));
                continue;
            }

            if bridge.lifecycle_state != "completed" {
                pending.push(format!(
                    "bridge {} for child {} not completed yet: {}",
                    bridge.tool_call_id, child.request_id, bridge.lifecycle_state
                ));
                continue;
            }

            let owner_answer = fetch_assistant_answer(&owner.graphql, &child.request_id).await?;
            if owner_answer.trim().is_empty() {
                pending.push(format!(
                    "child {} on {} has no owner-side assistant answer yet",
                    child.request_id, owner.agent_did
                ));
                continue;
            }
            let coordinator_answer = fetch_assistant_answer(coord_graphql, &child.request_id).await?;
            if coordinator_answer.trim().is_empty() {
                pending.push(format!(
                    "child {} on {} has no coordinator-side replicated answer yet",
                    child.request_id, owner.agent_did
                ));
                continue;
            }

            completed_by_owner.insert(
                owner.agent_did.clone(),
                CompletedChild {
                    tool_call_id: bridge.tool_call_id.clone(),
                    child_request_id: child.request_id.clone(),
                    child_session_id: child.session_id.clone(),
                    owner_agent_did: owner.agent_did.clone(),
                    owner_behavior_id: owner.behavior_id.clone(),
                    owner_answer,
                    coordinator_answer,
                },
            );
        }

        let expected = subagents
            .iter()
            .map(|node| node.agent_did.as_str())
            .collect::<HashSet<_>>();
        let seen = completed_by_owner
            .keys()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let missing = expected.difference(&seen).copied().collect::<Vec<_>>();
        anyhow::ensure!(
            missing.is_empty(),
            "completed children missing subagent owners {missing:?}; pending: {pending:?}; completed owners: {:?}",
            completed_by_owner.keys().collect::<Vec<_>>()
        );

        Ok(completed_by_owner)
    })
    .await
}

fn assert_child_lineage(
    child: &ChildRow,
    owner: &FleetNode,
    bridge: &BridgeRow,
    parent_request_id: &str,
) -> Result<()> {
    anyhow::ensure!(
        child.request_id == bridge.child_request_id,
        "child request id does not match its bridge: child={child:?} bridge={bridge:?}"
    );
    anyhow::ensure!(
        child.agent_did == owner.agent_did && child.behavior_id == owner.behavior_id,
        "child ownership does not match its materializing node: child={child:?} owner={} behavior={}",
        owner.agent_did,
        owner.behavior_id
    );
    anyhow::ensure!(
        child.caused_by_parent_request_id.as_deref() == Some(parent_request_id)
            && child.caused_by_parent_tool_call_id.as_deref()
                == Some(bridge.tool_call_id.as_str())
            && child.caused_by_trigger_kind.as_deref() == Some("subagent"),
        "child lineage does not match its parent bridge: child={child:?} bridge={bridge:?} parent={parent_request_id}"
    );
    anyhow::ensure!(
        !matches!(
            child.lifecycle_state.as_deref(),
            Some("failed" | "dead" | "interrupted" | "superseded")
        ),
        "child {} reached non-completed terminal state when observed: {child:?}",
        child.request_id
    );
    Ok(())
}

async fn fetch_spawn_bridges(graphql: &str, session_id: &str) -> Result<Vec<BridgeRow>> {
    let session_id = escape_graphql_string(session_id);
    let response = graphql_query(
        graphql,
        &format!(
            r#"{{
                AgentToolCall(
                    filter: {{
                        session_id: {{ _eq: "{session_id}" }},
                        tool_name: {{ _eq: "spawn_subagent" }}
                    }},
                    order: {{ started_at: ASC }}
                ) {{
                    tool_call_id
                    lifecycle_state
                    child_request_id
                    await_mode
                    args
                }}
            }}"#
        ),
    )
    .await?;
    let rows = response
        .pointer("/data/AgentToolCall")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let child_request_id = row
                .get("child_request_id")
                .and_then(Value::as_str)?
                .trim()
                .to_string();
            if child_request_id.is_empty() {
                return None;
            }
            let target_name = row
                .get("args")
                .and_then(Value::as_str)
                .and_then(|args| serde_json::from_str::<Value>(args).ok())
                .and_then(|args| {
                    args.get("name")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .unwrap_or_default();
            Some(BridgeRow {
                tool_call_id: row
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                lifecycle_state: row
                    .get("lifecycle_state")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                child_request_id,
                await_mode: row
                    .get("await_mode")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                target_name,
            })
        })
        .collect())
}

async fn find_child_on_any_subagent<'a>(
    subagents: &'a [FleetNode],
    request_id: &str,
) -> Result<Option<(&'a FleetNode, ChildRow)>> {
    for subagent in subagents {
        if let Some(row) = fetch_child_request(&subagent.graphql, request_id).await? {
            return Ok(Some((subagent, row)));
        }
    }
    Ok(None)
}

async fn fetch_child_request(graphql: &str, request_id: &str) -> Result<Option<ChildRow>> {
    let request_id = escape_graphql_string(request_id);
    let response = graphql_query(
        graphql,
        &format!(
            r#"{{
                AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                    request_id
                    session_id
                    agent_did
                    behavior_id
                    lifecycle_state
                    caused_by_parent_request_id
                    caused_by_parent_tool_call_id
                    caused_by_trigger_kind
                }}
            }}"#
        ),
    )
    .await?;
    Ok(response
        .pointer("/data/AgentRequest")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .map(|row| ChildRow {
            request_id: row
                .get("request_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            session_id: row
                .get("session_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            agent_did: row
                .get("agent_did")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            behavior_id: row
                .get("behavior_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            lifecycle_state: row
                .get("lifecycle_state")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            caused_by_parent_request_id: row
                .get("caused_by_parent_request_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            caused_by_parent_tool_call_id: row
                .get("caused_by_parent_tool_call_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            caused_by_trigger_kind: row
                .get("caused_by_trigger_kind")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        }))
}

async fn wait_for_request_terminal(
    graphql: &str,
    request_id: &str,
    timeout: Duration,
) -> Result<String> {
    wait_until_value(timeout, || async {
        let state = fetch_request_lifecycle(graphql, request_id)
            .await?
            .with_context(|| format!("AgentRequest({request_id}) not found"))?;
        anyhow::ensure!(
            is_terminal(&state),
            "AgentRequest({request_id}) not terminal yet: {state}"
        );
        Ok(state)
    })
    .await
}

async fn fetch_request_lifecycle(graphql: &str, request_id: &str) -> Result<Option<String>> {
    let request_id = escape_graphql_string(request_id);
    let response = graphql_query(
        graphql,
        &format!(
            r#"{{
                AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                    lifecycle_state
                }}
            }}"#
        ),
    )
    .await?;
    Ok(response
        .pointer("/data/AgentRequest")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("lifecycle_state"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned))
}

async fn wait_for_assistant_answer(
    graphql: &str,
    request_id: &str,
    timeout: Duration,
) -> Result<String> {
    wait_until_value(timeout, || async {
        let answer = fetch_assistant_answer(graphql, request_id).await?;
        anyhow::ensure!(
            !answer.trim().is_empty(),
            "AgentResponse({request_id}) is empty"
        );
        Ok(answer)
    })
    .await
}

async fn fetch_assistant_answer(graphql: &str, request_id: &str) -> Result<String> {
    let escaped = escape_graphql_string(request_id);
    let response = graphql_query(
        graphql,
        &format!(
            r#"{{
                AgentResponse(filter: {{ request_id: {{ _eq: "{escaped}" }} }}, limit: 1) {{
                    content
                    session_id
                }}
            }}"#
        ),
    )
    .await?;
    if let Some(row) = response
        .pointer("/data/AgentResponse")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
    {
        if let Some(content) = row
            .get("content")
            .and_then(Value::as_str)
            .filter(|content| !content.trim().is_empty())
        {
            return Ok(content.to_string());
        }
        if let Some(session_id) = row.get("session_id").and_then(Value::as_str) {
            return fetch_latest_assistant_message(graphql, session_id).await;
        }
    }
    Ok(String::new())
}

async fn fetch_latest_assistant_message(graphql: &str, session_id: &str) -> Result<String> {
    let session_id = escape_graphql_string(session_id);
    let response = graphql_query(
        graphql,
        &format!(
            r#"{{
                AgentMessage(
                    filter: {{ session_id: {{ _eq: "{session_id}" }}, role: {{ _eq: "assistant" }} }},
                    order: {{ sequence: DESC }},
                    limit: 1
                ) {{ content }}
            }}"#
        ),
    )
    .await?;
    Ok(response
        .pointer("/data/AgentMessage")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string())
}

async fn assert_subagent_store_scopes(
    coord: &FleetNode,
    subagents: &[FleetNode],
    completed_children: &HashMap<String, CompletedChild>,
) -> Result<()> {
    for subagent in subagents {
        let completed = completed_children
            .get(&subagent.agent_did)
            .with_context(|| format!("missing completed child for {}", subagent.agent_did))?;
        assert_eq!(completed.owner_agent_did, subagent.agent_did);
        assert_eq!(completed.owner_behavior_id, subagent.behavior_id);
        anyhow::ensure!(
            !completed.owner_answer.trim().is_empty()
                && !completed.coordinator_answer.trim().is_empty(),
            "completed child {} has empty answer(s): {completed:?}",
            completed.child_request_id
        );
        let local_child = fetch_child_request(&subagent.graphql, &completed.child_request_id)
            .await?
            .with_context(|| {
                format!(
                    "subagent {} missing its completed child {} from bridge {}",
                    subagent.agent_did, completed.child_request_id, completed.tool_call_id
                )
            })?;
        assert_eq!(local_child.agent_did, subagent.agent_did);
        assert_eq!(local_child.behavior_id, subagent.behavior_id);
        assert_eq!(local_child.lifecycle_state.as_deref(), Some("completed"));

        let allowed = HashSet::from([coord.agent_did.as_str(), subagent.agent_did.as_str()]);
        for collection in CONVERSATION_COLLECTIONS {
            let agent_dids = fetch_collection_agent_dids(&subagent.graphql, collection).await?;
            let unexpected = agent_dids
                .iter()
                .filter(|did| {
                    let did = did.trim();
                    did.is_empty() || !allowed.contains(did)
                })
                .cloned()
                .collect::<Vec<_>>();
            anyhow::ensure!(
                unexpected.is_empty(),
                "subagent {} store leaked unexpected agent_did values in {collection}: {:?}; allowed: {:?}",
                subagent.agent_did,
                unexpected,
                allowed
            );
        }
    }
    Ok(())
}

async fn fetch_collection_agent_dids(graphql: &str, collection: &str) -> Result<Vec<String>> {
    let response = graphql_query(graphql, &format!("{{ {collection} {{ agent_did }} }}")).await?;
    Ok(response
        .pointer(&format!("/data/{collection}"))
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    row.get("agent_did")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_default())
}

async fn assert_subagents_have_no_spawn_targets(subagents: &[FleetNode]) -> Result<()> {
    for subagent in subagents {
        let selection_id = escape_graphql_string(&subagent.tool_selection_id);
        let response = graphql_query(
            &subagent.graphql,
            &format!(
                r#"{{
                    ToolSelection(filter: {{ selection_id: {{ _eq: "{selection_id}" }} }}, limit: 1) {{
                        subagent_targets
                        subagent_spawn_enabled
                        subagent_allow_cross_deployment
                    }}
                }}"#
            ),
        )
        .await?;
        let row = first_graphql_row(&response, "ToolSelection")?;
        let target_count = row
            .get("subagent_targets")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or_default();
        anyhow::ensure!(
            target_count == 0,
            "subagent {} should not have onward targets: {row}",
            subagent.agent_did
        );
        anyhow::ensure!(
            row.get("subagent_spawn_enabled").and_then(Value::as_bool) == Some(false),
            "subagent {} should have spawn disabled: {row}",
            subagent.agent_did
        );
    }
    Ok(())
}

fn is_terminal(state: &str) -> bool {
    matches!(
        state,
        "completed" | "failed" | "dead" | "interrupted" | "superseded"
    )
}

async fn assert_endpoint_reachable(endpoint: &str) -> Result<()> {
    let url = format!("{}/models", endpoint.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("building live endpoint probe client")?;
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("probing live endpoint {url}"))?;
    anyhow::ensure!(
        response.status().is_success(),
        "live endpoint {url} returned {}",
        response.status()
    );
    Ok(())
}

async fn wait_until_value<T, F, Fut>(timeout: Duration, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let deadline = Instant::now() + timeout;
    let mut last_error = "condition not evaluated".to_string();
    loop {
        if Instant::now() >= deadline {
            bail!("timed out after {:?}: {last_error}", timeout);
        }
        match f().await {
            Ok(value) => return Ok(value),
            Err(error) if error.downcast_ref::<FatalFleetInvariant>().is_some() => {
                return Err(error);
            }
            Err(error) => last_error = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
