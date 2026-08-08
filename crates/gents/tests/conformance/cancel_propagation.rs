use std::sync::Arc;
use std::time::{Duration, Instant};

use gents::agent::p2p_reconcile::{
    resolve_template, EmbeddedRemoteP2pAdmin, FilterPredicate, PairingFilters, RemoteP2pAdmin,
};
use gents::background_completion::{observe_cancel_cascade_ack, CancelAckOutcome};
use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::tool_call_lifecycle::{
    AwaitMode, CancelCause, CancelPolicy, CascadeDispatch, ToolCallLifecycle,
};
use gents::{
    default_behavior_id_for_agent, AgentIdentity, DocumentRuntimeOptions, Gents, ToolCeiling,
};
use serde::Deserialize;
use serde_json::json;

use crate::lean_vocab_test::lean_cancel_propagation_cases;
use crate::support::fixtures::{bind_default_behavior_backend, test_identity};
use crate::support::interrupt::{wait_for_runtime_ready, BootedAgent};
use crate::support::mock_endpoint::MockModelEndpoint;
use crate::support::{first_optional_row, test_p2p_db_with_identity, TestDb};

struct RunningAgent {
    db: TestDb,
    booted: BootedAgent,
    _endpoint: MockModelEndpoint,
}

#[derive(Debug, Deserialize)]
struct RequestRow {
    agent_did: String,
    lifecycle_state: Option<String>,
    interrupt_requested_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BridgeRow {
    lifecycle_state: Option<String>,
    child_request_id: Option<String>,
    spawn_target_did: Option<String>,
    cancel_cascade_intent_at: Option<String>,
    cancel_pending_remote_ack: Option<bool>,
}

pub(super) async fn cancel_propagation_cases_drive_production_interrupt() {
    let cases = lean_cancel_propagation_cases();
    assert_eq!(
        cases.len(),
        1,
        "Lean should emit one cancel propagation row"
    );

    let case = &cases[0];
    assert_eq!(
        case.name,
        "cancel_propagates_across_declarative_subagent_legs"
    );
    assert_eq!(case.route, "declarative_subagent_pairing");
    assert_eq!(case.action, "cancel_parent");
    assert_eq!(case.parent_deployment, "coordinator");
    assert_eq!(case.child_deployment, "host");
    assert_eq!(case.bridge_collection, "AgentToolCall");
    assert_eq!(case.child_request_collection, "AgentRequest");
    assert!(case.cancel_intent_written_on_bridge);
    assert!(case.bridge_cancel_replicates_to_host);
    assert!(case.host_interrupts_child);
    assert!(case.child_terminal_replicates_to_coordinator);
    assert!(case.cancel_ack_returns_to_coordinator);
    assert!(case.no_third_party_rows);

    drive_declarative_cancel_propagation().await;
}

async fn drive_declarative_cancel_propagation() {
    let coord_identity: Arc<dyn AgentIdentity> =
        Arc::new(test_identity("cancel-propagation-coord"));
    let host_identity: Arc<dyn AgentIdentity> = Arc::new(test_identity("cancel-propagation-host"));
    let coord_did = coord_identity.did().to_string();
    let host_did = host_identity.did().to_string();
    let coord_behavior_id = default_behavior_id_for_agent(&coord_did);
    let host_behavior_id = default_behavior_id_for_agent(&host_did);

    let coord_db =
        test_p2p_db_with_identity("cancel-propagation-coord", coord_identity.clone()).await;
    let host_db = test_p2p_db_with_identity("cancel-propagation-host", host_identity.clone()).await;
    let coord_addr = wait_for_listen_addr(coord_db.node.as_ref()).await;
    let host_addr = wait_for_listen_addr(host_db.node.as_ref()).await;

    write_pairing(
        coord_db.node.as_ref(),
        "cancel-host",
        &host_did,
        "subagent-coordinator",
        &host_addr,
    )
    .await;
    write_pairing(
        host_db.node.as_ref(),
        "cancel-coord",
        &coord_did,
        "subagent-host",
        &coord_addr,
    )
    .await;

    let host = boot_agent(host_db, host_identity, "cancel-propagation-host").await;
    let coord = boot_agent(coord_db, coord_identity, "cancel-propagation-coord").await;

    wait_for_replicator_installed(
        coord.db.node.as_ref(),
        &coord_did,
        "cancel-host",
        Duration::from_secs(180),
    )
    .await;
    wait_for_replicator_installed(
        host.db.node.as_ref(),
        &host_did,
        "cancel-coord",
        Duration::from_secs(180),
    )
    .await;
    wait_for_connected_peer(coord.db.node.as_ref(), Duration::from_secs(60)).await;
    wait_for_connected_peer(host.db.node.as_ref(), Duration::from_secs(60)).await;
    let RunningAgent {
        db: coord_db,
        booted: coord_booted,
        _endpoint: _coord_endpoint,
    } = coord;
    coord_booted.shutdown().await;
    let coord_node = coord_db.node.clone();

    let parent_request_id = "cancel-propagation-parent";
    let parent_session_id = "cancel-propagation-parent-session";
    let parent_tool_call_id = "cancel-propagation-bridge";
    let child_request_id = "cancel-propagation-child";
    create_processing_request(
        coord_node.as_ref(),
        parent_request_id,
        parent_session_id,
        &coord_did,
        &coord_behavior_id,
        "parent work",
        0,
        None,
        None,
        None,
    )
    .await;

    create_processing_request(
        host.db.node.as_ref(),
        child_request_id,
        "cancel-propagation-child-session",
        &host_did,
        &host_behavior_id,
        "child work",
        1,
        Some(parent_request_id),
        Some(parent_tool_call_id),
        Some(&coord_did),
    )
    .await;

    let mut bridge = ToolCallLifecycle::new_subagent(
        coord_node.clone(),
        parent_request_id.to_string(),
        parent_session_id.to_string(),
        coord_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        json!({
            "agent_did": host_did.as_str(),
            "behavior_id": host_behavior_id.as_str(),
            "prompt": "child work",
            "await_mode": AwaitMode::Background.as_str(),
        })
        .to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        host_did.clone(),
    );
    bridge.start_running().await.expect("persist bridge");

    let replicated_bridge = replay_and_wait_for_bridge(
        host.db.node.as_ref(),
        coord_node.clone(),
        &host_addr,
        &host_did,
        parent_session_id,
        parent_tool_call_id,
    )
    .await;
    assert_eq!(
        replicated_bridge.spawn_target_did.as_deref(),
        Some(host_did.as_str())
    );
    assert_eq!(
        replicated_bridge.child_request_id.as_deref(),
        Some(child_request_id)
    );
    assert!(
        fetch_request(host.db.node.as_ref(), parent_request_id)
            .await
            .is_none(),
        "coordinator parent request must not replicate to the host"
    );
    let coord_bridge_before_cancel = wait_for_bridge(
        coord_node.as_ref(),
        parent_session_id,
        parent_tool_call_id,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        coord_bridge_before_cancel.lifecycle_state.as_deref(),
        Some("running")
    );

    let dispatch = bridge
        .cancel_during_run_with_cascade_dispatch(CancelCause::Interrupted, &coord_did)
        .await
        .expect("cancel bridge with remote cascade dispatch");
    let coord_bridge_after_cancel =
        fetch_bridge(coord_node.as_ref(), parent_session_id, parent_tool_call_id).await;
    assert!(
        matches!(dispatch, Some(CascadeDispatch::RemoteIntentWritten)),
        "expected remote cascade dispatch, got {dispatch:?}; before={coord_bridge_before_cancel:?}; after={coord_bridge_after_cancel:?}"
    );

    let host_bridge = wait_for_bridge_cancel_intent(
        host.db.node.as_ref(),
        parent_session_id,
        parent_tool_call_id,
        Duration::from_secs(120),
    )
    .await;
    assert_eq!(host_bridge.lifecycle_state.as_deref(), Some("cancelled"));
    assert!(host_bridge.cancel_cascade_intent_at.is_some());
    assert_eq!(host_bridge.cancel_pending_remote_ack, Some(true));

    let interrupted_on_host = wait_for_interrupt_requested_at(
        host.db.node.as_ref(),
        child_request_id,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(interrupted_on_host.agent_did, host_did);
    assert_eq!(
        interrupted_on_host.lifecycle_state.as_deref(),
        Some("processing")
    );

    let interrupted_on_coord = wait_for_interrupt_requested_at(
        coord_node.as_ref(),
        child_request_id,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(interrupted_on_coord.agent_did, host_did);

    let ack_outcomes = observe_cancel_cascade_ack(coord_node.clone(), &coord_did)
        .await
        .expect("observe cancel ack on coordinator");
    assert!(
        ack_outcomes.iter().any(|outcome| matches!(
            outcome,
            CancelAckOutcome::Acked {
                parent_tool_call_id: acked_tool_call_id
            } if acked_tool_call_id == parent_tool_call_id
        )),
        "expected cancel ack for {parent_tool_call_id}, got {ack_outcomes:?}"
    );
    let acked_bridge = wait_for_bridge_ack_cleared(
        coord_node.as_ref(),
        parent_session_id,
        parent_tool_call_id,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(acked_bridge.lifecycle_state.as_deref(), Some("cancelled"));
    assert_eq!(acked_bridge.cancel_pending_remote_ack, Some(false));

    assert_no_third_party_rows(coord_node.as_ref(), &coord_did, &host_did).await;
    assert_no_third_party_rows(host.db.node.as_ref(), &coord_did, &host_did).await;

    host.booted.shutdown().await;
    coord_db.node.shutdown().await;
    host.db.node.shutdown().await;
}

async fn boot_agent(db: TestDb, identity: Arc<dyn AgentIdentity>, name: &str) -> RunningAgent {
    let did = identity.did().to_string();
    let endpoint = MockModelEndpoint::start("default").expect("mock endpoint");
    bind_default_behavior_backend(
        db.node.as_ref(),
        &did,
        &format!("{name}-backend"),
        endpoint.endpoint(),
    )
    .await;
    let agent = Gents::from_default_behavior_documents(
        db.node.clone(),
        identity,
        DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::meta_only(),
            ..Default::default()
        },
    )
    .await
    .expect("document agent");
    let did = agent.agent_did().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));
    wait_for_runtime_ready(db.node.as_ref(), &did).await;
    RunningAgent {
        db,
        booted: BootedAgent::new(shutdown_tx, handle, did),
        _endpoint: endpoint,
    }
}

async fn write_pairing(
    node: &EmbeddedNode,
    peer_id: &str,
    peer_did: &str,
    template: &str,
    peer_addr: &str,
) {
    let collections = resolve_template(template)
        .unwrap_or_else(|| panic!("template {template} should resolve"))
        .collections
        .iter()
        .map(|collection| format!("\"{}\"", escape_graphql_string(collection)))
        .collect::<Vec<_>>()
        .join(", ");
    let peer_id = escape_graphql_string(peer_id);
    let peer_did = escape_graphql_string(peer_did);
    let template = escape_graphql_string(template);
    let peer_addr = escape_graphql_string(peer_addr);
    let now = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
    let mutation = format!(
        r#"mutation {{
            upsert_PeerPairingDesired(
                filter: {{ peer_id: {{ _eq: "{peer_id}" }} }},
                add: {{
                    peer_id: "{peer_id}",
                    agent_did: "{peer_did}",
                    collections: [{collections}],
                    replicator_addresses: ["{peer_addr}"],
                    profiles: null,
                    template: "{template}",
                    source: "operator",
                    created_at: "{now}",
                    updated_at: "{now}"
                }},
                update: {{
                    agent_did: "{peer_did}",
                    collections: [{collections}],
                    replicator_addresses: ["{peer_addr}"],
                    profiles: null,
                    template: "{template}",
                    source: "operator",
                    updated_at: "{now}"
                }}
            ) {{ _docID }}
        }}"#
    );
    exec(node, &mutation, "write PeerPairingDesired").await;
}

#[allow(clippy::too_many_arguments)]
async fn create_processing_request(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    agent_did: &str,
    behavior_id: &str,
    content: &str,
    subagent_depth: u32,
    parent_request_id: Option<&str>,
    parent_tool_call_id: Option<&str>,
    requester_did: Option<&str>,
) {
    let request_id = escape_graphql_string(request_id);
    let session_id = escape_graphql_string(session_id);
    let agent_did = escape_graphql_string(agent_did);
    let behavior_id = escape_graphql_string(behavior_id);
    let content = escape_graphql_string(content);
    let created_at = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
    let deadline =
        escape_graphql_string(&(chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339());
    let parent_request = graphql_nullable_string(parent_request_id);
    let parent_tool = graphql_nullable_string(parent_tool_call_id);
    let trigger_id = graphql_nullable_string(parent_tool_call_id);
    let requester_did = graphql_nullable_string(requester_did);
    let trigger_kind = if parent_tool_call_id.is_some() {
        "\"subagent\"".to_string()
    } else {
        "null".to_string()
    };
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "{agent_did}",
                requester_did: {requester_did},
                behavior_id: "{behavior_id}",
                session_id: "{session_id}",
                retry_parent_request: "",
                retry_root_request: "{request_id}",
                superseded_by_request: "",
                content: "{content}",
                status: "processing",
                lifecycle_state: "processing",
                backend_id: "",
                execution_origin: "interactive",
                failure_reason: "",
                created_at: "{created_at}",
                deadline: "{deadline}",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: {subagent_depth},
                caused_by_parent_request_id: {parent_request},
                caused_by_parent_tool_call_id: {parent_tool},
                caused_by_trigger_id: {trigger_id},
                caused_by_trigger_kind: {trigger_kind}
            }}) {{ _docID }}
        }}"#
    );
    exec(node, &mutation, "create processing AgentRequest").await;
}

fn graphql_nullable_string(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("\"{}\"", escape_graphql_string(value)),
        None => "null".to_string(),
    }
}

async fn wait_for_listen_addr(node: &EmbeddedNode) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let addrs = node
            .p2p()
            .expect("p2p should be enabled")
            .listen_addresses()
            .await
            .expect("listen addresses");
        if let Some(addr) = addrs.first() {
            return addr.clone();
        }
        if Instant::now() >= deadline {
            panic!("node never exposed a P2P listen address; last_addrs={addrs:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_connected_peer(node: &EmbeddedNode, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let peers = node
            .p2p()
            .expect("p2p should be enabled")
            .connected_peers()
            .await
            .expect("connected peers");
        if !peers.is_empty() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for a connected P2P peer; last_peers={peers:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_replicator_installed(
    node: &EmbeddedNode,
    agent_did: &str,
    peer_id: &str,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    let escaped_peer_id = escape_graphql_string(peer_id);
    let mut last = String::from("<none>");
    loop {
        let query = format!(
            r#"{{
                PeerPairingApplied(filter: {{ peer_id: {{ _eq: "{escaped_peer_id}" }} }}, limit: 1) {{
                    peer_id
                    collections
                    replicator_addresses
                    replicator_filter
                }}
            }}"#
        );
        let response = node.execute(&query).await;
        if let Some(row) = first_optional_row::<serde_json::Value>(&response, "PeerPairingApplied")
        {
            last = serde_json::to_string(&row).unwrap_or_else(|_| format!("{row:?}"));
            let installed = row
                .get("replicator_addresses")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|addresses| {
                    addresses
                        .iter()
                        .any(|address| address.as_str().is_some_and(|s| !s.trim().is_empty()))
                });
            if installed {
                return;
            }
        }
        if Instant::now() >= deadline {
            let runtime = runtime_diagnostic(node, agent_did).await;
            let p2p = p2p_diagnostic(node).await;
            panic!(
                "timed out waiting for PeerPairingApplied({peer_id}) to install a replicator; \
                 last row={last}; runtime={runtime}; p2p={p2p}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn runtime_diagnostic(node: &EmbeddedNode, agent_did: &str) -> String {
    let agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentRuntime(
                filter: {{ agent_did: {{ _eq: "{agent_did}" }} }},
                limit: 1
            ) {{
                process_state
                reconcile_phase
                active_generation
                router_generation
                last_reconcile_result
                last_reconcile_error
            }}
        }}"#
    );
    let response = match tokio::time::timeout(Duration::from_secs(2), node.execute(&query)).await {
        Ok(response) => response,
        Err(_) => return "<timed out after 2s>".to_string(),
    };
    let data = response
        .data
        .as_ref()
        .map(serde_json::Value::to_string)
        .unwrap_or_else(|| "<none>".to_string());
    format!("data={data}, errors={:?}", response.errors)
}

async fn p2p_diagnostic(node: &EmbeddedNode) -> String {
    let Some(p2p) = node.p2p() else {
        return "<disabled>".to_string();
    };
    let peers = tokio::time::timeout(Duration::from_secs(2), p2p.connected_peers()).await;
    let replicators = tokio::time::timeout(Duration::from_secs(2), p2p.get_replicators()).await;
    format!("connected_peers={peers:?}, replicators={replicators:?}")
}

async fn wait_for_interrupt_requested_at(
    node: &EmbeddedNode,
    request_id: &str,
    timeout: Duration,
) -> RequestRow {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    loop {
        if let Some(row) = fetch_request(node, request_id).await {
            if row
                .interrupt_requested_at
                .as_deref()
                .is_some_and(|s| !s.is_empty())
            {
                return row;
            }
            last = Some(row);
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for AgentRequest({request_id}) interrupt; last={last:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn fetch_request(node: &EmbeddedNode, request_id: &str) -> Option<RequestRow> {
    let request_id = escape_graphql_string(request_id);
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{request_id}" }} }}, limit: 1) {{
                agent_did
                lifecycle_state
                interrupt_requested_at
            }}
        }}"#
    );
    first_optional_row(&node.execute(&query).await, "AgentRequest")
}

async fn wait_for_bridge(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
    timeout: Duration,
) -> BridgeRow {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(row) = fetch_bridge(node, session_id, tool_call_id).await {
            return row;
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for AgentToolCall({session_id}/{tool_call_id})");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn replay_and_wait_for_bridge(
    receiver: &EmbeddedNode,
    sender: Arc<EmbeddedNode>,
    receiver_addr: &str,
    receiver_did: &str,
    session_id: &str,
    tool_call_id: &str,
) -> BridgeRow {
    // The pinned DefraDB selective-CAR path can strand the initial targeted
    // push (#1101). Production pairing recovery repairs that state by
    // reinstalling the replicator, which triggers a bounded full replay. Make
    // that recovery deterministic here: ordinary initial delivery is covered
    // by the R5 and filtered-replay suites, while this fixture must isolate
    // application-level cancellation from #1101. The coordinator daemon is
    // deliberately stopped so its background-completion loop cannot consume
    // the synthetic remote child.
    let admin = EmbeddedRemoteP2pAdmin::new(sender);
    let collections = vec!["AgentToolCall".to_string()];
    let replicators = admin
        .list_replicators()
        .await
        .expect("list coordinator replicators for bridge replay");
    if let Some(existing) = replicators
        .into_iter()
        .find(|replicator| replicator.address.as_deref() == Some(receiver_addr))
    {
        let id = existing.id.unwrap_or_else(|| receiver_addr.to_string());
        let old_collections = if existing.collections.is_empty() {
            collections.clone()
        } else {
            existing.collections
        };
        admin
            .delete_replicator(&id, &old_collections)
            .await
            .expect("remove coordinator replicator for bridge replay");
    }

    let mut filters = PairingFilters::new();
    filters.insert(
        "AgentToolCall".to_string(),
        FilterPredicate {
            field: "spawn_target_did".to_string(),
            value: receiver_did.to_string(),
        },
    );
    admin
        .add_replicator(&[receiver_addr.to_string()], &collections, &filters)
        .await
        .expect("reinstall coordinator replicator for bridge replay");

    wait_for_bridge(receiver, session_id, tool_call_id, Duration::from_secs(60)).await
}

async fn wait_for_bridge_cancel_intent(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
    timeout: Duration,
) -> BridgeRow {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    loop {
        if let Some(row) = fetch_bridge(node, session_id, tool_call_id).await {
            if row
                .cancel_cascade_intent_at
                .as_deref()
                .is_some_and(|s| !s.is_empty())
            {
                return row;
            }
            last = Some(row);
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for AgentToolCall({session_id}/{tool_call_id}) cancel intent; last={last:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_bridge_ack_cleared(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
    timeout: Duration,
) -> BridgeRow {
    let deadline = Instant::now() + timeout;
    let mut last = None;
    loop {
        if let Some(row) = fetch_bridge(node, session_id, tool_call_id).await {
            if row.cancel_pending_remote_ack == Some(false) {
                return row;
            }
            last = Some(row);
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for AgentToolCall({session_id}/{tool_call_id}) cancel ack; last={last:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn fetch_bridge(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
) -> Option<BridgeRow> {
    let session_id = escape_graphql_string(session_id);
    let tool_call_id = escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{session_id}" }},
                    tool_call_id: {{ _eq: "{tool_call_id}" }}
                }},
                limit: 1
            ) {{
                lifecycle_state
                child_request_id
                spawn_target_did
                cancel_cascade_intent_at
                cancel_pending_remote_ack
            }}
        }}"#
    );
    first_optional_row(&node.execute(&query).await, "AgentToolCall")
}

async fn assert_no_third_party_rows(node: &EmbeddedNode, coord_did: &str, host_did: &str) {
    let query = r#"{
        AgentRequest { agent_did }
        AgentToolCall { agent_did spawn_target_did }
    }"#;
    let response = node.execute(query).await;
    assert!(
        !response.has_errors(),
        "third-party row query failed: {:?}",
        response.errors
    );
    let data = response.data.expect("third-party query data");
    for row in data
        .get("AgentRequest")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(agent_did) = row.get("agent_did").and_then(serde_json::Value::as_str) else {
            continue;
        };
        assert!(
            agent_did == coord_did || agent_did == host_did,
            "unexpected AgentRequest agent_did {agent_did}"
        );
    }
    for row in data
        .get("AgentToolCall")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(agent_did) = row.get("agent_did").and_then(serde_json::Value::as_str) {
            assert!(
                agent_did == coord_did || agent_did == host_did,
                "unexpected AgentToolCall agent_did {agent_did}"
            );
        }
        if let Some(target_did) = row
            .get("spawn_target_did")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
        {
            assert!(
                target_did == coord_did || target_did == host_did,
                "unexpected AgentToolCall spawn_target_did {target_did}"
            );
        }
    }
}

async fn exec(node: &EmbeddedNode, statement: &str, context: &str) {
    let response = node.execute(statement).await;
    assert!(
        !response.has_errors(),
        "{context} failed: {:?}\n{statement}",
        response.errors
    );
}
