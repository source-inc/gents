use std::sync::Arc;
use std::time::Duration;

use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::llm::ToolCallHookAction;
use gents::{
    default_behavior_id_for_agent, load_agent_behavior, upsert_agent_behavior,
    upsert_tool_selection, AgentBehaviorDocument, AgentIdentity, DefraSessionHook,
    DocumentRuntimeOptions, FailurePolicy, Gents, ToolCeiling, ToolSelectionDocument,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::lean_vocab_test::{lean_r5_cross_deployment_cases, LeanR5CrossDeploymentCase};
use crate::support::fixtures::{bind_default_behavior_backend, test_identity};
use crate::support::interrupt::{wait_for_runtime_ready, BootedAgent};
use crate::support::mock_endpoint::MockModelEndpoint;
use crate::support::p2p_waits::{wait_for_connected_peer, wait_for_listen_addr};
use crate::support::{first_optional_row, test_db, test_p2p_db, TestDb};

const PARENT_AGENT_DID: &str = "did:test:r5-lean-parent";

struct RunningChildAgent {
    db: TestDb,
    booted: BootedAgent,
    _endpoint: MockModelEndpoint,
}

#[derive(Debug, Deserialize)]
struct ToolCallRow {
    request_id: String,
    tool_name: String,
    tool_call_id: String,
    lifecycle_state: Option<String>,
    await_mode: Option<String>,
    cancel_policy: Option<String>,
    child_request_id: Option<String>,
    unclaimed_deadline_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AgentRequestRow {
    request_id: String,
    agent_did: String,
    requester_did: Option<String>,
    behavior_id: String,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
    caused_by_trigger_id: Option<String>,
    caused_by_trigger_kind: Option<String>,
}

pub(super) async fn generated_r5_cross_deployment_cases_drive_production_dispatch() {
    let cases = lean_r5_cross_deployment_cases();
    assert_eq!(cases.len(), 2, "Lean should emit cross and local R5 rows");

    for case in cases {
        assert_eq!(case.action.as_str(), "spawn_subagent", "{}", case.name);
        assert_eq!(case.await_mode.as_str(), "background", "{}", case.name);
        assert_eq!(case.cancel_policy.as_str(), "cascade", "{}", case.name);
        assert_eq!(
            case.child_request_id.as_str(),
            "runtime_generated",
            "{}",
            case.name
        );

        if case.cross_deployment_routing_fired {
            drive_cross_deployment_case(case).await;
        } else {
            drive_single_deployment_case(case).await;
        }
    }
}

async fn drive_cross_deployment_case(case: &LeanR5CrossDeploymentCase) {
    assert_eq!(case.route.as_str(), "cross_deployment", "{}", case.name);
    assert_ne!(
        case.parent_deployment, case.child_deployment,
        "{} should cross deployments",
        case.name
    );
    assert!(case.child_owned_by_target_deployment, "{}", case.name);

    let child_agent = boot_child_agent(case).await;
    let parent_db = test_p2p_db(&format!("{}-parent", case.name)).await;
    install_one_way_replicator(
        parent_db.node.as_ref(),
        child_agent.db.node.as_ref(),
        &["AgentToolCall"],
    )
    .await;
    let (parent_db, hook, parent_session_id, _parent_behavior_id) = setup_parent_hook_on_db(
        case,
        false,
        Some(child_agent.booted.agent_did.as_str()),
        parent_db,
    )
    .await;

    let child_request_id = spawn_from_parent_hook(case, &hook).await;
    assert!(
        fetch_child_request_optional(parent_db.node.as_ref(), &child_request_id)
            .await
            .is_none(),
        "{}: A must persist the bridge without materializing B's child request",
        case.name
    );

    let bridge = fetch_tool_call(
        parent_db.node.as_ref(),
        &parent_session_id,
        &case.parent_tool_call_id,
    )
    .await;
    assert_bridge_matches_case(case, &bridge, &child_request_id);

    let replicated_bridge = wait_for_tool_call(
        child_agent.db.node.as_ref(),
        &parent_session_id,
        &case.parent_tool_call_id,
    )
    .await;
    assert_bridge_matches_case(case, &replicated_bridge, &child_request_id);
    assert!(
        fetch_child_request_optional(child_agent.db.node.as_ref(), &case.parent_request_id)
            .await
            .is_none(),
        "{}: the targeted bridge must not drag the coordinator parent request to B",
        case.name
    );

    let child = wait_for_child_request(child_agent.db.node.as_ref(), &child_request_id).await;
    assert_child_matches_case(case, &child, &child_request_id);
    let child_agent_did = child_agent.booted.agent_did.clone();
    assert_eq!(
        child.agent_did, child_agent_did,
        "{}: cross-deployment child must be locally owned by B",
        case.name
    );
    assert_eq!(
        child.requester_did.as_deref(),
        Some(PARENT_AGENT_DID),
        "{}: child request must route back only to its coordinator",
        case.name
    );

    let RunningChildAgent {
        db: child_db,
        booted,
        _endpoint,
    } = child_agent;
    booted.shutdown().await;
    parent_db.node.shutdown().await;
    child_db.node.shutdown().await;
}

async fn drive_single_deployment_case(case: &LeanR5CrossDeploymentCase) {
    assert_eq!(case.route.as_str(), "single_deployment", "{}", case.name);
    assert_eq!(
        case.parent_deployment, case.child_deployment,
        "{} should stay on one deployment",
        case.name
    );
    assert!(case.single_deployment_fallback, "{}", case.name);

    let (parent_db, hook, parent_session_id, parent_behavior_id) =
        setup_parent_hook(case, true).await;
    let _source = super::support::fixtures::spawn_subagent_source(
        parent_db.node.clone(),
        PARENT_AGENT_DID,
        &parent_behavior_id,
        &case.target_behavior_id,
    );
    let child_request_id = spawn_from_parent_hook(case, &hook).await;

    let bridge = fetch_tool_call(
        parent_db.node.as_ref(),
        &parent_session_id,
        &case.parent_tool_call_id,
    )
    .await;
    assert_bridge_matches_case(case, &bridge, &child_request_id);

    let child = wait_for_child_request(parent_db.node.as_ref(), &child_request_id).await;
    assert_child_matches_case(case, &child, &child_request_id);
    assert_eq!(
        child.agent_did, PARENT_AGENT_DID,
        "{}: same-deployment fallback should keep child ownership local",
        case.name
    );
}

async fn boot_child_agent(case: &LeanR5CrossDeploymentCase) -> RunningChildAgent {
    let db = test_p2p_db(&format!("{}-child", case.name)).await;
    write_pairing(db.node.as_ref(), "deployment-a", PARENT_AGENT_DID).await;

    let identity: Arc<dyn AgentIdentity> = Arc::new(test_identity(&format!("{}-child", case.name)));
    let child_agent_did = identity.did().to_string();
    let default_behavior_id = default_behavior_id_for_agent(&child_agent_did);
    let endpoint = MockModelEndpoint::start("default").expect("mock endpoint");
    bind_default_behavior_backend(
        db.node.as_ref(),
        &child_agent_did,
        &format!("{}-backend", case.name),
        endpoint.endpoint(),
    )
    .await;
    upsert_active_child_behavior_from_default(
        db.node.as_ref(),
        &default_behavior_id,
        &case.target_behavior_id,
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
    .expect("child agent");
    let child_agent_did = agent.agent_did().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));
    wait_for_runtime_ready(db.node.as_ref(), &child_agent_did).await;

    RunningChildAgent {
        db,
        booted: BootedAgent::new(shutdown_tx, handle, child_agent_did),
        _endpoint: endpoint,
    }
}

async fn setup_parent_hook(
    case: &LeanR5CrossDeploymentCase,
    target_is_local: bool,
) -> (TestDb, DefraSessionHook, String, String) {
    let db = test_db(&format!("{}-parent", case.name)).await;
    setup_parent_hook_on_db(case, target_is_local, None, db).await
}

async fn setup_parent_hook_on_db(
    case: &LeanR5CrossDeploymentCase,
    target_is_local: bool,
    remote_target_owner_did: Option<&str>,
    db: TestDb,
) -> (TestDb, DefraSessionHook, String, String) {
    let parent_behavior_id = format!("{}-parent-behavior", case.name);
    let parent_session_id = format!("{}-session", case.parent_request_id);
    let selection_id = format!("{parent_behavior_id}-tools");

    let target_owner_did = if target_is_local {
        PARENT_AGENT_DID.to_string()
    } else {
        remote_target_owner_did
            .expect("cross-deployment case must pass the booted child agent DID")
            .to_string()
    };

    upsert_tool_selection(
        db.node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: PARENT_AGENT_DID.to_string(),
            subagent_targets: Some(vec![gents::subagent_target_entry(
                case.target_behavior_id.clone(),
                target_owner_did,
                case.target_behavior_id.clone(),
                None,
            )]),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            subagent_allow_cross_deployment: Some(true),
            cross_deployment_spawn_timeout_seconds: Some(60),
            enable_defra_query: None,
            defra_query_collections: None,
            ..Default::default()
        },
    )
    .await
    .expect("parent tool selection");
    upsert_agent_behavior(
        db.node.as_ref(),
        &AgentBehaviorDocument {
            behavior_id: parent_behavior_id.clone(),
            agent_did: PARENT_AGENT_DID.to_string(),
            display_name: Some(parent_behavior_id.clone()),
            description: None,
            summary: None,
            system_prompt: None,
            request_context_template: None,
            backend_id: None,
            model_name: None,
            tool_selection_id: Some(selection_id),
            inference_profile_id: None,
            compaction_strategy: None,
            compaction_threshold: None,
            skill_refs: Vec::new(),
            skill_excludes: Vec::new(),
            enabled: true,
            created_at: Some("2026-05-20T00:00:00Z".to_string()),
        },
    )
    .await
    .expect("parent behavior");

    if target_is_local {
        upsert_agent_behavior(
            db.node.as_ref(),
            &AgentBehaviorDocument {
                behavior_id: case.target_behavior_id.clone(),
                agent_did: PARENT_AGENT_DID.to_string(),
                display_name: Some(case.target_behavior_id.clone()),
                description: None,
                summary: None,
                system_prompt: None,
                request_context_template: None,
                backend_id: None,
                model_name: None,
                tool_selection_id: None,
                inference_profile_id: None,
                compaction_strategy: None,
                compaction_threshold: None,
                skill_refs: Vec::new(),
                skill_excludes: Vec::new(),
                enabled: true,
                created_at: Some("2026-05-20T00:00:01Z".to_string()),
            },
        )
        .await
        .expect("local child behavior");
    }

    create_parent_request(
        db.node.as_ref(),
        &case.parent_request_id,
        &parent_session_id,
        &parent_behavior_id,
    )
    .await;
    crate::support::create_agent_session(
        db.node.as_ref(),
        &parent_session_id,
        &parent_behavior_id,
        "2026-05-20T00:00:00Z",
    )
    .await;

    let hook = DefraSessionHook::resume_with_identity_policy(
        db.node.clone(),
        &parent_session_id,
        &parent_behavior_id,
        PARENT_AGENT_DID,
        FailurePolicy::default(),
    )
    .await
    .expect("parent hook");
    let parent_request_doc_id =
        crate::support::exact_request_doc_id(db.node.as_ref(), &case.parent_request_id).await;
    hook.set_active_request_binding(
        Some(case.parent_request_id.clone()),
        Some(parent_request_doc_id),
        None,
    )
    .await;
    hook.set_request_deadline_at(Some(chrono::Utc::now() + chrono::Duration::minutes(5)))
        .await;

    (db, hook, parent_session_id, parent_behavior_id)
}

async fn spawn_from_parent_hook(
    case: &LeanR5CrossDeploymentCase,
    hook: &DefraSessionHook,
) -> String {
    let args = json!({
        "name": case.target_behavior_id.as_str(),
        "prompt": format!("child prompt for {}", case.name),
        "await_mode": case.await_mode.as_str()
    })
    .to_string();

    let action = hook
        .on_tool_call(
            &case.action,
            Some(format!("model-{}", case.parent_tool_call_id)),
            &case.parent_tool_call_id,
            &args,
        )
        .await;
    let receipt = skip_reason_json(action);
    assert_eq!(receipt["ok"], true, "{}", case.name);
    assert_eq!(
        receipt["behavior_id"].as_str(),
        Some(case.target_behavior_id.as_str()),
        "{}",
        case.name
    );
    assert_eq!(
        receipt["await_mode"].as_str(),
        Some(case.await_mode.as_str()),
        "{}",
        case.name
    );
    assert_eq!(receipt["status"], "running", "{}", case.name);
    receipt["child_request_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("{}: spawn receipt omitted child_request_id", case.name))
        .to_string()
}

async fn upsert_active_child_behavior_from_default(
    node: &EmbeddedNode,
    default_behavior_id: &str,
    target_behavior_id: &str,
) {
    let mut behavior = load_agent_behavior(node, default_behavior_id)
        .await
        .expect("load default child behavior")
        .expect("default child behavior");
    let child_agent_did = behavior.agent_did.clone();
    let selection_id = format!("{target_behavior_id}-r5-cross-deployment-tools");
    upsert_tool_selection(
        node,
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: child_agent_did,
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            subagent_allow_cross_deployment: Some(true),
            cross_deployment_spawn_timeout_seconds: Some(60),
            ..Default::default()
        },
    )
    .await
    .expect("upsert target child tool selection");
    behavior.behavior_id = target_behavior_id.to_string();
    behavior.display_name = Some(target_behavior_id.to_string());
    behavior.tool_selection_id = Some(selection_id);
    upsert_agent_behavior(node, &behavior)
        .await
        .expect("upsert target child behavior");
}

async fn create_parent_request(
    node: &EmbeddedNode,
    request_id: &str,
    session_id: &str,
    behavior_id: &str,
) {
    let request_id = escape_graphql_string(request_id);
    let session_id = escape_graphql_string(session_id);
    let behavior_id = escape_graphql_string(behavior_id);
    let agent_did = escape_graphql_string(PARENT_AGENT_DID);
    let created_at = chrono::Utc::now().to_rfc3339();
    let deadline = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{request_id}",
                agent_did: "{agent_did}",
                behavior_id: "{behavior_id}",
                session_id: "{session_id}",
                retry_parent_request: "",
                retry_root_request: "{request_id}",
                superseded_by_request: "",
                content: "R5 parent prompt",
                status: "processing",
                lifecycle_state: "processing",
                backend_id: "",
                execution_origin: "interactive",
                metadata: "",
                failure_reason: "",
                created_at: "{created_at}",
                deadline: "{deadline}",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: 0
            }}) {{ _docID }}
        }}"#
    );
    exec(node, &mutation, "create parent AgentRequest").await;
}

async fn write_pairing(node: &EmbeddedNode, peer_id: &str, peer_agent_did: &str) {
    let peer_id = escape_graphql_string(peer_id);
    let peer_agent_did = escape_graphql_string(peer_agent_did);
    let now = escape_graphql_string(&chrono::Utc::now().to_rfc3339());
    let mutation = format!(
        r#"mutation {{
            upsert_PeerPairingDesired(
                filter: {{ peer_id: {{ _eq: "{peer_id}" }} }},
                add: {{
                    peer_id: "{peer_id}",
                    agent_did: "{peer_agent_did}",
                    collections: null,
                    replicator_addresses: null,
                    profiles: null,
                    created_at: "{now}",
                    updated_at: "{now}"
                }},
                update: {{
                    agent_did: "{peer_agent_did}",
                    collections: null,
                    replicator_addresses: null,
                    profiles: null,
                    updated_at: "{now}"
                }}
            ) {{ _docID }}
        }}"#
    );
    exec(node, &mutation, "write PeerPairingDesired").await;
}

async fn install_one_way_replicator(
    sender: &EmbeddedNode,
    receiver: &EmbeddedNode,
    collections: &[&str],
) {
    let sender_addr = wait_for_listen_addr(sender).await;
    let receiver_addr = wait_for_listen_addr(receiver).await;
    let sender_p2p = sender.p2p().expect("sender p2p");
    let receiver_p2p = receiver.p2p().expect("receiver p2p");

    sender_p2p
        .connect_peer(&receiver_addr)
        .await
        .expect("connect sender to receiver");
    wait_for_connected_peer(sender).await;
    wait_for_connected_peer(receiver).await;

    let collection_names = collections
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    sender_p2p
        .add_collections(collection_names.clone())
        .await
        .expect("add sender p2p collections");
    receiver_p2p
        .add_collections(collection_names.clone())
        .await
        .expect("add receiver p2p collections");
    receiver_p2p
        .add_replicator(
            collection_names.clone(),
            Some(&sender_addr),
            Default::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("authorize sender as receiver-side replicator");
    sender_p2p
        .add_replicator(
            collection_names,
            Some(&receiver_addr),
            Default::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("install sender to receiver replicator");
}

async fn fetch_tool_call(node: &EmbeddedNode, session_id: &str, tool_call_id: &str) -> ToolCallRow {
    fetch_tool_call_optional(node, session_id, tool_call_id)
        .await
        .unwrap_or_else(|| panic!("AgentToolCall {session_id}/{tool_call_id} not found"))
}

async fn fetch_tool_call_optional(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
) -> Option<ToolCallRow> {
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
                request_id
                tool_name
                tool_call_id
                lifecycle_state
                await_mode
                cancel_policy
                child_request_id
                unclaimed_deadline_at
            }}
        }}"#
    );
    first_optional_row(&node.execute(&query).await, "AgentToolCall")
}

async fn fetch_child_request_optional(
    node: &EmbeddedNode,
    child_request_id: &str,
) -> Option<AgentRequestRow> {
    let child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{child_request_id}" }} }}, limit: 1) {{
                request_id
                agent_did
                requester_did
                behavior_id
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
                caused_by_trigger_id
                caused_by_trigger_kind
            }}
        }}"#
    );
    first_optional_row(&node.execute(&query).await, "AgentRequest")
}

async fn wait_for_tool_call(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
) -> ToolCallRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(tool_call) = fetch_tool_call_optional(node, session_id, tool_call_id).await {
            return tool_call;
        }
        if tokio::time::Instant::now() >= deadline {
            let diagnostic = agent_tool_call_diagnostic(node).await;
            panic!("tool call {tool_call_id} was not replicated; {diagnostic}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_child_request(node: &EmbeddedNode, child_request_id: &str) -> AgentRequestRow {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(child) = fetch_child_request_optional(node, child_request_id).await {
            return child;
        }
        if tokio::time::Instant::now() >= deadline {
            let diagnostic = agent_request_diagnostic(node).await;
            panic!("child request {child_request_id} was not materialized; {diagnostic}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn agent_request_diagnostic(node: &EmbeddedNode) -> String {
    let response = node
        .execute(
            r#"{
                AgentRequest {
                    request_id
                    agent_did
                    requester_did
                    behavior_id
                    caused_by_parent_request_id
                    caused_by_parent_tool_call_id
                    caused_by_trigger_id
                    caused_by_trigger_kind
                }
            }"#,
        )
        .await;
    format!(
        "AgentRequest errors={:?} data={:?}",
        response.errors, response.data
    )
}

async fn agent_tool_call_diagnostic(node: &EmbeddedNode) -> String {
    let response = node
        .execute(
            r#"{
                AgentToolCall {
                    request_id
                    session_id
                    tool_name
                    tool_call_id
                    lifecycle_state
                    child_request_id
                }
            }"#,
        )
        .await;
    format!(
        "AgentToolCall errors={:?} data={:?}",
        response.errors, response.data
    )
}

fn assert_bridge_matches_case(
    case: &LeanR5CrossDeploymentCase,
    bridge: &ToolCallRow,
    child_request_id: &str,
) {
    assert!(case.parent_trigger_persisted, "{}", case.name);
    assert_eq!(
        bridge.request_id, case.parent_request_id,
        "{}: bridge parent request",
        case.name
    );
    assert_eq!(
        bridge.tool_call_id, case.parent_tool_call_id,
        "{}: bridge tool id",
        case.name
    );
    assert_eq!(bridge.tool_name, "spawn_subagent", "{}", case.name);
    assert_eq!(
        bridge.lifecycle_state.as_deref(),
        Some("running"),
        "{}",
        case.name
    );
    assert_eq!(
        bridge.await_mode.as_deref(),
        Some(case.await_mode.as_str()),
        "{}",
        case.name
    );
    assert_eq!(
        bridge.cancel_policy.as_deref(),
        Some(case.cancel_policy.as_str()),
        "{}",
        case.name
    );
    assert_eq!(
        bridge.child_request_id.as_deref(),
        Some(child_request_id),
        "{}: bridge child_request_id",
        case.name
    );
    assert_eq!(
        bridge.unclaimed_deadline_at.is_some(),
        case.unclaimed_deadline_set,
        "{}: unclaimed deadline",
        case.name
    );
}

fn assert_child_matches_case(
    case: &LeanR5CrossDeploymentCase,
    child: &AgentRequestRow,
    child_request_id: &str,
) {
    assert!(case.child_materialized, "{}", case.name);
    assert_eq!(child.request_id, child_request_id, "{}", case.name);
    assert_eq!(
        child.behavior_id, case.target_behavior_id,
        "{}: child target behavior",
        case.name
    );
    assert_eq!(
        child.caused_by_parent_request_id.as_deref(),
        case.caused_by_parent_request_id_matches
            .then_some(case.parent_request_id.as_str()),
        "{}: parent request linkage",
        case.name
    );
    assert_eq!(
        child.caused_by_parent_tool_call_id.as_deref(),
        case.caused_by_parent_tool_call_id_matches
            .then_some(case.parent_tool_call_id.as_str()),
        "{}: parent tool linkage",
        case.name
    );
    assert_eq!(
        child.caused_by_trigger_id.as_deref(),
        Some(case.parent_tool_call_id.as_str()),
        "{}: trigger id linkage",
        case.name
    );
    assert_eq!(
        child.caused_by_trigger_kind.as_deref(),
        Some(case.caused_by_trigger_kind.as_str()),
        "{}: trigger kind linkage",
        case.name
    );
}

fn skip_reason_json(action: ToolCallHookAction) -> Value {
    let ToolCallHookAction::Skip { reason } = action else {
        panic!("expected Skip action, got {action:?}");
    };
    serde_json::from_str(&reason).expect("skip reason should be JSON")
}

async fn exec(node: &EmbeddedNode, statement: &str, context: &str) {
    let response = node.execute(statement).await;
    assert!(
        !response.has_errors(),
        "{context} failed: {:?}\n{statement}",
        response.errors
    );
}
