use std::sync::Arc;
use std::time::Duration;

use gents::defra_node::EmbeddedNode;
use gents::graphql::escape_graphql_string;
use gents::interrupt::{fetch_interrupt_requested_at, interrupt_request};
use gents::retry::execute_graphql_with_conflict_retry;
use gents::tool_call_lifecycle::{
    create_subagent_request_with_request_id_for_test,
    create_subagent_request_with_trusted_parent_request_id_for_test, AwaitMode, CancelPolicy,
    IllegalToolCallTransition, ToolCallLifecycle, MAX_SUBAGENT_DEPTH,
};
use gents::{
    default_behavior_id_for_agent, load_agent_behavior, upsert_agent_behavior,
    upsert_tool_selection, AgentBehaviorDocument, AgentIdentity, DocumentRuntimeOptions, Gents,
    ToolCeiling, ToolSelectionDocument,
};
use serde::Deserialize;

use crate::lean_vocab_test::lean_subagent_bridge_admission_cases;
use crate::support::fixtures::{bind_default_behavior_backend, test_identity};
use crate::support::interrupt::{create_runtime_request, wait_for_runtime_ready, BootedAgent};
use crate::support::mock_endpoint::MockModelEndpoint;
use crate::support::{
    first_optional_row, first_row, test_db as unsigned_test_db, test_db_with_identity, TestDb,
};

const SIGNED_BRIDGE_ASSERT_TIMEOUT: Duration = Duration::from_secs(120);

#[test]
fn generated_bridge_admission_cases_require_signed_exact_parent_evidence() {
    let cases = lean_subagent_bridge_admission_cases();
    assert_eq!(cases.len(), 9, "bridge admission decision table drifted");
    for case in cases {
        let admitted = case.bridge_signature_valid
            && case.bridge_signer_did == case.bridge_author_did
            && case.bridge_signer_did == case.admitted_parent_did
            && case.bridge_head_count == 1
            && case.observed_bridge_cid == case.current_bridge_cid
            && case.parent_request_matches
            && case.parent_tool_call_matches
            && case.child_request_matches;
        assert_eq!(admitted, case.admitted, "{} drifted from Lean", case.name);
        assert_eq!(
            case.outcome,
            if admitted {
                "childMaterialized"
            } else {
                "rejected"
            },
            "{} emitted the wrong materialization outcome",
            case.name
        );
    }
}

struct RunningAgent {
    booted: BootedAgent,
    _endpoint: MockModelEndpoint,
    behavior_id: String,
}

async fn boot_agent(db: &crate::support::TestDb, test_name: &str) -> RunningAgent {
    boot_agent_with_policy(db, test_name, None, true, true).await
}

async fn boot_agent_with_targets(
    db: &crate::support::TestDb,
    test_name: &str,
    subagent_targets: Vec<String>,
) -> RunningAgent {
    boot_agent_with_policy(db, test_name, Some(subagent_targets), true, true).await
}

async fn boot_agent_with_policy(
    db: &crate::support::TestDb,
    test_name: &str,
    subagent_targets: Option<Vec<String>>,
    spawn_enabled: bool,
    background_enabled: bool,
) -> RunningAgent {
    let identity: Arc<dyn AgentIdentity> = db
        .node_identity()
        .unwrap_or_else(|| Arc::new(test_identity(test_name)));
    let agent_did = identity.did().to_string();
    let behavior_id = default_behavior_id_for_agent(&agent_did);
    let endpoint = MockModelEndpoint::start("default").unwrap();
    bind_default_behavior_backend(
        db.node.as_ref(),
        &agent_did,
        "backend-subagent-source",
        endpoint.endpoint(),
    )
    .await;
    ensure_parent_subagent_authorization(
        db.node.as_ref(),
        &agent_did,
        &behavior_id,
        subagent_targets.unwrap_or_else(|| vec![behavior_id.clone()]),
        spawn_enabled,
        background_enabled,
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
    .unwrap();
    let agent_did = agent.agent_did().to_string();
    let behavior_id = agent.default_behavior_id().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));
    wait_for_runtime_ready(db.node.as_ref(), &agent_did).await;
    RunningAgent {
        booted: BootedAgent::new(shutdown_tx, handle, agent_did),
        _endpoint: endpoint,
        behavior_id,
    }
}

async fn signed_test_db(name: &str) -> TestDb {
    let identity: Arc<dyn AgentIdentity> = Arc::new(test_identity(name));
    test_db_with_identity(name, identity).await
}

async fn test_db(name: &str) -> TestDb {
    signed_test_db(name).await
}

async fn ensure_parent_subagent_authorization(
    node: &EmbeddedNode,
    agent_did: &str,
    behavior_id: &str,
    subagent_targets: Vec<String>,
    spawn_enabled: bool,
    background_enabled: bool,
) {
    let selection_id = format!("{behavior_id}-r3-subagent-tools");
    let target_entries = subagent_targets
        .into_iter()
        .map(|target_behavior_id| {
            gents::subagent_target_entry(
                target_behavior_id.clone(),
                agent_did,
                target_behavior_id,
                None,
            )
        })
        .collect();
    upsert_tool_selection(
        node,
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: agent_did.to_string(),
            subagent_targets: Some(target_entries),
            subagent_spawn_enabled: Some(spawn_enabled),
            subagent_background_enabled: Some(background_enabled),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mut behavior = match load_agent_behavior(node, behavior_id).await.unwrap() {
        Some(behavior) => behavior,
        None => AgentBehaviorDocument {
            behavior_id: behavior_id.to_string(),
            agent_did: agent_did.to_string(),
            display_name: Some(behavior_id.to_string()),
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
            created_at: Some("2026-05-12T00:00:00Z".to_string()),
        },
    };
    behavior.tool_selection_id = Some(selection_id);
    upsert_agent_behavior(node, &behavior).await.unwrap();
}

#[derive(Debug, Deserialize)]
struct ChildRequestRow {
    request_id: String,
    agent_did: String,
    requester_did: Option<String>,
    behavior_id: String,
    content: String,
    lifecycle_state: Option<String>,
    subagent_depth: Option<i64>,
    caused_by_parent_request_id: Option<String>,
    caused_by_parent_tool_call_id: Option<String>,
    caused_by_trigger_id: Option<String>,
    caused_by_trigger_kind: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ToolCallRow {
    lifecycle_state: Option<String>,
    result: Option<String>,
    tool_failure_class: Option<String>,
}

async fn wait_for_child_request(node: &EmbeddedNode, child_request_id: &str) -> ChildRequestRow {
    let deadline = tokio::time::Instant::now() + SIGNED_BRIDGE_ASSERT_TIMEOUT;
    loop {
        if let Some(row) = child_request_by_id(node, child_request_id).await {
            return row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for child AgentRequest {child_request_id}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn child_request_by_id(
    node: &EmbeddedNode,
    child_request_id: &str,
) -> Option<ChildRequestRow> {
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_child_request_id}" }} }},
                limit: 1
            ) {{
                request_id
                agent_did
                requester_did
                behavior_id
                content
                lifecycle_state
                subagent_depth
                caused_by_parent_request_id
                caused_by_parent_tool_call_id
                caused_by_trigger_id
                caused_by_trigger_kind
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    first_optional_row::<ChildRequestRow>(&response, "AgentRequest")
}

async fn wait_for_optional_child_request(
    node: &EmbeddedNode,
    child_request_id: &str,
    timeout: Duration,
) -> Option<ChildRequestRow> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(row) = child_request_by_id(node, child_request_id).await {
            return Some(row);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn signed_same_node_fixture_admits_targeted_bridge_without_parent_request() {
    let db = test_db("r3-subagent-source-xdep-targeted-bridge").await;
    // This deliberately isolates signed bridge admission and the paired-parent
    // path on one store. It is not evidence of multi-host replication: the
    // roadmap tracks the required remote-signer P2P fixture separately.
    let local_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let coordinator_did = local_did.clone();
    let target_behavior_id = "xdep-target-targeted-bridge";
    let parent_request_id = "r3-parent-targeted-bridge-not-replicated";
    let parent_tool_call_id = "r3-tc-targeted-bridge";
    let child_request_id = "r3-child-targeted-bridge";

    upsert_target_behavior_with_cross_deployment(
        db.node.as_ref(),
        &local_did,
        target_behavior_id,
        true,
    )
    .await;

    let mut paired = std::collections::HashSet::new();
    paired.insert(coordinator_did.to_string());
    let mut source = crate::support::fixtures::spawn_subagent_source_with_paired_peers(
        db.node.clone(),
        &local_did,
        target_behavior_id,
        target_behavior_id,
        paired,
    );
    source.wait_ready().await;

    write_targeted_cross_deployment_bridge(
        db.node.as_ref(),
        &coordinator_did,
        parent_request_id,
        parent_tool_call_id,
        child_request_id,
        target_behavior_id,
        &local_did,
        1,
    )
    .await;

    let child = wait_for_child_request(db.node.as_ref(), child_request_id).await;
    assert_eq!(child.agent_did, local_did);
    assert_eq!(
        child.requester_did.as_deref(),
        Some(coordinator_did.as_str())
    );
    assert_eq!(child.subagent_depth, Some(2));
    assert_eq!(
        child.caused_by_parent_request_id.as_deref(),
        Some(parent_request_id)
    );
}

#[tokio::test]
async fn trusted_path_normalizes_requester_did_before_stamping_route() {
    let db = test_db("r3-subagent-source-xdep-requester-normalization").await;
    let agent_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let child_request_id = "r3-child-normalized-requester";

    create_subagent_request_with_trusted_parent_request_id_for_test(
        db.node.as_ref(),
        child_request_id.to_string(),
        "r3-parent-not-replicated".to_string(),
        "r3-tc-normalized-requester".to_string(),
        0,
        agent_did,
        "test".to_string(),
        "prompt".to_string(),
        None,
        "  did:key:zNormalizedRequester  ".to_string(),
    )
    .await
    .expect("trusted child request should normalize requester DID");

    let child = wait_for_child_request(db.node.as_ref(), child_request_id).await;
    assert_eq!(
        child.requester_did.as_deref(),
        Some("did:key:zNormalizedRequester")
    );
}

async fn wait_for_child_interrupt_latch(
    node: &EmbeddedNode,
    child_request_id: &str,
    failure_message: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if fetch_interrupt_requested_at(node, child_request_id)
            .await
            .unwrap()
            .is_some()
        {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{failure_message}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_no_child_request_for_tool(
    node: &EmbeddedNode,
    parent_tool_call_id: &str,
    settle: Duration,
) {
    tokio::time::sleep(settle).await;
    let escaped_parent_tool_call_id = escape_graphql_string(parent_tool_call_id);
    let query = format!(
        r#"{{
            AgentRequest(
                filter: {{
                    caused_by_parent_tool_call_id: {{ _eq: "{escaped_parent_tool_call_id}" }}
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "query failed: {:?}",
        response.errors
    );
    let count = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRequest"))
        .and_then(|rows| rows.as_array())
        .map(Vec::len)
        .unwrap_or(0);
    assert_eq!(
        count, 0,
        "native AgentToolCall unexpectedly spawned {count} child request(s)"
    );
}

async fn fetch_tool_call(node: &EmbeddedNode, session_id: &str, tool_call_id: &str) -> ToolCallRow {
    let escaped_session_id = escape_graphql_string(session_id);
    let escaped_tool_call_id = escape_graphql_string(tool_call_id);
    let query = format!(
        r#"{{
            AgentToolCall(
                filter: {{
                    session_id: {{ _eq: "{escaped_session_id}" }},
                    tool_call_id: {{ _eq: "{escaped_tool_call_id}" }}
                }},
                limit: 1
            ) {{
                lifecycle_state
                result
                tool_failure_class
            }}
        }}"#
    );
    first_row(&node.execute(&query).await, "AgentToolCall")
}

async fn wait_for_tool_call_terminal(
    node: &EmbeddedNode,
    session_id: &str,
    tool_call_id: &str,
) -> ToolCallRow {
    let deadline = tokio::time::Instant::now() + SIGNED_BRIDGE_ASSERT_TIMEOUT;
    loop {
        let row = fetch_tool_call(node, session_id, tool_call_id).await;
        if row.lifecycle_state.as_deref() != Some("running") {
            return row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "tool call {tool_call_id} stayed running while awaiting source admission"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn recover_until_tool_call_terminal(
    node: &EmbeddedNode,
    agent_did: &str,
    session_id: &str,
    tool_call_id: &str,
) {
    // Bridge admission reads Defra's `_commits` projection as well as the live
    // row. Under parallel embedded-store startup that projection can become
    // observable after the row itself. Retry the idempotent recovery operation
    // only while the persisted tool call explicitly remains `running`.
    let deadline = tokio::time::Instant::now() + SIGNED_BRIDGE_ASSERT_TIMEOUT;
    loop {
        ToolCallLifecycle::recover_all(node, agent_did)
            .await
            .expect("tool-call recovery");
        let row = fetch_tool_call(node, session_id, tool_call_id).await;
        if row.lifecycle_state.as_deref() != Some("running") {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "tool call {tool_call_id} stayed running after repeated recovery"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn assert_tool_call_not_allowed(tool: &ToolCallRow, expected_path: &str, expected_requested: &str) {
    assert_eq!(tool.lifecycle_state.as_deref(), Some("failed"));
    assert_eq!(
        tool.tool_failure_class.as_deref(),
        Some("serviceUnavailable")
    );
    let result: serde_json::Value =
        serde_json::from_str(tool.result.as_deref().expect("tool result JSON")).unwrap();
    assert_eq!(result["failure_class"], "tool_not_allowed");
    assert_eq!(result["service_id"], "subagent");
    assert_eq!(result["path"], expected_path);
    assert_eq!(result["requested_tool_name"], expected_requested);
}

#[tokio::test]
async fn subagent_source_bridge_admission_materializes_child_request_from_tool_call() {
    let db = signed_test_db("r3-subagent-source-spawn").await;
    let running = boot_agent(&db, "r3-subagent-source-spawn").await;
    let parent_request_id = "r3-parent-spawn";
    let parent_tool_call_id = "r3-tc-spawn";
    let child_request_id = "r3-child-spawn";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        "r3-session-spawn",
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "child prompt from source"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-spawn".to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    let child = wait_for_child_request(db.node.as_ref(), child_request_id).await;
    assert_eq!(child.request_id, child_request_id);
    assert_eq!(child.agent_did, running.booted.agent_did);
    assert_eq!(child.behavior_id, running.behavior_id);
    assert_eq!(child.content, "child prompt from source");
    assert_eq!(child.subagent_depth, Some(1));
    assert_eq!(
        child.caused_by_parent_request_id.as_deref(),
        Some(parent_request_id)
    );
    assert_eq!(
        child.caused_by_parent_tool_call_id.as_deref(),
        Some(parent_tool_call_id)
    );
    assert_eq!(
        child.caused_by_trigger_id.as_deref(),
        Some(parent_tool_call_id)
    );
    assert_eq!(child.caused_by_trigger_kind.as_deref(), Some("subagent"));

    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_rejects_unsigned_bridge_admission() {
    let db = unsigned_test_db("r3-subagent-source-unsigned-bridge").await;
    let running = boot_agent(&db, "r3-subagent-source-unsigned-bridge").await;
    let parent_request_id = "r3-parent-unsigned-bridge";
    let parent_tool_call_id = "r3-tc-unsigned-bridge";
    let child_request_id = "r3-child-unsigned-bridge";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        "r3-session-unsigned-bridge",
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "unsigned bridge must not materialize"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-unsigned-bridge".to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(800),
    )
    .await;
    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_bridge_admission_rejects_declared_author_with_different_signer() {
    let db = signed_test_db("r3-subagent-source-wrong-signer").await;
    let running = boot_agent(&db, "r3-subagent-source-wrong-signer").await;
    let parent_request_id = "r3-parent-wrong-signer";
    let parent_tool_call_id = "r3-tc-wrong-signer";
    let child_request_id = "r3-child-wrong-signer";
    let claimed_parent_did = test_identity("r3-foreign-bridge-author").did().to_string();
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        "r3-session-wrong-signer",
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "wrong signer bridge must not materialize"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-wrong-signer".to_string(),
        claimed_parent_did,
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(800),
    )
    .await;
    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_rejects_mismatched_spawn_target_did_and_args() {
    let db = test_db("r3-subagent-source-target-mismatch").await;
    let running = boot_agent(&db, "r3-subagent-source-target-mismatch").await;
    let parent_request_id = "r3-parent-target-mismatch";
    let parent_tool_call_id = "r3-tc-target-mismatch";
    let child_request_id = "r3-child-target-mismatch";
    let parent_session_id = "r3-session-target-mismatch";
    let mismatched_args_did = "did:key:zArgsTargetMismatch";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "agent_did": mismatched_args_did,
        "behavior_id": running.behavior_id.clone(),
        "prompt": "child prompt from mismatched target"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        parent_session_id.to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    let tool =
        wait_for_tool_call_terminal(db.node.as_ref(), parent_session_id, parent_tool_call_id).await;
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
    assert_tool_call_not_allowed(&tool, "/agent_did", mismatched_args_did);

    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_rejects_unauthorized_target_without_child_request() {
    let db = test_db("r3-subagent-source-unauthorized").await;
    let running = boot_agent_with_targets(&db, "r3-subagent-source-unauthorized", Vec::new()).await;
    let parent_request_id = "r3-parent-unauthorized";
    let parent_tool_call_id = "r3-tc-unauthorized";
    let child_request_id = "r3-child-unauthorized";
    let parent_session_id = "r3-session-unauthorized";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "unauthorized child prompt from source"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        parent_session_id.to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    let tool =
        wait_for_tool_call_terminal(db.node.as_ref(), parent_session_id, parent_tool_call_id).await;
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
    assert_tool_call_not_allowed(&tool, "/name", &running.behavior_id);

    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_fails_unauthorized_target_even_when_target_is_not_active() {
    let db = test_db("r3-subagent-source-unauthorized-inactive").await;
    let running =
        boot_agent_with_targets(&db, "r3-subagent-source-unauthorized-inactive", Vec::new()).await;
    let parent_request_id = "r3-parent-unauthorized-inactive";
    let parent_tool_call_id = "r3-tc-unauthorized-inactive";
    let child_request_id = "r3-child-unauthorized-inactive";
    let parent_session_id = "r3-session-unauthorized-inactive";
    let target_behavior_id = "not-active-or-authorized";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "behavior_id": target_behavior_id,
        "prompt": "unauthorized inactive child prompt from source"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        parent_session_id.to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    let tool =
        wait_for_tool_call_terminal(db.node.as_ref(), parent_session_id, parent_tool_call_id).await;
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
    assert_tool_call_not_allowed(&tool, "/name", target_behavior_id);

    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_rejects_when_spawn_disabled_even_with_authorized_target() {
    let db = test_db("r3-subagent-source-spawn-disabled").await;
    let running = boot_agent(&db, "r3-subagent-source-spawn-disabled").await;
    ensure_parent_subagent_authorization(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        vec![running.behavior_id.clone()],
        false,
        true,
    )
    .await;
    let parent_request_id = "r3-parent-spawn-disabled";
    let parent_tool_call_id = "r3-tc-spawn-disabled";
    let child_request_id = "r3-child-spawn-disabled";
    let parent_session_id = "r3-session-spawn-disabled";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "spawn disabled child prompt from source"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        parent_session_id.to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    let tool =
        wait_for_tool_call_terminal(db.node.as_ref(), parent_session_id, parent_tool_call_id).await;
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
    assert_tool_call_not_allowed(&tool, "/", "spawn_subagent");

    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_rejects_background_when_background_disabled() {
    let db = test_db("r3-subagent-source-background-disabled").await;
    let running = boot_agent(&db, "r3-subagent-source-background-disabled").await;
    ensure_parent_subagent_authorization(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        vec![running.behavior_id.clone()],
        true,
        false,
    )
    .await;
    let parent_request_id = "r3-parent-background-disabled";
    let parent_tool_call_id = "r3-tc-background-disabled";
    let child_request_id = "r3-child-background-disabled";
    let parent_session_id = "r3-session-background-disabled";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "background disabled child prompt from source",
        "await_mode": "background"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        parent_session_id.to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    let tool =
        wait_for_tool_call_terminal(db.node.as_ref(), parent_session_id, parent_tool_call_id).await;
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
    assert_tool_call_not_allowed(&tool, "/await_mode", "background");

    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_ignores_native_tool_call_without_child_request_id() {
    let db = test_db("r3-subagent-source-native").await;
    let running = boot_agent(&db, "r3-subagent-source-native").await;
    let parent_request_id = "r3-parent-native";
    let parent_tool_call_id = "r3-tc-native";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        "r3-session-native",
        "parent prompt",
    )
    .await;

    let mut lifecycle = ToolCallLifecycle::new(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-native".to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "read_file".to_string(),
        "{}".to_string(),
        chrono::Utc::now() + chrono::Duration::minutes(5),
    );
    lifecycle.start_running().await.unwrap();

    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(750),
    )
    .await;

    running.booted.shutdown().await;
}

#[tokio::test]
async fn create_subagent_request_rejects_nonexistent_parent_request() {
    let db = test_db("r3-subagent-source-missing-parent").await;
    let agent_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let error = create_subagent_request_with_request_id_for_test(
        db.node.as_ref(),
        "r3-child-missing-parent".to_string(),
        "missing-parent-request".to_string(),
        "r3-tc-missing-parent".to_string(),
        0,
        agent_did,
        "test".to_string(),
        "prompt".to_string(),
        None,
    )
    .await
    .expect_err("missing parent request must be rejected");
    assert!(
        matches!(
            error.downcast_ref::<IllegalToolCallTransition>(),
            Some(IllegalToolCallTransition::ParentLinkageIncoherent)
        ),
        "expected ParentLinkageIncoherent, got {error:?}"
    );
}

#[tokio::test]
async fn create_subagent_request_enforces_depth_boundary() {
    let db = test_db("r3-subagent-source-depth").await;
    let agent_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    create_runtime_request(
        db.node.as_ref(),
        &agent_did,
        crate::support::AGENT_NAME,
        "r3-parent-depth",
        "r3-session-depth",
        "parent prompt",
    )
    .await;

    let child_id = create_subagent_request_with_request_id_for_test(
        db.node.as_ref(),
        "r3-child-depth-max".to_string(),
        "r3-parent-depth".to_string(),
        "r3-tc-depth-max".to_string(),
        MAX_SUBAGENT_DEPTH - 1,
        agent_did.clone(),
        "test".to_string(),
        "prompt".to_string(),
        None,
    )
    .await
    .expect("parent depth MAX-1 should create a child at MAX");
    let escaped_child_id = escape_graphql_string(&child_id);
    let query = format!(
        r#"{{
            AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_child_id}" }} }}, limit: 1) {{
                subagent_depth
            }}
        }}"#
    );
    #[derive(Deserialize)]
    struct DepthRow {
        subagent_depth: i64,
    }
    let row = first_row::<DepthRow>(&db.node.execute(&query).await, "AgentRequest");
    assert_eq!(row.subagent_depth, i64::from(MAX_SUBAGENT_DEPTH));

    let error = create_subagent_request_with_request_id_for_test(
        db.node.as_ref(),
        "r3-child-depth-over".to_string(),
        "r3-parent-depth".to_string(),
        "r3-tc-depth-over".to_string(),
        MAX_SUBAGENT_DEPTH,
        agent_did.clone(),
        "test".to_string(),
        "prompt".to_string(),
        None,
    )
    .await
    .expect_err("parent depth MAX should be rejected");
    assert!(
        matches!(
            error.downcast_ref::<IllegalToolCallTransition>(),
            Some(IllegalToolCallTransition::SubagentDepthExceeded)
        ),
        "expected SubagentDepthExceeded, got {error:?}"
    );

    let overflow_error = create_subagent_request_with_request_id_for_test(
        db.node.as_ref(),
        "r3-child-depth-overflow".to_string(),
        "r3-parent-depth".to_string(),
        "r3-tc-depth-overflow".to_string(),
        u32::MAX,
        agent_did,
        "test".to_string(),
        "prompt".to_string(),
        None,
    )
    .await
    .expect_err("u32::MAX parent depth must be rejected without overflow");
    assert!(
        matches!(
            overflow_error.downcast_ref::<IllegalToolCallTransition>(),
            Some(IllegalToolCallTransition::SubagentDepthExceeded)
        ),
        "expected overflow-safe SubagentDepthExceeded, got {overflow_error:?}"
    );
}

#[tokio::test]
async fn cascade_after_source_spawn_reaches_child_request() {
    let db = test_db("r3-subagent-source-cascade").await;
    let running = boot_agent(&db, "r3-subagent-source-cascade").await;
    let parent_request_id = "r3-parent-cascade";
    let parent_tool_call_id = "r3-tc-cascade";
    let child_request_id = "r3-child-cascade";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        "r3-session-cascade",
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "child prompt for cascade"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-cascade".to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Foreground,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();
    let _child = wait_for_child_request(db.node.as_ref(), child_request_id).await;

    interrupt_request(db.node.as_ref(), parent_request_id)
        .await
        .unwrap();
    mark_request_interrupted(db.node.as_ref(), parent_request_id).await;
    ToolCallLifecycle::recover_all(db.node.as_ref(), &running.booted.agent_did)
        .await
        .unwrap();

    wait_for_child_interrupt_latch(
        db.node.as_ref(),
        child_request_id,
        "cascade recovery should latch child interrupt_requested_at",
    )
    .await;

    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_skips_child_when_resolved_did_is_remote() {
    let db = test_db("r3-subagent-source-did-anchor").await;
    let identity: Arc<dyn AgentIdentity> = db.node_identity().expect("signed test identity");
    let agent_did = identity.did().to_string();
    let behavior_id = default_behavior_id_for_agent(&agent_did);
    let endpoint = MockModelEndpoint::start("default").unwrap();
    bind_default_behavior_backend(
        db.node.as_ref(),
        &agent_did,
        "backend-did-anchor",
        endpoint.endpoint(),
    )
    .await;

    let remote_did = "did:key:zRemotePeerNotUs";
    let selection_id = format!("{behavior_id}-r3-did-anchor-tools");
    upsert_tool_selection(
        db.node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: agent_did.clone(),
            subagent_targets: Some(vec![gents::subagent_target_entry(
                "remote-target",
                remote_did,
                "remote-behavior",
                None,
            )]),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            subagent_allow_cross_deployment: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut behavior = AgentBehaviorDocument {
        behavior_id: behavior_id.clone(),
        agent_did: agent_did.clone(),
        display_name: Some(behavior_id.clone()),
        description: None,
        summary: None,
        system_prompt: None,
        request_context_template: None,
        backend_id: None,
        model_name: None,
        tool_selection_id: Some(selection_id.clone()),
        inference_profile_id: None,
        compaction_strategy: None,
        compaction_threshold: None,
        skill_refs: Vec::new(),
        skill_excludes: Vec::new(),
        enabled: true,
        created_at: Some("2026-06-04T00:00:00Z".to_string()),
    };
    if let Some(existing) = load_agent_behavior(db.node.as_ref(), &behavior_id)
        .await
        .unwrap()
    {
        behavior = existing;
        behavior.tool_selection_id = Some(selection_id.clone());
    }
    upsert_agent_behavior(db.node.as_ref(), &behavior)
        .await
        .unwrap();

    let agent = Gents::from_default_behavior_documents(
        db.node.clone(),
        identity,
        DocumentRuntimeOptions {
            tool_ceiling: ToolCeiling::meta_only(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let agent_did = agent.agent_did().to_string();
    let behavior_id = agent.default_behavior_id().to_string();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(agent.run(shutdown_rx));
    wait_for_runtime_ready(db.node.as_ref(), &agent_did).await;
    let booted = BootedAgent::new(shutdown_tx, handle, agent_did.clone());

    let parent_request_id = "r3-parent-did-anchor";
    let parent_tool_call_id = "r3-tc-did-anchor";
    let child_request_id = "r3-child-did-anchor";
    create_runtime_request(
        db.node.as_ref(),
        &agent_did,
        &behavior_id,
        parent_request_id,
        "r3-session-did-anchor",
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "name": "remote-target",
        "agent_did": remote_did,
        "behavior_id": "remote-behavior",
        "prompt": "child prompt that should not run here"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-did-anchor".to_string(),
        agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        remote_did.to_string(),
    );
    lifecycle.start_running().await.unwrap();

    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(800),
    )
    .await;

    booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_parent_already_interrupted_never_leaves_live_child() {
    let db = test_db("r3-subagent-source-orphan-cancel").await;
    let running = boot_agent(&db, "r3-subagent-source-orphan-cancel").await;
    let parent_request_id = "r3-parent-orphan-cancel";
    let parent_tool_call_id = "r3-tc-orphan-cancel";
    let child_request_id = "r3-child-orphan-cancel";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        "r3-session-orphan-cancel",
        "parent prompt",
    )
    .await;

    interrupt_request(db.node.as_ref(), parent_request_id)
        .await
        .unwrap();

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "child prompt racing a parent cancel"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-orphan-cancel".to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    if wait_for_optional_child_request(db.node.as_ref(), child_request_id, Duration::from_secs(2))
        .await
        .is_some()
    {
        wait_for_child_interrupt_latch(
            db.node.as_ref(),
            child_request_id,
            "child was created but never interrupted despite parent cancel-before-materialize",
        )
        .await;
    }

    running.booted.shutdown().await;
}

#[tokio::test]
async fn signed_same_node_fixture_refuses_paired_child_when_target_flag_off() {
    let db = test_db("r3-subagent-source-xdep-flag-off").await;
    let local_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let target_behavior_id = "xdep-target-flag-off";
    let remote_parent_did = local_did.clone();

    upsert_target_behavior_with_cross_deployment(
        db.node.as_ref(),
        &local_did,
        target_behavior_id,
        false,
    )
    .await;

    let parent_request_id = "r3-parent-xdep-flag-off";
    let parent_session_id = "r3-session-xdep-flag-off";
    let parent_tool_call_id = "r3-tc-xdep-flag-off";
    let child_request_id = "r3-child-xdep-flag-off";
    create_remote_parent_request(
        db.node.as_ref(),
        &remote_parent_did,
        parent_request_id,
        parent_session_id,
    )
    .await;

    let mut paired = std::collections::HashSet::new();
    paired.insert(remote_parent_did.to_string());
    let mut source = crate::support::fixtures::spawn_subagent_source_with_paired_peers(
        db.node.clone(),
        &local_did,
        target_behavior_id,
        target_behavior_id,
        paired,
    );
    source.wait_ready().await;

    write_cross_deployment_bridge(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        &remote_parent_did,
        target_behavior_id,
        &local_did,
    )
    .await;

    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(800),
    )
    .await;
}

#[tokio::test]
async fn signed_same_node_fixture_materializes_paired_child_when_target_flag_on() {
    let db = test_db("r3-subagent-source-xdep-flag-on").await;
    // Same-node signed policy fixture only; do not treat this as a replication
    // claim. A true remote-author commit must arrive through Defra P2P.
    let local_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let target_behavior_id = "xdep-target-flag-on";
    let remote_parent_did = local_did.clone();

    upsert_target_behavior_with_cross_deployment(
        db.node.as_ref(),
        &local_did,
        target_behavior_id,
        true,
    )
    .await;

    let parent_request_id = "r3-parent-xdep-flag-on";
    let parent_session_id = "r3-session-xdep-flag-on";
    let parent_tool_call_id = "r3-tc-xdep-flag-on";
    let child_request_id = "r3-child-xdep-flag-on";
    create_remote_parent_request(
        db.node.as_ref(),
        &remote_parent_did,
        parent_request_id,
        parent_session_id,
    )
    .await;

    let mut paired = std::collections::HashSet::new();
    paired.insert(remote_parent_did.to_string());
    let mut source = crate::support::fixtures::spawn_subagent_source_with_paired_peers(
        db.node.clone(),
        &local_did,
        target_behavior_id,
        target_behavior_id,
        paired,
    );
    source.wait_ready().await;

    write_cross_deployment_bridge(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        &remote_parent_did,
        target_behavior_id,
        &local_did,
    )
    .await;

    let child = wait_for_child_request(db.node.as_ref(), child_request_id).await;
    assert_eq!(child.request_id, child_request_id);
    assert_eq!(child.agent_did, local_did);
    assert_eq!(child.behavior_id, target_behavior_id);
}

#[tokio::test]
async fn signed_same_node_fixture_refuses_spawn_targeting_other_host() {
    let db = test_db("r3-subagent-source-xdep-wrong-host").await;
    let local_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let other_host_did = "did:key:zDifferentTrustedHost";
    let target_behavior_id = "xdep-target-wrong-host";
    let remote_parent_did = local_did.clone();

    upsert_target_behavior_with_cross_deployment(
        db.node.as_ref(),
        &local_did,
        target_behavior_id,
        true,
    )
    .await;

    let parent_request_id = "r3-parent-xdep-wrong-host";
    let parent_session_id = "r3-session-xdep-wrong-host";
    let parent_tool_call_id = "r3-tc-xdep-wrong-host";
    let child_request_id = "r3-child-xdep-wrong-host";
    create_remote_parent_request(
        db.node.as_ref(),
        &remote_parent_did,
        parent_request_id,
        parent_session_id,
    )
    .await;

    let mut paired = std::collections::HashSet::new();
    paired.insert(remote_parent_did.to_string());
    let mut source = crate::support::fixtures::spawn_subagent_source_with_paired_peers(
        db.node.clone(),
        &local_did,
        target_behavior_id,
        target_behavior_id,
        paired,
    );
    source.wait_ready().await;

    write_cross_deployment_bridge(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        &remote_parent_did,
        target_behavior_id,
        other_host_did,
    )
    .await;

    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(800),
    )
    .await;
}

#[tokio::test]
async fn signed_same_node_fixture_refuses_missing_spawn_target_did() {
    let db = test_db("r3-subagent-source-xdep-missing-target").await;
    let local_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let target_behavior_id = "xdep-target-missing-target";
    let remote_parent_did = local_did.clone();

    upsert_target_behavior_with_cross_deployment(
        db.node.as_ref(),
        &local_did,
        target_behavior_id,
        true,
    )
    .await;

    let parent_request_id = "r3-parent-xdep-missing-target";
    let parent_session_id = "r3-session-xdep-missing-target";
    let parent_tool_call_id = "r3-tc-xdep-missing-target";
    let child_request_id = "r3-child-xdep-missing-target";
    create_remote_parent_request(
        db.node.as_ref(),
        &remote_parent_did,
        parent_request_id,
        parent_session_id,
    )
    .await;

    let mut paired = std::collections::HashSet::new();
    paired.insert(remote_parent_did.to_string());
    let mut source = crate::support::fixtures::spawn_subagent_source_with_paired_peers(
        db.node.clone(),
        &local_did,
        target_behavior_id,
        target_behavior_id,
        paired,
    );
    source.wait_ready().await;

    write_cross_deployment_bridge_with_spawn_target(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        &remote_parent_did,
        target_behavior_id,
        &local_did,
        None,
    )
    .await;

    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(800),
    )
    .await;
}

#[tokio::test]
async fn recovery_refuses_cross_deployment_orphan_when_flag_off() {
    let db = test_db("r3-subagent-source-orphan-xdep-flag-off").await;
    let agent_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let parent_request_id = "r3-parent-orphan-xdep";
    let parent_session_id = "r3-session-orphan-xdep";
    let parent_tool_call_id = "r3-tc-orphan-xdep";
    let child_request_id = "child-orphan-xdep";
    let remote_target_did = "did:key:zRemoteTargetForRecovery";

    let selection_id = format!("{}-orphan-xdep-tools", crate::support::AGENT_NAME);
    upsert_tool_selection(
        db.node.as_ref(),
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: agent_did.clone(),
            subagent_targets: Some(vec![gents::subagent_target_entry(
                "remote-recovery-target",
                remote_target_did,
                "remote-recovery-behavior",
                None,
            )]),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            subagent_allow_cross_deployment: Some(false),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut behavior = match load_agent_behavior(db.node.as_ref(), crate::support::AGENT_NAME)
        .await
        .unwrap()
    {
        Some(behavior) => behavior,
        None => AgentBehaviorDocument {
            behavior_id: crate::support::AGENT_NAME.to_string(),
            agent_did: agent_did.clone(),
            display_name: Some(crate::support::AGENT_NAME.to_string()),
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
            created_at: Some("2026-06-04T00:00:00Z".to_string()),
        },
    };
    behavior.tool_selection_id = Some(selection_id);
    upsert_agent_behavior(db.node.as_ref(), &behavior)
        .await
        .unwrap();

    create_runtime_request(
        db.node.as_ref(),
        &agent_did,
        crate::support::AGENT_NAME,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;
    create_orphan_cross_deployment_tool_call(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        &agent_did,
        "remote-recovery-target",
        remote_target_did,
        "remote-recovery-behavior",
    )
    .await;
    recover_until_tool_call_terminal(
        db.node.as_ref(),
        &agent_did,
        parent_session_id,
        parent_tool_call_id,
    )
    .await;
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
    let tool = fetch_tool_call(db.node.as_ref(), parent_session_id, parent_tool_call_id).await;
    assert_tool_call_not_allowed(&tool, "/name", "remote-recovery-target");
}

#[tokio::test]
async fn recovery_ignores_remote_parent_orphan_even_when_target_is_local() {
    let db = test_db("r3-subagent-source-orphan-remote-parent").await;
    let identity: Arc<dyn AgentIdentity> = Arc::new(test_identity("r3-orphan-remote-parent"));
    let local_did = identity.did().to_string();
    let target_behavior_id = "orphan-remote-parent-target";
    let remote_parent_did = "did:key:zRemoteParentForRecovery";
    let parent_request_id = "r3-parent-orphan-remote-parent";
    let parent_session_id = "r3-session-orphan-remote-parent";
    let parent_tool_call_id = "r3-tc-orphan-remote-parent";
    let child_request_id = "child-orphan-remote-parent";

    upsert_target_behavior_with_cross_deployment(
        db.node.as_ref(),
        &local_did,
        target_behavior_id,
        true,
    )
    .await;
    create_remote_parent_request(
        db.node.as_ref(),
        &remote_parent_did,
        parent_request_id,
        parent_session_id,
    )
    .await;
    create_orphan_cross_deployment_tool_call(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        remote_parent_did,
        target_behavior_id,
        &local_did,
        target_behavior_id,
    )
    .await;

    let report = ToolCallLifecycle::recover_all(db.node.as_ref(), &local_did)
        .await
        .unwrap();
    assert_eq!(report.tool_calls_recovered, 0);
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
}

#[tokio::test]
async fn subagent_source_interrupts_child_on_concurrent_parent_cancel() {
    let db = test_db("r3-subagent-source-orphan-cancel-race").await;
    let running = boot_agent(&db, "r3-subagent-source-orphan-cancel-race").await;
    let parent_request_id = "r3-parent-orphan-cancel-race";
    let parent_tool_call_id = "r3-tc-orphan-cancel-race";
    let child_request_id = "r3-child-orphan-cancel-race";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        "r3-session-orphan-cancel-race",
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "child prompt racing a concurrent parent cancel"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-orphan-cancel-race".to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );

    let node_for_interrupt = db.node.clone();
    let parent_for_interrupt = parent_request_id.to_string();
    let interrupt_task = tokio::spawn(async move {
        interrupt_request(node_for_interrupt.as_ref(), &parent_for_interrupt)
            .await
            .unwrap();
    });
    lifecycle.start_running().await.unwrap();
    interrupt_task.await.unwrap();

    // Exact-head admission makes both race outcomes safe: if cancellation wins
    // before admission, no child is materialized; if materialization wins, the
    // cascade must latch the child interrupt.
    tokio::time::sleep(Duration::from_millis(800)).await;
    if child_request_by_id(db.node.as_ref(), child_request_id)
        .await
        .is_some()
    {
        wait_for_child_interrupt_latch(
            db.node.as_ref(),
            child_request_id,
            "child created but never interrupted despite concurrent parent cancel",
        )
        .await;
    }

    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_interrupts_cascade_child_when_parent_reaches_dead_terminal() {
    let db = test_db("r3-subagent-source-orphan-parent-dead").await;
    let running = boot_agent(&db, "r3-subagent-source-orphan-parent-dead").await;
    let parent_request_id = "r3-parent-orphan-dead";
    let parent_tool_call_id = "r3-tc-orphan-dead";
    let child_request_id = "r3-child-orphan-dead";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        "r3-session-orphan-dead",
        "parent prompt",
    )
    .await;

    mark_request_dead(db.node.as_ref(), parent_request_id).await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "child prompt racing a parent death"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-orphan-dead".to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    let _child = wait_for_child_request(db.node.as_ref(), child_request_id).await;
    wait_for_child_interrupt_latch(
        db.node.as_ref(),
        child_request_id,
        "Cascade child created but never interrupted despite dead (cancel-worthy) parent",
    )
    .await;

    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_parent_completed_before_admission_never_interrupts_child() {
    let db = test_db("r3-subagent-source-cascade-parent-completed").await;
    let running = boot_agent(&db, "r3-subagent-source-cascade-parent-completed").await;
    let parent_request_id = "r3-parent-cascade-completed";
    let parent_tool_call_id = "r3-tc-cascade-completed";
    let child_request_id = "r3-child-cascade-completed";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        "r3-session-cascade-completed",
        "parent prompt",
    )
    .await;
    mark_request_completed(db.node.as_ref(), parent_request_id).await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "background child whose parent finished cleanly"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-cascade-completed".to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        CancelPolicy::Cascade,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    if wait_for_optional_child_request(db.node.as_ref(), child_request_id, Duration::from_secs(2))
        .await
        .is_some()
    {
        assert_child_not_interrupted(
            db.node.as_ref(),
            child_request_id,
            Duration::from_millis(800),
        )
        .await;
    }

    running.booted.shutdown().await;
}

#[tokio::test]
async fn subagent_source_does_not_interrupt_detached_child_when_parent_interrupted() {
    let db = test_db("r3-subagent-source-detached-parent-interrupted").await;
    let running = boot_agent(&db, "r3-subagent-source-detached-parent-interrupted").await;
    let parent_request_id = "r3-parent-detached-interrupted";
    let parent_tool_call_id = "r3-tc-detached-interrupted";
    let child_request_id = "r3-child-detached-interrupted";
    create_runtime_request(
        db.node.as_ref(),
        &running.booted.agent_did,
        &running.behavior_id,
        parent_request_id,
        "r3-session-detached-interrupted",
        "parent prompt",
    )
    .await;

    let args = serde_json::json!({
        "behavior_id": running.behavior_id.clone(),
        "prompt": "detached child that must outlive an interrupted parent"
    })
    .to_string();
    let mut lifecycle = ToolCallLifecycle::new_subagent(
        db.node.clone(),
        parent_request_id.to_string(),
        "r3-session-detached-interrupted".to_string(),
        running.booted.agent_did.clone(),
        parent_tool_call_id.to_string(),
        1,
        "spawn_subagent".to_string(),
        args,
        chrono::Utc::now() + chrono::Duration::minutes(5),
        AwaitMode::Background,
        CancelPolicy::Detach,
        child_request_id.to_string(),
        running.booted.agent_did.clone(),
    );
    lifecycle.start_running().await.unwrap();

    let _child = wait_for_child_request(db.node.as_ref(), child_request_id).await;
    interrupt_request(db.node.as_ref(), parent_request_id)
        .await
        .unwrap();
    mark_request_interrupted(db.node.as_ref(), parent_request_id).await;
    assert_child_not_interrupted(
        db.node.as_ref(),
        child_request_id,
        Duration::from_millis(800),
    )
    .await;

    running.booted.shutdown().await;
}

#[tokio::test]
async fn recovery_bridge_admission_materializes_orphan_child_for_running_subagent_tool() {
    let db = signed_test_db("r3-subagent-source-orphan-recovery").await;
    let agent_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let parent_request_id = "r3-parent-orphan";
    let parent_session_id = "r3-session-orphan";
    let parent_tool_call_id = "r3-tc-orphan";
    let child_request_id = "child-orphan-1";
    ensure_parent_subagent_authorization(
        db.node.as_ref(),
        &agent_did,
        crate::support::AGENT_NAME,
        vec![crate::support::AGENT_NAME.to_string()],
        true,
        true,
    )
    .await;
    create_runtime_request(
        db.node.as_ref(),
        &agent_did,
        crate::support::AGENT_NAME,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;
    create_orphan_subagent_tool_call_for_agent(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        AwaitMode::Foreground,
        &agent_did,
        crate::support::AGENT_NAME,
    )
    .await;
    let report = ToolCallLifecycle::recover_all(db.node.as_ref(), &agent_did)
        .await
        .unwrap();
    assert_eq!(
        report.tool_calls_recovered, 0,
        "orphan backfill should not terminalize a live non-expired tool call"
    );

    let child = wait_for_child_request(db.node.as_ref(), child_request_id).await;
    assert_eq!(child.request_id, child_request_id);
    assert_eq!(child.behavior_id, crate::support::AGENT_NAME);
    assert_eq!(child.content, "orphan child prompt");
    assert_eq!(child.lifecycle_state.as_deref(), Some("pending"));
    assert_eq!(child.subagent_depth, Some(1));
    assert_eq!(
        child.caused_by_parent_request_id.as_deref(),
        Some(parent_request_id)
    );
    assert_eq!(
        child.caused_by_parent_tool_call_id.as_deref(),
        Some(parent_tool_call_id)
    );
    assert_eq!(
        child.caused_by_trigger_id.as_deref(),
        Some(parent_tool_call_id)
    );
    assert_eq!(child.caused_by_trigger_kind.as_deref(), Some("subagent"));
}

#[tokio::test]
async fn recovery_rejects_unsigned_orphan_bridge_admission() {
    let db = unsigned_test_db("r3-subagent-source-orphan-unsigned-bridge").await;
    let parent_request_id = "r3-parent-orphan-unsigned";
    let parent_session_id = "r3-session-orphan-unsigned";
    let parent_tool_call_id = "r3-tc-orphan-unsigned";
    let child_request_id = "child-orphan-unsigned";
    ensure_parent_subagent_authorization(
        db.node.as_ref(),
        crate::support::AGENT_DID,
        crate::support::AGENT_NAME,
        vec![crate::support::AGENT_NAME.to_string()],
        true,
        true,
    )
    .await;
    crate::support::create_request(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        "processing",
        &chrono::Utc::now().to_rfc3339(),
    )
    .await;
    create_orphan_subagent_tool_call(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
    )
    .await;

    let report = ToolCallLifecycle::recover_all(db.node.as_ref(), crate::support::AGENT_DID)
        .await
        .unwrap();
    assert_eq!(report.tool_calls_recovered, 0);
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
}

#[tokio::test]
async fn recovery_bridge_admission_rejects_declared_author_with_different_signer() {
    let db = signed_test_db("r3-subagent-source-orphan-wrong-signer").await;
    let claimed_parent_did = test_identity("r3-orphan-foreign-author").did().to_string();
    let parent_request_id = "r3-parent-orphan-wrong-signer";
    let parent_session_id = "r3-session-orphan-wrong-signer";
    let parent_tool_call_id = "r3-tc-orphan-wrong-signer";
    let child_request_id = "child-orphan-wrong-signer";
    ensure_parent_subagent_authorization(
        db.node.as_ref(),
        &claimed_parent_did,
        crate::support::AGENT_NAME,
        vec![crate::support::AGENT_NAME.to_string()],
        true,
        true,
    )
    .await;
    create_runtime_request(
        db.node.as_ref(),
        &claimed_parent_did,
        crate::support::AGENT_NAME,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;
    create_orphan_subagent_tool_call_for_agent(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        AwaitMode::Foreground,
        &claimed_parent_did,
        crate::support::AGENT_NAME,
    )
    .await;

    let report = ToolCallLifecycle::recover_all(db.node.as_ref(), &claimed_parent_did)
        .await
        .unwrap();
    assert_eq!(report.tool_calls_recovered, 0);
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
}

#[tokio::test]
async fn recovery_rejects_unauthorized_orphan_child_request() {
    let db = test_db("r3-subagent-source-orphan-unauthorized").await;
    let agent_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let parent_request_id = "r3-parent-orphan-denied";
    let parent_session_id = "r3-session-orphan-denied";
    let parent_tool_call_id = "r3-tc-orphan-denied";
    let child_request_id = "child-orphan-denied";
    ensure_parent_subagent_authorization(
        db.node.as_ref(),
        &agent_did,
        crate::support::AGENT_NAME,
        vec!["authorized-other-behavior".to_string()],
        true,
        true,
    )
    .await;
    create_runtime_request(
        db.node.as_ref(),
        &agent_did,
        crate::support::AGENT_NAME,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;
    create_orphan_subagent_tool_call_for_agent(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        AwaitMode::Foreground,
        &agent_did,
        crate::support::AGENT_NAME,
    )
    .await;
    recover_until_tool_call_terminal(
        db.node.as_ref(),
        &agent_did,
        parent_session_id,
        parent_tool_call_id,
    )
    .await;
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;

    let tool = fetch_tool_call(db.node.as_ref(), parent_session_id, parent_tool_call_id).await;
    assert_tool_call_not_allowed(&tool, "/name", crate::support::AGENT_NAME);
}

#[tokio::test]
async fn recovery_rejects_orphan_when_spawn_disabled_even_with_authorized_target() {
    let db = test_db("r3-subagent-source-orphan-spawn-disabled").await;
    let agent_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let parent_request_id = "r3-parent-orphan-spawn-disabled";
    let parent_session_id = "r3-session-orphan-spawn-disabled";
    let parent_tool_call_id = "r3-tc-orphan-spawn-disabled";
    let child_request_id = "child-orphan-spawn-disabled";
    ensure_parent_subagent_authorization(
        db.node.as_ref(),
        &agent_did,
        crate::support::AGENT_NAME,
        vec![crate::support::AGENT_NAME.to_string()],
        false,
        true,
    )
    .await;
    create_runtime_request(
        db.node.as_ref(),
        &agent_did,
        crate::support::AGENT_NAME,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;
    create_orphan_subagent_tool_call_for_agent(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        AwaitMode::Foreground,
        &agent_did,
        crate::support::AGENT_NAME,
    )
    .await;
    recover_until_tool_call_terminal(
        db.node.as_ref(),
        &agent_did,
        parent_session_id,
        parent_tool_call_id,
    )
    .await;
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
    let tool = fetch_tool_call(db.node.as_ref(), parent_session_id, parent_tool_call_id).await;
    assert_tool_call_not_allowed(&tool, "/", "spawn_subagent");
}

#[tokio::test]
async fn recovery_rejects_background_orphan_when_background_disabled() {
    let db = test_db("r3-subagent-source-orphan-background-disabled").await;
    let agent_did = db
        .node_identity()
        .expect("signed test identity")
        .did()
        .to_string();
    let parent_request_id = "r3-parent-orphan-background-disabled";
    let parent_session_id = "r3-session-orphan-background-disabled";
    let parent_tool_call_id = "r3-tc-orphan-background-disabled";
    let child_request_id = "child-orphan-background-disabled";
    ensure_parent_subagent_authorization(
        db.node.as_ref(),
        &agent_did,
        crate::support::AGENT_NAME,
        vec![crate::support::AGENT_NAME.to_string()],
        true,
        false,
    )
    .await;
    create_runtime_request(
        db.node.as_ref(),
        &agent_did,
        crate::support::AGENT_NAME,
        parent_request_id,
        parent_session_id,
        "parent prompt",
    )
    .await;
    create_orphan_subagent_tool_call_for_agent(
        db.node.as_ref(),
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        AwaitMode::Background,
        &agent_did,
        crate::support::AGENT_NAME,
    )
    .await;
    recover_until_tool_call_terminal(
        db.node.as_ref(),
        &agent_did,
        parent_session_id,
        parent_tool_call_id,
    )
    .await;
    assert_no_child_request_for_tool(
        db.node.as_ref(),
        parent_tool_call_id,
        Duration::from_millis(0),
    )
    .await;
    let tool = fetch_tool_call(db.node.as_ref(), parent_session_id, parent_tool_call_id).await;
    assert_tool_call_not_allowed(&tool, "/await_mode", "background");
}

async fn assert_child_not_interrupted(
    node: &EmbeddedNode,
    child_request_id: &str,
    settle: Duration,
) {
    tokio::time::sleep(settle).await;
    let interrupt = fetch_interrupt_requested_at(node, child_request_id)
        .await
        .unwrap();
    assert!(
        interrupt.is_none(),
        "child {child_request_id} was unexpectedly interrupted (interrupt_requested_at = {interrupt:?})",
    );
}

async fn mark_request_dead(node: &EmbeddedNode, request_id: &str) {
    let escaped_request_id = escape_graphql_string(request_id);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                input: {{
                    status: "dead",
                    lifecycle_state: "dead"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response =
        execute_graphql_with_conflict_retry(node, &mutation, "mark test request dead").await;
    assert!(
        !response.has_errors(),
        "mark_request_dead failed: {:?}",
        response.errors
    );
}

async fn mark_request_completed(node: &EmbeddedNode, request_id: &str) {
    let escaped_request_id = escape_graphql_string(request_id);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                input: {{
                    status: "completed",
                    lifecycle_state: "completed"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response =
        execute_graphql_with_conflict_retry(node, &mutation, "mark test request completed").await;
    assert!(
        !response.has_errors(),
        "mark_request_completed failed: {:?}",
        response.errors
    );
}

async fn upsert_target_behavior_with_cross_deployment(
    node: &EmbeddedNode,
    agent_did: &str,
    target_behavior_id: &str,
    allow_cross_deployment: bool,
) {
    let selection_id = format!("{target_behavior_id}-xdep-tools");
    upsert_tool_selection(
        node,
        &ToolSelectionDocument {
            selection_id: selection_id.clone(),
            agent_did: agent_did.to_string(),
            subagent_targets: Some(vec![gents::subagent_target_entry(
                target_behavior_id,
                agent_did,
                target_behavior_id,
                None,
            )]),
            subagent_spawn_enabled: Some(true),
            subagent_background_enabled: Some(true),
            subagent_allow_cross_deployment: Some(allow_cross_deployment),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let behavior = AgentBehaviorDocument {
        behavior_id: target_behavior_id.to_string(),
        agent_did: agent_did.to_string(),
        display_name: Some(target_behavior_id.to_string()),
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
        created_at: Some("2026-06-04T00:00:00Z".to_string()),
    };
    upsert_agent_behavior(node, &behavior).await.unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn write_targeted_cross_deployment_bridge(
    node: &EmbeddedNode,
    coordinator_did: &str,
    parent_request_id: &str,
    parent_tool_call_id: &str,
    child_request_id: &str,
    target_behavior_id: &str,
    target_agent_did: &str,
    parent_subagent_depth: u32,
) {
    let parent_session_id = format!("session-{parent_tool_call_id}");
    let tool_call_key = format!("{parent_session_id}:{parent_tool_call_id}");
    let args = serde_json::json!({
        "name": target_behavior_id,
        "agent_did": target_agent_did,
        "behavior_id": target_behavior_id,
        "prompt": "targeted cross-deployment child prompt",
        "parent_subagent_depth": parent_subagent_depth,
    })
    .to_string();
    let started_at = chrono::Utc::now().to_rfc3339();
    let deadline_at = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "{tool_call_key}",
                request_id: "{parent_request_id}",
                session_id: "{parent_session_id}",
                agent_did: "{coordinator_did}",
                message_sequence: 1,
                tool_name: "spawn_subagent",
                tool_call_id: "{parent_tool_call_id}",
                args: "{args}",
                result: "",
                status: "called",
                lifecycle_state: "running",
                started_at: "{started_at}",
                deadline_at: "{deadline_at}",
                child_request_id: "{child_request_id}",
                spawn_target_did: "{target_agent_did}",
                await_mode: "background",
                cancel_policy: "cascade"
            }}) {{ _docID }}
        }}"#,
        tool_call_key = escape_graphql_string(&tool_call_key),
        parent_request_id = escape_graphql_string(parent_request_id),
        parent_session_id = escape_graphql_string(&parent_session_id),
        coordinator_did = escape_graphql_string(coordinator_did),
        parent_tool_call_id = escape_graphql_string(parent_tool_call_id),
        args = escape_graphql_string(&args),
        child_request_id = escape_graphql_string(child_request_id),
        target_agent_did = escape_graphql_string(target_agent_did),
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create targeted AgentToolCall failed: {:?}",
        response.errors
    );
}

async fn create_remote_parent_request(
    node: &EmbeddedNode,
    remote_agent_did: &str,
    parent_request_id: &str,
    parent_session_id: &str,
) {
    let escaped_request_id = escape_graphql_string(parent_request_id);
    let escaped_agent_did = escape_graphql_string(remote_agent_did);
    let escaped_session_id = escape_graphql_string(parent_session_id);
    let created_at = chrono::Utc::now().to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentRequest(input: {{
                request_id: "{escaped_request_id}",
                agent_did: "{escaped_agent_did}",
                behavior_id: "remote-parent-behavior",
                session_id: "{escaped_session_id}",
                retry_parent_request: "",
                retry_root_request: "{escaped_request_id}",
                superseded_by_request: "",
                content: "remote parent prompt",
                status: "processing",
                lifecycle_state: "processing",
                backend_id: "",
                execution_origin: "interactive",
                created_at: "{created_at}",
                retry_count: 0,
                max_retries: 3,
                subagent_depth: 0
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create remote parent AgentRequest failed: {:?}",
        response.errors
    );
}

#[allow(clippy::too_many_arguments)]
async fn write_cross_deployment_bridge(
    node: &EmbeddedNode,
    parent_request_id: &str,
    parent_session_id: &str,
    parent_tool_call_id: &str,
    child_request_id: &str,
    author_agent_did: &str,
    target_behavior_id: &str,
    target_agent_did: &str,
) {
    write_cross_deployment_bridge_with_spawn_target(
        node,
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        author_agent_did,
        target_behavior_id,
        target_agent_did,
        Some(target_agent_did),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn write_cross_deployment_bridge_with_spawn_target(
    node: &EmbeddedNode,
    parent_request_id: &str,
    parent_session_id: &str,
    parent_tool_call_id: &str,
    child_request_id: &str,
    author_agent_did: &str,
    target_behavior_id: &str,
    target_agent_did: &str,
    spawn_target_did: Option<&str>,
) {
    let escaped_parent_request_id = escape_graphql_string(parent_request_id);
    let escaped_parent_session_id = escape_graphql_string(parent_session_id);
    let escaped_parent_tool_call_id = escape_graphql_string(parent_tool_call_id);
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let escaped_author_agent_did = escape_graphql_string(author_agent_did);
    let spawn_target_field = spawn_target_did
        .map(|did| format!(r#"spawn_target_did: "{}","#, escape_graphql_string(did)))
        .unwrap_or_else(|| "spawn_target_did: null,".to_string());
    let tool_call_key = format!("{escaped_parent_session_id}:{escaped_parent_tool_call_id}");
    let args = serde_json::json!({
        "name": target_behavior_id,
        "agent_did": target_agent_did,
        "behavior_id": target_behavior_id,
        "prompt": "cross-deployment child prompt"
    })
    .to_string();
    let escaped_args = escape_graphql_string(&args);
    let started_at = chrono::Utc::now().to_rfc3339();
    let deadline_at = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "{tool_call_key}",
                request_id: "{escaped_parent_request_id}",
                agent_did: "{escaped_author_agent_did}",
                session_id: "{escaped_parent_session_id}",
                message_sequence: 1,
                tool_name: "spawn_subagent",
                tool_call_id: "{escaped_parent_tool_call_id}",
                args: "{escaped_args}",
                result: "",
                status: "called",
                lifecycle_state: "running",
                started_at: "{started_at}",
                deadline_at: "{deadline_at}",
                child_request_id: "{escaped_child_request_id}",
                {spawn_target_field}
                await_mode: "background",
                cancel_policy: "cascade",
                selected_service_id: null,
                selected_tool_name: null,
                tool_failure_class: null,
                latency_ms: null
            }}) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create cross-deployment AgentToolCall failed: {:?}",
        response.errors
    );
}

#[allow(clippy::too_many_arguments)]
async fn create_orphan_cross_deployment_tool_call(
    node: &EmbeddedNode,
    parent_request_id: &str,
    parent_session_id: &str,
    parent_tool_call_id: &str,
    child_request_id: &str,
    author_agent_did: &str,
    target_name: &str,
    target_agent_did: &str,
    target_behavior_id: &str,
) {
    let escaped_parent_request_id = escape_graphql_string(parent_request_id);
    let escaped_parent_session_id = escape_graphql_string(parent_session_id);
    let escaped_parent_tool_call_id = escape_graphql_string(parent_tool_call_id);
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let escaped_target_agent_did = escape_graphql_string(target_agent_did);
    let tool_call_key = format!("{escaped_parent_session_id}:{escaped_parent_tool_call_id}");
    let args = serde_json::json!({
        "name": target_name,
        "agent_did": target_agent_did,
        "behavior_id": target_behavior_id,
        "prompt": "orphan cross-deployment child prompt"
    })
    .to_string();
    let escaped_args = escape_graphql_string(&args);
    let started_at = chrono::Utc::now().to_rfc3339();
    let deadline_at = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    let mutation = format!(
        r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "{tool_call_key}",
                request_id: "{escaped_parent_request_id}",
                agent_did: "{agent_did}",
                session_id: "{escaped_parent_session_id}",
                message_sequence: 1,
                tool_name: "spawn_subagent",
                tool_call_id: "{escaped_parent_tool_call_id}",
                args: "{escaped_args}",
                result: "",
                status: "called",
                lifecycle_state: "running",
                started_at: "{started_at}",
                deadline_at: "{deadline_at}",
                child_request_id: "{escaped_child_request_id}",
                spawn_target_did: "{escaped_target_agent_did}",
                await_mode: "background",
                cancel_policy: "cascade",
                selected_service_id: null,
                selected_tool_name: null,
                tool_failure_class: null,
                latency_ms: null
            }}) {{ _docID }}
        }}"#,
        agent_did = escape_graphql_string(author_agent_did),
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create orphan cross-deployment AgentToolCall failed: {:?}",
        response.errors
    );
}

async fn mark_request_interrupted(node: &EmbeddedNode, request_id: &str) {
    let escaped_request_id = escape_graphql_string(request_id);
    let mutation = format!(
        r#"mutation {{
            update_AgentRequest(
                filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }},
                input: {{
                    status: "error",
                    lifecycle_state: "interrupted"
                }}
            ) {{ _docID }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "mark_request_interrupted failed: {:?}",
        response.errors
    );
}

async fn create_orphan_subagent_tool_call(
    node: &EmbeddedNode,
    parent_request_id: &str,
    parent_session_id: &str,
    parent_tool_call_id: &str,
    child_request_id: &str,
) {
    create_orphan_subagent_tool_call_with_await_mode(
        node,
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        AwaitMode::Foreground,
    )
    .await;
}

async fn create_orphan_subagent_tool_call_with_await_mode(
    node: &EmbeddedNode,
    parent_request_id: &str,
    parent_session_id: &str,
    parent_tool_call_id: &str,
    child_request_id: &str,
    await_mode: AwaitMode,
) {
    create_orphan_subagent_tool_call_for_agent(
        node,
        parent_request_id,
        parent_session_id,
        parent_tool_call_id,
        child_request_id,
        await_mode,
        crate::support::AGENT_DID,
        crate::support::AGENT_NAME,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn create_orphan_subagent_tool_call_for_agent(
    node: &EmbeddedNode,
    parent_request_id: &str,
    parent_session_id: &str,
    parent_tool_call_id: &str,
    child_request_id: &str,
    await_mode: AwaitMode,
    agent_did: &str,
    behavior_id: &str,
) {
    let escaped_parent_request_id = escape_graphql_string(parent_request_id);
    let escaped_parent_session_id = escape_graphql_string(parent_session_id);
    let escaped_parent_tool_call_id = escape_graphql_string(parent_tool_call_id);
    let escaped_child_request_id = escape_graphql_string(child_request_id);
    let escaped_agent_did = escape_graphql_string(agent_did);
    let escaped_spawn_target_did = escaped_agent_did.clone();
    let tool_call_key = format!("{escaped_parent_session_id}:{escaped_parent_tool_call_id}");
    let args = serde_json::json!({
        "behavior_id": behavior_id,
        "prompt": "orphan child prompt"
    })
    .to_string();
    let escaped_args = escape_graphql_string(&args);
    let started_at = chrono::Utc::now().to_rfc3339();
    let deadline_at = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    let await_mode = await_mode.as_str();
    let mutation = format!(
        r#"mutation {{
            create_AgentToolCall(input: {{
                tool_call_key: "{tool_call_key}",
                request_id: "{escaped_parent_request_id}",
                agent_did: "{agent_did}",
                session_id: "{escaped_parent_session_id}",
                message_sequence: 1,
                tool_name: "spawn_subagent",
                tool_call_id: "{escaped_parent_tool_call_id}",
                args: "{escaped_args}",
                result: "",
                status: "called",
                lifecycle_state: "running",
                started_at: "{started_at}",
                deadline_at: "{deadline_at}",
                child_request_id: "{escaped_child_request_id}",
                spawn_target_did: "{escaped_spawn_target_did}",
                await_mode: "{await_mode}",
                cancel_policy: "cascade",
                selected_service_id: null,
                selected_tool_name: null,
                tool_failure_class: null,
                latency_ms: null
            }}) {{ _docID }}
        }}"#,
        agent_did = escaped_agent_did,
    );
    let response = node.execute(&mutation).await;
    assert!(
        !response.has_errors(),
        "create orphan AgentToolCall failed: {:?}",
        response.errors
    );
}
