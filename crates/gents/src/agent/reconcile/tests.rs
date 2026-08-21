use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch, Mutex, Notify};

use super::*;
use crate::admission::BackendAdmissionConfig;
use crate::agent::PendingAgentBehavior;
use crate::backend_provider::BackendProviderKind;
use crate::config::AgentBehavior;
use crate::ensure_runtime_schemas;
use crate::graphql::escape_graphql_string;
use crate::identity::{AgentIdentity as _, AgentPrincipal, KeyIdentity};
use crate::lean_vocab_test::{
    assert_state_machine_contract_is_complete, lean_runtime_reconcile_case,
    lean_state_machine_contract,
};
use crate::runtime_status::RuntimeStatusHandle;
use crate::tool_surface::{
    BehaviorToolConfig, FileToolMode, ToolCeiling, ToolSelection, ToolSurface,
};
use crate::watcher::AgentRequest;

#[derive(Debug)]
struct PairingReconcileRuntimeProbes {
    operator_write_diverges: bool,
    operator_delete_diverges: bool,
    read_failure_self_loops: bool,
    install_converges: bool,
    teardown_converges: bool,
    replicator_install_converges: bool,
    replicator_teardown_converges: bool,
    dial_converges: bool,
    crash_restarts_slot: bool,
}

async fn test_node() -> Arc<defra_node::EmbeddedNode> {
    Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap())
}

fn test_identity(name: &str) -> KeyIdentity {
    let path = std::env::temp_dir().join(format!("{name}-{}.key", uuid::Uuid::new_v4()));
    KeyIdentity::load_or_create(path, None).unwrap()
}

fn stub_principal() -> Arc<AgentPrincipal> {
    let identity: Arc<dyn crate::identity::AgentIdentity> = Arc::new(
        KeyIdentity::load_or_create(
            std::env::temp_dir().join(format!("stub-principal-{}.key", uuid::Uuid::new_v4())),
            None,
        )
        .unwrap(),
    );
    Arc::new(AgentPrincipal {
        agent_did: identity.did().to_string(),
        identity,
        default_behavior_id: String::new(),
        display_name: None,
        enabled: true,
    })
}

async fn snapshot_for_behaviors(
    node: &defra_node::EmbeddedNode,
    default_behavior_id: &str,
    behaviors: Vec<Arc<AgentBehavior>>,
) -> ResolvedRuntimeSnapshot {
    let mut tool_surfaces = HashMap::new();
    for behavior in &behaviors {
        let tool_surface = behavior.tools.resolve(node).await.unwrap();
        tool_surfaces.insert(behavior.behavior_id.clone(), Arc::new(tool_surface));
    }
    ResolvedRuntimeSnapshot::from_parts(
        default_behavior_id.to_string(),
        behaviors,
        tool_surfaces,
        HashMap::new(),
    )
    .with_principal(stub_principal())
}

async fn snapshot_for_behaviors_with_admission(
    node: &defra_node::EmbeddedNode,
    default_behavior_id: &str,
    behaviors: Vec<Arc<AgentBehavior>>,
    backend_admission_configs: HashMap<String, BackendAdmissionConfig>,
) -> ResolvedRuntimeSnapshot {
    let mut tool_surfaces = HashMap::new();
    for behavior in &behaviors {
        let tool_surface = behavior.tools.resolve(node).await.unwrap();
        tool_surfaces.insert(behavior.behavior_id.clone(), Arc::new(tool_surface));
    }
    ResolvedRuntimeSnapshot::from_parts_with_admission_configs(
        default_behavior_id.to_string(),
        behaviors,
        tool_surfaces,
        backend_admission_configs,
        HashMap::new(),
    )
    .with_principal(stub_principal())
}

fn backend_admission_config(
    backend_id: &str,
    max_concurrent: usize,
    max_queue_depth: usize,
) -> BackendAdmissionConfig {
    BackendAdmissionConfig {
        backend_id: backend_id.to_string(),
        max_concurrent,
        max_queue_depth,
        enabled: true,
        probe_status: crate::backend_registry::HEALTHY_PROBE_STATUS.to_string(),
        measured_unhealthy: false,
        config_fingerprint: format!("{backend_id}:{max_concurrent}:{max_queue_depth}"),
    }
}

fn background_child_request(index: usize, behavior_id: &str) -> AgentRequest {
    AgentRequest {
        doc_id: format!("child-doc-{index}"),
        request_id: format!("child-request-{index}"),
        agent_did: "did:test:background-fanout-test".to_string(),
        requester_did: None,
        behavior_id: Some(behavior_id.to_string()),
        session_id: format!("child-session-{index}"),
        content: format!("background child {index}"),
        temperature: None,
        top_p: None,
        top_k: None,
        seed: None,
        max_tokens: None,
        max_total_tokens: None,
        metadata: None,
        execution_origin: Some("interactive".to_string()),
        created_at: chrono::Utc::now().to_rfc3339(),
        deadline: None,
        subagent_depth: 1,
        caused_by_parent_request_id: Some("parent-request".to_string()),
        caused_by_parent_request_doc_id: Some("parent-request-doc".to_string()),
        caused_by_parent_tool_call_id: Some("parent-tool-call".to_string()),
        caused_by_parent_tool_call_doc_id: Some("parent-tool-call-doc".to_string()),
        caused_by_trigger_id: None,
        caused_by_trigger_kind: None,
        caused_by_source_doc_id: None,
        caused_by_correlation: None,
        caused_by_trigger_context: None,
    }
}

#[tokio::test]
async fn pairing_reconcile_state_machine_contract_is_complete() {
    assert_state_machine_contract_is_complete("PairingReconcile");
    let machine = lean_state_machine_contract("PairingReconcile");
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let probes = PairingReconcileRuntimeProbes {
        operator_write_diverges: operator_write_changes_snapshot_fingerprint(node.as_ref()).await,
        operator_delete_diverges: operator_delete_yields_teardown_diff(),
        read_failure_self_loops: read_failure_is_noop_self_loop(node.clone()).await,
        install_converges: reconcile_install_applies_added_behavior(node.as_ref()).await,
        teardown_converges: reconcile_teardown_applies_removed_behavior(node.as_ref()).await,
        replicator_install_converges: pairing_replicator_install_diff_converges(),
        replicator_teardown_converges: pairing_replicator_teardown_diff_converges(),
        dial_converges: pairing_dial_is_available_for_desired_addresses(),
        crash_restarts_slot: slot_panic_restarts_behavior(node.as_ref()).await,
    };

    let mut rust_legal_pairs = BTreeSet::new();
    for from in &machine.states {
        for action in &machine.actions {
            if let Some(post) = rust_pairing_reconcile_step(from, action, &probes) {
                rust_legal_pairs.insert((from.clone(), post.to_string()));
            }
        }
    }

    let lean_legal_pairs = machine
        .legal_transitions
        .iter()
        .map(|pair| (pair.from.clone(), pair.to.clone()))
        .collect::<BTreeSet<_>>();
    let lean_illegal_pairs = machine
        .illegal_transitions
        .iter()
        .map(|pair| (pair.from.clone(), pair.to.clone()))
        .collect::<BTreeSet<_>>();

    assert_eq!(
        rust_legal_pairs, lean_legal_pairs,
        "PairingReconcile Lean legal transitions drifted from Rust diff/slot behavior"
    );
    assert!(
        rust_legal_pairs.is_disjoint(&lean_illegal_pairs),
        "PairingReconcile Rust transitions overlap Lean illegal transitions"
    );
}

fn rust_pairing_reconcile_step(
    phase: &str,
    action: &str,
    probes: &PairingReconcileRuntimeProbes,
) -> Option<&'static str> {
    match (phase, action) {
        ("idle" | "converged" | "crashed", "operatorWrite") if probes.operator_write_diverges => {
            Some("diverged")
        }
        ("idle" | "converged" | "crashed", "operatorDelete") if probes.operator_delete_diverges => {
            Some("diverged")
        }
        ("idle", "readFailure") if probes.read_failure_self_loops => Some("idle"),
        ("converged", "readFailure") if probes.read_failure_self_loops => Some("converged"),
        ("diverged", "readFailure") if probes.read_failure_self_loops => Some("diverged"),
        ("crashed", "readFailure") if probes.read_failure_self_loops => Some("crashed"),
        ("diverged", "dial") if probes.dial_converges => Some("converged"),
        ("converged" | "diverged", "peerDisconnected") => Some("diverged"),
        ("diverged", "reconcileInstall") if probes.install_converges => Some("converged"),
        ("diverged", "reconcileTeardown") if probes.teardown_converges => Some("converged"),
        ("diverged", "reconcileInstallReplicator") if probes.replicator_install_converges => {
            Some("converged")
        }
        ("diverged", "reconcileTeardownReplicator") if probes.replicator_teardown_converges => {
            Some("converged")
        }
        (_, "crash") if probes.crash_restarts_slot => Some("crashed"),
        _ => None,
    }
}

fn pairing_replicator_install_diff_converges() -> bool {
    use crate::agent::p2p_reconcile::{
        compute_owned_pairing_diff, DiffOp, PairingActual, PairingApplied, PairingDesired,
    };
    let desired = PairingDesired {
        collections: BTreeSet::new(),
        replicator_addresses: BTreeSet::from(["addr1".to_string()]),
        ..Default::default()
    };
    let actual = PairingActual::default();
    let applied = PairingApplied::default();
    compute_owned_pairing_diff(&desired, &actual, &applied)
        == vec![DiffOp::InstallReplicator("addr1".into())]
}

fn pairing_replicator_teardown_diff_converges() -> bool {
    use crate::agent::p2p_reconcile::{
        compute_owned_pairing_diff, DiffOp, PairingActual, PairingApplied, PairingDesired,
    };
    let desired = PairingDesired::default();
    let actual = PairingActual {
        collections: BTreeSet::new(),
        replicator_addresses: BTreeSet::from(["addr1".to_string()]),
        ..Default::default()
    };
    let applied = PairingApplied {
        collections: BTreeSet::new(),
        replicator_addresses: BTreeSet::from(["addr1".to_string()]),
        ..Default::default()
    };
    compute_owned_pairing_diff(&desired, &actual, &applied)
        == vec![DiffOp::TeardownReplicator("addr1".into())]
}

/// Probe for the `operatorDelete` transition: when the operator deletes the
/// desired row (desired empty) but the managed/live state still carries what was
/// installed, the diff must be non-empty (a teardown of the managed set), so the
/// state diverges and the reconciler has work to do. Distinct from the
/// `operatorWrite` probe — this exercises desired-None-over-non-empty-applied.
fn operator_delete_yields_teardown_diff() -> bool {
    use crate::agent::p2p_reconcile::{
        compute_owned_pairing_diff, PairingActual, PairingApplied, PairingDesired,
    };
    let desired = PairingDesired::default();
    let actual = PairingActual {
        collections: BTreeSet::new(),
        replicator_addresses: BTreeSet::from(["addr1".to_string()]),
        ..Default::default()
    };
    let applied = PairingApplied {
        collections: BTreeSet::new(),
        replicator_addresses: BTreeSet::from(["addr1".to_string()]),
        ..Default::default()
    };
    !compute_owned_pairing_diff(&desired, &actual, &applied).is_empty()
}

/// Probe for the `readFailure` transition: a failed `load_desired` read makes a
/// tick a no-op self-loop — `desired_read_failed` is set and NO ops are applied,
/// so the state is unchanged. Exercises the real `reconcile_peer_tick` with a
/// store whose desired read errors (the admin is never reached on this path).
async fn read_failure_is_noop_self_loop(node: Arc<defra_node::EmbeddedNode>) -> bool {
    use crate::agent::p2p_reconcile::{
        reconcile_peer_tick, EmbeddedRemoteP2pAdmin, LoadedPairingApplied, PairingDesired,
        PairingStateStore,
    };

    struct FailingDesiredStore;
    #[async_trait::async_trait]
    impl PairingStateStore for FailingDesiredStore {
        async fn load_desired(&self, _peer_id: &str) -> anyhow::Result<Option<PairingDesired>> {
            anyhow::bail!("simulated desired-state read failure")
        }
        async fn load_applied(&self, _peer_id: &str) -> anyhow::Result<LoadedPairingApplied> {
            Ok(LoadedPairingApplied::default())
        }
        async fn persist_applied(
            &self,
            _peer_id: &str,
            _applied: &LoadedPairingApplied,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn list_peer_ids(&self) -> anyhow::Result<BTreeSet<String>> {
            Ok(BTreeSet::new())
        }
    }

    let admin = EmbeddedRemoteP2pAdmin::new(node);
    let store = FailingDesiredStore;
    match reconcile_peer_tick(&admin, &store, "peer-a").await {
        Ok(outcome) => outcome.desired_read_failed && outcome.ops_applied.is_empty(),
        Err(_) => false,
    }
}

fn pairing_dial_is_available_for_desired_addresses() -> bool {
    use crate::agent::p2p_reconcile::PairingDesired;
    PairingDesired {
        collections: BTreeSet::new(),
        replicator_addresses: BTreeSet::from(["addr1".to_string()]),
        ..Default::default()
    }
    .has_wiring()
}

async fn operator_write_changes_snapshot_fingerprint(node: &defra_node::EmbeddedNode) -> bool {
    let mut initial_behavior = PendingAgentBehavior::new("general")
        .build_with_identity_for_test(test_identity("pairing-contract-initial"));
    initial_behavior.system_prompt = "before operator write".to_string();
    let mut updated_behavior = PendingAgentBehavior::new("general")
        .build_with_identity_for_test(test_identity("pairing-contract-updated"));
    updated_behavior.system_prompt = "after operator write".to_string();
    let current_resolved =
        snapshot_for_behaviors(node, "general", vec![Arc::new(initial_behavior)]).await;
    let proposed = snapshot_for_behaviors(node, "general", vec![Arc::new(updated_behavior)]).await;
    let current = current_resolved.activate(1, HashMap::new());
    let diff = diff_counts(&current, &proposed);

    current.configuration_fingerprint() != proposed.configuration_fingerprint()
        && diff.updated == 1
        && diff.added == 0
        && diff.removed == 0
}

async fn reconcile_install_applies_added_behavior(node: &defra_node::EmbeddedNode) -> bool {
    let behavior = PendingAgentBehavior::new("general")
        .build_with_identity_for_test(test_identity("pairing-contract-install"));
    let current_resolved = ResolvedRuntimeSnapshot::from_parts(
        "general".to_string(),
        Vec::new(),
        HashMap::new(),
        HashMap::new(),
    )
    .with_principal(stub_principal());
    let proposed = snapshot_for_behaviors(node, "general", vec![Arc::new(behavior)]).await;
    let current = current_resolved.activate(1, HashMap::new());
    let diff = diff_counts(&current, &proposed);
    let applied = proposed.clone().activate(2, HashMap::new());
    let rediff = diff_counts(&applied, &proposed);

    diff.added == 1
        && diff.updated == 0
        && diff.removed == 0
        && rediff.added == 0
        && rediff.updated == 0
        && rediff.removed == 0
}

async fn reconcile_teardown_applies_removed_behavior(node: &defra_node::EmbeddedNode) -> bool {
    let behavior = PendingAgentBehavior::new("general")
        .build_with_identity_for_test(test_identity("pairing-contract-teardown"));
    let current_resolved = snapshot_for_behaviors(node, "general", vec![Arc::new(behavior)]).await;
    let proposed = ResolvedRuntimeSnapshot::from_parts(
        "general".to_string(),
        Vec::new(),
        HashMap::new(),
        HashMap::new(),
    )
    .with_principal(stub_principal());
    let current = current_resolved.activate(1, HashMap::new());
    let diff = diff_counts(&current, &proposed);
    let applied = proposed.clone().activate(2, HashMap::new());
    let rediff = diff_counts(&applied, &proposed);

    diff.removed == 1
        && diff.added == 0
        && diff.updated == 0
        && rediff.added == 0
        && rediff.updated == 0
        && rediff.removed == 0
}

async fn slot_panic_restarts_behavior(node: &defra_node::EmbeddedNode) -> bool {
    let behavior = Arc::new(
        PendingAgentBehavior::new("general")
            .build_with_identity_for_test(test_identity("pairing-contract-slot-crash")),
    );
    let tool_surface = Arc::new(behavior.tools.resolve(node).await.unwrap());
    let starts = Arc::new(AtomicUsize::new(0));
    let (starts_tx, mut starts_rx) = watch::channel(0usize);
    let runner = {
        let starts = starts.clone();
        let starts_tx = starts_tx.clone();
        move |_behavior: Arc<AgentBehavior>,
              _tool_surface: Arc<ToolSurface>,
              request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
              mut shutdown: watch::Receiver<bool>| {
            let starts = starts.clone();
            let starts_tx = starts_tx.clone();
            async move {
                let attempt = starts.fetch_add(1, Ordering::SeqCst) + 1;
                starts_tx.send_replace(attempt);
                if attempt == 1 {
                    panic!("contract probe panic");
                }
                loop {
                    tokio::select! {
                        _ = shutdown.changed() => return Ok(()),
                        message = async {
                            let mut receiver = request_rx.lock().await;
                            receiver.recv().await
                        } => {
                            if message.is_none() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let slot = spawn_slot(
        behavior,
        tool_surface,
        crate::retry::RetryPolicy {
            max_retries: 1,
            base_delay_ms: 1,
            max_delay_ms: 1,
        },
        runner,
        shutdown_rx,
    );

    let restarted = tokio::time::timeout(
        Duration::from_secs(30),
        starts_rx.wait_for(|starts| *starts >= 2),
    )
    .await
    .is_ok_and(|result| result.is_ok());
    let _ = shutdown_tx.send(true);
    retire_slot(slot);
    restarted
}

#[tokio::test]
async fn behavior_slot_fans_out_background_children_to_backend_capacity() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();

    let mut behavior = PendingAgentBehavior::new("general")
        .build_with_identity_for_test(test_identity("background-fanout"));
    behavior.backend_id = Some("backend-wide".to_string());
    let snapshot = snapshot_for_behaviors_with_admission(
        node.as_ref(),
        "general",
        vec![Arc::new(behavior)],
        HashMap::from([(
            "backend-wide".to_string(),
            backend_admission_config("backend-wide", 3, 100),
        )]),
    )
    .await;

    let (started_tx, mut started_rx) = mpsc::channel::<String>(8);
    let release = Arc::new(Notify::new());
    let runner = {
        let release = release.clone();
        move |_behavior: Arc<AgentBehavior>,
              _tool_surface: Arc<ToolSurface>,
              request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
              mut shutdown: watch::Receiver<bool>| {
            let started_tx = started_tx.clone();
            let release = release.clone();
            async move {
                loop {
                    let message = tokio::select! {
                        _ = shutdown.changed() => return Ok(()),
                        message = async {
                            let mut receiver = request_rx.lock().await;
                            receiver.recv().await
                        } => message,
                    };
                    let Some(request) = message else {
                        return Ok(());
                    };
                    started_tx
                        .send(request.request_id)
                        .await
                        .expect("test receiver should stay open");
                    tokio::select! {
                        _ = shutdown.changed() => return Ok(()),
                        _ = release.notified() => {}
                    }
                }
            }
        }
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let slots = spawn_slots(
        &snapshot,
        crate::retry::RetryPolicy {
            max_retries: 1,
            base_delay_ms: 1,
            max_delay_ms: 1,
        },
        runner,
        shutdown_rx,
        None,
    );
    let dispatcher = slots
        .get("general")
        .expect("general slot")
        .dispatcher
        .clone();

    for index in 0..3 {
        dispatcher
            .send(background_child_request(index, "general"))
            .await
            .unwrap();
    }

    let started = tokio::time::timeout(Duration::from_millis(300), async {
        let mut request_ids = BTreeSet::new();
        while request_ids.len() < 3 {
            let request_id = started_rx
                .recv()
                .await
                .expect("runner should report started requests");
            request_ids.insert(request_id);
        }
        request_ids
    })
    .await
    .expect("executor should start all same-behavior background children concurrently");

    assert_eq!(
        started,
        BTreeSet::from([
            "child-request-0".to_string(),
            "child-request-1".to_string(),
            "child-request-2".to_string(),
        ])
    );

    release.notify_waiters();
    let _ = shutdown_tx.send(true);
    for slot in slots.into_values() {
        retire_slot(slot);
    }
}

#[tokio::test]
async fn generation_supervisor_rotates_dispatcher_on_backend_capacity_change() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let agent_did = "did:test:reconcile-capacity-test";
    let runtime_status = RuntimeStatusHandle::new(node.clone(), agent_did);

    let mut behavior = PendingAgentBehavior::new("general")
        .build_with_identity_for_test(test_identity("capacity-general"));
    behavior.backend_id = Some("backend-general".to_string());
    let behavior = Arc::new(behavior);
    let initial_snapshot = snapshot_for_behaviors_with_admission(
        node.as_ref(),
        "general",
        vec![behavior.clone()],
        HashMap::from([(
            "backend-general".to_string(),
            backend_admission_config("backend-general", 1, 100),
        )]),
    )
    .await;
    let updated_snapshot = snapshot_for_behaviors_with_admission(
        node.as_ref(),
        "general",
        vec![behavior],
        HashMap::from([(
            "backend-general".to_string(),
            backend_admission_config("backend-general", 3, 100),
        )]),
    )
    .await;

    let runner = move |_behavior: Arc<AgentBehavior>,
                       _tool_surface: Arc<ToolSurface>,
                       request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
                       mut shutdown: watch::Receiver<bool>| async move {
        loop {
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                message = async {
                    let mut receiver = request_rx.lock().await;
                    receiver.recv().await
                } => {
                    if message.is_none() {
                        return Ok(());
                    }
                }
            }
        }
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = GenerationSupervisor::bootstrap(
        initial_snapshot,
        crate::admission::AdmissionRegistry::new(node.clone()),
        crate::retry::RetryPolicy {
            max_retries: 3,
            base_delay_ms: 5,
            max_delay_ms: 25,
        },
        runner,
        runtime_status,
        shutdown_rx.clone(),
        None,
    )
    .unwrap();
    let active_snapshot = supervisor.current_snapshot();
    let initial_dispatcher = active_snapshot
        .dispatchers
        .get("general")
        .expect("initial general dispatcher")
        .clone();
    assert_eq!(
        active_snapshot
            .behavior_executor_capacities
            .get("general")
            .copied(),
        Some(1)
    );
    let (active_tx, mut active_rx) = watch::channel(active_snapshot);
    let (proposal_tx, proposal_rx) = mpsc::channel(4);

    let task = tokio::spawn(supervisor.run(active_tx, proposal_rx, shutdown_rx));

    proposal_tx.send(updated_snapshot).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), active_rx.changed())
        .await
        .expect("capacity update should publish")
        .unwrap();
    let updated_active = active_rx.borrow().clone();
    let updated_dispatcher = updated_active
        .dispatchers
        .get("general")
        .expect("updated general dispatcher");
    assert!(!initial_dispatcher.same_channel(updated_dispatcher));
    assert_eq!(
        updated_active
            .behavior_executor_capacities
            .get("general")
            .copied(),
        Some(3)
    );

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("supervisor should stop on shutdown")
        .unwrap()
        .unwrap();
}

#[derive(Debug, serde::Deserialize)]
struct RuntimeStatusRow {
    reconcile_phase: String,
    active_generation: i64,
    last_reconcile_result: String,
    last_reconcile_error: String,
}

async fn fetch_runtime_status(
    node: &defra_node::EmbeddedNode,
    agent_did: &str,
) -> RuntimeStatusRow {
    let agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentRuntime(filter: {{ agent_did: {{ _eq: "{agent_did}" }} }}, limit: 1) {{
                reconcile_phase
                active_generation
                last_reconcile_result
                last_reconcile_error
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    assert!(
        !response.has_errors(),
        "AgentRuntime query failed: {:?}",
        response.errors
    );
    let value = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentRuntime"))
        .and_then(|rows| rows.as_array())
        .and_then(|rows| rows.first())
        .cloned()
        .expect("AgentRuntime row");
    serde_json::from_value(value).expect("decode AgentRuntime row")
}

#[tokio::test]
async fn generation_supervisor_rotates_dispatcher_on_behavior_change() {
    let publish = lean_runtime_reconcile_case("publish_changed_snapshot");
    assert!(publish.legal);

    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let agent_did = "did:test:reconcile-test";
    let runtime_status = RuntimeStatusHandle::new(node.clone(), agent_did);

    let starts = Arc::new(StdMutex::new(HashMap::<String, usize>::new()));
    let mut initial_behavior =
        PendingAgentBehavior::new("general").build_with_identity_for_test(test_identity("general"));
    initial_behavior.system_prompt = "initial prompt".to_string();
    let mut updated_behavior =
        PendingAgentBehavior::new("general").build_with_identity_for_test(test_identity("general"));
    updated_behavior.system_prompt = "updated prompt".to_string();

    let initial_snapshot =
        snapshot_for_behaviors(node.as_ref(), "general", vec![Arc::new(initial_behavior)]).await;
    let updated_snapshot =
        snapshot_for_behaviors(node.as_ref(), "general", vec![Arc::new(updated_behavior)]).await;

    let runner = {
        let starts = starts.clone();
        move |behavior: Arc<AgentBehavior>,
              _tool_surface: Arc<ToolSurface>,
              request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
              mut shutdown: watch::Receiver<bool>| {
            let starts = starts.clone();
            async move {
                *starts
                    .lock()
                    .unwrap()
                    .entry(behavior.behavior_id.clone())
                    .or_default() += 1;
                loop {
                    tokio::select! {
                        _ = shutdown.changed() => return Ok(()),
                        message = async {
                            let mut receiver = request_rx.lock().await;
                            receiver.recv().await
                        } => {
                            if message.is_none() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = GenerationSupervisor::bootstrap(
        initial_snapshot,
        crate::admission::AdmissionRegistry::new(node.clone()),
        crate::retry::RetryPolicy {
            max_retries: 3,
            base_delay_ms: 5,
            max_delay_ms: 25,
        },
        runner,
        runtime_status.clone(),
        shutdown_rx.clone(),
        None,
    )
    .unwrap();
    let active_snapshot = supervisor.current_snapshot();
    assert_eq!(
        active_snapshot.generation,
        publish.pre_active_generation as u64
    );
    assert!(active_snapshot.dispatchers.contains_key("general"));
    let (active_tx, mut active_rx) = watch::channel(active_snapshot);
    let (proposal_tx, proposal_rx) = mpsc::channel(4);

    let task = tokio::spawn(supervisor.run(active_tx, proposal_rx, shutdown_rx));

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if starts
                .lock()
                .unwrap()
                .get("general")
                .copied()
                .unwrap_or_default()
                >= 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("initial behavior slot should start");

    proposal_tx.send(updated_snapshot).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), active_rx.changed())
        .await
        .expect("generation update should publish")
        .unwrap();
    let updated_active = active_rx.borrow().clone();
    assert_eq!(
        updated_active.generation,
        publish.post_active_generation as u64
    );
    assert_eq!(
        updated_active
            .behaviors
            .get("general")
            .expect("updated behavior")
            .system_prompt,
        "updated prompt"
    );
    let status = fetch_runtime_status(node.as_ref(), agent_did).await;
    assert_eq!(status.reconcile_phase, publish.post_phase.as_str());
    assert_eq!(
        status.active_generation,
        publish.post_active_generation as i64
    );
    assert_eq!(status.last_reconcile_result, "applied");
    assert!(status.last_reconcile_error.is_empty());

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if starts
                .lock()
                .unwrap()
                .get("general")
                .copied()
                .unwrap_or_default()
                >= 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement behavior slot should start");

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("supervisor should stop on shutdown")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn generation_supervisor_keeps_previous_generation_after_failed_apply() {
    let apply_failed = lean_runtime_reconcile_case("apply_failed_clears_pending");
    assert!(apply_failed.legal);

    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let agent_did = "did:test:reconcile-failure-test";
    let runtime_status = RuntimeStatusHandle::new(node.clone(), agent_did);

    let initial_behavior = PendingAgentBehavior::new("general")
        .build_with_identity_for_test(test_identity("general-initial"));
    let mut updated_behavior = PendingAgentBehavior::new("general")
        .build_with_identity_for_test(test_identity("general-updated"));
    updated_behavior.system_prompt = "updated prompt".to_string();

    let initial_snapshot =
        snapshot_for_behaviors(node.as_ref(), "general", vec![Arc::new(initial_behavior)]).await;
    let valid_updated_snapshot =
        snapshot_for_behaviors(node.as_ref(), "general", vec![Arc::new(updated_behavior)]).await;
    let invalid_snapshot = ResolvedRuntimeSnapshot::from_parts(
        "general".to_string(),
        valid_updated_snapshot.behaviors.values().cloned().collect(),
        HashMap::new(),
        HashMap::new(),
    )
    .with_principal(stub_principal());

    let runner = move |_behavior: Arc<AgentBehavior>,
                       _tool_surface: Arc<ToolSurface>,
                       request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
                       mut shutdown: watch::Receiver<bool>| async move {
        loop {
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                message = async {
                    let mut receiver = request_rx.lock().await;
                    receiver.recv().await
                } => {
                    if message.is_none() {
                        return Ok(());
                    }
                }
            }
        }
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = GenerationSupervisor::bootstrap(
        initial_snapshot,
        crate::admission::AdmissionRegistry::new(node.clone()),
        crate::retry::RetryPolicy {
            max_retries: 3,
            base_delay_ms: 5,
            max_delay_ms: 25,
        },
        runner,
        runtime_status.clone(),
        shutdown_rx.clone(),
        None,
    )
    .unwrap();
    let initial_active = supervisor.current_snapshot();
    let (active_tx, mut active_rx) = watch::channel(initial_active.clone());
    let (proposal_tx, proposal_rx) = mpsc::channel(4);

    let task = tokio::spawn(supervisor.run(active_tx, proposal_rx, shutdown_rx));

    proposal_tx.send(invalid_snapshot).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!active_rx.has_changed().unwrap());
    assert_eq!(
        active_rx.borrow().generation,
        apply_failed.post_active_generation as u64
    );
    let failed_status = fetch_runtime_status(node.as_ref(), agent_did).await;
    assert_eq!(
        failed_status.reconcile_phase,
        apply_failed.post_phase.as_str()
    );
    assert_eq!(failed_status.active_generation, 0);
    assert_eq!(failed_status.last_reconcile_result, "error");
    assert!(!failed_status.last_reconcile_error.is_empty());

    proposal_tx.send(valid_updated_snapshot).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), active_rx.changed())
        .await
        .expect("valid update should publish after failed apply")
        .unwrap();
    assert_eq!(active_rx.borrow().generation, 2);
    let recovered_status = fetch_runtime_status(node.as_ref(), agent_did).await;
    assert_eq!(recovered_status.reconcile_phase, "idle");
    assert_eq!(recovered_status.active_generation, 2);
    assert_eq!(recovered_status.last_reconcile_result, "applied");
    assert!(recovered_status.last_reconcile_error.is_empty());

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("supervisor should stop on shutdown")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn generation_supervisor_rotates_dispatcher_on_tool_surface_change() {
    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let agent_did = "did:test:reconcile-tool-surface-test";
    let runtime_status = RuntimeStatusHandle::new(node.clone(), agent_did);
    let identity = Arc::new(test_identity("tool-surface-general"));
    let principal = Arc::new(AgentPrincipal {
        agent_did: identity.did().to_string(),
        identity: identity.clone(),
        default_behavior_id: String::new(),
        display_name: None,
        enabled: true,
    });

    let initial_behavior = Arc::new(AgentBehavior {
        skills: Vec::new(),
        behavior_id: "general".to_string(),
        principal: principal.clone(),
        backend_id: Some("backend-general".to_string()),
        backend_provider_kind: BackendProviderKind::OpenAiCompatible,
        openai_wire_api: crate::OpenAiWireApi::ChatCompletions,
        backend_endpoint: "http://127.0.0.1:8999/v1".to_string(),
        backend_api_key: None,
        backend_api_key_env_var: None,
        model_name: "default".to_string(),
        context_window: crate::config::DEFAULT_CONTEXT_WINDOW,
        max_output_tokens: crate::config::DEFAULT_MAX_OUTPUT_TOKENS,
        max_turns: crate::config::DEFAULT_MAX_TURNS,
        system_prompt: "initial".to_string(),
        request_context_template: None,
        tools: BehaviorToolConfig::meta_only(),
        compaction_threshold: crate::config::DEFAULT_COMPACTION_THRESHOLD,
        compaction_strategy: crate::compaction::CompactionStrategy::StripThenSummarize,
        stream_batch_ms: crate::config::DEFAULT_STREAM_BATCH_MS,
        stream_liveness_timeout: Duration::from_secs(
            crate::config::DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
        ),
        deadline_duration: Duration::from_secs(crate::config::DEFAULT_DEADLINE_DURATION_SECS),
        completion_retry: crate::agent::completion_retry::CompletionRetryProfileFields::default(),
        sampling: crate::config::SamplingConfig::default(),
    });
    let updated_behavior = Arc::new(AgentBehavior {
        skills: Vec::new(),
        behavior_id: "general".to_string(),
        principal: principal.clone(),
        backend_id: Some("backend-general".to_string()),
        backend_provider_kind: BackendProviderKind::OpenAiCompatible,
        openai_wire_api: crate::OpenAiWireApi::ChatCompletions,
        backend_endpoint: "http://127.0.0.1:8999/v1".to_string(),
        backend_api_key: None,
        backend_api_key_env_var: None,
        model_name: "default".to_string(),
        context_window: crate::config::DEFAULT_CONTEXT_WINDOW,
        max_output_tokens: crate::config::DEFAULT_MAX_OUTPUT_TOKENS,
        max_turns: crate::config::DEFAULT_MAX_TURNS,
        system_prompt: "initial".to_string(),
        request_context_template: None,
        tools: BehaviorToolConfig::from_selection(
            "general",
            ToolSelection {
                file_tools: FileToolMode::ReadOnly,
                file_tool_root: None,
                bash: crate::tool_surface::BashMode::Off,
                command_policy: None,
                cli_tool_names: Vec::new(),
                enable_meta_tools: false,
                allowed_mcp_service_ids: Vec::new(),
                backgroundable_tool_names: Vec::new(),
                approval_required_tools: Vec::new(),
                enable_memory: false,
                enable_session_history_tool: false,
                enable_context_budget: true,
                enable_defra_query: false,
                defra_query_collections: Vec::new(),
                write_tools: Vec::new(),
                query_tools: Vec::new(),
                enable_self_config: false,
                self_config_categories: None,
                self_config_no_lockout: false,
                self_config_dry_run: false,
                enable_lsp: false,
                lsp_config: None,
            },
            &ToolCeiling::readonly(),
            Vec::new(),
        )
        .unwrap(),
        compaction_threshold: crate::config::DEFAULT_COMPACTION_THRESHOLD,
        compaction_strategy: crate::compaction::CompactionStrategy::StripThenSummarize,
        stream_batch_ms: crate::config::DEFAULT_STREAM_BATCH_MS,
        stream_liveness_timeout: Duration::from_secs(
            crate::config::DEFAULT_STREAM_LIVENESS_TIMEOUT_SECS,
        ),
        deadline_duration: Duration::from_secs(crate::config::DEFAULT_DEADLINE_DURATION_SECS),
        completion_retry: crate::agent::completion_retry::CompletionRetryProfileFields::default(),
        sampling: crate::config::SamplingConfig::default(),
    });

    let initial_snapshot =
        snapshot_for_behaviors(node.as_ref(), "general", vec![initial_behavior]).await;
    let updated_snapshot =
        snapshot_for_behaviors(node.as_ref(), "general", vec![updated_behavior]).await;

    let observed_tool_names = Arc::new(StdMutex::new(Vec::<Vec<String>>::new()));
    let runner = {
        let observed_tool_names = observed_tool_names.clone();
        move |_behavior: Arc<AgentBehavior>,
              tool_surface: Arc<ToolSurface>,
              request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
              mut shutdown: watch::Receiver<bool>| {
            let observed_tool_names = observed_tool_names.clone();
            async move {
                observed_tool_names
                    .lock()
                    .unwrap()
                    .push(tool_surface.tool_names());
                loop {
                    tokio::select! {
                        _ = shutdown.changed() => return Ok(()),
                        message = async {
                            let mut receiver = request_rx.lock().await;
                            receiver.recv().await
                        } => {
                            if message.is_none() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = GenerationSupervisor::bootstrap(
        initial_snapshot,
        crate::admission::AdmissionRegistry::new(node.clone()),
        crate::retry::RetryPolicy {
            max_retries: 3,
            base_delay_ms: 5,
            max_delay_ms: 25,
        },
        runner,
        runtime_status.clone(),
        shutdown_rx.clone(),
        None,
    )
    .unwrap();
    let active_snapshot = supervisor.current_snapshot();
    let (active_tx, mut active_rx) = watch::channel(active_snapshot);
    let (proposal_tx, proposal_rx) = mpsc::channel(4);

    let task = tokio::spawn(supervisor.run(active_tx, proposal_rx, shutdown_rx));

    proposal_tx.send(updated_snapshot).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), active_rx.changed())
        .await
        .expect("tool-surface update should publish")
        .unwrap();
    assert_eq!(active_rx.borrow().generation, 2);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if observed_tool_names
                .lock()
                .unwrap()
                .iter()
                .any(|tool_names| tool_names.contains(&"read_file".to_string()))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement slot should observe file tools");

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("supervisor should stop on shutdown")
        .unwrap()
        .unwrap();
}

/// #559: a slot retired by a generation change must release its behavior from
/// the startup barrier (superseded) instead of orphaning the pending entry —
/// the policy's retirement hook is the only path that knowledge can take.
#[tokio::test]
async fn retiring_a_slot_notifies_the_failure_policy() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordingPolicy {
        retired: std::sync::Mutex<Vec<(String, bool)>>,
        demote_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl slot::SlotFailurePolicy for RecordingPolicy {
        fn build_failure_budget(&self) -> u32 {
            3
        }
        async fn try_demote(&self, _behavior_id: &str, _error: &str) -> bool {
            self.demote_calls.fetch_add(1, Ordering::SeqCst);
            false
        }
        async fn on_slot_retired(&self, behavior_id: &str, recreated: bool) {
            self.retired
                .lock()
                .expect("retired mutex")
                .push((behavior_id.to_string(), recreated));
        }
    }

    let node = test_node().await;
    ensure_runtime_schemas(node.as_ref()).await.unwrap();
    let runtime_status =
        crate::runtime_status::RuntimeStatusHandle::new(node.clone(), "did:test:policy");
    let behavior = Arc::new(
        PendingAgentBehavior::new("general")
            .build_with_identity_for_test(test_identity("policy-retirement-559")),
    );
    let initial_snapshot =
        snapshot_for_behaviors(node.as_ref(), "general", vec![behavior.clone()]).await;
    // A runner that parks until shutdown: the behavior never "starts", exactly
    // the mid-startup window the retirement release exists for.
    let runner = |_behavior: Arc<AgentBehavior>,
                  _tool_surface: Arc<ToolSurface>,
                  _request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
                  mut shutdown: watch::Receiver<bool>| async move {
        let _ = shutdown.changed().await;
        Ok(())
    };

    let policy = Arc::new(RecordingPolicy {
        retired: std::sync::Mutex::new(Vec::new()),
        demote_calls: AtomicUsize::new(0),
    });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = GenerationSupervisor::bootstrap(
        initial_snapshot,
        crate::admission::AdmissionRegistry::new(node.clone()),
        crate::retry::RetryPolicy {
            max_retries: 1,
            base_delay_ms: 1,
            max_delay_ms: 2,
        },
        runner,
        runtime_status,
        shutdown_rx.clone(),
        Some(policy.clone() as Arc<dyn slot::SlotFailurePolicy>),
    )
    .unwrap();
    let (active_tx, mut active_rx) = watch::channel(supervisor.current_snapshot());
    let (proposal_tx, proposal_rx) = mpsc::channel(4);
    let task = tokio::spawn(supervisor.run(active_tx, proposal_rx, shutdown_rx));

    // A generation without the behavior: its slot is retired outright.
    let empty_snapshot = snapshot_for_behaviors(node.as_ref(), "general", vec![]).await;
    proposal_tx.send(empty_snapshot).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), active_rx.changed())
        .await
        .expect("removal generation should publish")
        .unwrap();

    // The retirement hook runs on a spawned task; give it a beat.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if policy
            .retired
            .lock()
            .expect("retired mutex")
            .contains(&("general".to_string(), false))
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "retirement must notify the policy so the barrier is released"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("supervisor should stop on shutdown")
        .unwrap()
        .unwrap();
}
