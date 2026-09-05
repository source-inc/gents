use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::context::{RuntimeContext, StartupBarrier};
use crate::admission::{AdmissionRegistry, BackendAdmissionConfig};
use crate::agent::reconcile::GenerationSupervisor;
use crate::agent::{
    DocumentResolveContext, Gents, ProcessLifecycleObserver, ProcessLifecycleState,
};
use crate::backend_registry;
use crate::health_checker::{spawn_health_checker, ServiceHealthMap};
use crate::runtime_snapshot::ResolvedRuntimeSnapshot;
use crate::runtime_status::{ReconcilePhase, RuntimeStatusHandle};
use crate::tool_surface::{ToolRuntimeContext, ToolSurface};

enum BackgroundTaskResult {
    Router(Result<()>),
    ExecutorStatus(Result<()>),
    Reconcile(Result<()>),
    Control(Result<()>),
    SubagentCompletion(Result<()>),
    GraphRunReconcile(Result<()>),
    CrossDeploymentCancelMirror(Result<()>),
    PairingReconcile(Result<()>),
    EnrollmentReconcile(Result<()>),
    RegistryHeartbeat(Result<()>),
    EndpointHeartbeat(Result<()>),
    SessionHydrationReconcile(Result<()>),
    PersonaRequestReconcile(Result<()>),
    DirectoryProjection(Result<()>),
}

#[cfg(test)]
pub(super) type TestSlotRunner = Arc<
    dyn Fn(
            u64,
            watch::Receiver<bool>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

struct StartupSlotFailurePolicy {
    barrier: Arc<StartupBarrier>,
    runtime_status: RuntimeStatusHandle,
    admission_gate: super::router::RuntimeAdmissionGate,
    runtime_cancel: CancellationToken,
    fatal_error: mpsc::UnboundedSender<anyhow::Error>,
    observer: Option<Arc<dyn crate::startup_readiness::StartupBuildFailureObserver>>,
    budget: u32,
}

#[async_trait::async_trait]
impl crate::agent::reconcile::SlotFailurePolicy for StartupSlotFailurePolicy {
    fn build_failure_budget(&self) -> u32 {
        self.budget.max(1)
    }

    fn on_build_failure(&self, behavior_id: &str, failure_number: u32, error: &str) {
        if let Some(observer) = &self.observer {
            observer.on_build_failure(
                behavior_id,
                failure_number,
                self.build_failure_budget(),
                error,
            );
        }
    }

    async fn on_slot_created(&self, behavior_id: &str, generation: u64) -> Result<()> {
        self.runtime_status
            .readiness()
            .register_slot(behavior_id, generation)
            .await
            .with_context(|| {
                format!("register behavior readiness slot {behavior_id} generation {generation}")
            })?;
        self.barrier
            .register_behavior(behavior_id, generation)
            .await;
        Ok(())
    }

    async fn try_demote(&self, behavior_id: &str, generation: u64, error: &str) -> Result<bool> {
        if !self.barrier.is_pending(behavior_id, generation).await {
            return Ok(false);
        }
        let reason = format!(
            "demoted after {} consecutive startup build failures; last error: {error}",
            self.build_failure_budget()
        );
        let applied = match self
            .runtime_status
            .readiness()
            .demote_slot(behavior_id, generation, reason)
            .await
        {
            Ok(applied) => applied,
            Err(error) => {
                // The executor has exhausted its restart budget and is about
                // to park. If that veto cannot become durable, close routing
                // before returning so the last Ready observation can never
                // admit another request into its dead queue. Runtime teardown
                // will then publish a terminal state or fail boundedly.
                self.admission_gate.close().await;
                let rendered = format!("{error:#}");
                if self.fatal_error.send(error).is_err() {
                    tracing::error!(
                        error = %rendered,
                        "runtime coordinator stopped before demotion failure was delivered"
                    );
                }
                self.runtime_cancel.cancel();
                return Err(anyhow!(rendered));
            }
        };
        if !applied {
            return Ok(false);
        }
        self.barrier
            .mark_behavior_demoted(behavior_id, generation)
            .await;
        tracing::error!(
            behavior_id = %behavior_id,
            budget = self.build_failure_budget(),
            error = %error,
            "behavior demoted: its completion client failed to build repeatedly; \
             the process will report Ready without it. Fix the behavior/backend \
             config — a config change re-admits it with a fresh budget."
        );
        Ok(true)
    }

    async fn on_slot_retired(&self, behavior_id: &str, generation: u64, recreated: bool) {
        if let Err(error) = self
            .runtime_status
            .readiness()
            .retire_slot(behavior_id, generation)
            .await
        {
            tracing::error!(behavior_id, generation, error = %error, "failed to retire behavior readiness slot");
        }
        self.barrier
            .mark_behavior_superseded(behavior_id, generation)
            .await;
        let _ = recreated;
    }
}

/// Sole owner of the process-state/admission ordering boundary.
///
/// Ready is persisted before the gate opens. Beginning shutdown marks the
/// lifecycle terminal under the same mutex, closes the gate (waiting for any
/// in-progress routing admission), and only then publishes ShuttingDown.
struct RuntimeLifecycleCoordinator {
    closing: Mutex<bool>,
    admission_gate: super::router::RuntimeAdmissionGate,
    runtime_status: RuntimeStatusHandle,
    observer: Option<Arc<dyn ProcessLifecycleObserver>>,
}

impl RuntimeLifecycleCoordinator {
    fn new(
        admission_gate: super::router::RuntimeAdmissionGate,
        runtime_status: RuntimeStatusHandle,
        observer: Option<Arc<dyn ProcessLifecycleObserver>>,
    ) -> Self {
        Self {
            closing: Mutex::new(false),
            admission_gate,
            runtime_status,
            observer,
        }
    }

    async fn become_ready(&self) -> Result<bool> {
        let closing = self.closing.lock().await;
        if *closing {
            return Ok(false);
        }
        self.runtime_status
            .set_process_state_durable(ProcessLifecycleState::Ready)
            .await?;
        self.admission_gate.open().await;
        if let Some(observer) = &self.observer {
            observer.on_process_state_change(ProcessLifecycleState::Ready);
        }
        Ok(true)
    }

    async fn begin_shutdown(&self) -> Result<()> {
        let mut closing = self.closing.lock().await;
        if *closing {
            return Ok(());
        }
        *closing = true;
        self.admission_gate.close().await;
        self.runtime_status
            .set_process_state_durable(ProcessLifecycleState::ShuttingDown)
            .await?;
        if let Some(observer) = &self.observer {
            observer.on_process_state_change(ProcessLifecycleState::ShuttingDown);
        }
        Ok(())
    }

    async fn finish_shutdown(&self) -> Result<()> {
        let _closing = self.closing.lock().await;
        self.runtime_status
            .set_process_state_durable(ProcessLifecycleState::Shutdown)
            .await?;
        if let Some(observer) = &self.observer {
            observer.on_process_state_change(ProcessLifecycleState::Shutdown);
        }
        Ok(())
    }
}

pub(in crate::agent) async fn run_agent(
    agent: Gents,
    external_shutdown: watch::Receiver<bool>,
) -> Result<()> {
    crate::migration::ensure_all_runtime_migrations(agent.node.clone())
        .await
        .context("ensure runtime schema migrations")?;
    let (runtime_status_owner, runtime_status) =
        RuntimeStatusHandle::start(agent.node.clone(), agent.agent_did().to_string());
    run_agent_with_runtime_status(
        agent,
        external_shutdown,
        runtime_status_owner,
        runtime_status,
        #[cfg(test)]
        None,
    )
    .await
}

#[cfg(test)]
pub(super) async fn run_agent_with_readiness_writer(
    agent: Gents,
    external_shutdown: watch::Receiver<bool>,
    writer: Arc<dyn crate::behavior_readiness_publisher::BehaviorReadinessWriter>,
    retry_delay: std::time::Duration,
) -> Result<()> {
    run_agent_with_readiness_writer_and_slot_runner(
        agent,
        external_shutdown,
        writer,
        retry_delay,
        None,
    )
    .await
}

#[cfg(test)]
pub(super) async fn run_agent_with_readiness_writer_and_slot_runner(
    agent: Gents,
    external_shutdown: watch::Receiver<bool>,
    writer: Arc<dyn crate::behavior_readiness_publisher::BehaviorReadinessWriter>,
    retry_delay: std::time::Duration,
    slot_runner: Option<TestSlotRunner>,
) -> Result<()> {
    crate::migration::ensure_all_runtime_migrations(agent.node.clone())
        .await
        .context("ensure runtime schema migrations")?;
    let (runtime_status_owner, runtime_status) = RuntimeStatusHandle::start_with_readiness_writer(
        agent.node.clone(),
        agent.agent_did().to_string(),
        writer,
        retry_delay,
    );
    run_agent_with_runtime_status(
        agent,
        external_shutdown,
        runtime_status_owner,
        runtime_status,
        slot_runner,
    )
    .await
}

async fn run_agent_with_runtime_status(
    agent: Gents,
    external_shutdown: watch::Receiver<bool>,
    runtime_status_owner: crate::runtime_status::RuntimeStatusOwner,
    runtime_status: RuntimeStatusHandle,
    #[cfg(test)] slot_runner: Option<TestSlotRunner>,
) -> Result<()> {
    let terminal_observer = agent.process_state_observer.clone();
    let initialized = runtime_status
        .initialize_startup(agent.default_behavior_id())
        .await;
    let body_result = match initialized {
        Ok(()) => {
            run_agent_owned(
                agent,
                external_shutdown,
                runtime_status.clone(),
                #[cfg(test)]
                slot_runner,
            )
            .await
        }
        Err(error) => Err(error.context("initialize runtime behavior readiness")),
    };

    finish_run_agent(
        body_result,
        runtime_status_owner,
        runtime_status,
        terminal_observer,
    )
    .await
}

async fn finish_run_agent(
    body_result: Result<()>,
    runtime_status_owner: crate::runtime_status::RuntimeStatusOwner,
    runtime_status: RuntimeStatusHandle,
    terminal_observer: Option<Arc<dyn crate::agent::ProcessLifecycleObserver>>,
) -> Result<()> {
    let mut teardown_error = None;
    let process_state = runtime_status.readiness().observation().process_state();
    if process_state != gents_protocol::row::BehaviorReadinessProcessState::Shutdown {
        if process_state != gents_protocol::row::BehaviorReadinessProcessState::ShuttingDown {
            if let Err(error) = runtime_status
                .set_process_state_durable(ProcessLifecycleState::ShuttingDown)
                .await
            {
                tracing::error!(error = %error, "failed to publish terminal ShuttingDown state");
                teardown_error = Some(error);
            } else if let Some(observer) = &terminal_observer {
                observer.on_process_state_change(ProcessLifecycleState::ShuttingDown);
            }
        }
        if let Err(error) = runtime_status
            .set_process_state_durable(ProcessLifecycleState::Shutdown)
            .await
        {
            tracing::error!(error = %error, "failed to publish terminal Shutdown state");
            if teardown_error.is_none() {
                teardown_error = Some(error);
            }
        } else if let Some(observer) = &terminal_observer {
            observer.on_process_state_change(ProcessLifecycleState::Shutdown);
        }
    }
    if let Err(error) = runtime_status_owner.close().await {
        tracing::error!(error = %error, "failed to close runtime behavior readiness owner");
        if teardown_error.is_none() {
            teardown_error = Some(error);
        }
    }

    match body_result {
        Err(error) => Err(error),
        Ok(()) => teardown_error.map_or(Ok(()), Err),
    }
}

async fn run_agent_owned(
    agent: Gents,
    mut external_shutdown: watch::Receiver<bool>,
    runtime_status: RuntimeStatusHandle,
    #[cfg(test)] slot_runner: Option<TestSlotRunner>,
) -> Result<()> {
    let cancel = CancellationToken::new();
    let (fatal_error_tx, mut fatal_error_rx) = mpsc::unbounded_channel();
    // Every owned runtime task listens to this internal signal. If any one
    // background task exits unexpectedly, the coordinator closes admission,
    // broadcasts shutdown to the remaining tasks and awaits their joins.
    let (runtime_shutdown_tx, shutdown) = watch::channel(false);
    runtime_status
        .set_reconcile_phase(ReconcilePhase::Resolving)
        .await;
    if let Some(observer) = &agent.process_state_observer {
        observer.on_process_state_change(ProcessLifecycleState::Recovering);
    }
    let health_map = ServiceHealthMap::new();
    let tool_runtime = ToolRuntimeContext::new_with_agent_did(
        agent.node.clone(),
        agent.mcp_pool.clone(),
        health_map.clone(),
        agent.local_hostname.clone(),
        agent.local_subnet.clone(),
        agent.agent_did().to_string(),
        Some(agent.principal_arc().identity.clone()),
    );
    backend_registry::probe_and_promote_enabled_backends(agent.node.as_ref()).await;

    let resolved_snapshot = match resolve_startup_snapshot(&agent).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            runtime_status.publish_error(&format!("{error:#}")).await;
            return Err(error);
        }
    };
    if let Err(error) = validate_startup_snapshot(&agent, &tool_runtime, &resolved_snapshot).await {
        runtime_status.publish_error(&format!("{error:#}")).await;
        return Err(error);
    }
    // DefraDB's event bus is live-only. Subscribe before the MCP health
    // checker's first cycle can persist a transition that makes a required
    // service (and therefore its dependent behavior) runnable.
    let control_subscription = agent
        .document_runtime_context()
        .is_some()
        .then(|| agent.node.subscribe(&[defra_node::EventName::Update]));
    log_recovery(
        agent.node.as_ref(),
        agent.agent_did(),
        agent.default_behavior_id(),
    )
    .await;
    let local_deployment_id = crate::callback::ensure_local_host_deployment(agent.node.as_ref())
        .await
        .context("ensure local HostDeployment")?;
    for (behavior_id, reason) in &agent.unavailable_behaviors {
        tracing::warn!(
            behavior_id = %behavior_id,
            public_reason = ?reason.public_reason,
            diagnostic = %reason.diagnostic,
            "behavior unavailable at startup"
        );
    }

    let startup_barrier = Arc::new(StartupBarrier::new(
        &resolved_snapshot
            .behaviors
            .values()
            .cloned()
            .collect::<Vec<_>>(),
    ));
    #[cfg(not(test))]
    let admission_gate = super::router::RuntimeAdmissionGate::closed();
    #[cfg(test)]
    let admission_gate = super::router::RuntimeAdmissionGate::closed_with_dispatch_probe(
        agent.router_dispatch_probe.clone(),
    );
    let lifecycle = Arc::new(RuntimeLifecycleCoordinator::new(
        admission_gate.clone(),
        runtime_status.clone(),
        agent.process_state_observer.clone(),
    ));
    let admission_registry = AdmissionRegistry::new(agent.node.clone());
    let (enrollment_owner, enrollment_handle) =
        crate::agent::p2p_reconcile::enrollment_authority_channel();
    let lsp_pool = tool_runtime.lsp_pool.clone();
    let runtime = RuntimeContext {
        node: agent.node.clone(),
        tool_runtime,
        admission_registry: admission_registry.clone(),
        hook_failure_policy: agent.hook_failure_policy,
        rendered_request_capture_factory: agent.rendered_request_capture_factory.clone(),
        background_execution_registry: agent.background_execution_registry.clone(),
        startup_barrier: startup_barrier.clone(),
        startup_readiness: agent.startup_readiness.clone(),
        runtime_status: runtime_status.clone(),
        operator_tool_root: agent.operator_tool_root().map(PathBuf::from),
        enrollment_authority: enrollment_handle.clone(),
    };
    let runtime_for_runner = runtime.clone();
    #[cfg(test)]
    let test_slot_runner = slot_runner;
    let readiness_fatal_error = fatal_error_tx.clone();
    let slot_failure_policy: Arc<dyn crate::agent::reconcile::SlotFailurePolicy> =
        Arc::new(StartupSlotFailurePolicy {
            barrier: startup_barrier.clone(),
            runtime_status: runtime_status.clone(),
            admission_gate: admission_gate.clone(),
            runtime_cancel: cancel.clone(),
            fatal_error: fatal_error_tx,
            observer: agent.startup_build_failure_observer.clone(),
            budget: agent.startup_readiness.build_failure_budget,
        });
    let generation_supervisor = GenerationSupervisor::bootstrap(
        resolved_snapshot,
        admission_registry.clone(),
        agent.retry_policy.clone(),
        move |behavior, tool_surface, request_rx, generation, shutdown| {
            let runtime = runtime_for_runner.clone();
            #[cfg(test)]
            let test_slot_runner = test_slot_runner.clone();
            async move {
                #[cfg(test)]
                if let Some(test_slot_runner) = test_slot_runner {
                    return test_slot_runner(generation, shutdown.clone()).await;
                }
                runtime
                    .run_behavior(behavior, tool_surface, request_rx, generation, shutdown)
                    .await
            }
        },
        runtime_status.clone(),
        shutdown.clone(),
        Some(slot_failure_policy),
    )
    .await?;
    let initial_active_snapshot = generation_supervisor.current_snapshot();
    if let Err(error) = runtime_status
        .publish_startup_snapshot(initial_active_snapshot.as_ref())
        .await
    {
        // Bootstrap owns live generation-one slots before the initial source is
        // durable. Transfer them through the same ordered shutdown boundary as
        // the steady-state supervisor; dropping JoinHandles here would detach
        // executors that can outlive the readiness owner.
        runtime_shutdown_tx.send_replace(true);
        cancel.cancel();
        generation_supervisor.shutdown_slots().await;
        return Err(error).context("durably publish startup behavior readiness source");
    }
    let (active_snapshot_tx, active_snapshot_rx) = watch::channel(initial_active_snapshot.clone());
    let (reconcile_tx, reconcile_rx) = mpsc::channel(8);
    let _reconcile_tx_guard = reconcile_tx.clone();
    let health_checker = spawn_health_checker(
        agent.node.clone(),
        agent.mcp_pool.clone(),
        health_map.clone(),
        agent.local_hostname.clone(),
        agent.local_subnet.clone(),
        cancel.child_token(),
        agent.health_checker_options.clone(),
        agent.agent_did().to_string(),
    );
    let (backend_health_events_tx, backend_health_events_rx) = mpsc::channel::<()>(1);
    let backend_prober = crate::backend_health::spawn_backend_prober(
        agent.node.clone(),
        agent.backend_health.clone(),
        agent.backend_prober_options.clone(),
        backend_health_events_tx,
        cancel.child_token(),
        agent.agent_did().to_string(),
    );

    let runtime_snapshot_observer_handle =
        if let Some(observer) = agent.runtime_snapshot_observer.clone() {
            let mut snapshot_rx = active_snapshot_rx.clone();
            let mut observer_shutdown = shutdown.clone();
            Some(tokio::spawn(async move {
                loop {
                    let (generation, fingerprint, runnable) = {
                        let snapshot = snapshot_rx.borrow_and_update();
                        let mut ids: Vec<String> = snapshot.behaviors.keys().cloned().collect();
                        ids.sort();
                        (
                            snapshot.generation,
                            snapshot.configuration_fingerprint(),
                            ids,
                        )
                    };
                    observer.on_generation_published(generation, &fingerprint, &runnable);

                    tokio::select! {
                        changed = snapshot_rx.changed() => {
                            if changed.is_err() {
                                break;
                            }
                        }
                        _ = observer_shutdown.changed() => break,
                    }
                }
            }))
        } else {
            None
        };

    let trigger_engine_node = agent.node.clone();
    let trigger_engine_schedule_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_event_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_goal_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_subagent_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_engine_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_materializer_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_peer_admission: Arc<
        dyn crate::agent::p2p_reconcile::PeerAdmissionAuthority,
    > = Arc::new(enrollment_handle.clone());
    let trigger_engine_cancel = cancel.child_token();
    let trigger_engine_startup_barrier = startup_barrier.clone();
    // Construct the `ManualSource` up-front so the `ManualTriggerHandle` can
    // be published to in-process callers (via the `OnceCell` on
    // `Gents`) before `run()` awaits shutdown. Deferring construction
    // into the spawned task would race the callers that cloned `Gents`
    // and are polling for the handle.
    let (manual_source, manual_trigger_handle) =
        crate::trigger_engine::manual_source::ManualSource::new(trigger_engine_cancel.clone());
    let _ = agent.manual_trigger_handle.set(manual_trigger_handle);
    let trigger_engine_deployment_id = local_deployment_id.clone();
    let trigger_engine_handle = tokio::spawn(async move {
        tokio::select! {
            _ = trigger_engine_cancel.cancelled() => return,
            _ = trigger_engine_startup_barrier.wait_ready() => {}
        }
        match crate::trigger_engine::production_materializer::recover_workspace_binding_pending_requests(
            trigger_engine_node.as_ref(),
            &trigger_engine_deployment_id,
        )
        .await
        {
            Ok(recovered) if recovered > 0 => tracing::info!(
                recovered,
                "recovered workspace-binding-pending requests"
            ),
            Ok(_) => {}
            Err(error) => tracing::warn!(%error, "workspace binding recovery sweep failed"),
        }
        let materializer: Arc<dyn crate::trigger_engine::MaterializerHandle> = Arc::new(
            crate::trigger_engine::production_materializer::ProductionMaterializer::new(
                trigger_engine_node.clone(),
                trigger_engine_materializer_snapshot_rx,
            )
            .with_local_deployment_id(trigger_engine_deployment_id),
        );
        let schedule_source: Box<dyn crate::trigger_engine::TriggerSource> =
            Box::new(crate::trigger_engine::schedule_source::ScheduleSource::new(
                trigger_engine_schedule_snapshot_rx,
                trigger_engine_node.clone(),
                trigger_engine_cancel.clone(),
            ));
        let event_source: Box<dyn crate::trigger_engine::TriggerSource> =
            Box::new(crate::trigger_engine::event_source::EventSource::new(
                trigger_engine_event_snapshot_rx,
                trigger_engine_node.clone(),
                trigger_engine_cancel.clone(),
            ));
        let subagent_source: Box<dyn crate::trigger_engine::TriggerSource> =
            Box::new(crate::trigger_engine::subagent_source::SubagentSource::new(
                trigger_engine_subagent_snapshot_rx,
                trigger_engine_node.clone(),
                trigger_engine_peer_admission,
                trigger_engine_cancel.clone(),
            ));
        let goal_source: Box<dyn crate::trigger_engine::TriggerSource> =
            Box::new(crate::trigger_engine::goal_source::GoalSource::new(
                trigger_engine_goal_snapshot_rx,
                trigger_engine_node.clone(),
                trigger_engine_cancel.clone(),
            ));
        let manual_source_box: Box<dyn crate::trigger_engine::TriggerSource> =
            Box::new(manual_source);
        let sources: Vec<Box<dyn crate::trigger_engine::TriggerSource>> = vec![
            schedule_source,
            event_source,
            goal_source,
            subagent_source,
            manual_source_box,
        ];
        let engine = crate::trigger_engine::TriggerEngine::new(
            trigger_engine_engine_snapshot_rx,
            materializer,
        );
        engine.run(sources, trigger_engine_cancel).await;
    });

    let callback_node = agent.node.clone();
    let callback_deployment_id = local_deployment_id.clone();
    let callback_ceiling = agent
        .document_runtime_context()
        .and_then(|context| context.tool_ceiling.root())
        .map(std::path::Path::to_path_buf);
    crate::workspace::install_process_operator_tool_root(callback_ceiling.clone());
    let callback_cancel = cancel.child_token();
    let callback_startup_barrier = startup_barrier.clone();
    let callback_engine_handle = tokio::spawn(async move {
        tokio::select! {
            _ = callback_cancel.cancelled() => return,
            _ = callback_startup_barrier.wait_ready() => {}
        }
        if let Err(error) = crate::callback::run_callback_engine(
            callback_node,
            callback_deployment_id,
            callback_ceiling,
            callback_cancel,
        )
        .await
        {
            tracing::error!(%error, "callback engine exited");
        }
    });

    let ready_cancel = cancel.child_token();
    let ready_startup_barrier = startup_barrier.clone();
    let ready_lifecycle = lifecycle.clone();
    let ready_behavior_count = initial_active_snapshot.behaviors.len();
    let ready_unavailable_count = initial_active_snapshot.unavailable_behaviors.len();
    let ready_runtime_status = runtime_status.clone();
    let readiness_handle = tokio::spawn(async move {
        let mut watchdog = tokio::time::interval(std::time::Duration::from_secs(60));
        watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        watchdog.tick().await;
        loop {
            tokio::select! {
                _ = ready_cancel.cancelled() => return,
                _ = ready_startup_barrier.wait_ready() => break,
                _ = watchdog.tick() => {
                    let pending = ready_startup_barrier.pending_behaviors().await;
                    tracing::warn!(
                        pending_behaviors = ?pending,
                        "startup readiness barrier is still waiting; a build may be wedged"
                    );
                }
            }
        }
        let became_ready = match ready_lifecycle.become_ready().await {
            Ok(ready) => ready,
            Err(error) => {
                tracing::error!(error = %error, "failed to durably publish runtime readiness");
                let _ = readiness_fatal_error
                    .send(error.context("durably publish runtime Ready state"));
                return;
            }
        };
        if !became_ready {
            return;
        }
        let demoted = ready_runtime_status.readiness().observation();
        let demoted = demoted.demotions();
        if demoted.is_empty() {
            tracing::info!(
                runnable_behaviors = ready_behavior_count,
                unavailable_behaviors = ready_unavailable_count,
                "gents ready"
            );
        } else {
            let mut demoted_ids: Vec<&String> = demoted.keys().collect();
            demoted_ids.sort();
            tracing::warn!(
                runnable_behaviors = ready_behavior_count.saturating_sub(demoted.len()),
                unavailable_behaviors = ready_unavailable_count + demoted.len(),
                demoted_behaviors = ?demoted_ids,
                "gents ready (degraded: startup build failures demoted behaviors)"
            );
        }
    });

    let mut background_tasks = JoinSet::new();

    let completion_node = agent.node.clone();
    let completion_agent_did = agent.agent_did().to_string();
    let completion_background_executions = agent.background_execution_registry.clone();
    let completion_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::SubagentCompletion(
            crate::background_completion::run_background_completion_observer(
                completion_node,
                completion_agent_did,
                completion_background_executions,
                completion_cancel,
            )
            .await,
        )
    });

    let graph_run_node = agent.node.clone();
    let graph_run_owner_did = agent.agent_did().to_string();
    let graph_run_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::GraphRunReconcile(
            crate::graph_pipeline::run_graph_run_reconciler(
                graph_run_node,
                graph_run_owner_did,
                graph_run_cancel,
            )
            .await,
        )
    });

    let cancel_mirror_node = agent.node.clone();
    let cancel_mirror_snapshot_rx = active_snapshot_rx.clone();
    let cancel_mirror_peer_admission: Arc<dyn crate::agent::p2p_reconcile::PeerAdmissionAuthority> =
        Arc::new(enrollment_handle.clone());
    let cancel_mirror_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::CrossDeploymentCancelMirror(
            crate::trigger_engine::cross_deployment_cancel_mirror::run_cross_deployment_cancel_mirror(
                cancel_mirror_node,
                cancel_mirror_snapshot_rx,
                cancel_mirror_peer_admission,
                cancel_mirror_cancel,
            )
            .await,
        )
    });

    let enrollment_node = agent.node.clone();
    let enrollment_identity = agent.principal_arc().identity.clone();
    let enrollment_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::EnrollmentReconcile(
            crate::agent::p2p_reconcile::run_enrollment_reconciler(
                enrollment_node,
                enrollment_identity,
                enrollment_owner,
                enrollment_cancel,
            )
            .await,
        )
    });

    let pairing_node = agent.node.clone();
    let pairing_identity = agent.principal_arc().identity.clone();
    let pairing_enrollment = enrollment_handle.clone();
    let pairing_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::PairingReconcile(
            crate::agent::p2p_reconcile::run_pairing_reconciler(
                pairing_node,
                pairing_identity,
                pairing_enrollment,
                pairing_cancel,
            )
            .await,
        )
    });

    let registry_node = agent.node.clone();
    let registry_agent_did = agent.agent_did().to_string();
    let registry_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::RegistryHeartbeat(
            crate::agent::p2p_reconcile::run_registry_heartbeat(
                registry_node,
                registry_agent_did,
                crate::agent::p2p_reconcile::resolve_network_id(),
                registry_cancel,
            )
            .await,
        )
    });

    let endpoint_node = agent.node.clone();
    let endpoint_identity = agent.principal_arc().identity.clone();
    let endpoint_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::EndpointHeartbeat(
            crate::agent::p2p_reconcile::run_endpoint_heartbeat(
                endpoint_node,
                endpoint_identity,
                endpoint_cancel,
            )
            .await,
        )
    });

    let hydration_node = agent.node.clone();
    let hydration_enrollment = enrollment_handle.clone();
    let hydration_identity = agent.principal_arc().identity.clone();
    let hydration_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::SessionHydrationReconcile(
            crate::agent::p2p_reconcile::run_session_hydration_reconciler(
                hydration_node,
                hydration_enrollment,
                hydration_identity,
                hydration_cancel,
            )
            .await,
        )
    });

    let persona_request_node = agent.node.clone();
    let persona_request_ceiling = agent
        .document_runtime_context()
        .and_then(|context| context.tool_ceiling.root())
        .map(std::path::Path::to_path_buf);
    let persona_request_authority = enrollment_handle;
    let persona_request_identity = agent.principal_arc().identity.clone();
    let persona_request_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::PersonaRequestReconcile(
            crate::agent::p2p_reconcile::run_persona_request_reconciler(
                persona_request_node,
                persona_request_ceiling,
                persona_request_authority,
                persona_request_identity,
                persona_request_cancel,
            )
            .await,
        )
    });

    let directory_node = agent.node.clone();
    let directory_source_did = agent.agent_did().to_string();
    let directory_ceiling = agent
        .document_runtime_context()
        .and_then(|context| context.tool_ceiling.root())
        .map(std::path::Path::to_path_buf);
    let directory_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::DirectoryProjection(
            crate::agent::directory_projection::run_directory_projection(
                directory_node,
                directory_source_did,
                directory_ceiling,
                directory_cancel,
            )
            .await,
        )
    });

    let router_node = agent.node.clone();
    let router_agent_did = agent.agent_did().to_string();
    let router_deployment_id = local_deployment_id.clone();
    let router_active_snapshot_rx = active_snapshot_rx.clone();
    let router_shutdown = shutdown.clone();
    let router_admission_gate = admission_gate.clone();
    let router_runtime_status = runtime_status.clone();
    background_tasks.spawn(async move {
        BackgroundTaskResult::Router(
            super::router::run_router(
                router_node,
                router_agent_did,
                router_deployment_id,
                router_active_snapshot_rx,
                router_shutdown,
                router_admission_gate,
                router_runtime_status,
            )
            .await,
        )
    });

    let executor_status_active_snapshot_rx = active_snapshot_rx.clone();
    let executor_status_runtime_status = runtime_status.clone();
    let executor_status_shutdown = shutdown.clone();
    background_tasks.spawn(async move {
        BackgroundTaskResult::ExecutorStatus(
            crate::runtime_status::run_executor_status_observer(
                executor_status_active_snapshot_rx,
                executor_status_runtime_status,
                executor_status_shutdown,
            )
            .await,
        )
    });

    let reconcile_active_snapshot_tx = active_snapshot_tx.clone();
    let reconcile_shutdown = shutdown.clone();
    background_tasks.spawn(async move {
        BackgroundTaskResult::Reconcile(
            generation_supervisor
                .run(
                    reconcile_active_snapshot_tx,
                    reconcile_rx,
                    reconcile_shutdown,
                )
                .await,
        )
    });

    if agent.document_runtime_context().is_some() {
        let control_node = agent.node.clone();
        let control_subscription =
            control_subscription.expect("document runtime context has control subscription");
        let control_agent_did = agent.agent_did().to_string();
        let control_context = agent
            .document_runtime_context()
            .cloned()
            .expect("checked document runtime context");
        let control_tx = reconcile_tx.clone();
        let control_runtime_status = runtime_status.clone();
        let control_shutdown = shutdown.clone();
        background_tasks.spawn(async move {
            BackgroundTaskResult::Control(
                super::control_watcher::run_control_watcher(
                    control_node,
                    control_subscription,
                    control_agent_did,
                    control_context,
                    control_tx,
                    control_runtime_status,
                    backend_health_events_rx,
                    control_shutdown,
                )
                .await,
            )
        });
    }

    let result = tokio::select! {
        biased;
        Some(error) = fatal_error_rx.recv() => Err(error),
        _ = external_shutdown.changed() => Ok(()),
        Some(joined) = background_tasks.join_next() => match joined {
            Ok(BackgroundTaskResult::Router(result)) => result,
            Ok(BackgroundTaskResult::ExecutorStatus(result)) => result,
            Ok(BackgroundTaskResult::Reconcile(result)) => result,
            Ok(BackgroundTaskResult::Control(result)) => result,
            Ok(BackgroundTaskResult::SubagentCompletion(result)) => result,
            Ok(BackgroundTaskResult::GraphRunReconcile(result)) => result,
            Ok(BackgroundTaskResult::CrossDeploymentCancelMirror(result)) => result,
            Ok(BackgroundTaskResult::PairingReconcile(result)) => result,
            Ok(BackgroundTaskResult::EnrollmentReconcile(result)) => result,
            Ok(BackgroundTaskResult::RegistryHeartbeat(result)) => result,
            Ok(BackgroundTaskResult::EndpointHeartbeat(result)) => result,
            Ok(BackgroundTaskResult::SessionHydrationReconcile(result)) => result,
            Ok(BackgroundTaskResult::PersonaRequestReconcile(result)) => result,
            Ok(BackgroundTaskResult::DirectoryProjection(result)) => result,
            Err(error) => Err(anyhow!("background task join failed: {error}")),
        },
        else => Ok(()),
    };

    let mut teardown_error = None;
    if let Err(error) = lifecycle.begin_shutdown().await {
        tracing::error!(error = %error, "failed to begin ordered runtime shutdown");
        teardown_error = Some(error);
    }

    runtime_shutdown_tx.send_replace(true);
    cancel.cancel();
    while let Some(joined) = background_tasks.join_next().await {
        if let Err(error) = joined {
            if !error.is_cancelled() {
                tracing::error!(error = %error, "background task exited during shutdown");
            }
        }
    }

    let _ = readiness_handle.await;
    let _ = trigger_engine_handle.await;
    let _ = callback_engine_handle.await;
    let _ = health_checker.await;
    let _ = backend_prober.await;
    if let Some(handle) = runtime_snapshot_observer_handle {
        let _ = handle.await;
    }
    lsp_pool.shutdown().await;

    if let Err(error) = lifecycle.finish_shutdown().await {
        tracing::error!(error = %error, "failed to finish ordered runtime shutdown");
        if teardown_error.is_none() {
            teardown_error = Some(error);
        }
    }

    match result {
        Err(error) => Err(error),
        Ok(()) => teardown_error.map_or(Ok(()), Err),
    }
}

async fn log_recovery(node: &defra_node::EmbeddedNode, agent_did: &str, default_behavior_id: &str) {
    // Sweep order lives in `startup_recovery`, not here: the inference-call
    // sweep is parent-gated and must run after request repair (#1001).
    let outcome = crate::startup_recovery::run_startup_recovery(node, agent_did).await;
    let mut recovered_any = false;

    match outcome.tool_calls {
        Ok(report) => {
            if report.tool_calls_recovered > 0 {
                recovered_any = true;
                tracing::info!(
                    agent_did = %agent_did,
                    count = report.tool_calls_recovered,
                    "recovered stuck tool calls"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                agent_did = %agent_did,
                error = %error,
                "startup tool-call recovery failed"
            );
        }
    }

    match outcome.inference_calls {
        Ok(report) => {
            if report.calls_recovered > 0 {
                recovered_any = true;
                tracing::info!(
                    agent_did = %agent_did,
                    count = report.calls_recovered,
                    "recovered stale inference calls"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                agent_did = %agent_did,
                error = %error,
                "startup inference-call recovery failed"
            );
        }
    }

    match outcome.requests {
        Ok(report) => {
            if report.requests_recovered > 0 {
                recovered_any = true;
                tracing::info!(
                    agent_did = %agent_did,
                    count = report.requests_recovered,
                    "recovered stuck requests"
                );
            }
            if report.background_wakes_redriven > 0 {
                recovered_any = true;
                tracing::info!(
                    agent_did = %agent_did,
                    count = report.background_wakes_redriven,
                    "redrove failed background-completion wakes"
                );
            }
            if report.responses_recovered > 0 {
                recovered_any = true;
                tracing::info!(
                    agent_did = %agent_did,
                    count = report.responses_recovered,
                    "recovered stuck responses"
                );
            }
        }
        Err(error) => {
            tracing::warn!(agent_did = %agent_did, error = %error, "startup recovery failed");
        }
    }

    if !recovered_any {
        tracing::debug!(
            agent_did = %agent_did,
            default_behavior_id = %default_behavior_id,
            "startup recovery found no stuck documents"
        );
    }
}

fn is_degraded_startup_unavailable_reason(
    reason: gents_protocol::row::BehaviorReadinessUnavailableReason,
) -> bool {
    use gents_protocol::row::BehaviorReadinessUnavailableReason as Reason;

    matches!(
        reason,
        Reason::BehaviorDisabled
            | Reason::BackendNotConfigured
            | Reason::BackendDisabled
            | Reason::BackendTemporarilyUnavailable
            | Reason::CredentialsRequired
    )
}

async fn validate_startup_snapshot(
    agent: &Gents,
    tool_runtime: &ToolRuntimeContext,
    snapshot: &ResolvedRuntimeSnapshot,
) -> Result<()> {
    snapshot
        .validate_behavior_readiness_source()
        .with_context(|| {
            format!(
                "validate behavior readiness source for {}",
                agent.agent_did()
            )
        })?;

    if snapshot.behaviors.is_empty() {
        let mut unavailable = snapshot
            .unavailable_behaviors
            .iter()
            .map(|(behavior_id, reason)| (behavior_id.clone(), reason.clone()))
            .collect::<Vec<_>>();
        unavailable.sort_by(|left, right| left.0.cmp(&right.0));

        if unavailable.is_empty() {
            anyhow::bail!(
                "agent {} has no runnable behaviors at startup",
                agent.agent_did()
            );
        }

        let blocking = unavailable
            .iter()
            .filter(|(_, reason)| !is_degraded_startup_unavailable_reason(reason.public_reason))
            .map(|(behavior_id, reason)| format!("{behavior_id}: {}", reason.public_message()))
            .collect::<Vec<_>>();
        if !blocking.is_empty() {
            anyhow::bail!(
                "agent {} has no runnable behaviors at startup due to invalid configuration ({})",
                agent.agent_did(),
                blocking.join("; ")
            );
        }
    }

    let mut behavior_ids = snapshot.behaviors.keys().cloned().collect::<Vec<_>>();
    behavior_ids.sort();

    for behavior_id in behavior_ids {
        let tool_surface = snapshot
            .tool_surfaces
            .get(&behavior_id)
            .ok_or_else(|| anyhow!("missing tool surface for behavior {behavior_id}"))?;
        tool_surface
            .build_tools(tool_runtime)
            .with_context(|| format!("building startup tool surface for behavior {behavior_id}"))?;
    }

    Ok(())
}

async fn resolve_tool_surfaces(
    node: &defra_node::EmbeddedNode,
    behaviors: &[Arc<crate::config::AgentBehavior>],
) -> Result<HashMap<String, Arc<ToolSurface>>> {
    let mut tool_surfaces = HashMap::with_capacity(behaviors.len());
    for behavior in behaviors {
        let tool_surface = behavior.tools.resolve(node).await?;
        tool_surfaces.insert(behavior.behavior_id.clone(), Arc::new(tool_surface));
    }
    Ok(tool_surfaces)
}

async fn resolve_startup_snapshot(agent: &Gents) -> Result<ResolvedRuntimeSnapshot> {
    match agent.document_runtime_context() {
        Some(resolve_context) => {
            resolve_document_snapshot_with_tools(agent.node.as_ref(), resolve_context).await
        }
        None => {
            let tool_surfaces =
                resolve_tool_surfaces(agent.node.as_ref(), &agent.behaviors).await?;
            let backend_admission_configs =
                resolve_backend_admission_configs(agent.node.as_ref(), &agent.behaviors).await?;
            Ok(ResolvedRuntimeSnapshot::from_parts_with_admission_configs(
                agent.default_behavior_id().to_string(),
                agent.behaviors.clone(),
                tool_surfaces,
                backend_admission_configs,
                agent.unavailable_behaviors.clone(),
            ))
            .map(|snapshot| {
                snapshot
                    .with_principal(agent.principal_arc())
                    .with_local_did(agent.agent_did().to_string())
            })
        }
    }
}

async fn resolve_backend_admission_configs(
    node: &defra_node::EmbeddedNode,
    behaviors: &[Arc<crate::config::AgentBehavior>],
) -> Result<HashMap<String, BackendAdmissionConfig>> {
    let mut configs = HashMap::new();
    for behavior in behaviors {
        let Some(backend_id) = behavior
            .backend_id
            .as_deref()
            .map(str::trim)
            .filter(|backend_id| !backend_id.is_empty())
        else {
            continue;
        };
        if configs.contains_key(backend_id) {
            continue;
        }
        let (resolved_backend_id, config) = async {
            let backend = backend_registry::lookup_backend(node, backend_id)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "behavior {} references missing backend {}",
                        behavior.behavior_id,
                        backend_id
                    )
                })?;
            tracing::Span::current().record("backend_enabled", backend.enabled);
            tracing::Span::current().record("probe_status", backend.probe_status.as_str());
            tracing::Span::current().record("max_concurrent", backend.max_concurrent);
            tracing::Span::current().record("max_queue_depth", backend.max_queue_depth);
            Ok::<_, anyhow::Error>((
                backend.backend_id.clone(),
                BackendAdmissionConfig::from_backend(&backend)?,
            ))
        }
        .instrument(tracing::info_span!(
            "backend.admission_resolve",
            behavior_id = %behavior.behavior_id,
            backend_id = %backend_id,
            backend_enabled = tracing::field::Empty,
            probe_status = tracing::field::Empty,
            max_concurrent = tracing::field::Empty,
            max_queue_depth = tracing::field::Empty,
        ))
        .await?;
        configs.insert(resolved_backend_id, config);
    }
    Ok(configs)
}

async fn resolve_document_snapshot_with_tools(
    node: &defra_node::EmbeddedNode,
    resolve_context: &DocumentResolveContext,
) -> Result<ResolvedRuntimeSnapshot> {
    crate::agent::resolve_document_runtime_snapshot(node, resolve_context).await
}

#[cfg(test)]
mod degraded_reason_tests {
    use super::is_degraded_startup_unavailable_reason;
    use gents_protocol::row::BehaviorReadinessUnavailableReason as Reason;

    #[test]
    fn unprobed_backend_is_degraded() {
        assert!(is_degraded_startup_unavailable_reason(
            Reason::BackendTemporarilyUnavailable
        ));
    }

    #[test]
    fn disabled_behavior_is_degraded() {
        assert!(is_degraded_startup_unavailable_reason(
            Reason::BehaviorDisabled
        ));
    }

    #[test]
    fn no_backend_binding_is_degraded() {
        // A backendless behavior (e.g. the seeded bootstrap default before a
        // backend is configured) must not be fatal at startup.
        assert!(is_degraded_startup_unavailable_reason(
            Reason::BackendNotConfigured
        ));
    }

    #[test]
    fn unknown_structural_reason_is_blocking() {
        assert!(!is_degraded_startup_unavailable_reason(
            Reason::ToolConfigurationInvalid
        ));
    }
}

#[cfg(test)]
mod startup_slot_failure_policy_tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::agent::reconcile::SlotFailurePolicy as _;
    use crate::behavior_readiness_publisher::{
        BehaviorReadinessWriter, FatalBehaviorReadinessWrite,
    };
    use crate::runtime_snapshot::ActiveRuntimeSnapshot;

    use super::*;

    struct FailDemotionWriter;

    #[async_trait::async_trait]
    impl BehaviorReadinessWriter for FailDemotionWriter {
        async fn upsert(
            &self,
            _agent_did: &str,
            snapshot: &gents_protocol::row::BehaviorReadinessSnapshot,
            _updated_at: &str,
        ) -> Result<()> {
            if snapshot.behaviors.iter().any(|entry| {
                entry.reason
                    == Some(gents_protocol::row::BehaviorReadinessUnavailableReason::ExecutorStartFailed)
            }) {
                return Err(FatalBehaviorReadinessWrite.into());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn aborted_staged_generation_does_not_release_active_startup_obligation() {
        let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
        crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();
        let (status_owner, runtime_status) =
            RuntimeStatusHandle::start(node.clone(), "did:test:startup-generation-barrier");
        runtime_status.initialize_startup("general").await.unwrap();
        let barrier = Arc::new(StartupBarrier::ready_for_test());
        let (fatal_error, _fatal_errors) = mpsc::unbounded_channel();
        let policy = StartupSlotFailurePolicy {
            barrier: barrier.clone(),
            runtime_status,
            admission_gate: super::super::router::RuntimeAdmissionGate::closed(),
            runtime_cancel: CancellationToken::new(),
            fatal_error,
            observer: None,
            budget: 1,
        };

        policy.on_slot_created("general", 1).await.unwrap();
        policy.on_slot_created("general", 2).await.unwrap();
        assert!(barrier.is_pending("general", 1).await);
        assert!(barrier.is_pending("general", 2).await);

        policy.on_slot_retired("general", 2, true).await;
        assert!(barrier.is_pending("general", 1).await);
        assert!(!barrier.is_pending("general", 2).await);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), barrier.wait_ready())
                .await
                .is_err(),
            "aborting staged generation two released unresolved generation one"
        );

        policy.on_slot_retired("general", 1, true).await;
        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.wait_ready())
            .await
            .expect("exact generation-one retirement should release the final obligation");
        status_owner.close().await.unwrap();
        node.shutdown().await;
    }

    #[tokio::test]
    async fn demotion_persistence_failure_closes_admission_and_cancels_runtime() {
        let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
        crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();
        let (status_owner, runtime_status) = RuntimeStatusHandle::start_with_readiness_writer(
            node.clone(),
            "did:test:demotion-fail-closed",
            Arc::new(FailDemotionWriter),
            std::time::Duration::from_millis(1),
        );
        runtime_status.initialize_startup("general").await.unwrap();
        runtime_status
            .readiness()
            .register_slot("general", 1)
            .await
            .unwrap();
        let (dispatcher, _requests) = mpsc::channel(1);
        runtime_status
            .readiness()
            .publish_snapshot(&ActiveRuntimeSnapshot {
                generation: 1,
                principal: None,
                local_did: String::new(),
                default_behavior_id: "general".to_string(),
                behaviors: HashMap::new(),
                tool_surfaces: HashMap::new(),
                backend_admission_configs: HashMap::new(),
                unavailable_behaviors: HashMap::new(),
                active_schedules: HashMap::new(),
                unavailable_schedules: HashSet::new(),
                active_event_triggers: HashMap::new(),
                unavailable_event_triggers: HashSet::new(),
                active_tasks: HashMap::new(),
                dispatchers: HashMap::from([("general".to_string(), dispatcher)]),
                behavior_executor_capacities: HashMap::new(),
                behavior_executor_queue_capacities: HashMap::new(),
            })
            .await
            .unwrap();
        runtime_status
            .readiness()
            .set_router_generation(1)
            .await
            .unwrap();
        runtime_status
            .set_process_state_durable(ProcessLifecycleState::Ready)
            .await
            .unwrap();

        let barrier = Arc::new(StartupBarrier::ready_for_test());
        barrier.register_behavior("general", 1).await;
        let gate = super::super::router::RuntimeAdmissionGate::closed();
        gate.open().await;
        let runtime_cancel = CancellationToken::new();
        let (fatal_error, mut fatal_errors) = mpsc::unbounded_channel();
        let policy = StartupSlotFailurePolicy {
            barrier,
            runtime_status: runtime_status.clone(),
            admission_gate: gate.clone(),
            runtime_cancel: runtime_cancel.clone(),
            fatal_error,
            observer: None,
            budget: 1,
        };

        assert!(policy
            .try_demote("general", 1, "executor failed")
            .await
            .is_err());
        assert!(!gate.is_open().await, "failed veto left routing open");
        assert!(
            runtime_cancel.is_cancelled(),
            "runtime teardown was not armed"
        );
        let fatal = fatal_errors
            .recv()
            .await
            .expect("runtime coordinator must receive demotion persistence failure");
        assert_eq!(fatal.to_string(), "injected fatal behavior readiness write");
        assert_eq!(
            runtime_status
                .readiness()
                .observation()
                .demotion_reason("general"),
            None,
            "failed persistence must not forge a committed demotion"
        );

        status_owner.close().await.unwrap();
        node.shutdown().await;
    }
}

#[cfg(test)]
mod run_agent_teardown_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc;

    use crate::behavior_readiness_publisher::BehaviorReadinessWriter;

    use super::*;

    // This is a deadlock guard, not a teardown latency SLO. The full test
    // suite runs many embedded DefraDB nodes concurrently on shared runners.
    const DEADLOCK_GUARD: Duration = Duration::from_secs(30);

    struct StuckAfterInitializeWriter {
        attempts: mpsc::UnboundedSender<()>,
        writes: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl BehaviorReadinessWriter for StuckAfterInitializeWriter {
        async fn upsert(
            &self,
            _agent_did: &str,
            _snapshot: &gents_protocol::row::BehaviorReadinessSnapshot,
            _updated_at: &str,
        ) -> Result<()> {
            if self.writes.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(());
            }
            let _ = self.attempts.send(());
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn saturated_stuck_publisher_cannot_wedge_run_agent_teardown() {
        let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
        crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();
        let (attempts_tx, mut attempts_rx) = mpsc::unbounded_channel();
        let (owner, runtime_status) = RuntimeStatusHandle::start_with_readiness_writer(
            node.clone(),
            "did:test:run-agent-teardown",
            Arc::new(StuckAfterInitializeWriter {
                attempts: attempts_tx,
                writes: AtomicUsize::new(0),
            }),
            Duration::from_millis(1),
        );
        runtime_status.initialize_startup("general").await.unwrap();

        let blocked = {
            let runtime_status = runtime_status.clone();
            tokio::spawn(async move {
                runtime_status
                    .set_process_state_durable(ProcessLifecycleState::Ready)
                    .await
            })
        };
        attempts_rx.recv().await.expect("Ready write must be stuck");
        let queued = (0..64)
            .map(|generation| {
                let runtime_status = runtime_status.clone();
                tokio::spawn(async move {
                    runtime_status
                        .readiness()
                        .set_router_generation(generation)
                        .await
                })
            })
            .collect::<Vec<_>>();
        tokio::time::timeout(Duration::from_secs(1), async {
            while runtime_status.readiness().command_capacity_for_test() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("test must saturate the runtime publisher queue");

        let admission_gate = super::super::router::RuntimeAdmissionGate::closed();
        admission_gate.open().await;
        admission_gate.close().await;
        assert!(!admission_gate.is_open().await);

        let error = tokio::time::timeout(
            DEADLOCK_GUARD,
            finish_run_agent(
                Err(anyhow::anyhow!("sentinel runtime body failure")),
                owner,
                runtime_status,
                None,
            ),
        )
        .await
        .expect("run_agent teardown must return boundedly")
        .expect_err("runtime body failure must be preserved");
        assert_eq!(error.to_string(), "sentinel runtime body failure");
        assert!(blocked.await.unwrap().is_err());
        let mut rejected = 0;
        for queued in queued {
            if queued.await.unwrap().is_err() {
                rejected += 1;
            }
        }
        assert!(
            rejected > 0,
            "publisher cancellation must reject queued work"
        );
        node.shutdown().await;
    }
}

#[cfg(test)]
mod lifecycle_coordinator_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    use tokio::sync::{Barrier, Notify, Semaphore};

    use super::*;

    #[derive(Default)]
    struct RecordingObserver {
        states: StdMutex<Vec<ProcessLifecycleState>>,
    }

    impl ProcessLifecycleObserver for RecordingObserver {
        fn on_process_state_change(&self, state: ProcessLifecycleState) {
            self.states.lock().expect("observer mutex").push(state);
        }
    }

    struct FailFirstReadyWriter {
        calls: AtomicUsize,
        ready_attempted: Barrier,
        release: Semaphore,
    }

    #[async_trait::async_trait]
    impl crate::behavior_readiness_publisher::BehaviorReadinessWriter for FailFirstReadyWriter {
        async fn upsert(
            &self,
            _agent_did: &str,
            _snapshot: &gents_protocol::row::BehaviorReadinessSnapshot,
            _updated_at: &str,
        ) -> Result<()> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                self.ready_attempted.wait().await;
                self.release.acquire().await.unwrap().forget();
                anyhow::bail!("injected first Ready persistence failure");
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn admission_gate_waits_for_durable_ready_retry_ack() {
        let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
        crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();
        let writer = Arc::new(FailFirstReadyWriter {
            calls: AtomicUsize::new(0),
            ready_attempted: Barrier::new(2),
            release: Semaphore::new(0),
        });
        let (status_owner, status) = RuntimeStatusHandle::start_with_readiness_writer(
            node.clone(),
            "did:test:ready-gate-persistence",
            writer.clone(),
            std::time::Duration::from_millis(5),
        );
        status.initialize_startup("general").await.unwrap();
        let gate = super::super::router::RuntimeAdmissionGate::closed();
        let coordinator = Arc::new(RuntimeLifecycleCoordinator::new(gate.clone(), status, None));
        let becoming_ready = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move { coordinator.become_ready().await })
        };

        writer.ready_attempted.wait().await;
        assert!(
            !gate.is_open().await,
            "Ready write failure opened admission"
        );
        assert!(
            !becoming_ready.is_finished(),
            "Ready lifecycle acknowledged before persistence retry"
        );
        writer.release.add_permits(1);
        assert!(becoming_ready.await.unwrap().unwrap());
        assert!(gate.is_open().await, "durable Ready did not open admission");

        coordinator.begin_shutdown().await.unwrap();
        coordinator.finish_shutdown().await.unwrap();
        status_owner.close().await.unwrap();
        node.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_wins_over_a_late_startup_barrier_release() {
        let node = Arc::new(defra_node::EmbeddedNode::builder().build().await.unwrap());
        crate::ensure_runtime_schemas(node.as_ref()).await.unwrap();
        let status = RuntimeStatusHandle::new(node.clone(), "did:test:lifecycle-race");
        status.initialize_startup("general").await.unwrap();
        let gate = super::super::router::RuntimeAdmissionGate::closed();
        let observer = Arc::new(RecordingObserver::default());
        let coordinator = Arc::new(RuntimeLifecycleCoordinator::new(
            gate.clone(),
            status,
            Some(observer.clone()),
        ));
        let waiting = Arc::new(Barrier::new(2));
        let release = Arc::new(Notify::new());

        let ready_task = {
            let coordinator = coordinator.clone();
            let waiting = waiting.clone();
            let release = release.clone();
            tokio::spawn(async move {
                waiting.wait().await;
                release.notified().await;
                coordinator.become_ready().await
            })
        };

        waiting.wait().await;
        coordinator.begin_shutdown().await.unwrap();
        release.notify_one();

        assert!(
            !ready_task.await.unwrap().unwrap(),
            "closing lifecycle reopened Ready"
        );
        assert!(
            !gate.is_open().await,
            "closing lifecycle reopened admission"
        );
        assert_eq!(
            *observer.states.lock().expect("observer mutex"),
            vec![ProcessLifecycleState::ShuttingDown],
            "Ready must never become observable after shutdown begins"
        );
        node.shutdown().await;
    }
}
