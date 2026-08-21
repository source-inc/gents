use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::context::{RuntimeContext, StartupBarrier};
use crate::admission::{AdmissionRegistry, BackendAdmissionConfig};
use crate::agent::reconcile::GenerationSupervisor;
use crate::agent::{DocumentResolveContext, Gents, ProcessLifecycleState};
use crate::backend_registry;
use crate::health_checker::{spawn_health_checker, ServiceHealthMap};
use crate::runtime_snapshot::ResolvedRuntimeSnapshot;
use crate::runtime_status::{ReconcilePhase, RuntimeStatusHandle};
use crate::tool_surface::{ToolRuntimeContext, ToolSurface};

enum BackgroundTaskResult {
    Router(Result<()>),
    RouterObserver(Result<()>),
    ExecutorStatus(Result<()>),
    Reconcile(Result<()>),
    Control(Result<()>),
    SubagentCompletion(Result<()>),
    CrossDeploymentCancelMirror(Result<()>),
    PairingReconcile(Result<()>),
    RegistryHeartbeat(Result<()>),
    EndpointHeartbeat(Result<()>),
    NetworkReconcile(Result<()>),
    ReciprocalReconcile(Result<()>),
    BearerClaimReconcile(Result<()>),
    PersonaRequestReconcile(Result<()>),
    DiscoveryReconcile(Result<()>),
    DirectoryProjection(Result<()>),
}

struct StartupSlotFailurePolicy {
    barrier: Arc<StartupBarrier>,
    demotions: Arc<crate::startup_readiness::StartupDemotions>,
    runtime_status: RuntimeStatusHandle,
    budget: u32,
}

#[async_trait::async_trait]
impl crate::agent::reconcile::SlotFailurePolicy for StartupSlotFailurePolicy {
    fn build_failure_budget(&self) -> u32 {
        self.budget.max(1)
    }

    async fn try_demote(&self, behavior_id: &str, error: &str) -> bool {
        if !self.barrier.is_pending(behavior_id).await {
            return false;
        }
        let reason = format!(
            "demoted after {} consecutive startup build failures; last error: {error}",
            self.build_failure_budget()
        );
        self.demotions.record(behavior_id, reason.clone());
        self.barrier.mark_behavior_demoted(behavior_id).await;
        self.runtime_status.record_startup_demotion().await;
        tracing::error!(
            behavior_id = %behavior_id,
            budget = self.build_failure_budget(),
            error = %error,
            "behavior demoted: its completion client failed to build repeatedly; \
             the process will report Ready without it. Fix the behavior/backend \
             config — a config change re-admits it with a fresh budget."
        );
        true
    }

    async fn on_slot_retired(&self, behavior_id: &str, recreated: bool) {
        self.barrier.mark_behavior_superseded(behavior_id).await;
        let _ = recreated;
        self.demotions.clear(behavior_id);
    }
}

pub(in crate::agent) async fn run_agent(
    agent: Gents,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let cancel = CancellationToken::new();
    crate::migration::ensure_all_runtime_migrations(agent.node.clone())
        .await
        .context("ensure runtime schema migrations")?;
    let runtime_status =
        RuntimeStatusHandle::new(agent.node.clone(), agent.agent_did().to_string());
    runtime_status
        .set_process_state(ProcessLifecycleState::Recovering)
        .await;
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
    let _health_checker = spawn_health_checker(
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
    let _backend_prober = crate::backend_health::spawn_backend_prober(
        agent.node.clone(),
        agent.backend_health.clone(),
        agent.backend_prober_options.clone(),
        backend_health_events_tx,
        cancel.child_token(),
    );

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
        tracing::warn!(behavior_id = %behavior_id, reason = %reason, "behavior unavailable at startup");
    }

    let startup_barrier = Arc::new(StartupBarrier::new(
        &resolved_snapshot
            .behaviors
            .values()
            .cloned()
            .collect::<Vec<_>>(),
    ));
    let admission_registry = AdmissionRegistry::new(agent.node.clone());
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
        startup_demotions: runtime_status.startup_demotions(),
        operator_tool_root: agent.operator_tool_root().map(PathBuf::from),
    };
    let runtime_for_runner = runtime.clone();
    let startup_demotions = runtime_status.startup_demotions();
    let slot_failure_policy: Arc<dyn crate::agent::reconcile::SlotFailurePolicy> =
        Arc::new(StartupSlotFailurePolicy {
            barrier: startup_barrier.clone(),
            demotions: startup_demotions.clone(),
            runtime_status: runtime_status.clone(),
            budget: agent.startup_readiness.build_failure_budget,
        });
    let generation_supervisor = GenerationSupervisor::bootstrap(
        resolved_snapshot,
        admission_registry.clone(),
        agent.retry_policy.clone(),
        move |behavior, tool_surface, request_rx, shutdown| {
            let runtime = runtime_for_runner.clone();
            async move {
                runtime
                    .run_behavior(behavior, tool_surface, request_rx, shutdown)
                    .await
            }
        },
        runtime_status.clone(),
        shutdown.clone(),
        Some(slot_failure_policy),
    )?;
    let initial_active_snapshot = generation_supervisor.current_snapshot();
    runtime_status
        .publish_startup_snapshot(initial_active_snapshot.as_ref())
        .await;
    let (active_snapshot_tx, active_snapshot_rx) = watch::channel(initial_active_snapshot.clone());
    let (reconcile_tx, reconcile_rx) = mpsc::channel(8);
    let _reconcile_tx_guard = reconcile_tx.clone();

    if let Some(observer) = agent.runtime_snapshot_observer.clone() {
        let mut snapshot_rx = active_snapshot_rx.clone();
        let mut observer_shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                let (generation, runnable) = {
                    let snapshot = snapshot_rx.borrow_and_update();
                    let mut ids: Vec<String> = snapshot.behaviors.keys().cloned().collect();
                    ids.sort();
                    (snapshot.generation, ids)
                };
                observer.on_generation_published(generation, &runnable);

                tokio::select! {
                    changed = snapshot_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = observer_shutdown.changed() => break,
                }
            }
        });
    }

    let trigger_engine_node = agent.node.clone();
    let trigger_engine_schedule_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_event_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_goal_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_subagent_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_engine_snapshot_rx = active_snapshot_rx.clone();
    let trigger_engine_materializer_snapshot_rx = active_snapshot_rx.clone();
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

    // DefraDB's event bus is live-only: updates published before a subscriber
    // exists cannot be replayed. Establish the control subscription before
    // the readiness task can publish `Ready`, so callers that wait for Ready
    // cannot race the watcher startup and lose their first config mutation.
    let control_subscription = agent
        .document_runtime_context()
        .is_some()
        .then(|| agent.node.subscribe(&[defra_node::EventName::Update]));

    let ready_cancel = cancel.child_token();
    let ready_startup_barrier = startup_barrier.clone();
    let ready_observer = agent.process_state_observer.clone();
    let ready_runtime_status = runtime_status.clone();
    let ready_behavior_count = initial_active_snapshot.behaviors.len();
    let ready_unavailable_count = initial_active_snapshot.unavailable_behaviors.len();
    let ready_demotions = startup_demotions.clone();
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
        ready_runtime_status
            .set_process_state(ProcessLifecycleState::Ready)
            .await;
        if let Some(observer) = &ready_observer {
            observer.on_process_state_change(ProcessLifecycleState::Ready);
        }
        let demoted = ready_demotions.snapshot();
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

    let cancel_mirror_node = agent.node.clone();
    let cancel_mirror_snapshot_rx = active_snapshot_rx.clone();
    let cancel_mirror_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::CrossDeploymentCancelMirror(
            crate::trigger_engine::cross_deployment_cancel_mirror::run_cross_deployment_cancel_mirror(
                cancel_mirror_node,
                cancel_mirror_snapshot_rx,
                cancel_mirror_cancel,
            )
            .await,
        )
    });

    let pairing_node = agent.node.clone();
    let pairing_identity = agent.principal_arc().identity.clone();
    let pairing_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::PairingReconcile(
            crate::agent::p2p_reconcile::run_pairing_reconciler(
                pairing_node,
                pairing_identity,
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

    let network_node = agent.node.clone();
    let network_identity = agent.principal_arc().identity.clone();
    let network_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::NetworkReconcile(
            crate::agent::p2p_reconcile::run_network_reconciler(
                network_node,
                network_identity,
                network_cancel,
            )
            .await,
        )
    });

    let bearer_node = agent.node.clone();
    let bearer_identity = agent.principal_arc().identity.clone();
    let bearer_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::BearerClaimReconcile(
            crate::agent::p2p_reconcile::run_bearer_claim_reconciler(
                bearer_node,
                bearer_identity,
                bearer_cancel,
            )
            .await,
        )
    });

    let persona_request_node = agent.node.clone();
    let persona_request_ceiling = agent
        .document_runtime_context()
        .and_then(|context| context.tool_ceiling.root())
        .map(std::path::Path::to_path_buf);
    let persona_request_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::PersonaRequestReconcile(
            crate::agent::p2p_reconcile::run_persona_request_reconciler(
                persona_request_node,
                persona_request_ceiling,
                persona_request_cancel,
            )
            .await,
        )
    });

    let reciprocal_node = agent.node.clone();
    let reciprocal_identity = agent.principal_arc().identity.clone();
    let reciprocal_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::ReciprocalReconcile(
            crate::agent::p2p_reconcile::run_reciprocal_reconciler(
                reciprocal_node,
                reciprocal_identity,
                reciprocal_cancel,
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

    let discovery_node = agent.node.clone();
    let discovery_cancel = cancel.child_token();
    background_tasks.spawn(async move {
        BackgroundTaskResult::DiscoveryReconcile(
            crate::agent::p2p_reconcile::run_discovery_reconciler(discovery_node, discovery_cancel)
                .await,
        )
    });

    let router_node = agent.node.clone();
    let router_agent_did = agent.agent_did().to_string();
    let router_deployment_id = local_deployment_id.clone();
    let router_active_snapshot_rx = active_snapshot_rx.clone();
    let router_shutdown = shutdown.clone();
    let router_startup_demotions = startup_demotions.clone();
    background_tasks.spawn(async move {
        BackgroundTaskResult::Router(
            super::router::run_router(
                router_node,
                router_agent_did,
                router_deployment_id,
                router_active_snapshot_rx,
                router_shutdown,
                router_startup_demotions,
            )
            .await,
        )
    });

    let router_observer_active_snapshot_rx = active_snapshot_rx.clone();
    let router_observer_runtime_status = runtime_status.clone();
    let router_observer_shutdown = shutdown.clone();
    background_tasks.spawn(async move {
        BackgroundTaskResult::RouterObserver(
            super::router::run_router_generation_observer(
                router_observer_active_snapshot_rx,
                router_observer_runtime_status,
                router_observer_shutdown,
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

    let (result, shutdown_requested) = tokio::select! {
        _ = shutdown.changed() => (Ok(()), true),
        Some(joined) = background_tasks.join_next() => match joined {
            Ok(BackgroundTaskResult::Router(result)) => (result, false),
            Ok(BackgroundTaskResult::RouterObserver(result)) => (result, false),
            Ok(BackgroundTaskResult::ExecutorStatus(result)) => (result, false),
            Ok(BackgroundTaskResult::Reconcile(result)) => (result, false),
            Ok(BackgroundTaskResult::Control(result)) => (result, false),
            Ok(BackgroundTaskResult::SubagentCompletion(result)) => (result, false),
            Ok(BackgroundTaskResult::CrossDeploymentCancelMirror(result)) => (result, false),
            Ok(BackgroundTaskResult::PairingReconcile(result)) => (result, false),
            Ok(BackgroundTaskResult::RegistryHeartbeat(result)) => (result, false),
            Ok(BackgroundTaskResult::EndpointHeartbeat(result)) => (result, false),
            Ok(BackgroundTaskResult::NetworkReconcile(result)) => (result, false),
            Ok(BackgroundTaskResult::ReciprocalReconcile(result)) => (result, false),
            Ok(BackgroundTaskResult::BearerClaimReconcile(result)) => (result, false),
            Ok(BackgroundTaskResult::PersonaRequestReconcile(result)) => (result, false),
            Ok(BackgroundTaskResult::DiscoveryReconcile(result)) => (result, false),
            Ok(BackgroundTaskResult::DirectoryProjection(result)) => (result, false),
            Err(error) => (Err(anyhow!("background task join failed: {error}")), false),
        },
        else => (Ok(()), false),
    };

    if let Some(observer) = &agent.process_state_observer {
        observer.on_process_state_change(ProcessLifecycleState::ShuttingDown);
    }
    runtime_status
        .set_process_state(ProcessLifecycleState::ShuttingDown)
        .await;

    cancel.cancel();
    if !shutdown_requested {
        background_tasks.abort_all();
    }
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
    lsp_pool.shutdown().await;

    if let Some(observer) = &agent.process_state_observer {
        observer.on_process_state_change(ProcessLifecycleState::Shutdown);
    }
    runtime_status
        .set_process_state(ProcessLifecycleState::Shutdown)
        .await;

    result
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
            if report.conversations_recovered > 0 {
                recovered_any = true;
                tracing::info!(
                    agent_did = %agent_did,
                    count = report.conversations_recovered,
                    "recovered stuck conversations"
                );
            }
            // Failures are the OPPOSITE of a recovery and must never be folded
            // into the recovered count (#693): a fully failed pass used to log
            // "recovered stuck conversations count=2" and read as healthy.
            if report.conversations_failed > 0 {
                recovered_any = true;
                tracing::warn!(
                    agent_did = %agent_did,
                    count = report.conversations_failed,
                    "failed to recover stuck conversations; they remain stuck and will be \
                     retried on the next startup"
                );
            }
            if report.duplicate_conversation_sessions > 0 {
                recovered_any = true;
                tracing::warn!(
                    agent_did = %agent_did,
                    count = report.duplicate_conversation_sessions,
                    "sessions carry duplicate AgentConversation documents; the canonical \
                     document was recovered and the duplicates converged onto it. This store \
                     predates the unique session_id index (DefraDB cannot add it retroactively) \
                     or received duplicates over replication"
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

fn is_degraded_startup_unavailable_reason(reason: &str) -> bool {
    let reason = reason.trim();
    reason.ends_with(" is disabled")
        || (reason.contains(" backend ")
            && reason.contains(" is unavailable (enabled=")
            && reason.contains(" probe_status="))
        || reason.contains("did not advertise model")
        || reason.contains("startup readiness probe")
        || reason.contains("has no backend binding")
}

async fn validate_startup_snapshot(
    agent: &Gents,
    tool_runtime: &ToolRuntimeContext,
    snapshot: &ResolvedRuntimeSnapshot,
) -> Result<()> {
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
            .filter(|(_, reason)| !is_degraded_startup_unavailable_reason(reason))
            .map(|(behavior_id, reason)| format!("{behavior_id}: {reason}"))
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
            let paired_peer_dids =
                load_startup_paired_peer_dids(agent.node.as_ref(), agent.agent_did()).await?;
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
                    .with_paired_peer_dids(paired_peer_dids)
            })
        }
    }
}

#[derive(Debug, Deserialize)]
struct StartupPeerPairingDesiredRow {
    peer_id: String,
    agent_did: Option<String>,
}

async fn load_startup_paired_peer_dids(
    node: &defra_node::EmbeddedNode,
    local_did: &str,
) -> Result<HashSet<String>> {
    let query = r#"{
        PeerPairingDesired {
            peer_id
            agent_did
        }
    }"#;
    let response = node.execute(query).await;
    if response.has_errors() {
        anyhow::bail!(
            "query PeerPairingDesired for startup paired peer DIDs failed: {:?}",
            response.errors
        );
    }
    let rows: Vec<StartupPeerPairingDesiredRow> = response
        .data
        .as_ref()
        .and_then(|d| d.get("PeerPairingDesired"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    let local_did = local_did.trim();
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            row.agent_did
                .as_deref()
                .map(str::trim)
                .filter(|did| !did.is_empty())
                .map(ToOwned::to_owned)
                .or_else(|| {
                    let peer_id = row.peer_id.trim();
                    peer_id.starts_with("did:").then(|| peer_id.to_string())
                })
        })
        .filter(|did| !did.trim().is_empty() && did.trim() != local_did)
        .collect())
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

    #[test]
    fn unprobed_backend_is_degraded() {
        assert!(is_degraded_startup_unavailable_reason(
            "behavior 'default' backend workstation-1 is unavailable (enabled=true probe_status=unknown)"
        ));
    }

    #[test]
    fn disabled_behavior_is_degraded() {
        assert!(is_degraded_startup_unavailable_reason(
            "behavior 'x' is disabled"
        ));
    }

    #[test]
    fn no_backend_binding_is_degraded() {
        // A backendless behavior (e.g. the seeded bootstrap default before a
        // backend is configured) must not be fatal at startup.
        assert!(is_degraded_startup_unavailable_reason(
            "behavior did:key:zABC:default has no backend binding"
        ));
    }

    #[test]
    fn unknown_structural_reason_is_blocking() {
        assert!(!is_degraded_startup_unavailable_reason(
            "behavior 'default' references missing tool selection 'gone'"
        ));
    }
}
