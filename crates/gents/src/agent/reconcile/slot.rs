use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use anyhow::Result;
use futures::FutureExt;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::{JoinHandle, JoinSet};

use crate::admission::BackendAdmissionConfig;
use crate::config::AgentBehavior;
use crate::retry::RetryPolicy;
use crate::runtime_snapshot::{ResolvedRuntimeSnapshot, ScopedBehaviorConfigProvenance};
use crate::startup_readiness::{BuildOutcome, BuildStanding};
use crate::tool_surface::ToolSurface;
use crate::watcher::AgentRequest;

use std::collections::HashMap;

const BEHAVIOR_EXECUTOR_QUEUE_CAPACITY: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BehaviorSlotState {
    Active,
    Retiring,
}

pub(super) struct BehaviorSlot {
    pub(super) dispatcher: mpsc::Sender<AgentRequest>,
    pub(super) state_tx: watch::Sender<BehaviorSlotState>,
    pub(super) handle: JoinHandle<()>,
    pub(super) behavior_fingerprint: String,
    pub(super) tool_surface_fingerprint: String,
    pub(super) config_provenance: ScopedBehaviorConfigProvenance,
    pub(super) executor_capacity: usize,
    pub(super) queue_capacity: usize,
}

impl BehaviorSlot {
    pub(super) fn matches(
        &self,
        behavior: &Arc<AgentBehavior>,
        tool_surface: &Arc<ToolSurface>,
        config_provenance: &ScopedBehaviorConfigProvenance,
        executor_capacity: usize,
    ) -> bool {
        self.behavior_fingerprint == format!("{behavior:?}")
            && self.tool_surface_fingerprint == format!("{tool_surface:?}")
            && &self.config_provenance == config_provenance
            && self.executor_capacity == executor_capacity
    }
}

#[async_trait::async_trait]
pub(crate) trait SlotFailurePolicy: Send + Sync {
    fn build_failure_budget(&self) -> u32;
    async fn try_demote(&self, behavior_id: &str, error: &str) -> bool;
    async fn on_slot_retired(&self, behavior_id: &str, recreated: bool);
}

async fn handle_slot_failure(
    behavior_id: &str,
    error: &str,
    failure_policy: Option<&dyn SlotFailurePolicy>,
    standing: &Arc<std::sync::Mutex<BuildStanding>>,
    shutdown: &mut watch::Receiver<bool>,
    state_rx: &mut watch::Receiver<BehaviorSlotState>,
) -> bool {
    let Some(policy) = failure_policy else {
        return false;
    };
    enum Verdict {
        Restart,
        AlreadyDemoted,
        Transitioned,
        StillPending,
    }
    let verdict = match standing.lock() {
        Err(_) => Verdict::Restart,
        Ok(mut standing) => {
            if standing.released() {
                if *standing == BuildStanding::Demoted {
                    Verdict::AlreadyDemoted
                } else {
                    Verdict::Restart
                }
            } else {
                let next = standing.step(policy.build_failure_budget(), BuildOutcome::Failed);
                *standing = next;
                if next == BuildStanding::Demoted {
                    Verdict::Transitioned
                } else {
                    Verdict::StillPending
                }
            }
        }
    };
    match verdict {
        Verdict::Restart | Verdict::StillPending => return false,
        Verdict::AlreadyDemoted => {
            park_until_retired(shutdown, state_rx).await;
            return true;
        }
        Verdict::Transitioned => {}
    }
    if policy.try_demote(behavior_id, error).await {
        park_until_retired(shutdown, state_rx).await;
        true
    } else {
        if let Ok(mut standing) = standing.lock() {
            *standing = BuildStanding::Ready;
        }
        false
    }
}

async fn park_until_retired(
    shutdown: &mut watch::Receiver<bool>,
    state_rx: &mut watch::Receiver<BehaviorSlotState>,
) {
    loop {
        if *shutdown.borrow() || *state_rx.borrow() == BehaviorSlotState::Retiring {
            return;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            changed = state_rx.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

pub(super) fn spawn_slots<F, Fut>(
    resolved_snapshot: &ResolvedRuntimeSnapshot,
    retry_policy: RetryPolicy,
    runner: F,
    shutdown: watch::Receiver<bool>,
    failure_policy: Option<Arc<dyn SlotFailurePolicy>>,
) -> HashMap<String, BehaviorSlot>
where
    F: Fn(
            Arc<AgentBehavior>,
            Arc<ToolSurface>,
            ScopedBehaviorConfigProvenance,
            Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
            watch::Receiver<bool>,
        ) -> Fut
        + Send
        + Sync
        + Clone
        + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let mut slots = HashMap::with_capacity(resolved_snapshot.behaviors.len());
    for (behavior_id, behavior) in &resolved_snapshot.behaviors {
        let tool_surface = resolved_snapshot
            .tool_surfaces
            .get(behavior_id)
            .cloned()
            .expect("resolved snapshot should include tool surfaces for runnable behaviors");
        let config_provenance = resolved_snapshot.scoped_config_provenance_for(behavior_id);
        slots.insert(
            behavior_id.clone(),
            spawn_slot_with_capacity(
                behavior.clone(),
                tool_surface,
                config_provenance,
                behavior_executor_capacity(behavior, &resolved_snapshot.backend_admission_configs),
                retry_policy.clone(),
                runner.clone(),
                shutdown.clone(),
                failure_policy.clone(),
            ),
        );
    }
    slots
}

#[cfg(test)]
pub(super) fn spawn_slot<F, Fut>(
    behavior: Arc<AgentBehavior>,
    tool_surface: Arc<ToolSurface>,
    retry_policy: RetryPolicy,
    runner: F,
    shutdown: watch::Receiver<bool>,
) -> BehaviorSlot
where
    F: Fn(
            Arc<AgentBehavior>,
            Arc<ToolSurface>,
            ScopedBehaviorConfigProvenance,
            Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
            watch::Receiver<bool>,
        ) -> Fut
        + Send
        + Sync
        + Clone
        + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    spawn_slot_with_capacity(
        behavior,
        tool_surface,
        ScopedBehaviorConfigProvenance {
            scope: crate::rendered_request::ConfigProvenanceScope::StaticOrOneShot,
            exact: None,
        },
        1,
        retry_policy,
        runner,
        shutdown,
        None,
    )
}

pub(super) fn spawn_slot_with_capacity<F, Fut>(
    behavior: Arc<AgentBehavior>,
    tool_surface: Arc<ToolSurface>,
    config_provenance: ScopedBehaviorConfigProvenance,
    executor_capacity: usize,
    retry_policy: RetryPolicy,
    runner: F,
    shutdown: watch::Receiver<bool>,
    failure_policy: Option<Arc<dyn SlotFailurePolicy>>,
) -> BehaviorSlot
where
    F: Fn(
            Arc<AgentBehavior>,
            Arc<ToolSurface>,
            ScopedBehaviorConfigProvenance,
            Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
            watch::Receiver<bool>,
        ) -> Fut
        + Send
        + Sync
        + Clone
        + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let executor_capacity = executor_capacity.max(1);
    let (dispatcher, request_rx) = mpsc::channel(BEHAVIOR_EXECUTOR_QUEUE_CAPACITY);
    let request_rx = Arc::new(Mutex::new(request_rx));
    let (state_tx, state_rx) = watch::channel(BehaviorSlotState::Active);
    let behavior_fingerprint = format!("{behavior:?}");
    let tool_surface_fingerprint = format!("{tool_surface:?}");

    let standing = Arc::new(std::sync::Mutex::new(BuildStanding::seeded()));
    let handle = tokio::spawn(run_slot_workers(
        behavior,
        tool_surface,
        config_provenance.clone(),
        request_rx,
        executor_capacity,
        retry_policy,
        runner,
        shutdown,
        state_rx,
        failure_policy,
        standing,
    ));

    BehaviorSlot {
        dispatcher,
        state_tx,
        handle,
        behavior_fingerprint,
        tool_surface_fingerprint,
        config_provenance,
        executor_capacity,
        queue_capacity: BEHAVIOR_EXECUTOR_QUEUE_CAPACITY,
    }
}

pub(super) fn behavior_executor_capacity(
    behavior: &AgentBehavior,
    backend_admission_configs: &HashMap<String, BackendAdmissionConfig>,
) -> usize {
    let Some(backend_id) = behavior
        .backend_id
        .as_deref()
        .map(str::trim)
        .filter(|backend_id| !backend_id.is_empty())
    else {
        return 1;
    };

    backend_admission_configs
        .get(backend_id)
        .filter(|config| config.is_available())
        .map(|config| config.max_concurrent.max(1))
        .unwrap_or(1)
}

pub(super) fn retire_slot(slot: BehaviorSlot) {
    let _ = slot.state_tx.send(BehaviorSlotState::Retiring);
    drop(slot.dispatcher);
    tokio::spawn(async move {
        if let Err(error) = slot.handle.await {
            if !error.is_cancelled() {
                tracing::error!(error = %error, "retired behavior slot join failed");
            }
        }
    });
}

async fn run_slot_loop<F, Fut>(
    behavior: Arc<AgentBehavior>,
    tool_surface: Arc<ToolSurface>,
    config_provenance: ScopedBehaviorConfigProvenance,
    request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
    retry_policy: RetryPolicy,
    runner: F,
    mut shutdown: watch::Receiver<bool>,
    mut state_rx: watch::Receiver<BehaviorSlotState>,
    failure_policy: Option<Arc<dyn SlotFailurePolicy>>,
    standing: Arc<std::sync::Mutex<BuildStanding>>,
) where
    F: Fn(
            Arc<AgentBehavior>,
            Arc<ToolSurface>,
            ScopedBehaviorConfigProvenance,
            Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
            watch::Receiver<bool>,
        ) -> Fut
        + Send
        + Sync
        + Clone
        + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let mut failure_count = 0u32;
    loop {
        if *shutdown.borrow() || *state_rx.borrow() == BehaviorSlotState::Retiring {
            return;
        }
        // A sibling worker may have spent the budget already; a demoted slot
        // must not keep rebuilding against a verdict that is already final.
        if standing
            .lock()
            .map(|standing| *standing == BuildStanding::Demoted)
            .unwrap_or(false)
        {
            park_until_retired(&mut shutdown, &mut state_rx).await;
            return;
        }

        let outcome = AssertUnwindSafe(runner(
            behavior.clone(),
            tool_surface.clone(),
            config_provenance.clone(),
            request_rx.clone(),
            shutdown.clone(),
        ))
        .catch_unwind()
        .await;

        if *shutdown.borrow() {
            return;
        }

        match outcome {
            Ok(Ok(())) if *state_rx.borrow() == BehaviorSlotState::Retiring => return,
            Ok(Ok(())) => {
                if let Ok(mut standing) = standing.lock() {
                    *standing = standing.step(u32::MAX, BuildOutcome::Started);
                }
                let delay = retry_policy.delay_for_attempt(failure_count);
                failure_count += 1;
                tracing::warn!(
                    behavior_id = %behavior.behavior_id,
                    delay_ms = delay.as_millis() as u64,
                    "behavior slot exited unexpectedly, scheduling restart"
                );
                if !wait_for_restart(delay, &mut shutdown).await {
                    return;
                }
            }
            Ok(Err(error)) => {
                if handle_slot_failure(
                    &behavior.behavior_id,
                    &format!("{error:#}"),
                    failure_policy.as_deref(),
                    &standing,
                    &mut shutdown,
                    &mut state_rx,
                )
                .await
                {
                    return;
                }
                let delay = retry_policy.delay_for_attempt(failure_count);
                failure_count += 1;
                tracing::error!(
                    behavior_id = %behavior.behavior_id,
                    error = %error,
                    delay_ms = delay.as_millis() as u64,
                    "behavior slot failed, scheduling restart"
                );
                if !wait_for_restart(delay, &mut shutdown).await {
                    return;
                }
            }
            Err(_) => {
                if handle_slot_failure(
                    &behavior.behavior_id,
                    "behavior runner panicked",
                    failure_policy.as_deref(),
                    &standing,
                    &mut shutdown,
                    &mut state_rx,
                )
                .await
                {
                    return;
                }
                let delay = retry_policy.delay_for_attempt(failure_count);
                failure_count += 1;
                tracing::error!(
                    behavior_id = %behavior.behavior_id,
                    delay_ms = delay.as_millis() as u64,
                    "behavior slot panicked, scheduling restart"
                );
                if !wait_for_restart(delay, &mut shutdown).await {
                    return;
                }
            }
        }
    }
}

async fn run_slot_workers<F, Fut>(
    behavior: Arc<AgentBehavior>,
    tool_surface: Arc<ToolSurface>,
    config_provenance: ScopedBehaviorConfigProvenance,
    request_rx: Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
    executor_capacity: usize,
    retry_policy: RetryPolicy,
    runner: F,
    shutdown: watch::Receiver<bool>,
    state_rx: watch::Receiver<BehaviorSlotState>,
    failure_policy: Option<Arc<dyn SlotFailurePolicy>>,
    standing: Arc<std::sync::Mutex<BuildStanding>>,
) where
    F: Fn(
            Arc<AgentBehavior>,
            Arc<ToolSurface>,
            ScopedBehaviorConfigProvenance,
            Arc<Mutex<mpsc::Receiver<AgentRequest>>>,
            watch::Receiver<bool>,
        ) -> Fut
        + Send
        + Sync
        + Clone
        + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    tracing::info!(
        behavior_id = %behavior.behavior_id,
        executor_capacity,
        queue_capacity = BEHAVIOR_EXECUTOR_QUEUE_CAPACITY,
        "behavior executor worker pool starting"
    );
    let mut workers = JoinSet::new();
    for worker_index in 0..executor_capacity {
        workers.spawn(run_slot_loop(
            behavior.clone(),
            tool_surface.clone(),
            config_provenance.clone(),
            request_rx.clone(),
            retry_policy.clone(),
            runner.clone(),
            shutdown.clone(),
            state_rx.clone(),
            failure_policy.clone(),
            standing.clone(),
        ));
        tracing::debug!(
            behavior_id = %behavior.behavior_id,
            worker_index,
            executor_capacity,
            "behavior executor worker spawned"
        );
    }

    while let Some(joined) = workers.join_next().await {
        if let Err(error) = joined {
            if !error.is_cancelled() {
                tracing::error!(
                    behavior_id = %behavior.behavior_id,
                    error = %error,
                    "behavior executor worker task join failed"
                );
            }
        }
    }
}

async fn wait_for_restart(
    delay: std::time::Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => true,
        _ = shutdown.changed() => false,
    }
}
