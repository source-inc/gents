use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, watch, Mutex};
use tracing::Instrument;

use crate::admission::AdmissionRegistry;
use crate::config::AgentBehavior;
use crate::retry::RetryPolicy;
use crate::runtime_snapshot::ActiveRuntimeSnapshot;
use crate::runtime_snapshot::{ResolvedRuntimeSnapshot, ScopedBehaviorConfigProvenance};
use crate::runtime_status::{ReconcilePhase, RuntimeStatusHandle};
use crate::tool_surface::ToolSurface;
use crate::watcher::AgentRequest;

mod diff;
mod slot;

use diff::diff_counts;
#[cfg(test)]
use slot::spawn_slot;
pub(in crate::agent) use slot::SlotFailurePolicy;
use slot::{
    behavior_executor_capacity, retire_slot, spawn_slot_with_capacity, spawn_slots, BehaviorSlot,
    BehaviorSlotState,
};

pub(super) struct GenerationSupervisor<F> {
    current_snapshot: Arc<ActiveRuntimeSnapshot>,
    active_slots: HashMap<String, BehaviorSlot>,
    admission_registry: AdmissionRegistry,
    retry_policy: RetryPolicy,
    runner: F,
    runtime_status: RuntimeStatusHandle,
    slot_failure_policy: Option<Arc<dyn SlotFailurePolicy>>,
}

impl<F, Fut> GenerationSupervisor<F>
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
    pub(super) fn bootstrap(
        resolved_snapshot: ResolvedRuntimeSnapshot,
        admission_registry: AdmissionRegistry,
        retry_policy: RetryPolicy,
        runner: F,
        runtime_status: RuntimeStatusHandle,
        shutdown: watch::Receiver<bool>,
        slot_failure_policy: Option<Arc<dyn SlotFailurePolicy>>,
    ) -> Result<Self> {
        resolved_snapshot.validate_config_provenance_scope()?;
        admission_registry.reconcile(1, &resolved_snapshot.backend_admission_configs);
        let active_slots = spawn_slots(
            &resolved_snapshot,
            retry_policy.clone(),
            runner.clone(),
            shutdown,
            slot_failure_policy.clone(),
        );
        let dispatchers = active_slots
            .iter()
            .map(|(behavior_id, slot)| (behavior_id.clone(), slot.dispatcher.clone()))
            .collect();
        let executor_capacities = active_slots
            .iter()
            .map(|(behavior_id, slot)| (behavior_id.clone(), slot.executor_capacity))
            .collect();
        let executor_queue_capacities = active_slots
            .iter()
            .map(|(behavior_id, slot)| (behavior_id.clone(), slot.queue_capacity))
            .collect();
        let current_snapshot = Arc::new(resolved_snapshot.activate_with_executor_metadata(
            1,
            dispatchers,
            executor_capacities,
            executor_queue_capacities,
        ));

        Ok(Self {
            current_snapshot,
            active_slots,
            admission_registry,
            retry_policy,
            runner,
            runtime_status,
            slot_failure_policy,
        })
    }

    pub(super) fn current_snapshot(&self) -> Arc<ActiveRuntimeSnapshot> {
        self.current_snapshot.clone()
    }

    pub(super) async fn run(
        mut self,
        active_snapshot_tx: watch::Sender<Arc<ActiveRuntimeSnapshot>>,
        mut proposals_rx: mpsc::Receiver<ResolvedRuntimeSnapshot>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                proposal = proposals_rx.recv() => {
                    let Some(proposal) = proposal else {
                        break;
                    };
                    let current_generation = self.current_snapshot.generation;
                    let next_generation = current_generation + 1;
                    let proposed_behavior_count = proposal.behaviors.len();
                    let proposed_unavailable_behavior_count = proposal.unavailable_behaviors.len();
                    let proposed_default_behavior_id = proposal.default_behavior_id.clone();

                    self.handle_proposal(proposal, &active_snapshot_tx, shutdown.clone())
                        .instrument(tracing::info_span!(
                            "runtime.reconcile",
                            current_generation,
                            next_generation,
                            proposed_behavior_count,
                            proposed_unavailable_behavior_count,
                            proposed_default_behavior_id = %proposed_default_behavior_id,
                        ))
                        .await;
                }
            }
        }

        self.shutdown_slots().await;
        Ok(())
    }

    async fn handle_proposal(
        &mut self,
        proposal: ResolvedRuntimeSnapshot,
        active_snapshot_tx: &watch::Sender<Arc<ActiveRuntimeSnapshot>>,
        shutdown: watch::Receiver<bool>,
    ) {
        self.runtime_status
            .set_reconcile_phase(ReconcilePhase::Diffing)
            .await;
        if proposal.configuration_fingerprint() == self.current_snapshot.configuration_fingerprint()
        {
            tracing::debug!(
                generation = self.current_snapshot.generation,
                "runtime reconcile noop: resolved snapshot matches active generation"
            );
            self.runtime_status
                .publish_noop(self.current_snapshot.as_ref())
                .await;
            return;
        }

        let diff = diff_counts(&self.current_snapshot, &proposal);
        let next_generation = self.current_snapshot.generation + 1;
        self.runtime_status
            .set_reconcile_phase(ReconcilePhase::Applying)
            .await;
        match self.apply_snapshot(proposal, next_generation, active_snapshot_tx, shutdown) {
            Ok(()) => {
                tracing::info!(
                    generation = next_generation,
                    added_behaviors = diff.added,
                    removed_behaviors = diff.removed,
                    updated_behaviors = diff.updated,
                    default_changed = diff.default_changed,
                    unavailable_changed = diff.unavailable_changed,
                    "runtime reconcile applied"
                );
                if diff.unavailable_changed {
                    for (behavior_id, reason) in &self.current_snapshot.unavailable_behaviors {
                        tracing::warn!(
                            behavior_id = %behavior_id,
                            reason = %reason,
                            "behavior unavailable after runtime reconcile"
                        );
                    }
                }
                self.runtime_status
                    .publish_applied(self.current_snapshot.as_ref())
                    .await;
            }
            Err(error) => {
                tracing::error!(
                    generation = next_generation,
                    added_behaviors = diff.added,
                    removed_behaviors = diff.removed,
                    updated_behaviors = diff.updated,
                    default_changed = diff.default_changed,
                    unavailable_changed = diff.unavailable_changed,
                    error = %error,
                    "runtime reconcile apply failed; keeping previous active generation"
                );
                self.runtime_status
                    .publish_error(&format!("{error:#}"))
                    .await;
            }
        }
    }

    fn apply_snapshot(
        &mut self,
        resolved_snapshot: ResolvedRuntimeSnapshot,
        generation: u64,
        active_snapshot_tx: &watch::Sender<Arc<ActiveRuntimeSnapshot>>,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        resolved_snapshot.validate_config_provenance_scope()?;
        let mut next_slots = HashMap::new();
        let mut retired_slots = Vec::new();
        let mut retired_behaviors: Vec<(String, bool)> = Vec::new();

        for (behavior_id, behavior) in &resolved_snapshot.behaviors {
            let tool_surface = resolved_snapshot
                .tool_surfaces
                .get(behavior_id)
                .cloned()
                .ok_or_else(|| anyhow!("missing tool surface for behavior {behavior_id}"))?;
            let config_provenance = resolved_snapshot.scoped_config_provenance_for(behavior_id);
            let executor_capacity =
                behavior_executor_capacity(behavior, &resolved_snapshot.backend_admission_configs);

            match self.active_slots.remove(behavior_id) {
                Some(existing)
                    if existing.matches(
                        behavior,
                        &tool_surface,
                        &config_provenance,
                        executor_capacity,
                    ) =>
                {
                    next_slots.insert(behavior_id.clone(), existing);
                }
                Some(existing) => {
                    retired_slots.push(existing);
                    retired_behaviors.push((behavior_id.clone(), true));
                    next_slots.insert(
                        behavior_id.clone(),
                        spawn_slot_with_capacity(
                            behavior.clone(),
                            tool_surface,
                            config_provenance,
                            executor_capacity,
                            self.retry_policy.clone(),
                            self.runner.clone(),
                            shutdown.clone(),
                            self.slot_failure_policy.clone(),
                        ),
                    );
                }
                None => {
                    next_slots.insert(
                        behavior_id.clone(),
                        spawn_slot_with_capacity(
                            behavior.clone(),
                            tool_surface,
                            config_provenance,
                            executor_capacity,
                            self.retry_policy.clone(),
                            self.runner.clone(),
                            shutdown.clone(),
                            self.slot_failure_policy.clone(),
                        ),
                    );
                }
            }
        }

        retired_behaviors.extend(
            self.active_slots
                .keys()
                .map(|behavior_id| (behavior_id.clone(), false)),
        );
        retired_slots.extend(self.active_slots.drain().map(|(_, slot)| slot));

        let dispatchers = next_slots
            .iter()
            .map(|(behavior_id, slot)| (behavior_id.clone(), slot.dispatcher.clone()))
            .collect();
        let executor_capacities = next_slots
            .iter()
            .map(|(behavior_id, slot)| (behavior_id.clone(), slot.executor_capacity))
            .collect();
        let executor_queue_capacities = next_slots
            .iter()
            .map(|(behavior_id, slot)| (behavior_id.clone(), slot.queue_capacity))
            .collect();
        let next_snapshot = Arc::new(resolved_snapshot.activate_with_executor_metadata(
            generation,
            dispatchers,
            executor_capacities,
            executor_queue_capacities,
        ));
        self.admission_registry
            .reconcile(generation, &next_snapshot.backend_admission_configs);

        self.current_snapshot = next_snapshot.clone();
        self.active_slots = next_slots;
        active_snapshot_tx
            .send(next_snapshot)
            .map_err(|_| anyhow!("active runtime snapshot receiver closed"))?;

        for slot in retired_slots {
            retire_slot(slot);
        }
        if let Some(policy) = self.slot_failure_policy.clone() {
            tokio::spawn(async move {
                for (behavior_id, recreated) in retired_behaviors {
                    policy.on_slot_retired(&behavior_id, recreated).await;
                }
            });
        }

        Ok(())
    }

    async fn shutdown_slots(self) {
        for slot in self.active_slots.into_values() {
            let _ = slot.state_tx.send(BehaviorSlotState::Retiring);
            drop(slot.dispatcher);
            if let Err(error) = slot.handle.await {
                if !error.is_cancelled() {
                    tracing::error!(error = %error, "behavior slot join failed during shutdown");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
