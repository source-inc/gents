use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, oneshot, watch};

use crate::runtime_snapshot::ResolvedRuntimeSnapshot;
use crate::runtime_status::{ReconcilePhase, RuntimeStatusHandle};

use super::super::document_view;
use super::super::DocumentResolveContext;

pub(super) const CONTROL_RECONCILE_DEBOUNCE: Duration = Duration::from_secs(5);
const CONTROL_RECONCILE_SETTLE_RETRY: Duration = Duration::from_secs(1);
const CONTROL_RECONCILE_SETTLE_WINDOW: Duration = Duration::from_secs(60);
pub(super) const CONTROL_FULL_RESCAN_INTERVAL: Duration = Duration::from_secs(10);
const CONTROL_WATCHER_IDLE_SLEEP: Duration = Duration::from_secs(60 * 60 * 24 * 365);

/// Why the control view became dirty.
///
/// Subscription and measured-health wakes are deliberately debounced so a
/// burst converges to one proposal. A periodic full scan is already the
/// delayed correctness fallback for a missed wake and must be resolved
/// immediately. Keeping this decision pure lets tests assert scheduling
/// semantics without treating time spent in asynchronous DefraDB reads as
/// debounce time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileWake {
    LiveUpdate,
    MeasuredHealth,
    PeriodicRescan,
}

impl ReconcileWake {
    fn delay(self) -> Duration {
        match self {
            Self::PeriodicRescan => Duration::ZERO,
            Self::LiveUpdate | Self::MeasuredHealth => CONTROL_RECONCILE_DEBOUNCE,
        }
    }
}

fn reconcile_deadline(wake: ReconcileWake) -> tokio::time::Instant {
    tokio::time::Instant::now() + wake.delay()
}

pub(super) async fn run_control_watcher(
    node: Arc<defra_node::EmbeddedNode>,
    subscription: events::Subscription,
    agent_did: String,
    resolve_context: DocumentResolveContext,
    proposals_tx: mpsc::Sender<ResolvedRuntimeSnapshot>,
    runtime_status: RuntimeStatusHandle,
    health_events_rx: mpsc::Receiver<()>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    run_control_watcher_inner(
        node,
        Some(subscription),
        agent_did,
        resolve_context,
        proposals_tx,
        runtime_status,
        health_events_rx,
        shutdown,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_control_watcher_inner(
    node: Arc<defra_node::EmbeddedNode>,
    mut subscription: Option<events::Subscription>,
    agent_did: String,
    resolve_context: DocumentResolveContext,
    proposals_tx: mpsc::Sender<ResolvedRuntimeSnapshot>,
    runtime_status: RuntimeStatusHandle,
    mut health_events_rx: mpsc::Receiver<()>,
    mut shutdown: watch::Receiver<bool>,
    startup_ready: Option<oneshot::Sender<()>>,
) -> Result<()> {
    let mut document_view =
        document_view::load_document_runtime_view(node.as_ref(), &agent_did).await?;
    let sleep = tokio::time::sleep(CONTROL_WATCHER_IDLE_SLEEP);
    tokio::pin!(sleep);
    let mut full_rescan = tokio::time::interval(CONTROL_FULL_RESCAN_INTERVAL);
    full_rescan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval` ticks immediately once. Consume that tick so the fallback is
    // genuinely periodic and does not manufacture a startup reconcile.
    full_rescan.tick().await;
    if let Some(startup_ready) = startup_ready {
        let _ = startup_ready.send(());
    }
    let mut dirty = false;
    let mut pending_visibility = false;
    let mut settle_deadline = None;
    let mut last_proposed_fingerprint = None::<String>;

    loop {
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = full_rescan.tick() => {
                match document_view::load_document_runtime_view(node.as_ref(), &agent_did).await {
                    Ok(reloaded) => {
                        document_view = reloaded;
                        pending_visibility = document_view.has_unresolved_behavior_references();
                        dirty = true;
                        settle_deadline = pending_visibility.then(|| {
                            tokio::time::Instant::now() + CONTROL_RECONCILE_SETTLE_WINDOW
                        });
                        runtime_status
                            .set_reconcile_phase(ReconcilePhase::Debouncing)
                            .await;
                        // A periodic rescan is already the delayed fallback for a
                        // missed or closed subscription. Resolve it immediately:
                        // applying the event debounce here would make the runtime
                        // non-idle for half of every rescan interval even when the
                        // loaded configuration is unchanged.
                        sleep
                            .as_mut()
                            .reset(reconcile_deadline(ReconcileWake::PeriodicRescan));
                    }
                    Err(error) => {
                        tracing::error!(
                            agent_did = %agent_did,
                            error = %error,
                            "runtime control watcher periodic full rescan failed"
                        );
                        runtime_status.publish_error(&format!("{error:#}")).await;
                    }
                }
            }
            _ = &mut sleep, if dirty => {
                if pending_visibility || settle_deadline.is_some() {
                    match document_view::load_document_runtime_view(node.as_ref(), &agent_did).await {
                        Ok(reloaded) => {
                            document_view = reloaded;
                            pending_visibility = document_view.has_unresolved_behavior_references();
                        }
                        Err(error) => {
                            tracing::error!(
                                agent_did = %agent_did,
                                error = %error,
                                "runtime control watcher failed to refresh document view during settle window"
                            );
                            runtime_status.publish_error(&format!("{error:#}")).await;
                            if settle_deadline.is_some_and(|deadline| tokio::time::Instant::now() < deadline) {
                                dirty = true;
                                sleep.as_mut().reset(tokio::time::Instant::now() + CONTROL_RECONCILE_SETTLE_RETRY);
                            } else {
                                dirty = false;
                                settle_deadline = None;
                                sleep.as_mut().reset(tokio::time::Instant::now() + CONTROL_WATCHER_IDLE_SLEEP);
                            }
                            continue;
                        }
                    }
                }
                if pending_visibility
                    && settle_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
                {
                    let pending_details = document_view.pending_visibility_details();
                    let pending_summary = super::router::format_pending_visibility_error(&pending_details);
                    tracing::warn!(
                        agent_did = %agent_did,
                        pending_references = %pending_details.join("; "),
                        "runtime control watcher is still waiting for referenced control documents"
                    );
                    runtime_status
                        .publish_error(&pending_summary)
                        .await;
                }
                if pending_visibility
                {
                    dirty = true;
                    sleep.as_mut().reset(tokio::time::Instant::now() + CONTROL_RECONCILE_SETTLE_RETRY);
                    continue;
                }
                runtime_status
                    .set_reconcile_phase(ReconcilePhase::Resolving)
                    .await;
                let mut proposed_update = false;
                match document_view::resolve_document_runtime_snapshot_from_view(
                    node.as_ref(),
                    &resolve_context,
                    &document_view,
                )
                .await
                {
                    Ok(snapshot) => {
                        let fingerprint = snapshot.configuration_fingerprint();
                        if last_proposed_fingerprint.as_deref() != Some(fingerprint.as_str()) {
                            if proposals_tx.send(snapshot).await.is_err() {
                                return Ok(());
                            }
                            last_proposed_fingerprint = Some(fingerprint);
                            proposed_update = true;
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            agent_did = %agent_did,
                            error = %error,
                            "runtime reconcile resolve failed; keeping previous active generation"
                        );
                        runtime_status.publish_error(&format!("{error:#}")).await;
                    }
                }
                if !proposed_update {
                    runtime_status
                        .set_reconcile_phase(ReconcilePhase::Idle)
                        .await;
                }
                if settle_deadline.is_some_and(|deadline| tokio::time::Instant::now() < deadline) {
                    dirty = true;
                    sleep.as_mut().reset(tokio::time::Instant::now() + CONTROL_RECONCILE_SETTLE_RETRY);
                } else {
                    dirty = false;
                    settle_deadline = None;
                    sleep.as_mut().reset(tokio::time::Instant::now() + CONTROL_WATCHER_IDLE_SLEEP);
                }
            }
            Some(()) = health_events_rx.recv() => {
                tracing::info!(
                    agent_did = %agent_did,
                    "backend measured-health transition detected; scheduling reconcile"
                );
                dirty = true;
                runtime_status
                    .set_reconcile_phase(ReconcilePhase::Debouncing)
                    .await;
                sleep
                    .as_mut()
                    .reset(reconcile_deadline(ReconcileWake::MeasuredHealth));
            }
            message = async {
                subscription
                    .as_mut()
                    .expect("open control subscription must be present")
                    .recv()
                    .await
            }, if subscription.is_some() => {
                let Some(message) = message else {
                    tracing::warn!(
                        agent_did = %agent_did,
                        "runtime control update subscription closed; forcing durable rescan before reopen"
                    );
                    match document_view::load_document_runtime_view(node.as_ref(), &agent_did).await {
                        Ok(reloaded) => {
                            document_view = reloaded;
                            pending_visibility = document_view.has_unresolved_behavior_references();
                            dirty = true;
                            settle_deadline = pending_visibility.then(|| {
                                tokio::time::Instant::now() + CONTROL_RECONCILE_SETTLE_WINDOW
                            });
                            runtime_status
                                .set_reconcile_phase(ReconcilePhase::Debouncing)
                                .await;
                            sleep
                                .as_mut()
                                .reset(reconcile_deadline(ReconcileWake::PeriodicRescan));
                        }
                        Err(error) => {
                            tracing::error!(
                                agent_did = %agent_did,
                                error = %error,
                                "runtime control watcher durable rescan after subscription close failed"
                            );
                            runtime_status.publish_error(&format!("{error:#}")).await;
                        }
                    }
                    tokio::select! {
                        _ = shutdown.changed() => return Ok(()),
                        _ = tokio::time::sleep(
                            crate::trigger_engine::subscription_source::UPDATE_SUBSCRIPTION_REOPEN_DELAY,
                        ) => {}
                    }
                    subscription = Some(node.subscribe(&[defra_node::EventName::Update]));
                    tracing::info!(
                        agent_did = %agent_did,
                        "runtime control watcher reopened global Update subscription"
                    );
                    continue;
                };

                let dropped = subscription
                    .as_mut()
                    .expect("open control subscription must be present")
                    .check_and_reset_dropped();
                if dropped > 0 {
                    tracing::warn!(
                        agent_did = %agent_did,
                        dropped = dropped,
                        "runtime control watcher dropped events, forcing full reconcile"
                    );
                    match document_view::load_document_runtime_view(node.as_ref(), &agent_did).await {
                        Ok(reloaded) => {
                            document_view = reloaded;
                            pending_visibility = document_view.has_unresolved_behavior_references();
                        }
                        Err(error) => {
                            tracing::error!(
                                agent_did = %agent_did,
                                error = %error,
                                "runtime control watcher failed to resync document view after dropped events"
                            );
                            runtime_status.publish_error(&format!("{error:#}")).await;
                            continue;
                        }
                    }
                    dirty = true;
                    settle_deadline =
                        Some(tokio::time::Instant::now() + CONTROL_RECONCILE_SETTLE_WINDOW);
                    runtime_status
                        .set_reconcile_phase(ReconcilePhase::Debouncing)
                        .await;
                    sleep
                        .as_mut()
                        .reset(reconcile_deadline(ReconcileWake::LiveUpdate));
                    continue;
                }

                let Some(update) = message.as_update() else {
                    continue;
                };
                match document_view::apply_control_update(
                    node.as_ref(),
                    &agent_did,
                    update.collection_id.as_str(),
                    &update.doc_id,
                    &mut document_view,
                )
                .await
                {
                    Ok(document_view::ControlUpdateOutcome::Irrelevant) => continue,
                    Ok(document_view::ControlUpdateOutcome::Applied) => {}
                    Ok(document_view::ControlUpdateOutcome::PendingVisibility) => {
                        pending_visibility = true;
                    }
                    Err(error) => {
                        tracing::error!(
                            agent_did = %agent_did,
                            collection_id = %update.collection_id,
                            doc_id = %update.doc_id,
                            error = %error,
                            "runtime control update apply failed; forcing full resync"
                        );
                        match document_view::load_document_runtime_view(node.as_ref(), &agent_did).await {
                            Ok(reloaded) => {
                                document_view = reloaded;
                                pending_visibility = document_view.has_unresolved_behavior_references();
                            }
                            Err(resync_error) => {
                                tracing::error!(
                                    agent_did = %agent_did,
                                    error = %resync_error,
                                    "runtime control watcher failed to resync document view after update error"
                                );
                                runtime_status
                                    .publish_error(&format!("{resync_error:#}"))
                                    .await;
                                continue;
                            }
                        }
                    }
                }

                tracing::info!(
                    agent_did = %agent_did,
                    doc_id = %update.doc_id,
                    collection_id = %update.collection_id,
                    is_relay = update.is_relay,
                    "runtime control update detected"
                );
                dirty = true;
                settle_deadline =
                    Some(tokio::time::Instant::now() + CONTROL_RECONCILE_SETTLE_WINDOW);
                runtime_status
                    .set_reconcile_phase(ReconcilePhase::Debouncing)
                    .await;
                sleep
                    .as_mut()
                    .reset(reconcile_deadline(ReconcileWake::LiveUpdate));
            }
        }
    }
}

#[cfg(test)]
mod scheduling_tests {
    use super::*;

    #[test]
    fn periodic_rescan_is_immediate_while_live_wakes_are_debounced() {
        assert_eq!(ReconcileWake::PeriodicRescan.delay(), Duration::ZERO);
        assert_eq!(
            ReconcileWake::LiveUpdate.delay(),
            CONTROL_RECONCILE_DEBOUNCE
        );
        assert_eq!(
            ReconcileWake::MeasuredHealth.delay(),
            CONTROL_RECONCILE_DEBOUNCE
        );
    }
}
