use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::sync::watch;

use crate::lifecycle::{ClaimOutcome, ExecutionOrigin, RequestLifecycle};
use crate::runtime_snapshot::ActiveRuntimeSnapshot;
use crate::runtime_status::RuntimeStatusHandle;
use crate::streaming::{DefraStreamWriter, StreamWriter};
use crate::watcher::{AgentRequest, DefraWatcher, Watcher};

use super::context::BehaviorResolution;

pub(super) async fn run_router(
    node: Arc<defra_node::EmbeddedNode>,
    agent_did: String,
    local_deployment_id: String,
    active_snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    shutdown: watch::Receiver<bool>,
    startup_demotions: Arc<crate::startup_readiness::StartupDemotions>,
) -> Result<()> {
    let watcher =
        DefraWatcher::new(node.clone(), &agent_did).with_local_deployment_id(local_deployment_id);
    run_router_with_watcher(
        node,
        agent_did,
        watcher,
        active_snapshot_rx,
        shutdown,
        startup_demotions,
    )
    .await
}

async fn run_router_with_watcher<W>(
    node: Arc<defra_node::EmbeddedNode>,
    agent_did: String,
    mut watcher: W,
    mut active_snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    mut shutdown: watch::Receiver<bool>,
    startup_demotions: Arc<crate::startup_readiness::StartupDemotions>,
) -> Result<()>
where
    W: Watcher,
{
    let mut active_snapshot = active_snapshot_rx.borrow().clone();

    loop {
        let Some(request) = wait_for_next_request_with_latest_snapshot(
            &agent_did,
            &mut watcher,
            &mut active_snapshot,
            &mut active_snapshot_rx,
            &mut shutdown,
        )
        .await?
        else {
            return Ok(());
        };

        let resolution = resolve_behavior_for_request(
            node.as_ref(),
            &request,
            active_snapshot.default_behavior_id.as_str(),
        )
        .await?;
        if let Some(reason) = resolution.rejection_reason.as_deref() {
            tracing::warn!(
                request_id = %request.request_id,
                session_id = %request.session_id,
                behavior_id = %resolution.behavior_id,
                reason = %reason,
                "rejecting request before dispatch"
            );
            fail_routed_request(
                node.clone(),
                agent_did.as_str(),
                request,
                resolution.behavior_id.as_str(),
                reason,
            )
            .await?;
            continue;
        }

        if let Some(reason) = startup_demotions.reason(&resolution.behavior_id) {
            tracing::warn!(
                request_id = %request.request_id,
                session_id = %request.session_id,
                behavior_id = %resolution.behavior_id,
                reason = %reason,
                "behavior demoted at startup; rejecting request"
            );
            fail_routed_request(
                node.clone(),
                agent_did.as_str(),
                request,
                resolution.behavior_id.as_str(),
                reason.as_str(),
            )
            .await?;
            continue;
        }

        match active_snapshot.dispatchers.get(&resolution.behavior_id) {
            Some(dispatcher) => {
                tracing::info!(
                    request_id = %request.request_id,
                    session_id = %request.session_id,
                    behavior_id = %resolution.behavior_id,
                    "dispatching request to behavior executor"
                );
                dispatcher.send(request).await.map_err(|_| {
                    anyhow!(
                        "executor queue for behavior {} closed unexpectedly",
                        resolution.behavior_id
                    )
                })?;
            }
            None => {
                let error_message = active_snapshot
                    .unavailable_reason(&resolution.behavior_id)
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| {
                        format!(
                            "behavior {} is not loaded for principal {}",
                            resolution.behavior_id, agent_did
                        )
                    });
                tracing::warn!(
                    request_id = %request.request_id,
                    session_id = %request.session_id,
                    behavior_id = %resolution.behavior_id,
                    reason = %error_message,
                    "behavior unavailable for request"
                );
                fail_routed_request(
                    node.clone(),
                    agent_did.as_str(),
                    request,
                    resolution.behavior_id.as_str(),
                    error_message.as_str(),
                )
                .await?;
            }
        }
    }
}

pub(super) async fn wait_for_next_request_with_latest_snapshot<W>(
    agent_did: &str,
    watcher: &mut W,
    active_snapshot: &mut Arc<ActiveRuntimeSnapshot>,
    active_snapshot_rx: &mut watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<AgentRequest>>
where
    W: Watcher,
{
    loop {
        *active_snapshot = active_snapshot_rx.borrow_and_update().clone();
        let request = tokio::select! {
            biased;

            _ = shutdown.changed() => return Ok(None),
            changed = active_snapshot_rx.changed() => {
                if changed.is_err() {
                    return Ok(None);
                }
                *active_snapshot = active_snapshot_rx.borrow_and_update().clone();
                continue;
            }
            req = watcher.next_request() => {
                match req {
                    Some(Ok(req)) => req,
                    Some(Err(error)) => {
                        tracing::error!(agent_did = %agent_did, error = %error, "watcher error, retrying");
                        continue;
                    }
                    None => return Ok(None),
                }
            }
        };
        *active_snapshot = active_snapshot_rx.borrow_and_update().clone();
        return Ok(Some(request));
    }
}

pub(super) async fn run_router_generation_observer(
    mut active_snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    runtime_status: RuntimeStatusHandle,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut observed_generation = 0u64;

    loop {
        let active_snapshot = active_snapshot_rx.borrow().clone();
        if observed_generation != active_snapshot.generation {
            runtime_status
                .publish_router_generation(active_snapshot.generation)
                .await;
            observed_generation = active_snapshot.generation;
        }

        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            changed = active_snapshot_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
        }
    }
}

pub(super) async fn resolve_behavior_for_request(
    node: &defra_node::EmbeddedNode,
    request: &AgentRequest,
    default_behavior_id: &str,
) -> Result<BehaviorResolution> {
    let requested_behavior_id =
        normalize_optional_string(request.behavior_id.as_deref()).map(ToOwned::to_owned);
    let session_behavior_id =
        crate::session::load_session_behavior_id(node, &request.session_id).await?;
    let behavior_id = requested_behavior_id
        .clone()
        .or_else(|| session_behavior_id.clone())
        .unwrap_or_else(|| default_behavior_id.to_string());

    let rejection_reason = match (
        session_behavior_id.as_deref(),
        requested_behavior_id.as_deref(),
    ) {
        (Some(existing), Some(requested)) if existing != requested => Some(format!(
            "session {} is pinned to behavior {} and cannot switch to {}",
            request.session_id, existing, requested
        )),
        _ => None,
    };

    Ok(BehaviorResolution {
        behavior_id,
        rejection_reason,
    })
}

async fn fail_routed_request(
    node: Arc<defra_node::EmbeddedNode>,
    agent_did: &str,
    request: AgentRequest,
    behavior_id: &str,
    error_message: &str,
) -> Result<()> {
    let mut lifecycle = RequestLifecycle::new_with_execution_binding(
        node.clone(),
        behavior_id,
        agent_did,
        request.clone(),
        Duration::from_secs(30).as_secs(),
        ExecutionOrigin::from_persisted(request.execution_origin.as_deref()),
        "",
    );

    match lifecycle.claim_with_identity().await {
        Ok(ClaimOutcome::Claimed) => {}
        Ok(ClaimOutcome::Queued) => {
            tracing::info!(
                request_id = %request.request_id,
                session_id = %request.session_id,
                behavior_id = %behavior_id,
                "rejected request queued behind an earlier same-session request"
            );
            return Ok(());
        }
        Ok(ClaimOutcome::Interrupted) | Ok(ClaimOutcome::Expired) => return Ok(()),
        Err(error) => {
            tracing::warn!(
                request_id = %request.request_id,
                session_id = %request.session_id,
                behavior_id = %behavior_id,
                error = %error,
                "failed to claim rejected request"
            );
            return Ok(());
        }
    }

    let stream_writer = DefraStreamWriter::new(node, agent_did, Duration::from_millis(0));
    if lifecycle.response_exists().await.unwrap_or(false) {
        stream_writer
            .finalize_existing_request_error(&request.request_id, error_message)
            .await?;
        return Ok(());
    }

    let doc_id = stream_writer
        .begin_with_requester_did(
            &request.session_id,
            &request.request_id,
            Some(&request.doc_id),
            behavior_id,
            request.requester_did.as_deref(),
        )
        .await?;
    let _ = stream_writer
        .write_tokens(&doc_id, &format!("Error: {error_message}"))
        .await?;
    stream_writer.finalize_error(&doc_id, error_message).await?;
    lifecycle.fail_with_reason(error_message).await?;
    Ok(())
}

fn normalize_optional_string(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

pub(in crate::agent) fn default_hostname() -> String {
    hostname::get()
        .map(|host| host.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

pub(super) fn format_pending_visibility_error(details: &[String]) -> String {
    if details.is_empty() {
        return "waiting for referenced control documents to become visible".to_string();
    }
    format!(
        "waiting for referenced control documents to become visible: {}",
        details.join("; ")
    )
}
