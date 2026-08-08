use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use defra_node::EmbeddedNode;

use crate::event_delivery_contract::{EventDeliveryRuntimeContract, EventDeliverySourceContract};
use crate::tool_call_lifecycle::IllegalToolCallTransition;
use crate::UpdateSubscriptionSource;

mod cooldown;
mod query;
#[cfg(test)]
mod tests;

use cooldown::{
    mark_processed, prune_processed_requests, request_is_cooling_down,
    take_next_eligible_pending_request, GOSSIP_FALLBACK_POLL, PROCESSED_REQUEST_COOLDOWN,
};
pub(crate) use query::{
    load_agent_request_at_cid, load_agent_request_at_cid_in_txn,
    load_agent_request_at_cid_with_identity,
};

#[derive(Debug, Clone)]
pub struct AgentRequest {
    pub doc_id: String,
    pub request_id: String,
    pub agent_did: String,
    pub requester_did: Option<String>,
    pub behavior_id: Option<String>,
    pub session_id: String,
    pub content: String,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub top_k: Option<i64>,
    pub max_tokens: Option<i64>,
    pub metadata: Option<String>,
    pub execution_origin: Option<String>,
    pub created_at: String,
    pub deadline: Option<String>,
    pub subagent_depth: u32,
    pub caused_by_parent_request_id: Option<String>,
    pub caused_by_parent_tool_call_id: Option<String>,
}

pub fn validate_agent_request_subagent_coherence(req: &AgentRequest) -> Result<()> {
    let has_parent_req = req.caused_by_parent_request_id.is_some();
    let has_parent_tc = req.caused_by_parent_tool_call_id.is_some();
    let request_only_control_link =
        has_parent_req && !has_parent_tc && (is_steering_queue(req) || is_goal_queue(req));
    if has_parent_req != has_parent_tc && !request_only_control_link {
        return Err(IllegalToolCallTransition::ParentLinkageIncoherent.into());
    }
    let is_top_level = !has_parent_req;
    if is_top_level && req.subagent_depth != 0 {
        return Err(IllegalToolCallTransition::ParentLinkageIncoherent.into());
    }
    if !is_top_level && req.subagent_depth == 0 && !is_goal_queue(req) {
        return Err(IllegalToolCallTransition::ParentLinkageIncoherent.into());
    }
    Ok(())
}

fn is_steering_queue(req: &AgentRequest) -> bool {
    let Some(metadata) = req
        .metadata
        .as_deref()
        .map(str::trim)
        .filter(|m| !m.is_empty())
    else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(metadata) else {
        return false;
    };
    value
        .get("queue")
        .and_then(|queue| queue.get("source"))
        .and_then(serde_json::Value::as_str)
        == Some("steering")
}

fn is_goal_queue(req: &AgentRequest) -> bool {
    crate::lifecycle::queue::is_goal_queue(req.metadata.as_deref())
}

pub trait Watcher: Send + Sync {
    fn next_request(
        &mut self,
    ) -> impl std::future::Future<Output = Option<Result<AgentRequest>>> + Send;
}

pub struct DefraWatcher {
    node: Arc<EmbeddedNode>,
    agent_did: String,
    subscription_source: Arc<dyn UpdateSubscriptionSource>,
    subscription: events::Subscription,
    processed_request_ids: HashMap<String, Instant>,
}

impl DefraWatcher {
    pub fn new(node: Arc<EmbeddedNode>, agent_did: &str) -> Self {
        Self::with_subscription_source(node.clone(), node, agent_did)
    }

    pub fn with_subscription_source(
        subs: Arc<dyn UpdateSubscriptionSource>,
        node: Arc<EmbeddedNode>,
        agent_did: &str,
    ) -> Self {
        let subscription = subs.subscribe_updates();
        Self {
            node,
            agent_did: agent_did.to_string(),
            subscription_source: subs,
            subscription,
            processed_request_ids: HashMap::new(),
        }
    }
}

impl EventDeliveryRuntimeContract for DefraWatcher {
    const EVENT_DELIVERY_CONTRACT: EventDeliverySourceContract = EventDeliverySourceContract {
        name: "Watcher",
        dedupe_policy: "ttl_cooldown",
        rescan_bounded_by: 1,
        deviation: None,
    };
}

fn request_update_wakeup(message: &events::Message) -> Option<&events::Update> {
    message.as_update()
}

impl Watcher for DefraWatcher {
    async fn next_request(&mut self) -> Option<Result<AgentRequest>> {
        loop {
            let now = Instant::now();
            prune_processed_requests(&mut self.processed_request_ids, now);

            match self.pending_requests().await {
                Ok(requests) => {
                    let requests = requests
                        .into_iter()
                        .filter(|request| !is_deprecated_background_completion_wakeup(request))
                        .collect::<Vec<_>>();
                    let pending_count = requests.len();
                    if let Some(request) = take_next_eligible_pending_request(
                        &mut self.processed_request_ids,
                        requests,
                        now,
                    ) {
                        return Some(Ok(request));
                    }

                    if pending_count > 0 {
                        tracing::debug!(
                            pending_count,
                            cooldown_secs = PROCESSED_REQUEST_COOLDOWN.as_secs(),
                            "all pending requests are cooling down"
                        );
                    }
                }
                Err(e) => return Some(Err(e)),
            }

            let msg = match tokio::time::timeout(GOSSIP_FALLBACK_POLL, self.subscription.recv())
                .await
            {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    tracing::warn!(
                        "request watcher subscription channel closed; reopening after durable poll"
                    );
                    tokio::time::sleep(
                            crate::trigger_engine::subscription_source::UPDATE_SUBSCRIPTION_REOPEN_DELAY,
                        )
                        .await;
                    self.subscription = self.subscription_source.subscribe_updates();
                    continue;
                }
                Err(_timeout) => {
                    tracing::trace!("gossip quiet, polling for pending requests");
                    continue;
                }
            };

            let Some(update) = request_update_wakeup(&msg) else {
                continue;
            };

            let doc_id = &update.doc_id;
            tracing::trace!(doc_id = %doc_id, is_relay = update.is_relay, "DefraDB update event received");

            let dropped = self.subscription.check_and_reset_dropped();
            if dropped > 0 {
                tracing::warn!(
                    dropped = dropped,
                    "event bus dropped messages — may have missed requests"
                );
            }

            match self.try_fetch_request(doc_id).await {
                Ok(Some(request)) => {
                    if is_deprecated_background_completion_wakeup(&request) {
                        continue;
                    }
                    let now = Instant::now();
                    if request_is_cooling_down(
                        &mut self.processed_request_ids,
                        &request.request_id,
                        now,
                    ) {
                        tracing::debug!(
                            request_id = %request.request_id,
                            doc_id = %doc_id,
                            cooldown_secs = PROCESSED_REQUEST_COOLDOWN.as_secs(),
                            "skipping cooling-down P2P request"
                        );
                        continue;
                    }
                    tracing::info!(
                        request_id = %request.request_id,
                        session_id = %request.session_id,
                        "new agent request detected via P2P"
                    );
                    mark_processed(&mut self.processed_request_ids, &request.request_id, now);
                    return Some(Ok(request));
                }
                Ok(None) => continue,
                Err(e) => {
                    tracing::error!(error = %e, doc_id = %doc_id, "failed to query agent request");
                    return Some(Err(e));
                }
            }
        }
    }
}

/// Legacy runtimes persisted a synthetic scheduled request when a background
/// child completed. Reading one of those rows must neither execute nor mutate
/// it: durable cleanup is an explicit operator action, not a watcher side
/// effect.
fn is_deprecated_background_completion_wakeup(request: &AgentRequest) -> bool {
    let deprecated = crate::lifecycle::queue::is_deprecated_background_completion_wakeup(
        request.execution_origin.as_deref(),
        request.metadata.as_deref(),
    );
    if deprecated {
        tracing::warn!(
            request_id = %request.request_id,
            session_id = %request.session_id,
            "ignored deprecated background completion wake without mutating it"
        );
    }
    deprecated
}
