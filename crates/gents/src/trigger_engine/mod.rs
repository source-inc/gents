//! Trigger engine scaffold.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{watch, Mutex};
use tokio_util::sync::CancellationToken;

use crate::runtime_snapshot::ActiveRuntimeSnapshot;

pub(crate) mod cross_deployment_cancel_mirror;
pub(crate) mod event_source;
pub(crate) mod goal_source;
pub(crate) mod manual_source;
pub(crate) mod production_materializer;
pub(crate) mod schedule_source;
pub(crate) mod subagent_source;
pub mod subscription_source;

#[cfg(test)]
mod tests;

type TriggerLockKey = (String, String, TriggerKind);
type TriggerLock = Arc<Mutex<()>>;
type TriggerLockMap = HashMap<TriggerLockKey, TriggerLock>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TriggerKind {
    Schedule,
    #[allow(dead_code)]
    Event,
    #[allow(dead_code)]
    Manual,
}

impl TriggerKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            TriggerKind::Schedule => "schedule",
            TriggerKind::Event => "event",
            TriggerKind::Manual => "manual",
        }
    }
}

pub struct FireIntent {
    pub trigger_id: Option<String>,
    pub trigger_kind: TriggerKind,
    pub task: crate::runtime_snapshot::ResolvedTask,
    pub concurrency: crate::runtime_snapshot::ConcurrencyMode,
    pub event_vars: serde_json::Value,
    pub doc_vars: Option<serde_json::Value>,
    pub args_vars: Option<serde_json::Value>,
    pub pre_materialized_request_id: Option<String>,
    /// Stable logical id for materialization after rendering/concurrency.
    /// Crash-recoverable sources use this to make request creation replayable.
    pub materialization_request_id: Option<String>,
    pub on_result: Box<dyn FnOnce(FireResult) + Send>,
}

impl FireIntent {
    fn well_formed_error(&self) -> Option<&'static str> {
        match self.trigger_kind {
            TriggerKind::Manual if self.trigger_id.is_some() => {
                Some("Manual trigger intent must not carry trigger_id")
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum FireResult {
    #[allow(dead_code)]
    Fired {
        request_id: String,
    },
    #[allow(dead_code)]
    Skipped {
        reason: String,
    },
    Errored {
        error: String,
    },
}

pub trait TriggerSource: Send + Sync {
    fn next_fire(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<FireIntent>> + Send + '_>>;
}

pub(crate) trait MaterializerHandle: Send + Sync {
    fn materialize(
        &self,
        task: &crate::runtime_snapshot::ResolvedTask,
        trigger_id: Option<&str>,
        trigger_kind: TriggerKind,
        rendered_prompt: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + '_>>;

    fn materialize_with_request_id(
        &self,
        task: &crate::runtime_snapshot::ResolvedTask,
        trigger_id: Option<&str>,
        trigger_kind: TriggerKind,
        rendered_prompt: &str,
        _request_id: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + '_>>
    {
        self.materialize(task, trigger_id, trigger_kind, rendered_prompt)
    }

    /// Check whether any active runtime `AgentRequest` of `agent_did` is
    /// currently bound to this trigger. Used by the concurrency gate to
    /// decide whether a new fire should skip or supersede.
    ///
    /// The DID scope is load-bearing: on a replicated fleet the store also
    /// holds OTHER agents' requests for the same human-chosen trigger id, and
    /// those must never gate this agent's fires (#605).
    fn has_active_runtime_request_for_trigger(
        &self,
        agent_did: &str,
        trigger_id: &str,
        trigger_kind: TriggerKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + '_>>;

    fn supersede_active_runtime_requests_for_trigger(
        &self,
        agent_did: &str,
        trigger_id: &str,
        trigger_kind: TriggerKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<usize>> + Send + '_>>;
}

/// Scaffolding for the trigger engine.
///
/// The engine owns a read handle onto the active runtime snapshot (to look up
/// behaviors / concurrency / enabled gates at fire time) and a materializer
/// handle (to create requests). Per-trigger mutexes serialize dispatches that
/// share a trigger id so the concurrency gate and request materialization
/// land atomically with respect to each other.
pub(crate) struct TriggerEngine {
    #[allow(dead_code)]
    snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    #[allow(dead_code)]
    materializer: Arc<dyn MaterializerHandle>,
    #[allow(dead_code)]
    per_trigger_locks: Mutex<TriggerLockMap>,
}

impl TriggerEngine {
    pub(crate) fn new(
        snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
        materializer: Arc<dyn MaterializerHandle>,
    ) -> Self {
        Self {
            snapshot_rx,
            materializer,
            per_trigger_locks: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn run(self, sources: Vec<Box<dyn TriggerSource>>, cancel: CancellationToken) {
        let engine = Arc::new(self);
        let mut join_set = tokio::task::JoinSet::new();
        for mut source in sources {
            let engine = engine.clone();
            let cancel = cancel.clone();
            join_set.spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        intent = source.next_fire() => {
                            match intent {
                                Some(intent) => {
                                    let _ = engine.dispatch(intent).await;
                                }
                                None => break,
                            }
                        }
                    }
                }
            });
        }
        // Wait for all source drivers to terminate (either via cancel or
        // their source returning `None`). Any panic in a driver task is
        // logged — individual source failures must not bring down the
        // entire engine.
        while let Some(joined) = join_set.join_next().await {
            if let Err(error) = joined {
                if !error.is_cancelled() {
                    tracing::error!(error = %error, "trigger engine source driver panicked");
                }
            }
        }
    }

    #[allow(dead_code)]
    async fn dispatch(&self, intent: FireIntent) -> FireResult {
        if let Some(error) = intent.well_formed_error() {
            let result = FireResult::Errored {
                error: error.to_string(),
            };
            (intent.on_result)(result.clone());
            return result;
        }

        if let Some(request_id) = intent.pre_materialized_request_id.clone() {
            let result = FireResult::Fired { request_id };
            (intent.on_result)(result.clone());
            return result;
        }

        let snapshot = self.snapshot_rx.borrow().clone();

        match intent.trigger_kind {
            TriggerKind::Schedule => {
                let Some(trigger_id) = intent.trigger_id.as_deref() else {
                    let result = FireResult::Errored {
                        error: "Schedule trigger missing trigger_id".to_string(),
                    };
                    (intent.on_result)(result.clone());
                    return result;
                };
                if snapshot.active_schedules().get(trigger_id).is_none() {
                    let result = FireResult::Skipped {
                        reason: "trigger disabled".to_string(),
                    };
                    (intent.on_result)(result.clone());
                    return result;
                }
            }
            TriggerKind::Event => {
                let Some(trigger_id) = intent.trigger_id.as_deref() else {
                    let result = FireResult::Errored {
                        error: "Event trigger missing trigger_id".to_string(),
                    };
                    (intent.on_result)(result.clone());
                    return result;
                };
                if snapshot.active_event_triggers().get(trigger_id).is_none() {
                    let result = FireResult::Skipped {
                        reason: "trigger disabled".to_string(),
                    };
                    (intent.on_result)(result.clone());
                    return result;
                }
            }
            TriggerKind::Manual => {}
        }

        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let (node_scope, ctx_scope) =
            crate::template::task_node_ctx(&snapshot.local_did, &intent.task.behavior_id, &now);
        let scope = crate::template::TemplateScope {
            event: intent.event_vars.clone(),
            doc: intent.doc_vars.clone(),
            args: intent.args_vars.clone(),
            node: node_scope,
            ctx: ctx_scope,
        };
        let rendered = match crate::template::render_template(&intent.task.prompt_template, &scope)
        {
            Ok(s) => s,
            Err(e) => {
                let result = FireResult::Errored {
                    error: format!("template: {e}"),
                };
                (intent.on_result)(result.clone());
                return result;
            }
        };

        let concurrency_agent_did = || {
            snapshot
                .behavior(&intent.task.behavior_id)
                .map(|behavior| behavior.agent_did().to_string())
                .ok_or_else(|| {
                    snapshot
                        .unavailable_reason(&intent.task.behavior_id)
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| {
                            format!("behavior {} is not loaded", intent.task.behavior_id)
                        })
                })
        };
        use crate::runtime_snapshot::ConcurrencyMode;
        match intent.concurrency {
            ConcurrencyMode::Parallel => {}
            ConcurrencyMode::Serial => {
                if let Some(trigger_id) = intent.trigger_id.as_deref() {
                    let agent_did = match concurrency_agent_did() {
                        Ok(did) => did,
                        Err(reason) => {
                            let result = FireResult::Errored {
                                error: format!("concurrency gate: {reason}"),
                            };
                            (intent.on_result)(result.clone());
                            return result;
                        }
                    };
                    match self
                        .materializer
                        .has_active_runtime_request_for_trigger(
                            &agent_did,
                            trigger_id,
                            intent.trigger_kind,
                        )
                        .await
                    {
                        Ok(true) => {
                            let result = FireResult::Skipped {
                                reason: "serial: prior fire still in-flight".to_string(),
                            };
                            (intent.on_result)(result.clone());
                            return result;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            let result = FireResult::Errored {
                                error: format!("in-flight query: {e}"),
                            };
                            (intent.on_result)(result.clone());
                            return result;
                        }
                    }
                }
            }
            ConcurrencyMode::LatestOnly => {
                if let Some(trigger_id) = intent.trigger_id.as_deref() {
                    let agent_did = match concurrency_agent_did() {
                        Ok(did) => did,
                        Err(reason) => {
                            let result = FireResult::Errored {
                                error: format!("concurrency gate: {reason}"),
                            };
                            (intent.on_result)(result.clone());
                            return result;
                        }
                    };
                    let lock_key = (
                        agent_did.clone(),
                        trigger_id.to_owned(),
                        intent.trigger_kind,
                    );
                    let lock = {
                        let mut map = self.per_trigger_locks.lock().await;
                        map.entry(lock_key)
                            .or_insert_with(|| Arc::new(Mutex::new(())))
                            .clone()
                    };
                    let _guard = lock.lock().await;
                    match self
                        .materializer
                        .supersede_active_runtime_requests_for_trigger(
                            &agent_did,
                            trigger_id,
                            intent.trigger_kind,
                        )
                        .await
                    {
                        Ok(_count) => {}
                        Err(e) => {
                            let result = FireResult::Errored {
                                error: format!("supersede: {e}"),
                            };
                            (intent.on_result)(result.clone());
                            return result;
                        }
                    }
                    return self.materialize_after_lock(intent, rendered).await;
                }
            }
        }

        self.materialize_after_lock(intent, rendered).await
    }

    async fn materialize_after_lock(&self, intent: FireIntent, rendered: String) -> FireResult {
        let materialized = match intent.materialization_request_id.as_deref() {
            Some(request_id) => self.materializer.materialize_with_request_id(
                &intent.task,
                intent.trigger_id.as_deref(),
                intent.trigger_kind,
                &rendered,
                request_id,
            ),
            None => self.materializer.materialize(
                &intent.task,
                intent.trigger_id.as_deref(),
                intent.trigger_kind,
                &rendered,
            ),
        };
        let request_id = match materialized.await {
            Ok(id) => id,
            Err(e) => {
                let result = FireResult::Errored {
                    error: format!("materialize: {e}"),
                };
                (intent.on_result)(result.clone());
                return result;
            }
        };

        let result = FireResult::Fired { request_id };
        (intent.on_result)(result.clone());
        result
    }
}

#[doc(hidden)]
pub async fn run_subagent_source_for_test(
    node: Arc<defra_node::EmbeddedNode>,
    snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    cancel: CancellationToken,
) {
    run_subagent_source_for_test_inner(node, snapshot_rx, cancel, None).await;
}

#[doc(hidden)]
pub async fn run_subagent_source_for_test_with_ready(
    node: Arc<defra_node::EmbeddedNode>,
    snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    cancel: CancellationToken,
    ready: tokio::sync::oneshot::Sender<()>,
) {
    run_subagent_source_for_test_inner(node, snapshot_rx, cancel, Some(ready)).await;
}

async fn run_subagent_source_for_test_inner(
    node: Arc<defra_node::EmbeddedNode>,
    snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    cancel: CancellationToken,
    ready: Option<tokio::sync::oneshot::Sender<()>>,
) {
    struct UnusedMaterializer;
    impl MaterializerHandle for UnusedMaterializer {
        fn materialize(
            &self,
            _task: &crate::runtime_snapshot::ResolvedTask,
            _trigger_id: Option<&str>,
            _trigger_kind: TriggerKind,
            _rendered_prompt: &str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + '_>>
        {
            Box::pin(async {
                unreachable!(
                    "SubagentSource fires pre-materialized requests; materialize is never called"
                )
            })
        }

        fn has_active_runtime_request_for_trigger(
            &self,
            _agent_did: &str,
            _trigger_id: &str,
            _trigger_kind: TriggerKind,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + '_>>
        {
            Box::pin(async { Ok(false) })
        }

        fn supersede_active_runtime_requests_for_trigger(
            &self,
            _agent_did: &str,
            _trigger_id: &str,
            _trigger_kind: TriggerKind,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<usize>> + Send + '_>>
        {
            Box::pin(async { Ok(0) })
        }
    }

    let subagent_source: Box<dyn TriggerSource> = Box::new(subagent_source::SubagentSource::new(
        snapshot_rx.clone(),
        node,
        cancel.clone(),
    ));
    if let Some(ready) = ready {
        let _ = ready.send(());
    }
    let materializer: Arc<dyn MaterializerHandle> = Arc::new(UnusedMaterializer);
    let engine = TriggerEngine::new(snapshot_rx, materializer);
    engine.run(vec![subagent_source], cancel).await;
}
