use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use defra_node::EmbeddedNode;
use rig::completion::CompletionError;

use super::client::current_context;
#[cfg(test)]
use super::client::{scope_request, AdmissionCallContext, CallKind};
use super::config::BackendAdmissionConfig;
use super::controller::{BackendAdmissionController, InferenceCallRecord};
use super::permit::AdmissionPermit;
use super::persistence::{persist_call_started, persist_terminal_call};
use super::provenance::RunningInferenceCallProvenance;

#[derive(Clone)]
pub(crate) struct AdmissionRegistry {
    inner: Arc<AdmissionRegistryInner>,
}

pub(super) struct AdmissionRegistryInner {
    node: Arc<EmbeddedNode>,
    runtime_instance_id: String,
    direct_oneoff: bool,
    state: Mutex<RegistryState>,
}

#[derive(Default)]
struct RegistryState {
    active: HashMap<String, Arc<BackendAdmissionController>>,
    draining: HashMap<String, Vec<Arc<BackendAdmissionController>>>,
    pending: HashMap<String, PendingControllerConfig>,
}

#[derive(Clone)]
struct PendingControllerConfig {
    generation: u64,
    config: BackendAdmissionConfig,
}

impl AdmissionRegistry {
    pub(crate) fn new(node: Arc<EmbeddedNode>) -> Self {
        Self {
            inner: Arc::new(AdmissionRegistryInner {
                node,
                runtime_instance_id: uuid::Uuid::new_v4().to_string(),
                direct_oneoff: false,
                state: Mutex::new(RegistryState::default()),
            }),
        }
    }

    /// Standalone one-shot execution has no reconciled runtime generation or
    /// admission controller. It still persists the exact call/render fence,
    /// but only this explicitly constructed registry may issue a direct call.
    pub(crate) fn new_direct_oneshot(node: Arc<EmbeddedNode>) -> Self {
        Self {
            inner: Arc::new(AdmissionRegistryInner {
                node,
                runtime_instance_id: uuid::Uuid::new_v4().to_string(),
                direct_oneoff: true,
                state: Mutex::new(RegistryState::default()),
            }),
        }
    }

    pub(crate) fn reconcile(
        &self,
        generation: u64,
        configs: &HashMap<String, BackendAdmissionConfig>,
    ) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("AdmissionRegistry state lock poisoned");
        state.prune_drained();

        let desired_ids = configs.keys().cloned().collect::<HashSet<_>>();
        let active_ids = state.active.keys().cloned().collect::<Vec<_>>();
        for backend_id in active_ids {
            let desired = configs
                .get(&backend_id)
                .filter(|config| config.is_available());
            match (state.active.remove(&backend_id), desired) {
                (Some(active), Some(config)) if active.matches(config) => {
                    state.active.insert(backend_id, active);
                }
                (Some(active), Some(config)) => {
                    active.close();
                    if active.is_drained() {
                        state.active.insert(
                            backend_id.clone(),
                            BackendAdmissionController::new(
                                generation,
                                config.clone(),
                                Arc::downgrade(&self.inner),
                            ),
                        );
                    } else {
                        state
                            .draining
                            .entry(backend_id.clone())
                            .or_default()
                            .push(active);
                        state.pending.insert(
                            backend_id,
                            PendingControllerConfig {
                                generation,
                                config: config.clone(),
                            },
                        );
                    }
                }
                (Some(active), None) => {
                    active.close();
                    if !active.is_drained() {
                        state
                            .draining
                            .entry(backend_id.clone())
                            .or_default()
                            .push(active);
                    }
                    state.pending.remove(&backend_id);
                }
                (None, _) => {}
            }
        }

        for (backend_id, config) in configs {
            if !config.is_available() || !desired_ids.contains(backend_id) {
                state.pending.remove(backend_id);
                continue;
            }
            if state.active.contains_key(backend_id) {
                continue;
            }
            if state.has_draining(backend_id) {
                state.pending.insert(
                    backend_id.clone(),
                    PendingControllerConfig {
                        generation,
                        config: config.clone(),
                    },
                );
                continue;
            }
            state.active.insert(
                backend_id.clone(),
                BackendAdmissionController::new(
                    generation,
                    config.clone(),
                    Arc::downgrade(&self.inner),
                ),
            );
        }

        let pending_ids = state.pending.keys().cloned().collect::<Vec<_>>();
        for backend_id in pending_ids {
            state.install_pending_if_ready(&self.inner, &backend_id);
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn acquire_for_test(
        &self,
        request_id: impl Into<String>,
        backend_id: impl Into<String>,
        behavior_id: impl Into<String>,
        agent_did: impl Into<String>,
        call_kind: CallKind,
    ) -> Result<AdmissionPermit, CompletionError> {
        use std::sync::atomic::AtomicU64;
        let context = AdmissionCallContext {
            request_id: request_id.into(),
            backend_id: backend_id.into(),
            behavior_id: behavior_id.into(),
            agent_did: agent_did.into(),
            session_id: "session-test".to_string(),
            call_kind,
            attempt: 1,
            call_seq: Arc::new(AtomicU64::new(0)),
            inference_token: None,
            terminal_failure_reason: None,
        };
        scope_request(context, async { self.acquire_current_call().await }).await
    }

    pub(super) async fn acquire_current_call(&self) -> Result<AdmissionPermit, CompletionError> {
        let context = current_context()?;
        let cancel_observer = context.inference_token.clone();
        let terminal_failure_observer = context.terminal_failure_reason.clone();
        let pending = context.next_call(&self.inner.runtime_instance_id);
        if self.inner.direct_oneoff {
            if pending.call_kind != super::CallKind::OneOff {
                return Err(CompletionError::ProviderError(
                    "direct one-shot admission registry received a non-oneoff call".into(),
                ));
            }
            let call = InferenceCallRecord::without_controller(pending);
            let running = persist_call_started(self.inner.node.clone(), &call).await?;
            let provenance =
                RunningInferenceCallProvenance::new(self.inner.node.clone(), call.clone(), running);
            return Ok(AdmissionPermit::new_direct(
                self.inner.node.clone(),
                call,
                provenance,
                cancel_observer,
                terminal_failure_observer,
            ));
        }
        if pending.backend_id.trim().is_empty() {
            return Err(CompletionError::ProviderError(format!(
                "behavior {} has no backend binding",
                pending.behavior_id
            )));
        }

        let controller = {
            let state = self
                .inner
                .state
                .lock()
                .expect("AdmissionRegistry state lock poisoned");
            state.active.get(&pending.backend_id).cloned()
        };

        match controller {
            Some(controller) => {
                controller
                    .acquire(
                        self.inner.node.clone(),
                        pending,
                        cancel_observer,
                        terminal_failure_observer,
                    )
                    .await
            }
            None => {
                let call = InferenceCallRecord::without_controller(pending);
                if let Err(error) = persist_terminal_call(
                    self.inner.node.clone(),
                    call,
                    "cancelled",
                    Some("BackendGone"),
                    None,
                )
                .await
                {
                    tracing::warn!(error = %error, "failed to persist backend-gone inference call");
                }
                Err(CompletionError::ProviderError(
                    "BackendGone: backend admission controller is not active".into(),
                ))
            }
        }
    }
}

impl AdmissionRegistryInner {
    pub(super) fn controller_drained(self: Arc<Self>, backend_id: String) {
        let mut state = self
            .state
            .lock()
            .expect("AdmissionRegistry state lock poisoned");
        state.install_pending_if_ready(&self, &backend_id);
    }
}

impl RegistryState {
    fn prune_drained(&mut self) {
        self.draining.retain(|_, controllers| {
            controllers.retain(|controller| !controller.is_drained());
            !controllers.is_empty()
        });
    }

    fn has_draining(&mut self, backend_id: &str) -> bool {
        self.prune_drained();
        self.draining
            .get(backend_id)
            .is_some_and(|controllers| !controllers.is_empty())
    }

    fn install_pending_if_ready(
        &mut self,
        registry: &Arc<AdmissionRegistryInner>,
        backend_id: &str,
    ) {
        self.prune_drained();
        if self.active.contains_key(backend_id) || self.has_draining(backend_id) {
            return;
        }
        let Some(pending) = self.pending.remove(backend_id) else {
            return;
        };
        if pending.config.is_available() {
            self.active.insert(
                backend_id.to_string(),
                BackendAdmissionController::new(
                    pending.generation,
                    pending.config,
                    Arc::downgrade(registry),
                ),
            );
        }
    }
}
