use std::sync::{Arc, Mutex};

use defra_node::EmbeddedNode;
use rig::completion::{CompletionError, Usage};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

use super::controller::{BackendAdmissionController, InferenceCallRecord};
use super::persistence::{persist_existing_call_terminal_with_render, spawn_persistence};
use super::provenance::RunningInferenceCallProvenance;
use super::stream_guard::StreamGuardLifecycle;

pub(crate) struct AdmissionPermit {
    node: Arc<EmbeddedNode>,
    controller: Option<Arc<BackendAdmissionController>>,
    permit: Option<OwnedSemaphorePermit>,
    call: InferenceCallRecord,
    doc_id: String,
    provenance: RunningInferenceCallProvenance,
    terminal: Option<PermitTerminal>,
    finished: bool,
    cancel_observer: Option<CancellationToken>,
    terminal_failure_observer: Option<Arc<Mutex<Option<String>>>>,
}

#[derive(Clone, Debug)]
struct PermitTerminal {
    call_state: &'static str,
    failure_reason: Option<String>,
    usage: Option<Usage>,
}

impl AdmissionPermit {
    pub(super) fn new(
        node: Arc<EmbeddedNode>,
        controller: Arc<BackendAdmissionController>,
        permit: OwnedSemaphorePermit,
        call: InferenceCallRecord,
        provenance: RunningInferenceCallProvenance,
        cancel_observer: Option<CancellationToken>,
        terminal_failure_observer: Option<Arc<Mutex<Option<String>>>>,
    ) -> Self {
        Self {
            node,
            controller: Some(controller),
            permit: Some(permit),
            call,
            doc_id: provenance.call_version().version.doc_id.clone(),
            provenance,
            terminal: None,
            finished: false,
            cancel_observer,
            terminal_failure_observer,
        }
    }

    pub(super) fn new_direct(
        node: Arc<EmbeddedNode>,
        call: InferenceCallRecord,
        provenance: RunningInferenceCallProvenance,
        cancel_observer: Option<CancellationToken>,
        terminal_failure_observer: Option<Arc<Mutex<Option<String>>>>,
    ) -> Self {
        Self {
            node,
            controller: None,
            permit: None,
            call,
            doc_id: provenance.call_version().version.doc_id.clone(),
            provenance,
            terminal: None,
            finished: false,
            cancel_observer,
            terminal_failure_observer,
        }
    }

    pub(crate) fn running_call_provenance(&self) -> &RunningInferenceCallProvenance {
        &self.provenance
    }

    /// A provider future may report success only after the render sink has
    /// durably established the V1 -> R -> V2 pre-send fence.
    pub(crate) async fn require_rendered_request_binding(&mut self) -> Result<(), CompletionError> {
        if self.provenance.rendered_request().is_some() {
            return Ok(());
        }
        let reason = "InferenceCallRenderBindingMissing: admitted provider path returned without an exact RenderedRequest binding";
        self.finish_failure(reason).await;
        Err(CompletionError::ProviderError(reason.to_string()))
    }

    pub(crate) async fn finish_success(&mut self, usage: Option<Usage>) {
        self.terminal = Some(PermitTerminal {
            call_state: "completed",
            failure_reason: None,
            usage,
        });
        self.finish().await;
    }

    pub(crate) async fn finish_failure(&mut self, reason: &str) {
        self.terminal = Some(PermitTerminal {
            call_state: "failed",
            failure_reason: Some(reason.to_string()),
            usage: None,
        });
        self.finish().await;
    }

    /// Mark this permit as cancelled due to user-initiated interrupt.
    /// On `finish()` or `Drop`, the controller persists the InferenceCall
    /// with `call_state = "cancelled"` and `failure_reason = "Cancelled"`.
    /// Idempotent with the existing `finished` guard — callers should not
    /// invoke `finish_*` after `mark_interrupted` and instead rely on the
    /// Drop path (or explicit `finish_success`/`finish_failure`) to persist.
    pub(crate) fn mark_interrupted(&mut self) {
        if self.finished {
            return;
        }
        self.terminal = Some(PermitTerminal {
            call_state: "cancelled",
            failure_reason: Some("Cancelled".to_string()),
            usage: None,
        });
    }

    async fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let mut terminal = self.terminal.clone().unwrap_or(PermitTerminal {
            call_state: "completed",
            failure_reason: None,
            usage: None,
        });
        if terminal.call_state == "completed"
            && self.provenance.provider_path_entered()
            && self.provenance.rendered_request().is_none()
        {
            terminal = PermitTerminal {
                call_state: "failed",
                failure_reason: Some(
                    "InferenceCallRenderBindingMissing: admitted provider path completed without an exact RenderedRequest binding"
                        .to_string(),
                ),
                usage: None,
            };
        }
        if let Err(error) = persist_existing_call_terminal_with_render(
            self.node.clone(),
            &self.doc_id,
            &self.call,
            "running",
            terminal.call_state,
            terminal.failure_reason.as_deref(),
            terminal.usage,
            self.provenance
                .rendered_request()
                .map(|_| self.provenance.call_version()),
            self.provenance.rendered_request(),
        )
        .await
        {
            tracing::warn!(call_id = %self.call.call_id, error = %error, "failed to persist terminal inference call state");
        }
    }
}

impl StreamGuardLifecycle for AdmissionPermit {
    fn cancel_before_poll(&mut self) -> bool {
        if self
            .cancel_observer
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            self.mark_interrupted();
            true
        } else {
            false
        }
    }

    fn mark_stream_success(&mut self, usage: Option<Usage>) {
        if self.terminal.is_none() {
            self.terminal = Some(PermitTerminal {
                call_state: "completed",
                failure_reason: None,
                usage,
            });
        }
    }

    fn mark_stream_error(&mut self, error: &CompletionError) {
        self.terminal = Some(PermitTerminal {
            call_state: "failed",
            failure_reason: Some(error.to_string()),
            usage: None,
        });
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        // Return the semaphore permit before the in-flight release: the
        // release can synchronously install a replacement controller, and a
        // drained controller must hold no outstanding permits (#1001; Lean
        // `InferenceCall.ControllerBookkeeping.drained_no_outstanding_permits`).
        // Field drop runs only after this body — including the observer lock
        // below — so the permit must be taken explicitly here.
        drop(self.permit.take());
        if let Some(controller) = &self.controller {
            controller.release_in_flight();
        }
        if self.finished {
            return;
        }
        self.finished = true;
        let terminal_failure_reason =
            self.terminal_failure_observer
                .as_ref()
                .and_then(|observer| match observer.lock() {
                    Ok(reason) => reason.clone(),
                    Err(poisoned) => poisoned.into_inner().clone(),
                });
        let mut terminal = self.terminal.clone().unwrap_or_else(|| {
            if self
                .cancel_observer
                .as_ref()
                .is_some_and(|t| t.is_cancelled())
            {
                PermitTerminal {
                    call_state: "cancelled",
                    failure_reason: Some("Cancelled".to_string()),
                    usage: None,
                }
            } else if let Some(reason) = terminal_failure_reason {
                PermitTerminal {
                    call_state: "failed",
                    failure_reason: Some(reason),
                    usage: None,
                }
            } else {
                PermitTerminal {
                    call_state: "failed",
                    failure_reason: Some("StreamDroppedBeforeTerminalResponse".to_string()),
                    usage: None,
                }
            }
        });
        if terminal.call_state == "completed"
            && self.provenance.provider_path_entered()
            && self.provenance.rendered_request().is_none()
        {
            terminal = PermitTerminal {
                call_state: "failed",
                failure_reason: Some(
                    "InferenceCallRenderBindingMissing: admitted provider path completed without an exact RenderedRequest binding"
                        .to_string(),
                ),
                usage: None,
            };
        }
        let node = self.node.clone();
        let doc_id = self.doc_id.clone();
        let call_id = self.call.call_id.clone();
        let call = self.call.clone();
        let running_call = self.provenance.call_version().clone();
        let rendered_request = self.provenance.rendered_request().cloned();
        spawn_persistence(async move {
            if let Err(error) = persist_existing_call_terminal_with_render(
                node,
                &doc_id,
                &call,
                "running",
                terminal.call_state,
                terminal.failure_reason.as_deref(),
                terminal.usage,
                rendered_request.as_ref().map(|_| &running_call),
                rendered_request.as_ref(),
            )
            .await
            {
                tracing::warn!(call_id = %call_id, error = %error, "failed to persist dropped inference call state");
            }
        });
    }
}
