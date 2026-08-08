use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use defra_node::EmbeddedNode;

use super::controller::InferenceCallRecord;

/// Cloneable pre-send provenance handle shared by admission and the rendered
/// request transport.
///
/// `call` is the exact signed running V1. The render sink persists R, then
/// `bind_rendered_request` performs and verifies the V1 -> V2 reverse-link
/// write before publishing R through this handle. HTTP forwarding is legal
/// only after that method succeeds.
#[derive(Clone)]
pub(crate) struct RunningInferenceCallProvenance {
    node: Arc<EmbeddedNode>,
    record: InferenceCallRecord,
    call: crate::SignedDocumentVersionRef,
    rendered_request: Arc<OnceLock<crate::SignedDocumentVersionRef>>,
    bind_lock: Arc<tokio::sync::Mutex<()>>,
    provider_path_entered: Arc<AtomicBool>,
}

impl RunningInferenceCallProvenance {
    pub(super) fn new(
        node: Arc<EmbeddedNode>,
        record: InferenceCallRecord,
        call: crate::SignedDocumentVersionRef,
    ) -> Self {
        Self {
            node,
            record,
            call,
            rendered_request: Arc::new(OnceLock::new()),
            bind_lock: Arc::new(tokio::sync::Mutex::new(())),
            provider_path_entered: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn call_version(&self) -> &crate::SignedDocumentVersionRef {
        &self.call
    }

    pub(crate) async fn bind_rendered_request(
        &self,
        rendered_request: crate::SignedDocumentVersionRef,
    ) -> Result<()> {
        let _bind_guard = self.bind_lock.lock().await;
        if let Some(bound) = self.rendered_request.get() {
            if bound == &rendered_request {
                return Ok(());
            }
            anyhow::bail!(
                "InferenceCall {} is already bound to RenderedRequest {}@{}, refusing {}@{}",
                self.record.call_id,
                bound.version.doc_id,
                bound.version.composite_commit_cid,
                rendered_request.version.doc_id,
                rendered_request.version.composite_commit_cid
            );
        }

        super::persistence::persist_rendered_request_binding(
            self.node.clone(),
            &self.record,
            &self.call,
            &rendered_request,
        )
        .await?;

        match self.rendered_request.set(rendered_request.clone()) {
            Ok(()) => Ok(()),
            Err(attempted) if self.rendered_request.get() == Some(&attempted) => Ok(()),
            Err(attempted) => {
                let bound = self
                    .rendered_request
                    .get()
                    .expect("OnceLock set failure requires an initialized value");
                anyhow::bail!(
                    "InferenceCall {} concurrent render binding conflict: durable {}@{}, in-memory {}@{}",
                    self.record.call_id,
                    attempted.version.doc_id,
                    attempted.version.composite_commit_cid,
                    bound.version.doc_id,
                    bound.version.composite_commit_cid
                )
            }
        }
    }

    pub(super) fn rendered_request(&self) -> Option<&crate::SignedDocumentVersionRef> {
        self.rendered_request.get()
    }

    pub(super) fn mark_provider_path_entered(&self) {
        self.provider_path_entered.store(true, Ordering::SeqCst);
    }

    pub(super) fn provider_path_entered(&self) -> bool {
        self.provider_path_entered.load(Ordering::SeqCst)
    }
}
