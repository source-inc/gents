//! Manual-fire `TriggerSource`.

use std::future::Future;
use std::pin::Pin;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{FireIntent, FireResult, TriggerKind, TriggerSource};
use crate::runtime_snapshot::{ActiveRuntimeSnapshot, ConcurrencyMode};

const MANUAL_CHANNEL_CAPACITY: usize = 32;

pub(crate) struct ManualSource {
    rx: mpsc::Receiver<FireIntent>,
    cancel: CancellationToken,
}

#[derive(Clone)]
pub(crate) struct ManualTriggerHandle {
    tx: mpsc::Sender<FireIntent>,
}

impl ManualSource {
    pub(crate) fn new(cancel: CancellationToken) -> (Self, ManualTriggerHandle) {
        let (tx, rx) = mpsc::channel(MANUAL_CHANNEL_CAPACITY);
        (Self { rx, cancel }, ManualTriggerHandle { tx })
    }
}

impl ManualTriggerHandle {
    #[allow(dead_code)]
    pub(crate) async fn run_task_now(
        &self,
        snapshot: &ActiveRuntimeSnapshot,
        task_id: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<oneshot::Receiver<FireResult>> {
        let resolved_task = snapshot
            .active_tasks()
            .get(task_id)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "task {task_id} is not in the active snapshot (check the task exists, is enabled, and its behavior is available)"
                )
            })?;

        let (result_tx, result_rx) = oneshot::channel();
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        let intent = FireIntent {
            trigger_id: None,
            trigger_kind: TriggerKind::Manual,
            task: resolved_task,
            concurrency: ConcurrencyMode::Parallel,
            event_vars: serde_json::json!({
                "fired_at": now,
                "trigger_id": serde_json::Value::Null,
                "trigger_kind": "manual",
            }),
            doc_vars: None,
            args_vars: Some(args),
            pre_materialized_request_id: None,
            materialization_request_id: None,
            on_result: Box::new(move |result| {
                let _ = result_tx.send(result);
            }),
        };

        self.tx.send(intent).await.map_err(|_| {
            anyhow::anyhow!("manual trigger channel is closed; engine has shut down")
        })?;
        Ok(result_rx)
    }
}

impl TriggerSource for ManualSource {
    fn next_fire(&mut self) -> Pin<Box<dyn Future<Output = Option<FireIntent>> + Send + '_>> {
        Box::pin(async move {
            tokio::select! {
                _ = self.cancel.cancelled() => None,
                intent = self.rx.recv() => intent,
            }
        })
    }
}
