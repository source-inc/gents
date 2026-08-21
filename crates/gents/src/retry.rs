//! Inference retry with exponential backoff and jitter.
//!
//! Wraps the streaming inference call with configurable retry behavior.
//! Only retries when the error is classified as transient (connection
//! failures, rate limits, timeouts) — permanent errors (auth, context
//! length) fail immediately.

use std::time::Duration;

use defra_node::{EmbeddedNode, QueryResponse};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::error::InferenceError;

pub use crate::graphql::{
    defradb_conflict_retry_backoff, is_defradb_transaction_conflict_text,
    DEFRA_DB_CONFLICT_INITIAL_BACKOFF_MS, DEFRA_DB_CONFLICT_MAX_RETRIES,
};
pub const TERMINAL_PERSISTENCE_MAX_RETRIES: u32 = 3;
pub const TERMINAL_PERSISTENCE_INITIAL_BACKOFF_MS: u64 = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 1000,
            max_delay_ms: 30_000,
        }
    }
}

impl RetryPolicy {
    /// Compute delay for a given attempt (0-indexed) with exponential
    /// backoff and +/-25% jitter.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base = self.base_delay_ms;
        let exponential = base.saturating_mul(1u64 << attempt.min(10));
        let capped = exponential.min(self.max_delay_ms);

        let jitter_range = capped / 4;
        let jitter = if jitter_range > 0 {
            let mut rng = rand::rng();
            rng.random_range(0..=jitter_range * 2) as i64 - jitter_range as i64
        } else {
            0
        };

        let final_ms = (capped as i64 + jitter).max(100) as u64;
        Duration::from_millis(final_ms)
    }

    pub fn has_retries(&self) -> bool {
        self.max_retries > 0
    }
}

pub fn is_retryable_streaming_error(error: &rig::agent::StreamingError) -> bool {
    let classified = crate::error::classify_completion_error(error);
    classified.is_retryable()
}

/// Retry a terminal persistence operation on every storage error, not only a
/// recognized transaction-conflict string. Terminal request/response writes
/// are idempotent and guarded by source state, so retrying an ambiguous or
/// transient local-storage failure is safe. The bound prevents one request
/// from monopolizing its behavior executor; after exhaustion the caller gets
/// the storage error and the transaction has no partially committed projection.
pub(crate) async fn retry_terminal_persistence_operation<T, F, Fut>(
    operation: &str,
    max_retries: u32,
    initial_backoff: Duration,
    mut attempt_operation: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut retry_index = 0;
    loop {
        match attempt_operation().await {
            Ok(value) => return Ok(value),
            Err(error) if retry_index < max_retries => {
                let backoff = initial_backoff.saturating_mul(1u32 << retry_index.min(10));
                tracing::warn!(
                    operation,
                    attempt = retry_index + 1,
                    max_retries,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %error,
                    "retrying terminal persistence after storage failure"
                );
                tokio::time::sleep(backoff).await;
                retry_index += 1;
            }
            Err(error) => {
                tracing::error!(
                    operation,
                    attempts = retry_index + 1,
                    max_retries,
                    error = %error,
                    "terminal persistence retries exhausted"
                );
                return Err(error);
            }
        }
    }
}

/// Execute one initial terminal mutation plus at most
/// [`TERMINAL_PERSISTENCE_MAX_RETRIES`] retries. Each attempt enters the shared
/// node mutation gate once; terminal backoff happens after that guard is gone.
pub(crate) async fn execute_graphql_with_terminal_persistence_retry(
    node: &EmbeddedNode,
    graphql: &str,
    operation: &str,
) -> anyhow::Result<QueryResponse> {
    execute_graphql_with_terminal_persistence_retry_using(node, node, graphql, operation).await
}

async fn execute_graphql_with_terminal_persistence_retry_using<E>(
    node: &EmbeddedNode,
    executor: &E,
    graphql: &str,
    operation: &str,
) -> anyhow::Result<QueryResponse>
where
    E: crate::graphql::GraphqlExecution + ?Sized,
{
    retry_terminal_persistence_operation(
        operation,
        TERMINAL_PERSISTENCE_MAX_RETRIES,
        Duration::from_millis(TERMINAL_PERSISTENCE_INITIAL_BACKOFF_MS),
        || async {
            crate::graphql::graphql_mutation_once_with_executor(node, executor, graphql, operation)
                .await
        },
    )
    .await
}

pub fn retries_exhausted(policy: &RetryPolicy, last_error: &InferenceError) -> InferenceError {
    InferenceError::RetriesExhausted {
        max_retries: policy.max_retries,
        last_error: last_error.to_string(),
    }
}

#[cfg(test)]
mod tests;
