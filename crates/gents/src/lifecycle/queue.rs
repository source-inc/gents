#![allow(dead_code)]

use anyhow::{Context, Result};
use defra_node::EmbeddedNode;
use serde::Deserialize;
use serde_json::Value;

use crate::config_client::ConfigApplyTxn;
use crate::graphql::{
    defradb_conflict_retry_backoff, escape_graphql_string, is_defradb_transaction_conflict_text,
    response_has_documents, DEFRA_DB_CONFLICT_MAX_RETRIES,
};
use crate::session;
use crate::watcher::AgentRequest;

use super::materialize::EnqueuedAgentRequest;
use super::{extract_single_doc_id, ExecutionOrigin, DEFAULT_REQUEST_MAX_RETRIES};

mod atomic_inputs;
mod coalescing;
mod draining;
mod enqueue;
mod goal_continuation;
mod metadata;
mod mutation;

pub(crate) use atomic_inputs::enqueue_background_completion_with_message;
#[cfg(test)]
use atomic_inputs::transaction_created_doc_id;
use atomic_inputs::{
    normalize_request_only_control_parent, steering_transaction_attempt,
    steering_transaction_error_is_retryable,
};
pub use coalescing::reconcile_coalesced_pending_request;
use coalescing::{
    coalesce_key, lookup_request_doc_id, lookup_request_doc_id_optional, parent_behavior_id,
    parent_linkage_graphql_fields, queue_row_to_enqueued_request, queue_source_and_key_match,
    request_only_parent_linkage_graphql_fields, PendingQueueRow,
};
pub use draining::drain_automated_wakeups;
pub(crate) use draining::drain_subagent_owned_queue;
pub(crate) use enqueue::{enqueue_session_request, enqueue_steering_request_with_message};
pub(crate) use goal_continuation::enqueue_goal_continuation;
pub use metadata::QueueSource;
pub(crate) use metadata::{
    is_automated_wakeup, is_deprecated_background_completion_wakeup, is_goal_queue,
    is_steering_input_message_key, is_subagent_owned_queue, parse_queue_hints, queue_metadata_json,
    steering_input_message_key, QueueHints, QueuePolicy,
};
use mutation::session_request_create_mutation;

pub(crate) struct EnqueuedBackgroundCompletionInput {
    pub(crate) request: EnqueuedAgentRequest,
    pub(crate) message_sequence: u32,
    pub(crate) created_request: bool,
}

#[cfg(test)]
mod tests;
