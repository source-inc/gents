use std::time::Duration;

use anyhow::{Context, Result};
use gents::UpdateSubscriptionSource;
use gents_codex_protocol as codex;
use tokio::sync::watch;

use super::progress::timestamp_millis;
use super::protocol::{
    send_committed_user_message, send_notification, send_thread_status_changed, timestamp_seconds,
    turn_value_with_timing,
};
use super::subagent_projection::{
    load_authorized_subagent_threads_for_root, LinkedSubagentThread, SubagentProjectionUpdateFilter,
};
use super::thread_projection::CodexThreadRecord;
use super::turn::{stream_gents_turn, TurnStreamOptions};
use super::turn_projection::TurnProjection;
use super::{ConnectionState, ShimState};
use crate::SubmittedRequest;

pub(super) async fn ensure_loaded_subagent_stream(
    connection: &ConnectionState,
    state: &ShimState,
    record: &CodexThreadRecord,
    baseline_turn: Option<codex::Turn>,
) {
    let Some(link) = record.subagent.clone() else {
        return;
    };
    let watcher_id = state.next_id("gents-child-stream");
    let task_connection = connection.clone();
    let task_state = state.clone();
    let task_watcher_id = watcher_id.clone();
    let thread_id = link.session_id.clone();
    let task = tokio::spawn(async move {
        let result =
            watch_loaded_subagent_thread(&task_connection, &task_state, link, baseline_turn).await;
        if let Err(error) = result {
            tracing::warn!(
                %error,
                child_thread_id = thread_id,
                "Codex shim loaded-child projection stopped"
            );
        }
        task_connection
            .clear_child_stream_if_current(&thread_id, &task_watcher_id)
            .await;
    });
    connection
        .replace_child_stream(record.session_id.clone(), watcher_id, task.abort_handle())
        .await;
}

async fn watch_loaded_subagent_thread(
    connection: &ConnectionState,
    state: &ShimState,
    initial_link: LinkedSubagentThread,
    baseline_turn: Option<codex::Turn>,
) -> Result<()> {
    let child_thread_id = initial_link.session_id.clone();
    let root_session_id = initial_link.root_session_id.clone();
    let mut projected_request_id = initial_link.latest_request_id.clone();
    if child_request_is_active(&initial_link.lifecycle_state) {
        let announce_turn = baseline_turn.is_none();
        let options = baseline_turn.map_or_else(
            || TurnStreamOptions::fresh_subagent(root_session_id.clone()),
            |turn| TurnStreamOptions::resumed_subagent(root_session_id.clone(), turn),
        );
        project_child_request(connection, state, &initial_link, options, announce_turn).await?;
    }

    let mut updates = state.node.subscribe_updates();
    let mut updates_closed = false;
    let subagent_update_filter = SubagentProjectionUpdateFilter::from_state(state);
    loop {
        if !state.is_thread_loaded(&child_thread_id).await {
            return Ok(());
        }
        if updates_closed {
            tokio::time::sleep(Duration::from_millis(
                state.poll_interval.as_millis().max(250) as u64,
            ))
            .await;
        } else {
            loop {
                let Some(message) = updates.recv().await else {
                    updates_closed = true;
                    tracing::warn!(
                        child_thread_id,
                        "Codex shim loaded-child update subscription closed; polling"
                    );
                    break;
                };
                let dropped = updates.check_and_reset_dropped();
                if dropped > 0 {
                    tracing::warn!(
                        child_thread_id,
                        dropped,
                        "Codex shim loaded-child update subscription dropped messages"
                    );
                    break;
                }
                if message.as_update().is_some_and(|update| {
                    subagent_update_filter.affects_collection_id(&update.collection_id)
                }) {
                    break;
                }
            }
        }

        let Some(link) = load_authorized_subagent_threads_for_root(state, &root_session_id)
            .await?
            .into_iter()
            .find(|link| link.session_id == child_thread_id)
        else {
            return Ok(());
        };
        if link.latest_request_id == projected_request_id {
            continue;
        }

        projected_request_id = link.latest_request_id.clone();
        project_child_request(
            connection,
            state,
            &link,
            TurnStreamOptions::fresh_subagent(root_session_id.clone()),
            true,
        )
        .await?;
    }
}

async fn project_child_request(
    connection: &ConnectionState,
    state: &ShimState,
    link: &LinkedSubagentThread,
    options: TurnStreamOptions,
    announce_turn: bool,
) -> Result<()> {
    let submitted = SubmittedRequest {
        request_id: link.latest_request_id.clone(),
        session_id: link.session_id.clone(),
        agent_did: link.agent_did.clone(),
        behavior_id: Some(link.behavior_id.clone()),
        temperature: None,
        top_p: None,
        top_k: None,
        seed: None,
        max_tokens: None,
        metadata: None,
        created_at: link.latest_request_created_at.clone(),
    };
    let turn_id = submitted.request_id.clone();
    let started_at = submitted.created_at.as_deref().and_then(timestamp_seconds);
    if announce_turn {
        send_notification(
            &connection.outbound,
            state,
            codex::ServerNotification::TurnStarted(codex::TurnStartedNotification {
                thread_id: link.session_id.clone(),
                turn: turn_value_with_timing(
                    &turn_id,
                    codex::TurnStatus::InProgress,
                    Vec::new(),
                    None,
                    started_at,
                    None,
                ),
            }),
        )
        .await?;
        send_thread_status_changed(
            &connection.outbound,
            state,
            &link.session_id,
            codex::ThreadStatus::Active {
                active_flags: Vec::new(),
            },
        )
        .await?;
        if !link.latest_request_content.trim().is_empty() {
            send_committed_user_message(
                &connection.outbound,
                state,
                &link.session_id,
                &turn_id,
                &[codex::UserInput::Text {
                    text: link.latest_request_content.clone(),
                    text_elements: Vec::new(),
                }],
                submitted.created_at.as_deref().and_then(timestamp_millis),
            )
            .await?;
        }
    }

    let cwd = state.thread_cwd(&link.session_id).await;
    let mut projection = TurnProjection::new(state, &link.session_id, &turn_id, cwd, started_at);
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    stream_gents_turn(
        connection,
        state,
        &submitted,
        &mut projection,
        cancel_rx,
        options,
    )
    .await
    .with_context(|| {
        format!(
            "projecting loaded child thread {} request {}",
            link.session_id, link.latest_request_id
        )
    })
}

fn child_request_is_active(lifecycle_state: &str) -> bool {
    matches!(
        lifecycle_state.trim(),
        "pending" | "claimed" | "processing" | "inputRequired"
    )
}

#[cfg(test)]
mod tests {
    use super::child_request_is_active;

    #[test]
    fn only_nonterminal_child_states_start_resumed_projection() {
        for state in ["pending", "claimed", "processing", "inputRequired"] {
            assert!(child_request_is_active(state), "{state}");
        }
        for state in ["completed", "failed", "dead", "interrupted"] {
            assert!(!child_request_is_active(state), "{state}");
        }
    }
}
