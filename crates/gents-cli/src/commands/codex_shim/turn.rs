mod active;
mod stream;
mod submission;

use anyhow::Result;
use gents_codex_protocol as codex;
use tokio::sync::watch;

pub(super) use active::interrupt_active_turn;

use active::{cancel_abandoned_steering_request, load_active_codex_turn};
pub(super) use active::{codex_turn_id_for_request, install_stream_control};
pub(super) use stream::{stream_gents_turn, TurnStreamOptions};
use submission::create_agent_request_with_retry;

use super::progress::timestamp_millis;
use super::protocol::{
    codex_steering_metadata, codex_turn_metadata, selected_skill_ids_from_input,
    send_committed_user_message, send_error, send_notification, send_result,
    send_thread_status_changed, timestamp_seconds, turn_value_with_timing, user_text_from_input,
};
use super::thread_projection::load_codex_thread;
use super::turn_projection::TurnProjection;
use super::{
    ConnectionState, ShimState, JSONRPC_INTERNAL_ERROR, JSONRPC_INVALID_PARAMS,
    JSONRPC_INVALID_REQUEST,
};
use crate::RequestSubmitOptions;

pub(super) async fn start_gents_turn(
    connection: &ConnectionState,
    state: &ShimState,
    request_id: codex::RequestId,
    thread_id: String,
    input: Vec<codex::UserInput>,
) -> Result<()> {
    if load_codex_thread(state, &thread_id)
        .await?
        .is_some_and(|record| record.is_subagent())
    {
        return send_error(
            &connection.outbound,
            request_id,
            JSONRPC_INVALID_PARAMS,
            "linked GENTS subagent threads are read-only; steer them from the parent thread"
                .to_string(),
        )
        .await;
    }
    let user_text = user_text_from_input(&input);
    let selected_skill_ids = selected_skill_ids_from_input(&input);
    if user_text.trim().is_empty() && selected_skill_ids.is_empty() {
        return send_error(
            &connection.outbound,
            request_id,
            JSONRPC_INVALID_REQUEST,
            "Codex turn input did not contain text for GENTS".to_string(),
        )
        .await;
    }

    let cwd = state.thread_cwd(&thread_id).await;
    let thread_name = state.thread_name(&thread_id).await;
    let metadata = codex_turn_metadata(&cwd, &selected_skill_ids, Some(&thread_name));

    let submitted = match create_agent_request_with_retry(
        state,
        &user_text,
        Some(&thread_id),
        RequestSubmitOptions {
            metadata: Some(metadata),
            ..RequestSubmitOptions::default()
        },
    )
    .await
    {
        Ok(submitted) => submitted,
        Err(err) => {
            return send_error(
                &connection.outbound,
                request_id,
                JSONRPC_INTERNAL_ERROR,
                format!("failed to submit GENTS AgentRequest: {err}"),
            )
            .await;
        }
    };

    let turn_id = submitted.request_id.clone();
    let started_at = submitted.created_at.as_deref().and_then(timestamp_seconds);
    let started_turn = turn_value_with_timing(
        &turn_id,
        codex::TurnStatus::InProgress,
        Vec::new(),
        None,
        started_at,
        None,
    );
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let stream_registration = install_stream_control(
        connection,
        thread_id.clone(),
        turn_id.clone(),
        None,
        cancel_tx,
    )
    .await;

    if let Err(err) = send_result(
        &connection.outbound,
        request_id,
        codex::TurnStartResponse {
            turn: started_turn.clone(),
        },
    )
    .await
    {
        stream_registration.clear().await;
        return Err(err);
    }

    send_notification(
        &connection.outbound,
        state,
        codex::ServerNotification::TurnStarted(codex::TurnStartedNotification {
            thread_id: thread_id.clone(),
            turn: started_turn,
        }),
    )
    .await?;
    send_thread_status_changed(
        &connection.outbound,
        state,
        &thread_id,
        codex::ThreadStatus::Active {
            active_flags: Vec::new(),
        },
    )
    .await?;

    send_committed_user_message(
        &connection.outbound,
        state,
        &thread_id,
        &turn_id,
        &input,
        submitted.created_at.as_deref().and_then(timestamp_millis),
    )
    .await?;

    let mut projection = TurnProjection::new(state, &thread_id, &turn_id, cwd.clone(), started_at);
    let result = match stream_gents_turn(
        connection,
        state,
        &submitted,
        &mut projection,
        cancel_rx,
        TurnStreamOptions::fresh(thread_id.clone()),
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(err) => {
            let message = format!("GENTS turn failed: {err}");
            projection
                .append_agent_delta(&connection.outbound, &format!("[agent error] {message}\n"))
                .await?;
            projection
                .finish_turn(
                    &connection.outbound,
                    codex::TurnStatus::Failed,
                    Some(message),
                )
                .await?;
            send_thread_status_changed(
                &connection.outbound,
                state,
                &thread_id,
                codex::ThreadStatus::SystemError,
            )
            .await
        }
    };

    stream_registration.clear().await;
    result
}

pub(super) async fn steer_gents_turn(
    connection: &ConnectionState,
    state: &ShimState,
    request_id: codex::RequestId,
    params: codex::TurnSteerParams,
) -> Result<()> {
    if load_codex_thread(state, &params.thread_id)
        .await?
        .is_some_and(|record| record.is_subagent())
    {
        return send_error(
            &connection.outbound,
            request_id,
            JSONRPC_INVALID_PARAMS,
            "linked GENTS subagent threads are read-only; steer them from the parent thread"
                .to_string(),
        )
        .await;
    }
    if params.expected_turn_id.trim().is_empty() {
        return send_error(
            &connection.outbound,
            request_id,
            JSONRPC_INVALID_REQUEST,
            "expectedTurnId must not be empty".to_string(),
        )
        .await;
    }

    let user_text = user_text_from_input(&params.input);
    let selected_skill_ids = selected_skill_ids_from_input(&params.input);
    if user_text.trim().is_empty() && selected_skill_ids.is_empty() {
        return send_error(
            &connection.outbound,
            request_id,
            JSONRPC_INVALID_REQUEST,
            "input must not be empty".to_string(),
        )
        .await;
    }

    let cwd = state.thread_cwd(&params.thread_id).await;

    let Some(active_turn) = load_active_codex_turn(state, &params.thread_id).await? else {
        return send_error(
            &connection.outbound,
            request_id,
            JSONRPC_INVALID_PARAMS,
            "no active turn to steer".to_string(),
        )
        .await;
    };
    if active_turn.turn_id != params.expected_turn_id {
        return send_error(
            &connection.outbound,
            request_id,
            JSONRPC_INVALID_PARAMS,
            format!(
                "expected active turn id `{}` but found `{}`",
                params.expected_turn_id, active_turn.turn_id
            ),
        )
        .await;
    }

    let turn_id = active_turn.turn_id.clone();
    let queued_after_request_id = active_turn.current_request_id.clone();
    let metadata = codex_steering_metadata(&cwd, &queued_after_request_id, &selected_skill_ids);
    let submitted = match create_agent_request_with_retry(
        state,
        &user_text,
        Some(&params.thread_id),
        RequestSubmitOptions {
            metadata: Some(metadata),
            ..RequestSubmitOptions::default()
        },
    )
    .await
    {
        Ok(submitted) => submitted,
        Err(err) => {
            return send_error(
                &connection.outbound,
                request_id,
                JSONRPC_INTERNAL_ERROR,
                format!("failed to submit GENTS steering AgentRequest: {err}"),
            )
            .await;
        }
    };

    let Some(current_active) = load_active_codex_turn(state, &params.thread_id).await? else {
        cancel_abandoned_steering_request(state, submitted.request_id.clone());
        return send_error(
            &connection.outbound,
            request_id,
            JSONRPC_INVALID_PARAMS,
            "active turn ended while submitting steering request".to_string(),
        )
        .await;
    };
    if current_active.turn_id != turn_id {
        let current_turn_id = current_active.turn_id.clone();
        cancel_abandoned_steering_request(state, submitted.request_id.clone());
        return send_error(
            &connection.outbound,
            request_id,
            JSONRPC_INVALID_PARAMS,
            format!("active turn changed from `{turn_id}` to `{current_turn_id}`"),
        )
        .await;
    }

    connection
        .remember_steering_input(submitted.request_id.clone(), params.input.clone())
        .await;
    send_result(
        &connection.outbound,
        request_id,
        codex::TurnSteerResponse {
            turn_id: turn_id.clone(),
        },
    )
    .await?;
    tracing::info!(
        turn_id,
        queued_after_request_id,
        steering_request_id = %submitted.request_id,
        "Codex shim accepted active-turn steering request"
    );
    Ok(())
}
