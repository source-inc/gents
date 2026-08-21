use crate::error::BridgeError;
use tauri::State;

use crate::commands::{rename_conversation, send_chat_message};
use crate::snapshot::build_session_snapshot_from_store_for_agent;
use crate::state::{current_core, DesktopAppState};
use crate::types::{
    ChatSendRequest, ChatSendResult, ConversationRenameRequest, DesktopSessionSnapshot,
};

#[tauri::command]
pub async fn desktop_session_snapshot(
    session_id: String,
    agent_did: Option<String>,
    request_id: Option<String>,
    state: State<'_, DesktopAppState>,
) -> Result<Option<DesktopSessionSnapshot>, BridgeError> {
    let Some(core) = current_core(&state) else {
        return Ok(None);
    };

    if let (Some(agent_did), Some(request_id)) = (agent_did.as_deref(), request_id.as_deref()) {
        if let Err(error) = core.refresh_local_request(agent_did, request_id).await {
            tracing::warn!(
                target: "gents_desktop::chat",
                agent_did,
                request_id,
                error = %error,
                "selected local request refresh failed; returning the last observed session"
            );
        }
    }
    let snapshot = core.store().snapshot();
    Ok(build_session_snapshot_from_store_for_agent(
        snapshot.as_ref(),
        agent_did.as_deref(),
        &session_id,
        request_id.as_deref(),
    ))
}

#[tauri::command]
pub async fn desktop_chat_send(
    request: ChatSendRequest,
    state: State<'_, DesktopAppState>,
) -> Result<ChatSendResult, BridgeError> {
    let Some(core) = current_core(&state) else {
        return Err(BridgeError::from_legacy_message(
            "desktop client is not running",
        ));
    };

    send_chat_message(core.as_ref(), request)
        .await
        .map_err(|error| BridgeError::from_legacy_message(error.to_string()))
}

#[tauri::command]
pub async fn desktop_conversation_rename(
    request: ConversationRenameRequest,
    state: State<'_, DesktopAppState>,
) -> Result<(), BridgeError> {
    let Some(core) = current_core(&state) else {
        return Err(BridgeError::from_legacy_message(
            "desktop client is not running",
        ));
    };

    rename_conversation(core.as_ref(), request)
        .await
        .map_err(|error| BridgeError::from_legacy_message(error.to_string()))
}

#[derive(Debug, Clone, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct RequestResendResultView {
    pub request_id: String,
    pub session_id: String,
}

#[tauri::command]
pub async fn desktop_request_resend(
    request_id: String,
    state: State<'_, DesktopAppState>,
) -> Result<RequestResendResultView, BridgeError> {
    let Some(core) = current_core(&state) else {
        return Err(BridgeError::from_legacy_message(
            "desktop client is not running",
        ));
    };

    let submitted = core
        .resend_request(&request_id)
        .await
        .map_err(|error| BridgeError::from_legacy_message(error.to_string()))?;
    Ok(RequestResendResultView {
        request_id: submitted.request_id,
        session_id: submitted.session_id,
    })
}

#[tauri::command]
pub async fn desktop_request_retry(
    request_id: String,
    state: State<'_, DesktopAppState>,
) -> Result<ChatSendResult, BridgeError> {
    let Some(core) = current_core(&state) else {
        return Err(BridgeError::from_legacy_message(
            "desktop client is not running",
        ));
    };

    let parent = core
        .store()
        .snapshot()
        .requests
        .iter()
        .find(|request| request.request_id == request_id)
        .cloned()
        .ok_or_else(|| {
            BridgeError::from_legacy_message(format!(
                "retry parent request not found: request_id={request_id}"
            ))
        })?;
    let submitted = core
        .retry_request(&parent)
        .await
        .map_err(|error| BridgeError::from_legacy_message(error.to_string()))?;
    Ok(ChatSendResult {
        session_id: submitted.session_id,
        request_id: submitted.request_id,
        agent_did: submitted.agent_did,
        behavior_id: submitted.behavior_id,
    })
}

#[tauri::command]
pub async fn desktop_request_timeline(
    agent_did: String,
    request_id: String,
    state: State<'_, DesktopAppState>,
) -> Result<serde_json::Value, BridgeError> {
    let Some(core) = current_core(&state) else {
        return Err(BridgeError::from_legacy_message(
            "desktop client is not running",
        ));
    };

    let timeline = core
        .request_timeline(&agent_did, &request_id)
        .await
        .map_err(|error| BridgeError::from_legacy_message(error.to_string()))?;
    serde_json::to_value(&timeline)
        .map_err(|error| BridgeError::from_legacy_message(error.to_string()))
}
