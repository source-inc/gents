use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use gents::InferenceBackend;
use gents_codex_protocol as codex;
use gents_codex_protocol::MessagePhase;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};

use super::{trace, Outbound, ShimState};

pub(super) fn client_request_from_jsonrpc(
    request: codex::JSONRPCRequest,
) -> std::result::Result<codex::ClientRequest, serde_json::Error> {
    serde_json::from_value(serde_json::to_value(request)?)
}

pub(super) async fn send_typed_json_result<T>(
    outbound: &Outbound,
    id: codex::RequestId,
    value: Value,
) -> Result<()>
where
    T: DeserializeOwned + Serialize,
{
    let response = serde_json::from_value::<T>(value)
        .with_context(|| format!("validating Codex response {}", std::any::type_name::<T>()))?;
    send_result(outbound, id, response).await
}

pub(super) async fn send_result<T>(
    outbound: &Outbound,
    id: codex::RequestId,
    response: T,
) -> Result<()>
where
    T: Serialize,
{
    let result = serde_json::to_value(response).context("serializing Codex response payload")?;
    send_json(outbound, &codex::JSONRPCResponse { id, result }).await
}

pub(super) async fn send_error(
    outbound: &Outbound,
    id: codex::RequestId,
    code: i64,
    message: String,
) -> Result<()> {
    send_json(
        outbound,
        &codex::JSONRPCError {
            id,
            error: codex::JSONRPCErrorError {
                code,
                data: None,
                message,
            },
        },
    )
    .await
}

pub(super) async fn send_notification(
    outbound: &Outbound,
    state: &ShimState,
    notification: codex::ServerNotification,
) -> Result<()> {
    trace::codex_notification(&state.trace_path, &notification);
    send_json(outbound, &notification).await
}

pub(super) async fn send_thread_status_changed(
    outbound: &Outbound,
    state: &ShimState,
    thread_id: &str,
    status: codex::ThreadStatus,
) -> Result<()> {
    send_notification(
        outbound,
        state,
        codex::ServerNotification::ThreadStatusChanged(codex::ThreadStatusChangedNotification {
            thread_id: thread_id.to_string(),
            status,
        }),
    )
    .await
}

async fn send_json<T>(outbound: &Outbound, value: &T) -> Result<()>
where
    T: Serialize,
{
    let text = serde_json::to_string(value).context("serializing Codex shim WebSocket message")?;
    outbound
        .send(text)
        .map_err(|_| anyhow::anyhow!("Codex shim WebSocket writer closed"))
}

pub(super) fn initialize_result(state: &ShimState) -> Value {
    json!({
        "userAgent": concat!("gents-codex-shim/", env!("CARGO_PKG_VERSION")),
        "codexHome": absolute_path(&state.codex_home),
        "platformFamily": std::env::consts::FAMILY,
        "platformOs": std::env::consts::OS
    })
}

pub(super) fn backend_model_summary(
    backend: &InferenceBackend,
    model_name: &str,
    selection_id: &str,
    is_default: bool,
) -> Value {
    let backend_name = backend.name.trim();
    let backend_label = if backend_name.is_empty() {
        backend.backend_id.as_str()
    } else {
        backend_name
    };
    json!({
        "id": selection_id,
        "model": selection_id,
        "upgrade": null,
        "upgradeInfo": null,
        "availabilityNux": null,
        "displayName": model_name,
        "description": format!("GENTS backend: {backend_label}"),
        "hidden": false,
        "supportedReasoningEfforts": [],
        "defaultReasoningEffort": "medium",
        "inputModalities": ["text"],
        "supportsPersonality": false,
        "additionalSpeedTiers": [],
        "serviceTiers": [],
        "defaultServiceTier": null,
        "isDefault": is_default,
    })
}

pub(super) fn thread_json(
    cwd: &Path,
    thread_id: &str,
    preview: Option<&str>,
    status: codex::ThreadStatus,
    turns: Vec<codex::Turn>,
) -> Value {
    let now = now_seconds();
    json!({
        "id": thread_id,
        "sessionId": thread_id,
        "forkedFromId": null,
        "preview": preview.unwrap_or(""),
        "ephemeral": false,
        "modelProvider": "gents",
        "createdAt": now,
        "updatedAt": now,
        "status": status,
        "path": null,
        "cwd": absolute_path(cwd),
        "cliVersion": env!("CARGO_PKG_VERSION"),
        "source": "cli",
        "threadSource": null,
        "agentNickname": null,
        "agentRole": null,
        "gitInfo": null,
        "name": null,
        "turns": turns
    })
}

pub(super) fn turn_value(
    turn_id: &str,
    status: codex::TurnStatus,
    items: Vec<codex::ThreadItem>,
    error: Option<codex::TurnError>,
) -> codex::Turn {
    let now = now_seconds();
    let completed_at = (!matches!(status, codex::TurnStatus::InProgress)).then_some(now);
    let mut turn = turn_value_with_timing(turn_id, status, items, error, Some(now), completed_at);
    turn.duration_ms = None;
    turn
}

pub(super) fn turn_value_with_timing(
    turn_id: &str,
    status: codex::TurnStatus,
    items: Vec<codex::ThreadItem>,
    error: Option<codex::TurnError>,
    started_at: Option<i64>,
    completed_at: Option<i64>,
) -> codex::Turn {
    let items_view = if items.is_empty() {
        codex::TurnItemsView::NotLoaded
    } else {
        codex::TurnItemsView::Full
    };
    codex::Turn {
        id: turn_id.to_string(),
        items,
        items_view,
        status,
        error,
        started_at,
        completed_at,
        duration_ms: started_at
            .zip(completed_at)
            .map(|(started, completed)| completed.saturating_sub(started).max(0) * 1000),
    }
}

pub(super) fn timestamp_seconds(raw: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|timestamp| timestamp.timestamp())
}

pub(super) fn agent_message_item(item_id: &str, text: &str) -> codex::ThreadItem {
    agent_message_item_with_phase(item_id, text, None)
}

pub(super) fn agent_message_item_with_phase(
    item_id: &str,
    text: &str,
    phase: Option<MessagePhase>,
) -> codex::ThreadItem {
    codex::ThreadItem::AgentMessage {
        id: item_id.to_string(),
        text: text.to_string(),
        phase,
        memory_citation: None,
    }
}

pub(super) async fn send_committed_user_message(
    outbound: &Outbound,
    state: &ShimState,
    thread_id: &str,
    turn_id: &str,
    input: &[codex::UserInput],
    completed_at_ms: Option<i64>,
) -> Result<()> {
    send_notification(
        outbound,
        state,
        codex::ServerNotification::ItemCompleted(codex::ItemCompletedNotification {
            item: codex::ThreadItem::UserMessage {
                id: state.next_id("gents-user-message"),
                content: input.to_vec(),
            },
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            completed_at_ms: completed_at_ms.unwrap_or_else(now_millis),
        }),
    )
    .await
}

pub(super) fn empty_rate_limits() -> codex::RateLimitSnapshot {
    codex::RateLimitSnapshot {
        limit_id: None,
        limit_name: None,
        primary: None,
        secondary: None,
        credits: None,
        plan_type: None,
        rate_limit_reached_type: None,
    }
}

pub(super) fn user_text_from_input(input: &[codex::UserInput]) -> String {
    input
        .iter()
        .filter_map(|item| match item {
            codex::UserInput::Text { text, .. } => Some(text.as_str()),
            codex::UserInput::Skill { .. }
            | codex::UserInput::Image { .. }
            | codex::UserInput::LocalImage { .. }
            | codex::UserInput::Mention { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) fn selected_skill_ids_from_input(input: &[codex::UserInput]) -> Vec<String> {
    input
        .iter()
        .filter_map(|item| match item {
            codex::UserInput::Skill { name, path } => path
                .file_name()
                .and_then(|segment| segment.to_str())
                .map(str::to_string)
                .filter(|id| !id.trim().is_empty())
                .or_else(|| {
                    let name = name.trim();
                    (!name.is_empty()).then(|| name.to_string())
                }),
            _ => None,
        })
        .collect()
}

pub(super) fn codex_turn_metadata(
    cwd: &Path,
    selected_skill_ids: &[String],
    conversation_title: Option<&str>,
) -> String {
    let mut metadata = json!({
        "codex_shim": {
            "cwd": absolute_path(cwd)
        }
    });
    if !selected_skill_ids.is_empty() {
        metadata["selected_skill_ids"] = json!(selected_skill_ids);
    }
    if let Some(title) = conversation_title.filter(|title| !title.trim().is_empty()) {
        metadata["conversation_title"] = json!(title.trim());
    }
    metadata.to_string()
}

pub(super) fn codex_steering_metadata(
    cwd: &Path,
    queued_after_request_id: &str,
    selected_skill_ids: &[String],
) -> String {
    let mut metadata = json!({
        "codex_shim": {
            "cwd": absolute_path(cwd)
        },
        "queue": {
            "source": "steering",
            "policy": "append",
            "key": null,
            "queued_after_request_id": queued_after_request_id
        }
    });
    if !selected_skill_ids.is_empty() {
        metadata["selected_skill_ids"] = json!(selected_skill_ids);
    }
    metadata.to_string()
}

pub(super) fn effective_cwd(state: &ShimState, cwd: Option<&str>) -> PathBuf {
    let Some(cwd) = cwd else {
        return state.cwd.clone();
    };
    let path = PathBuf::from(cwd);
    if path.is_absolute() {
        path
    } else {
        state.cwd.join(path)
    }
}

pub(super) fn absolute_path(path: &Path) -> String {
    if path.is_absolute() {
        path.display().to_string()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
            .display()
            .to_string()
    }
}

fn now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(super) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_text_items_from_codex_turn_payload() {
        let input = vec![
            codex::UserInput::Text {
                text: "hello".to_string(),
                text_elements: Vec::new(),
            },
            codex::UserInput::Image {
                detail: None,
                url: "https://example.invalid/image.png".to_string(),
            },
            codex::UserInput::Text {
                text: "world".to_string(),
                text_elements: Vec::new(),
            },
        ];

        assert_eq!(user_text_from_input(&input), "hello\nworld");
    }

    #[test]
    fn user_text_extraction_ignores_skill_selections() {
        let input = vec![
            codex::UserInput::Text {
                text: "summarize this".to_string(),
                text_elements: Vec::new(),
            },
            codex::UserInput::Skill {
                name: "Deep Research".to_string(),
                path: std::path::PathBuf::from("/gents/skills/research"),
            },
        ];
        assert_eq!(user_text_from_input(&input), "summarize this");
    }

    #[test]
    fn selected_skill_ids_extracts_reference_from_pill() {
        let input = vec![
            codex::UserInput::Text {
                text: "go".to_string(),
                text_elements: Vec::new(),
            },
            codex::UserInput::Skill {
                name: "Deep Research".to_string(),
                path: std::path::PathBuf::from("/gents/skills/research"),
            },
        ];
        assert_eq!(selected_skill_ids_from_input(&input), vec!["research"]);
        assert!(selected_skill_ids_from_input(&input[..1]).is_empty());
    }

    #[test]
    fn thread_status_uses_codex_tag_shape() {
        let thread = thread_json(
            Path::new("/tmp"),
            "thread-1",
            Some("preview"),
            codex::ThreadStatus::Idle,
            Vec::new(),
        );
        let typed: codex::Thread = serde_json::from_value(thread).unwrap();
        let serialized = serde_json::to_value(typed).unwrap();

        assert_eq!(serialized.pointer("/status/type"), Some(&json!("idle")));
        assert_eq!(serialized.pointer("/source"), Some(&json!("cli")));
    }

    #[test]
    fn turn_timing_uses_durable_seconds_and_derives_duration() {
        let turn = turn_value_with_timing(
            "turn-1",
            codex::TurnStatus::Completed,
            Vec::new(),
            None,
            Some(100),
            Some(125),
        );
        assert_eq!(turn.started_at, Some(100));
        assert_eq!(turn.completed_at, Some(125));
        assert_eq!(turn.duration_ms, Some(25_000));

        let incomplete = turn_value_with_timing(
            "turn-2",
            codex::TurnStatus::InProgress,
            Vec::new(),
            None,
            Some(100),
            None,
        );
        assert_eq!(incomplete.duration_ms, None);
    }
}
