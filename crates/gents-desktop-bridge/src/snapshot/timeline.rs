use serde_json::Value;

use std::collections::BTreeMap;

use gents_protocol::timeline::{
    build_timeline_order, has_durable_user_owner, DurableUserOwnerInput, OverlayInput,
    OverlayPlacement, PendingInput, PendingPlacement, TimelineMessageInput, TimelineRole,
    TimelineSlot,
};

use super::super::types::{
    normalize_optional, MessageView, PendingTurnView, RenderedTimelineItem, RenderedToolCallView,
    ResponseView, ToolCallView, ToolDetailFieldView, ToolDetailValueView,
};

pub(super) fn normalize_timeline_text(value: Option<&str>) -> String {
    value.map(str::trim).unwrap_or_default().to_string()
}

fn render_json_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        _ => serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()),
    }
}

fn parse_tool_detail_value(value: Option<&str>) -> Option<ToolDetailValueView> {
    let raw_text = normalize_optional(value)?;
    let parsed = serde_json::from_str::<Value>(&raw_text).ok();
    let fields = match parsed {
        Some(Value::Object(map)) => map
            .into_iter()
            .map(|(key, value)| ToolDetailFieldView {
                key,
                value: render_json_value(&value),
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };

    Some(ToolDetailValueView { raw_text, fields })
}

fn tool_status_kind(status: Option<&str>) -> String {
    match status.unwrap_or_default().to_ascii_lowercase().as_str() {
        "completed" | "complete" | "success" => "success".to_string(),
        "failed" | "error" | "cancelled" | "timedout" => "error".to_string(),
        "awaitingapproval" => "awaitingApproval".to_string(),
        _ => "running".to_string(),
    }
}

fn render_tool_call(tool: ToolCallView) -> RenderedToolCallView {
    RenderedToolCallView {
        item_key: tool.tool_call_key.clone(),
        tool_name: tool.tool_name.clone().unwrap_or_else(|| "tool".to_string()),
        status_kind: tool_status_kind(tool.lifecycle_state.as_deref().or(tool.status.as_deref())),
        status: tool.status.clone(),
        child_request_id: tool.child_request_id.clone(),
        await_mode: tool.await_mode.clone(),
        args: parse_tool_detail_value(tool.args.as_deref()),
        partial_output_tail: tool.partial_output_tail.clone(),
        partial_output_seq: tool.partial_output_seq,
        result: parse_tool_detail_value(tool.result.as_deref()),
        denial: tool.denial.clone(),
        cancel_cause: tool.cancel_cause.clone(),
    }
}

pub(super) fn has_materialized_user_owner(messages: &[MessageView], request_id: &str) -> bool {
    let ownership = messages
        .iter()
        .map(|message| {
            let role = message
                .display_role
                .as_deref()
                .or(message.role.as_deref())
                .unwrap_or_default();
            DurableUserOwnerInput {
                request_id: message.request_id.as_deref(),
                is_user: role.eq_ignore_ascii_case("user"),
                has_visible_content: normalize_optional(message.display_content.as_deref())
                    .is_some(),
                runtime_control: message.runtime_control,
            }
        })
        .collect::<Vec<_>>();
    has_durable_user_owner(&ownership, request_id)
}

fn message_presentation_key(
    message: &MessageView,
    role: &str,
    content: &Option<String>,
    reasoning: &Option<String>,
) -> Option<(i64, String, Option<String>, Option<String>, bool, bool)> {
    message.sequence.map(|sequence| {
        (
            sequence,
            role.to_ascii_lowercase(),
            content.clone(),
            reasoning.clone(),
            message.has_tool_calls,
            message.has_tool_results,
        )
    })
}

fn overlay_has_durable_owner(
    messages: &[MessageView],
    active_response_request_id: Option<&str>,
    overlay_content: &Option<String>,
    overlay_reasoning: &Option<String>,
) -> bool {
    let Some(request_id) = active_response_request_id else {
        return false;
    };
    if overlay_content.is_none() && overlay_reasoning.is_none() {
        return false;
    }

    messages.iter().any(|message| {
        message.request_id.as_deref() == Some(request_id)
            && message
                .display_role
                .as_deref()
                .or(message.role.as_deref())
                .is_some_and(|role| role.eq_ignore_ascii_case("assistant"))
            && !message.has_tool_results
            && !message.runtime_control
            && normalize_optional(message.display_content.as_deref()) == *overlay_content
            && normalize_optional(message.reasoning.as_deref()) == *overlay_reasoning
    })
}

fn tool_is_nonterminal(tool: &ToolCallView) -> bool {
    matches!(
        tool_status_kind(tool.lifecycle_state.as_deref().or(tool.status.as_deref())).as_str(),
        "running" | "awaitingApproval"
    )
}

fn render_timeline_order(
    order: Vec<TimelineSlot>,
    rendered_messages: &BTreeMap<String, RenderedTimelineItem>,
    tool_groups: &BTreeMap<Option<i64>, Vec<ToolCallView>>,
    pending_turn: Option<&PendingTurnView>,
    overlay_content: &Option<String>,
    overlay_reasoning: &Option<String>,
) -> Vec<RenderedTimelineItem> {
    let mut timeline = Vec::with_capacity(order.len());
    for slot in order {
        match slot {
            TimelineSlot::Message { key, .. } => {
                if let Some(item) = rendered_messages.get(&key) {
                    timeline.push(item.clone());
                }
            }
            TimelineSlot::ToolGroup { message_sequence } => {
                let tools = tool_groups
                    .get(&message_sequence)
                    .cloned()
                    .unwrap_or_default();
                timeline.push(RenderedTimelineItem::ToolGroup {
                    item_key: format!("tools-{}", message_sequence.unwrap_or(-1)),
                    message_sequence,
                    tools: tools.into_iter().map(render_tool_call).collect(),
                });
            }
            TimelineSlot::Pending => {
                if let Some(pending_turn) = pending_turn {
                    timeline.push(RenderedTimelineItem::PendingUserTurn {
                        item_key: format!("pending-{}", pending_turn.request_id),
                        request_id: pending_turn.request_id.clone(),
                        content: pending_turn.content.clone(),
                        selected_skill_ids: pending_turn.selected_skill_ids.clone(),
                        lifecycle_state: pending_turn.lifecycle_state.clone(),
                        created_at: pending_turn.created_at.clone(),
                    });
                }
            }
            TimelineSlot::Overlay => {
                if overlay_content.is_some() || overlay_reasoning.is_some() {
                    timeline.push(RenderedTimelineItem::LiveAssistant {
                        item_key: "live-assistant".to_string(),
                        content: overlay_content.clone(),
                        reasoning: overlay_reasoning.clone(),
                    });
                }
            }
        }
    }
    timeline
}

pub(super) fn build_rendered_timeline(
    messages: &[MessageView],
    tool_calls: &[ToolCallView],
    pending_turn: Option<&PendingTurnView>,
    active_response_overlay: Option<&ResponseView>,
    active_response_request_id: Option<&str>,
) -> Vec<RenderedTimelineItem> {
    // Group tool calls by their owning message sequence (rich lookup for the
    // mapping-back step); the presentation-neutral ORDER is decided by the
    // shared, Lean-fenced skeleton, not here.
    let mut tool_groups: BTreeMap<Option<i64>, Vec<ToolCallView>> = BTreeMap::new();
    for tool in tool_calls.iter().cloned() {
        tool_groups
            .entry(tool.message_sequence)
            .or_default()
            .push(tool);
    }
    let group_sequences: Vec<Option<i64>> = tool_groups.keys().copied().collect();

    // Candidate messages (step-2 filter: drop tool-result rows and rows with no
    // rendered content/reasoning/tool-calls — a presentation decision). For each
    // candidate, project the ordering-relevant fields the skeleton consumes, and
    // remember the rich content by key for mapping the slots back.
    let mut inputs: Vec<TimelineMessageInput> = Vec::new();
    let mut rendered_message: BTreeMap<String, RenderedTimelineItem> = BTreeMap::new();
    for message in messages.iter() {
        let role = message
            .display_role
            .as_deref()
            .or(message.role.as_deref())
            .unwrap_or("assistant");
        let is_user = role.eq_ignore_ascii_case("user");
        let is_background_control = is_user && message.runtime_control;
        let keep = !message.has_tool_results
            && !is_background_control
            && (!normalize_timeline_text(message.display_content.as_deref()).is_empty()
                || !normalize_timeline_text(message.reasoning.as_deref()).is_empty()
                || message.has_tool_calls);
        if !keep {
            continue;
        }
        let normalized_content = normalize_optional(message.display_content.as_deref());
        let normalized_reasoning = normalize_optional(message.reasoning.as_deref());
        let (emits_item, item) = if is_user {
            match normalized_content.clone() {
                Some(content) => (
                    true,
                    Some(RenderedTimelineItem::UserMessage {
                        item_key: message.message_key.clone(),
                        request_id: message.request_id.clone(),
                        sequence: message.sequence,
                        content,
                        timestamp: normalize_optional(message.timestamp.as_deref()),
                    }),
                ),
                None => (false, None),
            }
        } else if normalized_content.is_some() || normalized_reasoning.is_some() {
            (
                true,
                Some(RenderedTimelineItem::AssistantMessage {
                    item_key: message.message_key.clone(),
                    sequence: message.sequence,
                    content: normalized_content.clone(),
                    reasoning: normalized_reasoning.clone(),
                    timestamp: normalize_optional(message.timestamp.as_deref()),
                }),
            )
        } else {
            (false, None)
        };
        if let Some(item) = item {
            rendered_message
                .entry(message.message_key.clone())
                .or_insert(item);
        }
        // Presentation dedup token: the desktop only dedups by presentation when
        // the message carries a sequence (None opts out). Serialize the same
        // tuple the old `message_presentation_key` used, as an opaque token.
        let dedup_token =
            message_presentation_key(message, role, &normalized_content, &normalized_reasoning)
                .map(|key| format!("{key:?}"));
        inputs.push(TimelineMessageInput {
            key: message.message_key.clone(),
            sequence: message.sequence,
            role: if is_user {
                TimelineRole::User
            } else {
                TimelineRole::Assistant
            },
            emits_item,
            dedup_token,
        });
    }

    let overlay_content =
        active_response_overlay.and_then(|overlay| normalize_optional(overlay.content.as_deref()));
    let overlay_reasoning = active_response_overlay
        .and_then(|overlay| normalize_optional(overlay.reasoning.as_deref()));

    // Reduce the rich request-scoped ownership check to the neutral bit used by
    // the shared ordering contract. Do not infer ownership from the last item:
    // P2P can expose message 32 while the response head still contains message
    // 6's tail.
    let has_durable_owner = overlay_has_durable_owner(
        messages,
        active_response_request_id,
        &overlay_content,
        &overlay_reasoning,
    );

    // A tool belonging to this active, unmaterialized response is an orphan
    // until its owning assistant message is persisted. Keep the live reasoning
    // immediately before that running tool instead of letting it jump upward
    // only after materialization.
    let target_orphan = active_response_request_id.and_then(|request_id| {
        tool_calls
            .iter()
            .filter(|tool| {
                tool.request_id.as_deref() == Some(request_id)
                    && tool_is_nonterminal(tool)
                    && !inputs
                        .iter()
                        .any(|message| message.sequence == tool.message_sequence)
            })
            .map(|tool| tool.message_sequence)
            .max()
    });
    let overlay =
        (overlay_content.is_some() || overlay_reasoning.is_some()).then_some(OverlayInput {
            has_durable_owner,
            placement: target_orphan.map_or(OverlayPlacement::Tail, |message_sequence| {
                OverlayPlacement::BeforeOrphan { message_sequence }
            }),
        });
    let pending = pending_turn.map(|pending_turn| {
        let first_same_request_assistant = messages
            .iter()
            .filter(|message| {
                message.request_id.as_deref() == Some(pending_turn.request_id.as_str())
                    && message.sequence.is_some()
                    && message
                        .display_role
                        .as_deref()
                        .or(message.role.as_deref())
                        .is_some_and(|role| role.eq_ignore_ascii_case("assistant"))
                    && !message.has_tool_results
                    && !message.runtime_control
                    && (normalize_optional(message.display_content.as_deref()).is_some()
                        || normalize_optional(message.reasoning.as_deref()).is_some()
                        || message.has_tool_calls)
            })
            .min_by_key(|message| {
                message
                    .sequence
                    .map_or((0_i8, 0_i64), |sequence| (1, sequence))
            });
        PendingInput {
            placement: first_same_request_assistant
                .and_then(|message| message.sequence)
                .map_or(PendingPlacement::Tail, |message_sequence| {
                    PendingPlacement::BeforeMessage { message_sequence }
                }),
        }
    });
    let order = build_timeline_order(&inputs, &group_sequences, pending, overlay);
    render_timeline_order(
        order,
        &rendered_message,
        &tool_groups,
        pending_turn,
        &overlay_content,
        &overlay_reasoning,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        build_rendered_timeline, has_materialized_user_owner, render_tool_call, tool_status_kind,
        MessageView, RenderedTimelineItem, ResponseView, ToolCallView,
    };

    fn user_message(key: &str, sequence: i64, content: &str) -> MessageView {
        MessageView {
            message_key: key.to_string(),
            request_id: Some("request-1".to_string()),
            sequence: Some(sequence),
            role: Some("user".to_string()),
            content: Some(content.to_string()),
            display_role: Some("user".to_string()),
            display_content: Some(content.to_string()),
            reasoning: None,
            has_tool_calls: false,
            has_tool_results: false,
            runtime_control: false,
            timestamp: None,
        }
    }

    fn assistant_message(key: &str, request_id: &str, sequence: i64, content: &str) -> MessageView {
        MessageView {
            message_key: key.to_string(),
            request_id: Some(request_id.to_string()),
            sequence: Some(sequence),
            role: Some("assistant".to_string()),
            content: Some(content.to_string()),
            display_role: Some("assistant".to_string()),
            display_content: Some(content.to_string()),
            reasoning: None,
            has_tool_calls: false,
            has_tool_results: false,
            runtime_control: false,
            timestamp: None,
        }
    }

    fn streaming_response(content: &str) -> ResponseView {
        ResponseView {
            status: Some("streaming".to_string()),
            content: Some(content.to_string()),
            reasoning: None,
            error_message: None,
            token_count: None,
            materialized_message_sequence: None,
            materialized_at: None,
            interrupted_at: None,
            completed_at: None,
            cancel_cause: None,
            backend_id: None,
        }
    }

    #[test]
    fn status_kind_maps_awaiting_approval_and_terminals() {
        assert_eq!(
            tool_status_kind(Some("awaitingApproval")),
            "awaitingApproval"
        );
        assert_eq!(tool_status_kind(Some("completed")), "success");
        assert_eq!(tool_status_kind(Some("failed")), "error");
        assert_eq!(tool_status_kind(Some("cancelled")), "error");
        assert_eq!(tool_status_kind(Some("timedOut")), "error");
        assert_eq!(tool_status_kind(Some("running")), "running");
        assert_eq!(tool_status_kind(None), "running");
    }

    #[test]
    fn subagent_identity_and_await_mode_reach_rendered_tool() {
        let rendered = render_tool_call(ToolCallView {
            tool_call_key: "spawn-1".to_string(),
            request_id: Some("parent-1".to_string()),
            message_sequence: Some(2),
            tool_name: Some("spawn_subagent".to_string()),
            tool_call_id: Some("spawn-1".to_string()),
            args: Some(
                r#"{"name":"researcher","prompt":"trace the request flow","await_mode":"background"}"#
                    .to_string(),
            ),
            partial_output_tail: Some("reading watcher.rs".to_string()),
            partial_output_seq: Some(18),
            result: None,
            status: Some("running".to_string()),
            lifecycle_state: Some("running".to_string()),
            child_request_id: Some("child-request-1".to_string()),
            await_mode: Some("background".to_string()),
            started_at: None,
            completed_at: None,
            denial: None,
            cancel_cause: None,
        });

        assert_eq!(rendered.tool_name, "spawn_subagent");
        assert_eq!(
            rendered.child_request_id.as_deref(),
            Some("child-request-1")
        );
        assert_eq!(rendered.await_mode.as_deref(), Some("background"));
        assert_eq!(rendered.status_kind, "running");
    }

    #[test]
    fn background_completion_controls_do_not_render_as_user_turns() {
        let messages = vec![
            user_message("user-1", 1, "Please classify these sessions."),
            MessageView {
                runtime_control: true,
                ..user_message(
                    "notification",
                    2,
                    r#"<subagent-notification child_request_id="child-1">
<summary>classification complete</summary>
</subagent-notification>"#,
                )
            },
            MessageView {
                runtime_control: true,
                ..user_message(
                    "wake",
                    3,
                    gents::background_completion::BACKGROUND_COMPLETION_WAKE_PROMPT,
                )
            },
        ];

        let timeline = build_rendered_timeline(&messages, &[], None, None, None);

        assert!(has_materialized_user_owner(&messages, "request-1"));
        assert_eq!(timeline.len(), 1);
        assert!(matches!(
            &timeline[0],
            RenderedTimelineItem::UserMessage { content, .. }
                if content == "Please classify these sessions."
        ));
    }

    #[test]
    fn user_text_that_looks_like_a_control_message_still_renders() {
        let content = gents::background_completion::BACKGROUND_COMPLETION_WAKE_PROMPT;
        let messages = vec![user_message("literal-user-text", 1, content)];

        let timeline = build_rendered_timeline(&messages, &[], None, None, None);

        assert!(has_materialized_user_owner(&messages, "request-1"));
        assert!(matches!(
            &timeline[0],
            RenderedTimelineItem::UserMessage {
                content: rendered,
                ..
            } if rendered == content
        ));
    }

    #[test]
    fn stale_replicated_tail_is_hidden_when_earlier_turn_from_same_request_owns_it() {
        let stale = "Good catch — inspect the current system prompt.";
        let messages = vec![
            user_message("user-1", 1, "Check fleet status"),
            assistant_message("assistant-6", "request-1", 6, stale),
            assistant_message(
                "assistant-32",
                "request-1",
                32,
                "Diff is clean and scoped — applying now.",
            ),
        ];
        let response = streaming_response(stale);

        let timeline =
            build_rendered_timeline(&messages, &[], None, Some(&response), Some("request-1"));

        assert_eq!(
            timeline
                .iter()
                .filter(|item| matches!(item, RenderedTimelineItem::LiveAssistant { .. }))
                .count(),
            0,
            "an old response head must not re-emit message 6 after message 32 is durable"
        );
        assert!(timeline.iter().any(|item| matches!(
            item,
            RenderedTimelineItem::AssistantMessage {
                sequence: Some(32),
                ..
            }
        )));
    }

    #[test]
    fn identical_text_from_another_request_does_not_own_live_tail() {
        let repeated = "I am checking now.";
        let messages = vec![assistant_message("assistant-2", "request-old", 2, repeated)];
        let response = streaming_response(repeated);

        let timeline = build_rendered_timeline(
            &messages,
            &[],
            None,
            Some(&response),
            Some("request-current"),
        );

        assert!(timeline
            .iter()
            .any(|item| matches!(item, RenderedTimelineItem::LiveAssistant { .. })));
    }
}
