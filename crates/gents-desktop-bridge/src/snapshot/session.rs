use std::collections::HashMap;

use chrono::{DateTime, Utc};
use gents_protocol::row::{AgentMessageRow, AgentRequestRow, AgentToolCallRow};
use gents_protocol::transcript::{normalize_markdown_text, present_persisted_message};

use super::super::cause_derivation::{
    derive_response_cause, derive_tool_call_cause, RequestEvidence, ResponseEvidence,
    ToolCallEvidence,
};
use super::super::types::{
    normalize_optional, turn_state_label, CommandDenialView, DerivedCancelCauseView,
    DesktopSessionSnapshot, GoalView, MessageView, PendingTurnView, ResponseView,
    RetryEligibilityView, ToolCallView, ToolResultView,
};
use super::timeline::{build_rendered_timeline, has_materialized_user_owner};
use super::{request_matches_agent, source_matches_agent};

fn message_is_runtime_control(
    message: &AgentMessageRow,
    requests_by_id: &HashMap<&str, &AgentRequestRow>,
) -> bool {
    gents::background_completion::is_background_completion_notification_message_key(
        &message.message_key,
    ) || message
        .request_id
        .as_deref()
        .and_then(|request_id| requests_by_id.get(request_id))
        .is_some_and(|request| {
            gents::lifecycle::is_background_completion_request(request.metadata.as_deref())
        })
}

fn request_is_deprecated_background_completion(request: &AgentRequestRow) -> bool {
    gents::lifecycle::is_deprecated_background_completion_request(
        request.execution_origin.as_deref(),
        request.metadata.as_deref(),
    )
}

fn command_denial_from_row(row: &AgentToolCallRow) -> Option<CommandDenialView> {
    let rule_id = normalize_optional(row.denial_reason.as_deref())?;
    let (category, category_label, reason_line) = command_denial_presentation(&rule_id);
    let denied_command = normalize_optional(row.denied_command.as_deref())
        .or_else(|| first_token(row.denied_prefix.as_deref()))
        .or_else(|| first_token(row.denied_argv.as_deref()));

    Some(CommandDenialView {
        category: category.to_string(),
        category_label: category_label.to_string(),
        rule_id,
        reason_line: reason_line.to_string(),
        denied_command,
        denied_argument: normalize_optional(row.denied_argument.as_deref()),
        denied_subcommand: normalize_optional(row.denied_subcommand.as_deref()),
        diagnostic: normalize_optional(row.result.as_deref()).unwrap_or_default(),
    })
}

fn first_token(value: Option<&[String]>) -> Option<String> {
    value
        .and_then(|items| items.first().cloned())
        .and_then(|value| normalize_optional(Some(value.as_str())))
}

fn command_denial_presentation(rule_id: &str) -> (&'static str, &'static str, &'static str) {
    match rule_id {
        "forbiddenPrefix" => (
            "forbidden-prefix",
            "Forbidden prefix",
            "argv begins with a forbidden prefix configured on this behavior.",
        ),
        "allowedPrefixRequired" => (
            "allowed-prefix-required",
            "Allowed prefix required",
            "Policy requires argv to match one of the configured allowed prefixes; this argv matches none.",
        ),
        "disabledNetworkUnenforceable" => (
            "network-denied",
            "Network access denied",
            "Network mode is disabled, but the unrestricted bash tool can't enforce it - failing closed.",
        ),
        "disabledNetworkCommand" => (
            "network-denied",
            "Network access denied",
            "This command is denied because the behavior has network mode disabled.",
        ),
        "workspaceWriteSandboxUnavailable" => (
            "sandbox-violation",
            "Sandbox violation",
            "workspace_write needs an enforced sandbox before the command can run.",
        ),
        "readOnlyCommandNotAllowlisted"
        | "readOnlyArgumentNotAllowed"
        | "readOnlySubcommandRequired"
        | "readOnlySubcommandNotAllowlisted"
        | "readOnlyUrlRequired" => (
            "read-only-guard",
            "Read-only guard",
            "The read-only bash policy blocked this command.",
        ),
        _ => (
            "policy-config",
            "Policy configuration",
            "The command was denied by the configured command execution policy.",
        ),
    }
}

#[cfg(test)]
pub fn build_session_snapshot_from_store(
    store: &gents_desktop_core::client::ClientStore,
    session_id: &str,
    preferred_request_id: Option<&str>,
) -> Option<DesktopSessionSnapshot> {
    build_session_snapshot_from_store_for_agent(store, None, session_id, preferred_request_id)
}

pub fn build_session_snapshot_from_store_for_agent(
    store: &gents_desktop_core::client::ClientStore,
    agent_did: Option<&str>,
    session_id: &str,
    preferred_request_id: Option<&str>,
) -> Option<DesktopSessionSnapshot> {
    let conversation = store.conversations.iter().find(|row| {
        row.session_id == session_id
            && agent_did.is_none_or(|agent_did| row.agent_did.as_deref() == Some(agent_did))
    });
    let session_row = store
        .sessions
        .iter()
        .enumerate()
        .find(|(index, row)| {
            row.session_id == session_id
                && agent_did.is_none_or(|agent_did| {
                    source_matches_agent(&store.session_source_agent_dids, *index, agent_did, false)
                })
        })
        .map(|(_index, row)| row);
    let requests = agent_did.map_or_else(
        || store.requests_for_session(session_id),
        |agent_did| store.requests_for_session_for_agent(session_id, agent_did),
    );
    let goal = store
        .goals
        .iter()
        .filter(|row| {
            row.session_id == session_id
                && agent_did.is_none_or(|agent_did| row.agent_did == agent_did)
        })
        .min_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.goal_id.cmp(&right.goal_id))
        })
        .map(|row| GoalView {
            goal_id: row.goal_id.clone(),
            objective: normalize_optional(row.objective.as_deref()),
            status: normalize_optional(row.status.as_deref()),
            token_budget: row.token_budget,
            tokens_used: row.tokens_used.unwrap_or_default().max(0),
            active_time_seconds: row.active_time_seconds.unwrap_or_default().max(0),
            consecutive_blocked_audits: row.consecutive_blocked_audits.unwrap_or_default().max(0),
            continuation_sequence: row.continuation_sequence.unwrap_or_default().max(0),
            wrapup_requested: row.wrapup_requested.unwrap_or(false),
            wrapup_completed: row.wrapup_completed.unwrap_or(false),
            last_blocked_reason: normalize_optional(row.last_blocked_reason.as_deref()),
            last_failure: normalize_optional(row.last_failure.as_deref()),
            completion_evidence: normalize_optional(row.completion_evidence.as_deref()),
        });

    if conversation.is_none() && session_row.is_none() && requests.is_empty() && goal.is_none() {
        return None;
    }

    let transcript = agent_did.map_or_else(
        || store.transcript(session_id),
        |agent_did| store.transcript_for_agent(session_id, agent_did),
    );
    let latest_request_id = preferred_request_id
        .filter(|request_id| {
            requests.iter().any(|row| {
                row.request_id == *request_id && !request_is_deprecated_background_completion(row)
            })
        })
        .map(str::to_owned)
        .or_else(|| {
            agent_did.map_or_else(
                || store.latest_request_id_for_session(session_id),
                |agent_did| store.latest_request_id_for_session_for_agent(session_id, agent_did),
            )
        });
    let latest_request = latest_request_id
        .as_deref()
        .and_then(|request_id| {
            requests
                .iter()
                .find(|row| row.request_id == request_id)
                .copied()
        })
        .or_else(|| {
            latest_request_id
                .is_none()
                .then(|| {
                    requests
                        .iter()
                        .rev()
                        .find(|request| !request_is_deprecated_background_completion(request))
                        .copied()
                })
                .flatten()
        });
    let retry_eligibility = project_retry_eligibility(latest_request);
    let turn_state = latest_request_id
        .as_deref()
        .and_then(|request_id| {
            agent_did.map_or_else(
                || store.derive_turn_for_request(request_id),
                |agent_did| store.derive_turn_for_request_for_agent(request_id, agent_did),
            )
        })
        .or_else(|| {
            if agent_did.is_none() {
                store.derive_turn(session_id)
            } else {
                None
            }
        });
    let turn_state_label = turn_state.map(turn_state_label).map(str::to_owned);
    let latest_response = latest_request_id
        .as_deref()
        .and_then(|request_id| {
            agent_did.map_or_else(
                || store.latest_response_for_request(request_id),
                |agent_did| store.latest_response_for_request_for_agent(request_id, agent_did),
            )
        })
        .map(|row| {
            let req_evidence = latest_request
                .map(|r| RequestEvidence {
                    interrupt_requested_at: r.interrupt_requested_at.clone(),
                    caused_by_parent_request_id: r.caused_by_parent_request_id.clone(),
                })
                .unwrap_or_default();
            let resp_evidence = ResponseEvidence {
                interrupted_at: normalize_optional(row.interrupted_at.as_deref()),
            };
            let cancel_cause = derive_response_cause(&req_evidence, &resp_evidence);
            let backend_id =
                latest_request.and_then(|r| normalize_optional(r.backend_id.as_deref()));
            ResponseView {
                status: normalize_optional(row.status.as_deref()),
                content: row
                    .content
                    .as_deref()
                    .map(normalize_markdown_text)
                    .filter(|value| !value.is_empty()),
                reasoning: row
                    .reasoning
                    .as_deref()
                    .map(normalize_markdown_text)
                    .filter(|value| !value.is_empty()),
                error_message: normalize_optional(row.error_message.as_deref()),
                token_count: row.token_count,
                materialized_message_sequence: row.materialized_message_sequence,
                materialized_at: normalize_optional(row.materialized_at.as_deref()),
                interrupted_at: normalize_optional(row.interrupted_at.as_deref()),
                completed_at: normalize_optional(row.completed_at.as_deref()),
                cancel_cause,
                backend_id,
            }
        });
    let active_response_overlay = latest_response.clone().filter(|response| {
        let response_status = response
            .status
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase();
        matches!(
            turn_state,
            Some(gents_protocol::client_protocol::ClientTurnState::WaitingForClaim)
                | Some(gents_protocol::client_protocol::ClientTurnState::Streaming)
        ) && response.materialized_message_sequence.is_none()
            && response.interrupted_at.is_none()
            && !matches!(
                response_status.as_str(),
                "complete" | "completed" | "error" | "failed"
            )
            && (response
                .content
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
                || response
                    .reasoning
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty()))
    });
    let pending_turn = latest_request_id
        .as_deref()
        .and_then(|request_id| build_pending_turn(store, agent_did, session_id, request_id));

    let requests_by_id: HashMap<&str, &AgentRequestRow> = requests
        .iter()
        .map(|request| (request.request_id.as_str(), *request))
        .collect();
    let messages = transcript
        .messages
        .into_iter()
        .map(|row| {
            let role = normalize_optional(row.role.as_deref());
            let content = normalize_optional(row.content.as_deref());
            let presentation = role
                .as_deref()
                .zip(content.as_deref())
                .map(|(role, content)| present_persisted_message(role, content));

            MessageView {
                message_key: row.message_key.clone(),
                request_id: row.request_id.clone(),
                sequence: row.sequence,
                role,
                content,
                display_role: presentation
                    .as_ref()
                    .map(|presentation| presentation.role.label().to_ascii_lowercase()),
                display_content: presentation.as_ref().and_then(|presentation| {
                    normalize_optional(Some(presentation.body_markdown.as_str()))
                }),
                reasoning: presentation.as_ref().and_then(|presentation| {
                    presentation
                        .reasoning_markdown
                        .as_deref()
                        .and_then(|reasoning| normalize_optional(Some(reasoning)))
                }),
                has_tool_calls: presentation
                    .as_ref()
                    .is_some_and(|presentation| presentation.has_tool_calls),
                has_tool_results: presentation
                    .as_ref()
                    .is_some_and(|presentation| presentation.has_tool_results),
                runtime_control: message_is_runtime_control(row, &requests_by_id),
                timestamp: normalize_optional(row.timestamp.as_deref()),
            }
        })
        .collect::<Vec<_>>();

    let tool_calls = transcript
        .tool_calls
        .into_iter()
        .map(|row| {
            let cancel_cause =
                if let Some(persisted) = row.cancel_cause.as_deref().filter(|s| !s.is_empty()) {
                    Some(DerivedCancelCauseView {
                        cause: persisted.to_string(),
                        source: "toolLifecycle".into(),
                        confidence: "direct".into(),
                        at: normalize_optional(row.completed_at.as_deref()),
                        evidence: vec![format!(
                            "AgentToolCall.cancel_cause = {persisted:?} (persisted)"
                        )],
                    })
                } else {
                    let req_for_tool = row
                        .request_id
                        .as_deref()
                        .and_then(|rid| requests_by_id.get(rid).copied())
                        .or(latest_request);
                    let req_evidence = req_for_tool
                        .map(|r| RequestEvidence {
                            interrupt_requested_at: r.interrupt_requested_at.clone(),
                            caused_by_parent_request_id: r.caused_by_parent_request_id.clone(),
                        })
                        .unwrap_or_default();
                    let tool_evidence = ToolCallEvidence {
                        lifecycle_state: row.lifecycle_state.clone(),
                        deadline_at: row.deadline_at.clone(),
                        cancel_policy: row.cancel_policy.clone(),
                        completed_at: row.completed_at.clone(),
                        timed_out: row.lifecycle_state.as_deref() == Some("timedOut"),
                    };
                    derive_tool_call_cause(&req_evidence, &tool_evidence)
                };
            ToolCallView {
                tool_call_key: row.tool_call_key.clone(),
                request_id: normalize_optional(row.request_id.as_deref()),
                message_sequence: row.message_sequence,
                tool_name: normalize_optional(row.tool_name.as_deref()),
                tool_call_id: normalize_optional(row.tool_call_id.as_deref()),
                args: normalize_optional(row.args.as_deref()),
                partial_output_tail: normalize_optional(row.partial_output_tail.as_deref()),
                partial_output_seq: row.partial_output_seq,
                result: normalize_optional(row.result.as_deref()),
                status: normalize_optional(row.status.as_deref()),
                lifecycle_state: normalize_optional(row.lifecycle_state.as_deref()),
                child_request_id: normalize_optional(row.child_request_id.as_deref()),
                await_mode: normalize_optional(row.await_mode.as_deref()),
                cancel_policy: normalize_optional(row.cancel_policy.as_deref()),
                started_at: normalize_optional(row.started_at.as_deref()),
                deadline_at: normalize_optional(row.deadline_at.as_deref()),
                completed_at: normalize_optional(row.completed_at.as_deref()),
                denial: command_denial_from_row(&row),
                cancel_cause,
            }
        })
        .collect::<Vec<_>>();

    let tool_results = transcript
        .tool_results
        .into_iter()
        .map(|row| ToolResultView {
            tool_name: normalize_optional(row.tool_name.as_deref()),
            tool_input: normalize_optional(row.tool_input.as_deref()),
            output_text: normalize_optional(row.output_text.as_deref()),
            truncated: row.truncated,
            created_at: normalize_optional(row.created_at.as_deref()),
        })
        .collect::<Vec<_>>();

    let timeline_items = build_rendered_timeline(
        &messages,
        &tool_calls,
        pending_turn.as_ref(),
        active_response_overlay.as_ref(),
        active_response_overlay
            .as_ref()
            .and(latest_request_id.as_deref()),
    );

    Some(DesktopSessionSnapshot {
        session_id: session_id.to_string(),
        agent_did: conversation
            .and_then(|row| normalize_optional(row.agent_did.as_deref()))
            .or_else(|| {
                latest_request.and_then(|row| normalize_optional(row.agent_did.as_deref()))
            }),
        behavior_id: conversation
            .and_then(|row| normalize_optional(row.behavior_id.as_deref()))
            .or_else(|| session_row.and_then(|row| normalize_optional(row.behavior_id.as_deref())))
            .or_else(|| {
                latest_request.and_then(|row| normalize_optional(row.behavior_id.as_deref()))
            }),
        title: conversation.and_then(|row| normalize_optional(row.title.as_deref())),
        preview_text: conversation.and_then(|row| normalize_optional(row.preview_text.as_deref())),
        status: conversation
            .and_then(|row| normalize_optional(row.status.as_deref()))
            .or_else(|| session_row.and_then(|row| normalize_optional(row.status.as_deref()))),
        goal,
        turn_state: turn_state_label,
        latest_request_id,
        retry_eligibility,
        latest_response,
        active_response_overlay,
        pending_turn,
        timeline_items,
        messages,
        tool_calls,
        tool_results,
    })
}

fn project_retry_eligibility(request: Option<&AgentRequestRow>) -> RetryEligibilityView {
    let Some(request) = request else {
        return RetryEligibilityView {
            eligible: false,
            denial_reason: Some("requestNotObserved".to_string()),
        };
    };
    if request.lifecycle_state.as_deref() != Some("failed")
        || request.status.as_deref() != Some("error")
    {
        return RetryEligibilityView {
            eligible: false,
            denial_reason: Some("notFailed".to_string()),
        };
    }
    if request.execution_origin.as_deref() != Some("interactive") {
        return RetryEligibilityView {
            eligible: false,
            denial_reason: Some("nonInteractiveOrigin".to_string()),
        };
    }
    if request.retry_count.unwrap_or_default() >= request.max_retries.unwrap_or(3) {
        return RetryEligibilityView {
            eligible: false,
            denial_reason: Some("retryBudgetExhausted".to_string()),
        };
    }
    if let Some(deadline) = normalize_optional(request.deadline.as_deref()) {
        let Ok(deadline) = DateTime::parse_from_rfc3339(&deadline) else {
            return RetryEligibilityView {
                eligible: false,
                denial_reason: Some("invalidDeadline".to_string()),
            };
        };
        if Utc::now() > deadline.with_timezone(&Utc) {
            return RetryEligibilityView {
                eligible: false,
                denial_reason: Some("deadlineClosed".to_string()),
            };
        }
    }
    RetryEligibilityView {
        eligible: true,
        denial_reason: None,
    }
}

fn selected_skill_ids_from_metadata(metadata: Option<&str>) -> Vec<String> {
    let Some(metadata) = metadata.map(str::trim).filter(|value| !value.is_empty()) else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(metadata) else {
        return Vec::new();
    };

    value
        .get("selected_skill_ids")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn build_pending_turn(
    store: &gents_desktop_core::client::ClientStore,
    agent_did: Option<&str>,
    session_id: &str,
    request_id: &str,
) -> Option<PendingTurnView> {
    let request = store.requests.iter().find(|row| {
        row.request_id == request_id
            && row.session_id.as_deref() == Some(session_id)
            && agent_did.is_none_or(|agent_did| request_matches_agent(row, agent_did, false))
    })?;
    if gents::lifecycle::is_background_completion_request(request.metadata.as_deref()) {
        return None;
    }

    let lifecycle_state = normalize_optional(request.lifecycle_state.as_deref());
    let content = normalize_optional(request.content.as_deref())?;
    let transcript = agent_did.map_or_else(
        || store.transcript(session_id),
        |agent_did| store.transcript_for_agent(session_id, agent_did),
    );
    let requests_by_id = store
        .requests
        .iter()
        .map(|request| (request.request_id.as_str(), request))
        .collect::<HashMap<_, _>>();
    let messages = transcript
        .messages
        .into_iter()
        .map(|row| {
            let role = normalize_optional(row.role.as_deref());
            let body = normalize_optional(row.content.as_deref());
            let presentation = role
                .as_deref()
                .zip(body.as_deref())
                .map(|(role, content)| present_persisted_message(role, content));

            MessageView {
                message_key: row.message_key.clone(),
                request_id: row.request_id.clone(),
                sequence: row.sequence,
                role,
                content: body,
                display_role: presentation
                    .as_ref()
                    .map(|presentation| presentation.role.label().to_ascii_lowercase()),
                display_content: presentation.as_ref().and_then(|presentation| {
                    normalize_optional(Some(presentation.body_markdown.as_str()))
                }),
                reasoning: None,
                has_tool_calls: false,
                has_tool_results: false,
                runtime_control: message_is_runtime_control(row, &requests_by_id),
                timestamp: normalize_optional(row.timestamp.as_deref()),
            }
        })
        .collect::<Vec<_>>();

    if has_materialized_user_owner(&messages, request_id) {
        return None;
    }

    Some(PendingTurnView {
        request_id: request.request_id.clone(),
        content: content.to_string(),
        selected_skill_ids: selected_skill_ids_from_metadata(request.metadata.as_deref()),
        lifecycle_state,
        created_at: normalize_optional(request.created_at.as_deref()),
    })
}

#[cfg(test)]
mod retry_eligibility_tests {
    use super::*;

    fn request(origin: &str, retry_count: i64, max_retries: i64) -> AgentRequestRow {
        serde_json::from_value(serde_json::json!({
            "request_id": "request-1",
            "agent_did": "did:test:agent",
            "requester_did": "did:test:requester",
            "session_id": "session-1",
            "content": "try this",
            "status": "error",
            "lifecycle_state": "failed",
            "execution_origin": origin,
            "retry_count": retry_count,
            "max_retries": max_retries
        }))
        .expect("request")
    }

    #[test]
    fn projects_only_authoritatively_eligible_interactive_retry() {
        let interactive = request("interactive", 0, 3);
        assert!(project_retry_eligibility(Some(&interactive)).eligible);

        let scheduled = request("scheduled", 0, 3);
        let scheduled = project_retry_eligibility(Some(&scheduled));
        assert!(!scheduled.eligible);
        assert_eq!(
            scheduled.denial_reason.as_deref(),
            Some("nonInteractiveOrigin")
        );

        let exhausted = request("interactive", 3, 3);
        let exhausted = project_retry_eligibility(Some(&exhausted));
        assert!(!exhausted.eligible);
        assert_eq!(
            exhausted.denial_reason.as_deref(),
            Some("retryBudgetExhausted")
        );
    }
}
