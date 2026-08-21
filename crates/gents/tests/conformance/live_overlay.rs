use gents_protocol::client_protocol::ClientTurnState;
use gents_protocol::timeline::{has_durable_user_owner, DurableUserOwnerInput};

use crate::lean_vocab_test::{
    lean_live_overlay_cases, lean_pending_user_turn_cases, lean_request_progress_cases,
    LeanLiveOverlayCase, LeanPendingUserTurnCase, LeanRequestProgressCase,
};

fn parse_turn(label: &str) -> Option<ClientTurnState> {
    match label {
        "waitingForClaim" => Some(ClientTurnState::WaitingForClaim),
        "streaming" => Some(ClientTurnState::Streaming),
        "completed" => Some(ClientTurnState::Completed),
        "failed" => Some(ClientTurnState::Failed),
        "superseded" => Some(ClientTurnState::Superseded),
        "interrupted" => Some(ClientTurnState::Interrupted),
        _ => None,
    }
}

fn should_show_overlay(
    response_status: &str,
    materialized: bool,
    has_durable_owner: bool,
    turn: Option<ClientTurnState>,
    has_content: bool,
    has_reasoning: bool,
) -> bool {
    if materialized || has_durable_owner {
        return false;
    }
    if response_status == "complete" || response_status == "error" {
        return false;
    }
    let Some(turn) = turn else {
        return false;
    };
    if turn.is_terminal() {
        return false;
    }
    let renderable = matches!(
        turn,
        ClientTurnState::WaitingForClaim | ClientTurnState::Streaming
    );
    if !renderable {
        return false;
    }
    has_content || has_reasoning
}

#[test]
fn live_overlay_cases_match_lean_table() {
    let cases: &[LeanLiveOverlayCase] = lean_live_overlay_cases();
    assert!(!cases.is_empty(), "Lean LiveOverlay case table is empty");

    for case in cases {
        let actual = should_show_overlay(
            &case.response_status,
            case.materialized,
            case.has_durable_owner,
            parse_turn(&case.turn_label),
            case.has_content,
            case.has_reasoning,
        );
        assert_eq!(
            actual,
            case.expect_overlay,
            "case {name:?} expected overlay={expected}, got {actual}",
            name = case.name,
            expected = case.expect_overlay,
        );

        if case.turn_terminal {
            assert!(
                !case.expect_overlay,
                "case {:?} marks turn as terminal but expects overlay; contract violated",
                case.name,
            );
        }

        let _ = case.preceding_tool_calls;
    }
}

fn request_progress(lifecycle_state: &str) -> Option<(&'static str, bool)> {
    match lifecycle_state {
        "pending" => Some(("Queued", true)),
        "claimed" => Some(("Claimed", true)),
        "processing" => Some(("Working", true)),
        "inputRequired" => Some(("Waiting for input", false)),
        "completed" => Some(("Completed", false)),
        "failed" => Some(("Failed", false)),
        "superseded" => Some(("Superseded", false)),
        "dead" => Some(("Expired", false)),
        "interrupted" => Some(("Interrupted", false)),
        _ => None,
    }
}

#[test]
fn request_progress_cases_match_lean_table() {
    let cases: &[LeanRequestProgressCase] = lean_request_progress_cases();
    assert_eq!(cases.len(), 9, "every request lifecycle state is projected");

    for case in cases {
        let actual = request_progress(&case.lifecycle_state)
            .unwrap_or_else(|| panic!("case {:?} has unknown lifecycle state", case.name));
        assert_eq!(actual.0, case.label, "case {:?} label drifted", case.name);
        assert_eq!(
            actual.1, case.animated,
            "case {:?} animation drifted",
            case.name
        );
    }
}

#[test]
fn pending_user_turn_cases_match_lean_table() {
    let cases: &[LeanPendingUserTurnCase] = lean_pending_user_turn_cases();
    assert_eq!(cases.len(), 3, "ownership cases should stay exhaustive");

    for case in cases {
        let mut messages = (0..case.unrelated_user_turns)
            .map(|_| DurableUserOwnerInput {
                request_id: Some("unrelated-request"),
                is_user: true,
                has_visible_content: true,
                runtime_control: false,
            })
            .collect::<Vec<_>>();
        if case.has_durable_user_owner {
            messages.push(DurableUserOwnerInput {
                request_id: Some("request-under-test"),
                is_user: true,
                has_visible_content: true,
                runtime_control: false,
            });
        }
        let actual = !has_durable_user_owner(&messages, "request-under-test");
        assert_eq!(
            actual, case.expect_pending_turn,
            "case {:?} pending projection drifted with {} unrelated turns",
            case.name, case.unrelated_user_turns
        );
    }
}
