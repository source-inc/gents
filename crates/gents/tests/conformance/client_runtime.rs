use super::*;

#[test]
fn generated_client_shell_cases_cover_shell_projection_contracts() {
    let ephemeral = lean_client_shell_case("new_conversation_is_ephemeral");
    assert_eq!(ephemeral.post_selection_session, None);
    assert_eq!(ephemeral.post_workflow_kind.as_str(), "idle");

    let submitted = lean_client_shell_case("submitted_request_selects_session");
    assert_eq!(submitted.input.as_str(), "mutation.submitted");
    assert_eq!(submitted.post_selection_session, Some(1));
    assert_eq!(submitted.post_workflow_kind.as_str(), "awaiting");

    let snapshot = lean_client_shell_case("snapshot_preserves_selection");
    assert_eq!(snapshot.input.as_str(), "snapshot");
    assert!(snapshot.selection_preserved);
    assert_eq!(
        snapshot.pre_selection_session,
        snapshot.post_selection_session
    );

    let advanced = lean_client_shell_case("snapshot_workflow_advances_on_matching_request");
    assert!(advanced.workflow_advanced);
    assert_eq!(advanced.pre_workflow_kind.as_str(), "awaiting");
    assert_eq!(advanced.post_workflow_kind.as_str(), "idle");
    assert_eq!(advanced.pre_workflow_request, Some(101));
    assert_eq!(advanced.post_workflow_request, None);

    let stale = lean_client_shell_case("awaiting_stale_request_observation");
    assert!(!stale.workflow_advanced);
    assert_eq!(
        stale.property.as_str(),
        "awaiting_stale_request_observation"
    );
    assert_eq!(stale.post_workflow_kind.as_str(), "awaiting");
    assert_eq!(
        stale.frontend_expected_send_blocked_reason.as_deref(),
        Some("waitingForRequestObservation")
    );

    let matching = lean_client_shell_case("awaiting_matching_request_observation");
    assert_eq!(
        matching.frontend_expected_workflow_kind.as_str(),
        "turnInProgress"
    );
    assert_eq!(
        matching.frontend_expected_send_blocked_reason.as_deref(),
        Some("awaitingTurnTerminality")
    );

    let switched = lean_client_shell_case("stale_workflow_after_session_switch");
    assert!(switched.workflow_advanced);
    assert_eq!(switched.pre_selection_session, Some(1));
    assert_eq!(switched.post_selection_session, Some(2));
    assert_eq!(switched.post_workflow_kind.as_str(), "idle");
    assert_eq!(switched.frontend_expected_send_status.as_str(), "ready");

    let transport = lean_client_shell_case("transport_noop");
    assert!(transport.transport_noop);
    assert!(transport.selection_preserved);
    assert!(!transport.workflow_advanced);

    for (name, reason) in [
        ("blocked_submit_client_offline", "clientOffline"),
        ("blocked_submit_agent_not_selected", "agentNotSelected"),
        ("blocked_submit_composer_empty", "composerEmpty"),
        ("blocked_submit_mutation_in_flight", "mutationInFlight"),
        ("blocked_submit_awaiting_observation", "awaitingObservation"),
        ("blocked_submit_session_absent", "sessionAbsent"),
        ("blocked_submit_nonterminal_turn", "awaitingTurnTerminality"),
    ] {
        let case = lean_client_shell_case(name);
        assert!(!case.can_submit_before, "{name} should gate submit");
        assert_eq!(case.send_decision.as_str(), "blocked");
        assert_eq!(case.send_blocked_reason.as_deref(), Some(reason));
        assert_eq!(case.frontend_expected_send_status.as_str(), "disabled");
    }

    let terminal = lean_client_shell_case("terminal_follow_up_allowed");
    assert!(terminal.can_submit_before);
    assert_eq!(terminal.send_decision.as_str(), "ready");
    assert_eq!(terminal.frontend_expected_send_status.as_str(), "ready");

    let no_summary = lean_client_shell_case("terminal_follow_up_session_snapshot_without_summary");
    assert!(no_summary.can_submit_before);
    assert_eq!(no_summary.frontend_expected_send_status.as_str(), "ready");
    assert_eq!(no_summary.frontend_expected_active_request_id, Some(101));
}

#[test]
fn generated_runtime_reconcile_cases_pin_generation_and_admission_contract() {
    let publish = lean_runtime_reconcile_case("publish_changed_snapshot");
    assert!(publish.legal);
    assert_eq!(publish.action.as_str(), "publish");
    assert_eq!(publish.pre_phase.as_str(), "applying");
    assert_eq!(publish.post_phase.as_str(), "idle");
    assert_eq!(
        publish.pre_active_generation + 1,
        publish.post_active_generation
    );
    assert_eq!(
        publish.pre_router_generation,
        publish.post_router_generation
    );
    assert_eq!(
        publish.pre_ready_generation_count + 1,
        publish.post_ready_generation_count
    );
    assert_eq!(
        publish.pre_live_generation_count + 1,
        publish.post_live_generation_count
    );

    let router = lean_runtime_reconcile_case("router_observe_published_generation");
    assert!(router.legal);
    assert_eq!(router.pre_phase.as_str(), "idle");
    assert_eq!(router.post_phase.as_str(), "idle");
    assert_eq!(router.pre_active_generation, router.post_active_generation);
    assert_eq!(router.post_router_generation, router.post_active_generation);

    let accept = lean_runtime_reconcile_case("accept_request_after_router_observe");
    assert!(accept.legal);
    assert_eq!(accept.pre_phase.as_str(), "idle");
    assert_eq!(accept.post_phase.as_str(), "idle");
    assert_eq!(accept.pre_in_flight_count + 1, accept.post_in_flight_count);
    assert_eq!(accept.tracked_request_id, 500);
    assert_eq!(accept.tracked_session_id, 100);
    assert_eq!(
        accept.tracked_request_generation,
        accept.post_router_generation
    );
    assert_eq!(accept.tracked_request_session, accept.tracked_session_id);
    assert_eq!(
        accept.tracked_request_behavior,
        accept.tracked_session_behavior
    );

    let replay = lean_runtime_reconcile_case("replayed_request_is_not_accepted_twice");
    assert!(!replay.legal);
    assert_eq!(replay.action.as_str(), "acceptRequest");

    let retire = lean_runtime_reconcile_case("retire_unobserved_generation");
    assert!(retire.legal);
    assert_eq!(
        retire.pre_live_generation_count - 1,
        retire.post_live_generation_count
    );
    assert_eq!(
        retire.pre_ready_generation_count - 1,
        retire.post_ready_generation_count
    );

    let finish = lean_runtime_reconcile_case("finish_request_releases_generation");
    assert!(finish.legal);
    assert_eq!(finish.action.as_str(), "finishRequest");
    assert_eq!(finish.pre_in_flight_count, finish.post_in_flight_count + 1);
    assert_eq!(finish.tracked_request_id, 500);
    assert_eq!(finish.pre_active_generation, finish.post_active_generation);

    let apply_failed = lean_runtime_reconcile_case("apply_failed_clears_pending");
    assert!(apply_failed.legal);
    assert_eq!(apply_failed.action.as_str(), "applyFailed");
    assert_eq!(apply_failed.pre_phase.as_str(), "applying");
    assert_eq!(apply_failed.post_phase.as_str(), "idle");
    assert_eq!(
        apply_failed.pre_active_generation,
        apply_failed.post_active_generation
    );

    let covered = [
        "publish_changed_snapshot",
        "router_observe_published_generation",
        "accept_request_after_router_observe",
        "replayed_request_is_not_accepted_twice",
        "retire_unobserved_generation",
        "finish_request_releases_generation",
        "apply_failed_clears_pending",
    ]
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>();
    let emitted = lean_runtime_reconcile_cases()
        .iter()
        .map(|case| case.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        emitted, covered,
        "runtime-reconcile case set drifted from this consumer's coverage"
    );
}
