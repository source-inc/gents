use std::collections::BTreeMap;

use crate::lean_vocab_test::{
    lean_response_outcome_cases, lean_response_persistence_cut_cases,
    lean_response_recovery_cut_cases,
};

pub(crate) fn generated_cases_pin_exact_immutable_response_outcomes() {
    let cases = lean_response_outcome_cases()
        .iter()
        .map(|case| (case.name.as_str(), case))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(cases.len(), 9);

    let complete = cases["complete_exact_message_fresh"];
    assert_eq!(complete.kind, "complete");
    assert_eq!(complete.publish_outcome, "fresh");
    assert!(complete.has_final_message);
    assert_eq!(complete.final_message_role.as_deref(), Some("assistant"));
    assert_eq!(complete.resulting_fact_count, 1);

    let replay = cases["complete_identical_replay_idempotent"];
    assert_eq!(replay.publish_outcome, "idempotent");
    assert_eq!(replay.visible_sibling_count, 1);
    assert_eq!(replay.resulting_fact_count, 1);

    for name in [
        "complete_different_message_conflict",
        "complete_missing_message_rejected",
        "complete_user_message_rejected",
        "same_request_doc_different_version_conflict",
        "visible_sibling_set_rejected",
    ] {
        assert_eq!(cases[name].publish_outcome, "rejected", "case {name}");
    }

    assert_eq!(
        cases["error_without_message_fresh"].publish_outcome,
        "fresh"
    );
    assert_eq!(
        cases["interrupted_with_partial_message_fresh"].publish_outcome,
        "fresh"
    );

    let cuts = lean_response_persistence_cut_cases();
    assert_eq!(cuts.len(), 4);
    assert_eq!(cuts[0].pre_cut, "streaming");
    assert_eq!(cuts[0].post_cut, "message_durable");
    assert_eq!(cuts[1].post_cut, "outcome_durable");
    assert_eq!(cuts[2].post_cut, "request_terminal");
    assert!(cuts[2].request_terminal);
    assert_eq!(cuts[3].post_cut, "live_superseded");
    assert_eq!(cuts[3].live_stage, "superseded");
    assert_eq!(cuts[3].outcome_count, 1);

    let recovery = lean_response_recovery_cut_cases()
        .iter()
        .map(|case| (case.name.as_str(), case))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(recovery.len(), 4);

    let fresh = recovery["missing_response_exact_claim_publishes_failure"];
    assert!(!fresh.response_present);
    assert!(fresh.provenance_reconstructed);
    assert_eq!(fresh.source_cid, fresh.claim_parent_cid);
    assert_ne!(fresh.source_cid, fresh.claim_cid);
    assert_eq!(fresh.publish_outcome, "fresh");
    assert_eq!(fresh.terminalized_at_source, "recovery_decision");
    assert!(!fresh.request_terminal);
    assert_eq!(fresh.outcome_count, 1);

    let replay = recovery["missing_response_identical_retry_is_idempotent"];
    assert!(replay.provenance_reconstructed);
    assert_eq!(replay.publish_outcome, "idempotent");
    assert_eq!(replay.terminalized_at_source, "persisted_outcome");
    assert_eq!(replay.outcome_count, 1);

    let wrong_parent = recovery["missing_response_wrong_claim_parent_rejected"];
    assert!(!wrong_parent.provenance_reconstructed);
    assert_eq!(wrong_parent.publish_outcome, "rejected");
    assert_eq!(wrong_parent.terminalized_at_source, "none");
    assert_eq!(wrong_parent.outcome_count, 0);

    let terminal = recovery["missing_response_terminalizes_only_after_outcome"];
    assert_eq!(terminal.publish_outcome, "idempotent");
    assert_eq!(terminal.terminalized_at_source, "persisted_outcome");
    assert!(terminal.request_terminal);
    assert_eq!(terminal.outcome_count, 1);
}
