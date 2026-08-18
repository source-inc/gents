//! Conformance fence for `Proofs/SessionHydration`.

use std::collections::BTreeSet;

use gents::agent::p2p_reconcile::session_hydration::{
    apply_hydration_step, decide_hydration, HydrationCatalog, HydrationDocument, HydrationOutcome,
    HydrationRequest, HydrationState, HydrationVerdict, SessionOwner,
};

use crate::lean_vocab_test::lean_session_hydration_decision_cases;

fn request() -> HydrationRequest {
    HydrationRequest::from_row(
        "peer-1:session-1".into(),
        "did:key:requester-1".into(),
        "did:key:agent-1".into(),
        "session-1".into(),
    )
    .expect("valid hydration key")
}

fn document(id: &str, requester: &str, agent: &str, session: &str) -> HydrationDocument {
    HydrationDocument {
        collection: "AgentMessage".into(),
        doc_id: id.into(),
        requester_did: requester.into(),
        agent_did: agent.into(),
        session_id: session.into(),
    }
}

fn document_in_collection(
    collection: &str,
    id: &str,
    requester: &str,
    agent: &str,
    session: &str,
) -> HydrationDocument {
    HydrationDocument {
        collection: collection.into(),
        doc_id: id.into(),
        requester_did: requester.into(),
        agent_did: agent.into(),
        session_id: session.into(),
    }
}

fn admitted_catalog() -> HydrationCatalog {
    HydrationCatalog {
        paired_peer_ids: BTreeSet::from(["peer-1".into()]),
        active_member_dids: BTreeSet::from(["did:key:requester-1".into()]),
        sessions: BTreeSet::from([SessionOwner {
            session_id: "session-1".into(),
            requester_did: "did:key:requester-1".into(),
            agent_did: "did:key:agent-1".into(),
        }]),
        documents: BTreeSet::from([
            document(
                "owned",
                "did:key:requester-1",
                "did:key:agent-1",
                "session-1",
            ),
            document(
                "foreign-requester",
                "did:key:requester-2",
                "did:key:agent-1",
                "session-1",
            ),
            document(
                "foreign-session",
                "did:key:requester-1",
                "did:key:agent-1",
                "session-2",
            ),
            document_in_collection(
                "AgentSession",
                "wrong-collection",
                "did:key:requester-1",
                "did:key:agent-1",
                "session-1",
            ),
        ]),
    }
}

/// Mirrors Lean `decideAdmits_agrees`, `hydration_request_grants_nothing`,
/// and `session_ownership_required`.
#[test]
fn admission_matrix_matches_lean_conjuncts() {
    let req = request();
    let good = admitted_catalog();
    assert!(matches!(
        decide_hydration(&req, &good),
        HydrationVerdict::Admit(_)
    ));

    let mut cases = Vec::new();
    let mut unpaired = good.clone();
    unpaired.paired_peer_ids.clear();
    cases.push(unpaired);
    let mut inactive = good.clone();
    inactive.active_member_dids.clear();
    cases.push(inactive);
    let mut unowned = good;
    unowned.sessions.clear();
    cases.push(unowned);

    for catalog in cases {
        assert!(matches!(
            decide_hydration(&req, &catalog),
            HydrationVerdict::Reject(_)
        ));
        let state = HydrationState::default();
        let next = apply_hydration_step(&req, &catalog, &state);
        assert_eq!(next.delivered, state.delivered);
        assert_eq!(
            next.terminals.get(&req.request_key),
            Some(&(HydrationOutcome::Rejected, 0))
        );
    }
}

#[test]
fn generated_session_hydration_cases_match_decision_core() {
    let req = request();
    let base = admitted_catalog();
    let cases = lean_session_hydration_decision_cases();
    assert_eq!(cases.len(), 4);

    for case in cases {
        let mut catalog = base.clone();
        if !case.paired {
            catalog.paired_peer_ids.clear();
        }
        if !case.active_member {
            catalog.active_member_dids.clear();
        }
        if !case.owns_session {
            catalog.sessions.clear();
        }
        match decide_hydration(&req, &catalog) {
            HydrationVerdict::Admit(documents) => {
                assert!(case.expected_admit, "{} unexpectedly admitted", case.name);
                assert_eq!(
                    documents.len(),
                    case.expected_selected_count,
                    "{}",
                    case.name
                );
            }
            HydrationVerdict::Reject(_) => {
                assert!(!case.expected_admit, "{} unexpectedly rejected", case.name);
            }
        }
    }
}

/// Mirrors Lean `selected_tenancy_sound` and `selected_session_sound`.
#[test]
fn admitted_selection_is_exactly_requester_agent_session_scoped() {
    let req = request();
    let HydrationVerdict::Admit(documents) = decide_hydration(&req, &admitted_catalog()) else {
        panic!("request should be admitted");
    };
    assert_eq!(
        documents,
        BTreeSet::from([document(
            "owned",
            "did:key:requester-1",
            "did:key:agent-1",
            "session-1",
        )])
    );
}

/// Mirrors Lean `pending_reaches_terminal`, `applyStep_idempotent`, and
/// `pairing_noninterference`.
#[test]
fn serve_is_terminal_idempotent_and_pairing_neutral() {
    let req = request();
    let catalog = admitted_catalog();
    let state = HydrationState {
        pairing_state: BTreeSet::from(["stable-machine-filter".into()]),
        ..Default::default()
    };
    let once = apply_hydration_step(&req, &catalog, &state);
    let twice = apply_hydration_step(&req, &catalog, &once);

    assert_eq!(once, twice);
    assert_eq!(once.pairing_state, state.pairing_state);
    assert_eq!(
        once.terminals.get(&req.request_key),
        Some(&(HydrationOutcome::Served, 1))
    );
}

#[test]
fn request_key_binds_peer_and_session() {
    assert!(HydrationRequest::from_row(
        "peer-1:other-session".into(),
        "did:key:requester-1".into(),
        "did:key:agent-1".into(),
        "session-1".into(),
    )
    .is_err());
}
