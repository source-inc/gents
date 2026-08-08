use gents::agent::p2p_reconcile::templates::{
    builtin_templates, resolve_template, scope_filter, Delivery, FilterPredicate, Scope,
    AGENT_DIRECTORY_COLLECTION,
};

#[test]
fn resolve_template_is_total_over_catalog_and_id_faithful() {
    for t in builtin_templates() {
        let resolved = resolve_template(t.id).expect("catalog id resolves");
        assert_eq!(resolved.id, t.id, "resolution must not alias ids");
    }
    assert!(
        resolve_template("definitely-not-a-template").is_none(),
        "unknown id resolves to none"
    );
}

#[test]
fn conversation_scope_filters_transcript_and_grants_unfiltered_config() {
    let t = resolve_template("conversation").expect("conversation in catalog");
    assert_eq!(t.delivery, Delivery::Push);
    assert!(matches!(t.scope, Scope::PerCollection(_)));

    let filter = scope_filter(&t.scope, t.collections, "did:key:bob", "did:key:alice");
    for col in [
        "AgentRequest",
        "AgentResponse",
        "AgentResponseOutcome",
        "AgentMessage",
        "AgentToolCall",
        "AgentToolResult",
        "AgentToolApproval",
        "AgentSession",
        "AgentConversation",
        "CompactionEntry",
        "BearerPairingReady",
    ] {
        let pred = filter.get(col).expect("transcript collection filter");
        let expected_field = if col == "BearerPairingReady" {
            "claimant_did"
        } else {
            "requester_did"
        };
        assert_eq!(pred.field, expected_field);
        assert_eq!(pred.value, "did:key:bob");
    }
    for col in [
        "AgentBehavior",
        "ToolSelection",
        "InferenceBackend",
        "InferenceProfile",
        "ToolServiceRegistry",
        "Skill",
    ] {
        assert!(t.collections.contains(&col));
        assert!(!filter.contains_key(col), "config {col} must be unfiltered");
    }
}

#[test]
fn conversation_scope_excludes_another_requester_on_the_same_agent() {
    let t = resolve_template("conversation").expect("conversation in catalog");
    let phone_filter = scope_filter(&t.scope, t.collections, "did:key:phone", "did:key:amy");
    let predicate = phone_filter
        .get("AgentRequest")
        .expect("request filter is present");

    let phone_request = ("did:key:amy", Some("did:key:phone"));
    let classifier_request = ("did:key:amy", None);
    assert_eq!(predicate.field, "requester_did");
    assert_eq!(phone_request.1, Some(predicate.value.as_str()));
    assert_ne!(classifier_request.1, Some(predicate.value.as_str()));
}

#[test]
fn machine_scope_covers_conversation_and_home_owned_directory() {
    let template = resolve_template("machine").expect("machine in catalog");
    let filters = scope_filter(
        &template.scope,
        template.collections,
        "did:key:phone",
        "did:key:issuer",
    );

    for collection in [
        "AgentRequest",
        "AgentResponse",
        "AgentResponseOutcome",
        "AgentMessage",
        "AgentToolCall",
        "AgentToolResult",
        "AgentToolApproval",
        "AgentSession",
        "AgentConversation",
        "CompactionEntry",
        "BearerPairingReady",
    ] {
        let predicate = filters.get(collection).expect("conversation filter");
        let expected_field = if collection == "BearerPairingReady" {
            "claimant_did"
        } else {
            "requester_did"
        };
        assert_eq!(predicate.field, expected_field);
        assert_eq!(predicate.value, "did:key:phone");
    }
    assert_eq!(
        filters.get(AGENT_DIRECTORY_COLLECTION),
        Some(&FilterPredicate {
            field: "source_did".to_string(),
            value: "did:key:issuer".to_string(),
        })
    );
}

#[test]
fn unscoped_scope_resolves_to_no_filter() {
    for id in ["agent-config", "backup"] {
        let t = resolve_template(id).expect("template in catalog");
        assert!(matches!(t.scope, Scope::Unscoped));
        assert!(
            scope_filter(&t.scope, t.collections, "did:key:bob", "did:key:alice").is_empty(),
            "{id} must be unfiltered"
        );
    }
}

#[test]
fn subagent_templates_resolve_to_exact_directional_filters() {
    const RETURN_PROJECTION: &[&str] = &[
        "AgentRequest",
        "AgentResponse",
        "AgentResponseOutcome",
        "AgentMessage",
        "AgentToolCall",
        "AgentToolApproval",
    ];

    let coord = resolve_template("subagent-coordinator").expect("coordinator template");
    assert_eq!(coord.delivery, Delivery::Push);
    assert_eq!(coord.collections, &["AgentToolCall"]);
    let coord_filter = scope_filter(
        &coord.scope,
        coord.collections,
        "did:key:host",
        "did:key:coord",
    );
    assert_eq!(coord_filter.len(), 1);
    assert!(!coord_filter.contains_key("AgentRequest"));
    assert_eq!(
        coord_filter.get("AgentToolCall"),
        Some(&FilterPredicate {
            field: "spawn_target_did".to_string(),
            value: "did:key:host".to_string(),
        })
    );

    let host = resolve_template("subagent-host").expect("host template");
    assert_eq!(host.delivery, Delivery::Push);
    assert_eq!(host.collections, RETURN_PROJECTION);
    let host_filter = scope_filter(
        &host.scope,
        host.collections,
        "did:key:coord",
        "did:key:host",
    );
    assert_eq!(host_filter.len(), RETURN_PROJECTION.len());
    assert_eq!(
        host_filter.get("AgentRequest"),
        Some(&FilterPredicate {
            field: "requester_did".to_string(),
            value: "did:key:coord".to_string(),
        })
    );
    for collection in RETURN_PROJECTION {
        assert_eq!(
            host_filter.get(*collection),
            Some(&FilterPredicate {
                field: "requester_did".to_string(),
                value: "did:key:coord".to_string(),
            }),
            "unexpected host filter for {collection}"
        );
    }
    for local_collection in [
        "AgentToolResult",
        "AgentSession",
        "AgentConversation",
        "CompactionEntry",
    ] {
        assert!(!host.collections.contains(&local_collection));
        assert!(!host_filter.contains_key(local_collection));
    }

    for predicate in coord_filter.values().chain(host_filter.values()) {
        assert!(
            predicate.value == "did:key:coord" || predicate.value == "did:key:host",
            "subagent filters must not include third-party DIDs"
        );
    }
}

#[test]
fn subagent_host_message_filter_excludes_unrelated_host_history() {
    let host = resolve_template("subagent-host").expect("host template");
    let filter = scope_filter(
        &host.scope,
        host.collections,
        "did:key:coord",
        "did:key:host",
    );
    let predicate = filter.get("AgentMessage").expect("message filter");

    let child_requester_did = Some("did:key:coord");
    let unrelated_requester_did: Option<&str> = None;
    assert_eq!(predicate.field, "requester_did");
    assert_eq!(child_requester_did, Some(predicate.value.as_str()));
    assert_ne!(unrelated_requester_did, Some(predicate.value.as_str()));
}

#[test]
fn sixteen_peer_request_wave_is_reduced_to_one_target() {
    let coordinator = resolve_template("subagent-coordinator").expect("coordinator template");
    let host = resolve_template("subagent-host").expect("host template");
    let requester_did = "did:key:coordinator-07";

    let current_parent_request_matches = (0..16)
        .filter(|index| {
            let host_did = format!("did:key:host-{index:02}");
            scope_filter(
                &coordinator.scope,
                coordinator.collections,
                &host_did,
                requester_did,
            )
            .contains_key("AgentRequest")
        })
        .count();
    assert_eq!(current_parent_request_matches, 0);

    let routed_child_request_matches = (0..16)
        .filter(|index| {
            let peer_did = format!("did:key:coordinator-{index:02}");
            scope_filter(&host.scope, host.collections, &peer_did, "did:key:host")
                .get("AgentRequest")
                .is_some_and(|predicate| {
                    predicate.field == "requester_did" && predicate.value == requester_did
                })
        })
        .count();
    assert_eq!(routed_child_request_matches, 1);
}

#[test]
fn app_collections_template_is_unscoped_replicate_byo() {
    let t = resolve_template("app-collections").expect("app-collections in catalog");
    assert_eq!(t.id, "app-collections");
    assert!(matches!(t.delivery, Delivery::Replicate));
    assert!(matches!(t.scope, Scope::Unscoped));
    assert!(
        t.collections.is_empty(),
        "app-collections carries no fixed collections; the row supplies them"
    );
    let f = scope_filter(
        &t.scope,
        &["ChangeProposed"],
        "did:key:bob",
        "did:key:alice",
    );
    assert!(f.is_empty(), "unscoped app-collections must not filter");
}
