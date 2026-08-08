//! Phase A conformance: baseline registration, idempotence, single-version DAG.

use std::{collections::BTreeSet, sync::Arc};

use defra_node::EmbeddedNode;
use gents_migration::{
    ensure_migrations, ensure_migrations_dynamic, ensure_migrations_with_registry,
    BaselineCollectionOwned, CollectionExpectation, DynamicRegistry, Error, MigrationStep,
    Registry,
};

#[test]
fn default_baseline_matches_ordered_protocol_catalog() {
    let actual = gents_migration::DEFAULT_BASELINE
        .iter()
        .map(|entry| (entry.name, entry.sdl))
        .collect::<Vec<_>>();
    let expected = gents_protocol::schemas::RUNTIME_COLLECTION_NAMES
        .iter()
        .copied()
        .zip(gents_protocol::schemas::RUNTIME_ALL.iter().copied())
        .chain(
            gents_protocol::schemas::ALL_COLLECTION_NAMES
                .iter()
                .copied()
                .zip(gents_protocol::schemas::ALL.iter().copied()),
        )
        .collect::<Vec<_>>();

    assert_eq!(actual.len(), expected.len());
    for ((actual_name, actual_sdl), (expected_name, expected_sdl)) in
        actual.iter().zip(expected.iter())
    {
        assert_eq!(actual_name, expected_name);
        if *actual_name == gents_protocol::schemas::INFERENCE_PROFILE_NAME {
            assert_ne!(actual_sdl, expected_sdl, "changed schema must be frozen");
        } else {
            assert_eq!(actual_sdl, expected_sdl, "baseline drift for {actual_name}");
        }
    }
    assert!(gents_migration::DEFAULT_STEPS.iter().any(|step| matches!(
        step,
        MigrationStep::PatchVersioned { collection, .. }
            if *collection == gents_protocol::schemas::INFERENCE_PROFILE_NAME
    )));
}

async fn fresh_node() -> Arc<EmbeddedNode> {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = EmbeddedNode::builder()
        .data_path(dir.path())
        .build()
        .await
        .expect("build node");
    // Keep tempdir alive for the node lifetime by leaking (test process short).
    std::mem::forget(dir);
    Arc::new(node)
}

#[test]
fn default_baseline_covers_every_protocol_collection_once() {
    let baseline_names = gents_migration::DEFAULT_BASELINE
        .iter()
        .map(|entry| entry.name)
        .collect::<BTreeSet<_>>();
    let protocol_names = gents_protocol::schemas::ALL_COLLECTION_NAMES
        .iter()
        .chain(gents_protocol::schemas::RUNTIME_COLLECTION_NAMES.iter())
        .copied()
        .collect::<BTreeSet<_>>();

    assert_eq!(
        gents_migration::DEFAULT_BASELINE.len(),
        baseline_names.len(),
        "migration baseline must not contain duplicate collections"
    );
    assert!(
        gents_migration::DEFAULT_BASELINE
            .iter()
            .all(|entry| entry.expected_version.is_some()),
        "production migration baseline must pin every root version"
    );
    assert_eq!(
        baseline_names, protocol_names,
        "migration baseline must cover the full protocol schema catalog"
    );
}

#[tokio::test]
async fn ensure_migrations_registers_baseline_and_is_idempotent() {
    let node = fresh_node().await;
    let report1 = ensure_migrations(node.as_ref())
        .await
        .expect("first ensure");
    assert!(
        report1.baseline_registered + report1.baseline_already_present
            >= gents_migration::DEFAULT_BASELINE.len(),
        "expected full baseline coverage, got {report1:?}"
    );
    assert_eq!(report1.steps_applied, gents_migration::DEFAULT_STEPS.len());

    let report2 = ensure_migrations(node.as_ref())
        .await
        .expect("second ensure");
    assert_eq!(report2.steps_applied, 0);
    assert_eq!(
        report2.steps_already_current,
        gents_migration::DEFAULT_STEPS.len()
    );
    assert!(
        report2.baseline_already_present >= gents_migration::DEFAULT_BASELINE.len()
            || report2.baseline_registered + report2.baseline_already_present
                >= gents_migration::DEFAULT_BASELINE.len(),
        "re-run should be cheap/idempotent: {report2:?}"
    );

    // Every managed collection is present and active.
    for entry in gents_migration::DEFAULT_BASELINE {
        let cv = node
            .get_collection(entry.name)
            .expect("get_collection")
            .unwrap_or_else(|| panic!("missing collection {}", entry.name));
        assert!(cv.is_active, "{} should be active", entry.name);
        assert!(
            !cv.is_placeholder,
            "{} should not be placeholder",
            entry.name
        );
    }

    node.shutdown().await;
}

#[tokio::test]
async fn inference_profile_reasoning_effort_migration_preserves_existing_document() {
    let node = fresh_node().await;
    let baseline = gents_migration::DEFAULT_BASELINE
        .iter()
        .find(|entry| entry.name == gents_protocol::schemas::INFERENCE_PROFILE_NAME)
        .expect("InferenceProfile baseline");
    node.add_schema(baseline.sdl)
        .await
        .expect("register frozen profile baseline");

    let create = r#"mutation {
        create_InferenceProfile(input: {
            profile_id: "existing-profile"
            display_name: "Existing"
        }) { profile_id display_name }
    }"#;
    let response = node.execute(create).await;
    assert!(
        !response.has_errors(),
        "create profile: {:?}",
        response.errors
    );

    ensure_migrations(node.as_ref())
        .await
        .expect("apply production migrations");

    let response = node
        .execute(
            r#"{ InferenceProfile(filter: {profile_id: {_eq: "existing-profile"}}) {
                profile_id display_name reasoning_effort
            } }"#,
        )
        .await;
    assert!(
        !response.has_errors(),
        "query profile: {:?}",
        response.errors
    );
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("InferenceProfile"))
        .and_then(serde_json::Value::as_array)
        .expect("InferenceProfile rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["profile_id"], "existing-profile");
    assert_eq!(rows[0]["display_name"], "Existing");
    assert!(rows[0]["reasoning_effort"].is_null());

    node.shutdown().await;
}

#[tokio::test]
async fn rendered_request_reaches_a_pre_existing_store_through_the_baseline() {
    // A store created before RenderedRequest existed: every other baseline
    // collection registered at its frozen SDL, with live data in one of them.
    let node = fresh_node().await;
    for entry in gents_migration::DEFAULT_BASELINE {
        if entry.name == gents_protocol::schemas::RENDERED_REQUEST_NAME {
            continue;
        }
        node.add_schema(entry.sdl)
            .await
            .unwrap_or_else(|error| panic!("register {}: {error}", entry.name));
    }
    let create = r#"mutation {
        create_AgentSession(input: { session_id: "pre-upgrade-session" }) { session_id }
    }"#;
    let response = node.execute(create).await;
    assert!(
        !response.has_errors(),
        "seed pre-upgrade session: {:?}",
        response.errors
    );
    assert!(
        node.get_collection(gents_protocol::schemas::RENDERED_REQUEST_NAME)
            .expect("get_collection")
            .is_none(),
        "fixture must start without RenderedRequest"
    );

    ensure_migrations(node.as_ref())
        .await
        .expect("upgrade an existing store");

    let rendered = node
        .get_collection(gents_protocol::schemas::RENDERED_REQUEST_NAME)
        .expect("get_collection")
        .expect("RenderedRequest present after upgrade");
    assert!(rendered.is_active);
    assert!(!rendered.is_placeholder);
    let pin = gents_migration::DEFAULT_BASELINE
        .iter()
        .find(|entry| entry.name == gents_protocol::schemas::RENDERED_REQUEST_NAME)
        .and_then(|entry| entry.expected_version)
        .expect("RenderedRequest baseline pin");
    assert_eq!(rendered.version_id, pin);

    // Pre-upgrade data is untouched.
    let response = node
        .execute(r#"{ AgentSession(filter: {session_id: {_eq: "pre-upgrade-session"}}) { session_id } }"#)
        .await;
    assert!(
        !response.has_errors(),
        "read pre-upgrade session: {:?}",
        response.errors
    );
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("AgentSession"))
        .and_then(serde_json::Value::as_array)
        .expect("AgentSession rows");
    assert_eq!(rows.len(), 1);

    // The registered collection is usable, and `capture_key` is the unique
    // idempotency key the capture sink will depend on.
    let capture = r#"mutation {
        create_RenderedRequest(input: {
            capture_key: "rendered:v1:aaa"
            request_doc_id: "bae-req-1"
            request_source_commit_cid: "bafy-source-1"
            request_source_signer_did: "did:key:source"
            request_claim_commit_cid: "bafy-claim-1"
            request_claim_signer_did: "did:key:agent"
            request_id: "req-1"
            session_id: "pre-upgrade-session"
            agent_did: "did:key:agent"
            requester_did: "did:key:requester"
            behavior_id: "behavior-1"
            turn_index: 0
            attempt: 0
            capture_version: 1
            model_name: "test-model"
            source: "openai_responses"
            request_json: "{}"
            prompt_hash: "ph"
            tools_hash: "th"
            provenance_json: "{\"version\":1}"
            created_at: "2026-08-06T00:00:00Z"
        }) { capture_key turn_index attempt capture_version }
    }"#;
    let response = node.execute(capture).await;
    assert!(
        !response.has_errors(),
        "create RenderedRequest: {:?}",
        response.errors
    );
    let duplicate = node.execute(capture).await;
    assert!(
        duplicate.has_errors(),
        "unique capture_key must reject a second row: {:?}",
        duplicate.data
    );
    let response = node
        .execute(r#"{ RenderedRequest(filter: {capture_key: {_eq: "rendered:v1:aaa"}}) { capture_key } }"#)
        .await;
    assert!(
        !response.has_errors(),
        "read captures: {:?}",
        response.errors
    );
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get("RenderedRequest"))
        .and_then(serde_json::Value::as_array)
        .expect("RenderedRequest rows");
    assert_eq!(rows.len(), 1, "one capture per key, ever");

    // Every field is @immutable: a capture is a fact, never an update.
    let overwrite = node
        .execute(
            r#"mutation {
                update_RenderedRequest(
                    filter: { capture_key: { _eq: "rendered:v1:aaa" } }
                    input: { request_json: "{\"tampered\":true}" }
                ) { capture_key }
            }"#,
        )
        .await;
    assert!(
        overwrite.has_errors(),
        "@immutable must reject rewriting a captured request: {:?}",
        overwrite.data
    );

    node.shutdown().await;
}

#[tokio::test]
async fn multi_version_lineage_is_rejected() {
    let node = fresh_node().await;
    ensure_migrations(node.as_ref()).await.expect("baseline");

    // Apply a field patch outside the registry → foreign multi-version DAG.
    let patch = r#"[{"op":"add","path":"/AgentRequest/Fields/-","value":{"Name":"__foreign_test_field","Kind":"String"}}]"#;
    let _ = node
        .patch_collection("AgentRequest", patch)
        .await
        .expect("foreign patch");

    let err = ensure_migrations(node.as_ref())
        .await
        .expect_err("foreign multi-version DAG must fail");
    match err {
        Error::UnknownLineage { collection, .. } | Error::ForeignVersion { collection, .. } => {
            assert_eq!(collection, "AgentRequest");
        }
        other => panic!("unexpected error: {other}"),
    }

    node.shutdown().await;
}

#[tokio::test]
async fn single_version_unknown_root_is_rejected() {
    const EXPECTED_SDL: &str = "type PinnedFixture { name: String label: String }";
    const FOREIGN_SDL: &str = "type PinnedFixture { name: String }";

    let authoring_node = fresh_node().await;
    authoring_node
        .add_schema(EXPECTED_SDL)
        .await
        .expect("register expected root");
    let expected_root = authoring_node
        .get_collection("PinnedFixture")
        .expect("load expected root")
        .expect("expected root exists")
        .version_id;
    authoring_node.shutdown().await;

    let node = fresh_node().await;
    node.add_schema(FOREIGN_SDL)
        .await
        .expect("register foreign root");
    let registry = DynamicRegistry {
        baseline: vec![BaselineCollectionOwned {
            name: "PinnedFixture".into(),
            sdl: EXPECTED_SDL.into(),
            expected_version: Some(expected_root),
            expected_state: CollectionExpectation::dag_only(),
        }],
        steps: vec![],
    };

    let err = ensure_migrations_dynamic(node.as_ref(), &registry)
        .await
        .expect_err("single unknown root must fail closed");
    assert!(
        matches!(err, Error::UnknownLineage { ref collection, .. } if collection == "PinnedFixture"),
        "unexpected error: {err}"
    );

    node.shutdown().await;
}

#[tokio::test]
async fn empty_registry_injectable_for_tests() {
    // Engine accepts custom registries (conformance injects crash chains later).
    let empty = Registry {
        baseline: &[],
        steps: &[],
    };
    let node = fresh_node().await;
    let report = ensure_migrations_with_registry(node.as_ref(), &empty)
        .await
        .expect("empty registry");
    assert_eq!(report.baseline_registered, 0);
    node.shutdown().await;
}
