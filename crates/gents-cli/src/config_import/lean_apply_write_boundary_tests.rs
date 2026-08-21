use super::lean_vocab_test::{
    lean_apply_reconcile_cases, LeanApplyDesiredDoc, LeanApplyDocRef, LeanApplyLiveDoc,
    LeanApplyReconcileCase, LeanApplySelectedDoc,
};
use super::*;
use axum::{extract::State, routing::post, Json, Router};
use gents::BackendProviderKind;
use regex::Regex;
use serde_json::{json, Map};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

const DEFAULT_AGENT_DID: &str = "did:example:agent";
const DEFAULT_BEHAVIOR_ID: &str = "behavior-a";
const DEFAULT_TASK_ID: &str = "task-a";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedWrite {
    kind: String,
    collection: Collection,
    unique_value: String,
}

#[derive(Clone, Default)]
struct RecordingGraphqlState {
    queries: Arc<Mutex<Vec<String>>>,
    transactions: Arc<Mutex<BTreeMap<String, Vec<ObservedWrite>>>>,
    committed: Arc<Mutex<Vec<ObservedWrite>>>,
    next_tx_id: Arc<AtomicU64>,
    fail_injection: Arc<Mutex<Option<FailInjection>>>,
    tx_begin_count: Arc<AtomicU64>,
    tx_commit_count: Arc<AtomicU64>,
    tx_discard_count: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct FailInjection {
    tx_id: String,
    write_index: usize,
}

impl RecordingGraphqlState {
    fn committed_state(&self) -> Vec<ObservedWrite> {
        self.committed.lock().expect("committed lock").clone()
    }

    fn observed_writes(&self) -> Vec<ObservedWrite> {
        let mut all = self.committed.lock().expect("committed lock").clone();
        let txs = self.transactions.lock().expect("tx lock").clone();
        for (_id, writes) in txs.iter() {
            all.extend(writes.iter().cloned());
        }
        all
    }

    fn pending_state(&self) -> Vec<ObservedWrite> {
        self.transactions
            .lock()
            .expect("tx lock")
            .values()
            .flat_map(|writes| writes.iter().cloned())
            .collect()
    }

    fn tx_lifecycle_counts(&self) -> (u64, u64, u64) {
        (
            self.tx_begin_count.load(Ordering::SeqCst),
            self.tx_commit_count.load(Ordering::SeqCst),
            self.tx_discard_count.load(Ordering::SeqCst),
        )
    }

    fn install_fail_at(&self, tx_id: impl Into<String>, write_index: usize) {
        *self.fail_injection.lock().expect("fail lock") = Some(FailInjection {
            tx_id: tx_id.into(),
            write_index,
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generated_apply_reconcile_cases_fence_production_apply_write_boundary() {
    let cases = lean_apply_reconcile_cases();
    assert!(
        cases
            .iter()
            .any(|case| case.name == "production_write_boundary_all_collections"),
        "Lean must emit a production write-boundary case covering every collection"
    );

    for case in cases {
        assert_write_order_projection_matches_production(case);
        assert_prune_order_projection_matches_production(case);
        assert!(case.write_order_prefix_safe);
        assert!(case.prune_order_referrers_before_dependencies);
        assert!(case.delete_safety_holds);
        assert!(case.production_prefixes_referrers_closed);

        let desired_manifest = desired_manifest_from_lean(case);
        let desired_bundle =
            desired_state::export_bundle_from_manifest(&desired_manifest, "graphql")
                .unwrap_or_else(|error| {
                    panic!(
                        "failed to build desired apply bundle for Lean case {}: {error}",
                        case.name
                    )
                });
        let planned = diff_report_from_lean(case);
        assert_selected_documents_match_lean(case, desired_bundle.as_bundle(), &planned);

        let (graphql, recorder) = start_recording_graphql().await;
        let access = ConfigAccess::Graphql(graphql);
        let txn = access.begin_apply_txn().await.expect("begin apply tx");
        let counts = match apply_desired_state_changes(&txn, &desired_bundle, &planned).await {
            Ok(counts) => {
                txn.commit().await.expect("commit");
                counts
            }
            Err(error) => {
                let _ = txn.discard().await;
                panic!(
                    "production apply_desired_state_changes failed for Lean case {}: {error}",
                    case.name
                );
            }
        };

        assert_counts_match_lean(case, &counts);

        let observed = recorder.committed_state();
        let mut expected = case
            .expected_selected_writes
            .iter()
            .map(observed_write_from_lean)
            .collect::<Vec<_>>();
        expected.extend(
            case.expected_selected_delete_docs
                .iter()
                .map(observed_write_from_lean),
        );
        assert_eq!(
            observed, expected,
            "production mutation sequence drifted from Lean write-boundary projection for case {}",
            case.name
        );
        assert_observed_prefixes_are_referrer_closed(case, &observed);
        assert_live_payloads_not_written(case, &recorder);

        let (begin_count, commit_count, discard_count) = recorder.tx_lifecycle_counts();
        assert_eq!(
            (begin_count, commit_count, discard_count),
            (1, 1, 0),
            "success path must drive exactly one begin/commit and zero discard for Lean case {}",
            case.name,
        );

        if case.prefix_len > 0 && case.prefix_len < expected.len() {
            let initial_external_state = case
                .pre_live
                .iter()
                .map(observed_write_from_lean_live_doc)
                .collect::<Vec<_>>();
            let (graphql, recorder) =
                start_recording_graphql_with_committed_state(initial_external_state).await;
            let access = ConfigAccess::Graphql(graphql);

            let txn = access
                .begin_apply_txn()
                .await
                .expect("begin failure-case tx");
            recorder.install_fail_at("0", case.prefix_len);

            let result = apply_desired_state_changes(&txn, &desired_bundle, &planned).await;
            assert!(
                result.is_err(),
                "injected failure at write {} must surface as Err for Lean case {}",
                case.prefix_len,
                case.name,
            );
            let pending_after_failure = recorder.pending_state();

            let _ = txn.discard().await;

            let (begin_count, commit_count, discard_count) = recorder.tx_lifecycle_counts();
            assert_eq!(
                    (begin_count, commit_count, discard_count),
                    (1, 0, 1),
                    "failure path must drive exactly one begin/discard and zero commit for Lean case {}",
                    case.name,
                );

            let observed = recorder.committed_state();
            let expected = case
                .expected_external_state_after_abort
                .iter()
                .map(observed_write_from_lean_live_doc)
                .collect::<Vec<_>>();
            assert_eq!(
                    observed, expected,
                    "failure path must leave externally-observed committed state equal to Lean expected_external_state_after_abort for case {}",
                    case.name,
                );

            assert!(
                    pending_after_failure.len() <= case.prefix_len,
                    "failure path observed {} writes; the batch containing the failing write must be rejected and writes after it must not happen — cap is prefix_len = {} for Lean case {}",
                    pending_after_failure.len(),
                    case.prefix_len,
                    case.name,
                );
        }
    }
}

fn assert_write_order_projection_matches_production(case: &LeanApplyReconcileCase) {
    let expected_order = case
        .expected_write_order
        .iter()
        .map(|entry| {
            let collection = collection_from_lean_name(&entry.collection);
            assert_eq!(
                entry.graphql_type,
                collection.graphql_type(),
                "Lean GraphQL type mapping drifted for {:?}",
                collection
            );
            assert_eq!(
                entry.unique_field,
                collection.unique_field(),
                "Lean unique-field mapping drifted for {:?}",
                collection
            );
            assert_eq!(
                entry.apply_order,
                collection.apply_order() as usize,
                "Lean apply-order mapping drifted for {:?}",
                collection
            );
            collection
        })
        .collect::<Vec<_>>();

    assert_eq!(
        expected_order, CONFIG_APPLY_ORDER,
        "CONFIG_APPLY_ORDER must match Lean's production write order projection for case {}",
        case.name
    );
}

fn assert_prune_order_projection_matches_production(case: &LeanApplyReconcileCase) {
    let expected_order = case
        .expected_prune_order
        .iter()
        .map(|entry| {
            let collection = collection_from_lean_name(&entry.collection);
            assert_eq!(
                entry.graphql_type,
                collection.graphql_type(),
                "Lean prune GraphQL type mapping drifted for {:?}",
                collection
            );
            assert_eq!(
                entry.unique_field,
                collection.unique_field(),
                "Lean prune unique-field mapping drifted for {:?}",
                collection
            );
            assert_eq!(
                entry.apply_order,
                collection.apply_order() as usize,
                "Lean prune apply-order mapping drifted for {:?}",
                collection
            );
            collection
        })
        .collect::<Vec<_>>();

    assert_eq!(
        expected_order, CONFIG_PRUNE_ORDER,
        "CONFIG_PRUNE_ORDER must match Lean's production prune order projection for case {}",
        case.name
    );
}

fn assert_selected_documents_match_lean(
    case: &LeanApplyReconcileCase,
    desired_bundle: &ConfigExportBundle,
    planned: &desired_state::DesiredStateDiffReport,
) {
    let create_docs = case
        .expected_selected_create_docs
        .iter()
        .map(observed_write_from_lean)
        .collect::<Vec<_>>();
    let update_docs = case
        .expected_selected_update_docs
        .iter()
        .map(observed_write_from_lean)
        .collect::<Vec<_>>();
    let delete_docs = case
        .expected_selected_delete_docs
        .iter()
        .map(observed_write_from_lean)
        .collect::<Vec<_>>();

    for collection in Collection::ALL {
        let diff = planned.collections.get(collection);
        assert_eq!(
            diff.create,
            ids_for_collection(&create_docs, collection),
            "planned create ids must match Lean selected-create docs for case {} / {:?}",
            case.name,
            collection
        );
        assert_eq!(
            diff.update,
            ids_for_collection(&update_docs, collection),
            "planned update ids must match Lean selected-update docs for case {} / {:?}",
            case.name,
            collection
        );
        assert_eq!(
            diff.delete,
            ids_for_collection(&delete_docs, collection),
            "planned delete ids must match Lean selected-delete docs for case {} / {:?}",
            case.name,
            collection
        );

        let selected = select_apply_docs_for_collection(desired_bundle, planned, collection)
            .unwrap_or_else(|error| {
                panic!(
                    "production selection failed for Lean case {} / {:?}: {error}",
                    case.name, collection
                )
            });
        let actual_ids = selected
            .iter()
            .map(|doc| unique_value_from_doc(doc, collection))
            .collect::<Vec<_>>();
        let expected = case
            .expected_selected_writes
            .iter()
            .filter(|doc| collection_from_lean_ref(&doc.target) == collection)
            .collect::<Vec<_>>();
        let expected_ids = expected
            .iter()
            .map(|doc| doc.unique_value.clone())
            .collect::<Vec<_>>();

        assert_eq!(
            actual_ids, expected_ids,
            "selected production docs must match Lean selected writes for case {} / {:?}",
            case.name, collection
        );
        assert_no_live_only_doc_selected(case, collection, &actual_ids);
        assert_selected_docs_carry_lean_content(case, collection, &selected, &expected);
        assert_selected_docs_keep_live_fields_out(case, collection, &selected);
    }
}

fn assert_selected_docs_carry_lean_content(
    case: &LeanApplyReconcileCase,
    collection: Collection,
    selected: &[Value],
    expected: &[&LeanApplySelectedDoc],
) {
    let by_id = selected
        .iter()
        .map(|doc| (unique_value_from_doc(doc, collection), doc))
        .collect::<BTreeMap<_, _>>();
    for expected_doc in expected {
        let selected_doc = by_id.get(&expected_doc.unique_value).unwrap_or_else(|| {
            panic!(
                "selected production docs missing {} for Lean case {} / {:?}",
                expected_doc.unique_value, case.name, collection
            )
        });
        let encoded = serde_json::to_string(selected_doc).expect("selected doc JSON");
        assert!(
                encoded.contains(&expected_doc.content),
                "selected production doc for Lean case {} / {:?} / {} did not carry Lean content {:?}: {}",
                case.name,
                collection,
                expected_doc.unique_value,
                expected_doc.content,
                encoded
            );
    }
}

fn assert_selected_docs_keep_live_fields_out(
    case: &LeanApplyReconcileCase,
    collection: Collection,
    selected: &[Value],
) {
    let prepared = prepare_import_documents(
        collection.graphql_type(),
        collection.unique_field(),
        selected,
        true,
    )
    .unwrap_or_else(|error| {
        panic!(
            "failed to prepare selected docs for Lean case {} / {:?}: {error}",
            case.name, collection
        )
    });
    for doc in prepared {
        for field in runtime_owned_fields(collection) {
            assert!(
                doc.add_doc.get(field).is_none(),
                "add document for Lean case {} / {:?} / {} contains live-owned field {field}",
                case.name,
                collection,
                doc.unique_value
            );
            let update_doc = doc.update_doc.as_ref().expect("override update doc");
            assert!(
                update_doc.get(field).is_none(),
                "update document for Lean case {} / {:?} / {} contains live-owned field {field}",
                case.name,
                collection,
                doc.unique_value
            );
        }
    }
}

fn assert_no_live_only_doc_selected(
    case: &LeanApplyReconcileCase,
    collection: Collection,
    actual_ids: &[String],
) {
    let actual_ids = actual_ids.iter().collect::<BTreeSet<_>>();
    for live_only in &case.expected_live_only {
        if collection_from_lean_ref(live_only) == collection {
            assert!(
                !actual_ids.contains(&live_only.id),
                "live-only doc was selected for production write in Lean case {} / {:?}: {}",
                case.name,
                collection,
                live_only.id
            );
        }
    }
}

fn assert_counts_match_lean(case: &LeanApplyReconcileCase, counts: &ConfigApplyCounts) {
    for collection in Collection::ALL {
        let expected_writes = case
            .expected_selected_writes
            .iter()
            .filter(|doc| collection_from_lean_ref(&doc.target) == collection)
            .count();
        let expected_deletes = case
            .expected_selected_delete_docs
            .iter()
            .filter(|doc| collection_from_lean_ref(&doc.target) == collection)
            .count();
        assert_eq!(
            count_for_collection(counts, collection),
            expected_writes + expected_deletes,
            "apply_desired_state_changes count mismatch for Lean case {} / {:?}",
            case.name,
            collection
        );
    }
}

fn assert_observed_prefixes_are_referrer_closed(
    case: &LeanApplyReconcileCase,
    observed: &[ObservedWrite],
) {
    let refs_by_key = case
        .pre_desired
        .iter()
        .chain(case.manifest.iter())
        .map(|doc| {
            (
                doc_key_from_desired(doc),
                doc.refs.iter().map(doc_key).collect(),
            )
        })
        .collect::<BTreeMap<_, Vec<_>>>();
    let mut present = case
        .pre_desired
        .iter()
        .map(doc_key_from_desired)
        .collect::<BTreeSet<_>>();

    assert_present_refs_closed(case, 0, &present, &refs_by_key);

    for (index, mutation) in observed.iter().enumerate() {
        let prefix_len = index + 1;
        let key = (mutation.collection, mutation.unique_value.clone());
        match mutation.kind.as_str() {
            "write" => {
                present.insert(key.clone());
            }
            "delete" => {
                for referrer in &present {
                    let refs = refs_by_key.get(referrer).cloned().unwrap_or_default();
                    assert!(
                            !refs.contains(&key),
                            "production delete prefix {prefix_len} deletes {:?} while live referrer {:?} still references it in Lean case {}",
                            key,
                            referrer,
                            case.name
                        );
                }
                present.remove(&key);
            }
            "live" => {}
            other => panic!("unknown observed mutation kind {other:?}"),
        }

        assert_present_refs_closed(case, prefix_len, &present, &refs_by_key);
    }
}

fn assert_present_refs_closed(
    case: &LeanApplyReconcileCase,
    prefix_len: usize,
    present: &BTreeSet<(Collection, String)>,
    refs_by_key: &BTreeMap<(Collection, String), Vec<(Collection, String)>>,
) {
    for key in present {
        let refs = refs_by_key.get(key).cloned().unwrap_or_default();
        for reference in refs {
            assert!(
                    present.contains(&reference),
                    "production prefix {prefix_len} leaves referrer {:?} dangling on {:?} in Lean case {}",
                    key,
                    reference,
                    case.name
                );
        }
    }
}

fn assert_live_payloads_not_written(
    case: &LeanApplyReconcileCase,
    recorder: &RecordingGraphqlState,
) {
    let queries = recorder.queries.lock().expect("queries lock").join("\n");
    for live in &case.pre_live {
        assert!(
            !queries.contains(&live.content),
            "production write boundary leaked live payload {:?} into GraphQL for Lean case {}",
            live.content,
            case.name
        );
    }
}

async fn start_recording_graphql() -> (String, RecordingGraphqlState) {
    start_recording_graphql_with_committed_state(Vec::new()).await
}

async fn start_recording_graphql_with_committed_state(
    committed: Vec<ObservedWrite>,
) -> (String, RecordingGraphqlState) {
    let state = RecordingGraphqlState::default();
    *state.committed.lock().expect("committed lock") = committed;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind recording GraphQL listener");
    let addr = listener.local_addr().expect("recording GraphQL addr");
    let app = Router::new()
        .route("/api/v0/graphql", post(recording_graphql_handler))
        .route("/api/v0/tx", post(recording_tx_begin_handler))
        .route(
            "/api/v0/tx/{id}",
            post(recording_tx_commit_handler).delete(recording_tx_discard_handler),
        )
        .with_state(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("recording GraphQL server");
    });
    (format!("http://{addr}/api/v0/graphql"), state)
}

async fn recording_tx_begin_handler(State(state): State<RecordingGraphqlState>) -> Json<Value> {
    let id = state.next_tx_id.fetch_add(1, Ordering::SeqCst);
    state
        .transactions
        .lock()
        .expect("tx lock")
        .insert(id.to_string(), Vec::new());
    state.tx_begin_count.fetch_add(1, Ordering::SeqCst);
    Json(json!({ "id": id.to_string() }))
}

async fn recording_tx_commit_handler(
    State(state): State<RecordingGraphqlState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::http::StatusCode {
    let mut transactions = state.transactions.lock().expect("tx lock");
    let Some(writes) = transactions.remove(&id) else {
        return axum::http::StatusCode::NOT_FOUND;
    };
    drop(transactions);
    state
        .committed
        .lock()
        .expect("committed lock")
        .extend(writes);
    state.tx_commit_count.fetch_add(1, Ordering::SeqCst);
    axum::http::StatusCode::OK
}

async fn recording_tx_discard_handler(
    State(state): State<RecordingGraphqlState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::http::StatusCode {
    let removed = state
        .transactions
        .lock()
        .expect("tx lock")
        .remove(&id)
        .is_some();
    if removed {
        state.tx_discard_count.fetch_add(1, Ordering::SeqCst);
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::NOT_FOUND
    }
}

async fn recording_graphql_handler(
    State(state): State<RecordingGraphqlState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    let query = body
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    state
        .queries
        .lock()
        .expect("queries lock")
        .push(query.clone());

    let tx_id = headers
        .get("x-defradb-tx")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if query.contains("mutation") {
        let writes = parse_mutation_writes(&query);

        if let Some(fail) = state.fail_injection.lock().expect("fail lock").clone() {
            if tx_id.as_deref() == Some(fail.tx_id.as_str()) {
                let prior = state
                    .transactions
                    .lock()
                    .expect("tx lock")
                    .get(&fail.tx_id)
                    .map(|v| v.len())
                    .unwrap_or(0);
                if (prior..prior + writes.len()).contains(&fail.write_index) {
                    return Json(json!({
                        "errors": [{ "message": "injected failure at recorder" }]
                    }));
                }
            }
        }

        match tx_id {
            Some(id) => {
                let mut transactions = state.transactions.lock().expect("tx lock");
                let entry = transactions.entry(id).or_default();
                entry.extend(writes);
            }
            None => {
                state
                    .committed
                    .lock()
                    .expect("committed lock")
                    .extend(writes);
            }
        }
        Json(json!({ "data": aliased_mutation_response(&query) }))
    } else {
        Json(json!({ "data": empty_collection_query_response(&query) }))
    }
}

fn aliased_mutation_response(query: &str) -> Value {
    let alias_re = Regex::new(r"\b(doc_\d+)\s*:").expect("alias regex");
    let mut data = Map::new();
    for capture in alias_re.captures_iter(query) {
        let alias = capture[1].to_string();
        data.insert(alias.clone(), json!({ "_docID": format!("{alias}-id") }));
    }
    Value::Object(data)
}

fn empty_collection_query_response(query: &str) -> Value {
    let mut data = Map::new();
    for collection in Collection::ALL {
        if query.contains(&format!("{}(", collection.graphql_type())) {
            data.insert(
                collection.graphql_type().to_string(),
                Value::Array(Vec::new()),
            );
        }
    }
    Value::Object(data)
}

fn parse_mutation_writes(query: &str) -> Vec<ObservedWrite> {
    let field_re =
        Regex::new(r"(?:\bdoc_\d+\s*:\s*)?(create|update|upsert|delete)_([A-Za-z]+)\s*\(")
            .expect("mutation field regex");
    let matches = field_re
        .captures_iter(query)
        .map(|capture| {
            let whole = capture.get(0).expect("whole match");
            let action = capture.get(1).expect("action match").as_str();
            let collection_name = capture.get(2).expect("collection match").as_str();
            (
                whole.start(),
                if action == "delete" {
                    "delete"
                } else {
                    "write"
                },
                collection_from_lean_name(collection_name),
            )
        })
        .collect::<Vec<_>>();

    let mut writes = Vec::new();
    for (index, (start, kind, collection)) in matches.iter().copied().enumerate() {
        let end = matches
            .get(index + 1)
            .map(|(next_start, _, _)| *next_start)
            .unwrap_or(query.len());
        let segment = &query[start..end];
        let value_re = Regex::new(&format!(
            r#"\b{}\s*:\s*(?:"([^"]+)"|\{{\s*_eq\s*:\s*"([^"]+)"\s*\}})"#,
            regex::escape(collection.unique_field())
        ))
        .expect("unique-field regex");
        let unique_value = value_re
            .captures(segment)
            .and_then(|capture| capture.get(1).or_else(|| capture.get(2)))
            .unwrap_or_else(|| {
                panic!(
                    "mutation segment for {:?} did not carry unique field {}: {}",
                    collection,
                    collection.unique_field(),
                    segment
                )
            })
            .as_str()
            .to_string();
        writes.push(ObservedWrite {
            kind: kind.to_string(),
            collection,
            unique_value,
        });
    }
    writes
}

fn desired_manifest_from_lean(
    case: &LeanApplyReconcileCase,
) -> desired_state::DesiredStateManifest {
    let agent_did = agent_did_for_case(case);
    let principal_doc = docs_for_collection(case, Collection::AgentPrincipal)
        .into_iter()
        .next();
    desired_state::DesiredStateManifest {
        agent_principal: principal_doc
            .map(|doc| desired_principal(doc))
            .unwrap_or_else(|| desired_state::DesiredAgentPrincipal {
                agent_did: agent_did.clone(),
                display_name: Some("default-principal".to_string()),
                default_behavior_id: first_manifest_id(case, Collection::AgentBehavior),
                enabled: true,
            }),
        agent_behaviors: docs_for_collection(case, Collection::AgentBehavior)
            .into_iter()
            .map(|doc| desired_behavior(doc, &agent_did))
            .collect(),
        skills: docs_for_collection(case, Collection::Skill)
            .into_iter()
            .map(|doc| desired_skill(doc, &agent_did))
            .collect(),
        datastore_tool_surfaces: docs_for_collection(case, Collection::DatastoreToolSurface)
            .into_iter()
            .map(|doc| desired_datastore_tool_surface(doc, &agent_did))
            .collect(),
        tool_selections: docs_for_collection(case, Collection::ToolSelection)
            .into_iter()
            .map(|doc| desired_tool_selection(doc, &agent_did))
            .collect(),
        inference_backends: docs_for_collection(case, Collection::InferenceBackend)
            .into_iter()
            .map(desired_backend)
            .collect(),
        inference_profiles: docs_for_collection(case, Collection::InferenceProfile)
            .into_iter()
            .map(desired_profile)
            .collect(),
        tool_service_registries: docs_for_collection(case, Collection::ToolServiceRegistry)
            .into_iter()
            .map(desired_tool_service)
            .collect(),
        projection_acp_bindings: docs_for_collection(case, Collection::ProjectionAcpBinding)
            .into_iter()
            .map(|doc| desired_projection_acp_binding(doc, &agent_did))
            .collect(),
        peer_pairings: docs_for_collection(case, Collection::PeerPairingDesired)
            .into_iter()
            .map(desired_peer_pairing)
            .collect(),
        tasks: docs_for_collection(case, Collection::Task)
            .into_iter()
            .map(desired_task)
            .collect(),
        schedules: docs_for_collection(case, Collection::Schedule)
            .into_iter()
            .map(desired_schedule)
            .collect(),
        event_triggers: docs_for_collection(case, Collection::EventTrigger)
            .into_iter()
            .map(desired_event_trigger)
            .collect(),
    }
}

fn desired_principal(doc: &LeanApplyDesiredDoc) -> desired_state::DesiredAgentPrincipal {
    desired_state::DesiredAgentPrincipal {
        agent_did: doc.id.clone(),
        display_name: Some(doc.content.clone()),
        default_behavior_id: ref_id(doc, Collection::AgentBehavior),
        enabled: true,
    }
}

fn desired_behavior(
    doc: &LeanApplyDesiredDoc,
    agent_did: &str,
) -> desired_state::DesiredAgentBehavior {
    desired_state::DesiredAgentBehavior {
        behavior_id: doc.id.clone(),
        agent_did: agent_did.to_string(),
        display_name: Some(doc.content.clone()),
        description: None,
        summary: None,
        system_prompt: Some(doc.content.clone()),
        request_context_template: None,
        backend_id: ref_id(doc, Collection::InferenceBackend),
        model_name: Some(format!("model-{}", doc.id)),
        tool_selection_id: ref_id(doc, Collection::ToolSelection),
        inference_profile_id: ref_id(doc, Collection::InferenceProfile),
        compaction_strategy: None,
        compaction_threshold: None,
        enabled: true,
        skill_refs: ref_ids_for_collection(&doc.refs, Collection::Skill),
        skill_excludes: Vec::new(),
    }
}

fn desired_skill(doc: &LeanApplyDesiredDoc, agent_did: &str) -> desired_state::DesiredSkill {
    desired_state::DesiredSkill {
        skill_id: doc.id.clone(),
        agent_did: agent_did.to_string(),
        scope: "behavior".to_string(),
        name: doc.content.clone(),
        description: Some(doc.content.clone()),
        instructions: None,
        tool_refs: ref_ids_for_collection(&doc.refs, Collection::ToolServiceRegistry),
        display_name: Some(doc.content.clone()),
        interface_json: None,
        enabled: true,
    }
}

fn desired_datastore_tool_surface(
    doc: &LeanApplyDesiredDoc,
    agent_did: &str,
) -> desired_state::DesiredDatastoreToolSurface {
    desired_state::DesiredDatastoreToolSurface {
        surface_id: doc.id.clone(),
        agent_did: agent_did.to_string(),
        display_name: Some(doc.content.clone()),
        enabled: true,
        entries: Vec::new(),
    }
}

fn desired_tool_selection(
    doc: &LeanApplyDesiredDoc,
    agent_did: &str,
) -> desired_state::DesiredToolSelection {
    desired_state::DesiredToolSelection {
        selection_id: doc.id.clone(),
        agent_did: agent_did.to_string(),
        display_name: Some(doc.content.clone()),
        tool_policy_version: None,
        enable_file_tools: false,
        file_tools_mode: "disabled".to_string(),
        file_tool_root: None,
        enable_bash: false,
        bash_mode: "disabled".to_string(),
        command_execution_policy: None,
        command_allowed_argv_prefixes: Vec::new(),
        command_forbidden_argv_prefixes: Vec::new(),
        read_only_command_allowlist: Vec::new(),
        command_network_mode: None,
        cli_tool_names: Vec::new(),
        enable_meta_tools: false,
        allowed_mcp_service_ids: doc
            .refs
            .iter()
            .filter(|reference| {
                collection_from_lean_ref(reference) == Collection::ToolServiceRegistry
            })
            .map(|reference| reference.id.clone())
            .collect(),
        delegate_to: Vec::new(),
        backgroundable_tool_names: Vec::new(),
        enable_memory: false,
        enable_session_history_tool: false,
        enable_context_budget: true,
        enable_defra_query: true,
        defra_query_collections: Vec::new(),
        subagent_targets: Vec::new(),
        subagent_spawn_enabled: false,
        subagent_steering_enabled: false,
        subagent_background_enabled: false,
        subagent_default_await_mode: None,
        subagent_allow_cross_deployment: false,
        cross_deployment_spawn_timeout_seconds: None,
        write_tools: Vec::new(),
        datastore_tool_surface_ids: Vec::new(),
        enable_self_config: false,
        self_config_categories: Vec::new(),
        self_config_no_lockout: false,
        self_config_dry_run: false,
        enable_lsp: false,
        lsp_config: None,
    }
}

fn desired_backend(doc: &LeanApplyDesiredDoc) -> desired_state::DesiredInferenceBackend {
    desired_state::DesiredInferenceBackend {
        backend_id: doc.id.clone(),
        name: doc.content.clone(),
        provider_kind: BackendProviderKind::OpenAiCompatible,
        openai_wire_api: None,
        endpoint: format!("http://127.0.0.1/{}/v1", doc.id),
        api_key: None,
        api_key_env_var: None,
        max_concurrent: 1,
        max_queue_depth: 10,
        enabled: true,
        models: vec![doc.content.clone()],
    }
}

fn desired_profile(doc: &LeanApplyDesiredDoc) -> desired_state::DesiredInferenceProfile {
    desired_state::DesiredInferenceProfile {
        profile_id: doc.id.clone(),
        display_name: Some(doc.content.clone()),
        context_window: None,
        max_output_tokens: None,
        max_turns: None,
        temperature: None,
        stream_batch_ms: None,
        stream_liveness_timeout_secs: None,
        deadline_duration_secs: None,
        retry_max_transport: None,
        retry_backoff_ms: None,
        retry_max_resample: None,
        retry_allow_repair: None,
        retry_interactive_max: None,
        ..Default::default()
    }
}

fn desired_tool_service(doc: &LeanApplyDesiredDoc) -> desired_state::DesiredToolServiceRegistry {
    desired_state::DesiredToolServiceRegistry {
        service_id: doc.id.clone(),
        display_name: Some(doc.content.clone()),
        description: Some(doc.content.clone()),
        hostname: None,
        tailscale_ip: None,
        lan_ip: None,
        mcp_port: None,
        mcp_path: None,
        send_agent_did: false,
    }
}

fn desired_projection_acp_binding(
    doc: &LeanApplyDesiredDoc,
    agent_did: &str,
) -> desired_state::DesiredProjectionAcpBinding {
    desired_state::DesiredProjectionAcpBinding {
        binding_id: doc.id.clone(),
        agent_did: Some(agent_did.to_string()),
        behavior_id: ref_id(doc, Collection::AgentBehavior),
        projection_id: Some(format!("projection-{}", doc.id)),
        policy_id: format!("policy-{}", doc.content),
        staged_policy_id: None,
        previous_policy_id: None,
        resource_map_json: Some(r#"{"AgentRequest":"AgentRequest"}"#.to_string()),
        publication_status: Some("published".to_string()),
        published_at: None,
        enabled: true,
    }
}

fn desired_task(doc: &LeanApplyDesiredDoc) -> desired_state::DesiredTask {
    desired_state::DesiredTask {
        task_id: doc.id.clone(),
        name: doc.content.clone(),
        description: Some(doc.content.clone()),
        behavior_id: ref_id(doc, Collection::AgentBehavior)
            .unwrap_or_else(|| DEFAULT_BEHAVIOR_ID.to_string()),
        prompt_template: doc.content.clone(),
        enabled: true,
        output_schema_ref: None,
    }
}

fn desired_peer_pairing(doc: &LeanApplyDesiredDoc) -> desired_state::DesiredPeerPairing {
    desired_state::DesiredPeerPairing {
        peer_did: format!("did:key:{}", doc.content),
        addresses: vec![format!("{}@127.0.0.1:4100", doc.id)],
        template: "conversation".to_string(),
        enabled: true,
        peer_id: doc.id.clone(),
    }
}

fn desired_schedule(doc: &LeanApplyDesiredDoc) -> desired_state::DesiredSchedule {
    desired_state::DesiredSchedule {
        schedule_id: doc.id.clone(),
        task_id: ref_id(doc, Collection::Task).unwrap_or_else(|| DEFAULT_TASK_ID.to_string()),
        interval_secs: Some(60),
        cron: None,
        timezone: None,
        missed_run_policy: None,
        enabled: true,
        concurrency: doc.content.clone(),
    }
}

fn desired_event_trigger(doc: &LeanApplyDesiredDoc) -> desired_state::DesiredEventTrigger {
    desired_state::DesiredEventTrigger {
        trigger_id: doc.id.clone(),
        task_id: ref_id(doc, Collection::Task).unwrap_or_else(|| DEFAULT_TASK_ID.to_string()),
        source_collection: "Task".to_string(),
        event_kind: "created".to_string(),
        filter: Some(doc.content.clone()),
        correlation_field: None,
        fire_mode: None,
        expected_count: None,
        expected_count_field: None,
        group_timeout_secs: None,
        group_min_count: None,
        enabled: true,
        concurrency: "serial".to_string(),
    }
}

fn diff_report_from_lean(case: &LeanApplyReconcileCase) -> desired_state::DesiredStateDiffReport {
    let collections = desired_state::DesiredStateDiffCollections {
        agent_principal: diff_for_collection(case, Collection::AgentPrincipal),
        agent_behaviors: diff_for_collection(case, Collection::AgentBehavior),
        skills: diff_for_collection(case, Collection::Skill),
        datastore_tool_surfaces: diff_for_collection(case, Collection::DatastoreToolSurface),
        // WorkspaceRoot is not part of Collection::ALL yet, so no Lean
        // fixture ever references it; this is always an empty diff.
        workspace_roots: diff_for_collection(case, Collection::WorkspaceRoot),
        tool_selections: diff_for_collection(case, Collection::ToolSelection),
        inference_backends: diff_for_collection(case, Collection::InferenceBackend),
        inference_profiles: diff_for_collection(case, Collection::InferenceProfile),
        tool_service_registries: diff_for_collection(case, Collection::ToolServiceRegistry),
        projection_acp_bindings: diff_for_collection(case, Collection::ProjectionAcpBinding),
        peer_pairings: diff_for_collection(case, Collection::PeerPairingDesired),
        tasks: diff_for_collection(case, Collection::Task),
        schedules: diff_for_collection(case, Collection::Schedule),
        event_triggers: diff_for_collection(case, Collection::EventTrigger),
    };
    let counts = collections.counts();
    let ok = counts.is_exact_match();
    desired_state::DesiredStateDiffReport {
        status: "diffed",
        ok,
        root: format!("lean://{}", case.name),
        access_mode: "graphql".to_string(),
        agent_did: agent_did_for_case(case),
        live_validation_errors: Vec::new(),
        counts,
        collections,
    }
}

fn diff_for_collection(
    case: &LeanApplyReconcileCase,
    collection: Collection,
) -> desired_state::DesiredStateCollectionDiff {
    desired_state::DesiredStateCollectionDiff {
        create: ref_ids_for_collection(&case.expected_create, collection),
        update: ref_ids_for_collection(&case.expected_update, collection),
        delete: ref_ids_for_collection(&case.expected_delete, collection),
        unchanged: ref_ids_for_collection(&case.expected_unchanged, collection),
        live_only: ref_ids_for_collection(&case.expected_live_only, collection),
    }
}

fn collection_from_lean_name(name: &str) -> Collection {
    Collection::ALL
        .into_iter()
        .find(|collection| collection.graphql_type() == name)
        .unwrap_or_else(|| panic!("unknown Lean collection name {name:?}"))
}

fn collection_from_lean_ref(reference: &LeanApplyDocRef) -> Collection {
    collection_from_lean_name(&reference.collection)
}

fn docs_for_collection(
    case: &LeanApplyReconcileCase,
    collection: Collection,
) -> Vec<&LeanApplyDesiredDoc> {
    case.manifest
        .iter()
        .filter(|doc| collection_from_lean_name(&doc.collection) == collection)
        .collect()
}

fn first_manifest_id(case: &LeanApplyReconcileCase, collection: Collection) -> Option<String> {
    docs_for_collection(case, collection)
        .into_iter()
        .next()
        .map(|doc| doc.id.clone())
}

fn agent_did_for_case(case: &LeanApplyReconcileCase) -> String {
    first_manifest_id(case, Collection::AgentPrincipal)
        .unwrap_or_else(|| DEFAULT_AGENT_DID.to_string())
}

fn ref_id(doc: &LeanApplyDesiredDoc, collection: Collection) -> Option<String> {
    doc.refs
        .iter()
        .find(|reference| collection_from_lean_ref(reference) == collection)
        .map(|reference| reference.id.clone())
}

fn ref_ids_for_collection(refs: &[LeanApplyDocRef], collection: Collection) -> Vec<String> {
    refs.iter()
        .filter(|reference| collection_from_lean_ref(reference) == collection)
        .map(|reference| reference.id.clone())
        .collect()
}

fn ids_for_collection(writes: &[ObservedWrite], collection: Collection) -> Vec<String> {
    writes
        .iter()
        .filter(|write| write.collection == collection)
        .map(|write| write.unique_value.clone())
        .collect()
}

fn observed_write_from_lean(doc: &LeanApplySelectedDoc) -> ObservedWrite {
    assert_eq!(doc.unique_value, doc.target.id);
    assert_eq!(doc.graphql_type, doc.target.collection);
    let collection = collection_from_lean_ref(&doc.target);
    assert_eq!(doc.unique_field, collection.unique_field());
    ObservedWrite {
        kind: if doc.action == "delete" {
            "delete".to_string()
        } else {
            "write".to_string()
        },
        collection,
        unique_value: doc.unique_value.clone(),
    }
}

fn observed_write_from_lean_live_doc(doc: &LeanApplyLiveDoc) -> ObservedWrite {
    ObservedWrite {
        kind: "live".to_string(),
        collection: collection_from_lean_name(&doc.collection),
        unique_value: doc.id.clone(),
    }
}

fn unique_value_from_doc(doc: &Value, collection: Collection) -> String {
    doc.get(collection.unique_field())
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            panic!(
                "selected {:?} document is missing unique field {}: {}",
                collection,
                collection.unique_field(),
                doc
            )
        })
        .to_string()
}

fn count_for_collection(counts: &ConfigApplyCounts, collection: Collection) -> usize {
    match collection {
        Collection::AgentPrincipal => counts.agent_principal,
        Collection::AgentBehavior => counts.agent_behaviors,
        Collection::Skill => counts.skills,
        Collection::DatastoreToolSurface => counts.datastore_tool_surfaces,
        Collection::WorkspaceRoot => counts.workspace_roots,
        Collection::ToolSelection => counts.tool_selections,
        Collection::InferenceBackend => counts.inference_backends,
        Collection::InferenceProfile => counts.inference_profiles,
        Collection::ToolServiceRegistry => counts.tool_service_registries,
        Collection::ProjectionAcpBinding => counts.projection_acp_bindings,
        Collection::PeerPairingDesired => counts.peer_pairings,
        Collection::Task => counts.tasks,
        Collection::Schedule => counts.schedules,
        Collection::EventTrigger => counts.event_triggers,
    }
}

fn runtime_owned_fields(collection: Collection) -> &'static [&'static str] {
    match collection {
        Collection::InferenceBackend => &["probe_status"],
        Collection::ToolServiceRegistry => &["tools", "version"],
        Collection::ProjectionAcpBinding => &[],
        Collection::PeerPairingDesired => &[],
        Collection::Schedule => &[
            "next_run_at",
            "last_attempt_at",
            "last_status",
            "last_error",
            "fire_count",
        ],
        Collection::EventTrigger => &[
            "last_attempt_at",
            "last_fired_source_doc_id",
            "last_status",
            "last_error",
            "fire_count",
        ],
        _ => &[],
    }
}

fn doc_key(reference: &LeanApplyDocRef) -> (Collection, String) {
    (collection_from_lean_ref(reference), reference.id.clone())
}

fn doc_key_from_desired(doc: &LeanApplyDesiredDoc) -> (Collection, String) {
    (collection_from_lean_name(&doc.collection), doc.id.clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_apply_txn_round_trip_against_recorder() {
    let (graphql, recorder) = start_recording_graphql().await;
    let access = ConfigAccess::Graphql(graphql);
    let txn = access.begin_apply_txn().await.expect("begin");

    let _ = txn
        .execute("mutation { doc_0: create_Task(input: { task_id: \"task-a\" }) { _docID } }")
        .await
        .expect("execute in tx");

    assert!(recorder.committed_state().is_empty());

    txn.commit().await.expect("commit");

    let committed = recorder.committed_state();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].unique_value, "task-a");
    let (begin_count, commit_count, discard_count) = recorder.tx_lifecycle_counts();
    assert_eq!((begin_count, commit_count, discard_count), (1, 1, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_apply_txn_discard_leaves_committed_empty() {
    let (graphql, recorder) = start_recording_graphql().await;
    let access = ConfigAccess::Graphql(graphql);
    let txn = access.begin_apply_txn().await.expect("begin");

    let _ = txn
        .execute("mutation { doc_0: create_Task(input: { task_id: \"task-a\" }) { _docID } }")
        .await
        .expect("execute in tx");

    txn.discard().await.expect("discard");

    assert!(recorder.committed_state().is_empty());
    let (begin_count, commit_count, discard_count) = recorder.tx_lifecycle_counts();
    assert_eq!((begin_count, commit_count, discard_count), (1, 0, 1));
}

#[cfg(test)]
mod recorder_unit_tests {
    use super::*;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recorder_begin_returns_numeric_id_and_commit_appends_to_committed() {
        let (graphql, recorder) = start_recording_graphql().await;
        let api_base = crate::graphql_access::graphql_api_base(&graphql).expect("graphql endpoint");
        let client = reqwest::Client::new();

        let begin = client
            .post(format!("{api_base}/tx"))
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        let txn_id = begin
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .to_string();
        assert!(txn_id.parse::<u64>().is_ok(), "tx id must be numeric");

        let _write = client
                .post(&graphql)
                .header("x-defradb-tx", &txn_id)
                .json(&json!({
                    "query": "mutation { doc_0: create_Task(input: { task_id: \"task-a\" }) { _docID } }",
                }))
                .send()
                .await
                .unwrap();

        assert!(recorder.committed_state().is_empty());

        let commit = client
            .post(format!("{api_base}/tx/{txn_id}"))
            .send()
            .await
            .unwrap();
        assert!(commit.status().is_success());

        let committed = recorder.committed_state();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].collection, Collection::Task);
        assert_eq!(committed[0].unique_value, "task-a");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recorder_discard_drops_pending_writes() {
        let (graphql, recorder) = start_recording_graphql().await;
        let api_base = crate::graphql_access::graphql_api_base(&graphql).expect("graphql endpoint");
        let client = reqwest::Client::new();

        let begin = client
            .post(format!("{api_base}/tx"))
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        let txn_id = begin
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .to_string();

        let _write = client
                .post(&graphql)
                .header("x-defradb-tx", &txn_id)
                .json(&serde_json::json!({
                    "query": "mutation { doc_0: create_Task(input: { task_id: \"task-a\" }) { _docID } }",
                }))
                .send()
                .await
                .unwrap();

        let discard = client
            .delete(format!("{api_base}/tx/{txn_id}"))
            .send()
            .await
            .unwrap();
        assert!(discard.status().is_success());

        assert!(
            recorder.committed_state().is_empty(),
            "discarded tx must not contribute to committed state"
        );
        let (begin_count, commit_count, discard_count) = recorder.tx_lifecycle_counts();
        assert_eq!((begin_count, commit_count, discard_count), (1, 0, 1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recorder_fail_injection_aborts_at_target_index() {
        let (graphql, recorder) = start_recording_graphql().await;
        let api_base = crate::graphql_access::graphql_api_base(&graphql).expect("graphql endpoint");
        let client = reqwest::Client::new();

        let begin = client
            .post(format!("{api_base}/tx"))
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        let txn_id = begin
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .to_string();
        recorder.install_fail_at(&txn_id, 1);

        let ok = client
                .post(&graphql)
                .header("x-defradb-tx", &txn_id)
                .json(&serde_json::json!({
                    "query": "mutation { doc_0: create_Task(input: { task_id: \"task-a\" }) { _docID } }",
                }))
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap();
        assert!(ok.get("errors").is_none(), "first mutation should succeed");

        let pending_after_first = recorder.observed_writes();
        assert_eq!(
            pending_after_first.len(),
            1,
            "first mutation must be buffered into tx pending window"
        );
        assert_eq!(pending_after_first[0].unique_value, "task-a");
        assert!(
            recorder.committed_state().is_empty(),
            "buffered tx writes must not appear in committed state yet"
        );

        let fail = client
                .post(&graphql)
                .header("x-defradb-tx", &txn_id)
                .json(&serde_json::json!({
                    "query": "mutation { doc_0: create_Task(input: { task_id: \"task-b\" }) { _docID } }",
                }))
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap();
        assert!(fail.get("errors").is_some(), "second mutation should fail");
    }
}
