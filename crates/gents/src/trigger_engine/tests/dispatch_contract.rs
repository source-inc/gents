use super::*;

pub(super) async fn trigger_engine_dispatch_matches_lean_generated_contract_cases() {
    let cases = lean_trigger_dispatch_cases();
    assert!(
        !cases.is_empty(),
        "Lean trigger dispatch contract should emit at least one case"
    );
    assert_eq!(
        cases.len(),
        lean_trigger_dispatch_case_count(),
        "Lean trigger dispatch case-count sentinel drifted"
    );

    for case in cases {
        let trigger_kind = trigger_kind_from_lean(&case.trigger_kind);
        let concurrency = concurrency_from_lean(&case.concurrency);
        let task = resolved_task(&format!("lean case {}", case.name));
        let snapshot = snapshot_from_trigger_contract(case, &task, concurrency);
        let (_tx, rx) = watch::channel(snapshot);
        let materializer = SpyMaterializer::new();
        materializer.track_materialized_nonterminal();
        let target_key = case
            .trigger_id
            .as_ref()
            .map(|trigger_id| (trigger_id.clone(), trigger_kind));
        let expects_target_supersede = target_key.as_ref().is_some_and(|(trigger_id, kind)| {
            case.expected_supersede_call_keys.iter().any(|key| {
                key.trigger_id == *trigger_id && trigger_kind_from_lean(&key.trigger_kind) == *kind
            })
        });
        // Lean emits both lists by scanning `before.requests` in order; consume
        // superseded ids as we seed matching prior target keys to preserve that
        // request-id alignment.
        let mut superseded_prior_ids = case.superseded_prior_ids.iter();
        for key in &case.prior_nonterminal_keys {
            let (prior_trigger_id, prior_trigger_kind) = trigger_key_from_lean(key);
            if target_key.as_ref().is_some_and(|(target_id, target_kind)| {
                target_id == &prior_trigger_id && *target_kind == prior_trigger_kind
            }) {
                if let Some(request_id) = superseded_prior_ids.next() {
                    materializer.mark_nonterminal_request(
                        &prior_trigger_id,
                        prior_trigger_kind,
                        request_id.clone(),
                    );
                } else {
                    assert!(
                        !expects_target_supersede,
                        "Lean case {} emitted fewer superseded_prior_ids than prior target keys",
                        case.name
                    );
                    materializer.mark_nonterminal(&prior_trigger_id, prior_trigger_kind);
                }
            } else {
                materializer.mark_nonterminal(&prior_trigger_id, prior_trigger_kind);
            }
        }
        assert!(
            superseded_prior_ids.next().is_none(),
            "Lean case {} emitted superseded_prior_ids that were not backed by prior target keys",
            case.name
        );
        let engine = TriggerEngine::new(rx, materializer.clone());

        let intent = FireIntent {
            trigger_id: case.trigger_id.clone(),
            trigger_kind,
            task,
            concurrency,
            event_vars: serde_json::json!({}),
            doc_vars: None,
            args_vars: None,
            pre_materialized_request_id: None,
            materialization_request_id: None,
            on_result: Box::new(|_| {}),
        };

        let result = engine.dispatch(intent).await;
        let expected_delta = case
            .request_count_after
            .checked_sub(case.request_count_before)
            .unwrap_or_else(|| panic!("Lean case {} shrank request count", case.name));

        match (case.expected_result.as_str(), result) {
            ("fired", FireResult::Fired { .. }) => {}
            ("skipped", FireResult::Skipped { reason }) => assert_eq!(
                Some(reason.as_str()),
                case.expected_skip_reason.as_deref(),
                "Lean case {} skip reason drifted",
                case.name
            ),
            (expected, other) => panic!(
                "Lean case {} expected {expected}, but TriggerEngine returned {other:?}",
                case.name
            ),
        }

        let calls = materializer.calls();
        assert_eq!(
            calls.len(),
            expected_delta,
            "Lean case {} materialize delta drifted",
            case.name
        );
        if expected_delta == 1 {
            let (trigger_id, kind, rendered) = &calls[0];
            assert_eq!(
                trigger_id.as_deref(),
                case.expected_materialize_trigger_id.as_deref(),
                "Lean case {} materialize trigger_id drifted",
                case.name
            );
            assert_eq!(
                kind.as_str(),
                case.expected_materialize_trigger_kind.as_deref().unwrap(),
                "Lean case {} materialize trigger_kind drifted",
                case.name
            );
            assert_eq!(
                rendered,
                &format!("lean case {}", case.name),
                "Lean case {} rendered prompt drifted",
                case.name
            );
            assert_eq!(
                case.expected_execution_origin.as_deref(),
                Some(execution_origin_for_trigger_kind(*kind).as_str()),
                "Lean case {} execution-origin contract no longer matches production materializer mapping",
                case.name
            );
            let expected_request_kind = if trigger_id.is_some() {
                Some(kind.as_str())
            } else {
                None
            };
            assert_eq!(
                case.expected_request_caused_by_id.as_deref(),
                trigger_id.as_deref(),
                "Lean case {} request caused_by id drifted",
                case.name
            );
            assert_eq!(
                case.expected_request_caused_by_kind.as_deref(),
                expected_request_kind,
                "Lean case {} request caused_by kind drifted",
                case.name
            );
        } else {
            assert!(
                case.expected_materialize_trigger_id.is_none()
                    && case.expected_materialize_trigger_kind.is_none()
                    && case.expected_execution_origin.is_none(),
                "Lean case {} should not carry materialization fields when skipped",
                case.name
            );
        }

        let supersede_calls = materializer.supersede_calls();
        let expected_supersede_calls = case
            .expected_supersede_call_keys
            .iter()
            .map(trigger_key_from_lean)
            .collect::<Vec<_>>();
        assert_eq!(
            supersede_calls, expected_supersede_calls,
            "Lean case {} supersede calls drifted",
            case.name
        );
        assert_eq!(
            materializer.superseded_request_ids(),
            case.superseded_prior_ids,
            "Lean case {} superseded concrete request ids drifted",
            case.name
        );

        if let Some(trigger_id) = case.trigger_id.as_deref() {
            assert_eq!(
                materializer.nonterminal_count_for(trigger_id, trigger_kind),
                case.target_nonterminal_count_after.unwrap_or(0),
                "Lean case {} target non-terminal count drifted",
                case.name
            );
        }
    }
}
