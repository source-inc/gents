use super::*;
use serde_json::json;
use std::collections::BTreeSet;

#[test]
fn config_apply_order_contains_each_collection_once() {
    let actual = CONFIG_APPLY_ORDER_FOR_TESTS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let expected = Collection::ALL.into_iter().collect::<BTreeSet<_>>();

    assert_eq!(actual, expected);
    assert_eq!(CONFIG_APPLY_ORDER_FOR_TESTS.len(), Collection::ALL.len());
}

#[test]
fn config_prune_order_contains_each_collection_once() {
    let actual = CONFIG_PRUNE_ORDER_FOR_TESTS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let expected = Collection::ALL.into_iter().collect::<BTreeSet<_>>();

    assert_eq!(actual, expected);
    assert_eq!(CONFIG_PRUNE_ORDER_FOR_TESTS.len(), Collection::ALL.len());
}

#[test]
fn every_apply_collection_schema_has_a_recreate_identity_field() {
    use gents_protocol::schemas;

    for collection in Collection::ALL {
        let schema = match collection {
            Collection::AgentPrincipal => schemas::AGENT_PRINCIPAL,
            Collection::AgentBehavior => schemas::AGENT_BEHAVIOR,
            Collection::Skill => schemas::SKILL,
            Collection::DatastoreToolSurface => schemas::DATASTORE_TOOL_SURFACE,
            Collection::WorkspaceRoot => schemas::WORKSPACE_ROOT,
            Collection::ToolSelection => schemas::TOOL_SELECTION,
            Collection::InferenceBackend => schemas::INFERENCE_BACKEND,
            Collection::InferenceProfile => schemas::INFERENCE_PROFILE,
            Collection::ToolServiceRegistry => schemas::TOOL_SERVICE_REGISTRY,
            Collection::ProjectionAcpBinding => schemas::PROJECTION_ACP_BINDING,
            Collection::PeerPairingDesired => schemas::PEER_PAIRING_DESIRED,
            Collection::Task => schemas::TASK,
            Collection::Schedule => schemas::SCHEDULE,
            Collection::EventTrigger => schemas::EVENT_TRIGGER,
        };

        assert!(
            schema
                .lines()
                .any(|line| line.trim_start().starts_with("updated_at:")),
            "{} must expose updated_at for tombstone-safe recreation",
            collection.graphql_type()
        );
    }
}

#[test]
fn config_apply_order_has_retry_safe_prefixes() {
    for prefix_len in 0..=CONFIG_APPLY_ORDER_FOR_TESTS.len() {
        let prefix = &CONFIG_APPLY_ORDER_FOR_TESTS[..prefix_len];
        let suffix = &CONFIG_APPLY_ORDER_FOR_TESTS[prefix_len..];
        for written in prefix {
            for pending in suffix {
                assert!(
                    written.apply_order() <= pending.apply_order(),
                    "prefix {prefix_len} writes {:?} before lower-rank {:?}",
                    written,
                    pending,
                );
            }
        }
    }
}

#[test]
fn config_prune_order_deletes_referrers_before_dependencies() {
    for prefix_len in 0..=CONFIG_PRUNE_ORDER_FOR_TESTS.len() {
        let prefix = &CONFIG_PRUNE_ORDER_FOR_TESTS[..prefix_len];
        let suffix = &CONFIG_PRUNE_ORDER_FOR_TESTS[prefix_len..];
        for deleted in prefix {
            for pending in suffix {
                assert!(
                    pending.apply_order() <= deleted.apply_order(),
                    "prefix {prefix_len} deletes lower-rank {:?} before higher-rank {:?}",
                    deleted,
                    pending,
                );
            }
        }
    }
}

#[test]
fn build_aliased_mutation_wraps_all_fields() {
    let fields = vec![
        AliasedMutationField {
            alias: "doc_0".to_string(),
            field: r#"doc_0: create_Task(input: { task_id: "a" }) { _docID }"#.to_string(),
        },
        AliasedMutationField {
            alias: "doc_1".to_string(),
            field: r#"doc_1: create_Task(input: { task_id: "b" }) { _docID }"#.to_string(),
        },
    ];

    assert_eq!(
        build_aliased_mutation(&fields),
        r#"mutation {
doc_0: create_Task(input: { task_id: "a" }) { _docID }
doc_1: create_Task(input: { task_id: "b" }) { _docID }
}"#
    );
}

#[test]
fn delete_mutation_field_escapes_unique_value() {
    let field = delete_mutation_field(7, "Task", "task_id", r#"task"with\chars"#);

    assert_eq!(field.alias, "doc_7");
    assert_eq!(
        field.field,
        r#"doc_7: delete_Task(
            filter: { task_id: { _eq: "task\"with\\chars" } }
        ) { _docID }"#
    );
}

#[test]
fn manifest_pairing_delete_is_owner_scoped_and_escaped() {
    let field = manifest_pairing_delete_mutation_field(
        3,
        r#"peer"with\chars"#,
        r#"manifest:did:key:owner"with\chars"#,
    );

    assert_eq!(field.alias, "doc_3");
    assert!(field
        .field
        .contains(r#"peer_id: { _eq: "peer\"with\\chars" }"#));
    assert!(field
        .field
        .contains(r#"source: { _eq: "manifest:did:key:owner\"with\\chars" }"#));
}

#[test]
fn extract_aliased_mutation_doc_id_accepts_object_and_array_shapes() {
    let response = json!({
        "data": {
            "doc_0": { "_docID": "doc-a" },
            "doc_1": [{ "_docID": "doc-b" }]
        }
    });

    assert_eq!(
        extract_aliased_mutation_doc_id(&response, "doc_0", "Task").unwrap(),
        "doc-a"
    );
    assert_eq!(
        extract_aliased_mutation_doc_id(&response, "doc_1", "Task").unwrap(),
        "doc-b"
    );
}

#[test]
fn delete_mutation_field_targets_unique_field() {
    let field = delete_mutation_field(0, "Task", "task_id", "task-a");

    assert_eq!(field.alias, "doc_0");
    assert!(
        field.field.contains(r#"doc_0: delete_Task("#),
        "expected delete field, got {}",
        field.field
    );
    assert!(field.field.contains(r#"task_id: { _eq: "task-a" }"#));
}

#[test]
fn has_duplicate_unique_values_detects_repeated_import_ids() {
    let docs = vec![
        PreparedImportDocument {
            unique_value: "task-a".to_string(),
            add_doc: json!({ "task_id": "task-a" }),
            update_doc: Some(json!({ "task_id": "task-a" })),
        },
        PreparedImportDocument {
            unique_value: "task-b".to_string(),
            add_doc: json!({ "task_id": "task-b" }),
            update_doc: Some(json!({ "task_id": "task-b" })),
        },
        PreparedImportDocument {
            unique_value: "task-a".to_string(),
            add_doc: json!({ "task_id": "task-a", "enabled": true }),
            update_doc: Some(json!({ "enabled": true })),
        },
    ];

    assert!(has_duplicate_unique_values(&docs));
}

#[test]
fn generic_override_stamps_only_the_add_branch() {
    let doc = PreparedImportDocument {
        unique_value: "tools-a".to_string(),
        add_doc: json!({
            "selection_id": "tools-a",
            "agent_did": "did:example:agent"
        }),
        update_doc: Some(json!({ "agent_did": "did:example:agent" })),
    };

    let field =
        generic_import_mutation_field(0, "ToolSelection", "selection_id", &doc, true).unwrap();

    let (_, update) = field.field.split_once("update:").unwrap();
    assert!(field.field.contains("updated_at:"));
    assert!(!update.contains("updated_at:"));
}

#[tokio::test]
async fn generic_override_recreates_a_tombstoned_tool_selection() -> Result<()> {
    use gents::defra_node::{EmbeddedNode, StorageBackend};
    use gents::ensure_runtime_schemas;

    let tempdir = tempfile::tempdir()?;
    let node = EmbeddedNode::builder()
        .data_path(tempdir.path().join("data"))
        .with_storage_backend(StorageBackend::RocksDb)
        .build()
        .await?;
    ensure_runtime_schemas(&node).await?;
    let access = ConfigAccess::Local(std::sync::Arc::new(node));
    let doc = json!({
        "selection_id": "tools-a",
        "agent_did": "did:example:agent",
        "display_name": "Tools A"
    });

    let txn = access.begin_apply_txn().await?;
    apply_import_collection(
        &txn,
        "ToolSelection",
        "selection_id",
        std::slice::from_ref(&doc),
        true,
    )
    .await?;
    txn.commit().await?;
    let first_doc_id = tool_selection_doc_id(&access).await?;

    access
        .execute(
            r#"mutation {
                    delete_ToolSelection(filter: { selection_id: { _eq: "tools-a" } }) { _docID }
                }"#,
        )
        .await?;

    let txn = access.begin_apply_txn().await?;
    apply_import_collection(
        &txn,
        "ToolSelection",
        "selection_id",
        std::slice::from_ref(&doc),
        true,
    )
    .await?;
    txn.commit().await?;
    let recreated_doc_id = tool_selection_doc_id(&access).await?;

    assert_ne!(first_doc_id, recreated_doc_id);
    Ok(())
}

async fn tool_selection_doc_id(access: &ConfigAccess) -> Result<String> {
    let response = access
        .execute(
            r#"{
                    ToolSelection(filter: { selection_id: { _eq: "tools-a" } }) { _docID }
                }"#,
        )
        .await?;
    response
        .pointer("/data/ToolSelection/0/_docID")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("live ToolSelection missing after apply: {response}"))
}

#[test]
fn custom_override_mutation_field_updates_live_doc_by_doc_id() {
    let doc = PreparedImportDocument {
        unique_value: "task-a".to_string(),
        add_doc: json!({ "task_id": "task-a", "enabled": true }),
        update_doc: Some(json!({ "enabled": false })),
    };
    let existing = vec![ExistingDocumentRef {
        doc_id: "doc-live".to_string(),
        deleted: false,
    }];

    let field = custom_override_mutation_field(0, "Task", "task_id", &doc, &existing).unwrap();

    assert_eq!(field.alias, "doc_0");
    assert!(
        field
            .field
            .contains(r#"doc_0: update_Task(docID: "doc-live""#),
        "expected update field, got {}",
        field.field
    );
    assert!(field.field.contains("enabled: false"));
}

#[test]
fn custom_override_mutation_field_recreates_deleted_doc() {
    let doc = PreparedImportDocument {
        unique_value: "task-a".to_string(),
        add_doc: json!({ "task_id": "task-a", "enabled": true }),
        update_doc: Some(json!({ "enabled": false })),
    };
    let existing = vec![ExistingDocumentRef {
        doc_id: "doc-deleted".to_string(),
        deleted: true,
    }];

    let field = custom_override_mutation_field(0, "Task", "task_id", &doc, &existing).unwrap();

    assert_eq!(field.alias, "doc_0");
    assert!(
        field.field.contains("doc_0: create_Task(input:"),
        "expected create field, got {}",
        field.field
    );
    assert!(field.field.contains("updated_at:"));
}

#[test]
fn custom_selector_accepts_arbitrary_tombstone_history() {
    let rows = (0..32)
        .map(|index| ExistingDocumentRef {
            doc_id: format!("deleted-{index}"),
            deleted: true,
        })
        .collect::<Vec<_>>();

    let selected = select_existing_import_document("Task", "task_id", "task-a", &rows)
        .unwrap()
        .expect("tombstone history should select recreate");
    assert!(selected.deleted);
}

#[test]
fn custom_selector_prefers_the_only_live_row_over_tombstones() {
    let mut rows = (0..32)
        .map(|index| ExistingDocumentRef {
            doc_id: format!("deleted-{index}"),
            deleted: true,
        })
        .collect::<Vec<_>>();
    rows.push(ExistingDocumentRef {
        doc_id: "live".to_string(),
        deleted: false,
    });

    let selected = select_existing_import_document("Task", "task_id", "task-a", &rows)
        .unwrap()
        .expect("live row should be selected");
    assert_eq!(selected.doc_id, "live");
}

#[test]
fn custom_selector_rejects_multiple_live_rows() {
    let rows = vec![
        ExistingDocumentRef {
            doc_id: "live-a".to_string(),
            deleted: false,
        },
        ExistingDocumentRef {
            doc_id: "live-b".to_string(),
            deleted: false,
        },
    ];

    let error = select_existing_import_document("Task", "task_id", "task-a", &rows).unwrap_err();
    assert!(error.to_string().contains("multiple live Task documents"));
}
