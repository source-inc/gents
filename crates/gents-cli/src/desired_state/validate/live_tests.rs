use anyhow::Result;
use gents::defra_node::{EmbeddedNode, StorageBackend};
use gents::ensure_runtime_schemas;

use super::*;
use crate::config_writes::ConfigAccess;

fn manifest_with_subagent_targets(targets: Vec<SubagentTarget>) -> DesiredStateManifest {
    use super::super::{DesiredAgentPrincipal, DesiredStateManifest, DesiredToolSelection};
    let targets: Vec<String> = targets.iter().map(SubagentTarget::to_entry).collect();
    DesiredStateManifest {
        agent_principal: DesiredAgentPrincipal {
            agent_did: "did:key:test-live-validate".to_string(),
            display_name: None,
            default_behavior_id: None,
            enabled: true,
        },
        agent_behaviors: Vec::new(),
        skills: Vec::new(),
        datastore_tool_surfaces: Vec::new(),
        tool_selections: vec![DesiredToolSelection {
            selection_id: "live-test-sel".to_string(),
            agent_did: "did:key:test-live-validate".to_string(),
            display_name: None,
            tool_policy_version: None,
            enable_file_tools: false,
            file_tools_mode: "ReadOnly".to_string(),
            file_tool_root: None,
            enable_bash: false,
            bash_mode: "ReadOnly".to_string(),
            command_execution_policy: None,
            command_allowed_argv_prefixes: Vec::new(),
            command_forbidden_argv_prefixes: Vec::new(),
            read_only_command_allowlist: Vec::new(),
            command_network_mode: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: false,
            allowed_mcp_service_ids: Vec::new(),
            delegate_to: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: true,
            defra_query_collections: Vec::new(),
            subagent_targets: targets,
            subagent_spawn_enabled: true,
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
        }],
        inference_backends: Vec::new(),
        inference_profiles: Vec::new(),
        tool_service_registries: Vec::new(),
        projection_acp_bindings: Vec::new(),
        peer_pairings: Vec::new(),
        tasks: Vec::new(),
        schedules: Vec::new(),
        event_triggers: Vec::new(),
    }
}

#[tokio::test]
async fn live_validate_rejects_invalid_event_trigger_collection_identifier() -> Result<()> {
    use super::super::DesiredEventTrigger;

    let tempdir = tempfile::tempdir()?;
    let node = EmbeddedNode::builder()
        .data_path(tempdir.path().join("data"))
        .with_storage_backend(StorageBackend::Lark)
        .build()
        .await?;
    ensure_runtime_schemas(&node).await?;
    let access = ConfigAccess::Local(std::sync::Arc::new(node));

    let mut manifest = manifest_with_subagent_targets(Vec::new());
    manifest.event_triggers.push(DesiredEventTrigger {
        trigger_id: "malformed-source".to_string(),
        task_id: "unused-task".to_string(),
        source_collection: "AgentMessage) { _docID } mutation {".to_string(),
        event_kind: "created".to_string(),
        filter: Some("{}".to_string()),
        correlation_field: None,
        fire_mode: None,
        expected_count: None,
        expected_count_field: None,
        group_timeout_secs: None,
        group_min_count: None,
        enabled: true,
        concurrency: "serial".to_string(),
    });

    let errors = validate_manifest_against_live(&manifest, &access).await?;
    assert!(
        errors.iter().any(|error| {
            error.contains("malformed-source")
                && error.contains("invalid source_collection")
                && error.contains("invalid identifier")
        }),
        "expected direct live-validation identifier rejection, got {errors:?}"
    );
    Ok(())
}

#[tokio::test]
async fn live_validate_does_not_resolve_remote_subagent_target() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let data_dir = tempdir.path().join("data");
    let node = EmbeddedNode::builder()
        .data_path(&data_dir)
        .with_storage_backend(StorageBackend::Lark)
        .build()
        .await?;
    ensure_runtime_schemas(&node).await?;

    let access = ConfigAccess::Local(std::sync::Arc::new(node));

    let manifest = manifest_with_subagent_targets(vec![SubagentTarget {
        name: "remote-researcher".to_string(),
        agent_did: "did:key:zRemotePeer".to_string(),
        behavior_id: "does-not-exist-locally".to_string(),
        description: None,
    }]);
    let errors = validate_manifest_against_live(&manifest, &access).await?;

    assert!(
        !errors
            .iter()
            .any(|msg| msg.contains("does-not-exist-locally") || msg.contains("live-test-sel")),
        "remote subagent target must not trigger live resolution errors, got {errors:?}"
    );
    Ok(())
}

#[tokio::test]
async fn live_validate_passes_for_known_subagent_target() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let data_dir = tempdir.path().join("data");
    let node = EmbeddedNode::builder()
        .data_path(&data_dir)
        .with_storage_backend(StorageBackend::Lark)
        .build()
        .await?;
    ensure_runtime_schemas(&node).await?;

    let access = ConfigAccess::Local(std::sync::Arc::new(node));

    let manifest = manifest_with_subagent_targets(vec![SubagentTarget {
        name: "researcher".to_string(),
        agent_did: "did:key:test-live-validate".to_string(),
        behavior_id: "amy-research".to_string(),
        description: None,
    }]);
    let errors = validate_manifest_against_live(&manifest, &access).await?;

    assert!(
        !errors
            .iter()
            .any(|msg| msg.contains("amy-research") || msg.contains("live-test-sel")),
        "expected no subagent errors for known target, got {errors:?}"
    );
    Ok(())
}

#[tokio::test]
async fn live_validate_rejects_non_manifest_pairing_collision_and_diff_reports_it() -> Result<()> {
    use super::super::DesiredPeerPairing;
    use crate::commands::config::binding::{
        BoundDesiredManifest, ManifestBindMode, ManifestBindingContext,
    };
    use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
    use crate::config_import::apply_desired_state_changes;
    use crate::desired_state::{diff_manifests, export_bundle_from_manifest};
    use gents::graphql::escape_graphql_string;

    let tempdir = tempfile::tempdir()?;
    let node = EmbeddedNode::builder()
        .data_path(tempdir.path().join("data"))
        .with_storage_backend(StorageBackend::Lark)
        .build()
        .await?;
    ensure_runtime_schemas(&node).await?;
    let access = ConfigAccess::Local(std::sync::Arc::new(node));
    let peer_id = "aa".repeat(32);
    let peer_did = "did:key:remote";
    let address = format!("{peer_id}@127.0.0.1:4100");
    access
        .execute(&format!(
            r#"mutation {{ create_PeerPairingDesired(input: {{
                    peer_id: "{}",
                    agent_did: "{}",
                    collections: ["AgentRequest"],
                    replicator_addresses: ["{}"],
                    template: "conversation",
                    source: "operator"
                }}) {{ _docID }} }}"#,
            escape_graphql_string(&peer_id),
            escape_graphql_string(peer_did),
            escape_graphql_string(&address),
        ))
        .await?;

    let mut manifest = manifest_with_subagent_targets(Vec::new());
    manifest.peer_pairings.push(DesiredPeerPairing {
        peer_did: peer_did.to_string(),
        addresses: vec![address],
        template: "conversation".to_string(),
        enabled: false,
        peer_id,
    });
    let errors = validate_manifest_against_live(&manifest, &access).await?;
    assert!(errors.iter().any(|error| {
        error.contains("source \"operator\"") && error.contains("refusing to overwrite or delete")
    }));

    let owner_did = manifest.agent_principal.agent_did.clone();
    let bound = BoundDesiredManifest {
        context: ManifestBindingContext {
            bind_mode: ManifestBindMode::Manifest,
            target_agent_did: owner_did.clone(),
            source_manifest_dids: std::collections::BTreeSet::from([owner_did]),
        },
        manifest: manifest.clone(),
    };
    let report = crate::commands::config::diff::diff_bound_desired_manifest(
        std::path::Path::new("/ownership-collision"),
        &access,
        &bound,
    )
    .await?;
    assert_eq!(report.status, "diffed");
    assert!(!report.ok);
    assert!(report.live_validation_errors.iter().any(|error| {
        error.contains("source \"operator\"") && error.contains("refusing to overwrite or delete")
    }));

    manifest.peer_pairings.clear();
    manifest.tool_selections.clear();
    let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
    let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
    let planned = diff_manifests(
        std::path::Path::new("/ownership-safe"),
        access.mode(),
        &manifest,
        live_principal.as_ref(),
        &live_manifest,
        false,
    );
    assert!(planned.collections.peer_pairings.delete.is_empty());
    let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
    let txn = access.begin_apply_txn().await?;
    apply_desired_state_changes(&txn, &bundle, &planned).await?;
    txn.commit().await?;
    let rows = crate::graphql_rows(
        &access,
        "PeerPairingDesired",
        "{ PeerPairingDesired { peer_id source } }",
    )
    .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["source"], "operator");
    Ok(())
}

#[tokio::test]
async fn preboot_pairing_apply_is_idempotent_and_restart_loader_consumes_seed() -> Result<()> {
    use std::sync::Arc;

    use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
    use crate::config_import::apply_desired_state_changes;
    use crate::desired_state::{diff_manifests, export_bundle_from_manifest};
    use gents::agent::p2p_reconcile::{
        reconcile_peer_tick, GraphqlPairingStateStore, PairingFilters, PairingStateStore,
        RemoteP2pAdmin, RemoteP2pAdminResult, RemoteReplicator,
    };
    use gents::KeyIdentity;

    let tempdir = tempfile::tempdir()?;
    let data_path = tempdir.path().join("data");
    let node = EmbeddedNode::builder()
        .data_path(&data_path)
        .with_storage_backend(StorageBackend::Lark)
        .build()
        .await?;
    ensure_runtime_schemas(&node).await?;
    let access = ConfigAccess::Local(std::sync::Arc::new(node));
    let peer_id = "bb".repeat(32);
    let address = format!("{peer_id}@127.0.0.1:4100");
    let mut manifest = manifest_with_subagent_targets(Vec::new());
    manifest.tool_selections.clear();
    manifest
        .peer_pairings
        .push(super::super::DesiredPeerPairing {
            peer_did: "did:key:remote".to_string(),
            addresses: vec![address.clone()],
            template: "conversation".to_string(),
            enabled: true,
            peer_id: peer_id.clone(),
        });

    let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
    let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
    let planned = diff_manifests(
        std::path::Path::new("/preboot"),
        access.mode(),
        &manifest,
        live_principal.as_ref(),
        &live_manifest,
        false,
    );
    assert_eq!(
        planned.collections.peer_pairings.create,
        vec![peer_id.clone()]
    );
    let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
    let txn = access.begin_apply_txn().await?;
    let counts = apply_desired_state_changes(&txn, &bundle, &planned).await?;
    txn.commit().await?;
    assert_eq!(counts.peer_pairings, 1);

    let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
    let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
    let noop = diff_manifests(
        std::path::Path::new("/preboot"),
        access.mode(),
        &manifest,
        live_principal.as_ref(),
        &live_manifest,
        false,
    );
    assert_eq!(
        noop.collections.peer_pairings.unchanged,
        vec![peer_id.clone()]
    );
    assert!(!noop.counts.has_pending_apply());
    let txn = access.begin_apply_txn().await?;
    let repeated = apply_desired_state_changes(&txn, &bundle, &noop).await?;
    txn.commit().await?;
    assert_eq!(repeated.peer_pairings, 0);
    drop(access);

    let identity = Arc::new(KeyIdentity::load_or_create(
        tempdir.path().join("restart-identity.key"),
        None,
    )?);
    let restarted_node = Arc::new(
        EmbeddedNode::builder()
            .data_path(&data_path)
            .with_storage_backend(StorageBackend::Lark)
            .build()
            .await?,
    );
    let restarted_store = GraphqlPairingStateStore::new(restarted_node.clone(), identity.clone());
    let loaded = restarted_store
        .load_desired(&peer_id)
        .await?
        .expect("seeded pairing is visible to restarted reconciler");
    assert!(loaded.replicator_addresses.contains(&address));

    #[derive(Default)]
    struct RestartAdmin {
        added_replicators: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl RemoteP2pAdmin for RestartAdmin {
        async fn peer_info(&self) -> RemoteP2pAdminResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn active_peers(&self) -> RemoteP2pAdminResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn connect(&self, _addresses: &[String]) -> RemoteP2pAdminResult<()> {
            Ok(())
        }
        async fn list_replicators(&self) -> RemoteP2pAdminResult<Vec<RemoteReplicator>> {
            Ok(self
                .added_replicators
                .lock()
                .unwrap()
                .iter()
                .map(|address| RemoteReplicator {
                    id: Some(address.clone()),
                    collections: Vec::new(),
                    address: Some(address.clone()),
                })
                .collect())
        }
        async fn add_replicator(
            &self,
            addresses: &[String],
            _collections: &[String],
            _filters: &PairingFilters,
        ) -> RemoteP2pAdminResult<()> {
            self.added_replicators
                .lock()
                .unwrap()
                .extend_from_slice(addresses);
            Ok(())
        }
        async fn delete_replicator(
            &self,
            id: &str,
            _collections: &[String],
        ) -> RemoteP2pAdminResult<()> {
            self.added_replicators
                .lock()
                .unwrap()
                .retain(|address| address != id);
            Ok(())
        }
        async fn list_p2p_collections(&self) -> RemoteP2pAdminResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn resolve_collection_id(&self, name: &str) -> RemoteP2pAdminResult<Option<String>> {
            Ok(Some(name.to_string()))
        }
        async fn resolve_collection_name(&self, id: &str) -> RemoteP2pAdminResult<Option<String>> {
            Ok(Some(id.to_string()))
        }
        async fn add_p2p_collections(&self, _collections: &[String]) -> RemoteP2pAdminResult<()> {
            Ok(())
        }
        async fn delete_p2p_collections(
            &self,
            _collections: &[String],
        ) -> RemoteP2pAdminResult<()> {
            Ok(())
        }
        async fn list_p2p_documents(&self) -> RemoteP2pAdminResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn add_p2p_documents(&self, _doc_ids: &[String]) -> RemoteP2pAdminResult<()> {
            Ok(())
        }
        async fn delete_p2p_documents(&self, _doc_ids: &[String]) -> RemoteP2pAdminResult<()> {
            Ok(())
        }
        async fn sync_documents(
            &self,
            _collection_name: &str,
            _doc_ids: &[String],
            _timeout: Option<std::time::Duration>,
        ) -> RemoteP2pAdminResult<()> {
            Ok(())
        }
        async fn sync_collection_versions(
            &self,
            _version_ids: &[String],
            _timeout: Option<std::time::Duration>,
        ) -> RemoteP2pAdminResult<()> {
            Ok(())
        }
        async fn sync_branchable_collection(
            &self,
            _collection_id: &str,
            _timeout: Option<std::time::Duration>,
        ) -> RemoteP2pAdminResult<()> {
            Ok(())
        }
    }

    let admin = RestartAdmin::default();
    let outcome = reconcile_peer_tick(&admin, &restarted_store, &peer_id).await?;
    assert!(!outcome.ops_applied.is_empty());
    assert_eq!(
        admin.added_replicators.lock().unwrap().as_slice(),
        &[address.clone()]
    );
    drop(restarted_store);
    drop(restarted_node);

    manifest.peer_pairings[0].enabled = false;
    let node = EmbeddedNode::builder()
        .data_path(&data_path)
        .with_storage_backend(StorageBackend::Lark)
        .build()
        .await?;
    let access = ConfigAccess::Local(std::sync::Arc::new(node));
    let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
    let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
    let removal = diff_manifests(
        std::path::Path::new("/preboot"),
        access.mode(),
        &manifest,
        live_principal.as_ref(),
        &live_manifest,
        false,
    );
    assert_eq!(
        removal.collections.peer_pairings.delete,
        vec![peer_id.clone()]
    );
    let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
    let txn = access.begin_apply_txn().await?;
    apply_desired_state_changes(&txn, &bundle, &removal).await?;
    txn.commit().await?;
    let rows = crate::graphql_rows(
        &access,
        "PeerPairingDesired",
        "{ PeerPairingDesired { peer_id } }",
    )
    .await?;
    assert!(rows.is_empty());
    drop(access);
    let removal_node = Arc::new(
        EmbeddedNode::builder()
            .data_path(&data_path)
            .with_storage_backend(StorageBackend::Lark)
            .build()
            .await?,
    );
    let removal_store = GraphqlPairingStateStore::new(removal_node, identity);
    let outcome = reconcile_peer_tick(&admin, &removal_store, &peer_id).await?;
    assert!(!outcome.ops_applied.is_empty());
    assert!(admin.added_replicators.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn all_subagent_fields_persist_and_apply_is_idempotent() -> Result<()> {
    use std::path::PathBuf;

    use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
    use crate::config_import::{apply_desired_state_changes, diff_has_pending_apply};
    use crate::desired_state::{diff_manifests, export_bundle_from_manifest};

    let tempdir = tempfile::tempdir()?;
    let data_dir = tempdir.path().join("data");
    let node = EmbeddedNode::builder()
        .data_path(&data_dir)
        .with_storage_backend(StorageBackend::Lark)
        .build()
        .await?;
    ensure_runtime_schemas(&node).await?;

    let access = ConfigAccess::Local(std::sync::Arc::new(node));

    {
        use gents::graphql::escape_graphql_string;
        let did = escape_graphql_string("did:key:test-subagent-idempotency");
        access
                .execute(&format!(
                    r#"mutation {{ create_AgentPrincipal(input: {{ agent_did: "{did}", enabled: true }}) {{ _docID }} }}"#
                ))
                .await?;
    }

    let desired_manifest = {
        use super::super::{DesiredAgentPrincipal, DesiredStateManifest, DesiredToolSelection};
        DesiredStateManifest {
            agent_principal: DesiredAgentPrincipal {
                agent_did: "did:key:test-subagent-idempotency".to_string(),
                display_name: None,
                default_behavior_id: None,
                enabled: true,
            },
            agent_behaviors: Vec::new(),
            skills: Vec::new(),
            datastore_tool_surfaces: Vec::new(),
            tool_selections: vec![DesiredToolSelection {
                selection_id: "subagent-idempotency-sel".to_string(),
                agent_did: "did:key:test-subagent-idempotency".to_string(),
                display_name: None,
                tool_policy_version: None,
                enable_file_tools: false,
                file_tools_mode: "ReadOnly".to_string(),
                file_tool_root: None,
                enable_bash: false,
                bash_mode: "ReadOnly".to_string(),
                command_execution_policy: None,
                command_allowed_argv_prefixes: Vec::new(),
                command_forbidden_argv_prefixes: Vec::new(),
                read_only_command_allowlist: Vec::new(),
                command_network_mode: None,
                cli_tool_names: Vec::new(),
                enable_meta_tools: false,
                allowed_mcp_service_ids: Vec::new(),
                delegate_to: Vec::new(),
                backgroundable_tool_names: Vec::new(),
                enable_memory: false,
                enable_session_history_tool: false,
                enable_context_budget: true,
                enable_defra_query: true,
                defra_query_collections: Vec::new(),
                subagent_targets: vec![SubagentTarget {
                    name: "researcher".to_string(),
                    agent_did: "did:key:test-subagent-idempotency".to_string(),
                    behavior_id: "amy-research".to_string(),
                    description: None,
                }
                .to_entry()],
                subagent_spawn_enabled: true,
                subagent_steering_enabled: true,
                subagent_background_enabled: true,
                subagent_default_await_mode: Some("background".to_string()),
                subagent_allow_cross_deployment: true,
                cross_deployment_spawn_timeout_seconds: Some(90),
                write_tools: Vec::new(),
                datastore_tool_surface_ids: Vec::new(),
                enable_self_config: false,
                self_config_categories: Vec::new(),
                self_config_no_lockout: false,
                self_config_dry_run: false,
                enable_lsp: false,
                lsp_config: None,
            }],
            inference_backends: Vec::new(),
            inference_profiles: Vec::new(),
            tool_service_registries: Vec::new(),
            projection_acp_bindings: Vec::new(),
            peer_pairings: Vec::new(),
            tasks: Vec::new(),
            schedules: Vec::new(),
            event_triggers: Vec::new(),
        }
    };

    let root = PathBuf::from(".");
    let desired_bundle = export_bundle_from_manifest(&desired_manifest, "local")?;

    let live_bundle = build_desired_state_live_bundle(&access, &desired_manifest).await?;
    let (live_principal, live_manifest) =
        live_manifest_from_bundle(&desired_manifest, &live_bundle)?;
    let planned = diff_manifests(
        &root,
        "local",
        &desired_manifest,
        live_principal.as_ref(),
        &live_manifest,
        false,
    );

    let txn = access.begin_apply_txn().await?;
    apply_desired_state_changes(&txn, &desired_bundle, &planned).await?;
    txn.commit().await?;

    let remaining_bundle = build_desired_state_live_bundle(&access, &desired_manifest).await?;
    let (remaining_principal, remaining_manifest) =
        live_manifest_from_bundle(&desired_manifest, &remaining_bundle)?;

    let live_sel = remaining_manifest
        .tool_selections
        .iter()
        .find(|s| s.selection_id == "subagent-idempotency-sel")
        .expect("ToolSelection should exist after apply");

    assert_eq!(
        live_sel.subagent_targets,
        vec![SubagentTarget {
            name: "researcher".to_string(),
            agent_did: "did:key:test-subagent-idempotency".to_string(),
            behavior_id: "amy-research".to_string(),
            description: None,
        }
        .to_entry()],
        "subagent_targets must persist through apply"
    );
    assert_eq!(
        live_sel.subagent_spawn_enabled, true,
        "subagent_spawn_enabled must persist through apply"
    );
    assert_eq!(
        live_sel.subagent_steering_enabled, true,
        "subagent_steering_enabled must persist through apply"
    );
    assert_eq!(
        live_sel.subagent_background_enabled, true,
        "subagent_background_enabled must persist through apply"
    );
    assert_eq!(
        live_sel.subagent_default_await_mode.as_deref(),
        Some("background"),
        "subagent_default_await_mode must persist through apply"
    );
    assert_eq!(
        live_sel.subagent_allow_cross_deployment, true,
        "subagent_allow_cross_deployment must persist through apply"
    );
    assert_eq!(
        live_sel.cross_deployment_spawn_timeout_seconds,
        Some(90),
        "cross_deployment_spawn_timeout_seconds must persist through apply"
    );

    let second_diff = diff_manifests(
        &root,
        "local",
        &desired_manifest,
        remaining_principal.as_ref(),
        &remaining_manifest,
        false,
    );

    assert!(
        !diff_has_pending_apply(&second_diff.counts),
        "second diff must have no pending apply (idempotent); got: {:?}",
        second_diff.counts
    );
    assert!(
        second_diff
            .collections
            .tool_selections
            .unchanged
            .contains(&"subagent-idempotency-sel".to_string()),
        "tool selection must be in the 'unchanged' set after re-apply; got: {:?}",
        second_diff.collections.tool_selections
    );

    Ok(())
}

#[tokio::test]
async fn behavior_description_and_summary_persist_and_apply_is_idempotent() -> Result<()> {
    use std::path::PathBuf;

    use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
    use crate::config_import::{apply_desired_state_changes, diff_has_pending_apply};
    use crate::desired_state::{diff_manifests, export_bundle_from_manifest};

    let tempdir = tempfile::tempdir()?;
    let data_dir = tempdir.path().join("data");
    let node = EmbeddedNode::builder()
        .data_path(&data_dir)
        .with_storage_backend(StorageBackend::Lark)
        .build()
        .await?;
    ensure_runtime_schemas(&node).await?;

    let access = ConfigAccess::Local(std::sync::Arc::new(node));

    {
        use gents::graphql::escape_graphql_string;
        let did = escape_graphql_string("did:key:test-behavior-desc-idempotency");
        access
                .execute(&format!(
                    r#"mutation {{ create_AgentPrincipal(input: {{ agent_did: "{did}", enabled: true }}) {{ _docID }} }}"#
                ))
                .await?;
    }

    let desired_manifest = {
        use super::super::{DesiredAgentBehavior, DesiredAgentPrincipal, DesiredStateManifest};
        DesiredStateManifest {
            agent_principal: DesiredAgentPrincipal {
                agent_did: "did:key:test-behavior-desc-idempotency".to_string(),
                display_name: None,
                default_behavior_id: None,
                enabled: true,
            },
            agent_behaviors: vec![DesiredAgentBehavior {
                behavior_id: "desc-idempotency-behavior".to_string(),
                agent_did: "did:key:test-behavior-desc-idempotency".to_string(),
                display_name: Some("Research Assistant".to_string()),
                description: Some(
                    "A general-purpose assistant for research and writing tasks.".to_string(),
                ),
                summary: Some("Research assistant".to_string()),
                system_prompt: None,
                request_context_template: None,
                backend_id: None,
                model_name: None,
                tool_selection_id: None,
                inference_profile_id: None,
                compaction_strategy: None,
                compaction_threshold: None,
                enabled: true,
                skill_refs: Vec::new(),
                skill_excludes: Vec::new(),
            }],
            skills: Vec::new(),
            datastore_tool_surfaces: Vec::new(),
            tool_selections: Vec::new(),
            inference_backends: Vec::new(),
            inference_profiles: Vec::new(),
            tool_service_registries: Vec::new(),
            projection_acp_bindings: Vec::new(),
            peer_pairings: Vec::new(),
            tasks: Vec::new(),
            schedules: Vec::new(),
            event_triggers: Vec::new(),
        }
    };

    let root = PathBuf::from(".");
    let desired_bundle = export_bundle_from_manifest(&desired_manifest, "local")?;

    let live_bundle = build_desired_state_live_bundle(&access, &desired_manifest).await?;
    let (live_principal, live_manifest) =
        live_manifest_from_bundle(&desired_manifest, &live_bundle)?;
    let planned = diff_manifests(
        &root,
        "local",
        &desired_manifest,
        live_principal.as_ref(),
        &live_manifest,
        false,
    );

    let txn = access.begin_apply_txn().await?;
    apply_desired_state_changes(&txn, &desired_bundle, &planned).await?;
    txn.commit().await?;

    let remaining_bundle = build_desired_state_live_bundle(&access, &desired_manifest).await?;
    let (remaining_principal, remaining_manifest) =
        live_manifest_from_bundle(&desired_manifest, &remaining_bundle)?;

    let live_behavior = remaining_manifest
        .agent_behaviors
        .iter()
        .find(|b| b.behavior_id == "desc-idempotency-behavior")
        .expect("AgentBehavior should exist after apply");

    assert_eq!(
        live_behavior.description,
        Some("A general-purpose assistant for research and writing tasks.".to_string()),
        "description must persist through apply"
    );
    assert_eq!(
        live_behavior.summary,
        Some("Research assistant".to_string()),
        "summary must persist through apply"
    );

    let second_diff = diff_manifests(
        &root,
        "local",
        &desired_manifest,
        remaining_principal.as_ref(),
        &remaining_manifest,
        false,
    );

    assert!(
        !diff_has_pending_apply(&second_diff.counts),
        "second diff must have no pending apply (idempotent); got: {:?}",
        second_diff.counts
    );
    assert!(
        second_diff
            .collections
            .agent_behaviors
            .unchanged
            .contains(&"desc-idempotency-behavior".to_string()),
        "behavior must be in the 'unchanged' set after re-apply; got: {:?}",
        second_diff.collections.agent_behaviors
    );

    Ok(())
}

fn backend_entry(backend_id: &str) -> super::super::DesiredInferenceBackend {
    super::super::DesiredInferenceBackend {
        backend_id: backend_id.to_string(),
        name: backend_id.to_string(),
        provider_kind: Default::default(),
        openai_wire_api: None,
        endpoint: "http://127.0.0.1:9990/v1".to_string(),
        api_key: None,
        api_key_env_var: None,
        max_concurrent: 1,
        max_queue_depth: 8,
        enabled: true,
        models: Vec::new(),
    }
}

/// Regression test for #981: a live InferenceBackend absent from the
/// manifest (e.g. after a backend rename) must be reported live_only and
/// deleted by prune, even when no behavior references it.
#[tokio::test]
async fn diff_prune_detects_and_deletes_live_only_inference_backends() -> Result<()> {
    use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
    use crate::config_import::apply_desired_state_changes;
    use crate::desired_state::{diff_manifests, export_bundle_from_manifest};

    let tempdir = tempfile::tempdir()?;
    let node = EmbeddedNode::builder()
        .data_path(tempdir.path().join("data"))
        .with_storage_backend(StorageBackend::Lark)
        .build()
        .await?;
    ensure_runtime_schemas(&node).await?;
    let access = ConfigAccess::Local(std::sync::Arc::new(node));

    let mut manifest = manifest_with_subagent_targets(Vec::new());
    manifest.tool_selections.clear();
    manifest.inference_backends = vec![
        backend_entry("openai-sol-high"),
        backend_entry("openai-terra"),
    ];

    let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
    let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
    let planned = diff_manifests(
        std::path::Path::new("/backend-prune"),
        access.mode(),
        &manifest,
        live_principal.as_ref(),
        &live_manifest,
        false,
    );
    let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
    let txn = access.begin_apply_txn().await?;
    apply_desired_state_changes(&txn, &bundle, &planned).await?;
    txn.commit().await?;

    // Rename openai-sol-high -> openai-sol in the manifest; the live
    // document for the old id is now referenced by nothing.
    manifest.inference_backends = vec![backend_entry("openai-sol"), backend_entry("openai-terra")];

    let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
    let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
    let drift = diff_manifests(
        std::path::Path::new("/backend-prune"),
        access.mode(),
        &manifest,
        live_principal.as_ref(),
        &live_manifest,
        false,
    );
    assert_eq!(
        drift.collections.inference_backends.live_only,
        vec!["openai-sol-high".to_string()],
        "stale backend must be reported live_only; got: {:?}",
        drift.collections.inference_backends
    );

    let planned = diff_manifests(
        std::path::Path::new("/backend-prune"),
        access.mode(),
        &manifest,
        live_principal.as_ref(),
        &live_manifest,
        true,
    );
    assert_eq!(
        planned.collections.inference_backends.delete,
        vec!["openai-sol-high".to_string()],
        "prune must plan the stale backend for deletion; got: {:?}",
        planned.collections.inference_backends
    );
    let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
    let txn = access.begin_apply_txn().await?;
    apply_desired_state_changes(&txn, &bundle, &planned).await?;
    txn.commit().await?;

    let rows = crate::graphql_rows(
        &access,
        "InferenceBackend",
        "{ InferenceBackend { backend_id } }",
    )
    .await?;
    let mut ids = rows
        .iter()
        .filter_map(|row| row.get("backend_id").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(
        ids,
        vec!["openai-sol".to_string(), "openai-terra".to_string()]
    );
    Ok(())
}

/// InferenceBackend documents are node-global: a backend referenced by
/// another agent's behavior must never be treated as live_only (or
/// pruned) by this agent's manifest, while a backend referenced by no
/// one remains prunable.
#[tokio::test]
async fn prune_spares_backends_referenced_by_other_agents() -> Result<()> {
    use crate::config_bundle::{build_desired_state_live_bundle, live_manifest_from_bundle};
    use crate::config_import::apply_desired_state_changes;
    use crate::desired_state::{diff_manifests, export_bundle_from_manifest};

    let tempdir = tempfile::tempdir()?;
    let node = EmbeddedNode::builder()
        .data_path(tempdir.path().join("data"))
        .with_storage_backend(StorageBackend::Lark)
        .build()
        .await?;
    ensure_runtime_schemas(&node).await?;
    let access = ConfigAccess::Local(std::sync::Arc::new(node));

    let mut manifest = manifest_with_subagent_targets(Vec::new());
    manifest.tool_selections.clear();
    manifest.inference_backends = vec![backend_entry("openai-sol")];

    let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
    let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
    let planned = diff_manifests(
        std::path::Path::new("/backend-prune-foreign"),
        access.mode(),
        &manifest,
        live_principal.as_ref(),
        &live_manifest,
        false,
    );
    let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
    let txn = access.begin_apply_txn().await?;
    apply_desired_state_changes(&txn, &bundle, &planned).await?;
    txn.commit().await?;

    access
        .execute(
            r#"mutation { create_InferenceBackend(input: {
                    backend_id: "other-agent-backend",
                    name: "other-agent-backend",
                    endpoint: "http://127.0.0.1:9991/v1",
                    max_concurrent: 1,
                    max_queue_depth: 8,
                    enabled: true
                }) { _docID } }"#,
        )
        .await?;
    access
        .execute(
            r#"mutation { create_AgentBehavior(input: {
                    behavior_id: "other-agent-behavior",
                    agent_did: "did:key:some-other-agent",
                    backend_id: "other-agent-backend",
                    enabled: true
                }) { _docID } }"#,
        )
        .await?;
    access
        .execute(
            r#"mutation { create_InferenceBackend(input: {
                    backend_id: "stale-backend",
                    name: "stale-backend",
                    endpoint: "http://127.0.0.1:9992/v1",
                    max_concurrent: 1,
                    max_queue_depth: 8,
                    enabled: true
                }) { _docID } }"#,
        )
        .await?;

    let live_bundle = build_desired_state_live_bundle(&access, &manifest).await?;
    let (live_principal, live_manifest) = live_manifest_from_bundle(&manifest, &live_bundle)?;
    let planned = diff_manifests(
        std::path::Path::new("/backend-prune-foreign"),
        access.mode(),
        &manifest,
        live_principal.as_ref(),
        &live_manifest,
        true,
    );
    assert_eq!(
        planned.collections.inference_backends.delete,
        vec!["stale-backend".to_string()],
        "only the unreferenced backend may be planned for deletion; got: {:?}",
        planned.collections.inference_backends
    );
    assert!(
        planned.collections.inference_backends.live_only.is_empty(),
        "the foreign-referenced backend must not appear live_only; got: {:?}",
        planned.collections.inference_backends
    );

    let bundle = export_bundle_from_manifest(&manifest, access.mode())?;
    let txn = access.begin_apply_txn().await?;
    apply_desired_state_changes(&txn, &bundle, &planned).await?;
    txn.commit().await?;

    let rows = crate::graphql_rows(
        &access,
        "InferenceBackend",
        "{ InferenceBackend { backend_id } }",
    )
    .await?;
    let mut ids = rows
        .iter()
        .filter_map(|row| row.get("backend_id").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(
        ids,
        vec!["openai-sol".to_string(), "other-agent-backend".to_string()]
    );
    Ok(())
}
