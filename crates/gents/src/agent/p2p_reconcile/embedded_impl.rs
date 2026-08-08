//! Embedded-node implementation of the runtime pairing admin seam.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use defra_p2p_adapter::{
    P2PError, P2PResult, P2pDocumentRequest, ReplicationFilter, ReplicationFilters,
};
use tokio::time::timeout;

use crate::defra_node::EmbeddedNode;

use super::templates::PairingFilters;
use super::{RemoteP2pAdmin, RemoteP2pAdminError, RemoteP2pAdminResult, RemoteReplicator};

const DEFAULT_EMBEDDED_ADMIN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct EmbeddedRemoteP2pAdmin {
    node: Arc<EmbeddedNode>,
    timeout: Duration,
}

impl EmbeddedRemoteP2pAdmin {
    pub fn new(node: Arc<EmbeddedNode>) -> Self {
        Self {
            node,
            timeout: DEFAULT_EMBEDDED_ADMIN_TIMEOUT,
        }
    }

    pub fn with_timeout(node: Arc<EmbeddedNode>, timeout: Duration) -> Self {
        Self { node, timeout }
    }

    fn p2p(&self) -> RemoteP2pAdminResult<Arc<dyn defra_p2p_adapter::P2POperations>> {
        self.node.p2p_arc().ok_or_else(|| {
            RemoteP2pAdminError::LocalError("embedded node has no P2P transport".into())
        })
    }

    async fn run<T, F>(&self, operation: &'static str, future: F) -> RemoteP2pAdminResult<T>
    where
        F: Future<Output = P2PResult<T>>,
    {
        match timeout(self.timeout, future).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(map_p2p_error(operation, error)),
            Err(_) => Err(RemoteP2pAdminError::RpcTimeout),
        }
    }
}

#[async_trait]
impl RemoteP2pAdmin for EmbeddedRemoteP2pAdmin {
    async fn peer_info(&self) -> RemoteP2pAdminResult<Vec<String>> {
        let p2p = self.p2p()?;
        let peer_id = self.run("local_peer_id", p2p.local_peer_id()).await?;
        let addresses = self.run("listen_addresses", p2p.listen_addresses()).await?;
        Ok(addresses
            .into_iter()
            .map(|addr| {
                if addr.starts_with('/') {
                    format!("{addr}/p2p/{peer_id}")
                } else {
                    addr
                }
            })
            .collect())
    }

    async fn active_peers(&self) -> RemoteP2pAdminResult<Vec<String>> {
        let p2p = self.p2p()?;
        self.run("connected_peers", p2p.connected_peers()).await
    }

    async fn connect(&self, addresses: &[String]) -> RemoteP2pAdminResult<()> {
        let p2p = self.p2p()?;
        for addr in addresses {
            self.run("connect_peer", p2p.connect_peer(addr)).await?;
        }
        Ok(())
    }

    async fn list_replicators(&self) -> RemoteP2pAdminResult<Vec<RemoteReplicator>> {
        let p2p = self.p2p()?;
        let replicators = self.run("get_replicators", p2p.get_replicators()).await?;
        Ok(replicators
            .into_iter()
            .map(|r| RemoteReplicator {
                id: r.id,
                collections: r.collections,
                address: r.address,
            })
            .collect())
    }

    async fn add_replicator(
        &self,
        addresses: &[String],
        collections: &[String],
        filters: &PairingFilters,
    ) -> RemoteP2pAdminResult<()> {
        let p2p = self.p2p()?;
        let addr = addresses.first().map(String::as_str);
        let defra_filters = to_defra_filters(filters);
        self.run(
            "add_replicator",
            p2p.add_replicator(collections.to_vec(), addr, defra_filters, Vec::new(), None),
        )
        .await
    }

    async fn delete_replicator(
        &self,
        id: &str,
        collections: &[String],
    ) -> RemoteP2pAdminResult<()> {
        let p2p = self.p2p()?;
        let addr = (!id.trim().is_empty()).then_some(id);
        self.run(
            "remove_replicator",
            p2p.remove_replicator(collections.to_vec(), addr),
        )
        .await
    }

    async fn list_p2p_collections(&self) -> RemoteP2pAdminResult<Vec<String>> {
        let p2p = self.p2p()?;
        self.run("get_collections", p2p.get_collections()).await
    }

    async fn resolve_collection_id(&self, name: &str) -> RemoteP2pAdminResult<Option<String>> {
        match self.node.get_collection(name) {
            Ok(Some(def)) => Ok(Some(def.collection_id)),
            Ok(None) => Ok(None),
            Err(error) => Err(RemoteP2pAdminError::LocalError(format!(
                "resolve_collection_id({name}): {error}"
            ))),
        }
    }

    async fn resolve_collection_name(&self, id: &str) -> RemoteP2pAdminResult<Option<String>> {
        let names = self.node.list_collections().map_err(|error| {
            RemoteP2pAdminError::LocalError(format!("list_collections for id {id}: {error}"))
        })?;
        for name in names {
            match self.node.get_collection(&name) {
                Ok(Some(def)) if def.collection_id == id => return Ok(Some(def.name)),
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        collection_name = %name,
                        %error,
                        "resolve_collection_name failed to fetch a collection definition"
                    );
                }
            }
        }
        Ok(None)
    }

    async fn add_p2p_collections(&self, collections: &[String]) -> RemoteP2pAdminResult<()> {
        let p2p = self.p2p()?;
        self.run("add_collections", p2p.add_collections(collections.to_vec()))
            .await
    }

    async fn delete_p2p_collections(&self, collections: &[String]) -> RemoteP2pAdminResult<()> {
        let p2p = self.p2p()?;
        self.run(
            "remove_collections",
            p2p.remove_collections(collections.to_vec()),
        )
        .await
    }

    async fn list_p2p_documents(&self) -> RemoteP2pAdminResult<Vec<String>> {
        let p2p = self.p2p()?;
        let documents = self.run("get_documents", p2p.get_documents()).await?;
        Ok(documents.into_iter().map(|d| d.doc_id).collect())
    }

    async fn add_p2p_documents(&self, doc_ids: &[String]) -> RemoteP2pAdminResult<()> {
        let p2p = self.p2p()?;
        let docs = document_requests(doc_ids);
        self.run("add_documents", p2p.add_documents(docs)).await
    }

    async fn delete_p2p_documents(&self, doc_ids: &[String]) -> RemoteP2pAdminResult<()> {
        let p2p = self.p2p()?;
        let docs = document_requests(doc_ids);
        self.run("remove_documents", p2p.remove_documents(docs))
            .await
    }

    async fn sync_documents(
        &self,
        collection_name: &str,
        doc_ids: &[String],
        timeout_override: Option<Duration>,
    ) -> RemoteP2pAdminResult<()> {
        let p2p = self.p2p()?;
        let future = p2p.sync_documents(collection_name, doc_ids.to_vec());
        match timeout(timeout_override.unwrap_or(self.timeout), future).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(map_p2p_error("sync_documents", error)),
            Err(_) => Err(RemoteP2pAdminError::RpcTimeout),
        }
    }

    async fn sync_collection_versions(
        &self,
        version_ids: &[String],
        timeout_override: Option<Duration>,
    ) -> RemoteP2pAdminResult<()> {
        let p2p = self.p2p()?;
        let future = p2p.sync_collection_versions(version_ids.to_vec());
        match timeout(timeout_override.unwrap_or(self.timeout), future).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(map_p2p_error("sync_collection_versions", error)),
            Err(_) => Err(RemoteP2pAdminError::RpcTimeout),
        }
    }

    async fn sync_branchable_collection(
        &self,
        collection_id: &str,
        timeout_override: Option<Duration>,
    ) -> RemoteP2pAdminResult<()> {
        let p2p = self.p2p()?;
        let future = p2p.sync_branchable_collection(collection_id);
        match timeout(timeout_override.unwrap_or(self.timeout), future).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(map_p2p_error("sync_branchable_collection", error)),
            Err(_) => Err(RemoteP2pAdminError::RpcTimeout),
        }
    }
}

fn to_defra_filters(filters: &PairingFilters) -> ReplicationFilters {
    filters
        .iter()
        .map(|(collection, predicate)| {
            (
                collection.clone(),
                ReplicationFilter {
                    field: predicate.field.clone(),
                    value: serde_json::Value::String(predicate.value.clone()),
                    conditions: None,
                },
            )
        })
        .collect()
}

fn document_requests(doc_ids: &[String]) -> Vec<P2pDocumentRequest> {
    doc_ids
        .iter()
        .cloned()
        .map(|doc_id| P2pDocumentRequest {
            collection: String::new(),
            doc_id,
        })
        .collect()
}

fn map_p2p_error(operation: &'static str, error: P2PError) -> RemoteP2pAdminError {
    match error {
        P2PError::NotFound(message) => RemoteP2pAdminError::RemoteNotFound(message),
        P2PError::Transport(message) | P2PError::Internal(message) => {
            RemoteP2pAdminError::RpcError(format!("{operation}: {message}"))
        }
        P2PError::InvalidInput(message) | P2PError::Unsupported(message) => {
            RemoteP2pAdminError::LocalError(format!("{operation}: {message}"))
        }
        _ => RemoteP2pAdminError::RpcError(format!("{operation}: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::net::{IpAddr, Ipv4Addr};

    use p2p::iroh::{IrohDiscoveryConfig, IrohRelayModeConfig};

    use super::*;
    use crate::agent::p2p_reconcile::templates::{Scope, SUBAGENT_HOST_TEMPLATE};
    use crate::agent::p2p_reconcile::{resolve_template, scope_filter, FilterPredicate};
    use crate::defra_node::P2PConfig;
    use crate::ensure_runtime_schemas;
    use crate::graphql::escape_graphql_string;

    const TEST_SCHEMA: &str = r#"
        type P2pReconcileThing {
            name: String
        }
    "#;

    struct TestNode {
        node: Arc<EmbeddedNode>,
        _tempdir: tempfile::TempDir,
        _signed_identity: crate::test_support::SignedTestIdentity,
    }

    async fn p2p_node() -> TestNode {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let signed_identity = crate::test_support::signed_test_identity("p2p-reconcile-identity");
        let node = Arc::new(
            EmbeddedNode::builder()
                .data_path(tempdir.path())
                .with_node_identity_did(signed_identity.did())
                .with_p2p(P2PConfig {
                    port: 0,
                    bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                    relay_mode: IrohRelayModeConfig::Disabled,
                    discovery: IrohDiscoveryConfig::Disabled,
                    max_concurrent_multipath_paths: None,
                    secret_key_path: None,
                    load_persisted_collections: false,
                    max_concurrent_dag_fetches: p2p::sync::DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
                    max_concurrent_push_tasks: p2p::sync::DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
                    rate_limit_burst: p2p::sync::DEFAULT_RATE_LIMIT_BURST,
                    rate_limit_rate: p2p::sync::DEFAULT_RATE_LIMIT_RATE,
                    max_doc_sync_request_doc_ids: p2p::sync::DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
                    max_pending_dags: p2p::sync::DEFAULT_MAX_PENDING_DAGS,
                })
                .build()
                .await
                .expect("embedded p2p node"),
        );
        TestNode {
            node,
            _tempdir: tempdir,
            _signed_identity: signed_identity,
        }
    }

    async fn test_node() -> TestNode {
        let test = p2p_node().await;
        test.node
            .add_schema(TEST_SCHEMA)
            .await
            .expect("test schema");
        test
    }

    async fn runtime_test_node() -> TestNode {
        let test = p2p_node().await;
        ensure_runtime_schemas(&test.node)
            .await
            .expect("runtime schemas");
        test
    }

    async fn wait_for_peer_info(admin: &EmbeddedRemoteP2pAdmin) -> Vec<String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let addresses = admin.peer_info().await.expect("peer info");
            if !addresses.is_empty() {
                return addresses;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("node never exposed a P2P address");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn collection_id(node: &EmbeddedNode, name: &str) -> String {
        node.get_collection(name)
            .expect("collection lookup")
            .expect("collection")
            .collection_id
    }

    async fn wait_for_active_peer(admin: &EmbeddedRemoteP2pAdmin) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let peers = admin.active_peers().await.expect("active peers");
            if !peers.is_empty() {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("node never reported an active P2P peer");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn exec(node: &EmbeddedNode, statement: &str, context: &str) {
        let response = node.execute(statement).await;
        assert!(
            !response.has_errors(),
            "{context} failed: {:?}\n{statement}",
            response.errors
        );
    }

    async fn seed_agent_request(node: &EmbeddedNode, request_id: &str, requester_did: &str) {
        let request_id = escape_graphql_string(request_id);
        let requester_did = escape_graphql_string(requester_did);
        let agent_did = "did:key:host";
        let session_id = escape_graphql_string(&format!("{request_id}-session"));
        let behavior_id = escape_graphql_string(&format!("{agent_did}:default"));
        let mutation = format!(
            r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{request_id}",
                    agent_did: "{agent_did}",
                    requester_did: "{requester_did}",
                    behavior_id: "{behavior_id}",
                    session_id: "{session_id}",
                    retry_parent_request: "",
                    retry_root_request: "{request_id}",
                    superseded_by_request: "",
                    content: "issue 604 filtered replay",
                    status: "processing",
                    lifecycle_state: "processing",
                    backend_id: "",
                    execution_origin: "interactive",
                    failure_reason: "",
                    created_at: "2026-07-06T00:00:00Z",
                    deadline: "2026-07-06T01:00:00Z",
                    retry_count: 0,
                    max_retries: 3,
                    subagent_depth: 0
                }}) {{ _docID }}
            }}"#
        );
        exec(node, &mutation, "seed AgentRequest").await;
    }

    async fn seed_agent_tool_call(node: &EmbeddedNode, tool_call_id: &str, spawn_target_did: &str) {
        let tool_call_id = escape_graphql_string(tool_call_id);
        let spawn_target_did = escape_graphql_string(spawn_target_did);
        let tool_call_key = escape_graphql_string(&format!("issue-604-session:{tool_call_id}"));
        let mutation = format!(
            r#"mutation {{
                create_AgentToolCall(input: {{
                    tool_call_key: "{tool_call_key}",
                    request_id: "parent-match",
                    session_id: "issue-604-session",
                    agent_did: "did:key:coord",
                    message_sequence: 1,
                    tool_name: "spawn_subagent",
                    tool_call_id: "{tool_call_id}",
                    args: "{{}}",
                    result: "",
                    status: "called",
                    lifecycle_state: "running",
                    started_at: "2026-07-06T00:00:00Z",
                    await_mode: "background",
                    cancel_policy: "cascade",
                    child_request_id: "child-{tool_call_id}",
                    spawn_target_did: "{spawn_target_did}"
                }}) {{ _docID }}
            }}"#
        );
        exec(node, &mutation, "seed AgentToolCall").await;
    }

    async fn seed_subagent_return_artifacts(
        node: &Arc<EmbeddedNode>,
        suffix: &str,
        requester_did: Option<&str>,
    ) {
        let request_id = format!("return-{suffix}-response");
        let session_id = format!("return-{suffix}-session");
        let agent_did = "did:key:host";
        let behavior_id = "did:key:host:default";

        crate::session::ensure_session_with_behavior_id_and_requester_did(
            node,
            &session_id,
            "default",
            agent_did,
            behavior_id,
            requester_did,
        )
        .await
        .expect("create routed AgentSession");
        crate::session::upsert_conversation_from_request_with_identity_and_requester_did(
            node,
            &session_id,
            "default",
            agent_did,
            behavior_id,
            &request_id,
            "child prompt",
            "completed",
            requester_did,
        )
        .await
        .expect("create routed AgentConversation");
        crate::session::save_message_with_requester_did(
            node,
            &session_id,
            agent_did,
            requester_did,
            1,
            "assistant",
            "child result",
            None,
        )
        .await
        .expect("create routed AgentMessage");
        crate::streaming::DefraStreamWriter::new(
            Arc::clone(node),
            agent_did,
            Duration::from_secs(60),
        )
        .begin_with_requester_did(&session_id, &request_id, behavior_id, requester_did)
        .await
        .expect("create routed AgentResponse");
    }

    async fn collection_values(
        node: &EmbeddedNode,
        collection: &str,
        field: &str,
    ) -> BTreeSet<String> {
        let query = format!("{{ {collection} {{ {field} }} }}");
        let response = node.execute(&query).await;
        assert!(
            !response.has_errors(),
            "query {collection}.{field} failed: {:?}",
            response.errors
        );
        response
            .data
            .as_ref()
            .and_then(|data| data.get(collection))
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|row| row.get(field).and_then(serde_json::Value::as_str))
            .map(ToOwned::to_owned)
            .collect()
    }

    async fn wait_for_value(node: &EmbeddedNode, collection: &str, field: &str, expected: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let last = collection_values(node, collection, field).await;
            if last.contains(expected) {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for {collection}.{field}={expected}; last={last:?}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    #[tokio::test]
    async fn embedded_collections_round_trip() {
        let test = test_node().await;
        let node = Arc::clone(&test.node);
        let admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&node));
        let collection_name = "P2pReconcileThing".to_string();
        let expected_collection_id = collection_id(&node, "P2pReconcileThing");

        admin
            .add_p2p_collections(std::slice::from_ref(&collection_name))
            .await
            .expect("add collection");

        let collections = admin
            .list_p2p_collections()
            .await
            .expect("list collections");
        assert!(collections.contains(&expected_collection_id));

        node.shutdown().await;
    }

    /// Catalog-wide fence for defradb's replication-filter validation: every
    /// builtin template's filter fields must be `@immutable` in the target
    /// collection's schema, or `add_replicator` rejects the install at
    /// pairing time (the exact failure that shipped in #873's merge: the
    /// machine template filters `AgentDirectoryEntry.source_did`, which the
    /// amended schema left mutable). Runs against real runtime schemas so a
    /// new template or filter rule cannot pass review while violating the
    /// constraint defradb only enforces at install time.
    #[tokio::test]
    async fn embedded_all_builtin_template_filters_pass_replicator_validation() {
        let local_test = runtime_test_node().await;
        let remote_test = runtime_test_node().await;
        let local = Arc::clone(&local_test.node);
        let remote = Arc::clone(&remote_test.node);
        let local_admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&local));
        let remote_admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&remote));
        let remote_addresses = wait_for_peer_info(&remote_admin).await;
        local_admin
            .connect(&remote_addresses)
            .await
            .expect("connect remote");
        wait_for_active_peer(&local_admin).await;
        wait_for_active_peer(&remote_admin).await;

        for template in super::super::templates::builtin_templates() {
            if template.collections.is_empty() {
                continue; // app-collections: bring-your-own collection set.
            }
            let filters = super::super::templates::scope_filter(
                &template.scope,
                template.collections,
                "did:key:z6MkPeerForFilterValidation",
                "did:key:z6MkSelfForFilterValidation",
            );
            match &template.scope {
                Scope::Unscoped => assert!(
                    filters.is_empty(),
                    "unscoped template '{}' unexpectedly has filters",
                    template.id
                ),
                Scope::PerCollection(rules) => {
                    let filter_collections =
                        filters.keys().map(String::as_str).collect::<BTreeSet<_>>();
                    let scoped_collections = rules
                        .iter()
                        .map(|rule| rule.collection)
                        .collect::<BTreeSet<_>>();
                    assert_eq!(
                        filter_collections, scoped_collections,
                        "per-collection template '{}' must filter every scoped collection",
                        template.id
                    );
                }
                Scope::PeerDid { .. } => {}
            }
            local_admin
                .add_replicator(
                    &remote_addresses,
                    &template
                        .collections
                        .iter()
                        .map(|c| (*c).to_string())
                        .collect::<Vec<_>>(),
                    &filters,
                )
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "template '{}' filter set rejected by add_replicator: {error:#}",
                        template.id
                    )
                });
        }

        local.shutdown().await;
        remote.shutdown().await;
    }

    #[tokio::test]
    async fn embedded_replicators_round_trip() {
        let local_test = test_node().await;
        let remote_test = test_node().await;
        let local = Arc::clone(&local_test.node);
        let remote = Arc::clone(&remote_test.node);
        let local_admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&local));
        let remote_admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&remote));
        let remote_addresses = wait_for_peer_info(&remote_admin).await;
        let collection_name = "P2pReconcileThing".to_string();
        let expected_collection_id = collection_id(&local, "P2pReconcileThing");

        local_admin
            .connect(&remote_addresses)
            .await
            .expect("connect remote");
        local_admin
            .add_replicator(
                &remote_addresses,
                std::slice::from_ref(&collection_name),
                &PairingFilters::default(),
            )
            .await
            .expect("add replicator");

        let replicators = local_admin
            .list_replicators()
            .await
            .expect("list replicators");
        assert!(
            replicators
                .iter()
                .any(|r| r.collections.contains(&expected_collection_id)),
            "replicators={replicators:?}"
        );

        local.shutdown().await;
        remote.shutdown().await;
    }

    #[tokio::test]
    async fn embedded_filtered_replicator_replays_existing_matching_documents() {
        let sender_test = runtime_test_node().await;
        let receiver_test = runtime_test_node().await;
        let sender = Arc::clone(&sender_test.node);
        let receiver = Arc::clone(&receiver_test.node);
        let sender_admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&sender));
        let receiver_admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&receiver));
        let sender_addresses = wait_for_peer_info(&sender_admin).await;
        let receiver_addresses = wait_for_peer_info(&receiver_admin).await;

        seed_agent_request(&sender, "child-match", "did:key:coord").await;
        seed_agent_request(&sender, "child-other", "did:key:other").await;
        seed_agent_tool_call(&sender, "bridge-match", "did:key:host").await;
        seed_agent_tool_call(&sender, "bridge-other", "did:key:other").await;

        let collections = vec!["AgentRequest".to_string(), "AgentToolCall".to_string()];
        sender_admin
            .add_p2p_collections(&collections)
            .await
            .expect("add sender p2p collections");
        receiver_admin
            .add_p2p_collections(&collections)
            .await
            .expect("add receiver p2p collections");

        sender_admin
            .connect(&receiver_addresses)
            .await
            .expect("connect sender to receiver");
        wait_for_active_peer(&sender_admin).await;
        wait_for_active_peer(&receiver_admin).await;

        // Receiver-side authorization mirrors the production embedded P2P setup;
        // the data flow asserted below is still sender -> receiver.
        receiver_admin
            .add_replicator(&sender_addresses, &collections, &PairingFilters::default())
            .await
            .expect("authorize sender as receiver-side replicator");

        let mut filters = PairingFilters::new();
        filters.insert(
            "AgentRequest".to_string(),
            FilterPredicate {
                field: "requester_did".to_string(),
                value: "did:key:coord".to_string(),
            },
        );
        filters.insert(
            "AgentToolCall".to_string(),
            FilterPredicate {
                field: "spawn_target_did".to_string(),
                value: "did:key:host".to_string(),
            },
        );
        sender_admin
            .add_replicator(&receiver_addresses, &collections, &filters)
            .await
            .expect("install filtered sender to receiver replicator");

        wait_for_value(&receiver, "AgentRequest", "request_id", "child-match").await;
        wait_for_value(&receiver, "AgentToolCall", "tool_call_id", "bridge-match").await;

        let request_ids = collection_values(&receiver, "AgentRequest", "request_id").await;
        let tool_call_ids = collection_values(&receiver, "AgentToolCall", "tool_call_id").await;
        assert_eq!(
            request_ids,
            BTreeSet::from(["child-match".to_string()]),
            "filtered request replay should not leak non-matching rows"
        );
        assert_eq!(
            tool_call_ids,
            BTreeSet::from(["bridge-match".to_string()]),
            "filtered tool-call replay should not leak non-matching rows"
        );

        sender.shutdown().await;
        receiver.shutdown().await;
    }

    #[tokio::test]
    async fn embedded_subagent_host_replays_only_return_projection() {
        let sender_test = runtime_test_node().await;
        let receiver_test = runtime_test_node().await;
        let sender = Arc::clone(&sender_test.node);
        let receiver = Arc::clone(&receiver_test.node);
        let sender_admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&sender));
        let receiver_admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&receiver));
        let sender_addresses = wait_for_peer_info(&sender_admin).await;
        let receiver_addresses = wait_for_peer_info(&receiver_admin).await;

        seed_subagent_return_artifacts(&sender, "match", Some("did:key:coord")).await;
        seed_subagent_return_artifacts(&sender, "unrelated", None).await;

        let template = resolve_template(SUBAGENT_HOST_TEMPLATE).expect("subagent-host template");
        let collections = template
            .collections
            .iter()
            .map(|collection| (*collection).to_string())
            .collect::<Vec<_>>();

        sender_admin
            .connect(&receiver_addresses)
            .await
            .expect("connect sender to receiver");
        wait_for_active_peer(&sender_admin).await;
        wait_for_active_peer(&receiver_admin).await;
        // `subagent-host` is a Push template. Production deliberately leaves
        // the subscription set empty so whole collections never gossip; the
        // two per-peer replicators below are the only authorized channels.
        receiver_admin
            .add_replicator(&sender_addresses, &collections, &PairingFilters::default())
            .await
            .expect("authorize only the subagent return projection");

        let filters = scope_filter(
            &template.scope,
            template.collections,
            "did:key:coord",
            "did:key:host",
        );
        sender_admin
            .add_replicator(&receiver_addresses, &collections, &filters)
            .await
            .expect("install requester-scoped return replicator");

        wait_for_value(
            &receiver,
            "AgentResponse",
            "response_key",
            "return-match-response",
        )
        .await;
        wait_for_value(
            &receiver,
            "AgentMessage",
            "message_key",
            "return-match-session:1",
        )
        .await;
        assert_eq!(
            collection_values(&receiver, "AgentResponse", "response_key").await,
            BTreeSet::from(["return-match-response".to_string()])
        );
        assert_eq!(
            collection_values(&receiver, "AgentMessage", "message_key").await,
            BTreeSet::from(["return-match-session:1".to_string()])
        );
        assert_eq!(
            collection_values(&receiver, "AgentSession", "session_id").await,
            BTreeSet::new(),
            "host-local session ownership must not cross the return leg"
        );
        assert_eq!(
            collection_values(&receiver, "AgentConversation", "session_id").await,
            BTreeSet::new(),
            "host-local conversation metadata must not cross the return leg"
        );

        sender.shutdown().await;
        receiver.shutdown().await;
    }
}
