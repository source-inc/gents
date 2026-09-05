//! Embedded-node implementation of the runtime pairing admin seam.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use defra_p2p_adapter::{P2PError, P2PResult, P2pDocumentRequest};
use tokio::time::timeout;

use crate::defra_node::EmbeddedNode;

use super::templates::{to_replication_filters, PairingFilters};
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
                filters: Some(r.filters),
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
        let defra_filters =
            to_replication_filters(filters).map_err(RemoteP2pAdminError::LocalError)?;
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
        let sync_timeout = timeout_override.unwrap_or(self.timeout);
        let future = p2p.sync_documents(collection_name, doc_ids.to_vec(), Some(sync_timeout));
        match timeout(sync_timeout, future).await {
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

pub(super) async fn push_documents_to_peer(
    node: &Arc<EmbeddedNode>,
    peer_id: &str,
    documents: &std::collections::BTreeSet<super::session_hydration::HydrationDocument>,
) -> anyhow::Result<()> {
    use anyhow::Context as _;

    let p2p = node
        .p2p_arc()
        .context("embedded node has no P2P transport for hydration delivery")?;
    let docs = documents
        .iter()
        .map(|document| P2pDocumentRequest {
            collection: document.collection.clone(),
            doc_id: document.doc_id.clone(),
        })
        .collect::<Vec<_>>();
    timeout(
        DEFAULT_EMBEDDED_ADMIN_TIMEOUT,
        p2p.push_documents_to_peer(peer_id, docs),
    )
    .await
    .with_context(|| format!("push hydration documents to peer {peer_id} timed out"))?
    .with_context(|| format!("push hydration documents to peer {peer_id}"))
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::stream;
    use p2p::iroh::{IrohDiscoveryConfig, IrohRelayModeConfig};
    use rig::completion::{
        CompletionError, CompletionModel, CompletionRequest, CompletionResponse,
    };
    use rig::streaming::{RawStreamingChoice, StreamingCompletionResponse};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::agent::completion_retry::CompletionRetryProfileFields;
    use crate::agent::daemon::BehaviorDaemon;
    use crate::agent::p2p_reconcile::templates::{
        Scope, SUBAGENT_COORDINATOR_TEMPLATE, SUBAGENT_HOST_TEMPLATE,
    };
    use crate::agent::p2p_reconcile::{combine_filters, equality_filter};
    use crate::agent::p2p_reconcile::{
        enrollment_authority_channel, run_enrollment_reconciler, GraphqlEnrollmentStore,
    };
    use crate::agent::p2p_reconcile::{resolve_template, scope_filter};
    use crate::agent::runtime::StartupBarrier;
    use crate::backend_provider::BackendProviderKind;
    use crate::compaction::CompactionStrategy;
    use crate::config::{AgentBehavior, SamplingConfig};
    use crate::defra_node::P2PConfig;
    use crate::ensure_runtime_schemas;
    use crate::graphql::escape_graphql_string;
    use crate::hook::{BackgroundExecutionRegistry, BackgroundToolRegistry, FailurePolicy};
    use crate::identity::{AgentIdentity, AgentPrincipal, KeyIdentity};
    use crate::llm::tool::ToolDyn;
    use crate::prompt::LayeredPromptBuilder;
    use crate::tool_surface::BehaviorToolConfig;
    use gents_protocol::enrollment::{
        derive_enrollment_id, encode_offer, enrollment_schema_fingerprint, EnrollmentDecisionKind,
        EnrollmentOfferRecord, EnrollmentRequestRecord, ENROLLMENT_PROTOCOL_VERSION,
    };
    use gents_protocol::network_token::NetworkRecord;

    const TEST_SCHEMA: &str = r#"
        type P2pReconcileThing {
            name: String
        }
    "#;

    struct TestNode {
        node: Arc<EmbeddedNode>,
        _tempdir: tempfile::TempDir,
    }

    #[test]
    fn adapter_preserves_conjunctive_filters_as_conditions() {
        let filters = [(
            "AgentRequest".to_string(),
            combine_filters(
                equality_filter("requester_did", "did:key:phone"),
                equality_filter("lifecycle_state", "pending"),
            ),
        )]
        .into_iter()
        .collect();

        let converted = to_replication_filters(&filters).expect("supported filters");
        assert_eq!(
            converted["AgentRequest"].conditions.as_ref(),
            serde_json::json!({
                "_and": [
                    { "requester_did": { "_eq": "did:key:phone" } },
                    { "lifecycle_state": { "_eq": "pending" } }
                ]
            })
            .as_object()
        );
    }

    async fn p2p_node_with_identity(node_identity_did: Option<&str>) -> TestNode {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let mut builder = EmbeddedNode::builder()
            .data_path(tempdir.path())
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
                rebroadcast_on_merge: false,
            });
        if let Some(did) = node_identity_did {
            builder = builder.with_node_identity_did(did);
        }
        let node = Arc::new(builder.build().await.expect("embedded p2p node"));
        TestNode {
            node,
            _tempdir: tempdir,
        }
    }

    async fn p2p_node() -> TestNode {
        p2p_node_with_identity(None).await
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

    async fn runtime_test_node_with_identity(identity: &dyn AgentIdentity) -> TestNode {
        let test = p2p_node_with_identity(Some(identity.did())).await;
        ensure_runtime_schemas(&test.node)
            .await
            .expect("runtime schemas");
        test
    }

    fn bs58_sig(signature: &[u8]) -> String {
        bs58::encode(signature).into_string()
    }

    async fn authorize_enrollment_peer(
        node: Arc<EmbeddedNode>,
        admin_identity: Arc<dyn AgentIdentity>,
        member_identity: Arc<dyn AgentIdentity>,
        member_peer: &str,
        member_ticket: &str,
    ) -> String {
        let issued = chrono::Utc::now();
        let issued_at = issued.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let expires_at = (issued + chrono::Duration::minutes(5))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let network_id = format!("network-{}", uuid::Uuid::new_v4());

        let mut network = NetworkRecord {
            network_id: network_id.clone(),
            admin_did: admin_identity.did().to_string(),
            display_name: "final admission test".to_string(),
            default_template: "conversation".to_string(),
            created_at: issued_at.clone(),
            sig: Vec::new(),
        };
        network.sig = admin_identity
            .sign(&network.signing_payload())
            .await
            .expect("sign test network");
        let response = node
            .execute(&format!(
                r#"mutation {{ create_AgentNetwork(input: {{
                    network_id: "{}", admin_did: "{}", display_name: "{}",
                    default_template: "{}", created_at: "{}", admin_sig: "{}"
                }}) {{ _docID }} }}"#,
                escape_graphql_string(&network.network_id),
                escape_graphql_string(&network.admin_did),
                escape_graphql_string(&network.display_name),
                escape_graphql_string(&network.default_template),
                escape_graphql_string(&network.created_at),
                escape_graphql_string(&bs58_sig(&network.sig)),
            ))
            .await;
        assert!(
            !response.has_errors(),
            "create signed AgentNetwork: {:?}",
            response.errors
        );

        let p2p = node.p2p().expect("admin P2P enabled");
        let server_peer = p2p.local_peer_id().await.expect("admin peer id");
        let server_ticket = p2p
            .shareable_address()
            .await
            .expect("admin ticket lookup")
            .expect("admin shareable ticket");
        let challenge = format!("challenge-{network_id}-{member_peer}");
        let offer_id = format!(
            "offer-{}",
            derive_enrollment_id(
                "gents-enrollment-offer-v1",
                &[
                    &network_id,
                    admin_identity.did(),
                    &server_peer,
                    &challenge,
                    &issued_at,
                ],
            )
        );
        let mut offer = EnrollmentOfferRecord {
            version: ENROLLMENT_PROTOCOL_VERSION,
            offer_id,
            challenge,
            network_id: network_id.clone(),
            admin_did: admin_identity.did().to_string(),
            server_peer,
            server_ticket,
            owner_agent: admin_identity.did().to_string(),
            profile: "client".to_string(),
            schema_fingerprint: enrollment_schema_fingerprint(),
            issued_at: issued_at.clone(),
            expires_at: expires_at.clone(),
            admin_sig: Vec::new(),
        };
        offer.admin_sig = admin_identity
            .sign(&offer.signing_payload())
            .await
            .expect("sign enrollment offer");
        let offer_token = encode_offer(&offer).expect("encode enrollment offer");
        let client_nonce = format!("nonce-{network_id}-{member_peer}");
        let request_id = format!(
            "enroll-{}",
            derive_enrollment_id(
                "gents-enrollment-request-id-v1",
                &[
                    &offer.offer_id,
                    member_identity.did(),
                    member_peer,
                    &client_nonce,
                ],
            )
        );
        let mut request = EnrollmentRequestRecord {
            protocol_version: ENROLLMENT_PROTOCOL_VERSION,
            request_id,
            request_digest: String::new(),
            offer_id: offer.offer_id.clone(),
            offer_token,
            challenge: offer.challenge.clone(),
            network_id,
            admin_did: admin_identity.did().to_string(),
            server_peer: offer.server_peer.clone(),
            candidate_did: member_identity.did().to_string(),
            candidate_peer: member_peer.to_string(),
            candidate_ticket: member_ticket.to_string(),
            owner_agent: offer.owner_agent.clone(),
            profile: offer.profile.clone(),
            client_nonce,
            issued_at,
            expires_at,
            candidate_sig: Vec::new(),
        };
        request.request_digest = request.computed_digest();
        request.candidate_sig = member_identity
            .sign(&request.signing_payload())
            .await
            .expect("sign enrollment request");
        request
            .validate_against_offer(&offer)
            .expect("validate enrollment request");

        let response = node
            .execute(&format!(
                r#"mutation {{ create_NetworkEnrollmentRequest(input: {{
                    protocol_version: {}, request_id: "{}", request_digest: "{}",
                    offer_id: "{}", offer_token: "{}", challenge: "{}",
                    network_id: "{}", admin_did: "{}", server_peer: "{}",
                    candidate_did: "{}", candidate_peer: "{}", candidate_ticket: "{}",
                    owner_agent: "{}", profile: "{}", client_nonce: "{}",
                    issued_at: "{}", expires_at: "{}", candidate_sig: "{}"
                }}) {{ _docID }} }}"#,
                request.protocol_version,
                escape_graphql_string(&request.request_id),
                escape_graphql_string(&request.request_digest),
                escape_graphql_string(&request.offer_id),
                escape_graphql_string(&request.offer_token),
                escape_graphql_string(&request.challenge),
                escape_graphql_string(&request.network_id),
                escape_graphql_string(&request.admin_did),
                escape_graphql_string(&request.server_peer),
                escape_graphql_string(&request.candidate_did),
                escape_graphql_string(&request.candidate_peer),
                escape_graphql_string(&request.candidate_ticket),
                escape_graphql_string(&request.owner_agent),
                escape_graphql_string(&request.profile),
                escape_graphql_string(&request.client_nonce),
                escape_graphql_string(&request.issued_at),
                escape_graphql_string(&request.expires_at),
                escape_graphql_string(&bs58_sig(&request.candidate_sig)),
            ))
            .await;
        assert!(
            !response.has_errors(),
            "create signed enrollment request: {:?}",
            response.errors
        );
        GraphqlEnrollmentStore::new(node, admin_identity)
            .decide_request(&request.request_id, EnrollmentDecisionKind::Approved)
            .await
            .expect("approve live enrollment request");
        request.request_id
    }

    #[derive(Clone)]
    struct CountingReplyModel(Arc<AtomicUsize>);

    #[allow(refining_impl_trait)]
    impl CompletionModel for CountingReplyModel {
        type Response = ();
        type StreamingResponse = ();
        type Client = ();

        fn make(_: &Self::Client, _: impl Into<String>) -> Self {
            Self(Arc::new(AtomicUsize::new(0)))
        }

        async fn completion(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse<()>, CompletionError> {
            Err(CompletionError::ProviderError("unused".into()))
        }

        async fn stream(
            &self,
            _request: CompletionRequest,
        ) -> Result<StreamingCompletionResponse<()>, CompletionError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let inner: rig::streaming::StreamingResult<()> = Box::pin(stream::iter(vec![
                Ok(RawStreamingChoice::Message("admitted reply".to_string())),
                Ok(RawStreamingChoice::FinalResponse(())),
            ]));
            Ok(StreamingCompletionResponse::stream(inner))
        }
    }

    fn test_behavior(identity: Arc<dyn AgentIdentity>) -> Arc<AgentBehavior> {
        let principal = Arc::new(AgentPrincipal {
            agent_did: identity.did().to_string(),
            identity,
            default_behavior_id: "behavior-1".to_string(),
            display_name: None,
            enabled: true,
        });
        Arc::new(AgentBehavior {
            behavior_id: "behavior-1".to_string(),
            principal,
            backend_id: Some("backend-behavior-1".to_string()),
            backend_provider_kind: BackendProviderKind::OpenAiCompatible,
            openai_wire_api: crate::OpenAiWireApi::ChatCompletions,
            backend_endpoint: "http://127.0.0.1:8999/v1".to_string(),
            backend_api_key: None,
            backend_api_key_env_var: None,
            model_name: "scripted".to_string(),
            context_window: 8_192,
            max_output_tokens: 1_024,
            max_turns: 2,
            system_prompt: "system".to_string(),
            request_context_template: None,
            tools: BehaviorToolConfig::meta_only(),
            compaction_threshold: 0.75,
            compaction_strategy: CompactionStrategy::StripThenSummarize,
            stream_batch_ms: 0,
            stream_liveness_timeout: Duration::from_secs(5),
            deadline_duration: Duration::from_secs(30),
            completion_retry: CompletionRetryProfileFields::default(),
            sampling: SamplingConfig::default(),
            skills: Vec::new(),
        })
    }

    fn behavior_daemon(
        node: Arc<EmbeddedNode>,
        behavior: Arc<AgentBehavior>,
        authority: super::super::EnrollmentAuthorityHandle,
        calls: Arc<AtomicUsize>,
    ) -> BehaviorDaemon<CountingReplyModel> {
        let prompt_builder = LayeredPromptBuilder::for_behavior(
            &behavior.system_prompt,
            &behavior.behavior_id,
            &[],
            false,
            &[],
        );
        let runtime_status = crate::runtime_status::RuntimeStatusHandle::new(
            node.clone(),
            behavior.agent_did().to_string(),
        );
        BehaviorDaemon::new(
            node.clone(),
            behavior.clone(),
            Arc::new(CountingReplyModel(calls)),
            prompt_builder.preamble().to_string(),
            Arc::new(Vec::<Box<dyn ToolDyn>>::new()),
            prompt_builder,
            FailurePolicy::default(),
            None,
            BackgroundToolRegistry::default(),
            BackgroundExecutionRegistry::default(),
            Arc::new(StartupBarrier::ready_for_test()),
            runtime_status,
            1,
            crate::request_admission::AgentRequestAdmissionVerifier::new(
                node,
                behavior.principal_identity().clone(),
                authority,
            ),
        )
    }

    async fn seed_cross_deployment_bridge(
        node: &EmbeddedNode,
        parent_request_id: &str,
        parent_doc_id: &str,
        coordinator_did: &str,
        host_did: &str,
        tool_call_id: &str,
        child_request_id: &str,
        sequence: u64,
    ) -> String {
        let args = serde_json::json!({
            "name": "remote-target",
            "behavior_id": "behavior-1",
            "prompt": "delegated work"
        })
        .to_string();
        let response = node
            .execute(&format!(
                r#"mutation {{ create_AgentToolCall(input: {{
                    tool_call_key: "remote-session:{}", request_id: "{}",
                    request_doc_id: "{}", session_id: "remote-session",
                    agent_did: "{}", requester_did: "{}",
                    message_sequence: {}, tool_name: "spawn_subagent", tool_call_id: "{}",
                    args: "{}", status: "called", lifecycle_state: "running",
                    await_mode: "background", cancel_policy: "cascade",
                    child_request_id: "{}", spawn_target_did: "{}"
                }}) {{ _docID }} }}"#,
                escape_graphql_string(tool_call_id),
                escape_graphql_string(parent_request_id),
                escape_graphql_string(parent_doc_id),
                escape_graphql_string(coordinator_did),
                escape_graphql_string(coordinator_did),
                sequence,
                escape_graphql_string(tool_call_id),
                escape_graphql_string(&args),
                escape_graphql_string(child_request_id),
                escape_graphql_string(host_did),
            ))
            .await;
        assert!(
            !response.has_errors(),
            "seed bridge {tool_call_id}: {:?}",
            response.errors
        );
        response
            .data
            .as_ref()
            .and_then(|data| {
                data.get("create_AgentToolCall")
                    .or_else(|| data.get("add_AgentToolCall"))
            })
            .and_then(|value| {
                value.get("_docID").or_else(|| {
                    value
                        .as_array()
                        .and_then(|rows| rows.first())
                        .and_then(|row| row.get("_docID"))
                })
            })
            .and_then(serde_json::Value::as_str)
            .expect("bridge doc id")
            .to_string()
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
        let session_id = format!("{request_id}-session");
        seed_agent_request_for_session(node, request_id, &session_id, Some(requester_did)).await;
    }

    async fn seed_agent_request_for_session(
        node: &EmbeddedNode,
        request_id: &str,
        session_id: &str,
        requester_did: Option<&str>,
    ) {
        let request_id = escape_graphql_string(request_id);
        let agent_did = "did:key:host";
        let requester_did_field = crate::session::requester_did_create_field(requester_did);
        let session_id = escape_graphql_string(session_id);
        let behavior_id = escape_graphql_string(&format!("{agent_did}:default"));
        let mutation = format!(
            r#"mutation {{
                create_AgentRequest(input: {{
                    request_id: "{request_id}",
                    agent_did: "{agent_did}",
                    {requester_did_field}
                    behavior_id: "{behavior_id}",
                    session_id: "{session_id}",
                    retry_parent_request: "",
                    retry_root_request: "{request_id}",
                    superseded_by_request: "",
                    content: "issue 604 filtered replay",
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

        seed_agent_request_for_session(node, &request_id, &session_id, requester_did).await;

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
        let requester_did_field = crate::session::requester_did_create_field(requester_did);
        let escaped_session_id = escape_graphql_string(&session_id);
        let escaped_request_id = escape_graphql_string(&request_id);
        let escaped_agent_did = escape_graphql_string(agent_did);
        let escaped_behavior_id = escape_graphql_string(behavior_id);
        let mutation = format!(
            r#"mutation {{
                create_AgentConversation(input: {{
                    session_id: "{escaped_session_id}",
                    agent_name: "default",
                    agent_did: "{escaped_agent_did}",
                    {requester_did_field}
                    behavior_id: "{escaped_behavior_id}",
                    title: "",
                    title_source: "placeholder",
                    preview_text: "child prompt",
                    status: "completed",
                    created_at: "2026-07-06T00:00:00Z",
                    updated_at: "2026-07-06T00:00:00Z",
                    latest_request_id: "{escaped_request_id}"
                }}) {{ _docID }}
            }}"#
        );
        exec(node, &mutation, "create routed AgentConversation").await;
        crate::session::save_message_with_requester_did(
            node,
            &session_id,
            agent_did,
            requester_did,
            1,
            "assistant",
            "child result",
            None,
            None,
            None,
        )
        .await
        .expect("create routed AgentMessage");
        let response = node.execute(&format!(
            r#"mutation {{ update_AgentRequest(filter: {{ request_id: {{ _eq: "{escaped_request_id}" }} }}, input: {{ lifecycle_state: "pending" }}) {{ {} }} }}"#,
            crate::watcher::AGENT_REQUEST_FIELDS,
        )).await;
        let request =
            crate::watcher::agent_request_from_mutation_response(&response, "update_AgentRequest")
                .expect("decode routed request")
                .expect("routed request");
        let mut lifecycle = crate::lifecycle::RequestLifecycle::new_with_agent_did(
            Arc::clone(node),
            "default",
            agent_did,
            request,
            60,
        );
        assert_eq!(
            lifecycle.claim().await.expect("claim routed request"),
            crate::lifecycle::ClaimOutcome::Claimed
        );
        let writer = crate::streaming::DefraStreamWriter::new(
            Arc::clone(node),
            agent_did,
            Duration::from_secs(60),
        );
        lifecycle
            .begin_owned_execution(&writer)
            .await
            .expect("create routed AgentResponse");
        lifecycle
            .terminalize_owned(
                &writer,
                crate::lifecycle::RequestTerminalOutcome::Completed,
                None,
            )
            .await
            .expect("complete routed request");
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

    async fn load_admission_request_by_id(
        node: &EmbeddedNode,
        request_id: &str,
    ) -> crate::watcher::AgentRequest {
        let response = node
            .execute(&format!(
                r#"{{ AgentRequest(filter: {{ request_id: {{ _eq: "{}" }} }}, limit: 1) {{ _docID }} }}"#,
                escape_graphql_string(request_id),
            ))
            .await;
        let doc_id = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(|rows| rows.as_array())
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("_docID"))
            .and_then(serde_json::Value::as_str)
            .expect("child doc id");
        crate::request_admission::load_request_for_admission_test(node, doc_id)
            .await
            .expect("load child")
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
            let filters = match &template.scope {
                Scope::ClientRoute => super::super::policy::resolve_template_filters(
                    template,
                    super::super::policy::PairingDirection::ClientToRuntime,
                    "did:key:z6MkPeerForFilterValidation",
                    "did:key:z6MkSelfForFilterValidation",
                ),
                _ => super::super::templates::scope_filter(
                    &template.scope,
                    template.collections,
                    "did:key:z6MkPeerForFilterValidation",
                    "did:key:z6MkSelfForFilterValidation",
                ),
            };
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
                Scope::ClientRoute => assert!(!filters.is_empty()),
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

        sender_admin
            .connect(&receiver_addresses)
            .await
            .expect("connect sender to receiver");
        wait_for_active_peer(&sender_admin).await;
        wait_for_active_peer(&receiver_admin).await;

        let collections = vec!["AgentRequest".to_string(), "AgentToolCall".to_string()];
        // Receiver-side authorization mirrors the production embedded P2P setup;
        // the data flow asserted below is still sender -> receiver. This is a
        // Push topology: neither peer may subscribe to the whole collections,
        // because collection subscriptions are an independent unfiltered
        // gossip path that can race the filtered replicator replay.
        receiver_admin
            .add_replicator(&sender_addresses, &collections, &PairingFilters::default())
            .await
            .expect("authorize sender as receiver-side replicator");
        assert!(
            sender_admin
                .list_p2p_collections()
                .await
                .expect("list sender P2P subscriptions")
                .is_empty(),
            "filtered Push sender must not subscribe to whole collections"
        );
        assert!(
            receiver_admin
                .list_p2p_collections()
                .await
                .expect("list receiver P2P subscriptions")
                .is_empty(),
            "filtered Push receiver must not subscribe to whole collections"
        );

        // Give any independently configured route a chance to replay before
        // installing the sender filter. With the former whole-collection
        // subscriptions this reliably admitted non-matching seeded rows.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let mut filters = PairingFilters::new();
        filters.insert(
            "AgentRequest".to_string(),
            equality_filter("requester_did", "did:key:coord"),
        );
        filters.insert(
            "AgentToolCall".to_string(),
            equality_filter("spawn_target_did", "did:key:host"),
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
    async fn cross_deployment_child_final_admission_uses_only_targeted_bridge_and_fresh_authority()
    {
        let identity_temp = tempfile::tempdir().expect("identity tempdir");
        let coordinator_identity: Arc<dyn AgentIdentity> = Arc::new(
            KeyIdentity::load_or_create(identity_temp.path().join("coordinator.key"), None)
                .expect("coordinator identity"),
        );
        let host_identity: Arc<dyn AgentIdentity> = Arc::new(
            KeyIdentity::load_or_create(identity_temp.path().join("host.key"), None)
                .expect("host identity"),
        );
        let coordinator_test = runtime_test_node_with_identity(coordinator_identity.as_ref()).await;
        let host_test = runtime_test_node_with_identity(host_identity.as_ref()).await;
        let coordinator = Arc::clone(&coordinator_test.node);
        let host = Arc::clone(&host_test.node);
        let coordinator_admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&coordinator));
        let host_admin = EmbeddedRemoteP2pAdmin::new(Arc::clone(&host));
        let coordinator_addresses = wait_for_peer_info(&coordinator_admin).await;
        let host_addresses = wait_for_peer_info(&host_admin).await;

        let host_did = host_identity.did().to_string();
        let coordinator_did = coordinator_identity.did().to_string();
        let parent_request_id = "remote-parent-1";

        let parent_response = coordinator
            .execute(&format!(
                r#"mutation {{ create_AgentRequest(input: {{
                    request_id: "{parent_request_id}", agent_did: "{coordinator_did}",
                    requester_did: "{coordinator_did}", behavior_id: "parent-behavior",
                    session_id: "remote-session", retry_root_request: "{parent_request_id}",
                    content: "remote parent", lifecycle_state: "processing",
                    execution_origin: "interactive", created_at: "2026-08-30T00:00:00Z",
                    retry_count: 0, max_retries: 3, subagent_depth: 0
                }}) {{ _docID }} }}"#,
            ))
            .await;
        assert!(
            !parent_response.has_errors(),
            "seed parent: {:?}",
            parent_response.errors
        );
        let parent_doc_id = parent_response
            .data
            .as_ref()
            .and_then(|data| {
                data.get("create_AgentRequest")
                    .or_else(|| data.get("add_AgentRequest"))
            })
            .and_then(|value| {
                value.get("_docID").or_else(|| {
                    value
                        .as_array()
                        .and_then(|rows| rows.first())
                        .and_then(|row| row.get("_docID"))
                })
            })
            .and_then(serde_json::Value::as_str)
            .expect("parent doc id")
            .to_string();
        let bridge_doc_id = seed_cross_deployment_bridge(
            &coordinator,
            parent_request_id,
            &parent_doc_id,
            &coordinator_did,
            &host_did,
            "bridge-1",
            "remote-child-1",
            1,
        )
        .await;
        let revoked_bridge_doc_id = seed_cross_deployment_bridge(
            &coordinator,
            parent_request_id,
            &parent_doc_id,
            &coordinator_did,
            &host_did,
            "bridge-2",
            "remote-child-2",
            2,
        )
        .await;
        let immutable_update = coordinator
            .execute(&format!(
                r#"mutation {{ update_AgentToolCall(
                    filter: {{ _docID: {{ _eq: "{}" }} }},
                    input: {{ request_id: "retargeted-parent" }}
                ) {{ _docID }} }}"#,
                escape_graphql_string(&bridge_doc_id),
            ))
            .await;
        assert!(
            immutable_update.has_errors(),
            "bridge parent request identity must be immutable"
        );

        let policy_response = host
            .execute(&format!(
                r#"mutation {{
                    selection: create_ToolSelection(input: {{
                        selection_id: "host-selection", agent_did: "{}",
                        subagent_allow_cross_deployment: true
                    }}) {{ _docID }}
                    behavior: create_AgentBehavior(input: {{
                        behavior_id: "behavior-1", agent_did: "{}",
                        tool_selection_id: "host-selection", enabled: true
                    }}) {{ _docID }}
                }}"#,
                escape_graphql_string(&host_did),
                escape_graphql_string(&host_did),
            ))
            .await;
        assert!(
            !policy_response.has_errors(),
            "seed target policy: {:?}",
            policy_response.errors
        );

        let template =
            resolve_template(SUBAGENT_COORDINATOR_TEMPLATE).expect("subagent-coordinator template");
        let collections = template
            .collections
            .iter()
            .map(|collection| (*collection).to_string())
            .collect::<Vec<_>>();
        coordinator_admin
            .connect(&host_addresses)
            .await
            .expect("connect host");
        wait_for_active_peer(&coordinator_admin).await;
        wait_for_active_peer(&host_admin).await;
        let coordinator_p2p = coordinator.p2p().expect("coordinator P2P enabled");
        let coordinator_peer = coordinator_p2p
            .local_peer_id()
            .await
            .expect("coordinator peer id");
        let coordinator_ticket = coordinator_p2p
            .shareable_address()
            .await
            .expect("coordinator ticket lookup")
            .expect("coordinator shareable ticket");
        let enrollment_request_id = authorize_enrollment_peer(
            host.clone(),
            host_identity.clone(),
            coordinator_identity.clone(),
            &coordinator_peer,
            &coordinator_ticket,
        )
        .await;
        let (authority_owner, authority) = enrollment_authority_channel();
        let authority_cancel = CancellationToken::new();
        let authority_task = tokio::spawn(run_enrollment_reconciler(
            host.clone(),
            host_identity.clone(),
            authority_owner,
            authority_cancel.clone(),
        ));
        host_admin
            .add_replicator(
                &coordinator_addresses,
                &collections,
                &PairingFilters::default(),
            )
            .await
            .expect("authorize coordinator route");
        let filters = scope_filter(
            &template.scope,
            template.collections,
            &host_did,
            &coordinator_did,
        );
        coordinator_admin
            .add_replicator(&host_addresses, &collections, &filters)
            .await
            .expect("install targeted coordinator bridge route");
        wait_for_value(&host, "AgentToolCall", "tool_call_id", "bridge-1").await;
        wait_for_value(&host, "AgentToolCall", "tool_call_id", "bridge-2").await;
        assert!(
            collection_values(&host, "AgentRequest", "request_id")
                .await
                .is_empty(),
            "coordinator parent request must not replicate to the host"
        );

        crate::tool_call_lifecycle::create_subagent_request_with_trusted_parent_request_id(
            &host,
            "remote-child-1".to_string(),
            parent_request_id.to_string(),
            parent_doc_id.clone(),
            "bridge-1".to_string(),
            bridge_doc_id,
            0,
            host_did.clone(),
            "behavior-1".to_string(),
            "delegated work".to_string(),
            None,
            coordinator_did.clone(),
        )
        .await
        .expect("materialize fresh target-signed cross-deployment child");
        crate::tool_call_lifecycle::create_subagent_request_with_trusted_parent_request_id(
            &host,
            "remote-child-2".to_string(),
            parent_request_id.to_string(),
            parent_doc_id,
            "bridge-2".to_string(),
            revoked_bridge_doc_id,
            0,
            host_did.clone(),
            "behavior-1".to_string(),
            "delegated work after revocation".to_string(),
            None,
            coordinator_did.clone(),
        )
        .await
        .expect("materialize pending target-signed cross-deployment child");

        let behavior = test_behavior(host_identity.clone());
        let fresh_calls = Arc::new(AtomicUsize::new(0));
        let mut fresh_daemon = behavior_daemon(
            host.clone(),
            behavior.clone(),
            authority.clone(),
            fresh_calls.clone(),
        );
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let fresh = load_admission_request_by_id(&host, "remote-child-1").await;
        fresh_daemon
            .process_request(fresh, shutdown_rx.clone())
            .await;
        assert!(
            fresh_calls.load(Ordering::SeqCst) > 0,
            "fresh cross-deployment child did not reach the provider"
        );

        GraphqlEnrollmentStore::new(host.clone(), host_identity.clone())
            .revoke_request(&enrollment_request_id)
            .await
            .expect("durably revoke signed enrollment generation");
        let revoked_calls = Arc::new(AtomicUsize::new(0));
        let mut revoked_daemon =
            behavior_daemon(host.clone(), behavior, authority, revoked_calls.clone());
        let revoked = load_admission_request_by_id(&host, "remote-child-2").await;
        revoked_daemon.process_request(revoked, shutdown_rx).await;
        assert_eq!(
            revoked_calls.load(Ordering::SeqCst),
            0,
            "revoked pending cross-deployment child reached the provider"
        );
        let rejected = host
            .execute(
                r#"{ AgentRequest(filter: { request_id: { _eq: "remote-child-2" } }, limit: 1) {
                    lifecycle_state claimed_at
                } }"#,
            )
            .await;
        let rejected = rejected
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(|rows| rows.as_array())
            .and_then(|rows| rows.first())
            .expect("terminal denied child");
        assert_eq!(
            rejected
                .get("lifecycle_state")
                .and_then(serde_json::Value::as_str),
            Some("failed")
        );
        assert!(rejected
            .get("claimed_at")
            .is_none_or(serde_json::Value::is_null));

        authority_cancel.cancel();
        authority_task
            .await
            .expect("join enrollment authority")
            .expect("enrollment authority shutdown");
        coordinator.shutdown().await;
        host.shutdown().await;
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
