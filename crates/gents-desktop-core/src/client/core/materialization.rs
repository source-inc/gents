use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use defra_node::EmbeddedNode;
use defra_p2p_adapter::P2POperations as P2POps;
use gents_protocol::client_protocol::ClientTurnState;
use gents_protocol::schemas::{AGENT_MESSAGE_NAME, AGENT_RESPONSE_NAME};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::super::observe::ObservedStore;
use super::super::peer_directory::PeerDirectory;
use super::super::query::{
    load_agent_scoped_snapshot_with_peer_records, load_full_snapshot_with_peer_records,
};
use super::super::store::ClientStore;
use super::p2p_ops::p2p_sync_branchable_collection;

const MATERIALIZATION_MONITOR_INTERVAL: Duration = Duration::from_secs(1);
const MATERIALIZATION_STALL_THRESHOLD: Duration = Duration::from_secs(5);
const MATERIALIZATION_REPAIR_COOLDOWN: Duration = Duration::from_secs(5);
const MATERIALIZATION_REFRESH_DELAY: Duration = Duration::from_millis(250);
const MATERIALIZATION_P2P_REPAIR_ENV: &str = "GENTS_DESKTOP_MATERIALIZATION_P2P_REPAIR";

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterializationSignature {
    response_status: Option<String>,
    progress_seq: Option<i64>,
    materialized_message_sequence: Option<i64>,
    response_content_len: usize,
    response_reasoning_len: usize,
    message_count: usize,
    tool_call_count: usize,
    completed_tool_call_count: usize,
    tool_result_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterializationCandidate {
    session_id: String,
    request_id: String,
    agent_did: Option<String>,
    signature: MaterializationSignature,
}

#[derive(Debug, Clone)]
struct MaterializationEntry {
    session_id: String,
    signature: MaterializationSignature,
    first_observed_at: Instant,
    last_repair_at: Option<Instant>,
}

#[derive(Debug, Default)]
struct MaterializationTracker {
    entries: HashMap<String, MaterializationEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterializationRepair {
    session_id: String,
    request_id: String,
    agent_did: Option<String>,
    stalled_for: Duration,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RepairSummary {
    synced_collections: Vec<String>,
}

pub(super) fn spawn_materialization_supervisor_task(
    node: Arc<EmbeddedNode>,
    p2p: Arc<dyn P2POps>,
    store: Arc<ObservedStore>,
    peer_directory: Arc<RwLock<PeerDirectory>>,
    requester_did: String,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tracker = MaterializationTracker::default();
        let mut ticker = tokio::time::interval(MATERIALIZATION_MONITOR_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;

            let snapshot = store.snapshot();
            let repairs = tracker.due_repairs(snapshot.as_ref(), Instant::now());
            if !repairs.is_empty() && !materialization_p2p_repair_enabled() {
                tracing::debug!(
                    target: "gents_desktop_core::materialization",
                    repairs = repairs.len(),
                    env = MATERIALIZATION_P2P_REPAIR_ENV,
                    "skipping opt-in P2P materialization repair"
                );
                continue;
            }

            for repair in repairs {
                tracing::warn!(
                    target: "gents_desktop_core::materialization",
                    request_id = %repair.request_id,
                    session_id = %repair.session_id,
                    stalled_ms = repair.stalled_for.as_millis() as u64,
                    "desktop detected stalled materialization; syncing turn documents"
                );
                match repair_request_materialization(
                    node.as_ref(),
                    &p2p,
                    &repair.session_id,
                    &repair.request_id,
                )
                .await
                {
                    Ok(summary) => {
                        tracing::warn!(
                            target: "gents_desktop_core::materialization",
                            request_id = %repair.request_id,
                            session_id = %repair.session_id,
                            stalled_ms = repair.stalled_for.as_millis() as u64,
                            synced_collections = ?summary.synced_collections,
                            "desktop repaired stalled materialization"
                        );
                        tokio::time::sleep(MATERIALIZATION_REFRESH_DELAY).await;
                        let peers = peer_directory.read().await.records().to_vec();
                        let snapshot_result = match repair.agent_did.as_deref() {
                            Some(did) => {
                                load_agent_scoped_snapshot_with_peer_records(
                                    node.as_ref(),
                                    did,
                                    &peers,
                                    &requester_did,
                                )
                                .await
                            }
                            None => {
                                load_full_snapshot_with_peer_records(
                                    node.as_ref(),
                                    &peers,
                                    &requester_did,
                                )
                                .await
                            }
                        };
                        match snapshot_result {
                            Ok(snapshot) => match repair.agent_did.as_deref() {
                                Some(did) => {
                                    store.replace_agent_snapshot(did, snapshot);
                                }
                                None => {
                                    store.replace_snapshot(snapshot);
                                }
                            },
                            Err(error) => {
                                tracing::warn!(
                                    target: "gents_desktop_core::materialization",
                                    request_id = %repair.request_id,
                                    session_id = %repair.session_id,
                                    error = %error,
                                    "desktop could not refresh snapshot after materialization repair"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "gents_desktop_core::materialization",
                            request_id = %repair.request_id,
                            session_id = %repair.session_id,
                            stalled_ms = repair.stalled_for.as_millis() as u64,
                            error = %error,
                            "desktop materialization repair failed"
                        );
                    }
                }
            }
        }
    })
}

fn materialization_p2p_repair_enabled() -> bool {
    let Ok(value) = std::env::var(MATERIALIZATION_P2P_REPAIR_ENV) else {
        return false;
    };
    let value = value.trim().to_ascii_lowercase();
    matches!(value.as_str(), "1" | "true" | "yes" | "on")
}

impl MaterializationTracker {
    fn due_repairs(&mut self, store: &ClientStore, now: Instant) -> Vec<MaterializationRepair> {
        let candidates = streaming_materialization_candidates(store);
        let active_request_ids: HashSet<&str> = candidates
            .iter()
            .map(|candidate| candidate.request_id.as_str())
            .collect();
        self.entries
            .retain(|request_id, _| active_request_ids.contains(request_id.as_str()));

        let mut repairs = Vec::new();
        for candidate in candidates {
            let entry = self
                .entries
                .entry(candidate.request_id.clone())
                .or_insert_with(|| MaterializationEntry {
                    session_id: candidate.session_id.clone(),
                    signature: candidate.signature.clone(),
                    first_observed_at: now,
                    last_repair_at: None,
                });

            if entry.session_id != candidate.session_id || entry.signature != candidate.signature {
                entry.session_id = candidate.session_id;
                entry.signature = candidate.signature;
                entry.first_observed_at = now;
                entry.last_repair_at = None;
                continue;
            }

            let stalled_for = now.saturating_duration_since(entry.first_observed_at);
            if stalled_for < MATERIALIZATION_STALL_THRESHOLD {
                continue;
            }

            if entry.last_repair_at.is_some_and(|last| {
                now.saturating_duration_since(last) < MATERIALIZATION_REPAIR_COOLDOWN
            }) {
                continue;
            }

            entry.last_repair_at = Some(now);
            repairs.push(MaterializationRepair {
                session_id: candidate.session_id,
                request_id: candidate.request_id,
                agent_did: candidate.agent_did,
                stalled_for,
            });
        }

        repairs
    }
}

fn streaming_materialization_candidates(store: &ClientStore) -> Vec<MaterializationCandidate> {
    let mut request_ids = HashSet::new();
    let mut candidates = Vec::new();

    for conversation in &store.conversations {
        let Some(request_id) = nonempty(conversation.latest_request_id.as_deref()) else {
            continue;
        };
        if !request_ids.insert(request_id.clone()) {
            continue;
        }
        if store.derive_turn_for_request(&request_id) != Some(ClientTurnState::Streaming) {
            continue;
        }

        let Some(request) = store.request_row(&request_id) else {
            continue;
        };
        if !matches!(
            request.status.as_deref(),
            Some("processing" | "pending" | "claimed")
        ) {
            continue;
        }

        let Some(response) = store.latest_response_for_request(&request_id) else {
            continue;
        };
        let session_id = nonempty(request.session_id.as_deref())
            .unwrap_or_else(|| conversation.session_id.clone());
        let agent_did = nonempty(request.agent_did.as_deref())
            .or_else(|| nonempty(conversation.agent_did.as_deref()));
        let transcript = store.transcript(&session_id);
        let completed_tool_call_count = transcript
            .tool_calls
            .iter()
            .filter(|row| tool_call_is_completed(row))
            .count();

        candidates.push(MaterializationCandidate {
            session_id,
            request_id,
            agent_did,
            signature: MaterializationSignature {
                response_status: response.status.clone(),
                progress_seq: response.progress_seq,
                materialized_message_sequence: response.materialized_message_sequence,
                response_content_len: response.content.as_deref().map_or(0, str::len),
                response_reasoning_len: response.reasoning.as_deref().map_or(0, str::len),
                message_count: transcript.messages.len(),
                tool_call_count: transcript.tool_calls.len(),
                completed_tool_call_count,
                tool_result_count: transcript.tool_results.len(),
            },
        });
    }

    candidates
}

fn tool_call_is_completed(row: &gents_protocol::row::AgentToolCallRow) -> bool {
    row.completed_at
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || row
            .status
            .as_deref()
            .is_some_and(|value| matches!(value, "completed" | "success" | "ok"))
}

async fn repair_request_materialization(
    node: &EmbeddedNode,
    p2p: &Arc<dyn P2POps>,
    _session_id: &str,
    _request_id: &str,
) -> Result<RepairSummary> {
    let mut summary = RepairSummary::default();

    sync_branchable_collection_by_name(node, p2p, AGENT_RESPONSE_NAME, &mut summary).await?;
    sync_branchable_collection_by_name(node, p2p, AGENT_MESSAGE_NAME, &mut summary).await?;
    Ok(summary)
}

async fn sync_branchable_collection_by_name(
    node: &EmbeddedNode,
    p2p: &Arc<dyn P2POps>,
    collection_name: &str,
    summary: &mut RepairSummary,
) -> Result<()> {
    let collection_id = node
        .get_collection(collection_name)
        .map_err(|error| anyhow!("loading collection id for {collection_name}: {error}"))?
        .ok_or_else(|| anyhow!("collection {collection_name} not found"))?
        .collection_id;
    p2p_sync_branchable_collection(p2p, &collection_id).await?;
    summary.synced_collections.push(collection_name.to_string());
    Ok(())
}

fn nonempty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::RwLock as StdRwLock;

    use async_trait::async_trait;
    use defra_node::NodeBuilder;
    use defra_p2p_adapter::{
        ExplicitReplayCapabilityInput, P2PResult, P2pDocumentInfo, P2pDocumentRequest,
        ReplicationFilter, ReplicatorInfo,
    };
    use gents_protocol::row::{AgentConversationRow, AgentRequestRow, AgentResponseRow};

    use super::*;
    use crate::client::schema::ensure_runtime_schemas;
    use crate::client::store::ClientStoreRows;

    #[derive(Default)]
    struct RecordingP2P {
        sync_branchable_calls: StdRwLock<Vec<String>>,
        notify_calls: AtomicUsize,
    }

    impl RecordingP2P {
        fn sync_branchable_calls(&self) -> Vec<String> {
            self.sync_branchable_calls
                .read()
                .expect("sync branchable lock poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl P2POps for RecordingP2P {
        async fn local_peer_id(&self) -> P2PResult<String> {
            Ok("local-peer".to_string())
        }

        async fn listen_addresses(&self) -> P2PResult<Vec<String>> {
            Ok(vec!["127.0.0.1:56000/p2p/local-peer".to_string()])
        }

        async fn shareable_address(&self) -> P2PResult<Option<String>> {
            Ok(Some("127.0.0.1:56000/p2p/local-peer".to_string()))
        }

        async fn connected_peers(&self) -> P2PResult<Vec<String>> {
            Ok(vec!["127.0.0.1:56001/p2p/peer-alpha".to_string()])
        }

        async fn connect_peer(&self, _addr: &str) -> P2PResult<()> {
            Ok(())
        }

        async fn disconnect_peer(&self, _addr: &str) -> P2PResult<()> {
            Ok(())
        }

        async fn notify_network_change(&self) -> P2PResult<()> {
            self.notify_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn get_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
            Ok(Vec::new())
        }

        async fn add_replicator(
            &self,
            _collections: Vec<String>,
            _addr: Option<&str>,
            _filters: BTreeMap<String, ReplicationFilter>,
            _explicit_replay_capabilities: Vec<ExplicitReplayCapabilityInput>,
            _expected_authorizer_did: Option<&str>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn remove_replicator(
            &self,
            _collections: Vec<String>,
            _addr: Option<&str>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn get_collections(&self) -> P2PResult<Vec<String>> {
            Ok(Vec::new())
        }

        async fn add_collections(&self, _collections: Vec<String>) -> P2PResult<()> {
            Ok(())
        }

        async fn remove_collections(&self, _collections: Vec<String>) -> P2PResult<()> {
            Ok(())
        }

        async fn get_documents(&self) -> P2PResult<Vec<P2pDocumentInfo>> {
            Ok(Vec::new())
        }

        async fn add_documents(&self, _docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
            Ok(())
        }

        async fn remove_documents(&self, _docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
            Ok(())
        }

        async fn sync_documents(
            &self,
            _collection_name: &str,
            _doc_ids: Vec<String>,
            _timeout: Option<Duration>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn sync_branchable_collection(&self, collection_id: &str) -> P2PResult<()> {
            self.sync_branchable_calls
                .write()
                .expect("sync branchable lock poisoned")
                .push(collection_id.to_string());
            Ok(())
        }

        async fn sync_collection_versions(&self, _version_ids: Vec<String>) -> P2PResult<()> {
            Ok(())
        }
    }

    #[test]
    fn tracker_triggers_repair_after_stall_threshold() {
        let mut tracker = MaterializationTracker::default();
        let candidate = make_candidate(256, 7, 4);
        let now = Instant::now();

        assert!(tracker
            .observe_for_test(vec![candidate.clone()], now)
            .is_empty());
        assert!(tracker
            .observe_for_test(
                vec![candidate],
                now + MATERIALIZATION_STALL_THRESHOLD + Duration::from_millis(1)
            )
            .iter()
            .any(|repair| repair.request_id == "req-1"));
    }

    #[test]
    fn tracker_waits_for_progress_to_stop_before_repairing() {
        let mut tracker = MaterializationTracker::default();
        let now = Instant::now();

        assert!(tracker
            .observe_for_test(vec![make_candidate(256, 7, 4)], now)
            .is_empty());
        assert!(tracker
            .observe_for_test(
                vec![make_candidate(512, 7, 4)],
                now + MATERIALIZATION_STALL_THRESHOLD + Duration::from_millis(1)
            )
            .is_empty());
        assert_eq!(
            tracker
                .observe_for_test(
                    vec![make_candidate(512, 7, 4)],
                    now + (MATERIALIZATION_STALL_THRESHOLD * 2) + Duration::from_millis(2)
                )
                .len(),
            1
        );
    }

    #[test]
    fn tracker_triggers_repair_when_tail_length_plateaus_within_boundary() {
        let mut tracker = MaterializationTracker::default();
        let now = Instant::now();

        assert!(tracker
            .observe_for_test(vec![make_candidate(128, 7, 4)], now)
            .is_empty());
        assert!(tracker
            .observe_for_test(
                vec![make_candidate(256, 7, 4)],
                now + Duration::from_secs(1)
            )
            .is_empty());

        let stalled = tracker.observe_for_test(
            vec![make_candidate(256, 7, 4)],
            now + Duration::from_secs(1)
                + MATERIALIZATION_STALL_THRESHOLD
                + Duration::from_millis(1),
        );
        assert_eq!(stalled.len(), 1, "expected stall when tail plateaus");
    }

    #[tokio::test]
    async fn repair_request_materialization_syncs_response_and_message_collections() {
        let node = Arc::new(NodeBuilder::default().build().await.expect("embedded node"));
        ensure_runtime_schemas(node.as_ref())
            .await
            .expect("runtime schemas");
        seed_turn_documents(node.as_ref()).await;

        let recording = Arc::new(RecordingP2P::default());
        let p2p: Arc<dyn P2POps> = recording.clone();
        let message_collection_id = node
            .get_collection(AGENT_MESSAGE_NAME)
            .expect("lookup AgentMessage collection")
            .expect("AgentMessage collection")
            .collection_id;

        let response_collection_id = node
            .get_collection(AGENT_RESPONSE_NAME)
            .expect("lookup AgentResponse collection")
            .expect("AgentResponse collection")
            .collection_id;
        let summary = repair_request_materialization(node.as_ref(), &p2p, "sess-1", "req-1")
            .await
            .expect("repair succeeds");

        assert!(summary
            .synced_collections
            .contains(&AGENT_RESPONSE_NAME.to_string()));
        assert_eq!(
            recording.sync_branchable_calls(),
            vec![response_collection_id, message_collection_id]
        );
    }

    impl MaterializationTracker {
        fn observe_for_test(
            &mut self,
            candidates: Vec<MaterializationCandidate>,
            now: Instant,
        ) -> Vec<MaterializationRepair> {
            let active_request_ids: HashSet<&str> = candidates
                .iter()
                .map(|candidate| candidate.request_id.as_str())
                .collect();
            self.entries
                .retain(|request_id, _| active_request_ids.contains(request_id.as_str()));

            let mut repairs = Vec::new();
            for candidate in candidates {
                let entry = self
                    .entries
                    .entry(candidate.request_id.clone())
                    .or_insert_with(|| MaterializationEntry {
                        session_id: candidate.session_id.clone(),
                        signature: candidate.signature.clone(),
                        first_observed_at: now,
                        last_repair_at: None,
                    });

                if entry.session_id != candidate.session_id
                    || entry.signature != candidate.signature
                {
                    entry.session_id = candidate.session_id;
                    entry.signature = candidate.signature;
                    entry.first_observed_at = now;
                    entry.last_repair_at = None;
                    continue;
                }

                let stalled_for = now.saturating_duration_since(entry.first_observed_at);
                if stalled_for < MATERIALIZATION_STALL_THRESHOLD {
                    continue;
                }
                if entry.last_repair_at.is_some_and(|last| {
                    now.saturating_duration_since(last) < MATERIALIZATION_REPAIR_COOLDOWN
                }) {
                    continue;
                }
                entry.last_repair_at = Some(now);
                repairs.push(MaterializationRepair {
                    session_id: candidate.session_id,
                    request_id: candidate.request_id,
                    agent_did: candidate.agent_did,
                    stalled_for,
                });
            }
            repairs
        }
    }

    fn make_candidate(
        response_content_len: usize,
        progress_seq: i64,
        message_count: usize,
    ) -> MaterializationCandidate {
        MaterializationCandidate {
            session_id: "sess-1".to_string(),
            request_id: "req-1".to_string(),
            agent_did: Some("did:amy".to_string()),
            signature: MaterializationSignature {
                response_status: Some("streaming".to_string()),
                progress_seq: Some(progress_seq),
                materialized_message_sequence: None,
                response_content_len,
                response_reasoning_len: 0,
                message_count,
                tool_call_count: 2,
                completed_tool_call_count: 2,
                tool_result_count: 2,
            },
        }
    }

    async fn seed_turn_documents(node: &EmbeddedNode) {
        let mutation = r#"mutation {
            create_AgentConversation(input: {
                session_id: "sess-1",
                agent_name: "amy",
                agent_did: "did:test:amy",
                behavior_id: "default",
                title: "Debug session",
                title_source: "generated",
                preview_text: "hello",
                status: "active",
                created_at: "2026-04-22T00:00:00Z",
                updated_at: "2026-04-22T00:00:00Z",
                latest_request_id: "req-1"
            }) { _docID }
            create_AgentRequest(input: {
                request_id: "req-1",
                agent_did: "did:test:amy",
                behavior_id: "default",
                session_id: "sess-1",
                content: "hello",
                status: "processing",
                lifecycle_state: "processing",
                created_at: "2026-04-22T00:00:00Z"
            }) { _docID }
            create_AgentResponse(input: {
                response_key: "req-1",
                request_id: "req-1",
                agent_did: "did:test:amy",
                behavior_id: "default",
                session_id: "sess-1",
                content: "partial",
                reasoning: "",
                status: "streaming",
                error_message: "",
                token_count: 1,
                progress_seq: 1,
                created_at: "2026-04-22T00:00:00Z"
            }) { _docID }
        }"#;
        let response = node.execute(mutation).await;
        assert!(!response.has_errors(), "{:?}", response.errors);
    }

    #[test]
    fn streaming_materialization_candidates_only_include_streaming_latest_turns() {
        let store = ClientStore::from_rows(ClientStoreRows {
            conversations: vec![AgentConversationRow {
                session_id: "sess-1".to_string(),
                agent_name: None,
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: Some("default".to_string()),
                title: None,
                title_source: None,
                preview_text: None,
                status: Some("active".to_string()),
                created_at: None,
                updated_at: None,
                latest_request_id: Some("req-1".to_string()),
            }],
            requests: vec![AgentRequestRow {
                request_id: "req-1".to_string(),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: Some("default".to_string()),
                session_id: Some("sess-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: None,
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                max_total_tokens: None,
                metadata: None,
                status: Some("processing".to_string()),
                lifecycle_state: Some("processing".to_string()),
                backend_id: None,
                execution_origin: None,
                caused_by_trigger_id: None,
                caused_by_trigger_kind: None,
                caused_by_correlation: None,
                caused_by_trigger_context: None,
                caused_by_parent_request_id: None,
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: None,
                claimed_at: None,
                deadline: None,
                retry_count: None,
                max_retries: None,
                interrupt_requested_at: None,
                valid_until: None,
                workspace_id: None,
                workspace_authority: None,
                workspace_owner_deployment_id: None,
                workspace_seal_hash: None,
            }],
            responses: vec![AgentResponseRow {
                response_key: "req-1".to_string(),
                request_id: Some("req-1".to_string()),
                agent_did: Some("did:test:amy".to_string()),
                requester_did: None,
                behavior_id: Some("default".to_string()),
                session_id: Some("sess-1".to_string()),
                content: Some("partial".to_string()),
                reasoning: None,
                status: Some("streaming".to_string()),
                error_message: None,
                token_count: Some(1),
                progress_seq: Some(3),
                reasoning_progress_seq: Some(0),
                materialized_message_sequence: None,
                materialized_at: None,
                created_at: None,
                completed_at: None,
                interrupted_at: None,
            }],
            ..ClientStoreRows::default()
        });

        let candidates = streaming_materialization_candidates(&store);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].request_id, "req-1");
        assert_eq!(candidates[0].signature.response_content_len, 7);
    }

    #[test]
    fn streaming_materialization_candidates_carries_agent_did() {
        let store = ClientStore::from_rows(ClientStoreRows {
            conversations: vec![AgentConversationRow {
                session_id: "sess-1".to_string(),
                agent_name: None,
                agent_did: Some("did:amy".to_string()),
                requester_did: None,
                behavior_id: Some("default".to_string()),
                title: None,
                title_source: None,
                preview_text: None,
                status: Some("active".to_string()),
                created_at: None,
                updated_at: None,
                latest_request_id: Some("req-1".to_string()),
            }],
            requests: vec![AgentRequestRow {
                request_id: "req-1".to_string(),
                agent_did: Some("did:amy".to_string()),
                requester_did: None,
                behavior_id: Some("default".to_string()),
                session_id: Some("sess-1".to_string()),
                retry_parent_request: None,
                retry_root_request: None,
                superseded_by_request: None,
                content: None,
                temperature: None,
                top_p: None,
                top_k: None,
                seed: None,
                max_tokens: None,
                max_total_tokens: None,
                metadata: None,
                status: Some("processing".to_string()),
                lifecycle_state: Some("processing".to_string()),
                backend_id: None,
                execution_origin: None,
                caused_by_trigger_id: None,
                caused_by_trigger_kind: None,
                caused_by_correlation: None,
                caused_by_trigger_context: None,
                caused_by_parent_request_id: None,
                failure_reason: None,
                terminalized_at: None,
                terminal_redrive_attempts: None,
                created_at: None,
                claimed_at: None,
                deadline: None,
                retry_count: None,
                max_retries: None,
                interrupt_requested_at: None,
                valid_until: None,
                workspace_id: None,
                workspace_authority: None,
                workspace_owner_deployment_id: None,
                workspace_seal_hash: None,
            }],
            responses: vec![AgentResponseRow {
                response_key: "req-1".to_string(),
                request_id: Some("req-1".to_string()),
                agent_did: Some("did:amy".to_string()),
                requester_did: None,
                behavior_id: Some("default".to_string()),
                session_id: Some("sess-1".to_string()),
                content: Some("partial".to_string()),
                reasoning: None,
                status: Some("streaming".to_string()),
                error_message: None,
                token_count: Some(1),
                progress_seq: Some(3),
                reasoning_progress_seq: Some(0),
                materialized_message_sequence: None,
                materialized_at: None,
                created_at: None,
                completed_at: None,
                interrupted_at: None,
            }],
            ..ClientStoreRows::default()
        });

        let candidates = streaming_materialization_candidates(&store);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].agent_did.as_deref(), Some("did:amy"));
    }
}
