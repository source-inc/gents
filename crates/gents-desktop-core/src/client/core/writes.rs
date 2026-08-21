use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock as StdRwLock};

use anyhow::{bail, Context, Result};
use defra_p2p_adapter::P2POperations as P2POps;
use gents_protocol::row::{
    AgentBehaviorRow, AgentPrincipalRow, AgentRequestRow, EventTriggerRow, InferenceBackendRow,
    InferenceProfileRow, ScheduleRow, SkillRow, TaskRow, ToolSelectionRow, ToolServiceRegistryRow,
};
use p2p::iroh::parse_public_peer_addr;
use tokio::time::{sleep, Instant};

use super::super::mutations::{self, PeerMutationResult, SubmitRequestOptions, SubmittedRequest};
use super::super::observe::ObservedStore;
use super::super::peer_directory::PeerRecord;
use super::super::query::load_chat_patch;
use super::super::schema::subscribed_collection_names;
use super::super::store::{ClientStore, ClientStoreRows};
use super::bootstrap::{
    add_replicator_with_retry_until, connect_peer_with_retry_until, is_connected_peer,
    normalize_required,
};
use super::p2p_ops;
use super::p2p_ops::{p2p_disconnect_peer, p2p_remove_replicator};
use super::{
    ClientCore, ClientPeerStatus, BOOTSTRAP_OPERATION_BACKOFF, PEER_ADD_OPERATION_TIMEOUT,
};

const REQUEST_PATCH_SIGNATURE_CAPACITY: usize = 2_048;

fn is_terminal_lifecycle_state(value: Option<&str>) -> bool {
    matches!(
        value,
        Some("completed" | "failed" | "superseded" | "dead" | "interrupted")
    )
}

fn row_matches_source(
    sources: &[Option<String>],
    index: usize,
    source_agent_did: &str,
    is_remote_source: bool,
) -> bool {
    match sources.get(index).and_then(|source| source.as_deref()) {
        Some(source) => source == source_agent_did,
        None => !is_remote_source,
    }
}

fn retain_sourced_rows<T>(
    rows: &mut Vec<T>,
    sources: &mut Vec<Option<String>>,
    source_agent_did: &str,
    is_remote_source: bool,
    should_delete: impl Fn(&T) -> bool,
) {
    let previous_rows = std::mem::take(rows);
    let previous_sources = std::mem::take(sources);

    for (index, row) in previous_rows.into_iter().enumerate() {
        if should_delete(&row)
            && row_matches_source(&previous_sources, index, source_agent_did, is_remote_source)
        {
            continue;
        }
        rows.push(row);
        sources.push(previous_sources.get(index).cloned().unwrap_or_default());
    }
}

fn chat_patch_signature(patch: &ClientStore) -> (usize, usize, u64) {
    let rows = patch.row_count();
    match serde_json::to_vec(&patch.to_rows()) {
        Ok(bytes) => {
            let mut hasher = DefaultHasher::new();
            bytes.hash(&mut hasher);
            (rows, bytes.len(), hasher.finish())
        }
        Err(_) => (rows, 0, 0),
    }
}

fn behavior_id_for_write(
    requested_behavior_id: Option<&str>,
    peer_record: Option<&PeerRecord>,
) -> Option<String> {
    requested_behavior_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            peer_record
                .filter(|record| record.is_bearer_pairing())
                .and_then(|record| record.default_behavior_id.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        })
}

fn ensure_peer_chat_ready(peer_record: Option<&PeerRecord>) -> Result<()> {
    if peer_record.is_some_and(|record| !record.is_chat_ready()) {
        bail!(
            "bearer pairing is still waiting for signed membership and reciprocal-replication readiness"
        );
    }
    Ok(())
}

impl ClientCore {
    pub async fn submit_request(
        &self,
        session_id: &str,
        agent_did: &str,
        content: &str,
        behavior_id: Option<&str>,
    ) -> Result<SubmittedRequest> {
        self.submit_request_with_options(
            session_id,
            agent_did,
            content,
            behavior_id,
            SubmitRequestOptions::default(),
        )
        .await
    }

    pub async fn submit_request_with_options(
        &self,
        session_id: &str,
        agent_did: &str,
        content: &str,
        behavior_id: Option<&str>,
        options: SubmitRequestOptions,
    ) -> Result<SubmittedRequest> {
        let snapshot = self.store.snapshot();
        let peer_record = self.peer_record_for_agent(agent_did).await;
        ensure_peer_chat_ready(peer_record.as_ref())?;
        let behavior_id = behavior_id_for_write(behavior_id, peer_record.as_ref());
        match mutations::submit_request(
            self.node.as_ref(),
            snapshot.as_ref(),
            session_id,
            agent_did,
            self.principal.did(),
            content,
            behavior_id.as_deref(),
            options,
        )
        .await
        {
            Ok(result) => {
                self.store
                    .set_focused_request_id(Some(result.request_id.clone()));
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    action = "chat_submit",
                    row_id = %result.request_id,
                    "desktop write saved"
                );
                Ok(result)
            }
            Err(error) => Err(self.record_mutation_error("submit request", error)),
        }
    }

    /// Reconstruct a request's persisted event timeline from the local P2P
    /// replica. Bounded so an unavailable replica fails the panel instead of
    /// hanging it.
    pub async fn request_timeline(
        &self,
        agent_did: &str,
        request_id: &str,
    ) -> Result<gents::run_timeline::RunTimeline> {
        normalize_required("agent_did", agent_did)?;
        let request_id = normalize_required("request_id", request_id)?;
        let access = gents::config_client::ConfigAccess::Local(self.node_arc());
        let timeline = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            gents::run_timeline_fetch::load_run_timeline(&access, request_id),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timed out loading timeline for {request_id}"))?
        .map_err(|error| anyhow::anyhow!("{}", strip_cli_operator_hints(&error.to_string())))?;
        Ok(timeline)
    }

    pub async fn list_tool_call_holds(
        &self,
        agent_did: &str,
    ) -> Result<Vec<gents::config_client::HeldToolCall>> {
        let agent_did = normalize_required("agent_did", agent_did)?;
        let access = gents::config_client::ConfigAccess::Local(self.node_arc());
        let held = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            gents::config_client::list_held_tool_calls(&access, Some(agent_did)),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timed out listing tool-call holds for {agent_did}"))?
        .map_err(|error| anyhow::anyhow!("{}", strip_cli_operator_hints(&error.to_string())))?;
        Ok(held)
    }

    pub async fn resolve_tool_call_hold(
        &self,
        agent_did: &str,
        tool_call_id: &str,
        approve: bool,
        reason: Option<String>,
    ) -> Result<String> {
        let agent_did = normalize_required("agent_did", agent_did)?;
        let tool_call_id = normalize_required("tool_call_id", tool_call_id)?;
        let approval_id = match self
            .resolve_tool_call_hold_inner(agent_did, tool_call_id, approve, reason)
            .await
        {
            Ok(approval_id) => approval_id,
            Err(error) => return Err(self.record_mutation_error("resolve tool-call hold", error)),
        };
        self.clear_mutation_error();
        tracing::info!(
            target: "gents_desktop_core::writes",
            action = "resolve_tool_call_hold",
            row_id = %tool_call_id,
            approve,
            "desktop write saved"
        );
        Ok(approval_id)
    }

    async fn resolve_tool_call_hold_inner(
        &self,
        agent_did: &str,
        tool_call_id: &str,
        approve: bool,
        reason: Option<String>,
    ) -> Result<String> {
        let access = gents::config_client::ConfigAccess::Local(self.node_arc());
        let held = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            gents::config_client::list_held_tool_calls(&access, Some(agent_did)),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timed out listing tool-call holds for {agent_did}"))?
        .map_err(|error| anyhow::anyhow!("{}", strip_cli_operator_hints(&error.to_string())))?;
        let mut targets = held.iter().filter(|call| call.tool_call_id == tool_call_id);
        let target = targets
            .next()
            .ok_or_else(|| anyhow::anyhow!("tool call {tool_call_id} is not awaiting approval"))?;
        if targets.next().is_some() {
            anyhow::bail!(
                "tool call {tool_call_id} is ambiguous across multiple held AgentToolCall documents"
            );
        }
        let verdict = gents::config_client::ToolApprovalVerdict {
            tool_call_doc_id: target.tool_call_doc_id.clone(),
            tool_call_id: tool_call_id.to_string(),
            agent_did: agent_did.to_string(),
            request_id: target.request_id.clone(),
            approve,
            approver_did: self.principal().did().to_string(),
            reason,
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            gents::config_client::write_tool_approval(&access, &verdict),
        )
        .await
        .map_err(|_| anyhow::anyhow!("timed out writing approval decision for {tool_call_id}"))?
        .map_err(|error| anyhow::anyhow!("{}", strip_cli_operator_hints(&error.to_string())))
    }

    pub async fn network_status(&self) -> NetworkStatus {
        let local_peer_id = p2p_ops::p2p_local_peer_id(&self.p2p).await;
        let listen_addresses = p2p_ops::p2p_listen_addresses(&self.p2p).await;
        let connected_peers = p2p_ops::p2p_connected_peers(&self.p2p).await;
        let replicators = p2p_ops::p2p_get_replicators(&self.p2p).await;
        let saved_peers = self.peer_directory.read().await.records().to_vec();

        NetworkStatus {
            local_peer_id: local_peer_id.map_err(|error| error.to_string()),
            listen_addresses: listen_addresses.map_err(|error| error.to_string()),
            connected_peers: connected_peers.map_err(|error| error.to_string()),
            replicators: replicators
                .map(|rows| {
                    rows.into_iter()
                        .map(|info| NetworkReplicator {
                            peer_id: info.id,
                            address: info.address,
                            collections: info.collections,
                            status: info.status,
                            last_status_change: info.last_status_change,
                        })
                        .collect()
                })
                .map_err(|error| error.to_string()),
            saved_peers,
        }
    }

    pub async fn peer_record_for_agent(&self, agent_did: &str) -> Option<PeerRecord> {
        let agent_did = agent_did.trim();
        if agent_did.is_empty() {
            return None;
        }
        let peer_directory = self.peer_directory.read().await;
        peer_directory
            .records()
            .iter()
            .find(|record| record.agent_did == agent_did)
            .cloned()
    }

    pub async fn refresh_local_request(
        &self,
        agent_did: &str,
        request_id: &str,
    ) -> Result<Option<u64>> {
        let agent_did = agent_did.trim();
        let request_id = request_id.trim();
        if agent_did.is_empty() || request_id.is_empty() {
            return Ok(None);
        }

        let patch = load_chat_patch(self.node.as_ref(), request_id).await?;
        let rows = patch.row_count();
        if rows == 0 {
            return Ok(None);
        }
        // This patch came from the embedded replica, just like the observer's
        // baseline snapshot. Keep its source untagged so both paths address a
        // durable document by the same identity.
        let signature = chat_patch_signature(&patch);
        let cache_key = format!("local\0{agent_did}\0{request_id}");
        {
            let mut signatures = self.request_patch_signatures.lock().await;
            if signatures.get(&cache_key) == Some(&signature) {
                return Ok(None);
            }
            if signatures.len() >= REQUEST_PATCH_SIGNATURE_CAPACITY {
                signatures.clear();
            }
            signatures.insert(cache_key, signature);
        }

        let (_rows, bytes, _hash) = signature;
        let terminal = patch
            .request_row(request_id)
            .is_some_and(|row| is_terminal_lifecycle_state(row.lifecycle_state.as_deref()));
        let version = self.store.merge_chat_patch(patch);
        tracing::debug!(
            target: "gents_desktop_core::replication",
            request_id,
            agent_did,
            version,
            rows,
            bytes,
            terminal,
            "desktop selected local request patch merged"
        );
        Ok(Some(version))
    }

    pub async fn rename_conversation(
        &self,
        agent_did: &str,
        session_id: &str,
        title: &str,
    ) -> Result<()> {
        let snapshot = self.store.snapshot();
        let result = mutations::rename_conversation(
            self.node.as_ref(),
            snapshot.as_ref(),
            agent_did,
            self.principal.did(),
            session_id,
            title,
        )
        .await;
        match result {
            Ok(()) => {
                if self.refresh_agent(agent_did).await?.is_none() {
                    self.refresh_store().await?;
                }
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    action = "chat_rename",
                    row_id = %session_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("rename conversation", error)),
        }
    }

    pub async fn delete_skill(&self, skill_id: &str, source_agent_did: &str) -> Result<()> {
        let skill_id = normalize_required("skill_id", skill_id)?;
        let source_agent_did = normalize_required("source_agent_did", source_agent_did)?;
        let snapshot = self.store.snapshot();
        if !snapshot.skills.iter().any(|row| {
            row.skill_id == skill_id && row.agent_did.as_deref() == Some(source_agent_did)
        }) {
            bail!("no Skill document with skill_id {skill_id:?} for {source_agent_did}");
        }

        let affected_behaviors = snapshot
            .behaviors
            .iter()
            .filter(|row| row.agent_did.as_deref() == Some(source_agent_did))
            .filter_map(|row| {
                let mut next = row.clone();
                let refs_before = next.skill_refs.len();
                let excludes_before = next.skill_excludes.len();
                next.skill_refs.retain(|id| id != skill_id);
                next.skill_excludes.retain(|id| id != skill_id);
                (next.skill_refs.len() != refs_before
                    || next.skill_excludes.len() != excludes_before)
                    .then_some(next)
            })
            .collect::<Vec<_>>();

        let result = async {
            let deleted =
                mutations::delete_skill(self.node.as_ref(), source_agent_did, skill_id).await?;
            if deleted == 0 {
                bail!("no Skill document with skill_id {skill_id:?} for {source_agent_did}");
            }
            for behavior in affected_behaviors {
                mutations::upsert_agent_behavior(self.node.as_ref(), &behavior).await?;
            }
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                let refresh_result = self.refresh_config_source(source_agent_did).await;
                complete_confirmed_delete(
                    self.store.as_ref(),
                    &self.last_mutation_error,
                    refresh_result,
                    "delete skill",
                    "config_skill_delete",
                    skill_id,
                    |rows| prune_deleted_skill_rows(rows, source_agent_did, skill_id),
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("delete skill", error)),
        }
    }

    pub async fn delete_task(&self, task_id: &str, source_agent_did: &str) -> Result<()> {
        let task_id = normalize_required("task_id", task_id)?;
        let source_agent_did = normalize_required("source_agent_did", source_agent_did)?;
        let snapshot = self.store.snapshot();
        if !snapshot.tasks.iter().enumerate().any(|(index, row)| {
            row.task_id == task_id
                && row_matches_source(
                    &snapshot.task_source_agent_dids,
                    index,
                    source_agent_did,
                    false,
                )
        }) {
            bail!("no Task document with task_id {task_id:?}");
        }
        let schedule_refs = snapshot
            .schedules
            .iter()
            .enumerate()
            .filter(|(index, row)| {
                row.task_id.as_deref() == Some(task_id)
                    && row_matches_source(
                        &snapshot.schedule_source_agent_dids,
                        *index,
                        source_agent_did,
                        false,
                    )
            })
            .count();
        let trigger_refs = snapshot
            .event_triggers
            .iter()
            .enumerate()
            .filter(|(index, row)| {
                row.task_id.as_deref() == Some(task_id)
                    && row_matches_source(
                        &snapshot.event_trigger_source_agent_dids,
                        *index,
                        source_agent_did,
                        false,
                    )
            })
            .count();
        if schedule_refs + trigger_refs > 0 {
            bail!(
                "task {task_id:?} is referenced by {schedule_refs} schedule(s) and {trigger_refs} event trigger(s); delete or detach those first"
            );
        }

        let result = async {
            let deleted = mutations::delete_task(self.node.as_ref(), task_id).await?;
            if deleted == 0 {
                bail!("no Task document with task_id {task_id:?}");
            }
            Ok(())
        }
        .await;
        self.finish_automation_delete(
            result,
            "delete task",
            "config_task_delete",
            task_id,
            source_agent_did,
            |rows| {
                retain_sourced_rows(
                    &mut rows.tasks,
                    &mut rows.task_source_agent_dids,
                    source_agent_did,
                    false,
                    |row| row.task_id == task_id,
                );
            },
        )
        .await
    }

    pub async fn delete_schedule(&self, schedule_id: &str, source_agent_did: &str) -> Result<()> {
        let schedule_id = normalize_required("schedule_id", schedule_id)?;
        let source_agent_did = normalize_required("source_agent_did", source_agent_did)?;
        let snapshot = self.store.snapshot();
        if !snapshot.schedules.iter().enumerate().any(|(index, row)| {
            row.schedule_id == schedule_id
                && row_matches_source(
                    &snapshot.schedule_source_agent_dids,
                    index,
                    source_agent_did,
                    false,
                )
        }) {
            bail!("no Schedule document with schedule_id {schedule_id:?}");
        }

        let result = async {
            let deleted = mutations::delete_schedule(self.node.as_ref(), schedule_id).await?;
            if deleted == 0 {
                bail!("no Schedule document with schedule_id {schedule_id:?}");
            }
            Ok(())
        }
        .await;
        self.finish_automation_delete(
            result,
            "delete schedule",
            "config_schedule_delete",
            schedule_id,
            source_agent_did,
            |rows| {
                retain_sourced_rows(
                    &mut rows.schedules,
                    &mut rows.schedule_source_agent_dids,
                    source_agent_did,
                    false,
                    |row| row.schedule_id == schedule_id,
                );
            },
        )
        .await
    }

    pub async fn delete_event_trigger(
        &self,
        trigger_id: &str,
        source_agent_did: &str,
    ) -> Result<()> {
        let trigger_id = normalize_required("trigger_id", trigger_id)?;
        let source_agent_did = normalize_required("source_agent_did", source_agent_did)?;
        let snapshot = self.store.snapshot();
        if !snapshot
            .event_triggers
            .iter()
            .enumerate()
            .any(|(index, row)| {
                row.trigger_id == trigger_id
                    && row_matches_source(
                        &snapshot.event_trigger_source_agent_dids,
                        index,
                        source_agent_did,
                        false,
                    )
            })
        {
            bail!("no EventTrigger document with trigger_id {trigger_id:?}");
        }

        let result = async {
            let deleted = mutations::delete_event_trigger(self.node.as_ref(), trigger_id).await?;
            if deleted == 0 {
                bail!("no EventTrigger document with trigger_id {trigger_id:?}");
            }
            Ok(())
        }
        .await;
        self.finish_automation_delete(
            result,
            "delete event trigger",
            "config_event_trigger_delete",
            trigger_id,
            source_agent_did,
            |rows| {
                retain_sourced_rows(
                    &mut rows.event_triggers,
                    &mut rows.event_trigger_source_agent_dids,
                    source_agent_did,
                    false,
                    |row| row.trigger_id == trigger_id,
                );
            },
        )
        .await
    }

    pub async fn delete_inference_backend(
        &self,
        backend_id: &str,
        source_agent_did: &str,
    ) -> Result<()> {
        let backend_id = normalize_required("backend_id", backend_id)?;
        let source_agent_did = normalize_required("source_agent_did", source_agent_did)?;
        let snapshot = self.store.snapshot();
        if !snapshot
            .inference_backends
            .iter()
            .enumerate()
            .any(|(index, row)| {
                row.backend_id == backend_id
                    && row_matches_source(
                        &snapshot.inference_backend_source_agent_dids,
                        index,
                        source_agent_did,
                        false,
                    )
            })
        {
            bail!("no InferenceBackend document with backend_id {backend_id:?}");
        }
        let referencing = snapshot
            .behaviors
            .iter()
            .filter(|row| {
                row.agent_did.as_deref() == Some(source_agent_did)
                    && row.backend_id.as_deref() == Some(backend_id)
            })
            .map(|row| row.behavior_id.clone())
            .collect::<Vec<_>>();
        if !referencing.is_empty() {
            bail!(
                "backend {backend_id:?} is referenced by behavior(s) {}; point them elsewhere first",
                referencing.join(", ")
            );
        }

        let result = async {
            let deleted =
                mutations::delete_inference_backend(self.node.as_ref(), backend_id).await?;
            if deleted == 0 {
                bail!("no InferenceBackend document with backend_id {backend_id:?}");
            }
            Ok(())
        }
        .await;
        self.finish_automation_delete(
            result,
            "delete inference backend",
            "config_backend_delete",
            backend_id,
            source_agent_did,
            |rows| {
                retain_sourced_rows(
                    &mut rows.inference_backends,
                    &mut rows.inference_backend_source_agent_dids,
                    source_agent_did,
                    false,
                    |row| row.backend_id == backend_id,
                );
            },
        )
        .await
    }

    pub async fn delete_inference_profile(
        &self,
        profile_id: &str,
        source_agent_did: &str,
    ) -> Result<()> {
        let profile_id = normalize_required("profile_id", profile_id)?;
        let source_agent_did = normalize_required("source_agent_did", source_agent_did)?;
        let snapshot = self.store.snapshot();
        if !snapshot
            .inference_profiles
            .iter()
            .enumerate()
            .any(|(index, row)| {
                row.profile_id == profile_id
                    && row_matches_source(
                        &snapshot.inference_profile_source_agent_dids,
                        index,
                        source_agent_did,
                        false,
                    )
            })
        {
            bail!("no InferenceProfile document with profile_id {profile_id:?}");
        }
        let referencing = snapshot
            .behaviors
            .iter()
            .filter(|row| {
                row.agent_did.as_deref() == Some(source_agent_did)
                    && row.inference_profile_id.as_deref() == Some(profile_id)
            })
            .map(|row| row.behavior_id.clone())
            .collect::<Vec<_>>();
        if !referencing.is_empty() {
            bail!(
                "profile {profile_id:?} is referenced by behavior(s) {}; point them elsewhere first",
                referencing.join(", ")
            );
        }

        let result = async {
            let deleted =
                mutations::delete_inference_profile(self.node.as_ref(), profile_id).await?;
            if deleted == 0 {
                bail!("no InferenceProfile document with profile_id {profile_id:?}");
            }
            Ok(())
        }
        .await;
        self.finish_automation_delete(
            result,
            "delete inference profile",
            "config_profile_delete",
            profile_id,
            source_agent_did,
            |rows| {
                retain_sourced_rows(
                    &mut rows.inference_profiles,
                    &mut rows.inference_profile_source_agent_dids,
                    source_agent_did,
                    false,
                    |row| row.profile_id == profile_id,
                );
            },
        )
        .await
    }

    pub async fn delete_tool_selection(
        &self,
        selection_id: &str,
        source_agent_did: &str,
    ) -> Result<()> {
        let selection_id = normalize_required("selection_id", selection_id)?;
        let source_agent_did = normalize_required("source_agent_did", source_agent_did)?;
        let snapshot = self.store.snapshot();
        if !snapshot.tool_selections.iter().any(|row| {
            row.selection_id == selection_id && row.agent_did.as_deref() == Some(source_agent_did)
        }) {
            bail!("no ToolSelection document with selection_id {selection_id:?}");
        }
        let referencing = snapshot
            .behaviors
            .iter()
            .filter(|row| {
                row.agent_did.as_deref() == Some(source_agent_did)
                    && row.tool_selection_id.as_deref() == Some(selection_id)
            })
            .map(|row| row.behavior_id.clone())
            .collect::<Vec<_>>();
        if !referencing.is_empty() {
            bail!(
                "tool selection {selection_id:?} is referenced by behavior(s) {}; point them elsewhere first",
                referencing.join(", ")
            );
        }

        let result = async {
            let deleted = mutations::delete_tool_selection(
                self.node.as_ref(),
                source_agent_did,
                selection_id,
            )
            .await?;
            if deleted == 0 {
                bail!("no ToolSelection document with selection_id {selection_id:?}");
            }
            Ok(())
        }
        .await;
        self.finish_automation_delete(
            result,
            "delete tool selection",
            "config_tool_selection_delete",
            selection_id,
            source_agent_did,
            |rows| {
                rows.tool_selections.retain(|row| {
                    row.selection_id != selection_id
                        || row.agent_did.as_deref() != Some(source_agent_did)
                });
            },
        )
        .await
    }

    pub async fn delete_tool_service(
        &self,
        service_id: &str,
        source_agent_did: &str,
    ) -> Result<()> {
        let service_id = normalize_required("service_id", service_id)?;
        let source_agent_did = normalize_required("source_agent_did", source_agent_did)?;
        let snapshot = self.store.snapshot();
        if !snapshot
            .tool_service_registries
            .iter()
            .enumerate()
            .any(|(index, row)| {
                row.service_id == service_id
                    && row_matches_source(
                        &snapshot.tool_service_registry_source_agent_dids,
                        index,
                        source_agent_did,
                        false,
                    )
            })
        {
            bail!("no ToolServiceRegistry document with service_id {service_id:?}");
        }
        let referencing = snapshot
            .tool_selections
            .iter()
            .filter(|row| {
                row.agent_did.as_deref() == Some(source_agent_did)
                    && row
                        .allowed_mcp_service_ids
                        .iter()
                        .any(|id| id == service_id)
            })
            .map(|row| row.selection_id.clone())
            .collect::<Vec<_>>();
        if !referencing.is_empty() {
            bail!(
                "tool service {service_id:?} is allowed by tool selection(s) {}; remove it there first",
                referencing.join(", ")
            );
        }

        let result = async {
            let deleted =
                mutations::delete_tool_service_registry(self.node.as_ref(), service_id).await?;
            if deleted == 0 {
                bail!("no ToolServiceRegistry document with service_id {service_id:?}");
            }
            Ok(())
        }
        .await;
        self.finish_automation_delete(
            result,
            "delete tool service",
            "config_tool_service_delete",
            service_id,
            source_agent_did,
            |rows| {
                retain_sourced_rows(
                    &mut rows.tool_service_registries,
                    &mut rows.tool_service_registry_source_agent_dids,
                    source_agent_did,
                    false,
                    |row| row.service_id == service_id,
                );
            },
        )
        .await
    }

    pub async fn delete_behavior(&self, behavior_id: &str, source_agent_did: &str) -> Result<()> {
        let behavior_id = normalize_required("behavior_id", behavior_id)?;
        let source_agent_did = normalize_required("source_agent_did", source_agent_did)?;
        let snapshot = self.store.snapshot();
        if !snapshot.behaviors.iter().any(|row| {
            row.behavior_id == behavior_id && row.agent_did.as_deref() == Some(source_agent_did)
        }) {
            bail!("no AgentBehavior document with behavior_id {behavior_id:?}");
        }
        let is_default = snapshot.agent_principals.iter().any(|principal| {
            principal.agent_did == source_agent_did
                && principal.default_behavior_id.as_deref() == Some(behavior_id)
        });
        if is_default {
            bail!(
                "behavior {behavior_id:?} is the agent's default behavior; make another behavior the default first"
            );
        }
        let referencing = snapshot
            .tasks
            .iter()
            .enumerate()
            .filter(|(index, task)| {
                task.behavior_id.as_deref() == Some(behavior_id)
                    && row_matches_source(
                        &snapshot.task_source_agent_dids,
                        *index,
                        source_agent_did,
                        false,
                    )
            })
            .map(|(_index, task)| task.task_id.clone())
            .collect::<Vec<_>>();
        if !referencing.is_empty() {
            bail!(
                "behavior {behavior_id:?} is referenced by task(s) {}; repoint or delete those first",
                referencing.join(", ")
            );
        }
        let subagent_referencing = tool_selections_referencing_behavior(
            &snapshot.tool_selections,
            source_agent_did,
            behavior_id,
        );
        if !subagent_referencing.is_empty() {
            bail!(
                "behavior {behavior_id:?} is a subagent target of tool selection(s) {}; remove it there first",
                subagent_referencing.join(", ")
            );
        }

        let result = async {
            let deleted =
                mutations::delete_agent_behavior(self.node.as_ref(), source_agent_did, behavior_id)
                    .await?;
            if deleted == 0 {
                bail!("no AgentBehavior document with behavior_id {behavior_id:?}");
            }
            Ok(())
        }
        .await;
        self.finish_automation_delete(
            result,
            "delete behavior",
            "config_behavior_delete",
            behavior_id,
            source_agent_did,
            |rows| {
                rows.behaviors.retain(|row| {
                    row.behavior_id != behavior_id
                        || row.agent_did.as_deref() != Some(source_agent_did)
                });
            },
        )
        .await
    }

    async fn finish_automation_delete(
        &self,
        result: Result<()>,
        action_label: &str,
        action: &str,
        row_id: &str,
        source_agent_did: &str,
        prune: impl FnOnce(&mut ClientStoreRows),
    ) -> Result<()> {
        match result {
            Ok(()) => {
                let refresh_result = self.refresh_config_source(source_agent_did).await;
                complete_confirmed_delete(
                    self.store.as_ref(),
                    &self.last_mutation_error,
                    refresh_result,
                    action_label,
                    action,
                    row_id,
                    prune,
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error(action_label, error)),
        }
    }

    async fn refresh_config_source(&self, source_agent_did: &str) -> Result<u64> {
        match self.refresh_agent(source_agent_did).await? {
            Some(version) => Ok(version),
            None => self.refresh_store().await,
        }
    }

    pub async fn resend_request(&self, stale_request_id: &str) -> Result<SubmittedRequest> {
        let snapshot = self.store.snapshot();
        let selected_agent_did = self.selected_agent_did();
        let mut candidates = snapshot
            .requests
            .iter()
            .filter(|row| row.request_id == stale_request_id)
            .filter(|row| {
                selected_agent_did
                    .as_deref()
                    .is_none_or(|did| row.agent_did.as_deref() == Some(did))
            });
        let stale = candidates
            .next()
            .ok_or_else(|| anyhow::anyhow!("request {stale_request_id} not found"))?;
        if candidates.next().is_some() {
            bail!("request {stale_request_id} is ambiguous across the selected agent scope");
        }
        let agent_did = stale
            .agent_did
            .as_deref()
            .context("stale request has no agent_did")?;
        let result = mutations::resend_request(
            self.node.as_ref(),
            snapshot.as_ref(),
            stale_request_id,
            agent_did,
            self.principal.did(),
        )
        .await;
        match result {
            Ok(result) => {
                self.store
                    .set_focused_request_id(Some(result.request_id.clone()));
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    action = "chat_resend",
                    row_id = %result.request_id,
                    stale_request_id = %stale_request_id,
                    "desktop write saved"
                );
                Ok(result)
            }
            Err(error) => Err(self.record_mutation_error("resend request", error)),
        }
    }

    pub async fn interrupt_request(&self, request_id: &str) -> Result<()> {
        match mutations::interrupt_request(self.node.as_ref(), request_id).await {
            Ok(()) => {
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    action = "chat_interrupt",
                    row_id = %request_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("interrupt request", error)),
        }
    }

    pub async fn retry_request(&self, parent: &AgentRequestRow) -> Result<SubmittedRequest> {
        let snapshot = self.store.snapshot();
        parent
            .agent_did
            .as_deref()
            .context("retry parent has no agent_did")?;
        let result = mutations::retry_request(
            self.node.as_ref(),
            snapshot.as_ref(),
            parent,
            self.principal.did(),
        )
        .await;
        match result {
            Ok(result) => {
                self.store
                    .set_focused_request_id(Some(result.request_id.clone()));
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    action = "chat_retry",
                    row_id = %result.request_id,
                    "desktop write saved"
                );
                Ok(result)
            }
            Err(error) => Err(self.record_mutation_error("retry request", error)),
        }
    }

    pub async fn rename_peer(&self, peer_id: &str, label: &str) -> Result<()> {
        let peer_id = normalize_required("peer_id", peer_id)?;
        let label = normalize_required("label", label)?;
        let mut peer_directory = self.peer_directory.write().await;
        let record = peer_directory
            .records()
            .iter()
            .find(|record| record.peer_id == peer_id)
            .cloned()
            .with_context(|| format!("peer {peer_id} not found"))?;
        let mut record = record;
        record.label = label.to_string();
        peer_directory.upsert(record).await?;
        self.clear_mutation_error();
        Ok(())
    }

    pub async fn add_peer(
        &self,
        label: &str,
        addr: &str,
        agent_did: &str,
        graphql: Option<&str>,
        default_behavior_id: Option<&str>,
    ) -> Result<PeerMutationResult> {
        let label = normalize_required("label", label)?;
        let addr = normalize_required("addr", addr)?;
        let agent_did = normalize_required("agent_did", agent_did)?;
        let graphql = graphql.map(str::trim).filter(|value| !value.is_empty());
        let default_behavior_id = default_behavior_id
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let record = {
            let mut peer_directory = self.peer_directory.write().await;
            peer_directory
                .upsert_saved_peer_with_graphql(
                    label,
                    addr,
                    agent_did,
                    graphql,
                    default_behavior_id,
                )
                .await?
        };

        let mut warning = None;

        let connected = match connect_peer_with_retry_until(
            &self.p2p,
            &record.addr,
            &record.label,
            PEER_ADD_OPERATION_TIMEOUT,
        )
        .await
        {
            Ok(()) => {
                match add_replicator_with_retry_until(
                    &self.p2p,
                    subscribed_collection_names()
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                    &record.addr,
                    &record.label,
                    PEER_ADD_OPERATION_TIMEOUT,
                )
                .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        append_warning(
                            &mut warning,
                            format!("deployment connected but replication setup failed: {error}"),
                        );
                    }
                }
                true
            }
            Err(error) => {
                append_warning(
                    &mut warning,
                    format!("deployment saved but dial failed: {error}"),
                );
                false
            }
        };

        if let Err(error) = super::bootstrap::configure_local_runtime_pairing(
            self.node.as_ref(),
            &self.p2p,
            &self.principal,
            &record,
        )
        .await
        {
            let prefix = if connected {
                "deployment connected"
            } else {
                "deployment saved"
            };
            append_warning(
                &mut warning,
                format!("{prefix} but reverse pairing failed: {error}"),
            );
        }
        if let Err(error) = self.refresh_agent(&record.agent_did).await {
            append_warning(
                &mut warning,
                format!("deployment saved but local replica refresh failed: {error}"),
            );
        }

        self.update_peer_status(ClientPeerStatus {
            peer_id: record.peer_id.clone(),
            label: record.label.clone(),
            agent_did: record.agent_did.clone(),
            addr: record.addr.clone(),
            dial_succeeded: connected,
            last_error: warning.clone(),
            pairing: Vec::new(),
        });
        self.clear_mutation_error();
        if let Some(warning) = warning.as_deref() {
            tracing::warn!(
                target: "gents_desktop_core::peer",
                peer_id = %record.peer_id,
                label = %record.label,
                error = %warning,
                "desktop deployment add warning"
            );
        } else {
            tracing::info!(
                target: "gents_desktop_core::peer",
                peer_id = %record.peer_id,
                label = %record.label,
                "desktop deployment added"
            );
        }

        Ok(PeerMutationResult {
            peer_id: record.peer_id,
            label: record.label,
            addr: record.addr,
            connected,
            warning,
        })
    }

    pub async fn remove_peer(&self, peer_id: &str) -> Result<PeerMutationResult> {
        let peer_id = normalize_required("peer_id", peer_id)?;
        let record = {
            let peer_directory = self.peer_directory.read().await;
            let record = peer_directory
                .records()
                .iter()
                .find(|record| record.peer_id == peer_id)
                .cloned()
                .with_context(|| format!("peer {peer_id} not found"))?;
            if record.source.as_deref() == Some("local-standard") {
                anyhow::bail!("the local runtime deployment cannot be removed");
            }
            record
        };

        if let Err(error) = cleanup_saved_peer_p2p(&self.p2p, &record).await {
            tracing::warn!(
                target: "gents_desktop_core::peer",
                peer_id = %record.peer_id,
                label = %record.label,
                error = %error,
                "desktop deployment P2P cleanup failed; deployment retained"
            );
            return Err(self.record_mutation_error("remove deployment", error));
        }

        let removed_result = {
            let mut peer_directory = self.peer_directory.write().await;
            let remove_result = peer_directory
                .remove(peer_id)
                .await
                .with_context(|| {
                    format!(
                        "P2P cleanup succeeded but removing peer {peer_id} from saved deployments failed"
                    )
                })
                .and_then(|removed| {
                    removed.with_context(|| format!("peer {peer_id} not found after P2P cleanup"))
                });
            match remove_result {
                Ok(removed) => Ok(removed),
                Err(remove_error) => match peer_directory.upsert(record.clone()).await {
                    Ok(()) => Err(anyhow::anyhow!(
                        "{remove_error}; saved deployment restored and retry is safe"
                    )),
                    Err(restore_error) => Err(anyhow::anyhow!(
                        "{remove_error}; restoring saved deployment also failed: {restore_error}"
                    )),
                },
            }
        };
        let removed = match removed_result {
            Ok(removed) => removed,
            Err(error) => return Err(self.record_mutation_error("remove deployment", error)),
        };

        if let Err(desired_error) =
            delete_peer_pairing_desired(self.node.as_ref(), &record.peer_id).await
        {
            let restore_result = {
                let mut peer_directory = self.peer_directory.write().await;
                peer_directory.upsert(record.clone()).await
            };
            let error = match restore_result {
                Ok(()) => anyhow::anyhow!(
                    "P2P teardown succeeded but pairing desired-state deletion failed: {desired_error}; saved deployment restored and retry is safe"
                ),
                Err(restore_error) => anyhow::anyhow!(
                    "P2P teardown succeeded but pairing desired-state deletion failed: {desired_error}; restoring saved deployment also failed: {restore_error}"
                ),
            };
            tracing::warn!(
                target: "gents_desktop_core::peer",
                peer_id = %record.peer_id,
                label = %record.label,
                error = %error,
                "desktop pairing desired-state deletion failed after P2P cleanup"
            );
            return Err(self.record_mutation_error("remove deployment", error));
        }

        {
            let mut statuses = self
                .peer_statuses
                .write()
                .expect("peer status lock poisoned");
            if let Some(index) = statuses
                .iter()
                .position(|status| status.peer_id == removed.peer_id)
            {
                statuses.remove(index);
            }
        }

        self.clear_mutation_error();
        tracing::info!(
            target: "gents_desktop_core::peer",
            peer_id = %removed.peer_id,
            label = %removed.label,
            "desktop deployment removed"
        );
        Ok(PeerMutationResult {
            peer_id: removed.peer_id,
            label: removed.label,
            addr: removed.addr,
            connected: false,
            warning: None,
        })
    }

    pub async fn save_behavior(&self, row: &AgentBehaviorRow) -> Result<()> {
        let result = mutations::upsert_agent_behavior(self.node.as_ref(), row).await;
        match result {
            Ok(()) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "behavior",
                    row_id = %row.behavior_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("save behavior", error)),
        }
    }

    pub async fn save_agent_principal(&self, row: &AgentPrincipalRow) -> Result<()> {
        let result = mutations::upsert_agent_principal(self.node.as_ref(), row).await;
        match result {
            Ok(()) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "agent_principal",
                    row_id = %row.agent_did,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("save agent principal", error)),
        }
    }

    pub async fn save_backend(&self, row: &InferenceBackendRow) -> Result<()> {
        match mutations::upsert_inference_backend(self.node.as_ref(), row).await {
            Ok(()) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "backend",
                    row_id = %row.backend_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("save backend", error)),
        }
    }

    pub async fn save_tool_selection(&self, row: &ToolSelectionRow) -> Result<()> {
        let result = mutations::upsert_tool_selection(self.node.as_ref(), row).await;
        match result {
            Ok(()) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "tool_selection",
                    row_id = %row.selection_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("save tool selection", error)),
        }
    }

    pub async fn save_tool_service_registry(&self, row: &ToolServiceRegistryRow) -> Result<()> {
        match mutations::upsert_tool_service_registry(self.node.as_ref(), row).await {
            Ok(()) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "tool_service_registry",
                    row_id = %row.service_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("save tool service registry", error)),
        }
    }

    pub async fn save_inference_profile(&self, row: &InferenceProfileRow) -> Result<()> {
        match mutations::upsert_inference_profile(self.node.as_ref(), row).await {
            Ok(()) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "inference_profile",
                    row_id = %row.profile_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("save inference profile", error)),
        }
    }

    pub async fn save_task(&self, row: &TaskRow) -> Result<()> {
        match mutations::upsert_task(self.node.as_ref(), row).await {
            Ok(()) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "task",
                    row_id = %row.task_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("save task", error)),
        }
    }

    pub async fn save_skill(&self, row: &SkillRow) -> Result<()> {
        match mutations::upsert_skill(self.node.as_ref(), row).await {
            Ok(()) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "skill",
                    row_id = %row.skill_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("save skill", error)),
        }
    }

    pub async fn save_schedule(&self, row: &ScheduleRow) -> Result<()> {
        match mutations::upsert_schedule(self.node.as_ref(), row).await {
            Ok(()) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "schedule",
                    row_id = %row.schedule_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("save schedule", error)),
        }
    }

    pub async fn save_event_trigger(&self, row: &EventTriggerRow) -> Result<()> {
        match mutations::upsert_event_trigger(self.node.as_ref(), row).await {
            Ok(()) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "event_trigger",
                    row_id = %row.trigger_id,
                    "desktop write saved"
                );
                Ok(())
            }
            Err(error) => Err(self.record_mutation_error("save event trigger", error)),
        }
    }

    pub async fn fire_task_now(
        &self,
        task_row: &TaskRow,
        args: serde_json::Value,
    ) -> Result<String> {
        match mutations::fire_task_now(self.node.as_ref(), task_row, args).await {
            Ok(doc_id) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "manual_run",
                    task_id = %task_row.task_id,
                    request_doc_id = %doc_id,
                    "desktop manual task run enqueued"
                );
                Ok(doc_id)
            }
            Err(error) => Err(self.record_mutation_error("fire task", error)),
        }
    }

    pub async fn fire_schedule_now(&self, row: &ScheduleRow) -> Result<String> {
        match mutations::fire_schedule_now(self.node.as_ref(), row).await {
            Ok(doc_id) => {
                self.refresh_store().await?;
                self.clear_mutation_error();
                tracing::info!(
                    target: "gents_desktop_core::writes",
                    doc_type = "schedule",
                    row_id = %row.schedule_id,
                    action = "run_now",
                    request_doc_id = %doc_id,
                    "desktop manual schedule run enqueued"
                );
                Ok(doc_id)
            }
            Err(error) => Err(self.record_mutation_error("fire schedule now", error)),
        }
    }

    pub(super) fn update_peer_status(&self, status: ClientPeerStatus) {
        let mut statuses = self
            .peer_statuses
            .write()
            .expect("peer status lock poisoned");
        if let Some(existing) = statuses
            .iter_mut()
            .find(|existing| existing.peer_id == status.peer_id)
        {
            *existing = status;
        } else {
            statuses.push(status);
            statuses.sort_by(|left, right| {
                left.label
                    .to_lowercase()
                    .cmp(&right.label.to_lowercase())
                    .then_with(|| left.peer_id.cmp(&right.peer_id))
            });
        }
    }

    pub(super) fn clear_mutation_error(&self) {
        *self
            .last_mutation_error
            .write()
            .expect("mutation error lock poisoned") = None;
    }

    fn record_mutation_error(&self, operation: &str, error: anyhow::Error) -> anyhow::Error {
        let message = format!("{operation} failed: {error}");
        *self
            .last_mutation_error
            .write()
            .expect("mutation error lock poisoned") = Some(message);
        error
    }
}

fn retain_rows_with_sources<T>(
    rows: &mut Vec<T>,
    sources: &mut Vec<Option<String>>,
    mut keep: impl FnMut(&T) -> bool,
) {
    let mut kept_rows = Vec::with_capacity(rows.len());
    let mut kept_sources = Vec::with_capacity(rows.len());

    for (index, row) in rows.drain(..).enumerate() {
        if keep(&row) {
            kept_rows.push(row);
            kept_sources.push(sources.get(index).cloned().unwrap_or_default());
        }
    }

    *rows = kept_rows;
    *sources = kept_sources;
}

fn prune_deleted_skill_rows(rows: &mut ClientStoreRows, agent_did: &str, skill_id: &str) {
    retain_rows_with_sources(&mut rows.skills, &mut rows.skill_source_agent_dids, |row| {
        !(row.skill_id == skill_id && row.agent_did.as_deref() == Some(agent_did))
    });

    for behavior in rows
        .behaviors
        .iter_mut()
        .filter(|row| row.agent_did.as_deref() == Some(agent_did))
    {
        behavior.skill_refs.retain(|id| id != skill_id);
        behavior.skill_excludes.retain(|id| id != skill_id);
    }
}

fn tool_selections_referencing_behavior(
    selections: &[ToolSelectionRow],
    agent_did: &str,
    behavior_id: &str,
) -> Vec<String> {
    let mut referencing = selections
        .iter()
        .filter(|selection| selection.agent_did.as_deref() == Some(agent_did))
        .filter(|selection| {
            selection.subagent_targets.iter().any(|entry| {
                let Ok(target) = serde_json::from_str::<serde_json::Value>(entry) else {
                    return false;
                };
                target.get("agent_did").and_then(serde_json::Value::as_str) == Some(agent_did)
                    && target
                        .get("behavior_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(behavior_id)
            })
        })
        .map(|selection| selection.selection_id.clone())
        .collect::<Vec<_>>();
    referencing.sort();
    referencing.dedup();
    referencing
}

fn complete_confirmed_delete(
    store: &ObservedStore,
    last_mutation_error: &StdRwLock<Option<String>>,
    refresh_result: Result<u64>,
    action_label: &str,
    action: &str,
    row_id: &str,
    prune: impl FnOnce(&mut ClientStoreRows),
) {
    match refresh_result {
        Ok(_) => {
            *last_mutation_error
                .write()
                .expect("mutation error lock poisoned") = None;
        }
        Err(error) => {
            let warning = format!(
                "{action_label} succeeded, but refreshing the source snapshot failed: {error}"
            );
            *last_mutation_error
                .write()
                .expect("mutation error lock poisoned") = Some(warning);
            tracing::warn!(
                target: "gents_desktop_core::writes",
                action = %action,
                row_id = %row_id,
                error = %error,
                "desktop write saved, but refreshing the source snapshot failed"
            );
        }
    }

    let mut rows = store.snapshot().to_rows();
    prune(&mut rows);
    store.replace_snapshot(ClientStore::from_rows(rows));

    tracing::info!(
        target: "gents_desktop_core::writes",
        action = %action,
        row_id = %row_id,
        "desktop write saved"
    );
}

pub(super) async fn cleanup_saved_peer_p2p(
    p2p: &Arc<dyn P2POps>,
    record: &PeerRecord,
) -> Result<()> {
    let collections = if super::bearer_pairing::is_bearer_peer(record) {
        super::bearer_pairing::bearer_replicator_collections(
            record.pairing_template.as_deref().unwrap_or("conversation"),
        )
    } else {
        subscribed_collection_names()
            .into_iter()
            .map(str::to_owned)
            .collect()
    };
    let replicator_result = p2p_remove_replicator(p2p, collections, &record.addr).await;
    let disconnect_result = async {
        p2p_disconnect_peer(p2p, &record.addr).await?;

        let Some(expected_peer_id) = parse_public_peer_addr(&record.addr)
            .ok()
            .map(|(peer_id, _)| peer_id.to_string())
        else {
            return Ok(());
        };
        let deadline = Instant::now() + PEER_ADD_OPERATION_TIMEOUT;
        loop {
            if !is_connected_peer(p2p, &expected_peer_id).await? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for peer {} to disconnect",
                    expected_peer_id
                );
            }
            sleep(BOOTSTRAP_OPERATION_BACKOFF).await;
        }
    }
    .await;

    match (replicator_result, disconnect_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(replicator_error), Ok(())) => anyhow::bail!(
            "transport disconnected but replicator cleanup failed for {} at {}: {}; saved deployment retained",
            record.label,
            record.addr,
            replicator_error
        ),
        (Ok(()), Err(disconnect_error)) => anyhow::bail!(
            "replicator removed but transport disconnect failed for {} at {}: {}; saved deployment retained",
            record.label,
            record.addr,
            disconnect_error
        ),
        (Err(replicator_error), Err(disconnect_error)) => anyhow::bail!(
            "replicator cleanup failed for {} at {}: {}; transport disconnect also failed: {}; saved deployment retained",
            record.label,
            record.addr,
            replicator_error,
            disconnect_error
        ),
    }
}

async fn delete_peer_pairing_desired(
    node: &defra_node::EmbeddedNode,
    peer_id: &str,
) -> Result<bool> {
    use gents_protocol::graphql::escape_graphql_string;

    let peer_id = escape_graphql_string(peer_id);
    let mutation = format!(
        r#"mutation {{
            delete_PeerPairingDesired(filter: {{ peer_id: {{ _eq: "{peer_id}" }} }}) {{
                _docID
            }}
        }}"#
    );
    let response = node.execute(&mutation).await;
    if response.has_errors() {
        anyhow::bail!(
            "delete PeerPairingDesired failed; saved deployment retained: {}",
            response
                .errors
                .iter()
                .map(|error| error.message.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        );
    }

    let deleted = response
        .data
        .as_ref()
        .and_then(|data| data.get("delete_PeerPairingDesired"))
        .and_then(|rows| rows.as_array())
        .context("delete PeerPairingDesired returned no result rows")?;
    Ok(!deleted.is_empty())
}

fn append_warning(warning: &mut Option<String>, message: String) {
    match warning {
        Some(existing) => {
            existing.push_str("; ");
            existing.push_str(&message);
        }
        None => *warning = Some(message),
    }
}

#[cfg(test)]
mod delete_source_tests {
    use super::*;
    use anyhow::anyhow;
    use serde_json::json;

    fn task(task_id: &str) -> TaskRow {
        serde_json::from_value(json!({ "task_id": task_id })).expect("task row")
    }

    fn tool_selection(
        selection_id: &str,
        agent_did: &str,
        subagent_targets: Vec<String>,
    ) -> ToolSelectionRow {
        serde_json::from_value(json!({
            "selection_id": selection_id,
            "agent_did": agent_did,
            "subagent_targets": subagent_targets,
        }))
        .expect("tool selection row")
    }

    fn peer_record(source: Option<&str>, default_behavior_id: Option<&str>) -> PeerRecord {
        let mut record = PeerRecord::new("Amy", "endpoint-amy", "did:key:amy");
        record.source = source.map(str::to_owned);
        record.default_behavior_id = default_behavior_id.map(str::to_owned);
        record
    }

    #[test]
    fn bearer_peer_signed_default_is_used_when_caller_omits_behavior() {
        let mut peer = peer_record(
            Some(super::super::bearer_pairing::BEARER_PAIRING_SOURCE),
            Some("default"),
        );
        peer.pairing_ready = true;

        ensure_peer_chat_ready(Some(&peer)).unwrap();
        assert_eq!(
            behavior_id_for_write(None, Some(&peer)).as_deref(),
            Some("default")
        );
        assert_eq!(
            behavior_id_for_write(Some(" review "), Some(&peer)).as_deref(),
            Some("review")
        );
    }

    #[test]
    fn pending_bearer_peer_rejects_chat_writes() {
        let peer = peer_record(
            Some(super::super::bearer_pairing::BEARER_PAIRING_SOURCE),
            Some("default"),
        );

        assert!(ensure_peer_chat_ready(Some(&peer))
            .unwrap_err()
            .to_string()
            .contains("still waiting"));
    }

    #[test]
    fn unsigned_legacy_peer_default_is_not_trusted_for_routing() {
        let peer = peer_record(Some("server-status"), Some("forged"));

        assert_eq!(behavior_id_for_write(None, Some(&peer)), None);
    }

    #[test]
    fn source_matching_distinguishes_remote_rows_from_local_rows() {
        let sources = vec![None, Some("did:remote".to_string())];

        assert!(row_matches_source(&sources, 0, "did:local", false));
        assert!(!row_matches_source(&sources, 0, "did:remote", true));
        assert!(row_matches_source(&sources, 1, "did:remote", true));
        assert!(!row_matches_source(&sources, 1, "did:other", true));
    }

    #[test]
    fn sourced_pruning_preserves_same_id_rows_and_parallel_attribution() {
        let mut rows = vec!["shared", "shared", "other"];
        let mut sources = vec![
            None,
            Some("did:remote".to_string()),
            Some("did:remote".to_string()),
        ];

        retain_sourced_rows(&mut rows, &mut sources, "did:remote", true, |row| {
            *row == "shared"
        });

        assert_eq!(rows, vec!["shared", "other"]);
        assert_eq!(sources, vec![None, Some("did:remote".to_string())]);
    }

    #[test]
    fn confirmed_delete_prunes_locally_and_warns_when_refresh_fails() {
        let rows = ClientStoreRows {
            tasks: vec![task("deleted"), task("retained")],
            task_source_agent_dids: vec![None, Some("did:key:remote".to_string())],
            ..ClientStoreRows::default()
        };
        let (store, _version_rx) = ObservedStore::new(ClientStore::from_rows(rows));
        let last_mutation_error = StdRwLock::new(None);

        complete_confirmed_delete(
            store.as_ref(),
            &last_mutation_error,
            Err(anyhow!("replica unavailable")),
            "delete task",
            "config_task_delete",
            "deleted",
            |rows| {
                retain_rows_with_sources(
                    &mut rows.tasks,
                    &mut rows.task_source_agent_dids,
                    |row| row.task_id != "deleted",
                );
            },
        );

        let snapshot = store.snapshot();
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.tasks[0].task_id, "retained");
        assert_eq!(
            snapshot.task_source_agent_dids,
            vec![Some("did:key:remote".to_string())]
        );
        assert_eq!(
            last_mutation_error
                .read()
                .expect("mutation error lock poisoned")
                .as_deref(),
            Some(
                "delete task succeeded, but refreshing the source snapshot failed: replica unavailable"
            )
        );
    }

    #[test]
    fn subagent_behavior_references_are_scoped_to_the_owning_agent() {
        let local_target = json!({
            "name": "local",
            "agent_did": "did:key:alpha",
            "behavior_id": "research",
        })
        .to_string();
        let remote_target = json!({
            "name": "remote",
            "agent_did": "did:key:beta",
            "behavior_id": "research",
        })
        .to_string();
        let selections = vec![
            tool_selection("alpha-local", "did:key:alpha", vec![local_target.clone()]),
            tool_selection("alpha-remote", "did:key:alpha", vec![remote_target]),
            tool_selection("beta-local", "did:key:beta", vec![local_target]),
        ];

        assert_eq!(
            tool_selections_referencing_behavior(&selections, "did:key:alpha", "research"),
            vec!["alpha-local"]
        );
        assert!(
            tool_selections_referencing_behavior(&selections, "did:key:alpha", "writer").is_empty()
        );
    }
}

fn strip_cli_operator_hints(message: &str) -> String {
    message
        .lines()
        .filter(|line| {
            !line.contains("gents init")
                && !line.contains("gents server")
                && !line.contains("--graphql")
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

#[derive(Debug, Clone)]
pub struct NetworkStatus {
    pub local_peer_id: std::result::Result<String, String>,
    pub listen_addresses: std::result::Result<Vec<String>, String>,
    pub connected_peers: std::result::Result<Vec<String>, String>,
    pub replicators: std::result::Result<Vec<NetworkReplicator>, String>,
    pub saved_peers: Vec<super::super::peer_directory::PeerRecord>,
}

#[derive(Debug, Clone)]
pub struct NetworkReplicator {
    pub peer_id: Option<String>,
    pub address: Option<String>,
    pub collections: Vec<String>,
    pub status: Option<u8>,
    pub last_status_change: Option<String>,
}
