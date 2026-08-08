//! Event-trigger `TriggerSource`.
//!
//! Subscribes to DefraDB document events and emits `FireIntent`s whenever an
//! event from a `source_collection` referenced by the active runtime
//! snapshot's `active_event_triggers` lands. The subscription set is kept in
//! sync with the snapshot generation — see `reconcile_subscriptions` (Task 19).
//!
//! This file lands in staged tasks:
//! - Task 18: skeleton only — struct, constructor, no-op `next_fire` stub.
//! - Task 19: `reconcile_subscriptions` drives the desired-collections set
//!   from the snapshot at each generation bump.
//! - Task 20: full `next_fire` loop (poll subscription, filter by desired
//!   collections, build `FireIntent`).
//! - Task 21 (this file): filter probe + doc-var hydration via an
//!   introspected source-doc projection cached per source collection.
//! - Task 22: `on_result` callback body for bookkeeping writes.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use defra_node::{EmbeddedNode, ExecuteRetryPolicy, QueryRequest, QueryResponse};
use identity::Did;
use query::TransactionHandle;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::event_delivery_contract::{EventDeliveryRuntimeContract, EventDeliverySourceContract};
use crate::runtime_snapshot::ActiveRuntimeSnapshot;
use crate::UpdateSubscriptionSource;

use super::subscription_source::UPDATE_SUBSCRIPTION_REOPEN_DELAY;
use super::{FireIntent, TriggerKind, TriggerSource};

/// Page size for complete existing-document scans. Pagination, rather than a
/// total cap, is required for forward-only semantics: every pre-existing row
/// must be seeded, and every eligible durable row must remain recoverable after
/// a dropped subscription wake.
const SEEN_DOCS_PAGE_SIZE: usize = 500;
const EVENT_SOURCE_RESCAN_INTERVAL: Duration = Duration::from_secs(5);

pub struct EventSource {
    snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
    node: Arc<EmbeddedNode>,
    subscription_source: Arc<dyn UpdateSubscriptionSource>,
    subscription: Option<events::Subscription>,
    desired_collections: HashSet<String>,
    reconciled_generation: u64,
    #[allow(dead_code)]
    reconcile_debounce: Duration,
    cancel: CancellationToken,
    source_schema_cache: SourceSchemaCache,
    collection_id_to_name: HashMap<String, String>,
    pending_intents: Mutex<VecDeque<FireIntent>>,
    /// Periodic live rescan that closes the lossy-subscription gap. The
    /// interval is stored on the source so a busy stream of `next_fire()` calls
    /// does not reset the cadence.
    rescan_tick: tokio::time::Interval,
}

#[derive(Debug, Deserialize)]
struct SourceDocIdRow {
    #[serde(rename = "_docID")]
    doc_id: String,
}

#[derive(Debug, Deserialize)]
struct TxnCommitParentRow {
    cid: String,
    #[serde(rename = "fieldName")]
    field_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TxnCompositeHeadRow {
    cid: String,
    #[serde(default)]
    heads: Vec<TxnCommitParentRow>,
}

const EVENT_ACTIVATION_MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BaselineSourceRef {
    doc_id: String,
    composite_commit_cid: String,
    signer_did: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EventActivationManifest {
    manifest_version: u32,
    source_collection: String,
    sources: Vec<BaselineSourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct EventTriggerActivationRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    activation_key: String,
    agent_did: String,
    trigger_id: String,
    trigger_doc_id: String,
    trigger_commit_cid: String,
    trigger_signer_did: String,
    source_collection: String,
    event_kind: String,
    baseline_manifest_version: u32,
    baseline_source_manifest_json: String,
    created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct EventDeliveryAdmissionRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    delivery_key: String,
    request_id: String,
    agent_did: String,
    trigger_id: String,
    trigger_doc_id: String,
    trigger_commit_cid: String,
    trigger_signer_did: String,
    activation_doc_id: String,
    activation_commit_cid: String,
    activation_signer_did: String,
    source_collection: String,
    source_doc_id: String,
    source_commit_cid: String,
    source_signer_did: String,
    event_kind: String,
    created_at: String,
}

#[derive(Debug, Clone)]
struct LoadedActivation {
    row: EventTriggerActivationRow,
    source: crate::SignedDocumentVersionRef,
    manifest: EventActivationManifest,
}

#[derive(Debug, Clone)]
struct DeliveryWork {
    request_id: String,
    source: crate::SignedDocumentVersionRef,
}

async fn execute_with_identity(node: &EmbeddedNode, query: String, identity: Did) -> QueryResponse {
    node.execute_request_with_retry(
        QueryRequest::new(query).with_identity(Some(identity)),
        ExecuteRetryPolicy::default(),
    )
    .await
}

async fn execute_in_txn_with_identity(
    node: &EmbeddedNode,
    handle: &TransactionHandle,
    query: String,
    identity: Did,
) -> anyhow::Result<QueryResponse> {
    let response = node
        .execute_request_in_txn(
            QueryRequest::new(query).with_identity(Some(identity)),
            handle,
        )
        .await;
    if response.has_errors() {
        anyhow::bail!("transactional GraphQL failed: {:?}", response.errors);
    }
    Ok(response)
}

/// Per-source-collection schema cache.
///
/// `fields_for(collection, node)` runs a one-shot GraphQL introspection
/// (`__type(name: "<collection>") { fields { name } }`) the first time a
/// given source collection is seen, then memoizes the resulting projectable
/// field list. Subsequent hydrations for the same collection are a pure
/// cache hit. Entries are never invalidated — the active schema for a
/// collection is stable across the runtime's lifetime, and any schema
/// migration produces a new collection version whose identity the fire
/// path treats as a distinct source.
///
/// Filtering: DefraDB's GraphQL introspection exposes several auto-
/// generated fields on every collection (aggregates like `_count`,
/// `_sum`, and per-field wrappers). These are not direct scalars and
/// cannot be included in a plain projection — selecting them without
/// required arguments produces a parse error. We filter aggressively:
/// drop anything starting with `_` (GraphQL meta / DefraDB aggregate) and
/// anything whose name is an upper-case aggregate keyword.
#[derive(Default)]
pub(crate) struct SourceSchemaCache {
    by_collection: tokio::sync::Mutex<HashMap<String, Vec<String>>>,
}

impl SourceSchemaCache {
    async fn fields_for(
        &self,
        collection: &str,
        node: &EmbeddedNode,
        identity: Did,
    ) -> anyhow::Result<Vec<String>> {
        crate::graphql::validate_collection_identifier(collection)?;
        let mut guard = self.by_collection.lock().await;
        if let Some(fields) = guard.get(collection) {
            return Ok(fields.clone());
        }
        let query = format!(
            r#"query {{
                __type(name: "{name}") {{
                    fields {{ name }}
                }}
            }}"#,
            name = collection,
        );
        let response = execute_with_identity(node, query, identity).await;
        if response.has_errors() {
            anyhow::bail!("introspect {} failed: {:?}", collection, response.errors);
        }
        let Some(fields_arr) = response
            .data
            .as_ref()
            .and_then(|d| d.get("__type"))
            .and_then(|t| t.get("fields"))
            .and_then(serde_json::Value::as_array)
        else {
            anyhow::bail!("introspection returned no fields for {}", collection);
        };
        let fields: Vec<String> = fields_arr
            .iter()
            .filter_map(|f| f.get("name").and_then(|n| n.as_str()).map(str::to_string))
            .filter(|name| !name.starts_with('_'))
            .filter(|name| !is_defradb_aggregate_field(name))
            .collect();
        guard.insert(collection.to_string(), fields.clone());
        Ok(fields)
    }
}

fn is_defradb_aggregate_field(name: &str) -> bool {
    matches!(
        name,
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP" | "SIMILARITY" | "BM25"
    )
}

fn event_source_rescan_tick(interval: Duration) -> tokio::time::Interval {
    let interval = if interval.is_zero() {
        EVENT_SOURCE_RESCAN_INTERVAL
    } else {
        interval
    };
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick
}

impl EventSource {
    fn query_identity(&self) -> anyhow::Result<Did> {
        let did = self.node.node_identity_did().ok_or_else(|| {
            anyhow::anyhow!("EventSource durable operations require a DefraDB node identity")
        })?;
        Did::new(did).map_err(Into::into)
    }

    fn key_part(value: &str) -> String {
        format!("{}:{value}", value.len())
    }

    fn activation_key(
        trigger_doc_id: &str,
        trigger_commit_cid: &str,
        source_collection: &str,
        event_kind: &str,
    ) -> String {
        format!(
            "v1:{}:{}:{}:{}",
            Self::key_part(trigger_doc_id),
            Self::key_part(trigger_commit_cid),
            Self::key_part(source_collection),
            Self::key_part(event_kind)
        )
    }

    fn delivery_key(
        trigger_doc_id: &str,
        source_collection: &str,
        source_doc_id: &str,
        event_kind: &str,
    ) -> String {
        format!(
            "v1:{}:{}:{}:{}",
            Self::key_part(trigger_doc_id),
            Self::key_part(source_collection),
            Self::key_part(source_doc_id),
            Self::key_part(event_kind)
        )
    }

    pub fn new(
        snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
        node: Arc<EmbeddedNode>,
        cancel: CancellationToken,
    ) -> Self {
        Self::with_subscription_source(node.clone(), snapshot_rx, node, cancel)
    }

    pub fn with_subscription_source(
        subs: Arc<dyn UpdateSubscriptionSource>,
        snapshot_rx: watch::Receiver<Arc<ActiveRuntimeSnapshot>>,
        node: Arc<EmbeddedNode>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            snapshot_rx,
            node,
            subscription_source: subs,
            subscription: None,
            desired_collections: HashSet::new(),
            reconciled_generation: 0,
            reconcile_debounce: Duration::from_millis(250),
            cancel,
            source_schema_cache: SourceSchemaCache::default(),
            collection_id_to_name: HashMap::new(),
            pending_intents: Mutex::new(VecDeque::new()),
            rescan_tick: event_source_rescan_tick(EVENT_SOURCE_RESCAN_INTERVAL),
        }
    }

    #[doc(hidden)]
    pub fn with_rescan_interval(mut self, interval: Duration) -> Self {
        self.rescan_tick = event_source_rescan_tick(interval);
        self
    }

    /// Override the reconciliation debounce. Test-only hook mirroring
    /// `ScheduleSource::with_tick_every`.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn with_reconcile_debounce(mut self, debounce: Duration) -> Self {
        self.reconcile_debounce = debounce;
        self
    }

    /// Snapshot of the source-collection names the event source is currently
    /// filtering on. Test-only accessor.
    #[cfg(test)]
    pub(crate) fn subscribed_collections(&self) -> Vec<String> {
        let mut v: Vec<String> = self.desired_collections.iter().cloned().collect();
        v.sort();
        v
    }

    pub(crate) async fn reconcile_subscriptions(&mut self, snapshot: &ActiveRuntimeSnapshot) {
        // Resolve-time quarantine keeps identifier-invalid source
        // collections out of active_event_triggers, but this source must
        // not trust that another code path assembled the snapshot the same
        // way: names that fail the collection-identifier check never enter
        // the desired set, so no seed/rescan/probe query is built from them.
        let desired: HashSet<String> = snapshot
            .active_event_triggers()
            .values()
            .map(|t| t.source_collection.clone())
            .filter(
                |collection| match crate::graphql::validate_collection_identifier(collection) {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::warn!(
                            source_collection = %collection,
                            generation = snapshot.generation,
                            %error,
                            "event source refusing to observe source collection: \
                             not a valid GraphQL collection identifier",
                        );
                        false
                    }
                },
            )
            .collect();

        let added: Vec<String> = desired
            .difference(&self.desired_collections)
            .cloned()
            .collect();
        for added_collection in &added {
            tracing::info!(
                source_collection = %added_collection,
                generation = snapshot.generation,
                "event source now observing source collection",
            );
        }
        for removed in self.desired_collections.difference(&desired) {
            tracing::info!(
                source_collection = %removed,
                generation = snapshot.generation,
                "event source no longer observing source collection",
            );
        }

        self.desired_collections = desired;

        let mut triggers = snapshot
            .active_event_triggers()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        triggers.sort_by(|left, right| left.trigger_id.cmp(&right.trigger_id));
        let author_did = self.node.node_identity_did().unwrap_or_default().to_owned();
        for trigger in &triggers {
            if let Err(err) = self.ensure_activation(trigger, &author_did).await {
                tracing::warn!(
                    trigger_id = %trigger.trigger_id,
                    source_collection = %trigger.source_collection,
                    %err,
                    "event source could not establish durable activation baseline",
                );
            }
        }

        if self.subscription.is_none() && !self.desired_collections.is_empty() {
            let subscription = self.subscription_source.subscribe_updates();
            tracing::info!(
                collections = self.desired_collections.len(),
                generation = snapshot.generation,
                "event source opened global Update subscription",
            );
            self.subscription = Some(subscription);
        }

        self.reconciled_generation = snapshot.generation;
    }

    async fn load_doc_ids_for_collection(&self, collection: &str) -> anyhow::Result<Vec<String>> {
        crate::graphql::validate_collection_identifier(collection)?;
        let mut doc_ids = Vec::new();
        let mut offset = 0usize;
        loop {
            let query = format!(
                r#"query {{
                    {collection}(
                        order: {{ _docID: ASC }},
                        limit: {limit},
                        offset: {offset}
                    ) {{ _docID }}
                }}"#,
                collection = collection,
                limit = SEEN_DOCS_PAGE_SIZE,
                offset = offset,
            );
            let response = execute_with_identity(&self.node, query, self.query_identity()?).await;
            if response.has_errors() {
                anyhow::bail!(
                    "event source rescan page for {} offset={} failed: {:?}",
                    collection,
                    offset,
                    response.errors
                );
            }
            let rows: Vec<SourceDocIdRow> = response
                .data
                .as_ref()
                .and_then(|data| data.get(collection))
                .cloned()
                .map(serde_json::from_value)
                .transpose()?
                .unwrap_or_default();
            let page_len = rows.len();
            doc_ids.extend(rows.into_iter().map(|row| row.doc_id));
            if page_len < SEEN_DOCS_PAGE_SIZE {
                break;
            }
            offset += page_len;
        }
        Ok(doc_ids)
    }

    async fn exact_current_ref(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> anyhow::Result<crate::SignedDocumentVersionRef> {
        crate::document_version::verified_current_signed_document_version_with_identity(
            &self.node,
            collection,
            doc_id,
            Some(self.query_identity()?),
        )
        .await
    }

    async fn verify_pinned_ref(
        &self,
        collection: &str,
        doc_id: &str,
        cid: &str,
        signer_did: &str,
    ) -> anyhow::Result<()> {
        if doc_id.trim().is_empty() || cid.trim().is_empty() || signer_did.trim().is_empty() {
            anyhow::bail!("{collection} exact event-delivery reference is incomplete");
        }
        let signer = self.node.verified_block_signer_did(cid).await?;
        if signer != signer_did {
            anyhow::bail!(
                "{collection} {doc_id} pinned signer {signer_did} disagrees with cryptographic signer {signer}"
            );
        }
        let escaped_cid = crate::graphql::escape_graphql_string(cid);
        let response = execute_with_identity(
            &self.node,
            format!(r#"{{ {collection}(cid: ["{escaped_cid}"]) {{ _docID }} }}"#),
            self.query_identity()?,
        )
        .await;
        if response.has_errors() {
            anyhow::bail!(
                "loading exact {collection} event-delivery source {cid}: {:?}",
                response.errors
            );
        }
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get(collection))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("exact {collection} query returned no rows"))?;
        match rows.as_slice() {
            [row] if row.get("_docID").and_then(serde_json::Value::as_str) == Some(doc_id) => {
                Ok(())
            }
            rows => anyhow::bail!(
                "exact {collection} CID {cid} reconstructed {} documents or a different _docID",
                rows.len()
            ),
        }
    }

    async fn load_trigger_ref(
        &self,
        trigger: &crate::runtime_snapshot::ResolvedEventTrigger,
    ) -> anyhow::Result<crate::SignedDocumentVersionRef> {
        let escaped_trigger_id = crate::graphql::escape_graphql_string(&trigger.trigger_id);
        let response = execute_with_identity(
            &self.node,
            format!(
                r#"{{ EventTrigger(filter: {{ trigger_id: {{ _eq: "{escaped_trigger_id}" }} }}) {{
                    _docID trigger_id task_id source_collection event_kind filter enabled concurrency
                }} }}"#
            ),
            self.query_identity()?,
        )
        .await;
        if response.has_errors() {
            anyhow::bail!(
                "loading EventTrigger {} exact candidates: {:?}",
                trigger.trigger_id,
                response.errors
            );
        }
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("EventTrigger"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("EventTrigger candidate query returned no rows"))?;
        let row = match rows.as_slice() {
            [row] => row,
            rows => anyhow::bail!(
                "EventTrigger {} has {} visible logical candidates; refusing event admission",
                trigger.trigger_id,
                rows.len()
            ),
        };
        let doc_id = row
            .get("_docID")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("EventTrigger candidate has no _docID"))?;
        let matches_snapshot = row
            .get("task_id")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value == trigger.task_id)
            && row
                .get("source_collection")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == trigger.source_collection)
            && row
                .get("event_kind")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == trigger.event_kind)
            && row
                .get("concurrency")
                .and_then(serde_json::Value::as_str)
                .and_then(crate::runtime_snapshot::ConcurrencyMode::parse)
                == Some(trigger.concurrency)
            && row.get("filter").and_then(serde_json::Value::as_str) == trigger.filter.as_deref()
            && row
                .get("enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
        if !matches_snapshot {
            anyhow::bail!(
                "EventTrigger {} changed after the active runtime snapshot was resolved",
                trigger.trigger_id
            );
        }
        self.exact_current_ref("EventTrigger", doc_id).await
    }

    async fn exact_current_ref_in_txn(
        &self,
        handle: &TransactionHandle,
        collection: &str,
        doc_id: &str,
    ) -> anyhow::Result<crate::SignedDocumentVersionRef> {
        let response = execute_in_txn_with_identity(
            &self.node,
            handle,
            format!(
                r#"{{ _commits(docID: ["{}"], filter: {{ fieldName: {{ _eq: "_C" }} }}) {{
                    cid heads {{ cid fieldName }}
                }} }}"#,
                crate::graphql::escape_graphql_string(doc_id),
            ),
            self.query_identity()?,
        )
        .await?;
        let rows: Vec<TxnCompositeHeadRow> = response
            .data
            .as_ref()
            .and_then(|data| data.get("_commits"))
            .cloned()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        let nested = rows
            .iter()
            .flat_map(|row| row.heads.iter())
            .filter(|head| head.field_name.as_deref() == Some("_C"))
            .map(|head| head.cid.as_str())
            .collect::<HashSet<_>>();
        let current = rows
            .iter()
            .filter(|row| !nested.contains(row.cid.as_str()))
            .collect::<Vec<_>>();
        let current = match current.as_slice() {
            [current] => *current,
            rows => anyhow::bail!(
                "transaction snapshot has {} current composite heads for {collection} {doc_id}",
                rows.len()
            ),
        };
        let signer_did = self.node.verified_block_signer_did(&current.cid).await?;
        if signer_did.trim().is_empty() {
            anyhow::bail!("{collection} {doc_id} transaction snapshot has no verified signer");
        }
        Ok(crate::SignedDocumentVersionRef::new(
            crate::DocumentVersionRef::new(doc_id, &current.cid),
            signer_did,
        ))
    }

    async fn create_activation_in_snapshot(
        &self,
        trigger: &crate::runtime_snapshot::ResolvedEventTrigger,
        trigger_source: &crate::SignedDocumentVersionRef,
        agent_did: &str,
    ) -> anyhow::Result<(String, String)> {
        let handle = self.node.runner().begin_txn(false).await.map_err(|error| {
            anyhow::anyhow!("begin EventSource activation transaction: {error}")
        })?;
        let result = self
            .create_activation_in_snapshot_inner(&handle, trigger, trigger_source, agent_did)
            .await;
        match result {
            Ok(result) => {
                self.node
                    .runner()
                    .commit_txn(&handle)
                    .await
                    .map_err(|error| anyhow::anyhow!("commit EventSource activation: {error}"))?;
                Ok(result)
            }
            Err(error) => {
                if let Err(rollback_error) = self.node.runner().rollback_txn(&handle).await {
                    tracing::warn!(%rollback_error, "rolling back EventSource activation failed");
                }
                Err(error)
            }
        }
    }

    async fn create_activation_in_snapshot_inner(
        &self,
        handle: &TransactionHandle,
        trigger: &crate::runtime_snapshot::ResolvedEventTrigger,
        trigger_source: &crate::SignedDocumentVersionRef,
        agent_did: &str,
    ) -> anyhow::Result<(String, String)> {
        let transaction_trigger = self
            .exact_current_ref_in_txn(handle, "EventTrigger", &trigger_source.version.doc_id)
            .await?;
        if &transaction_trigger != trigger_source {
            anyhow::bail!("EventTrigger changed before activation snapshot was established");
        }

        let mut sources = Vec::new();
        let mut offset = 0usize;
        loop {
            let response = execute_in_txn_with_identity(
                &self.node,
                handle,
                format!(
                    r#"{{ {}(order: {{ _docID: ASC }}, limit: {}, offset: {}) {{ _docID }} }}"#,
                    trigger.source_collection, SEEN_DOCS_PAGE_SIZE, offset,
                ),
                self.query_identity()?,
            )
            .await?;
            let rows: Vec<SourceDocIdRow> = response
                .data
                .as_ref()
                .and_then(|data| data.get(&trigger.source_collection))
                .cloned()
                .map(serde_json::from_value)
                .transpose()?
                .unwrap_or_default();
            let page_len = rows.len();
            for row in rows {
                let source = self
                    .exact_current_ref_in_txn(handle, &trigger.source_collection, &row.doc_id)
                    .await?;
                sources.push(BaselineSourceRef {
                    doc_id: source.version.doc_id,
                    composite_commit_cid: source.version.composite_commit_cid,
                    signer_did: source.signer_did,
                });
            }
            if page_len < SEEN_DOCS_PAGE_SIZE {
                break;
            }
            offset += page_len;
        }
        sources.sort_by(|left, right| left.doc_id.cmp(&right.doc_id));
        if sources
            .windows(2)
            .any(|pair| pair[0].doc_id == pair[1].doc_id)
        {
            anyhow::bail!("activation snapshot contains duplicate source _docIDs");
        }
        let manifest = EventActivationManifest {
            manifest_version: EVENT_ACTIVATION_MANIFEST_VERSION,
            source_collection: trigger.source_collection.clone(),
            sources,
        };
        let manifest_json =
            crate::rendered_request::canonical_json_string(&serde_json::to_value(&manifest)?)?;
        let activation_key = Self::activation_key(
            &trigger_source.version.doc_id,
            &trigger_source.version.composite_commit_cid,
            &trigger.source_collection,
            &trigger.event_kind,
        );
        let mutation = format!(
            r#"mutation {{ create_EventTriggerActivation(input: {{
                activation_key: "{}" agent_did: "{}" trigger_id: "{}"
                trigger_doc_id: "{}" trigger_commit_cid: "{}" trigger_signer_did: "{}"
                source_collection: "{}" event_kind: "{}" baseline_manifest_version: {}
                baseline_source_manifest_json: "{}" created_at: "{}"
            }}) {{ _docID }} }}"#,
            crate::graphql::escape_graphql_string(&activation_key),
            crate::graphql::escape_graphql_string(agent_did),
            crate::graphql::escape_graphql_string(&trigger.trigger_id),
            crate::graphql::escape_graphql_string(&trigger_source.version.doc_id),
            crate::graphql::escape_graphql_string(&trigger_source.version.composite_commit_cid),
            crate::graphql::escape_graphql_string(&trigger_source.signer_did),
            crate::graphql::escape_graphql_string(&trigger.source_collection),
            crate::graphql::escape_graphql_string(&trigger.event_kind),
            EVENT_ACTIVATION_MANIFEST_VERSION,
            crate::graphql::escape_graphql_string(&manifest_json),
            crate::graphql::escape_graphql_string(&chrono::Utc::now().to_rfc3339()),
        );
        execute_in_txn_with_identity(&self.node, handle, mutation, self.query_identity()?).await?;
        Ok((activation_key, manifest_json))
    }

    async fn load_activation_candidates(
        &self,
        trigger_doc_id: &str,
        trigger_commit_cid: &str,
        source_collection: &str,
        event_kind: &str,
    ) -> anyhow::Result<Vec<EventTriggerActivationRow>> {
        let response = execute_with_identity(
            &self.node,
            format!(
                r#"{{ EventTriggerActivation(filter: {{
                    trigger_doc_id: {{ _eq: "{}" }},
                    trigger_commit_cid: {{ _eq: "{}" }},
                    source_collection: {{ _eq: "{}" }},
                    event_kind: {{ _eq: "{}" }}
                }}) {{
                    _docID activation_key agent_did trigger_id trigger_doc_id
                    trigger_commit_cid trigger_signer_did source_collection event_kind
                    baseline_manifest_version baseline_source_manifest_json created_at
                }} }}"#,
                crate::graphql::escape_graphql_string(trigger_doc_id),
                crate::graphql::escape_graphql_string(trigger_commit_cid),
                crate::graphql::escape_graphql_string(source_collection),
                crate::graphql::escape_graphql_string(event_kind),
            ),
            self.query_identity()?,
        )
        .await;
        if response.has_errors() {
            anyhow::bail!(
                "loading EventTriggerActivation candidates: {:?}",
                response.errors
            );
        }
        response
            .data
            .as_ref()
            .and_then(|data| data.get("EventTriggerActivation"))
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map(|rows| rows.unwrap_or_default())
            .map_err(Into::into)
    }

    async fn verify_activation_row(
        &self,
        row: EventTriggerActivationRow,
        expected_agent_did: &str,
    ) -> anyhow::Result<LoadedActivation> {
        if row.agent_did != expected_agent_did {
            anyhow::bail!(
                "EventTriggerActivation {} belongs to agent {}, expected {expected_agent_did}",
                row.doc_id,
                row.agent_did
            );
        }
        let source = self
            .exact_current_ref("EventTriggerActivation", &row.doc_id)
            .await?;
        if source.signer_did != row.agent_did {
            anyhow::bail!(
                "EventTriggerActivation {} signer {} does not match agent {}",
                row.doc_id,
                source.signer_did,
                row.agent_did
            );
        }
        self.verify_pinned_ref(
            "EventTrigger",
            &row.trigger_doc_id,
            &row.trigger_commit_cid,
            &row.trigger_signer_did,
        )
        .await?;
        let manifest: EventActivationManifest =
            serde_json::from_str(&row.baseline_source_manifest_json)?;
        if manifest.manifest_version != EVENT_ACTIVATION_MANIFEST_VERSION
            || row.baseline_manifest_version != EVENT_ACTIVATION_MANIFEST_VERSION
            || manifest.source_collection != row.source_collection
        {
            anyhow::bail!(
                "EventTriggerActivation {} has invalid manifest metadata",
                row.doc_id
            );
        }
        let canonical =
            crate::rendered_request::canonical_json_string(&serde_json::to_value(&manifest)?)?;
        if canonical != row.baseline_source_manifest_json {
            anyhow::bail!(
                "EventTriggerActivation {} manifest is not canonical",
                row.doc_id
            );
        }
        if manifest
            .sources
            .windows(2)
            .any(|pair| pair[0].doc_id >= pair[1].doc_id)
        {
            anyhow::bail!(
                "EventTriggerActivation {} baseline is not canonical",
                row.doc_id
            );
        }
        Ok(LoadedActivation {
            row,
            source,
            manifest,
        })
    }

    async fn ensure_activation(
        &self,
        trigger: &crate::runtime_snapshot::ResolvedEventTrigger,
        agent_did: &str,
    ) -> anyhow::Result<LoadedActivation> {
        let trigger_source = self.load_trigger_ref(trigger).await?;
        let candidates = self
            .load_activation_candidates(
                &trigger_source.version.doc_id,
                &trigger_source.version.composite_commit_cid,
                &trigger.source_collection,
                &trigger.event_kind,
            )
            .await?;
        match candidates.as_slice() {
            [row] => return self.verify_activation_row(row.clone(), agent_did).await,
            [] => {}
            rows => anyhow::bail!(
                "EventTriggerActivation logical twins for trigger {}: {} visible rows",
                trigger.trigger_id,
                rows.len()
            ),
        }

        let (activation_key, manifest_json) = self
            .create_activation_in_snapshot(trigger, &trigger_source, agent_did)
            .await?;
        let candidates = self
            .load_activation_candidates(
                &trigger_source.version.doc_id,
                &trigger_source.version.composite_commit_cid,
                &trigger.source_collection,
                &trigger.event_kind,
            )
            .await?;
        match candidates.as_slice() {
            [row]
                if row.activation_key == activation_key
                    && row.trigger_commit_cid == trigger_source.version.composite_commit_cid
                    && row.baseline_source_manifest_json == manifest_json =>
            {
                self.verify_activation_row(row.clone(), agent_did).await
            }
            rows => anyhow::bail!(
                "EventTriggerActivation create-and-compare observed {} conflicting rows",
                rows.len()
            ),
        }
    }

    async fn load_delivery_candidates(
        &self,
        trigger_doc_id: &str,
        source_collection: &str,
        source_doc_id: &str,
        event_kind: &str,
    ) -> anyhow::Result<Vec<EventDeliveryAdmissionRow>> {
        let response = execute_with_identity(
            &self.node,
            format!(
                r#"{{ EventDeliveryAdmission(filter: {{
                    trigger_doc_id: {{ _eq: "{}" }},
                    source_collection: {{ _eq: "{}" }},
                    source_doc_id: {{ _eq: "{}" }},
                    event_kind: {{ _eq: "{}" }}
                }}) {{
                    _docID delivery_key request_id agent_did trigger_id trigger_doc_id
                    trigger_commit_cid trigger_signer_did activation_doc_id
                    activation_commit_cid activation_signer_did source_collection
                    source_doc_id source_commit_cid source_signer_did event_kind created_at
                }} }}"#,
                crate::graphql::escape_graphql_string(trigger_doc_id),
                crate::graphql::escape_graphql_string(source_collection),
                crate::graphql::escape_graphql_string(source_doc_id),
                crate::graphql::escape_graphql_string(event_kind),
            ),
            self.query_identity()?,
        )
        .await;
        if response.has_errors() {
            anyhow::bail!(
                "loading EventDeliveryAdmission candidates: {:?}",
                response.errors
            );
        }
        response
            .data
            .as_ref()
            .and_then(|data| data.get("EventDeliveryAdmission"))
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map(|rows| rows.unwrap_or_default())
            .map_err(Into::into)
    }

    async fn verify_delivery_row(
        &self,
        row: &EventDeliveryAdmissionRow,
        expected_agent_did: &str,
    ) -> anyhow::Result<()> {
        if row.agent_did != expected_agent_did {
            anyhow::bail!(
                "EventDeliveryAdmission {} belongs to agent {}, expected {expected_agent_did}",
                row.doc_id,
                row.agent_did
            );
        }
        let source = self
            .exact_current_ref("EventDeliveryAdmission", &row.doc_id)
            .await?;
        if source.signer_did != row.agent_did {
            anyhow::bail!(
                "EventDeliveryAdmission {} signer {} does not match agent {}",
                row.doc_id,
                source.signer_did,
                row.agent_did
            );
        }
        self.verify_pinned_ref(
            "EventTrigger",
            &row.trigger_doc_id,
            &row.trigger_commit_cid,
            &row.trigger_signer_did,
        )
        .await?;
        self.verify_pinned_ref(
            "EventTriggerActivation",
            &row.activation_doc_id,
            &row.activation_commit_cid,
            &row.activation_signer_did,
        )
        .await?;
        self.verify_pinned_ref(
            &row.source_collection,
            &row.source_doc_id,
            &row.source_commit_cid,
            &row.source_signer_did,
        )
        .await
    }

    async fn request_materialized(&self, request_id: &str) -> anyhow::Result<bool> {
        let response = execute_with_identity(
            &self.node,
            format!(
                r#"{{ AgentRequest(filter: {{ request_id: {{ _eq: "{}" }} }}) {{ _docID request_id }} }}"#,
                crate::graphql::escape_graphql_string(request_id),
            ),
            self.query_identity()?,
        )
        .await;
        if response.has_errors() {
            anyhow::bail!("loading deterministic AgentRequest: {:?}", response.errors);
        }
        let rows = response
            .data
            .as_ref()
            .and_then(|data| data.get("AgentRequest"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| anyhow::anyhow!("AgentRequest query returned no rows"))?;
        match rows.as_slice() {
            [] => Ok(false),
            [_] => Ok(true),
            rows => anyhow::bail!(
                "deterministic request id {request_id} has {} visible logical twins",
                rows.len()
            ),
        }
    }

    async fn admit_delivery(
        &self,
        trigger: &crate::runtime_snapshot::ResolvedEventTrigger,
        activation: &LoadedActivation,
        source: &crate::SignedDocumentVersionRef,
        agent_did: &str,
        allow_create: bool,
    ) -> anyhow::Result<Option<DeliveryWork>> {
        let source_doc_id = source.version.doc_id.as_str();
        if activation
            .manifest
            .sources
            .iter()
            .any(|source| source.doc_id == source_doc_id)
        {
            return Ok(None);
        }
        let candidates = self
            .load_delivery_candidates(
                &activation.row.trigger_doc_id,
                &trigger.source_collection,
                source_doc_id,
                &trigger.event_kind,
            )
            .await?;
        match candidates.as_slice() {
            [row] => {
                self.verify_delivery_row(row, agent_did).await?;
                if self.request_materialized(&row.request_id).await? {
                    return Ok(None);
                }
                let current_trigger = self.load_trigger_ref(trigger).await?;
                if current_trigger.version.composite_commit_cid != row.trigger_commit_cid {
                    anyhow::bail!(
                        "pending EventDeliveryAdmission {} pins an older trigger version; refusing to materialize it with changed configuration",
                        row.doc_id
                    );
                }
                return Ok(Some(DeliveryWork {
                    request_id: row.request_id.clone(),
                    source: crate::SignedDocumentVersionRef::new(
                        crate::DocumentVersionRef::new(&row.source_doc_id, &row.source_commit_cid),
                        &row.source_signer_did,
                    ),
                }));
            }
            [] => {}
            rows => anyhow::bail!(
                "EventDeliveryAdmission logical twins for trigger {} source {}: {} visible rows",
                trigger.trigger_id,
                source_doc_id,
                rows.len()
            ),
        }

        if !allow_create {
            return Ok(None);
        }

        let trigger_source = self.load_trigger_ref(trigger).await?;
        if trigger_source.version.doc_id != activation.row.trigger_doc_id {
            anyhow::bail!(
                "EventTrigger {} physical document changed after activation",
                trigger.trigger_id
            );
        }
        let current_source = self
            .exact_current_ref(&trigger.source_collection, source_doc_id)
            .await?;
        if &current_source != source {
            anyhow::bail!(
                "{} {} changed before durable event admission",
                trigger.source_collection,
                source_doc_id
            );
        }
        let delivery_key = Self::delivery_key(
            &trigger_source.version.doc_id,
            &trigger.source_collection,
            source_doc_id,
            &trigger.event_kind,
        );
        let request_id = format!("event-delivery:{delivery_key}");
        let created_at = chrono::Utc::now().to_rfc3339();
        let mutation = format!(
            r#"mutation {{ create_EventDeliveryAdmission(input: {{
                delivery_key: "{}"
                request_id: "{}"
                agent_did: "{}"
                trigger_id: "{}"
                trigger_doc_id: "{}"
                trigger_commit_cid: "{}"
                trigger_signer_did: "{}"
                activation_doc_id: "{}"
                activation_commit_cid: "{}"
                activation_signer_did: "{}"
                source_collection: "{}"
                source_doc_id: "{}"
                source_commit_cid: "{}"
                source_signer_did: "{}"
                event_kind: "{}"
                created_at: "{}"
            }}) {{ _docID }} }}"#,
            crate::graphql::escape_graphql_string(&delivery_key),
            crate::graphql::escape_graphql_string(&request_id),
            crate::graphql::escape_graphql_string(agent_did),
            crate::graphql::escape_graphql_string(&trigger.trigger_id),
            crate::graphql::escape_graphql_string(&trigger_source.version.doc_id),
            crate::graphql::escape_graphql_string(&trigger_source.version.composite_commit_cid),
            crate::graphql::escape_graphql_string(&trigger_source.signer_did),
            crate::graphql::escape_graphql_string(&activation.source.version.doc_id),
            crate::graphql::escape_graphql_string(&activation.source.version.composite_commit_cid,),
            crate::graphql::escape_graphql_string(&activation.source.signer_did),
            crate::graphql::escape_graphql_string(&trigger.source_collection),
            crate::graphql::escape_graphql_string(source_doc_id),
            crate::graphql::escape_graphql_string(&source.version.composite_commit_cid),
            crate::graphql::escape_graphql_string(&source.signer_did),
            crate::graphql::escape_graphql_string(&trigger.event_kind),
            crate::graphql::escape_graphql_string(&created_at),
        );
        let response = execute_with_identity(&self.node, mutation, self.query_identity()?).await;
        if response.has_errors() {
            anyhow::bail!("creating EventDeliveryAdmission: {:?}", response.errors);
        }
        let candidates = self
            .load_delivery_candidates(
                &trigger_source.version.doc_id,
                &trigger.source_collection,
                source_doc_id,
                &trigger.event_kind,
            )
            .await?;
        match candidates.as_slice() {
            [row]
                if row.delivery_key == delivery_key
                    && row.request_id == request_id
                    && row.trigger_commit_cid == trigger_source.version.composite_commit_cid
                    && row.source_commit_cid == source.version.composite_commit_cid =>
            {
                self.verify_delivery_row(row, agent_did).await?;
                Ok(Some(DeliveryWork {
                    request_id,
                    source: source.clone(),
                }))
            }
            rows => anyhow::bail!(
                "EventDeliveryAdmission create-and-compare observed {} conflicting rows",
                rows.len()
            ),
        }
    }

    async fn resolve_collection_name(&mut self, collection_id: &str) -> Option<String> {
        if let Some(name) = self.collection_id_to_name.get(collection_id) {
            return Some(name.clone());
        }

        let names = match self.node.list_collections() {
            Ok(names) => names,
            Err(e) => {
                tracing::warn!(
                    collection_id = %collection_id,
                    error = %e,
                    "event source failed to list collections; dropping event",
                );
                return None;
            }
        };

        for name in names {
            let def = match self.node.get_collection(&name) {
                Ok(Some(def)) => def,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(
                        name = %name,
                        error = %e,
                        "event source failed to fetch collection definition while resolving id",
                    );
                    continue;
                }
            };
            self.collection_id_to_name
                .insert(def.collection_id.clone(), def.name.clone());
        }

        self.collection_id_to_name.get(collection_id).cloned()
    }

    /// Run the trigger's `filter` against the source doc, narrowed by
    /// `_docID`, via a `limit: 1` probe. Returns `Ok(true)` when the doc
    /// matches (so the fire should proceed), `Ok(false)` when it doesn't
    /// (so the dispatch loop should skip), and `Err` when the probe query
    /// itself errored — the caller treats errors as "skip this fire" so a
    /// transient GraphQL failure doesn't brick the source.
    ///
    /// Trust boundary, one defense per interpolation position:
    /// `source_collection` is validated as a collection identifier, the
    /// `_docID` from the event payload is escaped, and `trigger.filter` —
    /// spliced in whole as an object fragment, where escaping would break
    /// the syntax — is checked as a balanced filter object. A break-out has
    /// to close an `]`, `}` or `)` it never opened, which is what that
    /// check rejects.
    ///
    /// Note the CLI apply probe is not a substitute: it executes the filter
    /// rather than validating it, in a different embedding than this one.
    async fn probe_filter(
        &self,
        source: &crate::SignedDocumentVersionRef,
        trigger: &crate::runtime_snapshot::ResolvedEventTrigger,
    ) -> anyhow::Result<bool> {
        crate::graphql::validate_collection_identifier(&trigger.source_collection)?;
        let user_filter = trigger
            .filter
            .as_deref()
            .map(str::trim)
            .filter(|f| !f.is_empty());
        if let Some(filter) = user_filter {
            crate::graphql::validate_graphql_filter_fragment(filter)?;
        }
        let filter_literal = match user_filter {
            Some(f) => format!(
                r#"{{ _docID: {{ _eq: "{id}" }}, _and: [ {user_filter} ] }}"#,
                id = crate::graphql::escape_graphql_string(&source.version.doc_id),
                user_filter = f,
            ),
            None => format!(
                r#"{{ _docID: {{ _eq: "{id}" }} }}"#,
                id = crate::graphql::escape_graphql_string(&source.version.doc_id),
            ),
        };
        let query = format!(
            r#"query {{
                {collection}(cid: ["{cid}"], filter: {filter_literal}, limit: 1) {{
                    _docID
                }}
            }}"#,
            collection = trigger.source_collection,
            cid = crate::graphql::escape_graphql_string(&source.version.composite_commit_cid,),
            filter_literal = filter_literal,
        );
        let response = execute_with_identity(&self.node, query, self.query_identity()?).await;
        if response.has_errors() {
            anyhow::bail!("filter probe errors: {:?}", response.errors);
        }
        let rows = response
            .data
            .as_ref()
            .and_then(|d| d.get(&trigger.source_collection))
            .and_then(serde_json::Value::as_array);
        Ok(rows.is_some_and(|rs| !rs.is_empty()))
    }

    async fn fetch_source_doc(
        &self,
        collection: &str,
        source: &crate::SignedDocumentVersionRef,
    ) -> anyhow::Result<serde_json::Value> {
        // `fields_for` validates this same binding and `?`-propagates before
        // the query below is built, so it is the one gate for both sites.
        let fields = self
            .source_schema_cache
            .fields_for(collection, &self.node, self.query_identity()?)
            .await?;
        let projection = fields.join("\n                    ");
        let query = format!(
            r#"query {{
                {collection}(cid: ["{cid}"], limit: 1) {{
                    _docID
                    {projection}
                }}
            }}"#,
            collection = collection,
            cid = crate::graphql::escape_graphql_string(&source.version.composite_commit_cid,),
            projection = projection,
        );
        let response = execute_with_identity(&self.node, query, self.query_identity()?).await;
        if response.has_errors() {
            anyhow::bail!("fetch source doc errors: {:?}", response.errors);
        }
        let Some(rows) = response
            .data
            .as_ref()
            .and_then(|d| d.get(collection))
            .and_then(serde_json::Value::as_array)
        else {
            anyhow::bail!(
                "source doc {} not found in {} (no rows in response)",
                source.version.doc_id,
                collection
            );
        };
        let Some(row) = rows.first() else {
            anyhow::bail!(
                "source doc {} not found in {} (empty rows)",
                source.version.doc_id,
                collection
            );
        };
        if row.get("_docID").and_then(serde_json::Value::as_str)
            != Some(source.version.doc_id.as_str())
        {
            anyhow::bail!(
                "source CID {} reconstructed a different _docID",
                source.version.composite_commit_cid
            );
        }
        Ok(row.clone())
    }

    /// Build a `FireIntent` for every active `EventTrigger` whose
    /// `source_collection` matches `collection_name` AND `event_kind` matches
    /// `kind`. Each candidate's operator-authored filter is probed against
    /// `source_doc_id`; candidates that miss the filter or whose probe errors
    /// are skipped (those failures are isolated to the one candidate — they
    /// must not prevent the other matching triggers from firing). A
    /// successful candidate is hydrated via `fetch_source_doc` and wrapped in
    /// a `FireIntent` with a bookkeeping `on_result` callback identical to
    /// the single-trigger path.
    ///
    /// Candidates are ordered by `trigger_id` for determinism so tests and
    /// dispatch order are stable across ticks.
    ///
    /// Replaces the former `first_matching_trigger` helper, which silently
    /// dropped all but one matching trigger per event (and, worse, dropped
    /// the whole event when that one trigger's filter missed).
    async fn build_intents_for_all_matching(
        &self,
        snapshot: &ActiveRuntimeSnapshot,
        collection_name: &str,
        source_doc_id: &str,
        kind: &str,
    ) -> Vec<FireIntent> {
        let mut candidates: Vec<crate::runtime_snapshot::ResolvedEventTrigger> = snapshot
            .active_event_triggers()
            .values()
            .filter(|t| t.source_collection == collection_name && t.event_kind == kind)
            .cloned()
            .collect();
        candidates.sort_by(|a, b| a.trigger_id.cmp(&b.trigger_id));

        let mut intents = Vec::with_capacity(candidates.len());
        let author_did = self.node.node_identity_did().unwrap_or_default().to_owned();
        for trigger in candidates {
            let source = match self
                .exact_current_ref(&trigger.source_collection, source_doc_id)
                .await
            {
                Ok(source) => source,
                Err(err) => {
                    tracing::warn!(
                        trigger_id = %trigger.trigger_id,
                        source_collection = %collection_name,
                        %source_doc_id,
                        %err,
                        "event source: exact source verification failed",
                    );
                    continue;
                }
            };
            let activation = match self.ensure_activation(&trigger, &author_did).await {
                Ok(activation) => activation,
                Err(err) => {
                    tracing::warn!(
                        trigger_id = %trigger.trigger_id,
                        source_collection = %collection_name,
                        %source_doc_id,
                        %err,
                        "event source: durable activation is unavailable",
                    );
                    continue;
                }
            };
            let current_matches = match self.probe_filter(&source, &trigger).await {
                Ok(matches) => matches,
                Err(err) => {
                    tracing::warn!(
                        trigger_id = %trigger.trigger_id,
                        source_collection = %collection_name,
                        %source_doc_id,
                        %err,
                        "event source: current source filter probe failed",
                    );
                    false
                }
            };
            let work = match self
                .admit_delivery(&trigger, &activation, &source, &author_did, current_matches)
                .await
            {
                Ok(Some(work)) => work,
                Ok(None) => continue,
                Err(err) => {
                    tracing::warn!(
                        trigger_id = %trigger.trigger_id,
                        source_collection = %collection_name,
                        %source_doc_id,
                        %err,
                        "event source: durable delivery admission rejected",
                    );
                    continue;
                }
            };

            match self.probe_filter(&work.source, &trigger).await {
                Ok(true) => {}
                Ok(false) => continue,
                Err(err) => {
                    tracing::warn!(
                        trigger_id = %trigger.trigger_id,
                        source_collection = %collection_name,
                        %source_doc_id,
                        %err,
                        "event source: exact admitted source filter probe failed",
                    );
                    continue;
                }
            }

            let doc_vars = match self
                .fetch_source_doc(&trigger.source_collection, &work.source)
                .await
            {
                Ok(value) => Some(value),
                Err(err) => {
                    tracing::warn!(
                        trigger_id = %trigger.trigger_id,
                        source_collection = %collection_name,
                        %source_doc_id,
                        %err,
                        "event source: source-doc fetch failed; skipping this trigger",
                    );
                    continue;
                }
            };

            let fired_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
            let event_vars = serde_json::json!({
                "fired_at": fired_at,
                "trigger_id": trigger.trigger_id,
                "trigger_kind": TriggerKind::Event.as_str(),
                "source_collection": collection_name,
                "source_doc_id": source_doc_id,
            });

            tracing::info!(
                trigger_id = %trigger.trigger_id,
                source_collection = %collection_name,
                %source_doc_id,
                "event source matched event to trigger; emitting fire intent",
            );

            let trigger_id_for_callback = trigger.trigger_id.clone();
            let source_doc_id_for_callback = source_doc_id.to_string();

            intents.push(FireIntent {
                trigger_id: Some(trigger.trigger_id.clone()),
                trigger_kind: TriggerKind::Event,
                task: trigger.task.clone(),
                concurrency: trigger.concurrency,
                event_vars,
                doc_vars,
                args_vars: None,
                pre_materialized_request_id: None,
                materialization_request_id: Some(work.request_id),
                on_result: Box::new(move |result| {
                    tracing::info!(
                        trigger_id = %trigger_id_for_callback,
                        source_doc_id = %source_doc_id_for_callback,
                        result = ?result,
                        "durably admitted event trigger dispatch completed",
                    );
                }),
            });
        }
        intents
    }

    fn take_first_and_queue_rest(&self, mut intents: Vec<FireIntent>) -> Option<FireIntent> {
        if intents.is_empty() {
            return None;
        }
        let first = intents.remove(0);
        let mut queue = self
            .pending_intents
            .lock()
            .expect("pending_intents mutex poisoned");
        for intent in intents {
            queue.push_back(intent);
        }
        Some(first)
    }

    async fn rescan_created_docs(&mut self) -> Option<FireIntent> {
        let mut collections: Vec<String> = self.desired_collections.iter().cloned().collect();
        collections.sort();
        let snapshot = self.snapshot_rx.borrow().clone();

        for collection in collections {
            let doc_ids = match self.load_doc_ids_for_collection(&collection).await {
                Ok(doc_ids) => doc_ids,
                Err(err) => {
                    tracing::warn!(
                        source_collection = %collection,
                        %err,
                        "event source periodic rescan failed for collection",
                    );
                    continue;
                }
            };

            for doc_id in doc_ids {
                let intents = self
                    .build_intents_for_all_matching(
                        snapshot.as_ref(),
                        &collection,
                        &doc_id,
                        "created",
                    )
                    .await;
                if let Some(first) = self.take_first_and_queue_rest(intents) {
                    tracing::info!(
                        source_collection = %collection,
                        source_doc_id = %doc_id,
                        "event source periodic rescan emitted fire intent",
                    );
                    return Some(first);
                }
            }
        }
        None
    }
}

impl EventDeliveryRuntimeContract for EventSource {
    const EVENT_DELIVERY_CONTRACT: EventDeliverySourceContract = EventDeliverySourceContract {
        name: "EventSource",
        dedupe_policy: "monotone_once",
        rescan_bounded_by: 1,
        deviation: None,
    };
}

impl TriggerSource for EventSource {
    fn next_fire(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<FireIntent>> + Send + '_>> {
        Box::pin(async move {
            // Outer loop: reconcile-on-generation-bump, then race subscription
            // vs. snapshot-change vs. cancel. `None` here means "source is
            // permanently done, drop it" — an idle tick or an unmatched event
            // must not exit. Return `None` only on cancel or subscription
            // channel closure; keep looping otherwise so the engine's outer
            // driver doesn't teardown the source on the first miss.
            loop {
                if let Some(intent) = self
                    .pending_intents
                    .lock()
                    .expect("pending_intents mutex poisoned")
                    .pop_front()
                {
                    return Some(intent);
                }

                let snapshot = self.snapshot_rx.borrow().clone();
                if snapshot.generation > self.reconciled_generation {
                    self.reconcile_subscriptions(snapshot.as_ref()).await;
                }

                if self.subscription.is_none() || self.desired_collections.is_empty() {
                    tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => return None,
                        res = self.snapshot_rx.changed() => {
                            if res.is_err() {
                                return None;
                            }
                            continue;
                        }
                    }
                }

                // Step 3: race the subscription against snapshot changes and
                // cancel. Subscription is guaranteed Some here by the check
                // above, so we can take a &mut borrow for the recv poll.
                let mut message = None;
                let mut dropped = 0;
                let mut subscription_closed = false;
                let rescan_due = {
                    let subscription = self
                        .subscription
                        .as_mut()
                        .expect("subscription is Some when desired_collections is non-empty");
                    let rescan_due = tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => return None,
                        _ = self.rescan_tick.tick() => true,
                        res = self.snapshot_rx.changed() => {
                            if res.is_err() {
                                return None;
                            }
                            continue;
                        }
                        msg = subscription.recv() => {
                            match msg {
                                Some(m) => {
                                    message = Some(m);
                                    false
                                }
                                None => {
                                    subscription_closed = true;
                                    false
                                }
                            }
                        }
                    };
                    if !rescan_due {
                        dropped = subscription.check_and_reset_dropped();
                    }
                    rescan_due
                };
                if subscription_closed {
                    self.subscription = None;
                    tracing::warn!(
                        "event source subscription channel closed; reopening after durable rescan",
                    );
                    if let Some(intent) = self.rescan_created_docs().await {
                        return Some(intent);
                    }
                    tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => return None,
                        _ = tokio::time::sleep(UPDATE_SUBSCRIPTION_REOPEN_DELAY) => {}
                    }
                    if !self.desired_collections.is_empty() {
                        self.subscription = Some(self.subscription_source.subscribe_updates());
                        tracing::info!(
                            collections = self.desired_collections.len(),
                            "event source reopened global Update subscription",
                        );
                    }
                    continue;
                }
                if rescan_due {
                    if let Some(intent) = self.rescan_created_docs().await {
                        return Some(intent);
                    }
                    continue;
                }
                let message = message.expect("subscription recv branch sets message");

                if dropped > 0 {
                    // Dropped events are a correctness hazard. The periodic
                    // rescan closes the gap for created docs, and this log
                    // keeps the lossy event visible operationally.
                    tracing::warn!(
                        dropped = dropped,
                        "event source dropped messages; periodic rescan will recover created docs",
                    );
                }

                let Some(update) = message.as_update() else {
                    continue;
                };

                let collection_id = update.collection_id.clone();
                let doc_id = update.doc_id.clone();
                let Some(collection_name) = self.resolve_collection_name(&collection_id).await
                else {
                    tracing::trace!(
                        collection_id = %collection_id,
                        doc_id = %doc_id,
                        "event source could not resolve collection_id to name; skipping event",
                    );
                    continue;
                };

                if !self.desired_collections.contains(&collection_name) {
                    continue;
                }

                let snapshot = self.snapshot_rx.borrow().clone();
                let event_kind = "created";
                let intents = self
                    .build_intents_for_all_matching(
                        snapshot.as_ref(),
                        &collection_name,
                        &doc_id,
                        event_kind,
                    )
                    .await;
                if intents.is_empty() {
                    continue;
                }

                return self.take_first_and_queue_rest(intents);
            }
        })
    }
}
