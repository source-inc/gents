use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use defra_node::EmbeddedNode;
use serde_json::Value;
use tokio::time::MissedTickBehavior;

use crate::graphql::escape_graphql_string;
use crate::UpdateSubscriptionSource;

use super::claim::invocation_is_claimable;
use super::documents::{
    create_pending_invocation, idempotency_key, list_enabled_bindings, strip_secret_fields,
    validate_callback_binding, CallbackBindingDoc, CallbackInvocationDoc,
};
use super::run::run_owned_invocation;
use super::{CallbackEngine, LIFECYCLE_PENDING};

const SEEN_DOCS_SEED_LIMIT: usize = 10_000;
const CALLBACK_RESCAN_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
pub(super) struct SourceSchemaCache {
    by_collection: HashMap<String, Vec<String>>,
}

impl SourceSchemaCache {
    async fn fields_for(&mut self, collection: &str, node: &EmbeddedNode) -> Result<Vec<String>> {
        crate::graphql::validate_collection_identifier(collection)?;
        if let Some(fields) = self.by_collection.get(collection) {
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
        let response = node.execute(&query).await;
        if response.has_errors() {
            anyhow::bail!("introspect {collection} failed: {:?}", response.errors);
        }
        let Some(fields_arr) = response
            .data
            .as_ref()
            .and_then(|data| data.get("__type"))
            .and_then(|ty| ty.get("fields"))
            .and_then(Value::as_array)
        else {
            anyhow::bail!("introspection returned no fields for {collection}");
        };
        let fields: Vec<String> = fields_arr
            .iter()
            .filter_map(|field| {
                field
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .filter(|name| !name.starts_with('_'))
            .filter(|name| !is_defradb_aggregate_field(name))
            .filter(|name| !crate::toolset::is_secret_env_name(name))
            .collect();
        self.by_collection
            .insert(collection.to_string(), fields.clone());
        Ok(fields)
    }
}

fn is_defradb_aggregate_field(name: &str) -> bool {
    matches!(
        name,
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP" | "SIMILARITY" | "BM25"
    )
}

pub(super) fn rescan_tick() -> tokio::time::Interval {
    let mut tick = tokio::time::interval(CALLBACK_RESCAN_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick
}

impl CallbackEngine {
    pub(super) fn new(
        node: Arc<EmbeddedNode>,
        local_deployment_id: String,
        ceiling: Option<std::path::PathBuf>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self::with_subscription_source(node.clone(), node, local_deployment_id, ceiling, cancel)
    }

    pub(super) fn with_subscription_source(
        subs: Arc<dyn UpdateSubscriptionSource>,
        node: Arc<EmbeddedNode>,
        local_deployment_id: String,
        ceiling: Option<std::path::PathBuf>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            node,
            local_deployment_id,
            ceiling,
            subscription_source: subs,
            subscription: None,
            desired_collections: HashSet::new(),
            seen_docs: HashMap::new(),
            collection_id_to_name: HashMap::new(),
            rescan_tick: rescan_tick(),
            cancel,
        }
    }

    pub(super) async fn reconcile_bindings(&mut self) {
        let bindings = match list_enabled_bindings(self.node.as_ref()).await {
            Ok(bindings) => bindings,
            Err(error) => {
                tracing::warn!(%error, "callback engine failed to load CallbackBinding rows");
                return;
            }
        };
        let mut desired = HashSet::new();
        for binding in &bindings {
            desired.insert(binding.source_collection.clone());
        }
        let added: Vec<String> = desired
            .difference(&self.desired_collections)
            .cloned()
            .collect();
        for collection in &added {
            if let Err(error) = self.seed_seen_docs(collection).await {
                tracing::warn!(
                    source_collection = %collection,
                    %error,
                    "callback engine seed_seen_docs failed; forward-only semantics may be weaker"
                );
            }
        }
        self.desired_collections = desired;
        if self.subscription.is_none() && !self.desired_collections.is_empty() {
            self.subscription = Some(self.subscription_source.subscribe_updates());
        }
    }

    async fn seed_seen_docs(&mut self, collection: &str) -> Result<()> {
        let ids = load_doc_ids(self.node.as_ref(), collection).await?;
        self.seen_docs
            .entry(collection.to_string())
            .or_default()
            .extend(ids);
        Ok(())
    }

    fn has_seen(&self, collection: &str, doc_id: &str) -> bool {
        self.seen_docs
            .get(collection)
            .is_some_and(|docs| docs.contains(doc_id))
    }

    fn mark_seen(&mut self, collection: &str, doc_id: &str) {
        self.seen_docs
            .entry(collection.to_string())
            .or_default()
            .insert(doc_id.to_string());
    }

    pub(super) async fn rescan_created_docs(&mut self) {
        let collections: Vec<String> = self.desired_collections.iter().cloned().collect();
        for collection in collections {
            let ids = match load_doc_ids(self.node.as_ref(), &collection).await {
                Ok(ids) => ids,
                Err(error) => {
                    tracing::warn!(
                        source_collection = %collection,
                        %error,
                        "callback engine rescan query failed"
                    );
                    continue;
                }
            };
            for doc_id in ids {
                if self.cancel.is_cancelled() {
                    return;
                }
                if self.has_seen(&collection, &doc_id) {
                    continue;
                }
                self.handle_created_doc(&collection, &doc_id).await;
            }
        }
    }

    pub(super) async fn handle_update(&mut self, collection_id: &str, doc_id: &str) {
        let Some(collection) = self.resolve_collection_name(collection_id).await else {
            return;
        };
        if !self.desired_collections.contains(&collection) {
            return;
        }
        if self.has_seen(&collection, doc_id) {
            return;
        }
        self.handle_created_doc(&collection, doc_id).await;
    }

    pub(super) async fn handle_created_doc(&mut self, collection: &str, doc_id: &str) {
        let bindings = match list_enabled_bindings(self.node.as_ref()).await {
            Ok(bindings) => bindings,
            Err(error) => {
                tracing::warn!(%error, "callback engine reload bindings failed");
                return;
            }
        };
        let mut matched = false;
        let mut all_settled = true;
        for binding in bindings.into_iter().filter(|binding| {
            binding.source_collection == collection && binding.event_kind == "created"
        }) {
            match self
                .materialize_for_binding(&binding, collection, doc_id)
                .await
            {
                Ok(true) => matched = true,
                Ok(false) => {}
                Err(error) => {
                    all_settled = false;
                    tracing::warn!(
                        binding_id = %binding.binding_id,
                        source_collection = %collection,
                        source_doc_id = %doc_id,
                        %error,
                        "callback invocation materialize failed"
                    );
                }
            }
        }
        if all_settled || matched {
            self.mark_seen(collection, doc_id);
        }
    }

    async fn materialize_for_binding(
        &mut self,
        binding: &CallbackBindingDoc,
        collection: &str,
        doc_id: &str,
    ) -> Result<bool> {
        if let Err(error) = validate_callback_binding(binding) {
            tracing::warn!(
                binding_id = %binding.binding_id,
                %error,
                "callback binding invalid at scan"
            );
            return Ok(false);
        }
        match probe_filter(
            self.node.as_ref(),
            collection,
            doc_id,
            binding.filter.as_deref(),
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(error) => {
                tracing::warn!(
                    binding_id = %binding.binding_id,
                    %error,
                    "callback filter probe failed; skipping this binding"
                );
                return Ok(false);
            }
        }
        let key = idempotency_key(&binding.binding_id, doc_id, "created");
        let invocation = CallbackInvocationDoc {
            invocation_id: uuid::Uuid::new_v4().to_string(),
            owner_deployment_id: binding.owner_deployment_id.clone(),
            binding_id: binding.binding_id.clone(),
            source_collection: collection.to_string(),
            source_doc_id: doc_id.to_string(),
            source_version: Some("created".to_string()),
            idempotency_key: key,
            lifecycle_state: LIFECYCLE_PENDING.to_string(),
            attempts: Some(0),
            action_plan: None,
            action_journal: None,
            error: None,
            claimed_at: None,
            created_at: Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        };
        let stored = create_pending_invocation(self.node.as_ref(), &invocation).await?;
        if invocation_is_claimable(&self.local_deployment_id, &stored) {
            if let Err(error) = run_owned_invocation(
                self.node.as_ref(),
                &stored,
                binding,
                self.ceiling.as_deref(),
            )
            .await
            {
                tracing::warn!(
                    invocation_id = %stored.invocation_id,
                    %error,
                    "callback invocation run failed"
                );
            }
        }
        Ok(true)
    }

    async fn resolve_collection_name(&mut self, collection_id: &str) -> Option<String> {
        if let Some(name) = self.collection_id_to_name.get(collection_id) {
            return Some(name.clone());
        }
        let names = match self.node.list_collections() {
            Ok(names) => names,
            Err(error) => {
                tracing::warn!(%error, "callback engine failed to list collections");
                return None;
            }
        };
        for name in names {
            let def = match self.node.get_collection(&name) {
                Ok(Some(def)) => def,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(name = %name, %error, "callback engine collection lookup failed");
                    continue;
                }
            };
            self.collection_id_to_name
                .insert(def.collection_id.clone(), def.name.clone());
        }
        self.collection_id_to_name.get(collection_id).cloned()
    }
}

async fn load_doc_ids(node: &EmbeddedNode, collection: &str) -> Result<Vec<String>> {
    crate::graphql::validate_collection_identifier(collection)?;
    let query = format!(
        r#"query {{ {collection}(limit: {limit}) {{ _docID }} }}"#,
        limit = SEEN_DOCS_SEED_LIMIT,
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!(
            "callback source scan for {collection} failed: {:?}",
            response.errors
        );
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get(collection))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| {
            row.get("_docID")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect())
}

pub(super) async fn probe_filter(
    node: &EmbeddedNode,
    collection: &str,
    source_doc_id: &str,
    filter: Option<&str>,
) -> Result<bool> {
    crate::graphql::validate_collection_identifier(collection)?;
    let user_filter = filter.map(str::trim).filter(|value| !value.is_empty());
    if let Some(filter) = user_filter {
        crate::graphql::validate_graphql_filter_fragment(filter)?;
    }
    let filter_literal = match user_filter {
        Some(user_filter) => format!(
            r#"{{ _docID: {{ _eq: "{id}" }}, _and: [ {user_filter} ] }}"#,
            id = escape_graphql_string(source_doc_id),
        ),
        None => format!(
            r#"{{ _docID: {{ _eq: "{id}" }} }}"#,
            id = escape_graphql_string(source_doc_id),
        ),
    };
    let query = format!(
        r#"query {{
            {collection}(filter: {filter_literal}, limit: 1) {{
                _docID
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!("callback filter probe errors: {:?}", response.errors);
    }
    let rows = response
        .data
        .as_ref()
        .and_then(|data| data.get(collection))
        .and_then(Value::as_array);
    Ok(rows.is_some_and(|rows| !rows.is_empty()))
}

pub(super) async fn fetch_source_for_invocation(
    node: &EmbeddedNode,
    binding: &CallbackBindingDoc,
    invocation: &CallbackInvocationDoc,
) -> Result<Value> {
    let mut cache = SourceSchemaCache::default();
    fetch_source_doc(
        node,
        &mut cache,
        &invocation.source_collection,
        &invocation.source_doc_id,
        binding,
    )
    .await
}

pub(super) async fn fetch_source_doc(
    node: &EmbeddedNode,
    cache: &mut SourceSchemaCache,
    collection: &str,
    source_doc_id: &str,
    binding: &CallbackBindingDoc,
) -> Result<Value> {
    let projected = binding.projected_fields()?;
    let fields = if projected.is_empty() {
        cache.fields_for(collection, node).await?
    } else {
        projected
    };
    let projection = fields.join("\n                    ");
    let query = format!(
        r#"query {{
            {collection}(filter: {{ _docID: {{ _eq: "{id}" }} }}, limit: 1) {{
                _docID
                {projection}
            }}
        }}"#,
        id = escape_graphql_string(source_doc_id),
    );
    let response = node.execute(&query).await;
    if response.has_errors() {
        anyhow::bail!("fetch callback source doc errors: {:?}", response.errors);
    }
    let Some(row) = response
        .data
        .as_ref()
        .and_then(|data| data.get(collection))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .cloned()
    else {
        anyhow::bail!("source doc {source_doc_id} not found in {collection}");
    };
    Ok(strip_secret_fields(row))
}
