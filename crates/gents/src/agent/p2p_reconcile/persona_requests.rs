//! Server-side persona-request reconciler (directory persona catalog, PR 2 /
//! Task 4).
//!
//! `PersonaConfigRequest` rows are phone-authored asks to create, clone,
//! edit, or disable a persona's backing `AgentBehavior`. This reconciler is
//! the server-side sweep that drives every `pending` row to a terminal
//! `applied`/`rejected` status: it loads the row, builds a
//! [`PersonaCatalogView`] straight from the source collections
//! (`InferenceBackend`, `WorkspaceRoot`, `InferenceProfile`, this agent's own
//! `AgentBehavior` rows) — never from the `AgentDirectoryEntry` projection,
//! so this reconciler never depends on the directory sweep having already
//! run — runs [`decide_persona_request`], and on `Admit` calls
//! [`apply_persona_request`] before writing the outcome back onto the row.
//! Admission and materialization are the exact same core the agent's own
//! self-config tool and the `gents` CLI call, so the three write channels
//! can never drift.
//!
//! TRUST BASIS (v1 decision) — a `PersonaConfigRequest` row reaches this
//! node only two ways: replicated in over the `machine` pairing template
//! (paired devices only — the pushback rule in `p2p_reconcile::templates`
//! scopes replication to rows whose `requester_did` matches the peer, and
//! `PairingBearerClaim` admission is what got that peer paired in the first
//! place) or written locally. This reconciler does NOT re-check requester
//! membership against the network's membership ledger: it mirrors
//! `PairingBearerClaim`'s own note that an unsigned/unauthorized row "grants
//! nothing by itself" — authority here lives entirely in
//! [`decide_persona_request`] validating against the published catalog, not
//! in re-verifying who sent the row. Paired devices are trusted in v1; a
//! revoked-membership check can be layered in later without changing this
//! module's shape. One boundary IS enforced regardless: the request's
//! `agent_did` must name an enabled local `AgentPrincipal` (the catalog
//! view's `known_agent_dids`, Lean `agentOk`), so a request can never mint
//! orphan config for a phantom or foreign agent.
//!
//! CRASH REPAIR — [`apply_persona_request`] is idempotent: if a prior tick
//! applied the request (writing the `AgentBehavior`/`ToolSelection`) but
//! crashed or errored before this reconciler could write `status: applied`
//! back onto the row, the row is still `pending` and gets re-processed. The
//! re-run's catalog view includes the already-materialized behavior, so
//! `apply_persona_request` recognizes the `sel-{request_key}` selection and
//! returns `repaired: true` instead of minting a duplicate — this tick only
//! needs to (re)write the mark to converge.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use defra_node::{EmbeddedNode, EventName};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::agent::persona_ops::{
    apply_persona_request, decide_persona_request, BehaviorRef, PersonaCatalogView, PersonaOp,
    PersonaRequestDoc, PersonaVerdict,
};
use crate::graphql::escape_graphql_string;

use super::graphql_helpers::{ensure_no_errors, rows};

/// One reconcile sweep's outcome, split the same way
/// [`crate::agent::p2p_reconcile::bearer_claim::BearerClaimTickOutcome`]
/// splits admitted-fresh from repaired: `applied` is a first-time apply,
/// `repaired` is a re-processed row whose apply had already materialized
/// (crash between apply and mark), and `rejected` is admission failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersonaTickOutcome {
    pub applied: BTreeSet<String>,
    pub repaired: BTreeSet<String>,
    pub rejected: BTreeSet<String>,
}

#[async_trait]
pub trait PersonaRequestStore: Send + Sync {
    /// Rows with `status == "pending"`; terminal rows are filtered by the
    /// store's own query (or fixture), not by the tick.
    async fn load_pending_requests(&self) -> Result<Vec<PersonaRequestDoc>>;
    /// The published catalog plus `agent_did`'s own `AgentBehavior` rows,
    /// loaded straight from source collections — never from the directory
    /// projection.
    async fn load_catalog_view(&self, agent_did: &str) -> Result<PersonaCatalogView>;
    async fn mark_applied(&self, request_key: &str, behavior_id: &str) -> Result<()>;
    async fn mark_rejected(&self, request_key: &str, detail: &str) -> Result<()>;
}

/// One sweep: for each pending row, build the catalog view for its
/// `agent_did`, decide admission, and either apply + mark applied or mark
/// rejected. Mirrors `reconcile_directory_tick`'s per-entry error tolerance:
/// one row's failure is warned-and-skipped so it never wedges the rest of
/// the sweep, and only the first error is returned once every row has had a
/// chance to converge.
pub async fn reconcile_persona_tick(
    store: &dyn PersonaRequestStore,
    node: &Arc<EmbeddedNode>,
) -> Result<PersonaTickOutcome> {
    let pending = store
        .load_pending_requests()
        .await
        .context("load pending persona requests")?;

    let mut outcome = PersonaTickOutcome::default();
    let mut first_error: Option<anyhow::Error> = None;
    for doc in pending {
        let request_key = doc.request_key.clone();
        if let Err(error) = process_one_request(store, node, &doc, &mut outcome).await {
            tracing::warn!(
                request_key = %request_key,
                error = %error,
                "persona request reconcile failed; continuing sweep"
            );
            if first_error.is_none() {
                first_error =
                    Some(error.context(format!("reconcile persona request {request_key}")));
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(outcome)
}

async fn process_one_request(
    store: &dyn PersonaRequestStore,
    node: &Arc<EmbeddedNode>,
    doc: &PersonaRequestDoc,
    outcome: &mut PersonaTickOutcome,
) -> Result<()> {
    let catalog = store
        .load_catalog_view(&doc.agent_did)
        .await
        .with_context(|| format!("load persona catalog view for agent {}", doc.agent_did))?;

    match decide_persona_request(doc, &catalog) {
        PersonaVerdict::Admit => {
            let apply_outcome = apply_persona_request(node, doc, &catalog)
                .await
                .context("apply admitted persona request")?;
            store
                .mark_applied(&doc.request_key, &apply_outcome.behavior_id)
                .await
                .context("mark persona request applied")?;
            if apply_outcome.repaired {
                outcome.repaired.insert(doc.request_key.clone());
            } else {
                outcome.applied.insert(doc.request_key.clone());
            }
        }
        PersonaVerdict::Reject(detail) => {
            store
                .mark_rejected(&doc.request_key, &detail)
                .await
                .context("mark persona request rejected")?;
            outcome.rejected.insert(doc.request_key.clone());
        }
    }
    Ok(())
}

pub async fn run_persona_request_reconciler(
    node: Arc<EmbeddedNode>,
    ceiling_root: Option<std::path::PathBuf>,
    cancel: CancellationToken,
) -> Result<()> {
    let store = GraphqlPersonaRequestStore::with_ceiling(node.clone(), ceiling_root);
    let mut subscription = node.subscribe(&[EventName::Update]);
    let mut interval = tokio::time::interval(super::intervals::sweep_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    sweep_persona_requests(&store, &node).await;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = interval.tick() => sweep_persona_requests(&store, &node).await,
            message = subscription.recv() => {
                if message.is_none() {
                    tracing::warn!("persona-request reconciler update subscription closed; continuing with periodic sweeps");
                    continue;
                }
                let dropped = subscription.check_and_reset_dropped();
                if dropped > 0 {
                    tracing::warn!(dropped, "persona-request reconciler update subscription dropped messages");
                }
                sweep_persona_requests(&store, &node).await;
            }
        }
    }
}

async fn sweep_persona_requests(store: &GraphqlPersonaRequestStore, node: &Arc<EmbeddedNode>) {
    match reconcile_persona_tick(store, node).await {
        Ok(outcome) => {
            if !outcome.applied.is_empty()
                || !outcome.repaired.is_empty()
                || !outcome.rejected.is_empty()
            {
                tracing::info!(
                    applied = ?outcome.applied,
                    repaired = ?outcome.repaired,
                    rejected = ?outcome.rejected,
                    "reconciled persona config requests"
                );
            }
        }
        Err(error) => {
            tracing::warn!(error = %error, "persona-request reconcile sweep failed")
        }
    }
}

pub struct GraphqlPersonaRequestStore {
    node: Arc<EmbeddedNode>,
    /// Operator tool-root ceiling (`--tool-root`); see
    /// `directory_projection::filter_roots_to_ceiling`. `None` when the
    /// caller has no ceiling in scope (e.g. the self-config tool's read-only
    /// `list`): admission always runs against the reconciler's own
    /// ceiling-aware store, so a `None` here can never admit an unusable
    /// root.
    ceiling_root: Option<std::path::PathBuf>,
}

impl GraphqlPersonaRequestStore {
    pub fn new(node: Arc<EmbeddedNode>) -> Self {
        Self {
            node,
            ceiling_root: None,
        }
    }

    pub fn with_ceiling(node: Arc<EmbeddedNode>, ceiling_root: Option<std::path::PathBuf>) -> Self {
        Self { node, ceiling_root }
    }
}

#[async_trait]
impl PersonaRequestStore for GraphqlPersonaRequestStore {
    async fn load_pending_requests(&self) -> Result<Vec<PersonaRequestDoc>> {
        let query = r#"{
            PersonaConfigRequest(filter: { status: { _eq: "pending" } }) {
                request_key
                requester_did
                agent_did
                op
                behavior_id
                clone_from
                persona_name
                backend_model
                root
                preset
                profile_id
                created_at
                status
                status_detail
                applied_behavior_id
                processed_at
            }
        }"#;
        let response = self.node.execute(query).await;
        ensure_no_errors(&response, "query PersonaConfigRequest pending rows")?;
        Ok(
            rows::<PersonaRequestRow>(&response, "PersonaConfigRequest")?
                .into_iter()
                .filter_map(persona_request_doc_from_row)
                .collect(),
        )
    }

    async fn load_catalog_view(&self, agent_did: &str) -> Result<PersonaCatalogView> {
        load_catalog_view_from_node(&self.node, agent_did, self.ceiling_root.as_deref()).await
    }

    async fn mark_applied(&self, request_key: &str, behavior_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let mutation = mark_applied_mutation(request_key, behavior_id, &now);
        let response = self.node.execute(&mutation).await;
        ensure_no_errors(&response, "mark PersonaConfigRequest applied")
    }

    async fn mark_rejected(&self, request_key: &str, detail: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let mutation = mark_rejected_mutation(request_key, detail, &now);
        let response = self.node.execute(&mutation).await;
        ensure_no_errors(&response, "mark PersonaConfigRequest rejected")
    }
}

/// Build a [`PersonaCatalogView`] straight from source collections: enabled
/// `AgentPrincipal` DIDs, enabled `InferenceBackend` models
/// (`"backend_id|model_name"`), enabled `WorkspaceRoot` paths,
/// `InferenceProfile` ids, and `agent_did`'s own `AgentBehavior` rows.
/// Deliberately independent of the `AgentDirectoryEntry` projection
/// (`crate::agent::directory_projection`) — coupling admission to that
/// projection's sweep cadence would make this reconciler's correctness
/// depend on another reconciler having already run.
///
/// One multi-root document on purpose (the same shape as the directory
/// projection's `load_source_snapshot`): this loads once per pending row,
/// and per-collection executes would each pay their own parse/plan/
/// transaction overhead.
async fn load_catalog_view_from_node(
    node: &Arc<EmbeddedNode>,
    agent_did: &str,
    ceiling_root: Option<&std::path::Path>,
) -> Result<PersonaCatalogView> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            AgentPrincipal {{
                agent_did
                enabled
            }}
            InferenceBackend {{
                backend_id
                models
                enabled
            }}
            WorkspaceRoot {{
                root_path
                enabled
            }}
            InferenceProfile {{
                profile_id
            }}
            AgentBehavior(filter: {{ agent_did: {{ _eq: "{escaped_agent_did}" }} }}) {{
                behavior_id
                enabled
                tool_selection_id
            }}
        }}"#
    );
    let response = node.execute(&query).await;
    ensure_no_errors(&response, "query persona catalog sources")?;

    let known_agent_dids: BTreeSet<String> =
        rows::<AgentPrincipalCatalogRow>(&response, "AgentPrincipal")?
            .into_iter()
            .filter(|row| row.enabled.unwrap_or(true))
            .filter_map(|row| {
                let did = row.agent_did?.trim().to_string();
                (!did.is_empty()).then_some(did)
            })
            .collect();

    let available_models: BTreeSet<String> =
        rows::<InferenceBackendRow>(&response, "InferenceBackend")?
            .into_iter()
            .filter(|row| row.enabled.unwrap_or(false))
            .flat_map(|row| {
                let backend_id = row.backend_id.unwrap_or_default();
                row.models
                    .unwrap_or_default()
                    .into_iter()
                    .map(move |model| format!("{backend_id}|{model}"))
                    .collect::<Vec<_>>()
            })
            .collect();

    // Ceiling-filtered through the same predicate the directory catalog
    // publishes with, so admission can never admit a root the serve-time
    // guard would refuse (#1051): the `root ∈ allowed_roots` conjunct
    // enforces the operator ceiling for free.
    let allowed_roots: BTreeSet<String> =
        crate::agent::directory_projection::filter_roots_to_ceiling(
            rows::<WorkspaceRootRow>(&response, "WorkspaceRoot")?
                .into_iter()
                .filter(|row| row.enabled.unwrap_or(false))
                .filter_map(|row| row.root_path)
                .collect(),
            ceiling_root,
        )
        .into_iter()
        .collect();

    let available_profile_ids: BTreeSet<String> =
        rows::<InferenceProfileRow>(&response, "InferenceProfile")?
            .into_iter()
            .filter_map(|row| row.profile_id)
            .collect();

    let behaviors: BTreeMap<String, BehaviorRef> =
        rows::<AgentBehaviorCatalogRow>(&response, "AgentBehavior")?
            .into_iter()
            .filter_map(|row| {
                let behavior_id = row.behavior_id?.trim().to_string();
                if behavior_id.is_empty() {
                    return None;
                }
                Some((
                    behavior_id,
                    BehaviorRef {
                        enabled: row.enabled.unwrap_or(true),
                        tool_selection_id: row.tool_selection_id.unwrap_or_default(),
                    },
                ))
            })
            .collect();

    Ok(PersonaCatalogView {
        available_models,
        allowed_roots,
        available_profile_ids,
        known_agent_dids,
        behaviors,
    })
}

fn persona_request_doc_from_row(row: PersonaRequestRow) -> Option<PersonaRequestDoc> {
    let request_key = row.request_key?.trim().to_string();
    if request_key.is_empty() {
        return None;
    }
    let op_raw = row.op.unwrap_or_default().trim().to_string();
    let clone_from = row
        .clone_from
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_string());
    let op = PersonaOp::parse(&op_raw, clone_from);
    Some(PersonaRequestDoc {
        request_key,
        requester_did: row.requester_did.unwrap_or_default(),
        agent_did: row.agent_did.unwrap_or_default().trim().to_string(),
        op_raw,
        op,
        behavior_id: row.behavior_id,
        persona_name: row.persona_name,
        backend_model: row.backend_model,
        root: row.root,
        preset: row.preset,
        profile_id: row.profile_id,
        created_at: row.created_at,
        status: row.status,
        status_detail: row.status_detail,
        applied_behavior_id: row.applied_behavior_id,
        processed_at: row.processed_at,
    })
}

fn mark_applied_mutation(request_key: &str, behavior_id: &str, now: &str) -> String {
    let request_key = escape_graphql_string(request_key);
    let behavior_id = escape_graphql_string(behavior_id);
    let now = escape_graphql_string(now);
    format!(
        r#"mutation {{
            update_PersonaConfigRequest(
                filter: {{ request_key: {{ _eq: "{request_key}" }} }},
                input: {{
                    status: "applied",
                    status_detail: "",
                    applied_behavior_id: "{behavior_id}",
                    processed_at: "{now}"
                }}
            ) {{ _docID }}
        }}"#
    )
}

fn mark_rejected_mutation(request_key: &str, detail: &str, now: &str) -> String {
    let request_key = escape_graphql_string(request_key);
    let detail = escape_graphql_string(detail);
    let now = escape_graphql_string(now);
    format!(
        r#"mutation {{
            update_PersonaConfigRequest(
                filter: {{ request_key: {{ _eq: "{request_key}" }} }},
                input: {{
                    status: "rejected",
                    status_detail: "{detail}",
                    processed_at: "{now}"
                }}
            ) {{ _docID }}
        }}"#
    )
}

#[derive(Deserialize)]
struct PersonaRequestRow {
    request_key: Option<String>,
    #[serde(default)]
    requester_did: Option<String>,
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    behavior_id: Option<String>,
    #[serde(default)]
    clone_from: Option<String>,
    #[serde(default)]
    persona_name: Option<String>,
    #[serde(default)]
    backend_model: Option<String>,
    #[serde(default)]
    root: Option<String>,
    #[serde(default)]
    preset: Option<String>,
    #[serde(default)]
    profile_id: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    status_detail: Option<String>,
    #[serde(default)]
    applied_behavior_id: Option<String>,
    #[serde(default)]
    processed_at: Option<String>,
}

#[derive(Deserialize)]
struct AgentPrincipalCatalogRow {
    #[serde(default)]
    agent_did: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
}

#[derive(Deserialize)]
struct InferenceBackendRow {
    #[serde(default)]
    backend_id: Option<String>,
    #[serde(default)]
    models: Option<Vec<String>>,
    #[serde(default)]
    enabled: Option<bool>,
}

#[derive(Deserialize)]
struct WorkspaceRootRow {
    #[serde(default)]
    root_path: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
}

#[derive(Deserialize)]
struct InferenceProfileRow {
    #[serde(default)]
    profile_id: Option<String>,
}

#[derive(Deserialize)]
struct AgentBehaviorCatalogRow {
    #[serde(default)]
    behavior_id: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    tool_selection_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    async fn build_node(tempdir: &tempfile::TempDir) -> Arc<EmbeddedNode> {
        let node = EmbeddedNode::builder()
            .data_path(tempdir.path().join("data"))
            .build()
            .await
            .expect("embedded node boots");
        crate::ensure_runtime_schemas(&node)
            .await
            .expect("runtime schemas register");
        Arc::new(node)
    }

    fn happy_catalog(agent_did: &str) -> PersonaCatalogView {
        PersonaCatalogView {
            available_models: BTreeSet::from(["openai|gpt-5".to_string()]),
            allowed_roots: BTreeSet::new(),
            available_profile_ids: BTreeSet::from(["profile-1".to_string()]),
            known_agent_dids: BTreeSet::from([agent_did.to_string()]),
            behaviors: BTreeMap::new(),
        }
    }

    fn pending_create_doc(request_key: &str, agent_did: &str) -> PersonaRequestDoc {
        PersonaRequestDoc {
            request_key: request_key.to_string(),
            requester_did: "did:key:requester".to_string(),
            agent_did: agent_did.to_string(),
            op_raw: "create".to_string(),
            op: Some(PersonaOp::Create { clone_from: None }),
            persona_name: Some("Research Assistant".to_string()),
            backend_model: Some("openai|gpt-5".to_string()),
            preset: Some(crate::agent::persona_presets::PRESET_WRITE.to_string()),
            profile_id: Some("profile-1".to_string()),
            status: Some("pending".to_string()),
            ..Default::default()
        }
    }

    /// Fixture store: `all` holds every row regardless of status (mirroring
    /// what a real collection would contain); `load_pending_requests` filters
    /// by `status == "pending"` itself, exactly like the production query,
    /// so a terminal row in `all` is provably never reachable by the tick.
    #[derive(Default)]
    struct FixtureStore {
        all: Vec<PersonaRequestDoc>,
        catalog_by_agent: BTreeMap<String, PersonaCatalogView>,
        fail_catalog_for: BTreeSet<String>,
        applied: Mutex<Vec<(String, String)>>,
        rejected: Mutex<Vec<(String, String)>>,
        fail_mark_applied_once: Mutex<BTreeSet<String>>,
    }

    #[async_trait]
    impl PersonaRequestStore for FixtureStore {
        async fn load_pending_requests(&self) -> Result<Vec<PersonaRequestDoc>> {
            Ok(self
                .all
                .iter()
                .filter(|doc| doc.status.as_deref() == Some("pending"))
                .cloned()
                .collect())
        }

        async fn load_catalog_view(&self, agent_did: &str) -> Result<PersonaCatalogView> {
            if self.fail_catalog_for.contains(agent_did) {
                anyhow::bail!("simulated catalog load failure for {agent_did}");
            }
            Ok(self
                .catalog_by_agent
                .get(agent_did)
                .cloned()
                .unwrap_or_default())
        }

        async fn mark_applied(&self, request_key: &str, behavior_id: &str) -> Result<()> {
            if self
                .fail_mark_applied_once
                .lock()
                .unwrap()
                .remove(request_key)
            {
                anyhow::bail!("simulated mark_applied failure for {request_key}");
            }
            self.applied
                .lock()
                .unwrap()
                .push((request_key.to_string(), behavior_id.to_string()));
            Ok(())
        }

        async fn mark_rejected(&self, request_key: &str, detail: &str) -> Result<()> {
            self.rejected
                .lock()
                .unwrap()
                .push((request_key.to_string(), detail.to_string()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn pending_admit_applies_and_marks_behavior_id() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;

        let doc = pending_create_doc("req-1", "did:key:agent");
        let mut catalog_by_agent = BTreeMap::new();
        catalog_by_agent.insert("did:key:agent".to_string(), happy_catalog("did:key:agent"));
        let store = FixtureStore {
            all: vec![doc],
            catalog_by_agent,
            ..Default::default()
        };

        let outcome = reconcile_persona_tick(&store, &node).await?;
        assert_eq!(outcome.applied, BTreeSet::from(["req-1".to_string()]));
        assert!(outcome.repaired.is_empty());
        assert!(outcome.rejected.is_empty());

        let applied = store.applied.lock().unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].0, "req-1");
        assert_eq!(applied[0].1, "did:key:agent:research-assistant");
        Ok(())
    }

    #[tokio::test]
    async fn pending_invalid_is_rejected_with_conjunct_detail() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;

        let mut doc = pending_create_doc("req-invalid", "did:key:agent");
        doc.backend_model = Some("nope|nope".to_string());
        let mut catalog_by_agent = BTreeMap::new();
        catalog_by_agent.insert("did:key:agent".to_string(), happy_catalog("did:key:agent"));
        let store = FixtureStore {
            all: vec![doc],
            catalog_by_agent,
            ..Default::default()
        };

        let outcome = reconcile_persona_tick(&store, &node).await?;
        assert_eq!(
            outcome.rejected,
            BTreeSet::from(["req-invalid".to_string()])
        );
        assert!(outcome.applied.is_empty());

        let rejected = store.rejected.lock().unwrap();
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].0, "req-invalid");
        assert_eq!(
            rejected[0].1,
            r#"unknown model "nope|nope" — pick from the published available_models: [openai|gpt-5]"#
        );
        Ok(())
    }

    #[tokio::test]
    async fn terminal_rows_are_untouched() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;

        let pending_doc = pending_create_doc("req-pending", "did:key:agent");
        let mut applied_doc = pending_create_doc("req-already-applied", "did:key:agent");
        applied_doc.status = Some("applied".to_string());
        let mut rejected_doc = pending_create_doc("req-already-rejected", "did:key:agent");
        rejected_doc.status = Some("rejected".to_string());

        let mut catalog_by_agent = BTreeMap::new();
        catalog_by_agent.insert("did:key:agent".to_string(), happy_catalog("did:key:agent"));
        let store = FixtureStore {
            all: vec![pending_doc, applied_doc, rejected_doc],
            catalog_by_agent,
            ..Default::default()
        };

        let outcome = reconcile_persona_tick(&store, &node).await?;
        assert_eq!(outcome.applied, BTreeSet::from(["req-pending".to_string()]));
        assert!(outcome.rejected.is_empty());

        let applied = store.applied.lock().unwrap();
        assert_eq!(
            applied.len(),
            1,
            "the two terminal rows must never reach process_one_request"
        );
        assert_eq!(applied[0].0, "req-pending");
        Ok(())
    }

    #[tokio::test]
    async fn one_bad_row_does_not_wedge_the_sweep() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;

        let good_doc = pending_create_doc("req-good", "did:key:good-agent");
        let bad_doc = pending_create_doc("req-bad", "did:key:bad-agent");

        let mut catalog_by_agent = BTreeMap::new();
        catalog_by_agent.insert(
            "did:key:good-agent".to_string(),
            happy_catalog("did:key:good-agent"),
        );
        let mut fail_catalog_for = BTreeSet::new();
        fail_catalog_for.insert("did:key:bad-agent".to_string());

        // Bad row FIRST: a bail-early regression would never reach the good
        // row, so this ordering is what actually fences continue-after-failure.
        let store = FixtureStore {
            all: vec![bad_doc, good_doc],
            catalog_by_agent,
            fail_catalog_for,
            ..Default::default()
        };

        let result = reconcile_persona_tick(&store, &node).await;
        assert!(
            result.is_err(),
            "the bad row's error must still surface once the sweep has run"
        );

        let applied = store.applied.lock().unwrap();
        assert_eq!(
            applied.len(),
            1,
            "the good row must converge despite the bad row's failure"
        );
        assert_eq!(applied[0].0, "req-good");
        Ok(())
    }

    /// Mirrors `apply_persona_request`'s own crash-repair contract: a row
    /// whose apply succeeded but whose `mark_applied` failed must re-enter
    /// as pending (the fixture never advanced its own `status`), and the
    /// next tick must converge to `applied` via `repaired: true` without
    /// minting a duplicate `AgentBehavior`.
    #[tokio::test]
    async fn crash_between_apply_and_mark_repairs_without_duplicate() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;

        let doc = pending_create_doc("req-repair", "did:key:repair-agent");
        let agent_did = doc.agent_did.clone();
        let mut catalog_by_agent = BTreeMap::new();
        catalog_by_agent.insert(agent_did.clone(), happy_catalog(&agent_did));

        let mut fail_mark_applied_once = BTreeSet::new();
        fail_mark_applied_once.insert("req-repair".to_string());

        let store1 = FixtureStore {
            all: vec![doc.clone()],
            catalog_by_agent: catalog_by_agent.clone(),
            fail_mark_applied_once: Mutex::new(fail_mark_applied_once),
            ..Default::default()
        };

        let first_result = reconcile_persona_tick(&store1, &node).await;
        assert!(
            first_result.is_err(),
            "the injected mark_applied failure must surface"
        );
        assert!(
            store1.applied.lock().unwrap().is_empty(),
            "mark_applied failed, so no applied record for this attempt"
        );

        // The apply itself succeeded against the real node before the mark
        // failure: exactly one behavior now exists.
        let behaviors = crate::list_agent_behaviors(&node, &agent_did).await?;
        assert_eq!(
            behaviors.len(),
            1,
            "apply must have written exactly one behavior before the mark failure"
        );

        let mut catalog_after = happy_catalog(&agent_did);
        for behavior in &behaviors {
            catalog_after.behaviors.insert(
                behavior.behavior_id.clone(),
                BehaviorRef {
                    enabled: behavior.enabled,
                    tool_selection_id: behavior.tool_selection_id.clone().unwrap_or_default(),
                },
            );
        }
        let mut catalog_by_agent2 = BTreeMap::new();
        catalog_by_agent2.insert(agent_did.clone(), catalog_after);

        let store2 = FixtureStore {
            all: vec![doc],
            catalog_by_agent: catalog_by_agent2,
            ..Default::default()
        };

        let outcome = reconcile_persona_tick(&store2, &node).await?;
        assert_eq!(outcome.repaired, BTreeSet::from(["req-repair".to_string()]));
        assert!(outcome.applied.is_empty());

        let applied = store2.applied.lock().unwrap();
        assert_eq!(
            applied.len(),
            1,
            "the repair converges via a mark_applied call"
        );

        let behaviors_after = crate::list_agent_behaviors(&node, &agent_did).await?;
        assert_eq!(
            behaviors_after.len(),
            1,
            "repair must not mint a duplicate behavior"
        );
        Ok(())
    }

    #[derive(Deserialize)]
    struct PersonaStatusRow {
        status: Option<String>,
        #[serde(default)]
        applied_behavior_id: Option<String>,
        #[serde(default)]
        processed_at: Option<String>,
    }

    /// Embedded-node integration test against the real `GraphqlPersonaRequestStore`:
    /// seeds catalog sources and a pending create row (as a P2P-replicated
    /// row would arrive), runs one tick, and asserts the behavior/selection
    /// materialized and the row's status/applied_behavior_id/processed_at
    /// were written back. Then runs the directory projection tick over the
    /// same node and asserts the new persona is published — tying this PR's
    /// reconciler to PR 1/2's directory projection.
    #[tokio::test]
    async fn graphql_store_tick_applies_pending_request_and_directory_projection_publishes_it(
    ) -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;

        let seed = r#"mutation {
            create_AgentPrincipal(input: {
                agent_did: "did:key:persona-agent",
                display_name: "Persona Agent",
                enabled: true,
                created_at: "2026-07-23T00:00:00Z"
            }) { _docID }
            create_InferenceBackend(input: {
                backend_id: "openai",
                name: "OpenAI",
                enabled: true,
                models: ["gpt-5"]
            }) { _docID }
            create_InferenceProfile(input: {
                profile_id: "profile-1",
                display_name: "Fast"
            }) { _docID }
            create_WorkspaceRoot(input: {
                root_path: "/repo/allowed",
                display_name: "Allowed Root",
                enabled: true
            }) { _docID }
            create_PersonaConfigRequest(input: {
                request_key: "req-integration-1",
                requester_did: "did:key:phone",
                agent_did: "did:key:persona-agent",
                op: "create",
                persona_name: "Research Assistant",
                backend_model: "openai|gpt-5",
                root: "/repo/allowed",
                preset: "write",
                profile_id: "profile-1",
                created_at: "2026-07-23T00:00:00Z",
                status: "pending"
            }) { _docID }
        }"#;
        let response = node.execute(seed).await;
        ensure_no_errors(&response, "seed persona reconciler integration fixtures")?;

        let store = GraphqlPersonaRequestStore::new(node.clone());
        let outcome = reconcile_persona_tick(&store, &node).await?;
        assert_eq!(
            outcome.applied,
            BTreeSet::from(["req-integration-1".to_string()])
        );

        let behavior_id = "did:key:persona-agent:research-assistant";
        let behavior = crate::load_agent_behavior(&node, behavior_id)
            .await?
            .expect("behavior created");
        assert_eq!(
            behavior.display_name,
            Some("Research Assistant".to_string())
        );
        assert!(behavior.enabled);
        assert_eq!(
            behavior.tool_selection_id,
            Some("sel-req-integration-1".to_string())
        );

        let selection = crate::load_tool_selection(&node, "sel-req-integration-1")
            .await?
            .expect("selection created");
        assert_eq!(selection.enable_bash, Some(true));

        let status_query = r#"{
            PersonaConfigRequest(filter: { request_key: { _eq: "req-integration-1" } }) {
                status
                applied_behavior_id
                processed_at
            }
        }"#;
        let response = node.execute(status_query).await;
        ensure_no_errors(&response, "query PersonaConfigRequest after tick")?;
        let request_row = rows::<PersonaStatusRow>(&response, "PersonaConfigRequest")?
            .into_iter()
            .next()
            .expect("request row present");
        assert_eq!(request_row.status.as_deref(), Some("applied"));
        assert_eq!(
            request_row.applied_behavior_id.as_deref(),
            Some(behavior_id)
        );
        assert!(request_row.processed_at.is_some());

        // Ties PR 1+2: the directory projection tick now publishes the newly
        // materialized persona.
        use crate::agent::directory_projection::DirectoryStore as _;
        let directory_store =
            crate::agent::directory_projection::GraphqlDirectoryStore::new(node.clone(), None);
        let directory_source_did = "did:key:home";
        let directory_outcome = crate::agent::directory_projection::reconcile_directory_tick(
            &directory_store,
            directory_source_did,
        )
        .await?;
        assert!(directory_outcome.upserted.contains("did:key:persona-agent"));

        let entries = directory_store
            .list_directory_entries(directory_source_did)
            .await?;
        let entry = entries
            .get("did:key:persona-agent")
            .expect("directory entry for the persona agent");
        assert!(entry.behaviors.contains(&"Research Assistant".to_string()));
        assert!(entry.behavior_ids.contains(&behavior_id.to_string()));

        Ok(())
    }

    /// #1051: the reconciler's catalog view is ceiling-filtered, so the
    /// `root ∈ allowed_roots` conjunct rejects a root the serve-time
    /// operator-ceiling guard would refuse — a persona can no longer be
    /// admitted-yet-unusable.
    #[tokio::test]
    async fn ceiling_filtered_view_rejects_out_of_ceiling_root() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = build_node(&tempdir).await;

        let seed = r#"mutation {
            create_AgentPrincipal(input: {
                agent_did: "did:key:ceiling-agent",
                display_name: "Ceiling Agent",
                enabled: true,
                created_at: "2026-08-06T00:00:00Z"
            }) { _docID }
            create_InferenceBackend(input: {
                backend_id: "openai",
                name: "OpenAI",
                enabled: true,
                models: ["gpt-5"]
            }) { _docID }
            create_InferenceProfile(input: {
                profile_id: "profile-1",
                display_name: "Fast"
            }) { _docID }
            create_WorkspaceRoot(input: {
                root_path: "/ceil/ws/inside",
                display_name: "inside",
                enabled: true
            }) { _docID }
            create_WorkspaceRoot(input: {
                root_path: "/outside/app",
                display_name: "outside",
                enabled: true
            }) { _docID }
        }"#;
        ensure_no_errors(&node.execute(seed).await, "seed ceiling fixtures")?;

        let store = GraphqlPersonaRequestStore::with_ceiling(
            node.clone(),
            Some(std::path::PathBuf::from("/ceil/ws")),
        );
        let catalog = store.load_catalog_view("did:key:ceiling-agent").await?;
        assert!(
            catalog.allowed_roots.contains("/ceil/ws/inside"),
            "in-ceiling root must stay published"
        );
        assert!(
            !catalog.allowed_roots.contains("/outside/app"),
            "out-of-ceiling root must never enter the admission set"
        );

        let doc = PersonaRequestDoc {
            request_key: "pcr-ceiling".to_string(),
            requester_did: "did:key:phone".to_string(),
            agent_did: "did:key:ceiling-agent".to_string(),
            op_raw: "create".to_string(),
            op: Some(PersonaOp::Create { clone_from: None }),
            behavior_id: None,
            persona_name: Some("Escapee".to_string()),
            backend_model: Some("openai|gpt-5".to_string()),
            root: Some("/outside/app".to_string()),
            preset: Some("readonly".to_string()),
            profile_id: Some("profile-1".to_string()),
            created_at: None,
            status: Some("pending".to_string()),
            status_detail: None,
            applied_behavior_id: None,
            processed_at: None,
        };
        match crate::agent::persona_ops::decide_persona_request(&doc, &catalog) {
            crate::agent::persona_ops::PersonaVerdict::Reject(detail) => {
                assert!(
                    detail.contains("/outside/app"),
                    "rejection must name the offending root: {detail}"
                );
            }
            verdict => panic!("out-of-ceiling root must be rejected, got {verdict:?}"),
        }
        Ok(())
    }
}
