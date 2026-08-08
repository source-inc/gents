//! Fleet-discovery directory projection (issue #714).
//!
//! Projects AgentPrincipal x AgentBehavior x AgentRuntime into
//! AgentDirectoryEntry rows — the replicated agent index the `machine`
//! pairing template pushes to attached clients. Modeled in
//! `Proofs/PeerRegistryDiscovery/DirectoryProjection.lean`; fenced by
//! `tests/conformance/directory_projection.rs`. The sweep runs on source
//! Update events, so the load-bearing property is that a settled state is a
//! write-free fixpoint. Each runtime owns only the rows stamped with its
//! source DID; replicated foreign rows remain outside its projection.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use defra_node::{EmbeddedNode, EventName, QueryResponse};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::agent::persona_presets::{builtin_preset_names, preset_name, PresetFields};
use crate::graphql::escape_graphql_string;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub directory_key: String,
    pub agent_did: String,
    pub source_did: String,
    pub display_name: String,
    pub behaviors: Vec<String>,
    /// Index-aligned with `behaviors` (`behavior_ids[i]` names `behaviors[i]`)
    /// so clients can stamp `AgentRequest.behavior_id` from a picked display
    /// name.
    pub behavior_ids: Vec<String>,
    /// The agent's resolved default (from `AgentPrincipal.default_behavior_id`),
    /// so clients can badge threads bound to a NON-default behavior; empty
    /// when the principal has none.
    pub default_behavior_id: String,
    /// Per-behavior persona dimensions, index-aligned with `behavior_ids`.
    /// `"backend_id|model_name"`, split on the FIRST `|` (ids must not
    /// contain `|`); `""` only when both are blank, so a half-configured
    /// behavior yields `"|model"` or `"backend|"`.
    pub behavior_models: Vec<String>,
    /// Per-behavior file tool root (`""` when the behavior's `ToolSelection`
    /// is missing or unset), index-aligned with `behavior_ids`.
    pub behavior_roots: Vec<String>,
    /// Per-behavior built-in preset name, or `""` for a custom selection (or
    /// a missing one), index-aligned with `behavior_ids`.
    pub behavior_presets: Vec<String>,
    /// Per-behavior inference profile id (`""` = unset), index-aligned with
    /// `behavior_ids`.
    pub behavior_profiles: Vec<String>,
    /// Home-level pickable options for the persona composer; identical on
    /// every entry derived from the same source. Flattened into the four
    /// `available_models`/`allowed_roots`/`permission_presets`/
    /// `available_profiles` columns at upsert/list time.
    pub options: CatalogOptions,
    pub runtime_state: String,
    pub last_seen: String,
}

/// Per-behavior persona dimensions, as loaded from `AgentBehavior`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BehaviorInfo {
    pub behavior_id: String,
    pub display_name: String,
    pub backend_id: String,
    pub model_name: String,
    pub tool_selection_id: String,
    pub inference_profile_id: String,
}

/// A `ToolSelection`'s directory-relevant fields, keyed by `selection_id`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectionInfo {
    pub file_tool_root: String,
    pub preset: PresetFields,
}

/// Home-level, source-wide pickable options for the persona composer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogOptions {
    /// `"backend_id|model_name"`, sorted, deduped.
    pub available_models: Vec<String>,
    /// Enabled `WorkspaceRoot` paths, sorted.
    pub allowed_roots: Vec<String>,
    /// `builtin_preset_names()`.
    pub permission_presets: Vec<String>,
    /// `"profile_id|display_name"`, sorted; split on the FIRST `|` —
    /// display names may contain `|`, profile ids must not.
    pub available_profiles: Vec<String>,
    /// Compact JSON object per profile (#1050's info-card fields),
    /// INDEX-ALIGNED with `available_profiles` (`params[i]` describes
    /// `profiles[i]`). See [`render_profile_params_json`] for the exact
    /// shape.
    pub available_profile_params: Vec<String>,
}

/// Everything `derive_directory_entries` consumes, loaded as one unit. The
/// sweep runs on every source Update event (and concurrently across
/// automated homes), so the store contract is a *single* snapshot load per
/// tick — `GraphqlDirectoryStore` satisfies it with one multi-root query
/// rather than one round trip per collection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceSnapshot {
    /// One row per enabled principal: `(agent_did, display_name,
    /// default_behavior_id)`. `default_behavior_id` is empty when the
    /// principal has none.
    pub principals: Vec<(String, String, String)>,
    /// Per principal, the enabled behaviors as `BehaviorInfo` (display name
    /// falls back to the id when blank).
    pub behaviors: BTreeMap<String, Vec<BehaviorInfo>>,
    /// Per principal, `(process_state, updated_at)` from `AgentRuntime`.
    pub runtimes: BTreeMap<String, (String, String)>,
    /// `ToolSelection` rows keyed by `selection_id`.
    pub selections: BTreeMap<String, SelectionInfo>,
    /// Home-level composer options (backends, roots, presets, profiles).
    pub options: CatalogOptions,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirectoryTickOutcome {
    pub upserted: BTreeSet<String>,
    pub refreshed: BTreeSet<String>,
    pub retracted: BTreeSet<String>,
}

#[async_trait]
pub trait DirectoryStore: Send + Sync {
    /// Load every projection input in one shot (see [`SourceSnapshot`]).
    async fn load_source_snapshot(&self) -> Result<SourceSnapshot>;
    async fn list_directory_entries(
        &self,
        source_did: &str,
    ) -> Result<BTreeMap<String, DirectoryEntry>>;
    async fn upsert_directory_entry(&self, entry: &DirectoryEntry) -> Result<()>;
    async fn delete_directory_entry(&self, source_did: &str, agent_did: &str) -> Result<()>;
}

pub fn directory_entry_key(source_did: &str, agent_did: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(source_did.as_bytes());
    digest.update(b"\x1f");
    digest.update(agent_did.as_bytes());
    format!(
        "dir-{}",
        bs58::encode(&digest.finalize()[..16]).into_string()
    )
}

/// Canonicalize a runtime `updated_at` into the exact lexical form DefraDB's
/// `DateTime` column stores and returns, so a settled directory row stays a
/// write-free fixpoint.
///
/// `AgentRuntime.updated_at` is a *String* column the runtime writes as
/// `Utc::now().to_rfc3339()` — offset `+00:00`, sub-second precision — but
/// `AgentDirectoryEntry.last_seen` is a `DateTime` column that re-serializes on
/// storage: it renders the offset as `Z`, and its sub-second rendering is not
/// guaranteed byte-stable (Go's DefraDB trims trailing zeros; chrono buckets to
/// 3/6/9 digits). Comparing the raw source string against the normalized stored
/// string made `existing != desired` on *every* sweep, so each tick re-upserted
/// the row, and each upsert re-fired the Update-driven sweep — an unbounded
/// write/event storm that starved the runtime.
///
/// Quantizing to whole-second UTC `...Z` removes the sub-second ambiguity
/// entirely: DefraDB round-trips a second-precision `Z` value unchanged (the
/// conformance fence and the embedded-node fixpoint test pin this), and
/// sub-second freshness is not meaningful for a fleet "last seen". The function
/// is idempotent (`canon(canon(x)) == canon(x)`), preserving the model's
/// projection idempotence. Blank input (a principal with no `AgentRuntime` row)
/// stays blank → rendered as `null`. Genuinely unparseable non-blank input
/// passes through trimmed unchanged; that one row still fails its upsert, as
/// before, under the per-entry error tolerance in `reconcile_directory_tick`.
fn canonicalize_last_seen(updated_at: &str) -> String {
    let trimmed = updated_at.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    match chrono::DateTime::parse_from_rfc3339(trimmed) {
        Ok(parsed) => parsed
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Secs, true),
        Err(_) => trimmed.to_string(),
    }
}

/// The projection: one entry per principal, contents a function of the
/// principal's payload (display name, behavior names, runtime state). The
/// runtime `updated_at` is canonicalized (see `canonicalize_last_seen`) so the
/// derived `last_seen` matches its own stored `DateTime` round-trip.
/// Mirrors Lean `project`.
pub fn derive_directory_entries(
    source_did: &str,
    principals: &[(String, String, String)],
    behaviors: &BTreeMap<String, Vec<BehaviorInfo>>,
    runtimes: &BTreeMap<String, (String, String)>,
    selections: &BTreeMap<String, SelectionInfo>,
    options: &CatalogOptions,
) -> BTreeMap<String, DirectoryEntry> {
    principals
        .iter()
        .filter(|(did, _, _)| !did.trim().is_empty())
        .map(|(did, display_name, default_behavior_id)| {
            // Sort by (name, id) for a stable picker order, then dedup by id
            // (a behavior's id determines its identity; same id implies same
            // name, so duplicates land adjacent after the sort).
            let mut infos = behaviors.get(did).cloned().unwrap_or_default();
            infos.sort_by(|a, b| {
                a.display_name
                    .cmp(&b.display_name)
                    .then_with(|| a.behavior_id.cmp(&b.behavior_id))
            });
            infos.dedup_by(|a, b| a.behavior_id == b.behavior_id);

            let names = infos.iter().map(|info| info.display_name.clone()).collect();
            let ids: Vec<String> = infos.iter().map(|info| info.behavior_id.clone()).collect();
            let behavior_models = infos
                .iter()
                .map(|info| {
                    if info.backend_id.is_empty() && info.model_name.is_empty() {
                        String::new()
                    } else {
                        format!("{}|{}", info.backend_id, info.model_name)
                    }
                })
                .collect();
            let behavior_roots = infos
                .iter()
                .map(|info| {
                    selections
                        .get(&info.tool_selection_id)
                        .map(|selection| selection.file_tool_root.clone())
                        .unwrap_or_default()
                })
                .collect();
            let behavior_presets = infos
                .iter()
                .map(|info| {
                    selections
                        .get(&info.tool_selection_id)
                        .and_then(|selection| preset_name(&selection.preset))
                        .map(str::to_string)
                        .unwrap_or_default()
                })
                .collect();
            let behavior_profiles = infos
                .iter()
                .map(|info| info.inference_profile_id.clone())
                .collect();

            let (runtime_state, updated_at) = runtimes.get(did).cloned().unwrap_or_default();
            (
                did.clone(),
                DirectoryEntry {
                    directory_key: directory_entry_key(source_did, did),
                    agent_did: did.clone(),
                    source_did: source_did.to_string(),
                    display_name: display_name.clone(),
                    behaviors: names,
                    behavior_ids: ids,
                    default_behavior_id: default_behavior_id.clone(),
                    behavior_models,
                    behavior_roots,
                    behavior_presets,
                    behavior_profiles,
                    options: options.clone(),
                    runtime_state,
                    last_seen: canonicalize_last_seen(&updated_at),
                },
            )
        })
        .collect()
}

/// One reconcile sweep: derive the desired directory rows from source
/// collections and diff against this source's `AgentDirectoryEntry` rows,
/// upserting/refreshing/retracting exactly what has drifted. Mirrors Lean
/// `projectStep`; a settled state (desired == existing) must be a
/// write-free fixpoint (`settled_fixpoint`), since the sweep runs on every
/// Update event.
pub async fn reconcile_directory_tick(
    store: &dyn DirectoryStore,
    source_did: &str,
) -> Result<DirectoryTickOutcome> {
    let snapshot = store
        .load_source_snapshot()
        .await
        .context("load source snapshot")?;
    let desired = derive_directory_entries(
        source_did,
        &snapshot.principals,
        &snapshot.behaviors,
        &snapshot.runtimes,
        &snapshot.selections,
        &snapshot.options,
    );
    let existing = store
        .list_directory_entries(source_did)
        .await
        .context("list directory entries")?;

    // Per-entry error tolerance: one principal's malformed source row (e.g.
    // no AgentRuntime row, previously producing an unparseable `last_seen`)
    // must not abort the whole sweep. Warn-and-continue past each failure,
    // collecting only the first error to return once every entry has had a
    // chance to converge; outcome sets only ever record operations that
    // actually succeeded.
    let mut outcome = DirectoryTickOutcome::default();
    let mut first_error: Option<anyhow::Error> = None;
    for (did, entry) in &desired {
        match existing.get(did) {
            Some(row) if row == entry => {}
            Some(_) => match store.upsert_directory_entry(entry).await {
                Ok(()) => {
                    outcome.refreshed.insert(did.clone());
                }
                Err(error) => {
                    tracing::warn!(agent_did = %did, error = %error, "directory entry refresh failed; continuing sweep");
                    if first_error.is_none() {
                        first_error =
                            Some(error.context(format!("refresh directory entry for {did}")));
                    }
                }
            },
            None => match store.upsert_directory_entry(entry).await {
                Ok(()) => {
                    outcome.upserted.insert(did.clone());
                }
                Err(error) => {
                    tracing::warn!(agent_did = %did, error = %error, "directory entry upsert failed; continuing sweep");
                    if first_error.is_none() {
                        first_error =
                            Some(error.context(format!("upsert directory entry for {did}")));
                    }
                }
            },
        }
    }
    for did in existing.keys() {
        if !desired.contains_key(did) {
            match store.delete_directory_entry(source_did, did).await {
                Ok(()) => {
                    outcome.retracted.insert(did.clone());
                }
                Err(error) => {
                    tracing::warn!(agent_did = %did, error = %error, "directory entry retraction failed; continuing sweep");
                    if first_error.is_none() {
                        first_error =
                            Some(error.context(format!("retract directory entry for {did}")));
                    }
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(outcome)
}

pub async fn run_directory_projection(
    node: Arc<EmbeddedNode>,
    source_did: String,
    ceiling_root: Option<std::path::PathBuf>,
    cancel: CancellationToken,
) -> Result<()> {
    let store = GraphqlDirectoryStore::new(node.clone(), ceiling_root);
    let mut subscription = node.subscribe(&[EventName::Update]);
    let mut interval =
        tokio::time::interval(crate::agent::p2p_reconcile::intervals::sweep_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    sweep_directory(&store, &source_did).await;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = interval.tick() => sweep_directory(&store, &source_did).await,
            message = subscription.recv() => {
                if message.is_none() {
                    tracing::warn!("directory projection update subscription closed; continuing with periodic sweeps");
                    continue;
                }
                let dropped = subscription.check_and_reset_dropped();
                if dropped > 0 {
                    tracing::warn!(dropped, "directory projection update subscription dropped messages");
                }
                sweep_directory(&store, &source_did).await;
            }
        }
    }
}

async fn sweep_directory(store: &GraphqlDirectoryStore, source_did: &str) {
    match reconcile_directory_tick(store, source_did).await {
        Ok(outcome) => {
            if !outcome.upserted.is_empty()
                || !outcome.refreshed.is_empty()
                || !outcome.retracted.is_empty()
            {
                tracing::info!(
                    upserted = ?outcome.upserted,
                    refreshed = ?outcome.refreshed,
                    retracted = ?outcome.retracted,
                    "reconciled fleet-discovery directory rows"
                );
            }
        }
        Err(error) => {
            tracing::warn!(error = %error, "directory projection reconcile sweep failed")
        }
    }
}

// `pub(crate)`: the persona-request reconciler's embedded-node integration
// test constructs this directly to run a directory-projection tick over the
// same node and prove the two reconcilers converge together (issue's PR 1+2
// tie-in) — this is the only reason it needs to be more than module-private.
pub(crate) struct GraphqlDirectoryStore {
    node: Arc<EmbeddedNode>,
    /// Operator tool-root ceiling (`--tool-root`); see
    /// [`filter_roots_to_ceiling`].
    ceiling_root: Option<std::path::PathBuf>,
}

impl GraphqlDirectoryStore {
    pub(crate) fn new(node: Arc<EmbeddedNode>, ceiling_root: Option<std::path::PathBuf>) -> Self {
        Self { node, ceiling_root }
    }
}

#[async_trait]
impl DirectoryStore for GraphqlDirectoryStore {
    async fn load_source_snapshot(&self) -> Result<SourceSnapshot> {
        // One multi-root document on purpose: the sweep re-runs on every
        // source Update event, and homes running concurrent automation
        // multiply that rate — per-collection executes here would each pay
        // their own parse/plan/transaction overhead per sweep.
        let query = r#"{
            AgentPrincipal {
                agent_did
                display_name
                default_behavior_id
                enabled
            }
            AgentBehavior {
                agent_did
                display_name
                behavior_id
                backend_id
                model_name
                tool_selection_id
                inference_profile_id
                enabled
            }
            AgentRuntime {
                agent_did
                process_state
                updated_at
            }
            ToolSelection {
                selection_id
                file_tool_root
                enable_file_tools
                file_tools_mode
                enable_bash
                bash_mode
                command_allowed_argv_prefixes
                command_forbidden_argv_prefixes
                read_only_command_allowlist
                enable_self_config
                write_tools
            }
            InferenceBackend {
                backend_id
                models
                enabled
            }
            WorkspaceRoot {
                root_path
                enabled
            }
            InferenceProfile {
                profile_id
                display_name
                context_window
                max_output_tokens
                max_turns
                temperature
                top_p
            }
        }"#;
        let response = self.node.execute(query).await;
        ensure_no_errors(&response, "query directory source snapshot")?;
        Ok(SourceSnapshot {
            principals: parse_principals(&response)?,
            behaviors: parse_behaviors(&response)?,
            runtimes: parse_runtime_states(&response)?,
            selections: parse_tool_selections(&response)?,
            options: parse_catalog_options(&response, self.ceiling_root.as_deref())?,
        })
    }

    async fn list_directory_entries(
        &self,
        source_did: &str,
    ) -> Result<BTreeMap<String, DirectoryEntry>> {
        let source_did = escape_graphql_string(source_did);
        let query = format!(
            r#"{{
            AgentDirectoryEntry(filter: {{ source_did: {{ _eq: "{source_did}" }} }}) {{
                directory_key
                agent_did
                source_did
                display_name
                behaviors
                behavior_ids
                default_behavior_id
                behavior_models
                behavior_roots
                behavior_presets
                behavior_profiles
                available_models
                allowed_roots
                permission_presets
                available_profiles
                available_profile_params
                runtime_state
                last_seen
            }}
        }}"#
        );
        let response = self.node.execute(&query).await;
        ensure_no_errors(&response, "query AgentDirectoryEntry")?;
        Ok(rows::<DirectoryRow>(&response, "AgentDirectoryEntry")?
            .into_iter()
            .filter_map(|row| {
                let directory_key = row.directory_key?.trim().to_string();
                if directory_key.is_empty() {
                    return None;
                }
                let did = row.agent_did?.trim().to_string();
                if did.is_empty() {
                    return None;
                }
                let entry = DirectoryEntry {
                    directory_key,
                    agent_did: did.clone(),
                    source_did: row.source_did.unwrap_or_default(),
                    display_name: row.display_name.unwrap_or_default(),
                    behaviors: row.behaviors.unwrap_or_default(),
                    behavior_ids: row.behavior_ids.unwrap_or_default(),
                    default_behavior_id: row.default_behavior_id.unwrap_or_default(),
                    behavior_models: row.behavior_models.unwrap_or_default(),
                    behavior_roots: row.behavior_roots.unwrap_or_default(),
                    behavior_presets: row.behavior_presets.unwrap_or_default(),
                    behavior_profiles: row.behavior_profiles.unwrap_or_default(),
                    options: CatalogOptions {
                        available_models: row.available_models.unwrap_or_default(),
                        allowed_roots: row.allowed_roots.unwrap_or_default(),
                        permission_presets: row.permission_presets.unwrap_or_default(),
                        available_profiles: row.available_profiles.unwrap_or_default(),
                        available_profile_params: row.available_profile_params.unwrap_or_default(),
                    },
                    runtime_state: row.runtime_state.unwrap_or_default(),
                    last_seen: row.last_seen.unwrap_or_default(),
                };
                Some((did, entry))
            })
            .collect())
    }

    async fn upsert_directory_entry(&self, entry: &DirectoryEntry) -> Result<()> {
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let mutation = upsert_directory_entry_mutation(entry, &now);
        let response = self.node.execute(&mutation).await;
        ensure_no_errors(&response, "upsert AgentDirectoryEntry")
    }

    async fn delete_directory_entry(&self, source_did: &str, agent_did: &str) -> Result<()> {
        let mutation = delete_directory_entry_mutation(source_did, agent_did);
        let response = self.node.execute(&mutation).await;
        ensure_no_errors(&response, "delete AgentDirectoryEntry")
    }
}

fn upsert_directory_entry_mutation(entry: &DirectoryEntry, now: &str) -> String {
    let agent_did = escape_graphql_string(&entry.agent_did);
    let directory_key =
        escape_graphql_string(&directory_entry_key(&entry.source_did, &entry.agent_did));
    let source_did = escape_graphql_string(&entry.source_did);
    let display_name = escape_graphql_string(&entry.display_name);
    let behaviors = graphql_string_list_literal(entry.behaviors.iter().map(String::as_str));
    // Index-aligned with `behaviors`; rendered as null (never []) when empty
    // — an empty list literal types as JsonArray and corrupts nillable array
    // columns.
    let behavior_ids = graphql_string_list_literal(entry.behavior_ids.iter().map(String::as_str));
    let default_behavior_id = escape_graphql_string(&entry.default_behavior_id);
    let behavior_models =
        graphql_string_list_literal(entry.behavior_models.iter().map(String::as_str));
    let behavior_roots =
        graphql_string_list_literal(entry.behavior_roots.iter().map(String::as_str));
    let behavior_presets =
        graphql_string_list_literal(entry.behavior_presets.iter().map(String::as_str));
    let behavior_profiles =
        graphql_string_list_literal(entry.behavior_profiles.iter().map(String::as_str));
    let available_models =
        graphql_string_list_literal(entry.options.available_models.iter().map(String::as_str));
    let allowed_roots =
        graphql_string_list_literal(entry.options.allowed_roots.iter().map(String::as_str));
    let permission_presets =
        graphql_string_list_literal(entry.options.permission_presets.iter().map(String::as_str));
    let available_profiles =
        graphql_string_list_literal(entry.options.available_profiles.iter().map(String::as_str));
    let available_profile_params = graphql_string_list_literal(
        entry
            .options
            .available_profile_params
            .iter()
            .map(String::as_str),
    );
    let runtime_state = escape_graphql_string(&entry.runtime_state);
    let last_seen = graphql_nullable_datetime_literal(&entry.last_seen);
    let now = escape_graphql_string(now);
    format!(
        r#"mutation {{
            upsert_AgentDirectoryEntry(
                filter: {{ directory_key: {{ _eq: "{directory_key}" }} }},
                add: {{
                    directory_key: "{directory_key}",
                    agent_did: "{agent_did}",
                    source_did: "{source_did}",
                    display_name: "{display_name}",
                    behaviors: {behaviors},
                    behavior_ids: {behavior_ids},
                    default_behavior_id: "{default_behavior_id}",
                    behavior_models: {behavior_models},
                    behavior_roots: {behavior_roots},
                    behavior_presets: {behavior_presets},
                    behavior_profiles: {behavior_profiles},
                    available_models: {available_models},
                    allowed_roots: {allowed_roots},
                    permission_presets: {permission_presets},
                    available_profiles: {available_profiles},
                    available_profile_params: {available_profile_params},
                    runtime_state: "{runtime_state}",
                    last_seen: {last_seen},
                    updated_at: "{now}"
                }},
                update: {{
                    display_name: "{display_name}",
                    behaviors: {behaviors},
                    behavior_ids: {behavior_ids},
                    default_behavior_id: "{default_behavior_id}",
                    behavior_models: {behavior_models},
                    behavior_roots: {behavior_roots},
                    behavior_presets: {behavior_presets},
                    behavior_profiles: {behavior_profiles},
                    available_models: {available_models},
                    allowed_roots: {allowed_roots},
                    permission_presets: {permission_presets},
                    available_profiles: {available_profiles},
                    available_profile_params: {available_profile_params},
                    runtime_state: "{runtime_state}",
                    last_seen: {last_seen},
                    updated_at: "{now}"
                }}
            ) {{ _docID }}
        }}"#
    )
}

fn delete_directory_entry_mutation(source_did: &str, agent_did: &str) -> String {
    let directory_key = escape_graphql_string(&directory_entry_key(source_did, agent_did));
    format!(
        r#"mutation {{
            delete_AgentDirectoryEntry(filter: {{
                directory_key: {{ _eq: "{directory_key}" }}
            }}) {{ _docID }}
        }}"#
    )
}

/// Renders a GraphQL string-list literal. Empty renders as `null`, never
/// `[]` — an empty list literal types as `JsonArray` and corrupts nillable
/// array columns.
fn graphql_string_list_literal<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let items = values
        .into_iter()
        .map(|value| format!(r#""{}""#, escape_graphql_string(value)))
        .collect::<Vec<_>>();
    if items.is_empty() {
        "null".to_string()
    } else {
        format!("[{}]", items.join(", "))
    }
}

fn graphql_nullable_datetime_literal(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "null".to_string()
    } else {
        format!(r#""{}""#, escape_graphql_string(trimmed))
    }
}

fn ensure_no_errors(response: &QueryResponse, label: &str) -> Result<()> {
    if response.has_errors() {
        bail!("{label} failed: {:?}", response.errors);
    }
    Ok(())
}

fn rows<T>(response: &QueryResponse, field: &str) -> Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let Some(value) = response.data.as_ref().and_then(|data| data.get(field)) else {
        return Ok(Vec::new());
    };
    serde_json::from_value(value.clone()).with_context(|| format!("decode {field} rows"))
}

// The per-collection decoders behind `load_source_snapshot`, each reading its
// root field out of the shared multi-root response.

fn parse_principals(response: &QueryResponse) -> Result<Vec<(String, String, String)>> {
    Ok(rows::<PrincipalRow>(response, "AgentPrincipal")?
        .into_iter()
        .filter_map(|row| {
            if !row.enabled.unwrap_or(true) {
                return None;
            }
            let did = row.agent_did?.trim().to_string();
            if did.is_empty() {
                return None;
            }
            let display_name = row
                .display_name
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .to_string();
            let default_behavior_id = row
                .default_behavior_id
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .to_string();
            Some((did, display_name, default_behavior_id))
        })
        .collect())
}

fn parse_behaviors(response: &QueryResponse) -> Result<BTreeMap<String, Vec<BehaviorInfo>>> {
    let mut grouped: BTreeMap<String, Vec<BehaviorInfo>> = BTreeMap::new();
    for row in rows::<BehaviorRow>(response, "AgentBehavior")? {
        if !row.enabled.unwrap_or(true) {
            continue;
        }
        let Some(did) = row.agent_did.map(|did| did.trim().to_string()) else {
            continue;
        };
        if did.is_empty() {
            continue;
        }
        let Some(behavior_id) = row
            .behavior_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
        else {
            continue;
        };
        let display_name = row
            .display_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| behavior_id.clone());
        let backend_id = row
            .backend_id
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .to_string();
        let model_name = row
            .model_name
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .to_string();
        let tool_selection_id = row
            .tool_selection_id
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .to_string();
        let inference_profile_id = row
            .inference_profile_id
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .to_string();
        grouped.entry(did).or_default().push(BehaviorInfo {
            behavior_id,
            display_name,
            backend_id,
            model_name,
            tool_selection_id,
            inference_profile_id,
        });
    }
    Ok(grouped)
}

fn parse_runtime_states(response: &QueryResponse) -> Result<BTreeMap<String, (String, String)>> {
    Ok(rows::<RuntimeRow>(response, "AgentRuntime")?
        .into_iter()
        .filter_map(|row| {
            let did = row.agent_did?.trim().to_string();
            if did.is_empty() {
                return None;
            }
            let process_state = row
                .process_state
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .to_string();
            let updated_at = row
                .updated_at
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .to_string();
            Some((did, (process_state, updated_at)))
        })
        .collect())
}

fn parse_tool_selections(response: &QueryResponse) -> Result<BTreeMap<String, SelectionInfo>> {
    let mut grouped: BTreeMap<String, SelectionInfo> = BTreeMap::new();
    for row in rows::<ToolSelectionRow>(response, "ToolSelection")? {
        let Some(selection_id) = row
            .selection_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
        else {
            continue;
        };
        let file_tool_root = row
            .file_tool_root
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .to_string();
        let preset = PresetFields {
            enable_file_tools: row.enable_file_tools.unwrap_or_default(),
            file_tools_mode: row.file_tools_mode.unwrap_or_default(),
            enable_bash: row.enable_bash.unwrap_or_default(),
            bash_mode: row.bash_mode.unwrap_or_default(),
            command_allowed_argv_prefixes: row.command_allowed_argv_prefixes.unwrap_or_default(),
            command_forbidden_argv_prefixes: row
                .command_forbidden_argv_prefixes
                .unwrap_or_default(),
            read_only_command_allowlist: row.read_only_command_allowlist.unwrap_or_default(),
            enable_self_config: row.enable_self_config.unwrap_or_default(),
            write_tools: row.write_tools.unwrap_or_default(),
        };
        grouped.insert(
            selection_id,
            SelectionInfo {
                file_tool_root,
                preset,
            },
        );
    }
    Ok(grouped)
}

/// Hand-assembles a compact JSON object describing an `InferenceProfile`'s
/// display-relevant parameters (#1050's info-card fields), in a FIXED key
/// order: `context_window`, `max_output_tokens`, `max_turns`, `temperature`,
/// `top_p`. Deliberately NOT `serde_json::to_string` of a map — a map's key
/// order is not guaranteed stable, and byte-stability across sweeps is what
/// feeds `directory_projection`'s write-free settled fixpoint. Each key is
/// omitted entirely when its source field is `None`; `{}` when the profile
/// has none of them.
///
/// Floats render via Rust's `{}` `Display` on exactly the decoded row
/// value — no canonicalization (unlike `canonicalize_last_seen`). That's
/// sufficient for the fixpoint: the same stored row decodes to the same
/// `f64` and re-renders byte-identical on every sweep, so settled
/// comparison never spuriously drifts.
fn render_profile_params_json(
    context_window: Option<i64>,
    max_output_tokens: Option<i64>,
    max_turns: Option<i64>,
    temperature: Option<f64>,
    top_p: Option<f64>,
) -> String {
    let mut fields = Vec::new();
    if let Some(value) = context_window {
        fields.push(format!(r#""context_window":{value}"#));
    }
    if let Some(value) = max_output_tokens {
        fields.push(format!(r#""max_output_tokens":{value}"#));
    }
    if let Some(value) = max_turns {
        fields.push(format!(r#""max_turns":{value}"#));
    }
    if let Some(value) = temperature {
        fields.push(format!(r#""temperature":{value}"#));
    }
    if let Some(value) = top_p {
        fields.push(format!(r#""top_p":{value}"#));
    }
    format!("{{{}}}", fields.join(","))
}

/// Keep only workspace roots inside the operator tool-root ceiling
/// (`gents server --tool-root`). The serve-time guard in
/// `tool_surface::build` refuses any behavior whose file tool root escapes
/// the ceiling, so publishing such a root in the catalog — or admitting it
/// into a persona — mints admitted-yet-unusable config (#1051). Resolution
/// mirrors the guard exactly (`resolve_configured_tool_root` +
/// `starts_with`); a root (or ceiling) that fails to resolve is dropped,
/// fail-closed: never offer what the guard may refuse.
pub(crate) fn filter_roots_to_ceiling(
    roots: Vec<String>,
    ceiling_root: Option<&std::path::Path>,
) -> Vec<String> {
    let Some(ceiling) = ceiling_root else {
        return roots;
    };
    let Ok(ceiling) = crate::tool_surface::resolve_configured_tool_root(ceiling) else {
        return Vec::new();
    };
    roots
        .into_iter()
        .filter(|root| {
            crate::tool_surface::resolve_configured_tool_root(std::path::Path::new(root))
                .map(|resolved| resolved.starts_with(&ceiling))
                .unwrap_or(false)
        })
        .collect()
}

fn parse_catalog_options(
    response: &QueryResponse,
    ceiling_root: Option<&std::path::Path>,
) -> Result<CatalogOptions> {
    // Backend rows always carry `enabled` (backend_registry treats it as
    // required on decode), so the conservative null-means-disabled default
    // here is a dead branch — deliberately stricter than `parse_behaviors`'
    // null-means-enabled, because advertising models for a half-written
    // backend row is worse than omitting them for a tick.
    let mut available_models: Vec<String> =
        rows::<InferenceBackendRow>(response, "InferenceBackend")?
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
    available_models.sort();
    available_models.dedup();

    let mut allowed_roots: Vec<String> = filter_roots_to_ceiling(
        rows::<WorkspaceRootRow>(response, "WorkspaceRoot")?
            .into_iter()
            .filter(|row| row.enabled.unwrap_or(false))
            .filter_map(|row| row.root_path)
            .collect(),
        ceiling_root,
    );
    allowed_roots.sort();

    // Sort pairs first, then split (mirrors how `behaviors`/`behavior_ids`
    // stay aligned in `derive_directory_entries`): each tuple carries both
    // the `available_profiles` string and its aligned params JSON, so a
    // single sort keeps `available_profile_params[i]` describing
    // `available_profiles[i]` after the split.
    let mut profile_pairs: Vec<(String, String)> =
        rows::<InferenceProfileRow>(response, "InferenceProfile")?
            .into_iter()
            .filter_map(|row| {
                let profile_id = row.profile_id?;
                let display_name = row
                    .display_name
                    .filter(|name| !name.is_empty())
                    .unwrap_or_else(|| profile_id.clone());
                let params = render_profile_params_json(
                    row.context_window,
                    row.max_output_tokens,
                    row.max_turns,
                    row.temperature,
                    row.top_p,
                );
                Some((format!("{profile_id}|{display_name}"), params))
            })
            .collect();
    profile_pairs.sort();
    let (available_profiles, available_profile_params): (Vec<String>, Vec<String>) =
        profile_pairs.into_iter().unzip();

    Ok(CatalogOptions {
        available_models,
        allowed_roots,
        permission_presets: builtin_preset_names()
            .iter()
            .map(|name| name.to_string())
            .collect(),
        available_profiles,
        available_profile_params,
    })
}

#[derive(Deserialize)]
struct PrincipalRow {
    agent_did: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    default_behavior_id: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
}

#[derive(Deserialize)]
struct BehaviorRow {
    agent_did: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    behavior_id: Option<String>,
    #[serde(default)]
    backend_id: Option<String>,
    #[serde(default)]
    model_name: Option<String>,
    #[serde(default)]
    tool_selection_id: Option<String>,
    #[serde(default)]
    inference_profile_id: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
}

#[derive(Deserialize)]
struct ToolSelectionRow {
    #[serde(default)]
    selection_id: Option<String>,
    #[serde(default)]
    file_tool_root: Option<String>,
    #[serde(default)]
    enable_file_tools: Option<bool>,
    #[serde(default)]
    file_tools_mode: Option<String>,
    #[serde(default)]
    enable_bash: Option<bool>,
    #[serde(default)]
    bash_mode: Option<String>,
    #[serde(default)]
    command_allowed_argv_prefixes: Option<Vec<String>>,
    #[serde(default)]
    command_forbidden_argv_prefixes: Option<Vec<String>>,
    #[serde(default)]
    read_only_command_allowlist: Option<Vec<String>>,
    #[serde(default)]
    enable_self_config: Option<bool>,
    #[serde(default)]
    write_tools: Option<Vec<String>>,
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
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    context_window: Option<i64>,
    #[serde(default)]
    max_output_tokens: Option<i64>,
    #[serde(default)]
    max_turns: Option<i64>,
    #[serde(default)]
    temperature: Option<f64>,
    #[serde(default)]
    top_p: Option<f64>,
}

#[derive(Deserialize)]
struct RuntimeRow {
    agent_did: Option<String>,
    #[serde(default)]
    process_state: Option<String>,
    #[serde(default)]
    updated_at: Option<String>,
}

#[derive(Deserialize)]
struct DirectoryRow {
    #[serde(default)]
    directory_key: Option<String>,
    agent_did: Option<String>,
    #[serde(default)]
    source_did: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    behaviors: Option<Vec<String>>,
    #[serde(default)]
    behavior_ids: Option<Vec<String>>,
    #[serde(default)]
    default_behavior_id: Option<String>,
    #[serde(default)]
    behavior_models: Option<Vec<String>>,
    #[serde(default)]
    behavior_roots: Option<Vec<String>>,
    #[serde(default)]
    behavior_presets: Option<Vec<String>>,
    #[serde(default)]
    behavior_profiles: Option<Vec<String>>,
    #[serde(default)]
    available_models: Option<Vec<String>>,
    #[serde(default)]
    allowed_roots: Option<Vec<String>>,
    #[serde(default)]
    permission_presets: Option<Vec<String>>,
    #[serde(default)]
    available_profiles: Option<Vec<String>>,
    #[serde(default)]
    available_profile_params: Option<Vec<String>>,
    #[serde(default)]
    runtime_state: Option<String>,
    #[serde(default)]
    last_seen: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::filter_roots_to_ceiling;

    #[test]
    fn ceiling_filter_matrix() {
        let roots = vec![
            "/ceil/ws/app".to_string(),
            "/ceil/ws".to_string(),
            "/ceil/wsx/app".to_string(),
            "/outside/app".to_string(),
        ];

        // No ceiling: untouched.
        assert_eq!(
            filter_roots_to_ceiling(roots.clone(), None),
            roots,
            "no ceiling must publish every enabled root"
        );

        // Component-wise containment: "/ceil/wsx" is NOT within "/ceil/ws"
        // (the sibling-prefix trap a string prefix check would fall into).
        assert_eq!(
            filter_roots_to_ceiling(roots, Some(std::path::Path::new("/ceil/ws"))),
            vec!["/ceil/ws/app".to_string(), "/ceil/ws".to_string()],
            "only roots within the ceiling (incl. the ceiling itself) survive"
        );
    }

    use super::*;

    fn entry(agent_did: &str, last_seen: &str) -> DirectoryEntry {
        DirectoryEntry {
            directory_key: directory_entry_key("did:key:home", agent_did),
            agent_did: agent_did.to_string(),
            source_did: "did:key:home".to_string(),
            display_name: "Display".to_string(),
            behaviors: Vec::new(),
            behavior_ids: Vec::new(),
            default_behavior_id: String::new(),
            behavior_models: Vec::new(),
            behavior_roots: Vec::new(),
            behavior_presets: Vec::new(),
            behavior_profiles: Vec::new(),
            options: CatalogOptions::default(),
            runtime_state: "running".to_string(),
            last_seen: last_seen.to_string(),
        }
    }

    /// C2 regression: a principal with no `AgentRuntime` row derives a blank
    /// `last_seen`; the mutation must render `null`, never `""` — DefraDB
    /// rejects a non-RFC3339 `DateTime` string on create AND upsert, and an
    /// unconditional quoted render poisoned the whole directory sweep.
    #[test]
    fn directory_entry_key_partitions_same_agent_did_by_source() {
        assert_ne!(
            directory_entry_key("did:key:local-home", "did:key:shared-agent"),
            directory_entry_key("did:key:foreign-home", "did:key:shared-agent"),
        );
    }

    #[test]
    fn upsert_mutation_sets_immutable_source_did_only_when_adding() {
        let mutation = upsert_directory_entry_mutation(
            &entry("did:key:running", "2026-07-20T00:00:00Z"),
            "2026-07-23T00:00:00Z",
        );
        let (add, update) = mutation
            .split_once("update: {")
            .expect("upsert mutation has update payload");

        assert!(add.contains(r#"source_did: "did:key:home""#));
        assert!(
            !update.contains("source_did:"),
            "immutable source_did must not be resent in update payload: {update}"
        );
    }

    #[test]
    fn upsert_mutation_renders_null_for_blank_last_seen() {
        for blank in ["", "   "] {
            let mutation = upsert_directory_entry_mutation(
                &entry("did:key:no-runtime", blank),
                "2026-07-23T00:00:00Z",
            );
            assert!(
                mutation.contains("last_seen: null"),
                "blank last_seen ({blank:?}) must render as null: {mutation}"
            );
            assert!(!mutation.contains(r#"last_seen: """#));
        }
    }

    #[test]
    fn upsert_mutation_renders_quoted_last_seen_when_present() {
        let mutation = upsert_directory_entry_mutation(
            &entry("did:key:running", "2026-07-20T00:00:00Z"),
            "2026-07-23T00:00:00Z",
        );
        assert!(mutation.contains(r#"last_seen: "2026-07-20T00:00:00Z""#));
    }

    /// `behavior_ids` follows the same null-never-[] discipline as
    /// `behaviors`, and stays index-aligned when both are populated.
    #[test]
    fn upsert_mutation_renders_null_behavior_ids_when_empty_and_aligned_list_when_present() {
        let empty = upsert_directory_entry_mutation(
            &entry("did:key:no-behaviors", "2026-07-20T00:00:00Z"),
            "2026-07-23T00:00:00Z",
        );
        assert!(
            empty.contains("behavior_ids: null"),
            "empty behavior_ids must render as null, never []: {empty}"
        );

        let mut with_behaviors = entry("did:key:with-behaviors", "2026-07-20T00:00:00Z");
        with_behaviors.behaviors = vec!["Artist".to_string(), "Coder".to_string()];
        with_behaviors.behavior_ids = vec![
            "did:key:a:artist".to_string(),
            "did:key:a:coder".to_string(),
        ];
        let mutation = upsert_directory_entry_mutation(&with_behaviors, "2026-07-23T00:00:00Z");
        assert!(mutation.contains(r#"behaviors: ["Artist", "Coder"]"#));
        assert!(mutation.contains(r#"behavior_ids: ["did:key:a:artist", "did:key:a:coder"]"#));
    }

    /// The nine new dimension/option columns follow the same null-never-[]
    /// discipline as `behaviors`/`behavior_ids`, in BOTH the add and update
    /// payloads — an empty list literal types as `JsonArray` and corrupts
    /// nillable array columns. `available_profile_params` (#1050) is the
    /// newest of these.
    #[test]
    fn upsert_mutation_renders_null_for_all_nine_new_columns_when_empty() {
        let mutation = upsert_directory_entry_mutation(
            &entry("did:key:no-dimensions", "2026-07-20T00:00:00Z"),
            "2026-07-23T00:00:00Z",
        );
        for column in [
            "behavior_models",
            "behavior_roots",
            "behavior_presets",
            "behavior_profiles",
            "available_models",
            "allowed_roots",
            "permission_presets",
            "available_profiles",
            "available_profile_params",
        ] {
            let needle = format!("{column}: null");
            assert!(
                mutation.contains(&needle),
                "empty {column} must render as null, never []: {mutation}"
            );
        }
    }

    /// Populated dimension/option lists render as aligned array literals in
    /// both the add and update payloads. `available_profile_params` carries
    /// a JSON string (embedded double quotes) to pin that
    /// `graphql_string_list_literal`'s per-element `escape_graphql_string`
    /// survives the mutation round-trip rather than producing invalid
    /// GraphQL.
    #[test]
    fn upsert_mutation_renders_populated_dimension_and_option_lists() {
        let mut with_dimensions = entry("did:key:with-dimensions", "2026-07-20T00:00:00Z");
        with_dimensions.behavior_ids = vec!["did:key:a:coder".to_string()];
        with_dimensions.behavior_models = vec!["openai|gpt-5".to_string()];
        with_dimensions.behavior_roots = vec!["/repo/a".to_string()];
        with_dimensions.behavior_presets = vec!["readonly".to_string()];
        with_dimensions.behavior_profiles = vec!["fast-profile".to_string()];
        with_dimensions.options = CatalogOptions {
            available_models: vec!["openai|gpt-5".to_string()],
            allowed_roots: vec!["/repo/a".to_string()],
            permission_presets: vec!["readonly".to_string(), "write".to_string()],
            available_profiles: vec!["fast-profile|Fast".to_string()],
            available_profile_params: vec![
                r#"{"context_window":128000,"temperature":0.2}"#.to_string()
            ],
        };
        let mutation = upsert_directory_entry_mutation(&with_dimensions, "2026-07-23T00:00:00Z");
        assert!(mutation.contains(r#"behavior_models: ["openai|gpt-5"]"#));
        assert!(mutation.contains(r#"behavior_roots: ["/repo/a"]"#));
        assert!(mutation.contains(r#"behavior_presets: ["readonly"]"#));
        assert!(mutation.contains(r#"behavior_profiles: ["fast-profile"]"#));
        assert!(mutation.contains(r#"available_models: ["openai|gpt-5"]"#));
        assert!(mutation.contains(r#"allowed_roots: ["/repo/a"]"#));
        assert!(mutation.contains(r#"permission_presets: ["readonly", "write"]"#));
        assert!(mutation.contains(r#"available_profiles: ["fast-profile|Fast"]"#));
        assert!(
            mutation.contains(
                r#"available_profile_params: ["{\"context_window\":128000,\"temperature\":0.2}"]"#
            ),
            "the params JSON string's embedded quotes must survive escaped: {mutation}"
        );
    }

    /// `default_behavior_id` is a plain string field (unlike the nullable
    /// list/DateTime fields above): empty stays `""`, never `null`, so the
    /// client can compare it against a picked `behavior_id` unconditionally.
    #[test]
    fn upsert_mutation_renders_default_behavior_id_as_plain_string() {
        let empty = upsert_directory_entry_mutation(
            &entry("did:key:no-default", "2026-07-20T00:00:00Z"),
            "2026-07-23T00:00:00Z",
        );
        assert!(
            empty.contains(r#"default_behavior_id: """#),
            "empty default_behavior_id must render as an empty string, never null: {empty}"
        );

        let mut with_default = entry("did:key:with-default", "2026-07-20T00:00:00Z");
        with_default.default_behavior_id = "did:key:a:coder".to_string();
        let mutation = upsert_directory_entry_mutation(&with_default, "2026-07-23T00:00:00Z");
        assert!(mutation.contains(r#"default_behavior_id: "did:key:a:coder""#));
    }

    /// Round-trip consistency: a stored `null` `last_seen` must decode back
    /// to `""` so the settled comparison against `derive_directory_entries`'
    /// blank default holds (no perpetual refresh loop for runtime-less
    /// principals).
    #[test]
    fn nullable_datetime_literal_blank_maps_to_null_and_round_trips_via_default() {
        assert_eq!(graphql_nullable_datetime_literal(""), "null");
        assert_eq!(graphql_nullable_datetime_literal("  "), "null");
        // DirectoryRow.last_seen is `Option<String>`; DefraDB's stored `null`
        // decodes to `None`, and `unwrap_or_default()` in
        // `list_directory_entries` maps that back to `""` — the same value
        // `derive_directory_entries` defaults to for a runtime-less principal.
        let row: DirectoryRow = serde_json::from_value(serde_json::json!({
            "directory_key": "dir-no-runtime",
            "agent_did": "did:key:no-runtime",
            "source_did": "did:key:home",
            "display_name": "No Runtime",
            "behaviors": [],
            "runtime_state": "",
            "last_seen": null,
        }))
        .expect("stored directory row with null last_seen should deserialize");
        assert_eq!(row.last_seen.unwrap_or_default(), "");
    }

    #[test]
    fn canonicalize_last_seen_quantizes_offset_and_subseconds_to_utc_seconds() {
        // The exact shape the runtime writes: `Utc::now().to_rfc3339()` —
        // `+00:00` offset, sub-second precision. Must render whole-second `Z`.
        assert_eq!(
            canonicalize_last_seen("2026-07-23T23:30:55.845794+00:00"),
            "2026-07-23T23:30:55Z"
        );
        // Non-UTC offset is converted to UTC before quantizing.
        assert_eq!(
            canonicalize_last_seen("2026-07-23T18:30:55.5-05:00"),
            "2026-07-23T23:30:55Z"
        );
        // Already canonical → unchanged (idempotent).
        assert_eq!(
            canonicalize_last_seen("2026-07-23T23:30:55Z"),
            "2026-07-23T23:30:55Z"
        );
        assert_eq!(
            canonicalize_last_seen(&canonicalize_last_seen("2026-07-23T23:30:55.845794+00:00")),
            "2026-07-23T23:30:55Z"
        );
        // Blank stays blank (rendered as null downstream); unparseable passes
        // through trimmed (that one row fails its upsert, per-entry tolerant).
        assert_eq!(canonicalize_last_seen(""), "");
        assert_eq!(canonicalize_last_seen("   "), "");
        assert_eq!(canonicalize_last_seen("not-a-date"), "not-a-date");
    }

    /// Regression fence for the settled-fixpoint bug: an `AgentRuntime.updated_at`
    /// written in the runtime's real `Utc::now().to_rfc3339()` shape (offset
    /// `+00:00`, sub-second precision) used to differ from its own stored
    /// `AgentDirectoryEntry.last_seen` `DateTime` round-trip (offset `Z`),
    /// making every sweep re-upsert and — since the sweep runs on Update events
    /// — self-perpetuate into an unbounded write/event storm. Seeding the exact
    /// runtime format and asserting the second tick is write-free pins the fix.
    /// The param-JSON builder matrix (#1050): fixed key order
    /// (`context_window`, `max_output_tokens`, `max_turns`, `temperature`,
    /// `top_p`), each key omitted entirely when its source field is `None`,
    /// `{}` when the profile has none of them.
    #[test]
    fn render_profile_params_json_matrix() {
        assert_eq!(
            render_profile_params_json(Some(128000), Some(4096), Some(50), Some(0.2), Some(0.9)),
            r#"{"context_window":128000,"max_output_tokens":4096,"max_turns":50,"temperature":0.2,"top_p":0.9}"#
        );
        assert_eq!(
            render_profile_params_json(Some(128000), None, None, Some(0.2), None),
            r#"{"context_window":128000,"temperature":0.2}"#
        );
        assert_eq!(
            render_profile_params_json(None, None, None, None, None),
            "{}"
        );
    }

    /// Float rendering uses Rust's `{}` `Display` on exactly the decoded row
    /// value — no canonicalization — so the same row re-renders
    /// byte-identical on every sweep, which is all the settled-fixpoint
    /// comparison needs.
    #[test]
    fn render_profile_params_json_float_display_is_byte_stable_across_renders() {
        let first = render_profile_params_json(None, None, None, Some(0.1), Some(1.0));
        let second = render_profile_params_json(None, None, None, Some(0.1), Some(1.0));
        assert_eq!(first, second);
        assert_eq!(first, r#"{"temperature":0.1,"top_p":1}"#);
    }

    #[tokio::test]
    async fn graphql_tick_is_write_free_fixpoint_for_runtime_updated_at_format() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = Arc::new(
            EmbeddedNode::builder()
                .data_path(tempdir.path().join("data"))
                .build()
                .await?,
        );
        crate::ensure_runtime_schemas(&node).await?;
        let updated_at = Utc::now().to_rfc3339();
        let seed = format!(
            r#"mutation {{
                create_AgentPrincipal(input: {{
                    agent_did: "did:key:real",
                    display_name: "Real",
                    enabled: true,
                    created_at: "2026-07-23T00:00:00Z"
                }}) {{ _docID }}
                create_AgentRuntime(input: {{
                    agent_did: "did:key:real",
                    process_state: "running",
                    updated_at: "{updated_at}"
                }}) {{ _docID }}
            }}"#
        );
        let response = node.execute(&seed).await;
        ensure_no_errors(&response, "seed runtime-format updated_at")?;

        let store = GraphqlDirectoryStore::new(node.clone(), None);
        let first = reconcile_directory_tick(&store, "did:key:home").await?;
        assert_eq!(first.upserted, BTreeSet::from(["did:key:real".to_string()]));

        // The stored, canonicalized last_seen is whole-second UTC `Z`.
        let stored = store.list_directory_entries("did:key:home").await?;
        assert_eq!(
            stored.get("did:key:real").map(|e| e.last_seen.as_str()),
            Some(canonicalize_last_seen(&updated_at).as_str())
        );

        // The load-bearing property: a second tick over unchanged sources is a
        // write-free fixpoint (no self-perpetuating storm).
        let second = reconcile_directory_tick(&store, "did:key:home").await?;
        assert_eq!(
            second,
            DirectoryTickOutcome::default(),
            "settled state must be a write-free fixpoint for the runtime updated_at format"
        );
        Ok(())
    }

    /// C2 regression, embedded-node integration (mirrors reciprocal.rs's
    /// `graphql_tick_retracts_for_signed_revoked_membership`): two principals,
    /// one WITHOUT an `AgentRuntime` row, converge in one tick — the
    /// runtime-less principal's directory row must exist with `last_seen`
    /// round-tripping to `""` rather than aborting the sweep. Deleting a
    /// principal then retracts exactly its row.
    ///
    /// Also covers persona-catalog round-trip (issue #714 PR 3): one behavior
    /// wired to a readonly-matching `ToolSelection` (with root), backend,
    /// model, and profile; the other behavior wired to none of those, so its
    /// four dimension entries must derive `""`. A disabled `WorkspaceRoot`
    /// must be excluded from `allowed_roots`. The second tick over the same
    /// settled state must still be write-free — the nine new columns must
    /// not break the storm-regression invariant. The `InferenceProfile`
    /// carries a partial params set (`context_window` + `temperature`,
    /// `max_output_tokens`/`max_turns`/`top_p` unset) so
    /// `available_profile_params` (#1050) exercises the omit-when-null path
    /// through a real node, aligned with `available_profiles`, and stays
    /// write-free on the settled re-tick (the float-display byte-stability
    /// this fences).
    #[tokio::test]
    async fn graphql_tick_converges_runtime_less_principal_and_retracts_on_removal() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = Arc::new(
            EmbeddedNode::builder()
                .data_path(tempdir.path().join("data"))
                .build()
                .await?,
        );
        crate::ensure_runtime_schemas(&node).await?;

        let seed = r#"mutation {
            create_AgentPrincipal(input: {
                agent_did: "did:key:with-runtime",
                display_name: "With Runtime",
                default_behavior_id: "enabled-behavior",
                enabled: true,
                created_at: "2026-07-23T00:00:00Z"
            }) { _docID }
            create_AgentPrincipal(input: {
                agent_did: "did:key:no-runtime",
                display_name: "No Runtime",
                enabled: true,
                created_at: "2026-07-23T00:00:00Z"
            }) { _docID }
            create_AgentPrincipal(input: {
                agent_did: "did:key:disabled",
                display_name: "Disabled",
                enabled: false,
                created_at: "2026-07-23T00:00:00Z"
            }) { _docID }
            create_AgentBehavior(input: {
                behavior_id: "enabled-behavior",
                agent_did: "did:key:with-runtime",
                display_name: "Enabled Behavior",
                backend_id: "openai",
                model_name: "gpt-5",
                tool_selection_id: "readonly-selection",
                inference_profile_id: "fast-profile",
                enabled: true
            }) { _docID }
            create_AgentBehavior(input: {
                behavior_id: "artist-behavior",
                agent_did: "did:key:with-runtime",
                display_name: "Artist Behavior",
                enabled: true
            }) { _docID }
            create_AgentBehavior(input: {
                behavior_id: "disabled-behavior",
                agent_did: "did:key:with-runtime",
                display_name: "Disabled Behavior",
                enabled: false
            }) { _docID }
            create_AgentRuntime(input: {
                agent_did: "did:key:with-runtime",
                process_state: "running",
                updated_at: "2026-07-23T12:34:56.845794+00:00"
            }) { _docID }
            create_ToolSelection(input: {
                selection_id: "readonly-selection",
                agent_did: "did:key:with-runtime",
                file_tool_root: "/repo/with-runtime",
                enable_file_tools: true,
                file_tools_mode: "ReadOnly",
                enable_bash: true,
                bash_mode: "ReadOnly",
                enable_self_config: false
            }) { _docID }
            create_InferenceBackend(input: {
                backend_id: "openai",
                name: "OpenAI",
                enabled: true,
                models: ["gpt-5", "gpt-5-mini"]
            }) { _docID }
            create_InferenceProfile(input: {
                profile_id: "fast-profile",
                display_name: "Fast Profile",
                context_window: 128000,
                temperature: 0.2
            }) { _docID }
            create_WorkspaceRoot(input: {
                root_path: "/repo/enabled",
                display_name: "Enabled Root",
                enabled: true
            }) { _docID }
            create_WorkspaceRoot(input: {
                root_path: "/repo/disabled",
                display_name: "Disabled Root",
                enabled: false
            }) { _docID }
        }"#;
        let response = node.execute(seed).await;
        ensure_no_errors(&response, "seed directory projection principals")?;

        let store = GraphqlDirectoryStore::new(node.clone(), None);
        let outcome = reconcile_directory_tick(&store, "did:key:home").await?;
        assert_eq!(
            outcome.upserted,
            BTreeSet::from([
                "did:key:with-runtime".to_string(),
                "did:key:no-runtime".to_string(),
            ]),
            "the runtime-less principal must not wedge the sweep"
        );

        let entries = store.list_directory_entries("did:key:home").await?;
        let with_runtime = entries
            .get("did:key:with-runtime")
            .expect("with-runtime directory row");
        assert_eq!(with_runtime.runtime_state, "running");
        assert_eq!(
            with_runtime.behaviors,
            vec![
                "Artist Behavior".to_string(),
                "Enabled Behavior".to_string()
            ]
        );
        assert_eq!(
            with_runtime.behavior_ids,
            vec![
                "artist-behavior".to_string(),
                "enabled-behavior".to_string()
            ],
            "behavior_ids must round-trip index-aligned with behaviors through a real node"
        );
        assert_eq!(
            with_runtime.default_behavior_id, "enabled-behavior",
            "default_behavior_id must round-trip from AgentPrincipal through a real node"
        );
        // Persona dimensions, index-aligned with the sorted behavior_ids
        // above (artist-behavior first, enabled-behavior second): the
        // artist behavior has no backend/model/selection/profile wired up
        // and must derive "" on all four; the enabled behavior has all four
        // wired and must round-trip through a real node.
        assert_eq!(
            with_runtime.behavior_models,
            vec![String::new(), "openai|gpt-5".to_string()],
            "behavior_models must round-trip backend_id|model_name, aligned"
        );
        assert_eq!(
            with_runtime.behavior_roots,
            vec![String::new(), "/repo/with-runtime".to_string()],
            "behavior_roots must round-trip the wired selection's file_tool_root, aligned"
        );
        assert_eq!(
            with_runtime.behavior_presets,
            vec![String::new(), "readonly".to_string()],
            "behavior_presets must classify the readonly-matching selection, aligned"
        );
        assert_eq!(
            with_runtime.behavior_profiles,
            vec![String::new(), "fast-profile".to_string()],
            "behavior_profiles must round-trip inference_profile_id, aligned"
        );
        assert_eq!(
            with_runtime.options.available_models,
            vec!["openai|gpt-5".to_string(), "openai|gpt-5-mini".to_string()],
            "available_models must list every model of every enabled backend, sorted"
        );
        assert_eq!(
            with_runtime.options.allowed_roots,
            vec!["/repo/enabled".to_string()],
            "the disabled WorkspaceRoot must be excluded from allowed_roots"
        );
        assert_eq!(
            with_runtime.options.permission_presets,
            crate::agent::persona_presets::builtin_preset_names()
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            with_runtime.options.available_profiles,
            vec!["fast-profile|Fast Profile".to_string()]
        );
        assert_eq!(
            with_runtime.options.available_profile_params,
            vec![r#"{"context_window":128000,"temperature":0.2}"#.to_string()],
            "available_profile_params must round-trip through a real node, aligned with \
             available_profiles, omitting the unset max_output_tokens/max_turns/top_p keys"
        );
        assert!(
            !entries.contains_key("did:key:disabled"),
            "disabled principals must not be advertised"
        );
        // The non-canonical `+00:00`/sub-second updated_at is quantized to
        // whole-second UTC `Z`, matching its own stored DateTime round-trip so
        // the second tick below is write-free rather than a self-perpetuating
        // storm.
        assert_eq!(with_runtime.last_seen, "2026-07-23T12:34:56Z");
        let no_runtime = entries
            .get("did:key:no-runtime")
            .expect("no-runtime directory row present despite no AgentRuntime row");
        assert_eq!(
            no_runtime.last_seen, "",
            "runtime-less principal's stored null last_seen must round-trip to \"\""
        );
        assert_eq!(
            no_runtime.default_behavior_id, "",
            "a principal with no default_behavior_id must stay empty, not null-coerced garbage"
        );
        // Options are home-level, so a principal with no behaviors at all
        // still carries the same catalog on its row.
        assert_eq!(no_runtime.options, with_runtime.options);

        // Settled state is a write-free fixpoint: the runtime-less principal
        // must not keep re-triggering writes forever, and the eight new
        // dimension/option columns must not break settled-comparison either.
        let second = reconcile_directory_tick(&store, "did:key:home").await?;
        assert_eq!(second, DirectoryTickOutcome::default());

        let delete = r#"mutation {
            delete_AgentPrincipal(filter: { agent_did: { _eq: "did:key:no-runtime" } }) { _docID }
        }"#;
        let response = node.execute(delete).await;
        ensure_no_errors(&response, "delete directory-projection principal")?;

        let outcome = reconcile_directory_tick(&store, "did:key:home").await?;
        assert_eq!(
            outcome.retracted,
            BTreeSet::from(["did:key:no-runtime".to_string()])
        );

        let entries = store.list_directory_entries("did:key:home").await?;
        assert!(!entries.contains_key("did:key:no-runtime"));
        assert!(entries.contains_key("did:key:with-runtime"));

        Ok(())
    }
}

#[cfg(test)]
mod source_partition_regression_tests {
    use super::*;

    #[tokio::test]
    async fn graphql_tick_preserves_foreign_row_with_same_agent_did() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let node = Arc::new(
            EmbeddedNode::builder()
                .data_path(tempdir.path().join("data"))
                .build()
                .await?,
        );
        crate::ensure_runtime_schemas(&node).await?;

        let foreign_source = "did:key:foreign-home";
        let local_source = "did:key:local-home";
        let agent_did = "did:key:shared-agent";
        let foreign_key = directory_entry_key(foreign_source, agent_did);
        let seed = format!(
            r#"mutation {{
                create_AgentDirectoryEntry(input: {{
                    directory_key: "{foreign_key}",
                    agent_did: "{agent_did}",
                    source_did: "{foreign_source}",
                    display_name: "Foreign",
                    runtime_state: "running",
                    updated_at: "2026-07-23T00:00:00Z"
                }}) {{ _docID }}
                create_AgentPrincipal(input: {{
                    agent_did: "{agent_did}",
                    display_name: "Local",
                    enabled: true,
                    created_at: "2026-07-23T00:00:00Z"
                }}) {{ _docID }}
            }}"#
        );
        ensure_no_errors(
            &node.execute(&seed).await,
            "seed same-DID source partitions",
        )?;

        let store = GraphqlDirectoryStore::new(node.clone(), None);
        let first = reconcile_directory_tick(&store, local_source).await?;
        assert_eq!(first.upserted, BTreeSet::from([agent_did.to_string()]));
        assert_eq!(
            store.list_directory_entries(foreign_source).await?[agent_did].display_name,
            "Foreign",
            "local projection must not overwrite the foreign same-DID row"
        );
        assert_eq!(
            store.list_directory_entries(local_source).await?[agent_did].display_name,
            "Local"
        );

        let delete = format!(
            r#"mutation {{ delete_AgentPrincipal(filter: {{ agent_did: {{ _eq: "{agent_did}" }} }}) {{ _docID }} }}"#
        );
        ensure_no_errors(
            &node.execute(&delete).await,
            "delete local same-DID principal",
        )?;
        let retracted = reconcile_directory_tick(&store, local_source).await?;
        assert_eq!(retracted.retracted, BTreeSet::from([agent_did.to_string()]));
        assert!(store.list_directory_entries(local_source).await?.is_empty());
        assert_eq!(
            store.list_directory_entries(foreign_source).await?[agent_did].display_name,
            "Foreign"
        );
        Ok(())
    }
}
