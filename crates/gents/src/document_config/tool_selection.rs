use anyhow::Result;
use defra_node::EmbeddedNode;
use serde::{Deserialize, Serialize};

use super::graphql_fields;
use super::serde_helpers;
use crate::config_client::mint_recreate_identity_timestamp;
use crate::defra_query::DEFRA_QUERY_TOOL_NAME;
use crate::document_config::SubagentTarget;
use crate::graphql::escape_graphql_string;
use crate::meta_tools::META_TOOL_NAMES;
use crate::retry::execute_graphql_with_conflict_retry;
use crate::tool_surface::TOOL_POLICY_V1;
use crate::toolset::{
    CANCEL_PROCESS_TOOL_NAME, CANCEL_SUBAGENT_TOOL_NAME, CONTEXT_BUDGET_TOOL_NAME,
    LIST_PROCESSES_TOOL_NAME, LIST_SUBAGENTS_TOOL_NAME, READ_PROCESS_TOOL_NAME,
    READ_SUBAGENT_TOOL_NAME, SESSION_HISTORY_TOOL_NAME, SPAWN_PROCESS_TOOL_NAME,
    SPAWN_SUBAGENT_TOOL_NAME, STEER_SUBAGENT_TOOL_NAME, WAIT_PROCESS_TOOL_NAME,
    WAIT_SUBAGENT_TOOL_NAME,
};

/// One field of a [`WriteToolDecl`]: a named slot the bound write tool exposes,
/// and whether the agent must provide it.
///
/// `name` is trimmed at deserialization so the stored value, the runtime
/// [`crate::defra_write::BoundedWriteTool`], and `config validate` all agree on
/// the same canonical identifier (the field name is interpolated verbatim as a
/// GraphQL input key, so stray whitespace would otherwise corrupt the mutation).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct WriteToolField {
    pub name: String,
    pub required: bool,
}

impl<'de> serde::Deserialize<'de> for WriteToolField {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Raw {
            name: String,
            #[serde(default)]
            required: bool,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(WriteToolField {
            name: raw.name.trim().to_string(),
            required: raw.required,
        })
    }
}

/// A declarative, schema-bounded document-write tool. Each declaration becomes
/// one runtime `BoundedWriteTool` that writes exactly one validated document to
/// one collection. Stored in the `ToolSelection.write_tools` `[String]` column
/// as the JSON serialization of one declaration per entry — mirroring the
/// `subagent_targets` `[String]` precedent so there is no Lean/schema change
/// beyond adding the column.
///
/// `tool_name` and `collection` are trimmed at deserialization (see
/// [`WriteToolField`] for the rationale); `description` is free text and is
/// preserved verbatim.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct WriteToolDecl {
    pub tool_name: String,
    pub collection: String,
    pub description: String,
    pub fields: Vec<WriteToolField>,
}

impl<'de> serde::Deserialize<'de> for WriteToolDecl {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Raw {
            tool_name: String,
            collection: String,
            #[serde(default)]
            description: String,
            #[serde(default)]
            fields: Vec<WriteToolField>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(WriteToolDecl {
            tool_name: raw.tool_name.trim().to_string(),
            collection: raw.collection.trim().to_string(),
            description: raw.description,
            fields: raw.fields,
        })
    }
}

impl WriteToolDecl {
    /// A decl is well-formed iff it names a non-empty tool and target collection.
    /// Single source of truth for the registration/advertisement gate.
    pub fn is_well_formed(&self) -> bool {
        !self.tool_name.trim().is_empty() && !self.collection.trim().is_empty()
    }
}

/// True when `name` is already claimed by the built-in tool surface: the native
/// file/shell tools, the meta tools, the subagent/process control tools, or the
/// built-in singletons (`defra_query`, `context_budget`, `sessions`, `memory`).
///
/// A `write_tools` declaration whose `tool_name` collides with one of these
/// would be appended to the runtime tool vector under a name an existing
/// built-in already advertises: [`crate::tool_surface::ToolSurface::tool_names`]
/// dedupes the advertised list (so the model sees a single name) while
/// `build_tools` registers two `ToolDyn` impls and `BackgroundToolRegistry`
/// keys them by name with last-write-wins — silently shadowing the built-in.
/// The apply/ingest validators reject the collision instead.
pub fn is_reserved_builtin_tool_name(name: &str) -> bool {
    let name = name.trim();

    // Native tools have no shared name constants; these literals must stay in
    // sync with `crate::toolset::NativeTool::tool_name`. The
    // `reserved_names_cover_native_and_meta_tools` test guards against drift.
    const NATIVE_TOOL_NAMES: &[&str] = &[
        "list_files",
        "read_file",
        "glob",
        "grep",
        "write_file",
        "edit_file",
        "bash",
        "bash_unrestricted",
    ];
    const SUBAGENT_TOOL_NAMES: &[&str] = &[
        SPAWN_SUBAGENT_TOOL_NAME,
        WAIT_SUBAGENT_TOOL_NAME,
        LIST_SUBAGENTS_TOOL_NAME,
        READ_SUBAGENT_TOOL_NAME,
        STEER_SUBAGENT_TOOL_NAME,
        CANCEL_SUBAGENT_TOOL_NAME,
        SPAWN_PROCESS_TOOL_NAME,
        WAIT_PROCESS_TOOL_NAME,
        LIST_PROCESSES_TOOL_NAME,
        READ_PROCESS_TOOL_NAME,
        CANCEL_PROCESS_TOOL_NAME,
    ];
    // `memory` is reserved unconditionally: the `agent-memory` feature gates the
    // tool's availability, not the legitimacy of the name as a write-tool id.
    const SINGLETON_TOOL_NAMES: &[&str] = &[
        DEFRA_QUERY_TOOL_NAME,
        CONTEXT_BUDGET_TOOL_NAME,
        SESSION_HISTORY_TOOL_NAME,
        "memory",
    ];

    NATIVE_TOOL_NAMES.contains(&name)
        || META_TOOL_NAMES.contains(&name)
        || SUBAGENT_TOOL_NAMES.contains(&name)
        || SINGLETON_TOOL_NAMES.contains(&name)
        || crate::self_config::SELF_CONFIG_TOOL_NAMES.contains(&name)
}

/// Deserialize the `write_tools` field from either representation:
/// - a JSON array of [`WriteToolDecl`] objects (manifest / `config apply` input),
/// - a JSON array of strings, each the JSON serialization of one
///   [`WriteToolDecl`] (how DefraDB returns the `[String]` column),
/// - `null` / missing / empty string (→ `None`).
///
/// This mirrors how `subagent_targets` survives the GraphQL `[String]` round-trip
/// while keeping the manifest-facing shape a structured list of objects.
pub(crate) fn deserialize_optional_write_tools<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<WriteToolDecl>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;
    use serde_json::Value;

    let value = Option::<Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::String(s) if s.trim().is_empty() => Ok(Some(Vec::new())),
        Value::String(s) => {
            // A single JSON-string entry (defensive; the column is a list).
            let decl: WriteToolDecl = serde_json::from_str(&s).map_err(D::Error::custom)?;
            Ok(Some(vec![decl]))
        }
        Value::Array(items) => {
            let mut decls = Vec::with_capacity(items.len());
            for item in items {
                let decl = match item {
                    // DefraDB `[String]` column: each entry is a JSON string.
                    Value::String(s) => {
                        serde_json::from_str::<WriteToolDecl>(&s).map_err(D::Error::custom)?
                    }
                    // Manifest input: each entry is a JSON object.
                    other => {
                        serde_json::from_value::<WriteToolDecl>(other).map_err(D::Error::custom)?
                    }
                };
                decls.push(decl);
            }
            Ok(Some(decls))
        }
        other => Err(D::Error::custom(format!(
            "write_tools must be a list of WriteToolDecl objects or JSON strings, got {other}"
        ))),
    }
}

/// Encode the `write_tools` field for a GraphQL document mutation: each
/// [`WriteToolDecl`] is serialized to a JSON string so the value fits the
/// `[String]` column, then emitted via the shared string-list encoder (which
/// renders an empty list as `null`, never `[]`). Mirrors the `subagent_targets`
/// encode path.
fn graphql_write_tools_field(decls: Option<&[WriteToolDecl]>) -> Option<String> {
    let entries: Vec<String> = decls?
        .iter()
        .map(|decl| serde_json::to_string(decl).expect("WriteToolDecl serializes to JSON"))
        .collect();
    graphql_fields::graphql_string_list_field("write_tools", Some(&entries))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ToolSelectionDocument {
    #[serde(default)]
    pub selection_id: String,
    #[serde(default)]
    pub agent_did: String,
    pub display_name: Option<String>,
    pub tool_policy_version: Option<String>,
    pub enable_file_tools: Option<bool>,
    pub file_tools_mode: Option<String>,
    pub file_tool_root: Option<String>,
    pub enable_bash: Option<bool>,
    pub bash_mode: Option<String>,
    pub command_execution_policy: Option<String>,
    /// Argv-prefix allow gate on top of (or instead of pure allowlist admission
    /// for) read-only bash. Empty/absent means no prefix gate.
    ///
    /// When non-empty, **every** command must match an allowed prefix (global
    /// gate). In `ReadOnly` mode a matching prefix also admits commands that
    /// are not on the base allowlist, at subcommand precision (e.g.
    /// `systemctl is-active` without `systemctl stop`). Pairs with
    /// [`Self::command_forbidden_argv_prefixes`] (forbidden wins).
    ///
    /// Prefer this field to **extend** the surface with argv-precise families.
    /// Prefer [`Self::read_only_command_allowlist`] to **replace or narrow** the
    /// whole-executable base list. See `docs/macos-bash-sandbox.md`.
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub command_allowed_argv_prefixes: Option<Vec<String>>,
    /// Argv prefixes that are always denied (takes precedence over allowed
    /// prefixes and the read-only allowlist).
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub command_forbidden_argv_prefixes: Option<Vec<String>>,
    /// Optional **replacement** for the hardcoded `default_read_only_commands()`
    /// base used in `ReadOnly` bash mode. Whole-executable heads only (e.g.
    /// `cat`, `journalctl`) — not argv-prefix precise.
    ///
    /// Present **and non-empty** replaces the default base wholesale. Absent or
    /// empty is "no override" (keep the hardcoded default); empty must not
    /// become a deny-all surface.
    ///
    /// Unique vs [`Self::command_allowed_argv_prefixes`]: can **narrow** the
    /// default set (drop `sudo` / `curl`) without re-expressing every kept
    /// command as an argv prefix. See `docs/macos-bash-sandbox.md`.
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub read_only_command_allowlist: Option<Vec<String>>,
    pub command_network_mode: Option<String>,
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub cli_tool_names: Option<Vec<String>>,
    pub enable_meta_tools: Option<bool>,
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub allowed_mcp_service_ids: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub backgroundable_tool_names: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub approval_required_tools: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub subagent_targets: Option<Vec<String>>,
    pub subagent_spawn_enabled: Option<bool>,
    pub orchestration_enabled: Option<bool>,
    pub subagent_steering_enabled: Option<bool>,
    pub subagent_background_enabled: Option<bool>,
    pub subagent_default_await_mode: Option<String>,
    pub subagent_allow_cross_deployment: Option<bool>,
    pub cross_deployment_spawn_timeout_seconds: Option<i64>,
    pub enable_memory: Option<bool>,
    pub enable_session_history_tool: Option<bool>,
    pub enable_context_budget: Option<bool>,
    pub enable_defra_query: Option<bool>,
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub defra_query_collections: Option<Vec<String>>,
    #[serde(default, deserialize_with = "deserialize_optional_write_tools")]
    pub write_tools: Option<Vec<WriteToolDecl>>,
    /// Bare `surface_id` refs to same-agent `DatastoreToolSurface` docs.
    /// Expanded into create tools at snapshot build (fail-closed).
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub datastore_tool_surface_ids: Option<Vec<String>>,
    /// Self-configuration gate (#654): opt-in, never backfilled true.
    pub enable_self_config: Option<bool>,
    /// Self-config category allowlist; unset means the core spine
    /// (behavior, tools, profile). See `config_client::patch`.
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::deserialize_optional_string_vec"
    )]
    pub self_config_categories: Option<Vec<String>>,
    /// Opt-in guardrail: refuse self-config patches that would strip the
    /// agent's own reconfigure ability.
    pub self_config_no_lockout: Option<bool>,
    /// Opt-in guardrail: `get_my_config` accepts a patch preview.
    pub self_config_dry_run: Option<bool>,
}

/// Canonical per-principal id for the seeded `wide-open` preset. Prefixed with
/// the agent DID so it is globally unique AND passes the runtime document view's
/// `agent_did` hydration filter + cross-agent rejection (a single global preset
/// row would be invisible to other principals — see design §3.2).
pub fn wide_open_tool_selection_id_for_agent(agent_did: &str) -> String {
    format!("{agent_did}:wide-open")
}

/// The seeded `wide-open` preset for a principal: a `ToolSelection` that
/// reproduces today's permissive behavior, expressed explicitly and stamped at
/// the current policy version. Built by running the legacy-permissive backfill
/// over an empty document, so the preset value-set can never drift from the
/// backfill the secure-default flip relies on (single source of truth).
pub fn wide_open_tool_selection_document(agent_did: &str) -> ToolSelectionDocument {
    ToolSelectionDocument {
        selection_id: wide_open_tool_selection_id_for_agent(agent_did),
        agent_did: agent_did.to_string(),
        display_name: Some("Wide-open (permissive preset)".to_string()),
        ..Default::default()
    }
    .with_legacy_policy_defaults_backfilled()
}

impl ToolSelectionDocument {
    pub fn with_legacy_policy_defaults_backfilled(&self) -> Self {
        let mut backfilled = self.clone();
        if backfilled
            .tool_policy_version
            .as_deref()
            .map(str::trim)
            .is_some_and(|version| !version.is_empty())
        {
            return backfilled;
        }

        // The legacy default-TRUE capabilities: materialize them as `true` so a
        // backfilled V1 doc reproduces the historical permissive surface
        // bit-for-bit. `enable_meta_tools` and `enable_context_budget` are still
        // version-gated in `ToolSelection::from_document` via
        // `default_enabled(true)`; `enable_defra_query` is opt-in for every
        // policy version since #592, so this materialized `true` is the ONLY
        // thing keeping the wide-open preset (and legacy-doc upgrades)
        // defra_query-enabled. Omitting `enable_context_budget` here would
        // silently drop the (always-on) context-budget tool on the
        // secure-default flip.
        backfilled.enable_meta_tools.get_or_insert(true);
        backfilled.enable_defra_query.get_or_insert(true);
        backfilled.enable_context_budget.get_or_insert(true);
        backfilled.enable_file_tools.get_or_insert(false);
        backfilled.enable_bash.get_or_insert(false);
        backfilled.orchestration_enabled.get_or_insert(false);
        backfilled.subagent_spawn_enabled.get_or_insert(false);
        backfilled.subagent_steering_enabled.get_or_insert(false);
        backfilled.subagent_background_enabled.get_or_insert(false);
        backfilled
            .subagent_allow_cross_deployment
            .get_or_insert(false);
        backfilled.enable_memory.get_or_insert(false);
        backfilled.enable_session_history_tool.get_or_insert(false);
        backfilled.tool_policy_version = Some(TOOL_POLICY_V1.to_string());
        backfilled
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(targets) = &self.subagent_targets {
            for (i, target) in targets.iter().enumerate() {
                if target.is_empty() {
                    return Err(anyhow::anyhow!(
                        "subagent_targets[{}] is empty; each entry must be a valid SubagentTarget JSON object",
                        i
                    ));
                }
                // Every non-empty entry must be parseable as a SubagentTarget JSON
                // object AND pass structural validation (all fields non-empty).
                // Bare behavior-id strings are not valid — the runtime silently
                // drops them, which entrenches a silent misconfiguration. Reject
                // them here with a clear diagnostic.
                let parsed = SubagentTarget::parse(target).map_err(|e| {
                    anyhow::anyhow!(
                        "subagent_targets[{i}] is not a valid SubagentTarget JSON object \
                         (got {target:?}): {e}; \
                         use subagent_target_entry(name, agent_did, behavior_id, description) \
                         to build a valid entry"
                    )
                })?;
                if !parsed.is_structurally_valid() {
                    return Err(anyhow::anyhow!(
                        "subagent_targets[{i}] parsed as SubagentTarget but is not structurally \
                         valid (name, agent_did, and behavior_id must all be non-empty): {target:?}"
                    ));
                }
            }
        }
        if let Some(tool_names) = &self.backgroundable_tool_names {
            for (i, tool_name) in tool_names.iter().enumerate() {
                if tool_name.is_empty() {
                    return Err(anyhow::anyhow!(
                        "backgroundable_tool_names[{}] is empty; tool names must be non-empty strings",
                        i
                    ));
                }
            }
        }
        if let Some(tool_names) = &self.approval_required_tools {
            for (i, tool_name) in tool_names.iter().enumerate() {
                if tool_name.is_empty() {
                    return Err(anyhow::anyhow!(
                        "approval_required_tools[{}] is empty; tool names must be non-empty strings",
                        i
                    ));
                }
            }
        }
        if let Some(categories) = &self.self_config_categories {
            for (i, category) in categories.iter().enumerate() {
                if !crate::config_client::patch::SELF_CONFIG_CATEGORIES.contains(&category.as_str())
                {
                    return Err(anyhow::anyhow!(
                        "self_config_categories[{i}] is {category:?}; valid categories: {}",
                        crate::config_client::patch::SELF_CONFIG_CATEGORIES.join(", ")
                    ));
                }
            }
        }
        if let Some(mode) = self
            .subagent_default_await_mode
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            match mode {
                "foreground" => {}
                "background" if self.subagent_background_enabled.unwrap_or(false) => {}
                "background" => {
                    return Err(anyhow::anyhow!(
                        "subagent_default_await_mode cannot be background unless subagent_background_enabled is true"
                    ));
                }
                other => {
                    return Err(anyhow::anyhow!(
                        "subagent_default_await_mode must be foreground or background, got {other:?}"
                    ));
                }
            }
        }
        if let Some(decls) = &self.write_tools {
            // Sibling cli_tool_names are advertised as individually-named tools
            // in the same selection, so a write tool reusing one of those names
            // is the same dispatch collision as reusing a built-in name. (Other
            // categories: built-ins are covered by `is_reserved_builtin_tool_name`;
            // subagent targets are arguments to `spawn_subagent`, not tool names;
            // MCP/custom tools are runtime-discovered and guarded in
            // `ToolSurface::build_tools`.)
            let cli_tool_names: std::collections::HashSet<&str> = self
                .cli_tool_names
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|name| name.trim())
                .collect();
            let mut seen_tool_names = std::collections::HashSet::new();
            for (i, decl) in decls.iter().enumerate() {
                // A decl must name a non-empty tool AND target collection.
                // `is_well_formed()` is the single source of truth for that gate;
                // mirror it here so malformed decls fail validation loudly
                // instead of being silently dropped at registration.
                if !decl.is_well_formed() {
                    return Err(anyhow::anyhow!(
                        "write_tools[{i}] is malformed (tool_name and collection must both be \
                         non-empty): tool_name={:?}, collection={:?}",
                        decl.tool_name,
                        decl.collection
                    ));
                }
                // A declared write tool may not reuse a built-in tool name:
                // doing so silently shadows the built-in (see
                // `is_reserved_builtin_tool_name`).
                if is_reserved_builtin_tool_name(&decl.tool_name) {
                    return Err(anyhow::anyhow!(
                        "write_tools[{i}] tool_name {:?} collides with a built-in tool; declared \
                         write tools must use a name not already provided by the native, meta, \
                         subagent, or built-in (defra_query, context_budget, sessions, memory) \
                         tool surface",
                        decl.tool_name.trim()
                    ));
                }
                if cli_tool_names.contains(decl.tool_name.trim()) {
                    return Err(anyhow::anyhow!(
                        "write_tools[{i}] tool_name {:?} collides with a cli_tool_names entry in \
                         the same tool selection; each tool must have a unique name",
                        decl.tool_name.trim()
                    ));
                }
                let mut seen_field_names = std::collections::HashSet::new();
                for (j, field) in decl.fields.iter().enumerate() {
                    if field.name.trim().is_empty() {
                        return Err(anyhow::anyhow!(
                            "write_tools[{i}] (tool {:?}) has a field[{j}] with an empty name; \
                             every WriteToolField must have a non-empty name",
                            decl.tool_name
                        ));
                    }
                    if !seen_field_names.insert(field.name.trim()) {
                        return Err(anyhow::anyhow!(
                            "write_tools[{i}] (tool {:?}) has a duplicate field name {:?}; each \
                             WriteToolField in a declaration must have a unique name",
                            decl.tool_name,
                            field.name.trim()
                        ));
                    }
                }
                if !seen_tool_names.insert(decl.tool_name.trim()) {
                    return Err(anyhow::anyhow!(
                        "write_tools has a duplicate tool_name {:?}; each declared write tool \
                         must have a unique tool_name",
                        decl.tool_name.trim()
                    ));
                }
            }
        }
        Ok(())
    }
}

pub fn default_tool_selection_id_for_behavior(behavior_id: &str) -> String {
    format!("{behavior_id}-tools")
}

pub async fn load_tool_selection(
    node: &EmbeddedNode,
    selection_id: &str,
) -> Result<Option<ToolSelectionDocument>> {
    Ok(load_tool_selection_record(node, selection_id)
        .await?
        .map(|(_, selection)| selection))
}

pub(crate) async fn load_tool_selection_record(
    node: &EmbeddedNode,
    selection_id: &str,
) -> Result<Option<(String, ToolSelectionDocument)>> {
    let escaped_selection_id = escape_graphql_string(selection_id);
    let query = format!(
        r#"{{
            ToolSelection(
                filter: {{ selection_id: {{ _eq: "{escaped_selection_id}" }} }},
                limit: 1
            ) {{
                _docID
                selection_id
                agent_did
                display_name
                tool_policy_version
                enable_file_tools
                file_tools_mode
                file_tool_root
                enable_bash
                bash_mode
                command_execution_policy
                command_allowed_argv_prefixes
                command_forbidden_argv_prefixes
                read_only_command_allowlist
                command_network_mode
                cli_tool_names
                enable_meta_tools
                allowed_mcp_service_ids
                backgroundable_tool_names
                approval_required_tools
                subagent_targets
                subagent_spawn_enabled
                orchestration_enabled
                subagent_steering_enabled
                subagent_background_enabled
                subagent_default_await_mode
                subagent_allow_cross_deployment
                cross_deployment_spawn_timeout_seconds
                enable_memory
                enable_session_history_tool
                enable_context_budget
                enable_defra_query
                defra_query_collections
                write_tools
                datastore_tool_surface_ids
                enable_self_config
                self_config_categories
                self_config_no_lockout
                self_config_dry_run
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query ToolSelection failed: {:?}", resp.errors);
    }

    Ok(serde_helpers::first_row_with_doc_id(
        resp.data.as_ref(),
        "ToolSelection",
    ))
}

pub(crate) async fn load_tool_selection_by_doc_id(
    node: &EmbeddedNode,
    doc_id: &str,
) -> Result<Option<(String, ToolSelectionDocument)>> {
    let escaped_doc_id = escape_graphql_string(doc_id);
    let query = format!(
        r#"{{
            ToolSelection(
                filter: {{ _docID: {{ _eq: "{escaped_doc_id}" }} }},
                limit: 1
            ) {{
                _docID
                selection_id
                agent_did
                display_name
                tool_policy_version
                enable_file_tools
                file_tools_mode
                file_tool_root
                enable_bash
                bash_mode
                command_execution_policy
                command_allowed_argv_prefixes
                command_forbidden_argv_prefixes
                read_only_command_allowlist
                command_network_mode
                cli_tool_names
                enable_meta_tools
                allowed_mcp_service_ids
                backgroundable_tool_names
                approval_required_tools
                subagent_targets
                subagent_spawn_enabled
                orchestration_enabled
                subagent_steering_enabled
                subagent_background_enabled
                subagent_default_await_mode
                subagent_allow_cross_deployment
                cross_deployment_spawn_timeout_seconds
                enable_memory
                enable_session_history_tool
                enable_context_budget
                enable_defra_query
                defra_query_collections
                write_tools
                datastore_tool_surface_ids
                enable_self_config
                self_config_categories
                self_config_no_lockout
                self_config_dry_run
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("query ToolSelection by _docID failed: {:?}", resp.errors);
    }

    Ok(serde_helpers::first_row_with_doc_id(
        resp.data.as_ref(),
        "ToolSelection",
    ))
}

pub(crate) async fn list_tool_selection_records(
    node: &EmbeddedNode,
    agent_did: &str,
) -> Result<Vec<(String, ToolSelectionDocument)>> {
    let escaped_agent_did = escape_graphql_string(agent_did);
    let query = format!(
        r#"{{
            ToolSelection(
                filter: {{ agent_did: {{ _eq: "{escaped_agent_did}" }} }},
                order: {{ selection_id: ASC }}
            ) {{
                _docID
                selection_id
                agent_did
                display_name
                tool_policy_version
                enable_file_tools
                file_tools_mode
                file_tool_root
                enable_bash
                bash_mode
                command_execution_policy
                command_allowed_argv_prefixes
                command_forbidden_argv_prefixes
                read_only_command_allowlist
                command_network_mode
                cli_tool_names
                enable_meta_tools
                allowed_mcp_service_ids
                backgroundable_tool_names
                approval_required_tools
                subagent_targets
                subagent_spawn_enabled
                orchestration_enabled
                subagent_steering_enabled
                subagent_background_enabled
                subagent_default_await_mode
                subagent_allow_cross_deployment
                cross_deployment_spawn_timeout_seconds
                enable_memory
                enable_session_history_tool
                enable_context_budget
                enable_defra_query
                defra_query_collections
                write_tools
                datastore_tool_surface_ids
                enable_self_config
                self_config_categories
                self_config_no_lockout
                self_config_dry_run
            }}
        }}"#
    );

    let resp = node.execute(&query).await;
    if resp.has_errors() {
        anyhow::bail!("list ToolSelection failed: {:?}", resp.errors);
    }

    Ok(serde_helpers::rows_with_doc_id(
        resp.data.as_ref(),
        "ToolSelection",
    ))
}

pub(crate) async fn list_all_tool_selection_records(
    node: &EmbeddedNode,
) -> Result<Vec<(String, ToolSelectionDocument)>> {
    let query = r#"{
            ToolSelection(order: { selection_id: ASC }) {
                _docID
                selection_id
                agent_did
                display_name
                tool_policy_version
                enable_file_tools
                file_tools_mode
                file_tool_root
                enable_bash
                bash_mode
                command_execution_policy
                command_allowed_argv_prefixes
                command_forbidden_argv_prefixes
                read_only_command_allowlist
                command_network_mode
                cli_tool_names
                enable_meta_tools
                allowed_mcp_service_ids
                backgroundable_tool_names
                approval_required_tools
                subagent_targets
                subagent_spawn_enabled
                orchestration_enabled
                subagent_steering_enabled
                subagent_background_enabled
                subagent_default_await_mode
                subagent_allow_cross_deployment
                cross_deployment_spawn_timeout_seconds
                enable_memory
                enable_session_history_tool
                enable_context_budget
                enable_defra_query
                defra_query_collections
                write_tools
                datastore_tool_surface_ids
                enable_self_config
                self_config_categories
                self_config_no_lockout
                self_config_dry_run
            }
        }"#;

    let resp = node.execute(query).await;
    if resp.has_errors() {
        anyhow::bail!("list all ToolSelection failed: {:?}", resp.errors);
    }

    Ok(serde_helpers::rows_with_doc_id(
        resp.data.as_ref(),
        "ToolSelection",
    ))
}

pub async fn upsert_tool_selection(
    node: &EmbeddedNode,
    selection: &ToolSelectionDocument,
) -> Result<()> {
    let escaped_selection_id = escape_graphql_string(&selection.selection_id);
    let escaped_agent_did = escape_graphql_string(&selection.agent_did);

    let add_fields = vec![
        Some(format!(r#"selection_id: "{escaped_selection_id}""#)),
        Some(format!(r#"agent_did: "{escaped_agent_did}""#)),
        graphql_fields::graphql_string_field("display_name", selection.display_name.as_deref()),
        graphql_fields::graphql_string_field(
            "tool_policy_version",
            selection.tool_policy_version.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "enable_file_tools",
            selection.enable_file_tools,
        ),
        graphql_fields::graphql_string_field(
            "file_tools_mode",
            selection.file_tools_mode.as_deref(),
        ),
        Some(graphql_fields::graphql_nullable_string_field(
            "file_tool_root",
            selection.file_tool_root.as_deref(),
        )),
        graphql_fields::graphql_optional_bool_field("enable_bash", selection.enable_bash),
        graphql_fields::graphql_string_field("bash_mode", selection.bash_mode.as_deref()),
        graphql_fields::graphql_string_field(
            "command_execution_policy",
            selection.command_execution_policy.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "command_allowed_argv_prefixes",
            selection.command_allowed_argv_prefixes.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "command_forbidden_argv_prefixes",
            selection.command_forbidden_argv_prefixes.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "read_only_command_allowlist",
            selection.read_only_command_allowlist.as_deref(),
        ),
        graphql_fields::graphql_string_field(
            "command_network_mode",
            selection.command_network_mode.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "cli_tool_names",
            selection.cli_tool_names.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "enable_meta_tools",
            selection.enable_meta_tools,
        ),
        graphql_fields::graphql_string_list_field(
            "allowed_mcp_service_ids",
            selection.allowed_mcp_service_ids.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "backgroundable_tool_names",
            selection.backgroundable_tool_names.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "approval_required_tools",
            selection.approval_required_tools.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "subagent_targets",
            selection.subagent_targets.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "subagent_spawn_enabled",
            selection.subagent_spawn_enabled,
        ),
        graphql_fields::graphql_optional_bool_field(
            "orchestration_enabled",
            selection.orchestration_enabled,
        ),
        graphql_fields::graphql_optional_bool_field(
            "subagent_steering_enabled",
            selection.subagent_steering_enabled,
        ),
        graphql_fields::graphql_optional_bool_field(
            "subagent_background_enabled",
            selection.subagent_background_enabled,
        ),
        graphql_fields::graphql_string_field(
            "subagent_default_await_mode",
            selection.subagent_default_await_mode.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "subagent_allow_cross_deployment",
            selection.subagent_allow_cross_deployment,
        ),
        selection
            .cross_deployment_spawn_timeout_seconds
            .map(|value| format!("cross_deployment_spawn_timeout_seconds: {value}")),
        graphql_fields::graphql_optional_bool_field("enable_memory", selection.enable_memory),
        graphql_fields::graphql_optional_bool_field(
            "enable_session_history_tool",
            selection.enable_session_history_tool,
        ),
        graphql_fields::graphql_optional_bool_field(
            "enable_context_budget",
            selection.enable_context_budget,
        ),
        graphql_fields::graphql_optional_bool_field(
            "enable_defra_query",
            selection.enable_defra_query,
        ),
        graphql_fields::graphql_string_list_field(
            "defra_query_collections",
            selection.defra_query_collections.as_deref(),
        ),
        graphql_write_tools_field(selection.write_tools.as_deref()),
        graphql_fields::graphql_string_list_field(
            "datastore_tool_surface_ids",
            selection.datastore_tool_surface_ids.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "enable_self_config",
            selection.enable_self_config,
        ),
        graphql_fields::graphql_string_list_field(
            "self_config_categories",
            selection.self_config_categories.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "self_config_no_lockout",
            selection.self_config_no_lockout,
        ),
        graphql_fields::graphql_optional_bool_field(
            "self_config_dry_run",
            selection.self_config_dry_run,
        ),
        Some(format!(
            r#"updated_at: "{}""#,
            escape_graphql_string(&mint_recreate_identity_timestamp())
        )),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(",\n                    ");

    let update_fields = vec![
        Some(format!(r#"agent_did: "{escaped_agent_did}""#)),
        graphql_fields::graphql_string_field("display_name", selection.display_name.as_deref()),
        graphql_fields::graphql_string_field(
            "tool_policy_version",
            selection.tool_policy_version.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "enable_file_tools",
            selection.enable_file_tools,
        ),
        graphql_fields::graphql_string_field(
            "file_tools_mode",
            selection.file_tools_mode.as_deref(),
        ),
        Some(graphql_fields::graphql_nullable_string_field(
            "file_tool_root",
            selection.file_tool_root.as_deref(),
        )),
        graphql_fields::graphql_optional_bool_field("enable_bash", selection.enable_bash),
        graphql_fields::graphql_string_field("bash_mode", selection.bash_mode.as_deref()),
        graphql_fields::graphql_string_field(
            "command_execution_policy",
            selection.command_execution_policy.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "command_allowed_argv_prefixes",
            selection.command_allowed_argv_prefixes.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "command_forbidden_argv_prefixes",
            selection.command_forbidden_argv_prefixes.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "read_only_command_allowlist",
            selection.read_only_command_allowlist.as_deref(),
        ),
        graphql_fields::graphql_string_field(
            "command_network_mode",
            selection.command_network_mode.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "cli_tool_names",
            selection.cli_tool_names.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "enable_meta_tools",
            selection.enable_meta_tools,
        ),
        graphql_fields::graphql_string_list_field(
            "allowed_mcp_service_ids",
            selection.allowed_mcp_service_ids.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "backgroundable_tool_names",
            selection.backgroundable_tool_names.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "approval_required_tools",
            selection.approval_required_tools.as_deref(),
        ),
        graphql_fields::graphql_string_list_field(
            "subagent_targets",
            selection.subagent_targets.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "subagent_spawn_enabled",
            selection.subagent_spawn_enabled,
        ),
        graphql_fields::graphql_optional_bool_field(
            "orchestration_enabled",
            selection.orchestration_enabled,
        ),
        graphql_fields::graphql_optional_bool_field(
            "subagent_steering_enabled",
            selection.subagent_steering_enabled,
        ),
        graphql_fields::graphql_optional_bool_field(
            "subagent_background_enabled",
            selection.subagent_background_enabled,
        ),
        graphql_fields::graphql_string_field(
            "subagent_default_await_mode",
            selection.subagent_default_await_mode.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "subagent_allow_cross_deployment",
            selection.subagent_allow_cross_deployment,
        ),
        selection
            .cross_deployment_spawn_timeout_seconds
            .map(|value| format!("cross_deployment_spawn_timeout_seconds: {value}")),
        graphql_fields::graphql_optional_bool_field("enable_memory", selection.enable_memory),
        graphql_fields::graphql_optional_bool_field(
            "enable_session_history_tool",
            selection.enable_session_history_tool,
        ),
        graphql_fields::graphql_optional_bool_field(
            "enable_context_budget",
            selection.enable_context_budget,
        ),
        graphql_fields::graphql_optional_bool_field(
            "enable_defra_query",
            selection.enable_defra_query,
        ),
        graphql_fields::graphql_string_list_field(
            "defra_query_collections",
            selection.defra_query_collections.as_deref(),
        ),
        graphql_write_tools_field(selection.write_tools.as_deref()),
        graphql_fields::graphql_string_list_field(
            "datastore_tool_surface_ids",
            selection.datastore_tool_surface_ids.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "enable_self_config",
            selection.enable_self_config,
        ),
        graphql_fields::graphql_string_list_field(
            "self_config_categories",
            selection.self_config_categories.as_deref(),
        ),
        graphql_fields::graphql_optional_bool_field(
            "self_config_no_lockout",
            selection.self_config_no_lockout,
        ),
        graphql_fields::graphql_optional_bool_field(
            "self_config_dry_run",
            selection.self_config_dry_run,
        ),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(",\n                    ");

    let mutation = format!(
        r#"mutation {{
            upsert_ToolSelection(
                filter: {{ selection_id: {{ _eq: "{escaped_selection_id}" }} }},
                add: {{
                    {add_fields}
                }},
                update: {{
                    {update_fields}
                }}
            ) {{ _docID }}
        }}"#
    );

    let resp = execute_graphql_with_conflict_retry(node, &mutation, "upsert ToolSelection").await;
    if resp.has_errors() {
        anyhow::bail!("upsert ToolSelection failed: {:?}", resp.errors);
    }
    Ok(())
}
