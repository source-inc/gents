use std::collections::{BTreeMap, BTreeSet};

use crate::defra_query::CollectionScope;
use crate::document_config::{QueryToolDecl, SubagentTarget, WriteToolDecl, WriteToolField};
use crate::toolset::{CommandExecutionMode, CommandNetworkMode};

use super::modes::{BashMode, FileToolMode};
use super::selection::{SubagentToolConfig, ToolSelection};

pub const TOOL_POLICY_V1: &str = "tool-policy/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolPolicyVersion {
    LegacyDefaults,
    V1,
}

impl ToolPolicyVersion {
    pub fn parse(value: Option<&str>) -> anyhow::Result<Self> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            None | Some("legacy") | Some("legacy-permissive") => Ok(Self::LegacyDefaults),
            Some(TOOL_POLICY_V1) => Ok(Self::V1),
            Some(other) => anyhow::bail!("unknown tool policy version {other:?}"),
        }
    }

    pub fn default_enabled(self, legacy_default: bool) -> bool {
        match self {
            Self::LegacyDefaults => legacy_default,
            Self::V1 => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointScope<K, V> {
    None,
    Only(BTreeMap<K, V>),
    All,
}

impl<K, V> EndpointScope<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn none() -> Self {
        Self::None
    }

    pub fn all() -> Self {
        Self::All
    }

    pub fn only_units<I>(keys: I) -> EndpointScope<K, ()>
    where
        I: IntoIterator<Item = K>,
    {
        EndpointScope::Only(keys.into_iter().map(|key| (key, ())).collect())
    }

    pub fn only_map(map: BTreeMap<K, V>) -> Self {
        Self::Only(map)
    }

    pub fn is_deny_all(&self) -> bool {
        match self {
            Self::None => true,
            Self::Only(keys) => keys.is_empty(),
            Self::All => false,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Only(_) => "only",
            Self::All => "all",
        }
    }

    pub fn keys(&self) -> Vec<K> {
        match self {
            Self::Only(keys) => keys.keys().cloned().collect(),
            Self::None | Self::All => Vec::new(),
        }
    }

    pub fn permits(&self, key: &K) -> bool {
        match self {
            Self::None => false,
            Self::Only(keys) => keys.contains_key(key),
            Self::All => true,
        }
    }

    pub fn lookup(&self, key: &K) -> Option<&V> {
        match self {
            Self::Only(keys) => keys.get(key),
            Self::None | Self::All => None,
        }
    }

    pub fn meet_with<F>(&self, other: &Self, meet_value: F) -> Self
    where
        F: Fn(&V, &V) -> V,
    {
        match (self, other) {
            (Self::None, _) | (_, Self::None) => Self::None,
            (Self::All, right) => right.clone(),
            (left, Self::All) => left.clone(),
            (Self::Only(left), Self::Only(right)) => {
                let mut out = BTreeMap::new();
                for (key, left_value) in left {
                    if let Some(right_value) = right.get(key) {
                        out.insert(key.clone(), meet_value(left_value, right_value));
                    }
                }
                Self::Only(out)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPolicyBash {
    pub tool: BashMode,
    pub execution_mode: CommandExecutionMode,
    pub network_mode: CommandNetworkMode,
    pub sandbox: bool,
    pub allowed_argv_prefixes: EndpointScope<Vec<String>, ()>,
    /// Argv prefixes forbidden in any mode. A plain set with a **union** meet:
    /// a prefix forbidden by either the behavior or the ceiling is forbidden in
    /// the effective policy (top = ∅ = nothing forbidden). Mirrors Lean
    /// `BashPolicy.forbidden` (`bash_meet_forbidden_superset`).
    pub forbidden_argv_prefixes: BTreeSet<Vec<String>>,
    /// Command heads permitted in read-only mode, an `EndpointScope` with an
    /// **intersection** meet (top = `All`). Mirrors Lean `BashPolicy.readOnly`
    /// (`bash_meet_readonly_*`).
    pub read_only_allowlist: EndpointScope<String, ()>,
    /// Overlay `git_worktree_diff`: union meet (either side denies Git-metadata writes).
    pub deny_git_metadata_writes: bool,
}

impl ToolPolicyBash {
    pub fn off() -> Self {
        Self {
            tool: BashMode::Off,
            execution_mode: CommandExecutionMode::ReadOnly,
            network_mode: CommandNetworkMode::Inherit,
            sandbox: true,
            allowed_argv_prefixes: EndpointScope::<Vec<String>, ()>::all(),
            forbidden_argv_prefixes: BTreeSet::new(),
            read_only_allowlist: EndpointScope::all(),
            deny_git_metadata_writes: false,
        }
    }

    pub fn unrestricted() -> Self {
        Self {
            tool: BashMode::Unrestricted,
            execution_mode: CommandExecutionMode::Unrestricted,
            network_mode: CommandNetworkMode::Enabled,
            sandbox: true,
            allowed_argv_prefixes: EndpointScope::<Vec<String>, ()>::all(),
            forbidden_argv_prefixes: BTreeSet::new(),
            read_only_allowlist: EndpointScope::all(),
            deny_git_metadata_writes: false,
        }
    }

    fn meet(&self, other: &Self) -> Self {
        Self {
            tool: meet_bash_mode(self.tool, other.tool),
            execution_mode: meet_execution_mode(self.execution_mode, other.execution_mode),
            network_mode: meet_network_mode(self.network_mode, other.network_mode),
            sandbox: self.sandbox && other.sandbox,
            allowed_argv_prefixes: self
                .allowed_argv_prefixes
                .meet_with(&other.allowed_argv_prefixes, |(), ()| ()),
            forbidden_argv_prefixes: self
                .forbidden_argv_prefixes
                .union(&other.forbidden_argv_prefixes)
                .cloned()
                .collect(),
            read_only_allowlist: self
                .read_only_allowlist
                .meet_with(&other.read_only_allowlist, |(), ()| ()),
            deny_git_metadata_writes: self.deny_git_metadata_writes
                || other.deny_git_metadata_writes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPolicySurface {
    pub file: FileToolMode,
    pub bash: ToolPolicyBash,
    pub meta: bool,
    pub defra_query: bool,
    pub self_config: bool,
    pub memory: bool,
    pub session_history: bool,
    pub context_budget: bool,
    pub spawn: bool,
    pub steering: bool,
    pub background: bool,
    pub cross_deployment: bool,
    pub skills: bool,
    pub lsp: bool,
    pub cli_tools: EndpointScope<String, BTreeSet<String>>,
    pub mcp_services: EndpointScope<String, ()>,
    pub defra_collections: EndpointScope<String, ()>,
    pub self_config_categories: EndpointScope<String, ()>,
    pub subagent_targets: EndpointScope<(String, String), ()>,
    pub background_tools: EndpointScope<String, ()>,
    pub write_tools: EndpointScope<(String, String), BTreeSet<String>>,
    pub query_tools: EndpointScope<(String, String), BTreeSet<String>>,
}

impl ToolPolicySurface {
    pub fn secure_minimal() -> Self {
        Self {
            file: FileToolMode::Off,
            bash: ToolPolicyBash::off(),
            meta: false,
            defra_query: false,
            self_config: false,
            memory: false,
            session_history: false,
            context_budget: false,
            spawn: false,
            steering: false,
            background: false,
            cross_deployment: false,
            skills: false,
            lsp: false,
            cli_tools: EndpointScope::none(),
            mcp_services: EndpointScope::none(),
            defra_collections: EndpointScope::none(),
            self_config_categories: EndpointScope::none(),
            subagent_targets: EndpointScope::none(),
            background_tools: EndpointScope::none(),
            write_tools: EndpointScope::none(),
            query_tools: EndpointScope::none(),
        }
    }

    pub fn legacy_non_host_wide(file: FileToolMode, bash: BashMode) -> Self {
        Self {
            file,
            bash: ToolPolicyBash {
                tool: bash,
                execution_mode: match bash {
                    BashMode::Off | BashMode::ReadOnly => CommandExecutionMode::ReadOnly,
                    BashMode::Unrestricted => CommandExecutionMode::Unrestricted,
                },
                network_mode: CommandNetworkMode::Enabled,
                sandbox: true,
                allowed_argv_prefixes: EndpointScope::<Vec<String>, ()>::all(),
                forbidden_argv_prefixes: BTreeSet::new(),
                read_only_allowlist: EndpointScope::all(),
                deny_git_metadata_writes: false,
            },
            meta: true,
            defra_query: true,
            self_config: true,
            memory: true,
            session_history: true,
            context_budget: true,
            spawn: true,
            steering: true,
            background: true,
            cross_deployment: true,
            skills: true,
            lsp: true,
            cli_tools: EndpointScope::Only(BTreeMap::new()),
            mcp_services: EndpointScope::all(),
            defra_collections: EndpointScope::all(),
            self_config_categories: EndpointScope::all(),
            subagent_targets: EndpointScope::all(),
            background_tools: EndpointScope::all(),
            write_tools: EndpointScope::all(),
            query_tools: EndpointScope::all(),
        }
    }

    pub fn runtime_all() -> Self {
        Self {
            file: FileToolMode::ReadWrite,
            bash: ToolPolicyBash::unrestricted(),
            meta: true,
            defra_query: true,
            self_config: true,
            memory: true,
            session_history: true,
            context_budget: true,
            spawn: true,
            steering: true,
            background: true,
            cross_deployment: true,
            skills: true,
            lsp: true,
            cli_tools: EndpointScope::all(),
            mcp_services: EndpointScope::all(),
            defra_collections: EndpointScope::all(),
            self_config_categories: EndpointScope::all(),
            subagent_targets: EndpointScope::all(),
            background_tools: EndpointScope::all(),
            write_tools: EndpointScope::all(),
            query_tools: EndpointScope::all(),
        }
    }

    pub(crate) fn from_selection(
        selection: &ToolSelection,
        subagent_tools: &SubagentToolConfig,
    ) -> Self {
        let command_policy = selection.command_policy.as_ref();
        let allowed_argv_prefixes = command_policy
            .map(|policy| policy.allowed_argv_prefixes.clone())
            .unwrap_or_default();
        let forbidden_argv_prefixes = command_policy
            .map(|policy| policy.forbidden_argv_prefixes.clone())
            .unwrap_or_default();
        let read_only_allowlist = command_policy
            .map(|policy| policy.read_only_allowlist().to_vec())
            .unwrap_or_default();

        let mut cli_tools = BTreeMap::new();
        for name in &selection.cli_tool_names {
            cli_tools.insert(name.trim().to_string(), BTreeSet::new());
        }

        let mcp_services = if !selection.enable_meta_tools {
            EndpointScope::none()
        } else if selection.allowed_mcp_service_ids.is_empty() {
            EndpointScope::all()
        } else {
            EndpointScope::<String, ()>::only_units(
                selection
                    .allowed_mcp_service_ids
                    .iter()
                    .map(|service| service.trim().to_string()),
            )
        };

        let defra_collections = if !selection.enable_defra_query {
            EndpointScope::none()
        } else if selection.defra_query_collections.is_empty() {
            EndpointScope::all()
        } else {
            EndpointScope::<String, ()>::only_units(
                crate::defra_query::expand_collection_scope_aliases(
                    selection.defra_query_collections.iter().map(String::as_str),
                ),
            )
        };

        let self_config_categories = if !selection.enable_self_config {
            EndpointScope::none()
        } else {
            match &selection.self_config_categories {
                None => EndpointScope::<String, ()>::only_units(
                    crate::config_client::patch::DEFAULT_SELF_CONFIG_CATEGORIES
                        .iter()
                        .map(|category| category.to_string()),
                ),
                Some(categories) => EndpointScope::<String, ()>::only_units(
                    categories
                        .iter()
                        .map(|category| category.trim().to_string()),
                ),
            }
        };

        Self {
            file: selection.file_tools,
            bash: ToolPolicyBash {
                tool: selection.bash,
                execution_mode: command_policy
                    .map(|policy| policy.mode)
                    .unwrap_or(match selection.bash {
                        BashMode::Off | BashMode::ReadOnly => CommandExecutionMode::ReadOnly,
                        BashMode::Unrestricted => CommandExecutionMode::Unrestricted,
                    }),
                network_mode: command_policy
                    .map(|policy| policy.network_mode)
                    .unwrap_or(CommandNetworkMode::Inherit),
                sandbox: true,
                allowed_argv_prefixes: if allowed_argv_prefixes.is_empty() {
                    EndpointScope::<Vec<String>, ()>::all()
                } else {
                    EndpointScope::<Vec<String>, ()>::only_units(allowed_argv_prefixes)
                },
                forbidden_argv_prefixes: forbidden_argv_prefixes.into_iter().collect(),
                read_only_allowlist: if read_only_allowlist.is_empty() {
                    EndpointScope::all()
                } else {
                    EndpointScope::<String, ()>::only_units(
                        read_only_allowlist
                            .into_iter()
                            .map(|cmd| cmd.trim().to_string()),
                    )
                },
                deny_git_metadata_writes: command_policy
                    .map(|policy| policy.deny_git_metadata_writes())
                    .unwrap_or(false),
            },
            meta: selection.enable_meta_tools,
            defra_query: selection.enable_defra_query,
            self_config: selection.enable_self_config,
            memory: selection.enable_memory,
            session_history: selection.enable_session_history_tool,
            context_budget: selection.enable_context_budget,
            spawn: subagent_tools.spawn_enabled,
            steering: subagent_tools.steering_enabled,
            background: subagent_tools.background_enabled,
            cross_deployment: subagent_tools.allow_cross_deployment,
            skills: true,
            lsp: selection.enable_lsp && !matches!(selection.file_tools, FileToolMode::Off),
            cli_tools: EndpointScope::only_map(cli_tools),
            mcp_services,
            defra_collections,
            self_config_categories,
            subagent_targets: EndpointScope::<(String, String), ()>::only_units(
                subagent_tools.targets.iter().map(subagent_target_key),
            ),
            background_tools: EndpointScope::<String, ()>::only_units(
                selection
                    .backgroundable_tool_names
                    .iter()
                    .map(|name| name.trim().to_string()),
            ),
            write_tools: write_scope_from_decls(&selection.write_tools),
            query_tools: query_scope_from_decls(&selection.query_tools),
        }
    }

    pub fn meet(&self, other: &Self) -> Self {
        Self {
            file: meet_file_mode(self.file, other.file),
            bash: self.bash.meet(&other.bash),
            meta: self.meta && other.meta,
            defra_query: self.defra_query && other.defra_query,
            self_config: self.self_config && other.self_config,
            memory: self.memory && other.memory,
            session_history: self.session_history && other.session_history,
            context_budget: self.context_budget && other.context_budget,
            spawn: self.spawn && other.spawn,
            steering: self.steering && other.steering,
            background: self.background && other.background,
            cross_deployment: self.cross_deployment && other.cross_deployment,
            skills: self.skills && other.skills,
            lsp: self.lsp && other.lsp,
            cli_tools: self.cli_tools.meet_with(&other.cli_tools, |left, right| {
                left.intersection(right).cloned().collect()
            }),
            mcp_services: self
                .mcp_services
                .meet_with(&other.mcp_services, |(), ()| ()),
            defra_collections: self
                .defra_collections
                .meet_with(&other.defra_collections, |(), ()| ()),
            self_config_categories: self
                .self_config_categories
                .meet_with(&other.self_config_categories, |(), ()| ()),
            subagent_targets: self
                .subagent_targets
                .meet_with(&other.subagent_targets, |(), ()| ()),
            background_tools: self
                .background_tools
                .meet_with(&other.background_tools, |(), ()| ()),
            write_tools: self
                .write_tools
                .meet_with(&other.write_tools, |left, right| {
                    left.intersection(right).cloned().collect()
                }),
            query_tools: self
                .query_tools
                .meet_with(&other.query_tools, |left, right| {
                    left.intersection(right).cloned().collect()
                }),
        }
    }

    pub fn effective(behavior: &Self, ceiling: &Self, runtime: &Self) -> Self {
        behavior.meet(ceiling).meet(runtime)
    }

    pub fn include_meta_tools(&self) -> bool {
        self.meta && !self.mcp_services.is_deny_all()
    }

    pub fn include_defra_query(&self) -> bool {
        self.defra_query && !self.defra_collections.is_deny_all()
    }

    pub fn defra_query_collection_scope(&self) -> CollectionScope {
        match &self.defra_collections {
            EndpointScope::All => CollectionScope::all(),
            EndpointScope::None => CollectionScope::none(),
            EndpointScope::Only(keys) if keys.is_empty() => CollectionScope::none(),
            EndpointScope::Only(_) => CollectionScope::restricted(self.defra_collections.keys()),
        }
    }

    pub fn include_self_config(&self) -> bool {
        self.self_config && !self.self_config_categories.is_deny_all()
    }

    pub fn self_config_category_set(&self) -> std::collections::BTreeSet<String> {
        match &self.self_config_categories {
            EndpointScope::All => crate::config_client::patch::SELF_CONFIG_CATEGORIES
                .iter()
                .map(|category| category.to_string())
                .collect(),
            EndpointScope::None => std::collections::BTreeSet::new(),
            EndpointScope::Only(_) => self.self_config_categories.keys().into_iter().collect(),
        }
    }

    pub fn mcp_service_ids_for_runtime(&self) -> Vec<String> {
        match &self.mcp_services {
            EndpointScope::Only(_) => self.mcp_services.keys(),
            EndpointScope::None | EndpointScope::All => Vec::new(),
        }
    }

    pub fn defra_query_collections_for_runtime(&self) -> Vec<String> {
        match &self.defra_collections {
            EndpointScope::Only(_) => self.defra_collections.keys(),
            EndpointScope::None | EndpointScope::All => Vec::new(),
        }
    }

    pub fn write_decls_for_runtime(&self, decls: &[WriteToolDecl]) -> Vec<WriteToolDecl> {
        decls
            .iter()
            .filter_map(|decl| {
                let key = (
                    decl.tool_name.trim().to_string(),
                    decl.collection.trim().to_string(),
                );
                match &self.write_tools {
                    EndpointScope::None => None,
                    EndpointScope::All => Some(decl.clone()),
                    EndpointScope::Only(grants) => grants.get(&key).map(|fields| {
                        let mut narrowed = decl.clone();
                        narrowed.fields = decl
                            .fields
                            .iter()
                            .filter(|field| fields.contains(field.name.trim()))
                            .cloned()
                            .collect::<Vec<WriteToolField>>();
                        narrowed
                    }),
                }
            })
            .collect()
    }

    pub fn query_decls_for_runtime(&self, decls: &[QueryToolDecl]) -> Vec<QueryToolDecl> {
        decls
            .iter()
            .filter_map(|decl| {
                let key = (
                    decl.tool_name.trim().to_string(),
                    decl.collection.trim().to_string(),
                );
                match &self.query_tools {
                    EndpointScope::None => None,
                    EndpointScope::All => Some(decl.clone()),
                    EndpointScope::Only(grants) => grants.get(&key).and_then(|fields| {
                        // Dropping a runtime-filled filter would un-scope the
                        // read (fail-open). Drop the whole tool instead.
                        if decl.filter_fields.iter().any(|field| {
                            field.fill.is_some() && !fields.contains(field.name.trim())
                        }) {
                            return None;
                        }
                        let mut narrowed = decl.clone();
                        narrowed.fields = decl
                            .fields
                            .iter()
                            .filter(|field| fields.contains(field.trim()))
                            .cloned()
                            .collect();
                        narrowed.filter_fields = decl
                            .filter_fields
                            .iter()
                            .filter(|field| fields.contains(field.name.trim()))
                            .cloned()
                            .collect();
                        Some(narrowed)
                    }),
                }
            })
            .filter(|decl| decl.is_well_formed())
            .collect()
    }

    pub fn filter_background_tools(&self, names: Vec<String>) -> Vec<String> {
        filter_unit_scope(names, &self.background_tools)
    }

    pub fn filter_cli_names(&self, names: Vec<String>) -> Vec<String> {
        match &self.cli_tools {
            EndpointScope::None => Vec::new(),
            EndpointScope::All => names,
            EndpointScope::Only(tools) => names
                .into_iter()
                .filter(|name| tools.contains_key(name.trim()))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeToolAvailability {
    pub policy: ToolPolicySurface,
}

impl RuntimeToolAvailability {
    pub fn all() -> Self {
        Self {
            policy: ToolPolicySurface::runtime_all(),
        }
    }

    pub fn from_online_mcp_services<I, S>(service_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let service_ids = service_ids
            .into_iter()
            .map(Into::into)
            .map(|service| service.trim().to_string())
            .filter(|service| !service.is_empty())
            .collect::<BTreeSet<_>>();
        let mut policy = ToolPolicySurface::runtime_all();
        policy.mcp_services = if service_ids.is_empty() {
            EndpointScope::none()
        } else {
            EndpointScope::<String, ()>::only_units(service_ids)
        };
        Self { policy }
    }

    pub fn for_mcp_presence(mcp_services_online: bool) -> Self {
        let mut policy = ToolPolicySurface::runtime_all();
        policy.mcp_services = if mcp_services_online {
            EndpointScope::all()
        } else {
            EndpointScope::none()
        };
        Self { policy }
    }
}

fn filter_unit_scope<K>(names: Vec<K>, scope: &EndpointScope<K, ()>) -> Vec<K>
where
    K: Ord + Clone,
{
    match scope {
        EndpointScope::None => Vec::new(),
        EndpointScope::All => names,
        EndpointScope::Only(keys) => names
            .into_iter()
            .filter(|name| keys.contains_key(name))
            .collect(),
    }
}

fn write_scope_from_decls(
    write_tools: &[WriteToolDecl],
) -> EndpointScope<(String, String), BTreeSet<String>> {
    let mut grants = BTreeMap::new();
    for decl in write_tools {
        let fields = decl
            .fields
            .iter()
            .map(|field| field.name.trim().to_string())
            .filter(|field| !field.is_empty())
            .collect::<BTreeSet<_>>();
        grants.insert(
            (
                decl.tool_name.trim().to_string(),
                decl.collection.trim().to_string(),
            ),
            fields,
        );
    }
    EndpointScope::Only(grants)
}

fn query_scope_from_decls(
    query_tools: &[QueryToolDecl],
) -> EndpointScope<(String, String), BTreeSet<String>> {
    let mut grants = BTreeMap::new();
    for decl in query_tools {
        let mut fields = decl
            .fields
            .iter()
            .map(|field| field.trim().to_string())
            .filter(|field| !field.is_empty())
            .collect::<BTreeSet<_>>();
        fields.extend(
            decl.filter_fields
                .iter()
                .map(|field| field.name.trim().to_string())
                .filter(|field| !field.is_empty()),
        );
        grants.insert(
            (
                decl.tool_name.trim().to_string(),
                decl.collection.trim().to_string(),
            ),
            fields,
        );
    }
    EndpointScope::Only(grants)
}

fn subagent_target_key(target: &SubagentTarget) -> (String, String) {
    (
        target.agent_did.trim().to_string(),
        target.behavior_id.trim().to_string(),
    )
}

fn meet_file_mode(left: FileToolMode, right: FileToolMode) -> FileToolMode {
    if left.rank() <= right.rank() {
        left
    } else {
        right
    }
}

fn meet_bash_mode(left: BashMode, right: BashMode) -> BashMode {
    if left.rank() <= right.rank() {
        left
    } else {
        right
    }
}

pub(super) fn meet_execution_mode(
    left: CommandExecutionMode,
    right: CommandExecutionMode,
) -> CommandExecutionMode {
    left.meet(right)
}

pub(super) fn meet_network_mode(
    left: CommandNetworkMode,
    right: CommandNetworkMode,
) -> CommandNetworkMode {
    if network_mode_rank(left) <= network_mode_rank(right) {
        left
    } else {
        right
    }
}

fn network_mode_rank(mode: CommandNetworkMode) -> u8 {
    match mode {
        CommandNetworkMode::Disabled => 0,
        CommandNetworkMode::Inherit => 1,
        CommandNetworkMode::Enabled => 2,
    }
}
