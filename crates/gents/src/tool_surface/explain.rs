use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::Serialize;

use crate::defra_query::DEFRA_QUERY_TOOL_NAME;
use crate::meta_tools::META_TOOL_NAMES;
use crate::toolset::{
    background_tool_names, subagent_tool_names, CONTEXT_BUDGET_TOOL_NAME, SESSION_HISTORY_TOOL_NAME,
};

use super::{BehaviorToolConfig, RuntimeToolAvailability, ToolPolicySurface, ToolSurface};

const MEMORY_TOOL_NAME: &str = "memory";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ToolSurfaceExplanation {
    pub tool_names: Vec<String>,
    pub policy: ToolSurfacePolicyTrace,
    pub included: BTreeMap<String, Vec<String>>,
    pub excluded: BTreeMap<String, Vec<String>>,
    pub unavailable: BTreeMap<String, Vec<String>>,
    pub warnings: Vec<ToolSurfaceWarning>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ToolSurfaceWarning {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct ToolSurfacePolicyTrace {
    pub requested: BTreeMap<String, Vec<String>>,
    pub ceiling: BTreeMap<String, Vec<String>>,
    pub runtime: BTreeMap<String, Vec<String>>,
    pub effective: BTreeMap<String, Vec<String>>,
}

impl ToolSurfaceExplanation {
    #[allow(dead_code)]
    pub(crate) fn from_resolved(
        config: &BehaviorToolConfig,
        surface: &ToolSurface,
    ) -> ToolSurfaceExplanation {
        Self::from_resolved_with_runtime(config, &RuntimeToolAvailability::all(), surface)
    }

    pub(crate) fn from_resolved_with_runtime(
        config: &BehaviorToolConfig,
        availability: &RuntimeToolAvailability,
        surface: &ToolSurface,
    ) -> ToolSurfaceExplanation {
        let mut builder = ExplanationBuilder::default();

        builder.include_many("host", surface.host_tools.tool_names());
        explain_meta(config, surface, &mut builder);
        explain_subagents(config, surface, &mut builder);
        explain_background(config, surface, &mut builder);
        builder.include_many(
            "custom",
            surface
                .custom_tools
                .iter()
                .map(|tool| tool.name().to_string()),
        );
        explain_memory(config, &mut builder);
        explain_builtin_reads(config, surface, &mut builder);
        builder.include_many(
            "write_tools",
            surface
                .write_tools
                .iter()
                .map(|decl| decl.tool_name.clone()),
        );
        builder.include_many(
            "query_tools",
            surface
                .query_tools
                .iter()
                .map(|decl| decl.tool_name.clone()),
        );

        let tool_names = surface.tool_names();
        if surface.host_tools.tool_names().is_empty()
            && tool_names.iter().any(|name| {
                name == CONTEXT_BUDGET_TOOL_NAME
                    || name == SESSION_HISTORY_TOOL_NAME
                    || name == DEFRA_QUERY_TOOL_NAME
            })
        {
            builder.warn(
                "host_ceiling_not_global",
                "ToolCeiling currently clamps host-native file/bash/CLI tools only; built-in read tools can still be model-callable.",
            );
        }
        builder.finish(
            tool_names,
            ToolSurfacePolicyTrace {
                requested: policy_summary(config.behavior_policy()),
                ceiling: policy_summary(config.ceiling_policy()),
                runtime: policy_summary(&availability.policy),
                effective: policy_summary(&config.static_policy().meet(&availability.policy)),
            },
        )
    }
}

impl BehaviorToolConfig {
    pub fn explain_with_runtime(
        &self,
        mcp_services_online: bool,
        own_agent_did: &str,
        active_behavior_ids: &HashSet<String>,
    ) -> ToolSurfaceExplanation {
        let availability = RuntimeToolAvailability::for_mcp_presence(mcp_services_online);
        let surface = self.resolve_with_available_subagent_targets_for_runtime_availability(
            availability.clone(),
            own_agent_did,
            active_behavior_ids,
        );
        ToolSurfaceExplanation::from_resolved_with_runtime(self, &availability, &surface)
    }

    pub(crate) fn resolve_with_available_subagent_targets_for_runtime_availability(
        &self,
        availability: RuntimeToolAvailability,
        own_agent_did: &str,
        active_behavior_ids: &HashSet<String>,
    ) -> ToolSurface {
        let mut subagent_tools = self.subagent_tools().clone();
        let allow_cross_deployment = subagent_tools.allow_cross_deployment;
        subagent_tools.targets.retain(|target| {
            if target.agent_did == own_agent_did {
                active_behavior_ids.contains(&target.behavior_id)
            } else {
                allow_cross_deployment
            }
        });
        self.resolve_with_subagent_tools_for_runtime_availability(availability, subagent_tools)
    }

    pub fn explain_with_runtime_availability(
        &self,
        availability: RuntimeToolAvailability,
        own_agent_did: &str,
        active_behavior_ids: &HashSet<String>,
    ) -> ToolSurfaceExplanation {
        let surface = self.resolve_with_available_subagent_targets_for_runtime_availability(
            availability.clone(),
            own_agent_did,
            active_behavior_ids,
        );
        ToolSurfaceExplanation::from_resolved_with_runtime(self, &availability, &surface)
    }
}

#[derive(Default)]
struct ExplanationBuilder {
    included: BTreeMap<String, BTreeSet<String>>,
    excluded: BTreeMap<String, BTreeSet<String>>,
    unavailable: BTreeMap<String, BTreeSet<String>>,
    warnings: Vec<ToolSurfaceWarning>,
}

impl ExplanationBuilder {
    fn include_many<I>(&mut self, category: &str, names: I)
    where
        I: IntoIterator<Item = String>,
    {
        for name in names {
            self.insert(category, name, SurfaceStatus::Included);
        }
    }

    fn exclude(&mut self, category: &str, name: impl Into<String>) {
        self.insert(category, name.into(), SurfaceStatus::Excluded);
    }

    fn unavailable(&mut self, category: &str, name: impl Into<String>) {
        self.insert(category, name.into(), SurfaceStatus::Unavailable);
    }

    fn warn(&mut self, code: impl Into<String>, message: impl Into<String>) {
        let code = code.into();
        if self.warnings.iter().any(|warning| warning.code == code) {
            return;
        }
        self.warnings.push(ToolSurfaceWarning {
            code,
            message: message.into(),
        });
    }

    fn finish(
        self,
        tool_names: Vec<String>,
        policy: ToolSurfacePolicyTrace,
    ) -> ToolSurfaceExplanation {
        ToolSurfaceExplanation {
            tool_names,
            policy,
            included: into_vec_map(self.included),
            excluded: into_vec_map(self.excluded),
            unavailable: into_vec_map(self.unavailable),
            warnings: self.warnings,
        }
    }

    fn insert(&mut self, category: &str, name: String, status: SurfaceStatus) {
        let map = match status {
            SurfaceStatus::Included => &mut self.included,
            SurfaceStatus::Excluded => &mut self.excluded,
            SurfaceStatus::Unavailable => &mut self.unavailable,
        };
        map.entry(category.to_string()).or_default().insert(name);
    }
}

enum SurfaceStatus {
    Included,
    Excluded,
    Unavailable,
}

fn into_vec_map(map: BTreeMap<String, BTreeSet<String>>) -> BTreeMap<String, Vec<String>> {
    map.into_iter()
        .map(|(category, names)| (category, names.into_iter().collect()))
        .collect()
}

fn explain_meta(
    config: &BehaviorToolConfig,
    surface: &ToolSurface,
    builder: &mut ExplanationBuilder,
) {
    if surface.include_meta_tools {
        builder.include_many(
            "meta_mcp",
            META_TOOL_NAMES.iter().map(|name| (*name).to_string()),
        );
        if surface.allowed_mcp_service_ids.is_empty() {
            builder.warn(
                "mcp_empty_allowlist_all",
                "allowed_mcp_service_ids is empty, which currently means all online MCP services.",
            );
        }
        return;
    }

    for name in META_TOOL_NAMES {
        if config.meta_tools_requested() {
            builder.unavailable("meta_mcp", name.to_string());
        } else {
            builder.exclude("meta_mcp", name.to_string());
        }
    }
    if config.meta_tools_requested() {
        builder.warn(
            "meta_requested_no_online_mcp",
            "Meta tools are configured on, but no ToolServiceRegistry row is currently online.",
        );
    }
}

fn explain_subagents(
    config: &BehaviorToolConfig,
    surface: &ToolSurface,
    builder: &mut ExplanationBuilder,
) {
    let included = subagent_tool_names(&surface.subagent_tools);
    if !included.is_empty() {
        builder.include_many("subagent", included);
    } else if config.subagent_tools().tools_enabled() {
        builder.unavailable("subagent", "spawn_subagent");
        builder.warn(
            "subagent_targets_unavailable",
            "Subagent spawning is configured, but all targets were filtered out by active-behavior or cross-deployment availability.",
        );
    } else {
        builder.exclude("subagent", "spawn_subagent");
    }
}

fn explain_background(
    config: &BehaviorToolConfig,
    surface: &ToolSurface,
    builder: &mut ExplanationBuilder,
) {
    let included = background_tool_names(&surface.background_tools);
    if !included.is_empty() {
        builder.include_many("background_process", included);
    } else if config.background_tools().tools_enabled() {
        builder.unavailable("background_process", "spawn_process");
    } else {
        builder.exclude("background_process", "spawn_process");
    }
}

fn explain_memory(config: &BehaviorToolConfig, builder: &mut ExplanationBuilder) {
    if !config.memory_requested() {
        builder.exclude("built_in_memory", MEMORY_TOOL_NAME);
        return;
    }

    #[cfg(feature = "agent-memory")]
    {
        builder.include_many(
            "built_in_memory",
            [crate::toolset::MEMORY_TOOL_NAME.to_string()],
        );
    }

    #[cfg(not(feature = "agent-memory"))]
    {
        builder.unavailable("built_in_memory", MEMORY_TOOL_NAME);
        builder.warn(
            "memory_requested_compiled_out",
            "enable_memory is true, but this binary was built without the agent-memory feature.",
        );
    }
}

fn explain_builtin_reads(
    config: &BehaviorToolConfig,
    surface: &ToolSurface,
    builder: &mut ExplanationBuilder,
) {
    if surface.enable_context_budget_tool {
        builder.include_many("built_in_read", [CONTEXT_BUDGET_TOOL_NAME.to_string()]);
    } else if config.context_budget_requested() {
        builder.unavailable("built_in_read", CONTEXT_BUDGET_TOOL_NAME);
    } else {
        builder.exclude("built_in_read", CONTEXT_BUDGET_TOOL_NAME);
    }

    if surface.enable_session_history_tool {
        builder.include_many("built_in_read", [SESSION_HISTORY_TOOL_NAME.to_string()]);
    } else {
        builder.exclude("built_in_read", SESSION_HISTORY_TOOL_NAME);
    }

    if surface.enable_defra_query {
        builder.include_many("built_in_read", [DEFRA_QUERY_TOOL_NAME.to_string()]);
        if surface.defra_query_scope.is_unrestricted() {
            builder.warn(
                "defra_query_empty_scope_all",
                "defra_query has no collection allowlist (scope: all), so every collection except hard-blocked sensitive fields is readable.",
            );
        }
    } else if config.defra_query_requested() {
        builder.unavailable("built_in_read", DEFRA_QUERY_TOOL_NAME);
    } else {
        builder.exclude("built_in_read", DEFRA_QUERY_TOOL_NAME);
    }

    if surface.self_config.enabled {
        builder.include_many(
            "self_config",
            crate::self_config::self_config_tool_names(&surface.self_config),
        );
    } else if config.self_config_requested() {
        builder.unavailable("self_config", crate::self_config::GET_MY_CONFIG_TOOL_NAME);
    } else {
        builder.exclude("self_config", crate::self_config::GET_MY_CONFIG_TOOL_NAME);
    }

    if surface.lsp.is_some() {
        builder.include_many("lsp", [crate::toolset::lsp::LSP_TOOL_NAME.to_string()]);
        builder.warn(
            "lsp_host_exec",
            "enable_lsp starts host language-server processes (and their descendants) for the built-in catalog. Enabling the tools self-config category can activate any detected catalog server. Off macOS the server is unsandboxed and may write outside the tool root.",
        );
    } else {
        builder.exclude("lsp", crate::toolset::lsp::LSP_TOOL_NAME);
    }
}

fn policy_summary(policy: &ToolPolicySurface) -> BTreeMap<String, Vec<String>> {
    let mut summary = BTreeMap::new();
    summary.insert(
        "host".to_string(),
        vec![
            format!("file:{:?}", policy.file),
            format!("bash:{:?}", policy.bash.tool),
            format!("bash_mode:{:?}", policy.bash.execution_mode),
            format!("bash_network:{:?}", policy.bash.network_mode),
            format!("bash_allowed:{}", policy.bash.allowed_argv_prefixes.kind()),
        ],
    );
    summary.insert(
        "built_in_read".to_string(),
        [
            (policy.context_budget, CONTEXT_BUDGET_TOOL_NAME),
            (policy.session_history, SESSION_HISTORY_TOOL_NAME),
            // `include_defra_query` not the raw `defra_query` bit: a deny-all
            // collection scope (`Only(∅)`/`None`) gates the tool off, so the
            // effective trace must not list it as present.
            (policy.include_defra_query(), DEFRA_QUERY_TOOL_NAME),
        ]
        .into_iter()
        .filter_map(|(enabled, name)| enabled.then_some(name.to_string()))
        .collect(),
    );
    summary.insert(
        "defra_query".to_string(),
        vec![
            format!("enabled:{}", policy.defra_query),
            format!("collections:{}", policy.defra_collections.kind()),
        ],
    );
    summary.insert(
        "self_config".to_string(),
        vec![
            format!("enabled:{}", policy.self_config),
            format!("categories:{}", policy.self_config_categories.kind()),
        ],
    );
    summary.insert(
        "meta_mcp".to_string(),
        vec![
            format!("enabled:{}", policy.meta),
            format!("services:{}", policy.mcp_services.kind()),
        ],
    );
    summary.insert(
        "subagent".to_string(),
        vec![
            format!("spawn:{}", policy.spawn),
            format!("steering:{}", policy.steering),
            format!("cross_deployment:{}", policy.cross_deployment),
            format!("targets:{}", policy.subagent_targets.kind()),
        ],
    );
    summary.insert(
        "background_process".to_string(),
        vec![
            format!("enabled:{}", policy.background),
            format!("tools:{}", policy.background_tools.kind()),
        ],
    );
    summary.insert(
        "skills".to_string(),
        vec![format!("enabled:{}", policy.skills)],
    );
    summary.insert(
        "built_in_memory".to_string(),
        policy
            .memory
            .then_some(MEMORY_TOOL_NAME.to_string())
            .into_iter()
            .collect(),
    );
    summary.insert(
        "write_tools".to_string(),
        vec![format!("scope:{}", policy.write_tools.kind())],
    );
    summary.insert(
        "query_tools".to_string(),
        vec![format!("scope:{}", policy.query_tools.kind())],
    );
    summary
}
