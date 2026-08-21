mod behavior_config;
mod build;
mod explain;
mod modes;
mod policy;
mod runtime_context;
mod selection;

pub use behavior_config::BehaviorToolConfig;
pub(crate) use build::resolve_configured_tool_root;
pub use explain::{ToolSurfaceExplanation, ToolSurfaceWarning};
pub use modes::{BashMode, FileToolMode, ToolCeiling};
pub use policy::{
    EndpointScope, RuntimeToolAvailability, ToolPolicyBash, ToolPolicySurface, ToolPolicyVersion,
    TOOL_POLICY_V1,
};
pub use runtime_context::ToolRuntimeContext;
pub(crate) use selection::{BackgroundToolConfig, SubagentToolConfig};
pub use selection::{CustomToolFactory, ToolSelection};

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::llm::tool::ToolDyn;
use anyhow::Result;

use crate::defra_query::{
    build_defra_query_tool, BoundedQueryTool, CollectionScope, DEFRA_QUERY_TOOL_NAME,
};
use crate::defra_write::BoundedWriteTool;
use crate::document_config::{QueryToolDecl, SubagentTarget, WriteToolDecl};
use crate::meta_tools::{build_meta_tools, META_TOOL_NAMES};
use crate::toolset::{
    background_tool_names, build_background_tools, build_context_budget_tool, build_goal_tools,
    build_session_history_tool, build_subagent_tools, subagent_tool_names, CliToolConfig, ToolSet,
    CONTEXT_BUDGET_TOOL_NAME, SESSION_HISTORY_TOOL_NAME,
};
#[cfg(feature = "agent-memory")]
use crate::toolset::{build_memory_tool, MEMORY_TOOL_NAME};

const DEFAULT_CLI_TIMEOUT_SECS: u64 = 10;

#[derive(Clone)]
pub struct ToolSurface {
    host_tools: ToolSet,
    include_meta_tools: bool,
    allowed_mcp_service_ids: Vec<String>,
    subagent_tools: SubagentToolConfig,
    background_tools: BackgroundToolConfig,
    approval_required_tools: Vec<String>,
    custom_tools: Vec<CustomToolFactory>,
    pub(super) enable_memory: bool,
    pub(super) enable_context_budget_tool: bool,
    pub(super) enable_session_history_tool: bool,
    pub(super) enable_defra_query: bool,
    pub(super) defra_query_scope: CollectionScope,
    pub(super) write_tools: Vec<WriteToolDecl>,
    pub(super) query_tools: Vec<QueryToolDecl>,
    pub(super) enable_skills: bool,
    pub(super) self_config: SelfConfigToolConfig,
    pub(super) lsp: Option<crate::toolset::lsp::LspToolConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SelfConfigToolConfig {
    pub enabled: bool,
    pub behavior_id: String,
    pub categories: std::collections::BTreeSet<String>,
    pub no_lockout: bool,
    pub dry_run: bool,
}

impl ToolSurface {
    pub(crate) fn source_fill_fields(&self) -> std::collections::BTreeSet<String> {
        self.write_tools
            .iter()
            .flat_map(|decl| decl.fields.iter())
            .chain(
                self.query_tools
                    .iter()
                    .flat_map(|decl| decl.filter_fields.iter()),
            )
            .filter_map(|field| match &field.fill {
                Some(crate::document_config::WriteToolFieldFill::SourceField(source)) => {
                    Some(source.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub(crate) fn output_obligations(
        &self,
    ) -> Vec<(String, crate::document_config::WriteToolOutputObligation)> {
        self.write_tools
            .iter()
            .filter_map(|decl| {
                decl.output_obligation
                    .clone()
                    .map(|obligation| (decl.tool_name.clone(), obligation))
            })
            .collect()
    }

    pub fn host_tools(&self) -> &ToolSet {
        &self.host_tools
    }

    pub fn includes_meta_tools(&self) -> bool {
        self.include_meta_tools
    }

    pub fn includes_skills(&self) -> bool {
        self.enable_skills
    }

    pub fn allowed_mcp_service_ids(&self) -> &[String] {
        &self.allowed_mcp_service_ids
    }

    #[allow(dead_code)]
    pub(crate) fn subagent_tools(&self) -> &SubagentToolConfig {
        &self.subagent_tools
    }

    pub(crate) fn subagent_targets(&self) -> &[SubagentTarget] {
        if self.subagent_tools.spawn_enabled {
            &self.subagent_tools.targets
        } else {
            &[]
        }
    }

    pub(crate) fn background_tools(&self) -> &BackgroundToolConfig {
        &self.background_tools
    }

    pub(crate) fn approval_required_tools(&self) -> &[String] {
        &self.approval_required_tools
    }

    pub(crate) fn retain_subagent_targets(
        &mut self,
        own_agent_did: &str,
        active_behavior_ids: &HashSet<String>,
    ) {
        self.subagent_tools.targets.retain(|target| {
            if target.agent_did == own_agent_did {
                active_behavior_ids.contains(&target.behavior_id)
            } else {
                true
            }
        });
    }

    pub fn tool_names(&self) -> Vec<String> {
        let mut names = self.host_tools.tool_names();
        if self.include_meta_tools {
            names.extend(META_TOOL_NAMES.iter().map(|name| (*name).to_string()));
        }
        names.extend(subagent_tool_names(&self.subagent_tools));
        names.extend(background_tool_names(&self.background_tools));
        names.extend(self.custom_tools.iter().map(|tool| tool.name().to_string()));
        #[cfg(feature = "agent-memory")]
        if self.enable_memory {
            names.push(MEMORY_TOOL_NAME.to_string());
        }
        if self.enable_context_budget_tool {
            names.push(CONTEXT_BUDGET_TOOL_NAME.to_string());
        }
        if self.enable_session_history_tool {
            names.push(SESSION_HISTORY_TOOL_NAME.to_string());
        }
        if self.enable_defra_query {
            names.push(DEFRA_QUERY_TOOL_NAME.to_string());
        }
        if self.lsp.is_some() {
            names.push(crate::toolset::lsp::LSP_TOOL_NAME.to_string());
        }
        if self.include_meta_tools {
            names.push(crate::goal::GET_GOAL_TOOL_NAME.to_string());
            names.push(crate::goal::UPDATE_GOAL_TOOL_NAME.to_string());
        }
        names.extend(crate::self_config::self_config_tool_names(
            &self.self_config,
        ));
        for decl in &self.write_tools {
            if decl.is_well_formed() {
                names.push(decl.tool_name.clone());
            }
        }
        for decl in &self.query_tools {
            if decl.is_well_formed() {
                names.push(decl.tool_name.clone());
            }
        }
        build::dedupe_strings(names)
    }

    #[cfg(test)]
    pub(crate) fn lsp_config(&self) -> Option<&crate::toolset::lsp::LspToolConfig> {
        self.lsp.as_ref()
    }

    pub fn build_tools(&self, runtime: &ToolRuntimeContext) -> Result<Vec<Box<dyn ToolDyn>>> {
        let writethrough = self.lsp.as_ref().map(|config| {
            crate::toolset::lsp::LspWritethrough::new(runtime.lsp_pool.clone(), config.clone())
        });
        let mut tools = self
            .host_tools
            .build_native_tools_with_writethrough(writethrough)?;
        if self.include_meta_tools {
            tools.extend(build_meta_tools(
                runtime.node.clone(),
                runtime.mcp_pool.clone(),
                runtime.health_map.clone(),
                runtime.local_hostname.clone(),
                runtime.local_subnet.clone(),
                runtime.agent_did.clone(),
                self.allowed_mcp_service_ids.clone(),
            ));
        }
        tools.extend(build_subagent_tools(self.subagent_tools.clone()));
        tools.extend(build_background_tools(self.background_tools.clone()));
        for tool in &self.custom_tools {
            tools.push(tool.build()?);
        }
        #[cfg(feature = "agent-memory")]
        if self.enable_memory {
            tools.push(build_memory_tool(
                runtime.node.clone(),
                runtime.agent_did.clone(),
            ));
        }
        if self.enable_context_budget_tool {
            tools.push(build_context_budget_tool(
                runtime.node.clone(),
                runtime.agent_did.clone(),
            ));
        }
        if self.enable_session_history_tool {
            tools.push(build_session_history_tool(
                runtime.node.clone(),
                runtime.agent_did.clone(),
            ));
        }
        if self.enable_defra_query {
            tools.push(build_defra_query_tool(
                runtime.node.clone(),
                self.defra_query_scope.clone(),
            ));
        }
        if let Some(lsp) = &self.lsp {
            tools.push(Box::new(crate::toolset::lsp::LspTool::new(
                lsp.clone(),
                runtime.lsp_pool.clone(),
            )?));
        }
        if self.include_meta_tools {
            tools.extend(build_goal_tools());
        }
        tools.extend(crate::self_config::build_self_config_tools(
            runtime.node.clone(),
            runtime.agent_did.clone(),
            &self.self_config,
        ));
        let mut registered_names: HashSet<String> = tools.iter().map(|tool| tool.name()).collect();
        for decl in &self.write_tools {
            let tool = BoundedWriteTool::new(runtime.node.clone(), decl.clone());
            if !tool.is_well_formed() || !decl.output_obligation_is_well_formed() {
                anyhow::bail!(
                    "write tool `{}` reached registration with an invalid declaration",
                    decl.tool_name
                );
            }
            if !registered_names.insert(tool.name()) {
                anyhow::bail!(
                    "write tool `{}` reached registration with a duplicate tool name",
                    decl.tool_name
                );
            }
            tools.push(Box::new(tool) as Box<dyn ToolDyn>);
        }
        for decl in &self.query_tools {
            let tool = BoundedQueryTool::new(runtime.node.clone(), decl.clone());
            if !tool.is_well_formed() {
                anyhow::bail!(
                    "query tool `{}` reached registration with an invalid declaration",
                    decl.tool_name
                );
            }
            if !registered_names.insert(tool.name()) {
                anyhow::bail!(
                    "query tool `{}` reached registration with a duplicate tool name",
                    decl.tool_name
                );
            }
            tools.push(Box::new(tool) as Box<dyn ToolDyn>);
        }
        Ok(tools)
    }
}

impl std::fmt::Debug for ToolSurface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolSurface")
            .field("host_tools", &self.host_tools)
            .field("include_meta_tools", &self.include_meta_tools)
            .field("allowed_mcp_service_ids", &self.allowed_mcp_service_ids)
            .field("subagent_tools", &self.subagent_tools)
            .field("background_tools", &self.background_tools)
            .field(
                "custom_tools",
                &self
                    .custom_tools
                    .iter()
                    .map(|tool| tool.name())
                    .collect::<Vec<_>>(),
            )
            .field("enable_memory", &self.enable_memory)
            .field(
                "enable_context_budget_tool",
                &self.enable_context_budget_tool,
            )
            .field(
                "enable_session_history_tool",
                &self.enable_session_history_tool,
            )
            .field("enable_defra_query", &self.enable_defra_query)
            .field("defra_query_scope", &self.defra_query_scope)
            .field("write_tools", &self.write_tools)
            .field("query_tools", &self.query_tools)
            .field("enable_skills", &self.enable_skills)
            .field("self_config", &self.self_config)
            .finish()
    }
}

pub(crate) fn resolve_subagent_target_descriptions(
    tool_surface: &ToolSurface,
) -> Vec<(String, String)> {
    tool_surface
        .subagent_targets()
        .iter()
        .map(|target| (target.name.clone(), target.description_text().to_string()))
        .collect()
}

pub fn cli_tool(
    name: impl Into<String>,
    binary_path: impl Into<PathBuf>,
    description: impl Into<String>,
) -> CliToolConfig {
    CliToolConfig {
        name: name.into(),
        binary_path: binary_path.into(),
        description: description.into(),
        allowed_argv_prefixes: Vec::new(),
        env_vars: HashMap::new(),
        working_dir: None,
        timeout_secs: DEFAULT_CLI_TIMEOUT_SECS,
    }
}

#[cfg(test)]
mod tests;
