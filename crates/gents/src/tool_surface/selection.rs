use std::sync::Arc;

use crate::llm::tool::ToolDyn;
use anyhow::Result;

use super::modes::{BashMode, FileToolMode};

use std::path::PathBuf;

use crate::document_config::SubagentTarget;
use crate::tool_call_lifecycle::AwaitMode;
use crate::toolset::{
    default_read_only_command_policy, parse_argv_prefixes, CommandExecutionMode,
    CommandExecutionPolicy, CommandNetworkMode,
};

use super::policy::ToolPolicyVersion;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SubagentToolConfig {
    pub targets: Vec<SubagentTarget>,
    pub spawn_enabled: bool,
    pub steering_enabled: bool,
    pub background_enabled: bool,
    pub default_await_mode: AwaitMode,
    /// When false (default), cross-deployment (remote-DID) subagent delegation is
    /// disabled: remote-DID targets are not surfaced to the model and remote spawns
    /// are rejected at runtime. Cross-deployment is deferred pending ACP; only
    /// trusted-fleet deployments should opt in.
    pub allow_cross_deployment: bool,
}

impl SubagentToolConfig {
    pub(crate) fn from_document(selection: &crate::document_config::ToolSelectionDocument) -> Self {
        let background_enabled = selection.subagent_background_enabled.unwrap_or(false);
        let default_await_mode = selection
            .subagent_default_await_mode
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .and_then(AwaitMode::from_persisted)
            .filter(|mode| background_enabled || *mode != AwaitMode::Background)
            .unwrap_or_default();
        let targets = selection
            .subagent_targets
            .iter()
            .flatten()
            .filter_map(
                |entry| match crate::document_config::SubagentTarget::parse(entry) {
                    Ok(target) => Some(target),
                    Err(error) => {
                        tracing::warn!(
                            selection_id = %selection.selection_id,
                            entry = %entry,
                            %error,
                            "skipping malformed subagent_targets entry"
                        );
                        None
                    }
                },
            )
            .collect();
        Self {
            targets,
            spawn_enabled: selection.subagent_spawn_enabled.unwrap_or(false),
            steering_enabled: selection.subagent_steering_enabled.unwrap_or(false),
            background_enabled,
            default_await_mode,
            allow_cross_deployment: selection.subagent_allow_cross_deployment.unwrap_or(false),
        }
    }

    pub(crate) fn tools_enabled(&self) -> bool {
        self.spawn_enabled && !self.targets.is_empty()
    }

    /// Inspection is part of the background-subagent capability. A behavior
    /// that can launch a child asynchronously must also be able to read that
    /// child's transcript without requiring the stronger steering permission.
    pub(crate) fn background_inspection_tools_enabled(&self) -> bool {
        self.tools_enabled() && self.background_enabled
    }

    pub(crate) fn steer_subagent_enabled(&self) -> bool {
        self.background_inspection_tools_enabled() && self.steering_enabled
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BackgroundToolConfig {
    pub allowlist: Vec<String>,
}

impl BackgroundToolConfig {
    pub(crate) fn tools_enabled(&self) -> bool {
        !self.allowlist.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSelection {
    pub file_tools: FileToolMode,
    pub file_tool_root: Option<PathBuf>,
    pub bash: BashMode,
    pub command_policy: Option<CommandExecutionPolicy>,
    pub cli_tool_names: Vec<String>,
    pub enable_meta_tools: bool,
    pub allowed_mcp_service_ids: Vec<String>,
    pub backgroundable_tool_names: Vec<String>,
    pub approval_required_tools: Vec<String>,
    pub enable_memory: bool,
    pub enable_session_history_tool: bool,
    pub enable_context_budget: bool,
    pub enable_defra_query: bool,
    pub defra_query_collections: Vec<String>,
    pub write_tools: Vec<crate::document_config::WriteToolDecl>,
    pub query_tools: Vec<crate::document_config::QueryToolDecl>,
    pub enable_self_config: bool,
    pub self_config_categories: Option<Vec<String>>,
    pub self_config_no_lockout: bool,
    pub self_config_dry_run: bool,
    pub enable_lsp: bool,
    pub enable_graph_dsl: bool,
    pub lsp_config: Option<String>,
}

impl Default for ToolSelection {
    fn default() -> Self {
        Self {
            file_tools: FileToolMode::Off,
            file_tool_root: None,
            bash: BashMode::Off,
            command_policy: None,
            cli_tool_names: Vec::new(),
            enable_meta_tools: true,
            allowed_mcp_service_ids: Vec::new(),
            backgroundable_tool_names: Vec::new(),
            approval_required_tools: Vec::new(),
            enable_memory: false,
            enable_session_history_tool: false,
            enable_context_budget: true,
            enable_defra_query: false,
            defra_query_collections: Vec::new(),
            write_tools: Vec::new(),
            query_tools: Vec::new(),
            enable_self_config: false,
            self_config_categories: None,
            self_config_no_lockout: false,
            self_config_dry_run: false,
            enable_lsp: false,
            enable_graph_dsl: false,
            lsp_config: None,
        }
    }
}

impl ToolSelection {
    pub(crate) fn from_document(
        selection: &crate::document_config::ToolSelectionDocument,
    ) -> anyhow::Result<Self> {
        let policy_version = ToolPolicyVersion::parse(selection.tool_policy_version.as_deref())?;
        let bash = if selection.enable_bash.unwrap_or(false) {
            BashMode::parse(selection.bash_mode.as_deref().unwrap_or("ReadOnly"))?
        } else {
            BashMode::Off
        };
        Ok(Self {
            file_tools: if selection.enable_file_tools.unwrap_or(false) {
                FileToolMode::parse(selection.file_tools_mode.as_deref().unwrap_or("ReadOnly"))?
            } else {
                FileToolMode::Off
            },
            file_tool_root: selection
                .file_tool_root
                .as_deref()
                .and_then(normalize_optional_string)
                .map(PathBuf::from),
            bash,
            command_policy: command_policy_from_document(selection, bash)?,
            cli_tool_names: selection.cli_tool_names.clone().unwrap_or_default(),
            enable_meta_tools: selection
                .enable_meta_tools
                .unwrap_or(policy_version.default_enabled(true)),
            allowed_mcp_service_ids: selection
                .allowed_mcp_service_ids
                .clone()
                .unwrap_or_default(),
            backgroundable_tool_names: selection
                .backgroundable_tool_names
                .clone()
                .unwrap_or_default(),
            approval_required_tools: selection
                .approval_required_tools
                .clone()
                .unwrap_or_default(),
            enable_memory: selection.enable_memory.unwrap_or(false),
            enable_session_history_tool: selection.enable_session_history_tool.unwrap_or(false),
            enable_context_budget: selection
                .enable_context_budget
                .unwrap_or(policy_version.default_enabled(true)),
            enable_defra_query: selection.enable_defra_query.unwrap_or(false),
            defra_query_collections: selection
                .defra_query_collections
                .clone()
                .unwrap_or_default(),
            write_tools: selection.write_tools.clone().unwrap_or_default(),
            query_tools: Vec::new(),
            enable_self_config: selection.enable_self_config.unwrap_or(false),
            self_config_categories: selection.self_config_categories.clone(),
            self_config_no_lockout: selection.self_config_no_lockout.unwrap_or(false),
            self_config_dry_run: selection.self_config_dry_run.unwrap_or(false),
            enable_lsp: selection.enable_lsp.unwrap_or(false),
            enable_graph_dsl: selection.enable_graph_dsl.unwrap_or(false),
            lsp_config: selection
                .lsp_config
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        })
    }
}

fn command_policy_from_document(
    selection: &crate::document_config::ToolSelectionDocument,
    bash: BashMode,
) -> anyhow::Result<Option<CommandExecutionPolicy>> {
    let has_policy = selection
        .command_execution_policy
        .as_deref()
        .and_then(normalize_optional_string)
        .is_some()
        || selection
            .command_network_mode
            .as_deref()
            .and_then(normalize_optional_string)
            .is_some()
        || selection
            .command_allowed_argv_prefixes
            .as_ref()
            .is_some_and(|prefixes| !prefixes.is_empty())
        || selection
            .command_forbidden_argv_prefixes
            .as_ref()
            .is_some_and(|prefixes| !prefixes.is_empty())
        || selection
            .read_only_command_allowlist
            .as_ref()
            .is_some_and(|list| !list.is_empty());
    if !has_policy {
        return if matches!(bash, BashMode::Unrestricted) {
            Ok(Some(
                CommandExecutionPolicy::write_capable()
                    .with_mode(CommandExecutionMode::Unrestricted),
            ))
        } else {
            Ok(None)
        };
    }

    let requested_mode = selection
        .command_execution_policy
        .as_deref()
        .and_then(normalize_optional_string)
        .map(CommandExecutionMode::parse)
        .transpose()?;
    let mode = match bash {
        BashMode::Off | BashMode::ReadOnly => CommandExecutionMode::ReadOnly,
        BashMode::Unrestricted => requested_mode.unwrap_or(CommandExecutionMode::Unrestricted),
    };

    let allowed = parse_argv_prefixes(
        selection
            .command_allowed_argv_prefixes
            .as_deref()
            .unwrap_or(&[]),
    )?;
    let forbidden = parse_argv_prefixes(
        selection
            .command_forbidden_argv_prefixes
            .as_deref()
            .unwrap_or(&[]),
    )?;
    let network_mode = selection
        .command_network_mode
        .as_deref()
        .and_then(normalize_optional_string)
        .map(CommandNetworkMode::parse)
        .transpose()?
        .unwrap_or(CommandNetworkMode::Inherit);

    let base = if matches!(mode, CommandExecutionMode::ReadOnly) {
        default_read_only_command_policy()
    } else {
        CommandExecutionPolicy::write_capable()
    };
    let base = match (mode, selection.read_only_command_allowlist.as_deref()) {
        (CommandExecutionMode::ReadOnly, Some(list)) if !list.is_empty() => {
            base.with_read_only_allowlist(list.to_vec())
        }
        _ => base,
    };
    Ok(Some(
        base.with_mode(mode)
            .with_allowed_argv_prefixes(allowed)
            .with_forbidden_argv_prefixes(forbidden)
            .with_network_mode(network_mode),
    ))
}

fn normalize_optional_string(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

type CustomToolFactoryFn = Arc<dyn Fn() -> Result<Box<dyn ToolDyn>> + Send + Sync>;

#[derive(Clone)]
pub struct CustomToolFactory {
    name: String,
    factory: CustomToolFactoryFn,
}

impl CustomToolFactory {
    pub fn new(
        name: impl Into<String>,
        factory: impl Fn() -> Result<Box<dyn ToolDyn>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            factory: Arc::new(factory),
        }
    }

    pub fn from_tool<T>(tool: T) -> Self
    where
        T: ToolDyn + Clone + Send + Sync + 'static,
    {
        let name = tool.name();
        Self::new(name, move || Ok(Box::new(tool.clone()) as Box<dyn ToolDyn>))
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn build(&self) -> Result<Box<dyn ToolDyn>> {
        (self.factory)()
    }
}

impl std::fmt::Debug for CustomToolFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomToolFactory")
            .field("name", &self.name)
            .finish()
    }
}
