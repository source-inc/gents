mod actions;
mod admit;
mod auth;
mod catalog;
mod client;
mod config;
mod edits;
mod encoding;
mod pool;
#[cfg(test)]
mod tests;
mod uri;
mod writethrough;

pub use admit::admit_command;
pub use auth::{
    lsp_action_authorized, lsp_advertised, lsp_apply_authorized, LspAction, LspMutationSource,
};
pub use catalog::{builtin_catalog, primary_for_file, CatalogServer};
pub use config::LspConfigDocument;
pub use pool::{LspPool, PoolKey};
pub use writethrough::{LspWritethrough, MutationKind};

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use crate::llm::tool::{Tool, ToolDefinition};
use crate::tool_call_lifecycle::FailureClass;
use crate::tool_surface::{FileToolMode, ToolPolicyBash, ToolPolicySurface};
use crate::toolset::shared::{ToolContext, ToolError};
use crate::toolset::{lsp_sandbox_for_effective, CommandConstraints, CommandNetworkMode};

use actions::ActionRequest;
use config::apply_overrides;

pub const LSP_TOOL_NAME: &str = "lsp";

/// Shared acceptance semantics for persisted LSP results. Keep demo-pack and
/// live-test qualification on the same definition so ordinary hover text such
/// as `Result<T, ToolError>` is not mistaken for a failed tool call.
pub fn result_looks_failed(result: &str) -> bool {
    let trimmed = result.trim();
    let lower = trimmed.to_ascii_lowercase();
    trimmed.is_empty()
        || matches!(
            lower.as_str(),
            "no result" | "no symbols found" | "no hover information" | "no definition found"
        )
        || lower.starts_with("error:")
        || lower.starts_with("lsp error")
        || lower.starts_with("failed")
        || lower.starts_with("unavailable")
        || lower.starts_with("policydenied")
        || lower.contains("stdout closed")
        || lower.contains("timed out")
}

pub fn result_path_matches(expected: &str, actual: &str) -> bool {
    let expected = expected.replace('\\', "/");
    let expected = expected.trim_start_matches("./").trim_matches('/');
    let actual = actual.replace('\\', "/");
    let actual = actual.trim_start_matches("./").trim_end_matches('/');
    actual == expected || actual.ends_with(&format!("/{expected}"))
}

#[derive(Clone)]
pub struct LspToolConfig {
    pub lsp: bool,
    pub file: FileToolMode,
    pub workspace: PathBuf,
    pub session_id: String,
    pub behavior_id: String,
    pub digest: String,
    pub servers: Vec<CatalogServer>,
    pub constraints: CommandConstraints,
    pub format_on_write: bool,
    pub diagnostics_on_write: bool,
    pub diagnostics_on_edit: bool,
    pub diagnostics_deduplicate: bool,
    pub idle_timeout: Duration,
}

pub fn constraints_from_effective_policy(
    policy: &ToolPolicySurface,
    lsp_network_overlay: Option<CommandNetworkMode>,
) -> CommandConstraints {
    constraints_from_effective_bash(&policy.bash, lsp_network_overlay)
}

pub fn constraints_from_effective_bash(
    bash: &ToolPolicyBash,
    lsp_network_overlay: Option<CommandNetworkMode>,
) -> CommandConstraints {
    let (allowed, deny_all) = match &bash.allowed_argv_prefixes {
        crate::tool_surface::EndpointScope::All => (Vec::new(), false),
        crate::tool_surface::EndpointScope::None => (Vec::new(), true),
        crate::tool_surface::EndpointScope::Only(_) => (bash.allowed_argv_prefixes.keys(), false),
    };
    let sandbox = crate::toolset::lsp_sandbox_for_effective(bash.execution_mode);
    // The platform default disables network only when the effective LSP
    // sandbox can enforce that promise. In particular, a macOS behavior whose
    // effective execution mode is Unrestricted must inherit network rather
    // than constructing the rejected Disabled + Unrestricted combination.
    let desired = lsp_network_overlay.unwrap_or_else(|| {
        if matches!(
            sandbox,
            crate::toolset::CommandExecutionMode::WorkspaceWrite
        ) {
            crate::toolset::default_lsp_network_mode()
        } else {
            CommandNetworkMode::Inherit
        }
    });
    let network_mode = desired.meet(bash.network_mode);
    CommandConstraints {
        allowed_argv_prefixes: allowed,
        forbidden_argv_prefixes: bash.forbidden_argv_prefixes.iter().cloned().collect(),
        network_mode,
        execution_mode: bash.execution_mode,
        sandbox,
        deny_all_argv: deny_all,
        deny_git_metadata_writes: bash.deny_git_metadata_writes,
    }
}

#[derive(Clone)]
pub(crate) struct LspTool {
    config: LspToolConfig,
    pool: LspPool,
    context: ToolContext,
}

impl LspTool {
    pub fn new(config: LspToolConfig, pool: LspPool) -> Result<Self, anyhow::Error> {
        let context = ToolContext::new(config.workspace.clone(), false)?;
        Ok(Self {
            config,
            pool,
            context,
        })
    }

    fn effective_workspace(&self) -> PathBuf {
        overlay_workspace_or(&self.config.workspace)
    }

    fn effective_file_mode(&self) -> FileToolMode {
        crate::tool_call_lifecycle::runtime::current_tool_runtime_context()
            .and_then(|scope| scope.workspace_authority)
            .map(|authority| {
                self.config
                    .file
                    .meet(crate::workspace::workspace_authority_file_mode(authority))
            })
            .unwrap_or(self.config.file)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct LspArgs {
    action: String,
    #[serde(default)]
    file: Option<String>,
    #[serde(default)]
    line: Option<u32>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    new_name: Option<String>,
    #[serde(default)]
    apply: Option<bool>,
    #[serde(default)]
    payload: Option<String>,
    #[serde(default)]
    timeout: Option<u32>,
}

impl Tool for LspTool {
    const NAME: &'static str = LSP_TOOL_NAME;
    type Error = ToolError;
    type Args = LspArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: LSP_TOOL_NAME.to_string(),
            description: "Symbol-aware code intelligence from language servers. Position actions accept file + 1-indexed line + symbol; when line is omitted, Gents searches the file for symbol. symbols with a file lists qualified document symbols with kind and line; file=\"*\" + query searches the workspace. Prefer lsp over text search for definitions, references, renames, and code actions because it follows shadowing and cross-file callsites. status reports each configured server as starting/indexing, ready, not started, or unavailable and does not start one. reload retires the current snapshot's clients without starting replacements. workspace/executeCommand is never sent.".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "status",
                            "reload",
                            "capabilities",
                            "diagnostics",
                            "definition",
                            "type_definition",
                            "implementation",
                            "references",
                            "hover",
                            "symbols",
                            "rename",
                            "rename_file",
                            "code_actions",
                            "request"
                        ]
                    },
                    "file": {
                        "type": "string",
                        "description": "Path relative to the file-tool root. For diagnostics also a glob; use \"*\" for workspace symbols/diagnostics/reload."
                    },
                    "line": {
                        "type": "integer",
                        "description": "1-indexed line for hover, definition, references, rename, and code_actions. Omit to search the whole file for symbol."
                    },
                    "symbol": {
                        "type": "string",
                        "description": "Substring on that line used to pick the column. Use name#N for the Nth match (1-indexed)."
                    },
                    "query": {
                        "type": "string",
                        "description": "Workspace symbol query; code-action title substring or 0/1-based index when apply=true; or raw LSP method for action=request."
                    },
                    "new_name": {
                        "type": "string",
                        "description": "Required for rename and rename_file."
                    },
                    "apply": {
                        "type": "boolean",
                        "description": "rename/rename_file apply unless false. code_actions lists unless true."
                    },
                    "payload": {
                        "type": "string",
                        "description": "JSON object for action=request. Only allowlisted read methods; every URI field is validated and non-file schemes are rejected."
                    },
                    "timeout": {
                        "type": "integer",
                        "minimum": 5,
                        "maximum": 300,
                        "description": "Request timeout in seconds. This bounds the entire indexing-retry loop; linter fallback is also bounded and cancellable (default 20)."
                    }
                },
                "required": ["action"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let mut action = LspAction::parse(&args.action).ok_or_else(|| {
            ToolError::reported_failure(
                FailureClass::ArgumentInvalid,
                format!("unknown lsp action {}", args.action),
            )
        })?;
        if matches!(action, LspAction::CodeActionsList) && args.apply == Some(true) {
            action = LspAction::CodeActionsApply;
        }
        if !lsp_action_authorized(self.config.lsp, self.effective_file_mode(), action) {
            return Err(ToolError::reported_failure(
                FailureClass::PolicyDenied,
                "lsp action is not authorized for this file-tool mode".into(),
            ));
        }
        if matches!(action, LspAction::RequestRead | LspAction::RequestWrite) {
            let method = args.query.as_deref().unwrap_or("");
            actions::validate_raw_request(&self.context, method, args.payload.as_deref())?;
        }
        let workspace = self.effective_workspace();
        let detected = catalog::detect_admitted_servers(&workspace, &self.config.servers);
        let lease = if action.may_cold_start() {
            let file = args.file.as_deref().unwrap_or("");
            let is_glob = file.contains(['*', '?', '{', '[']);
            let path = if file.is_empty() || file == "*" {
                None
            } else if is_glob {
                let paths = crate::toolset::native_runner::NativeFsRunner::new(&self.context)
                    .glob_paths(file, actions::MAX_GLOB_TARGETS)
                    .await
                    .unwrap_or_default();
                if paths.is_empty() {
                    return Ok("no files matched the diagnostic glob".into());
                }
                let primary_names = paths
                    .iter()
                    .filter_map(|path| primary_for_file(&detected, path))
                    .map(|server| server.name.as_str())
                    .collect::<std::collections::BTreeSet<_>>();
                if primary_names.len() > 1 {
                    return Err(ToolError::reported_failure(
                        FailureClass::ArgumentInvalid,
                        "diagnostic glob spans multiple language servers; split it by file type"
                            .into(),
                    ));
                }
                paths
                    .iter()
                    .find(|path| primary_for_file(&detected, path).is_some())
                    .cloned()
                    .or_else(|| paths.into_iter().next())
            } else {
                Some(
                    edits::resolve_inbound_path(&self.context, file).map_err(|err| {
                        ToolError::reported_failure(FailureClass::PolicyDenied, err)
                    })?,
                )
            };
            let server = match path.as_ref() {
                Some(path) => primary_for_file(&detected, path).cloned(),
                None => catalog::primary_for_workspace(&detected).cloned(),
            };
            if let Some(server) = server {
                let session_id =
                    crate::tool_call_lifecycle::runtime::current_tool_runtime_context()
                        .and_then(|scope| scope.session_id)
                        .filter(|id| !id.is_empty())
                        .unwrap_or_else(|| self.config.session_id.clone());
                let key = PoolKey {
                    session_id,
                    behavior_id: self.config.behavior_id.clone(),
                    workspace_root: workspace.clone(),
                    server_name: server.name.clone(),
                    config_digest: self.config.digest.clone(),
                };
                Some(
                    self.pool
                        .get_or_start(key, &server, &self.config)
                        .await
                        .map_err(|err| {
                            ToolError::reported_failure(FailureClass::ServiceUnavailable, err)
                        })?,
                )
            } else {
                return Err(ToolError::reported_failure(
                    FailureClass::ServiceUnavailable,
                    catalog::unavailable_servers_message(
                        &workspace,
                        &self.config.servers,
                        path.as_deref(),
                    ),
                ));
            }
        } else {
            None
        };
        actions::dispatch(
            &self.context,
            lease.as_ref(),
            &self.pool,
            &self.config,
            &detected,
            &workspace,
            ActionRequest {
                action,
                file: args.file,
                line: args.line,
                symbol: args.symbol,
                query: args.query,
                new_name: args.new_name,
                apply: args.apply,
                payload: args.payload,
                timeout: args.timeout,
            },
        )
        .await
    }

    fn into_dyn_error(error: Self::Error) -> crate::llm::tool::ToolError {
        error.into_dispatch_error()
    }
}

pub(crate) fn overlay_workspace_or(fallback: &Path) -> PathBuf {
    crate::tool_call_lifecycle::runtime::current_tool_runtime_context()
        .and_then(|scope| scope.workspace_root)
        .unwrap_or_else(|| fallback.to_path_buf())
}

pub(crate) fn overlay_lsp_constraints(base: &CommandConstraints) -> CommandConstraints {
    let Some(authority) = crate::tool_call_lifecycle::runtime::current_tool_runtime_context()
        .and_then(|scope| scope.workspace_authority)
    else {
        return base.clone();
    };
    let mut constraints = base.clone();
    constraints.execution_mode = constraints.execution_mode.meet(authority.command_mode());
    constraints.sandbox = lsp_sandbox_for_effective(constraints.execution_mode);
    constraints
}

pub fn merge_catalog(raw_config: Option<&str>) -> Vec<CatalogServer> {
    let doc = LspConfigDocument::parse_operator(raw_config).unwrap_or_default();
    apply_overrides(builtin_catalog(), &doc)
}

pub fn config_digest(
    workspace: &std::path::Path,
    servers: &[CatalogServer],
    constraints: &CommandConstraints,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(workspace.to_string_lossy().as_bytes());
    hasher.update(format!("{:?}", constraints.execution_mode).as_bytes());
    hasher.update(format!("{:?}", constraints.sandbox).as_bytes());
    hasher.update(format!("{:?}", constraints.network_mode).as_bytes());
    hasher.update(constraints.deny_all_argv.to_string().as_bytes());
    hasher.update(constraints.deny_git_metadata_writes.to_string().as_bytes());
    for prefix in &constraints.allowed_argv_prefixes {
        hasher.update(prefix.join("\0").as_bytes());
    }
    for prefix in &constraints.forbidden_argv_prefixes {
        hasher.update(prefix.join("\0").as_bytes());
    }
    for server in servers {
        hasher.update(server.name.as_bytes());
        if let Ok(canonical) = admit_command(&server.command, workspace) {
            hasher.update(canonical.to_string_lossy().as_bytes());
        } else {
            hasher.update(server.command.as_bytes());
        }
        for arg in &server.args {
            hasher.update(arg.as_bytes());
        }
        if let Some(language_id) = &server.language_id {
            hasher.update(language_id.as_bytes());
        }
        if let Some(init) = &server.init_options {
            hasher.update(init.to_string().as_bytes());
        }
        if let Some(settings) = &server.settings {
            hasher.update(settings.to_string().as_bytes());
        }
        if let Some(caps) = &server.capabilities {
            hasher.update(caps.to_string().as_bytes());
        }
        if let Some(timings) = &server.workspace_ready_timings {
            hasher.update(timings.to_string().as_bytes());
        }
    }
    format!("{:x}", hasher.finalize())
}
