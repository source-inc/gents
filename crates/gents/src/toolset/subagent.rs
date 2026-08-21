use crate::llm::tool::Tool;
use crate::llm::tool::ToolDefinition;
use anyhow::anyhow;

use crate::background_tools::r4c_args::{
    ListBackgroundToolsArgs, ListSubagentsArgs, ReadSubagentArgs, ReadToolOutputArgs,
    SteerSubagentArgs,
};
use crate::background_tools::{
    BackgroundToolArgs, CancelSubagentArgs, CancelToolArgs, SpawnSubagentArgs, WaitSubagentArgs,
    WaitToolArgs, DEFAULT_WAIT_PROCESS_TIMEOUT_SECS, MAX_WAIT_PROCESS_TIMEOUT_SECS,
};
use crate::tool_call_lifecycle::AwaitMode;
use crate::tool_surface::{BackgroundToolConfig, SubagentToolConfig};

use super::shared::ToolError;
use super::{
    CANCEL_PROCESS_TOOL_NAME, CANCEL_SUBAGENT_TOOL_NAME, LIST_PROCESSES_TOOL_NAME,
    LIST_SUBAGENTS_TOOL_NAME, READ_PROCESS_TOOL_NAME, READ_SUBAGENT_TOOL_NAME,
    SPAWN_PROCESS_TOOL_NAME, SPAWN_SUBAGENT_TOOL_NAME, STEER_SUBAGENT_TOOL_NAME,
    WAIT_PROCESS_TOOL_NAME, WAIT_SUBAGENT_TOOL_NAME,
};

const SUBAGENT_SERVICE_ID: &str = "subagent";

#[derive(Clone)]
pub(super) struct SpawnSubagentTool {
    config: SubagentToolConfig,
}

#[derive(Clone)]
pub(super) struct SpawnProcessTool {
    config: BackgroundToolConfig,
}

impl SpawnSubagentTool {
    pub(super) fn new(config: SubagentToolConfig) -> Self {
        Self { config }
    }

    fn validate(&self, args: &SpawnSubagentArgs) -> Result<(), ToolError> {
        let name = args.name.trim();
        if name.is_empty() {
            return Err(invalid_arguments_error(
                SPAWN_SUBAGENT_TOOL_NAME,
                "/name",
                "name is required",
            ));
        }
        if !self.config.targets.iter().any(|target| target.name == name) {
            return Err(tool_not_allowed_error(
                SPAWN_SUBAGENT_TOOL_NAME,
                "/name",
                name,
                format!("'{name}' is not an allowed subagent target for this behavior"),
                self.allowed_target_names(),
            ));
        }
        if args.prompt.trim().is_empty() {
            return Err(invalid_arguments_error(
                SPAWN_SUBAGENT_TOOL_NAME,
                "/prompt",
                "prompt is required",
            ));
        }

        match args
            .await_mode
            .map(|mode| mode.as_await_mode())
            .unwrap_or(self.config.default_await_mode)
        {
            AwaitMode::Foreground => {}
            AwaitMode::Background if self.config.background_enabled => {}
            AwaitMode::Background => {
                return Err(tool_not_allowed_error(
                    SPAWN_SUBAGENT_TOOL_NAME,
                    "/await_mode",
                    "background",
                    "background subagent spawning is not enabled for this behavior",
                    self.allowed_target_names(),
                ));
            }
        }

        Ok(())
    }

    fn allowed_target_names(&self) -> Vec<String> {
        self.config
            .targets
            .iter()
            .map(|target| target.name.clone())
            .collect()
    }
}

impl SpawnProcessTool {
    pub(super) fn new(config: BackgroundToolConfig) -> Self {
        Self { config }
    }

    fn validate(&self, args: &BackgroundToolArgs) -> Result<(), ToolError> {
        let tool_name = args.tool_name.trim();
        if tool_name.is_empty() {
            return Err(background_invalid_arguments_error(
                SPAWN_PROCESS_TOOL_NAME,
                "/tool_name",
                "tool_name is required",
            ));
        }
        if !self.config.allowlist.iter().any(|name| name == tool_name) {
            return Err(background_tool_not_allowed_error(
                SPAWN_PROCESS_TOOL_NAME,
                "/tool_name",
                tool_name,
                format!("tool '{tool_name}' is not allowed for backgrounding by this behavior"),
                self.config.allowlist.clone(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) struct WaitSubagentTool;

#[derive(Clone, Copy)]
pub(super) struct ListSubagentsTool;

#[derive(Clone, Copy)]
pub(super) struct ReadSubagentTool;

#[derive(Clone, Copy)]
pub(super) struct SteerSubagentTool;

#[derive(Clone, Copy)]
pub(super) struct CancelSubagentTool;

#[derive(Clone, Copy)]
pub(super) struct WaitProcessTool;

#[derive(Clone, Copy)]
pub(super) struct ListProcessesTool;

#[derive(Clone, Copy)]
pub(super) struct ReadProcessTool;

#[derive(Clone, Copy)]
pub(super) struct CancelProcessTool;

impl Tool for SpawnSubagentTool {
    const NAME: &'static str = SPAWN_SUBAGENT_TOOL_NAME;

    type Error = ToolError;
    type Args = SpawnSubagentArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        let await_modes = if self.config.background_enabled {
            vec!["foreground", "background"]
        } else {
            vec!["foreground"]
        };

        let allowed_names = self.allowed_target_names();
        let name_description = subagent_target_name_description(&self.config.targets);

        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Spawn an authorized subagent by its friendly name. Foreground is the default await mode; background is available only when enabled for this behavior. When the parent request is workspace-bound, omit workspace or pass inherit to share that workspace with authority infimum. Pass {\"id\": \"...\"} to bind an existing Ready/Sealed IsolatedWorkspace, or {\"provision\": {\"policy\": \"git_worktree_diff\"}} to create a child IsolatedWorkspace."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "enum": allowed_names,
                        "description": name_description
                    },
                    "prompt": {
                        "type": "string",
                        "description": "Task prompt to send to the child subagent."
                    },
                    "await_mode": {
                        "type": "string",
                        "enum": await_modes,
                        "default": self.config.default_await_mode.as_str(),
                        "description": "Use foreground to wait for the child result. Use background only when the schema exposes it."
                    },
                    "workspace": {
                        "description": "inherit (default when the parent has workspace_id), {\"id\": \"<workspace_id>\"} to bind an existing Ready/Sealed workspace, or {\"provision\": {\"policy\": \"git_worktree_diff\"}} to create a child IsolatedWorkspace. Child authority cannot outrank the parent.",
                        "oneOf": [
                            { "type": "string", "enum": ["inherit"] },
                            {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["id"],
                                "properties": {
                                    "id": { "type": "string", "minLength": 1 },
                                    "authority": {
                                        "type": "string",
                                        "enum": ["readOnly", "readWrite", "integrate"]
                                    }
                                }
                            },
                            {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["provision"],
                                "properties": {
                                    "provision": {
                                        "type": "object",
                                        "additionalProperties": false,
                                        "properties": {
                                            "policy": {
                                                "type": "string",
                                                "enum": ["git_worktree_diff"]
                                            }
                                        }
                                    }
                                }
                            }
                        ]
                    }
                },
                "required": ["name", "prompt"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        self.validate(&args)?;
        Err(not_yet_executable_error(Self::NAME))
    }
}

impl Tool for WaitSubagentTool {
    const NAME: &'static str = WAIT_SUBAGENT_TOOL_NAME;

    type Error = ToolError;
    type Args = WaitSubagentArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Wait for an existing child subagent request to reach a terminal state."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "child_request_id": {
                        "type": "string",
                        "description": "Child AgentRequest ID returned by spawn_subagent."
                    }
                },
                "required": ["child_request_id"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        validate_child_request_id(WAIT_SUBAGENT_TOOL_NAME, &args.child_request_id)?;
        Err(not_yet_executable_error(Self::NAME))
    }
}

impl Tool for CancelSubagentTool {
    const NAME: &'static str = CANCEL_SUBAGENT_TOOL_NAME;

    type Error = ToolError;
    type Args = CancelSubagentArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Cancel an existing child subagent request and its live descendants."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "child_request_id": {
                        "type": "string",
                        "description": "Child AgentRequest ID returned by spawn_subagent."
                    },
                    "reason": {
                        "type": "string",
                        "description": "Optional human-readable cancellation reason."
                    }
                },
                "required": ["child_request_id"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        validate_child_request_id(CANCEL_SUBAGENT_TOOL_NAME, &args.child_request_id)?;
        if args
            .reason
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty())
        {
            return Err(invalid_arguments_error(
                CANCEL_SUBAGENT_TOOL_NAME,
                "/reason",
                "reason must be omitted or non-empty",
            ));
        }
        Err(not_yet_executable_error(Self::NAME))
    }
}

impl Tool for ListSubagentsTool {
    const NAME: &'static str = LIST_SUBAGENTS_TOOL_NAME;

    type Error = ToolError;
    type Args = ListSubagentsArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "List the canonical descendant graph owned by this parent lineage. \
Foreground, background, cross-behavior, and pending remote children use the same edge shape. \
Use `scope=all_descendants` for nested children."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["running", "terminal", "all"],
                        "default": "running",
                        "description": "Filter child subagents by bridge lifecycle state."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 20,
                        "description": "Maximum entries to return."
                    },
                    "scope": {
                        "type": "string",
                        "enum": ["direct_children", "all_descendants"],
                        "default": "direct_children",
                        "description": "Graph scope to enumerate."
                    },
                    "after": {
                        "type": "string",
                        "description": "Opaque cursor returned as next_cursor by a previous page."
                    }
                }
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let _ = args.validated_limit();
        Err(not_yet_executable_error(Self::NAME))
    }
}

impl Tool for ReadSubagentTool {
    const NAME: &'static str = READ_SUBAGENT_TOOL_NAME;

    type Error = ToolError;
    type Args = ReadSubagentArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description:
                "Read an incremental transcript slice from any visible canonical descendant. \
Paging is cursor-based and content-honest: pass `since_sequence` to resume, and the response \
returns `next_sequence` (the exact cursor to pass next), `has_more` (true when the token budget \
capped the read and more messages remain past `next_sequence`), and `terminal`/`lifecycle_state` \
(whether the subagent has finished). If the child request has not materialized yet, the response \
has an empty transcript, `lifecycle_state = \"awaiting_child_materialization\"`, and a \
`diagnostic` explaining the bridge state — keep polling. Output is never SILENTLY dropped: when `has_more` is true, \
read again with `since_sequence = next_sequence` to continue. One case is truncated rather than \
paged, but always explicitly marked: a single message larger than the entire budget is cut with a \
`[truncated: showed N of M chars]` marker and the cursor advances past it, so that one message's \
tail is not separately pageable."
                    .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "child_request_id": {
                        "type": "string",
                        "description": "Child request ID returned by spawn_subagent or list_subagents."
                    },
                    "since_sequence": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 0,
                        "description": "Resume cursor: first transcript sequence to include (0 = from start). Pass the prior response's `next_sequence` to continue."
                    },
                    "max_tokens": {
                        "type": "integer",
                        "minimum": 16,
                        "maximum": 6000,
                        "default": 1500,
                        "description": "Token budget for the returned transcript slice (estimated as chars/4). When the budget caps the read, `has_more` is true and `next_sequence` marks the resume point."
                    },
                    "include_user_messages": {
                        "type": "boolean",
                        "default": false,
                        "description": "Include ordinary user messages from the child session."
                    },
                    "include_tool_results": {
                        "type": "boolean",
                        "default": false,
                        "description": "Include capped tool-result snippets from the child session."
                    }
                },
                "required": ["child_request_id"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        validate_child_request_id(Self::NAME, &args.child_request_id)?;
        let _ = args.validated_max_tokens();
        let _ = args.validated_max_chars();
        Err(not_yet_executable_error(Self::NAME))
    }
}

impl Tool for SteerSubagentTool {
    const NAME: &'static str = STEER_SUBAGENT_TOOL_NAME;

    type Error = ToolError;
    type Args = SteerSubagentArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Append a steering message to a visible background subagent, optionally interrupting its active request first."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "child_request_id": {
                        "type": "string",
                        "description": "Child request ID returned by spawn_subagent or list_subagents."
                    },
                    "message": {
                        "type": "string",
                        "description": "User-role steering message to append to the child session."
                    },
                    "interrupt": {
                        "type": "boolean",
                        "default": false,
                        "description": "Interrupt the child session's active request before appending the steering request."
                    }
                },
                "required": ["child_request_id", "message"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        validate_child_request_id(Self::NAME, &args.child_request_id)?;
        if args.message.trim().is_empty() {
            return Err(invalid_arguments_error(
                Self::NAME,
                "/message",
                "message is required",
            ));
        }
        Err(not_yet_executable_error(Self::NAME))
    }
}

impl Tool for SpawnProcessTool {
    const NAME: &'static str = SPAWN_PROCESS_TOOL_NAME;

    type Error = ToolError;
    type Args = BackgroundToolArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Spawn a background process by running an allowlisted long-running tool (e.g. a shell command). Returns a process handle immediately. You are notified automatically when it completes — end your turn instead of polling or sleep-waiting; use wait_process only for short bounded waits."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "tool_name": {
                        "type": "string",
                        "enum": &self.config.allowlist,
                        "description": "Allowlisted tool name to run as a background process."
                    },
                    "args": {
                        "type": "object",
                        "description": "Arguments passed to the target tool."
                    }
                },
                "required": ["tool_name", "args"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        self.validate(&args)?;
        Err(background_not_yet_executable_error(Self::NAME))
    }
}

impl Tool for WaitProcessTool {
    const NAME: &'static str = WAIT_PROCESS_TOOL_NAME;

    type Error = ToolError;
    type Args = WaitToolArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: format!(
                "Wait up to timeout_secs for a background process in this session to reach a terminal state, including a handle returned on an earlier turn. A wait timeout, caller interruption, or caller deadline returns status \"running\" without cancelling the process. You are notified when it completes, so prefer ending your turn over waiting repeatedly. Default {DEFAULT_WAIT_PROCESS_TIMEOUT_SECS}s, maximum {MAX_WAIT_PROCESS_TIMEOUT_SECS}s."
            ),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "tool_call_id": {
                        "type": "string",
                        "description": "Process handle returned by spawn_process."
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "default": DEFAULT_WAIT_PROCESS_TIMEOUT_SECS,
                        "minimum": 1,
                        "maximum": MAX_WAIT_PROCESS_TIMEOUT_SECS,
                        "description": "How long to wait before returning a still-running snapshot; the process is never cancelled by a wait timeout."
                    }
                },
                "required": ["tool_call_id"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        validate_tool_call_id(WAIT_PROCESS_TOOL_NAME, &args.tool_call_id)?;
        Err(background_not_yet_executable_error(Self::NAME))
    }
}

impl Tool for ListProcessesTool {
    const NAME: &'static str = LIST_PROCESSES_TOOL_NAME;

    type Error = ToolError;
    type Args = ListBackgroundToolsArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "List background processes manageable by this session principal, including processes started on earlier turns.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["running", "terminal", "all"],
                        "default": "running",
                        "description": "Filter background processes by lifecycle state."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 20,
                        "description": "Maximum entries to return."
                    }
                }
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let _ = args.validated_limit();
        Err(background_not_yet_executable_error(Self::NAME))
    }
}

impl Tool for ReadProcessTool {
    const NAME: &'static str = READ_PROCESS_TOOL_NAME;

    type Error = ToolError;
    type Args = ReadToolOutputArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Read an incremental, byte-addressed slice of a manageable background process in this session, including one started on an earlier turn. \
captured output. stdout and stderr are merged in capture order behind a single byte cursor \
(stdout first, then a `--- stderr ---` boundary, then stderr), so you page through ALL output \
gap-free with one `offset`. The response returns `output` (the slice), `next_offset` (= offset + \
bytes returned; the exact cursor to pass next), `total_bytes` (total captured so far), `has_more` \
(true when `next_offset < total_bytes`), and `exited`/`exit_code` (whether the process finished). \
To read everything, start at offset 0 and loop with `offset = next_offset` until `has_more` is \
false. Pages are contiguous from the cursor; nothing in the middle is dropped."
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "tool_call_id": {
                        "type": "string",
                        "description": "Process handle returned by spawn_process or list_processes."
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 0,
                        "description": "Byte cursor into the combined output (0 = from start). Reads forward from here. Pass the prior response's `next_offset` to continue gap-free."
                    },
                    "max_tokens": {
                        "type": "integer",
                        "minimum": 64,
                        "maximum": 65536,
                        "default": 4096,
                        "description": "Token budget for the returned slice (estimated as chars/4). When the budget caps the slice, `has_more` is true and `next_offset` marks the resume point."
                    }
                },
                "required": ["tool_call_id"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        validate_tool_call_id(Self::NAME, &args.tool_call_id)?;
        let _ = args.validated_max_bytes();
        Err(background_not_yet_executable_error(Self::NAME))
    }
}

impl Tool for CancelProcessTool {
    const NAME: &'static str = CANCEL_PROCESS_TOOL_NAME;

    type Error = ToolError;
    type Args = CancelToolArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Cancel a manageable running background process in this session, including one started on an earlier turn.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "tool_call_id": {
                        "type": "string",
                        "description": "Process handle returned by spawn_process."
                    },
                    "reason": {
                        "type": "string",
                        "description": "Optional human-readable cancellation reason."
                    }
                },
                "required": ["tool_call_id"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        validate_tool_call_id(CANCEL_PROCESS_TOOL_NAME, &args.tool_call_id)?;
        if args
            .reason
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty())
        {
            return Err(background_invalid_arguments_error(
                CANCEL_PROCESS_TOOL_NAME,
                "/reason",
                "reason must be omitted or non-empty",
            ));
        }
        Err(background_not_yet_executable_error(Self::NAME))
    }
}

fn subagent_target_name_description(targets: &[crate::document_config::SubagentTarget]) -> String {
    let mut description = String::from(
        "Friendly name of the subagent to spawn, from this behavior's allowed targets.",
    );
    let entries: Vec<String> = targets
        .iter()
        .map(|target| {
            let desc = target.description_text();
            if desc.is_empty() {
                format!("'{}'", target.name)
            } else {
                format!("'{}': {}", target.name, desc)
            }
        })
        .collect();
    if !entries.is_empty() {
        description.push_str(" Available: ");
        description.push_str(&entries.join("; "));
        description.push('.');
    }
    description
}

fn validate_child_request_id(tool_name: &str, child_request_id: &str) -> Result<(), ToolError> {
    if child_request_id.trim().is_empty() {
        return Err(invalid_arguments_error(
            tool_name,
            "/child_request_id",
            "child_request_id is required",
        ));
    }
    Ok(())
}

fn validate_tool_call_id(tool_name: &str, tool_call_id: &str) -> Result<(), ToolError> {
    if tool_call_id.trim().is_empty() {
        return Err(background_invalid_arguments_error(
            tool_name,
            "/tool_call_id",
            "tool_call_id is required",
        ));
    }
    Ok(())
}

fn invalid_arguments_error(tool_name: &str, path: &str, message: impl Into<String>) -> ToolError {
    structured_error(serde_json::json!({
        "ok": false,
        "failure_class": "invalid_tool_arguments",
        "path": path,
        "message": message.into(),
        "retryable": false,
        "service_id": SUBAGENT_SERVICE_ID,
        "tool_name": tool_name
    }))
}

fn tool_not_allowed_error(
    tool_name: &str,
    path: &str,
    requested: &str,
    message: impl Into<String>,
    allowed_targets: Vec<String>,
) -> ToolError {
    structured_error(serde_json::json!({
        "ok": false,
        "failure_class": "tool_not_allowed",
        "path": path,
        "message": message.into(),
        "retryable": false,
        "service_id": SUBAGENT_SERVICE_ID,
        "tool_name": tool_name,
        "requested_tool_name": requested,
        "allowed_subagent_targets": allowed_targets
    }))
}

fn not_yet_executable_error(tool_name: &str) -> ToolError {
    structured_error(serde_json::json!({
        "ok": false,
        "failure_class": "service_unavailable",
        "path": "/",
        "message": format!("{tool_name} is registered but requires the R4b subagent hook runtime path before direct execution"),
        "retryable": true,
        "service_id": SUBAGENT_SERVICE_ID,
        "tool_name": tool_name
    }))
}

fn background_invalid_arguments_error(
    tool_name: &str,
    path: &str,
    message: impl Into<String>,
) -> ToolError {
    structured_error(serde_json::json!({
        "ok": false,
        "failure_class": "invalid_tool_arguments",
        "path": path,
        "message": message.into(),
        "retryable": false,
        "service_id": "process",
        "tool_name": tool_name
    }))
}

fn background_tool_not_allowed_error(
    tool_name: &str,
    path: &str,
    requested: &str,
    message: impl Into<String>,
    allowed_targets: Vec<String>,
) -> ToolError {
    structured_error(serde_json::json!({
        "ok": false,
        "failure_class": "tool_not_allowed",
        "path": path,
        "message": message.into(),
        "retryable": false,
        "service_id": "process",
        "tool_name": tool_name,
        "requested_tool_name": requested,
        "allowed_backgroundable_tool_names": allowed_targets
    }))
}

fn background_not_yet_executable_error(tool_name: &str) -> ToolError {
    structured_error(serde_json::json!({
        "ok": false,
        "failure_class": "service_unavailable",
        "path": "/",
        "message": format!("{tool_name} is registered but requires the R6 process hook runtime path before direct execution"),
        "retryable": true,
        "service_id": "process",
        "tool_name": tool_name
    }))
}

fn structured_error(error: serde_json::Value) -> ToolError {
    let message = serde_json::to_string_pretty(&error).unwrap_or_else(|_| error.to_string());
    anyhow!(message).into()
}
