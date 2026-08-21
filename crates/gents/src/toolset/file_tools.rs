use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use crate::llm::tool::Tool;
use crate::llm::tool::ToolDefinition;
use anyhow::{anyhow, Context as _, Result};
use gents_fs_runner::protocol::{
    GlobArgs as NativeGlobArgs, GrepArgs as NativeGrepArgs, ListFilesArgs as NativeListFilesArgs,
    NativeFsRunnerRequest,
};
use serde::Serialize;

use super::args::{EditFileArgs, GlobArgs, GrepArgs, ListFilesArgs, ReadFileArgs, WriteFileArgs};
use super::edit_match::{self, EditOutcome, EditRequest, MatchMode, Operation};
use super::native_runner::NativeFsRunner;
use super::shared::{cap_output, render_file_contents, ToolContext, ToolError};
use crate::tool_call_lifecycle::FailureClass;

fn deny_workspace_file_writes() -> Result<(), ToolError> {
    if crate::tool_call_lifecycle::runtime::current_tool_runtime_context()
        .and_then(|scope| scope.workspace_authority)
        .is_some_and(|authority| !authority.allows_file_writes())
    {
        return Err(ToolError::reported_failure(
            FailureClass::PolicyDenied,
            "workspace authority does not allow file writes".into(),
        ));
    }
    Ok(())
}

fn merge_optional_notes(left: Option<String>, right: Option<String>) -> Option<String> {
    match (left, right) {
        (None, None) => None,
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (Some(a), Some(b)) => Some(format!("{a}\n{b}")),
    }
}

pub(crate) fn content_hash(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("sha256:{:x}", Sha256::digest(bytes))
}

/// Per-path serialization for FILE MUTATORS — edit_file's read →
/// hash-check → match → write sequence AND write_file's overwrite share it:
/// without it, a concurrent mutation can land inside edit_file's validated
/// window and be silently overwritten (lost update). Scope: WITHIN this
/// process, keyed by canonical path (falling back to the resolved path for
/// not-yet-existing files). External writers are outside the lock — they
/// are caught by the expected_content_hash check at entry and otherwise
/// race as ordinary last-writer-wins POSIX writes (#724 documents the
/// optimistic-concurrency boundary; there is no OS-level file lock).
static FILE_MUTATION_LOCKS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<PathBuf, std::sync::Arc<tokio::sync::Mutex<()>>>>,
> = std::sync::LazyLock::new(Default::default);

fn canonical_lock_key(path: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }
    let mut suffix: Vec<std::ffi::OsString> = Vec::new();
    let mut current = path;
    while let Some(parent) = current.parent() {
        match current.file_name() {
            Some(name) => suffix.push(name.to_os_string()),
            None => break,
        }
        if let Ok(canonical) = std::fs::canonicalize(parent) {
            let mut key = canonical;
            for part in suffix.iter().rev() {
                key.push(part);
            }
            return key;
        }
        current = parent;
    }
    path.to_path_buf()
}

pub(crate) fn file_mutation_lock_for(path: &Path) -> std::sync::Arc<tokio::sync::Mutex<()>> {
    let key = canonical_lock_key(path);
    let mut locks = FILE_MUTATION_LOCKS
        .lock()
        .expect("file mutation lock registry poisoned");
    locks.entry(key).or_default().clone()
}

const OUTPUT_META_PREFIX: &str = "gents_fs: ";

#[derive(Clone)]
pub(super) struct ListFilesTool {
    context: ToolContext,
    native_runner: NativeFsRunner,
    default_max_entries: usize,
}

impl ListFilesTool {
    pub(super) fn new(context: ToolContext, default_max_entries: usize) -> Self {
        Self {
            native_runner: NativeFsRunner::new(&context),
            context,
            default_max_entries,
        }
    }
}

#[derive(Clone)]
pub(super) struct ReadFileTool {
    context: ToolContext,
    default_max_chars: usize,
}

impl ReadFileTool {
    pub(super) fn new(context: ToolContext, default_max_chars: usize) -> Self {
        Self {
            context,
            default_max_chars,
        }
    }
}

#[derive(Clone)]
pub(super) struct GlobTool {
    native_runner: NativeFsRunner,
    default_max_matches: usize,
}

impl GlobTool {
    pub(super) fn new(context: ToolContext, default_max_matches: usize) -> Self {
        Self {
            native_runner: NativeFsRunner::new(&context),
            default_max_matches,
        }
    }
}

#[derive(Clone)]
pub(super) struct GrepTool {
    native_runner: NativeFsRunner,
    default_max_matches: usize,
}

impl GrepTool {
    pub(super) fn new(context: ToolContext, default_max_matches: usize) -> Self {
        Self {
            native_runner: NativeFsRunner::new(&context),
            default_max_matches,
        }
    }
}

#[derive(Clone)]
pub(super) struct WriteFileTool {
    context: ToolContext,
    writethrough: Option<crate::toolset::lsp::LspWritethrough>,
}

impl WriteFileTool {
    pub(super) fn new(context: ToolContext) -> Self {
        Self {
            context,
            writethrough: None,
        }
    }

    pub(super) fn with_writethrough(
        mut self,
        writethrough: crate::toolset::lsp::LspWritethrough,
    ) -> Self {
        self.writethrough = Some(writethrough);
        self
    }
}

#[derive(Clone)]
pub(super) struct EditFileTool {
    context: ToolContext,
    writethrough: Option<crate::toolset::lsp::LspWritethrough>,
}

impl EditFileTool {
    pub(super) fn new(context: ToolContext) -> Self {
        Self {
            context,
            writethrough: None,
        }
    }

    pub(super) fn with_writethrough(
        mut self,
        writethrough: crate::toolset::lsp::LspWritethrough,
    ) -> Self {
        self.writethrough = Some(writethrough);
        self
    }
}

impl Tool for ListFilesTool {
    const NAME: &'static str = "list_files";

    type Error = ToolError;
    type Args = ListFilesArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: format!(
                "List files and directories under the allowed root ({}). Relative paths resolve from the active request workspace when one is provided, otherwise from the root. Returns compact text with stable gents_fs metadata, skips common generated directories and paths ignored by in-tree .gitignore files by default, and reports walk stats; large walks stop at a budget with partial results (walk.budget_exhausted=true). Set raw_json=true for structured JSON.",
                self.context.root().display()
            ),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "default": ".",
                        "description": "Directory to list, relative to the active workspace/root. Omit or pass an empty string for the active workspace/root."
                    },
                    "recursive": {
                        "type": "boolean",
                        "default": false,
                        "description": "When true, walk subdirectories while still skipping common generated directories."
                    },
                    "max_entries": {
                        "type": "integer",
                        "default": self.default_max_entries,
                        "minimum": 1,
                        "maximum": self.default_max_entries,
                        "description": "Maximum entries to return; higher values are capped by the tool."
                    },
                    "raw_json": {
                        "type": "boolean",
                        "default": false,
                        "description": "When true, return structured JSON instead of the compact default text."
                    }
                }
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        self.native_runner
            .run(
                NativeFsRunnerRequest::ListFiles(NativeListFilesArgs {
                    path: args.path,
                    recursive: args.recursive,
                    max_entries: args.max_entries.max(1).min(self.default_max_entries.max(1)),
                    raw_json: args.raw_json,
                    max_entries_visited: None,
                    max_wall_ms: None,
                }),
                Self::NAME,
            )
            .await
    }

    fn into_dyn_error(error: Self::Error) -> crate::llm::tool::ToolError {
        error.into_dispatch_error()
    }
}

impl Tool for ReadFileTool {
    const NAME: &'static str = "read_file";

    type Error = ToolError;
    type Args = ReadFileArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: format!(
                "Read a UTF-8 text file under the allowed root ({}). Relative paths resolve from the active request workspace when one is provided, otherwise from the root. Returns compact line-numbered text with stable gents_fs metadata, including content_hash — the raw-byte identity of the whole file, usable as edit_file expected_content_hash to guard against concurrent changes. Set raw_json=true for structured JSON.",
                self.context.root().display()
            ),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "File to read, relative to the active workspace/root unless an allowed absolute path is provided."
                    },
                    "start_line": {
                        "type": "integer",
                        "default": 1,
                        "minimum": 1,
                        "description": "First 1-based line to return."
                    },
                    "end_line": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Last 1-based line to return. Omit to read through the file end."
                    },
                    "max_chars": {
                        "type": "integer",
                        "default": self.default_max_chars,
                        "minimum": 1,
                        "maximum": self.default_max_chars,
                        "description": "Maximum characters to return; higher values are capped by the tool."
                    },
                    "raw_json": {
                        "type": "boolean",
                        "default": false,
                        "description": "When true, return structured JSON instead of the compact default text."
                    }
                },
                "required": ["path"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let path = self.context.resolve_existing_file(&args.path)?;
        let bytes = tokio::fs::read(&path).await?;
        let content_hash = content_hash(&bytes);
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let rendered = render_file_contents(&text, args.start_line, args.end_line);
        let max_chars = args.max_chars.min(self.default_max_chars).max(1);
        let (content, truncated) = cap_output(&rendered.content, max_chars);

        let output = ReadFileOutput {
            metadata: ReadFileMetadata {
                ok: true,
                status: "success",
                tool: Self::NAME,
                path: self.context.display_path(&path),
                returned_count: rendered.returned_lines,
                total_count: Some(rendered.total_lines),
                truncated,
                start_line: rendered.start_line,
                end_line: rendered.end_line,
                content_hash,
            },
            content,
        };

        Ok(render_tool_output(
            &output.metadata,
            format!("content:\n{}", output.content),
            &output,
            args.raw_json,
        )?)
    }

    fn into_dyn_error(error: Self::Error) -> crate::llm::tool::ToolError {
        error.into_dispatch_error()
    }
}

impl Tool for GlobTool {
    const NAME: &'static str = "glob";

    type Error = ToolError;
    type Args = GlobArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Find files matching a glob pattern (supports *, ?, **, [..], and {a,b} alternation) under the allowed root. Relative paths resolve from the active request workspace when one is provided, otherwise from the root. The pattern is matched against the FULL path relative to that directory, so it must include every leading directory (or start with **/); check the search_dir_entries / pattern_prefix_exists fields on a zero-match result before retrying. Returns compact text with stable gents_fs metadata, skips common generated directories and paths ignored by in-tree .gitignore files by default, and reports walk stats; large walks stop at a budget with partial results (walk.budget_exhausted=true). Set raw_json=true for structured JSON.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern matched against paths displayed relative to the active workspace/root."
                    },
                    "path": {
                        "type": "string",
                        "default": ".",
                        "description": "Directory to search, relative to the active workspace/root. Omit or pass an empty string for the active workspace/root."
                    },
                    "max_matches": {
                        "type": "integer",
                        "default": self.default_max_matches,
                        "minimum": 1,
                        "maximum": self.default_max_matches,
                        "description": "Maximum matching paths to return; higher values are capped by the tool."
                    },
                    "raw_json": {
                        "type": "boolean",
                        "default": false,
                        "description": "When true, return structured JSON instead of the compact default text."
                    }
                },
                "required": ["pattern"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        self.native_runner
            .run(
                NativeFsRunnerRequest::Glob(NativeGlobArgs {
                    pattern: args.pattern,
                    path: args.path,
                    max_matches: args.max_matches.min(self.default_max_matches).max(1),
                    raw_json: args.raw_json,
                    max_entries_visited: None,
                    max_wall_ms: None,
                }),
                Self::NAME,
            )
            .await
    }

    fn into_dyn_error(error: Self::Error) -> crate::llm::tool::ToolError {
        error.into_dispatch_error()
    }
}

impl Tool for GrepTool {
    const NAME: &'static str = "grep";

    type Error = ToolError;
    type Args = GrepArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Search text files under the allowed root with a regular expression (Rust regex syntax, case-insensitive by default; a pattern that fails to parse as regex is used as a literal substring — the result metadata reports pattern_syntax accordingly). Relative paths resolve from the active request workspace when one is provided, otherwise from the root. The path may be a directory or a single file; prefer passing the narrowest directory you can. Returns compact path:Lline matches with stable gents_fs metadata, skips common generated directories, paths ignored by in-tree .gitignore files, oversized files, and binary files by default, and reports walk stats; large walks stop at a budget with partial results (walk.budget_exhausted=true). Set raw_json=true for structured JSON.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Regular expression (Rust regex syntax) matched against each line. Patterns that fail to parse as regex are matched as literal substrings."
                    },
                    "path": {
                        "type": "string",
                        "default": ".",
                        "description": "Directory or file to search, relative to the active workspace/root. Omit or pass an empty string for the active workspace/root."
                    },
                    "case_sensitive": {
                        "type": "boolean",
                        "default": false,
                        "description": "When false, match case-insensitively."
                    },
                    "max_matches": {
                        "type": "integer",
                        "default": self.default_max_matches,
                        "minimum": 1,
                        "maximum": self.default_max_matches,
                        "description": "Maximum matching lines to return; higher values are capped by the tool."
                    },
                    "raw_json": {
                        "type": "boolean",
                        "default": false,
                        "description": "When true, return structured JSON instead of the compact default text."
                    }
                },
                "required": ["pattern"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        self.native_runner
            .run(
                NativeFsRunnerRequest::Grep(NativeGrepArgs {
                    pattern: args.pattern,
                    path: args.path,
                    case_sensitive: args.case_sensitive,
                    max_matches: args.max_matches.min(self.default_max_matches).max(1),
                    raw_json: args.raw_json,
                    max_entries_visited: None,
                    max_bytes_read: None,
                    max_wall_ms: None,
                }),
                Self::NAME,
            )
            .await
    }

    fn into_dyn_error(error: Self::Error) -> crate::llm::tool::ToolError {
        error.into_dispatch_error()
    }
}

impl Tool for WriteFileTool {
    const NAME: &'static str = "write_file";

    type Error = ToolError;
    type Args = WriteFileArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Write full file contents under the configured root. Relative paths resolve from the active request workspace when one is provided, otherwise from the root. Returns compact success metadata by default. Set raw_json=true for structured JSON.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "File to create or overwrite, relative to the active workspace/root unless an allowed absolute path is provided."
                    },
                    "content": {
                        "type": "string",
                        "description": "Complete file contents to write. Existing file contents are replaced."
                    },
                    "raw_json": {
                        "type": "boolean",
                        "default": false,
                        "description": "When true, return structured JSON instead of the compact default text."
                    }
                },
                "required": ["path", "content"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        deny_workspace_file_writes()?;
        let path = self.context.resolve_path_allow_create(&args.path)?;
        let lock = file_mutation_lock_for(&path);
        let _guard = lock.lock().await;
        let created = !path.exists();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, args.content.as_bytes()).await?;
        let mut bytes_written = args.content.len();
        let mut written_hash = content_hash(args.content.as_bytes());
        let mut note = None;
        if let Some(writethrough) = &self.writethrough {
            note = writethrough
                .after_mutation_under_lock(&path, crate::toolset::lsp::MutationKind::Write)
                .await;
            if let Ok(bytes) = tokio::fs::read(&path).await {
                bytes_written = bytes.len();
                written_hash = content_hash(&bytes);
            }
        }
        drop(_guard);
        if let Some(writethrough) = &self.writethrough {
            let diag = writethrough
                .diagnostics_after_unlock(&path, crate::toolset::lsp::MutationKind::Write)
                .await;
            note = merge_optional_notes(note, diag);
        }

        let output = WriteFileOutput {
            metadata: WriteFileMetadata {
                ok: true,
                status: "success",
                tool: Self::NAME,
                path: self.context.display_path(&path),
                returned_count: 0,
                total_count: Some(0),
                truncated: false,
                bytes_written,
                created,
                content_hash: written_hash,
            },
        };

        let mut body = format!(
            "write_file: wrote {} bytes to {}",
            output.metadata.bytes_written, output.metadata.path
        );
        if let Some(note) = note.filter(|note| !note.is_empty()) {
            body.push('\n');
            body.push_str(&note);
        }

        Ok(render_tool_output(
            &output.metadata,
            body,
            &output,
            args.raw_json,
        )?)
    }

    fn into_dyn_error(error: Self::Error) -> crate::llm::tool::ToolError {
        error.into_dispatch_error()
    }
}

impl Tool for EditFileTool {
    const NAME: &'static str = "edit_file";

    type Error = ToolError;
    type Args = EditFileArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Edit an existing file under the configured root by replacing old_text with new_text. Relative paths resolve from the active request workspace when one is provided, otherwise from the root. Matching tolerates trailing-whitespace, indentation, and unicode-punctuation drift (the result reports match_strategy; an exact match always wins), and files are matched with normalized line endings, so CRLF files edit cleanly. Use the smallest old_text that uniquely identifies the change; if it matches multiple places the call fails with the locations. On failure the error includes the closest near-match — re-read the file and build new old_text from CURRENT content instead of retrying the same call. Set dry_run=true to preview the diff without writing. Pass expected_content_hash (from read_file) to reject the edit if the file changed since you read it. Set raw_json=true for structured JSON.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Existing file to edit, relative to the active workspace/root unless an allowed absolute path is provided."
                    },
                    "old_text": {
                        "type": "string",
                        "description": "Text to locate. Whitespace/indentation drift is tolerated; interior wording must match. With match_mode=\"regex\" this is a Rust regex instead."
                    },
                    "new_text": {
                        "type": "string",
                        "description": "Replacement text. With match_mode=\"regex\", capture groups substitute ($1, $2, ...)."
                    },
                    "replace_all": {
                        "type": "boolean",
                        "default": false,
                        "description": "When false, old_text must match exactly one location. When true, replace every match."
                    },
                    "dry_run": {
                        "type": "boolean",
                        "default": false,
                        "description": "When true, return the diff that WOULD be applied without writing the file."
                    },
                    "match_mode": {
                        "type": "string",
                        "enum": ["ladder", "regex"],
                        "default": "ladder",
                        "description": "ladder = tolerant literal matching (default). regex = old_text is a Rust regex."
                    },
                    "operation": {
                        "type": "string",
                        "enum": ["replace", "insert_after", "insert_before", "delete"],
                        "default": "replace",
                        "description": "replace swaps old_text for new_text; insert_after/insert_before keep old_text and add new_text after/before it; delete removes old_text (new_text ignored)."
                    },
                    "expected_content_hash": {
                        "type": "string",
                        "description": "Optional content_hash from a prior read_file of this file. If the file's current bytes hash differently, the edit is rejected before matching."
                    },
                    "raw_json": {
                        "type": "boolean",
                        "default": false,
                        "description": "When true, return structured JSON instead of the compact default text."
                    }
                },
                "required": ["path", "old_text", "new_text"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        if !args.dry_run {
            deny_workspace_file_writes()?;
        }
        let path = self.context.resolve_existing_file(&args.path)?;
        let display = self.context.display_path(&path);
        let lock = file_mutation_lock_for(&path);
        let _guard = lock.lock().await;
        let raw = tokio::fs::read(&path).await?;
        let pre_edit_hash = content_hash(&raw);

        if let Some(expected) = &args.expected_content_hash {
            if expected != &pre_edit_hash {
                return Err(anyhow!(
                    "{display} has changed since it was read: expected {expected}, but the current content hashes to {pre_edit_hash}. Re-read the file and rebuild the edit from current content."
                )
                .into());
            }
        }

        let operation = match args.operation.as_deref() {
            None | Some("replace") => Operation::Replace,
            Some("insert_after") => Operation::InsertAfter,
            Some("insert_before") => Operation::InsertBefore,
            Some("delete") => Operation::Delete,
            Some(other) => {
                return Err(anyhow!(
                "unknown operation {other:?}; valid: replace, insert_after, insert_before, delete"
            )
                .into())
            }
        };
        let match_mode = match args.match_mode.as_deref() {
            None | Some("ladder") => MatchMode::Ladder,
            Some("regex") => MatchMode::Regex,
            Some(other) => {
                return Err(anyhow!("unknown match_mode {other:?}; valid: ladder, regex").into())
            }
        };

        // Strict UTF-8: a lossy conversion followed by a full-file rewrite
        // would silently replace every invalid byte with U+FFFD.
        let raw_text = String::from_utf8(raw).map_err(|error| {
            anyhow!(
                "{display} is not valid UTF-8 (first invalid byte at offset {}); edit_file only edits UTF-8 text files",
                error.utf8_error().valid_up_to()
            )
        })?;
        let normalized = edit_match::normalize_content(&raw_text);
        let old_text = args.old_text.replace("\r\n", "\n");
        let new_text = args.new_text.replace("\r\n", "\n");
        let request = EditRequest {
            old_text: &old_text,
            new_text: &new_text,
            replace_all: args.replace_all,
            operation,
            match_mode,
        };

        match edit_match::decide(&normalized.text, &request) {
            EditOutcome::Applied {
                result,
                strategy,
                replacements,
                first_changed_line,
                diff,
            } => {
                let post_text =
                    edit_match::restore_content(&result, normalized.ending, normalized.had_bom);
                let post_edit_hash = content_hash(post_text.as_bytes());
                let mut edit_note = None;
                if !args.dry_run {
                    tokio::fs::write(&path, post_text.as_bytes()).await?;
                    let verify = tokio::fs::read(&path).await?;
                    if verify != post_text.as_bytes() {
                        return Err(anyhow!(
                            "post-write verification failed for {display}: the bytes on disk do not match the edited content"
                        )
                        .into());
                    }
                    if let Some(writethrough) = &self.writethrough {
                        edit_note = writethrough
                            .after_mutation_under_lock(
                                &path,
                                crate::toolset::lsp::MutationKind::Edit,
                            )
                            .await;
                    }
                    drop(_guard);
                    if let Some(writethrough) = &self.writethrough {
                        let diag = writethrough
                            .diagnostics_after_unlock(
                                &path,
                                crate::toolset::lsp::MutationKind::Edit,
                            )
                            .await;
                        edit_note = merge_optional_notes(edit_note, diag);
                    }
                }
                let output = EditFileOutput {
                    metadata: EditFileMetadata {
                        ok: true,
                        status: "success",
                        tool: Self::NAME,
                        path: display,
                        returned_count: replacements,
                        total_count: Some(replacements),
                        truncated: false,
                        replacements_applied: replacements,
                        replace_all: args.replace_all,
                        dry_run: args.dry_run,
                        match_strategy: strategy.as_str(),
                        first_changed_line,
                        pre_edit_hash,
                        post_edit_hash,
                        bytes_written: if args.dry_run { 0 } else { post_text.len() },
                    },
                    diff,
                };
                let verb = if args.dry_run {
                    "dry-run preview for"
                } else {
                    "edited"
                };
                let mut body = format!(
                    "edit_file: {verb} {} ({} replacement{}, strategy {})\n{}",
                    output.metadata.path,
                    replacements,
                    if replacements != 1 { "s" } else { "" },
                    output.metadata.match_strategy,
                    output.diff,
                );
                if let Some(note) = edit_note.filter(|note| !note.is_empty()) {
                    body.push('\n');
                    body.push_str(&note);
                }
                Ok(render_tool_output(
                    &output.metadata,
                    body,
                    &output,
                    args.raw_json,
                )?)
            }
            EditOutcome::NotFound { closest } => {
                let mut message = format!("old_text was not found in {display}.");
                if let Some(c) = closest {
                    let _ = write!(
                        message,
                        "\nClosest match ({}% similar) at line {}:",
                        c.similarity_pct, c.line
                    );
                    if let Some((pattern_line, file_line)) = c.first_diff {
                        let _ = write!(
                            message,
                            "\n  your text: {pattern_line}\n  file has:  {file_line}"
                        );
                    }
                }
                message.push_str(
                    "\nRe-read the file and build old_text from its CURRENT content; do not retry the same call.",
                );
                Err(anyhow!(message).into())
            }
            EditOutcome::Ambiguous {
                strategy,
                count,
                previews,
            } => {
                let mut message = format!(
                    "old_text matches {count} locations in {display} (strategy {}):",
                    strategy.as_str()
                );
                for preview in previews {
                    let _ = write!(message, "\n  line {}: {}", preview.line, preview.text);
                }
                message.push_str(
                    "\nAdd surrounding context to make it unique, or set replace_all=true.",
                );
                Err(anyhow!(message).into())
            }
            EditOutcome::Noop { .. } => Err(anyhow!(
                "the edit would produce identical content in {display}; nothing was written. old_text and new_text are equivalent at the matched site."
            )
            .into()),
            EditOutcome::InvalidRegex { error } => Err(anyhow!(
                "match_mode=\"regex\": pattern failed to parse: {error}"
            )
            .into()),
        }
    }

    fn into_dyn_error(error: Self::Error) -> crate::llm::tool::ToolError {
        error.into_dispatch_error()
    }
}

#[derive(Serialize)]
struct ReadFileMetadata {
    ok: bool,
    status: &'static str,
    tool: &'static str,
    path: String,
    returned_count: usize,
    total_count: Option<usize>,
    truncated: bool,
    start_line: usize,
    end_line: usize,
    content_hash: String,
}

#[derive(Serialize)]
struct ReadFileOutput {
    #[serde(flatten)]
    metadata: ReadFileMetadata,
    content: String,
}

#[derive(Serialize)]
struct WriteFileMetadata {
    ok: bool,
    status: &'static str,
    tool: &'static str,
    path: String,
    returned_count: usize,
    total_count: Option<usize>,
    truncated: bool,
    bytes_written: usize,
    created: bool,
    content_hash: String,
}

#[derive(Serialize)]
struct WriteFileOutput {
    #[serde(flatten)]
    metadata: WriteFileMetadata,
}

#[derive(Serialize)]
struct EditFileMetadata {
    ok: bool,
    status: &'static str,
    tool: &'static str,
    path: String,
    returned_count: usize,
    total_count: Option<usize>,
    truncated: bool,
    replacements_applied: usize,
    replace_all: bool,
    dry_run: bool,
    match_strategy: &'static str,
    first_changed_line: usize,
    pre_edit_hash: String,
    post_edit_hash: String,
    bytes_written: usize,
}

#[derive(Serialize)]
struct EditFileOutput {
    #[serde(flatten)]
    metadata: EditFileMetadata,
    diff: String,
}

fn render_tool_output(
    metadata: &impl Serialize,
    body: String,
    raw_value: &impl Serialize,
    raw_json: bool,
) -> Result<String> {
    if raw_json {
        return render_json(raw_value);
    }

    let mut out = String::from(OUTPUT_META_PREFIX);
    out.push_str(&render_json(metadata)?);
    if !body.is_empty() {
        out.push('\n');
        out.push_str(&body);
    }
    Ok(out)
}

fn render_json(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).context("serializing tool output")
}
