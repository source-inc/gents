use std::collections::HashSet;
use std::path::Path;

use serde_json::{json, Value};

use crate::toolset::shared::ToolContext;

use super::catalog::primary_for_file;
use super::edits::{apply_prepared_with_held_locks, prepare_workspace_edit};
use super::pool::{LspLease, LspPool, PoolKey};
use super::LspToolConfig;

const MAX_WRITETHROUGH_DIAGNOSTICS: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    Write,
    Edit,
}

/// File-tool hook that talks only to already-Ready pooled clients.
#[derive(Clone)]
pub struct LspWritethrough {
    pool: LspPool,
    config: LspToolConfig,
}

impl LspWritethrough {
    pub fn new(pool: LspPool, config: LspToolConfig) -> Self {
        Self { pool, config }
    }

    /// Notify an already-Ready client. Never starts a server. Never invokes
    /// Biome/SwiftLint single-shot adapters.
    pub async fn after_mutation(&self, path: &Path) -> Option<String> {
        let format = self
            .after_mutation_under_lock(path, MutationKind::Write)
            .await;
        let diag = self
            .diagnostics_after_unlock(path, MutationKind::Write)
            .await;
        merge_notes(format, diag)
    }

    /// `didChange` plus optional format-on-write. Caller must already hold the
    /// path's file-mutation lock; this path never acquires it.
    pub async fn after_mutation_under_lock(
        &self,
        path: &Path,
        kind: MutationKind,
    ) -> Option<String> {
        let lease = self.ready_lease(path).await?;
        let uri = super::uri::path_to_file_uri(path);
        let text = std::fs::read_to_string(path).ok()?;
        let language_id = primary_for_file(&self.config.servers, path)
            .and_then(|server| server.language_id.clone())
            .unwrap_or_else(|| super::catalog::language_id_for_path(path));
        let _ = lease
            .client()
            .sync_document(&uri, &language_id, &text)
            .await;
        if kind != MutationKind::Write || !self.config.format_on_write {
            return None;
        }
        let edits = match lease
            .client()
            .request(
                "textDocument/formatting",
                json!({
                    "textDocument": { "uri": uri },
                    "options": { "tabSize": 4, "insertSpaces": true }
                }),
            )
            .await
        {
            Ok(Value::Array(arr)) if arr.is_empty() => return None,
            Ok(Value::Null) => return None,
            Ok(edits) => edits,
            Err(_) => {
                return Some("format-on-write skipped: language server formatting failed".into())
            }
        };
        let workspace = super::overlay_workspace_or(&self.config.workspace);
        let context = ToolContext::new(workspace, true).ok()?;
        let encoding = lease.client().position_encoding().await;
        let workspace_edit = json!({ "changes": { uri: edits } });
        let prepared = match prepare_workspace_edit(&context, &workspace_edit, encoding) {
            Ok(prepared) => prepared,
            Err(error) => return Some(format!("format-on-write skipped: {error}")),
        };
        match apply_prepared_with_held_locks(&context, lease.client(), &prepared).await {
            Ok(0) => None,
            Ok(n) => Some(format!("format-on-write applied {n} file(s)")),
            Err(error) => Some(format!("format-on-write skipped: {error}")),
        }
    }

    /// Diagnostics wait must happen after the file-mutation lock is released.
    pub async fn diagnostics_after_unlock(
        &self,
        path: &Path,
        kind: MutationKind,
    ) -> Option<String> {
        let wanted = match kind {
            MutationKind::Write => self.config.diagnostics_on_write,
            MutationKind::Edit => self.config.diagnostics_on_edit,
        };
        if !wanted {
            return None;
        }
        let lease = self.ready_lease(path).await?;
        let uri = super::uri::path_to_file_uri(path);
        let captured = lease.client().tracked_version(&uri).await.unwrap_or(1);
        let result = lease
            .client()
            .request(
                "textDocument/diagnostic",
                json!({ "textDocument": { "uri": uri } }),
            )
            .await
            .ok()?;
        Some(render_writethrough_diagnostics(
            &result,
            captured,
            self.config.diagnostics_deduplicate,
        ))
    }

    async fn ready_lease(&self, path: &Path) -> Option<LspLease> {
        let server = primary_for_file(&self.config.servers, path)?;
        if server.is_linter {
            return None;
        }
        let session_id = crate::tool_call_lifecycle::runtime::current_tool_runtime_context()
            .and_then(|scope| scope.session_id)
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| self.config.session_id.clone());
        let workspace_root = super::overlay_workspace_or(&self.config.workspace);
        let key = PoolKey {
            session_id,
            behavior_id: self.config.behavior_id.clone(),
            workspace_root,
            server_name: server.name.clone(),
            config_digest: self.config.digest.clone(),
        };
        self.pool.get_ready(&key).await
    }
}

fn merge_notes(left: Option<String>, right: Option<String>) -> Option<String> {
    match (left, right) {
        (None, None) => None,
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (Some(a), Some(b)) => Some(format!("{a}\n{b}")),
    }
}

fn render_writethrough_diagnostics(result: &Value, min_version: i64, dedup: bool) -> String {
    let items = collect_diagnostic_items(result, min_version);
    let mut lines = Vec::new();
    let mut seen = HashSet::new();
    for item in items {
        if dedup && !seen.insert(item.clone()) {
            continue;
        }
        lines.push(item);
        if lines.len() >= MAX_WRITETHROUGH_DIAGNOSTICS {
            break;
        }
    }
    if lines.is_empty() {
        "diagnostics: clean".into()
    } else {
        format!("diagnostics ({}):\n{}", lines.len(), lines.join("\n"))
    }
}

fn collect_diagnostic_items(result: &Value, min_version: i64) -> Vec<String> {
    let mut items = Vec::new();
    let version = result
        .get("version")
        .or_else(|| result.pointer("/textDocument/version"))
        .and_then(Value::as_i64)
        .unwrap_or(min_version);
    if version < min_version {
        return items;
    }
    let diagnostics = result
        .get("items")
        .or_else(|| result.get("diagnostics"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for diagnostic in diagnostics {
        let line = diagnostic
            .pointer("/range/start/line")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + 1;
        let message = diagnostic
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("diagnostic");
        items.push(format!("L{line}: {message}"));
    }
    items
}
