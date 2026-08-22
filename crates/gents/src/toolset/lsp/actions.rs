use std::path::Path;

use anyhow::Context as _;
use serde_json::{json, Value};

use crate::tool_call_lifecycle::FailureClass;
use crate::toolset::shared::{ToolContext, ToolError};

use super::auth::LspAction;
use super::catalog::{family_eligible, marker_matches, primary_for_file, CatalogServer};
use super::client::LspClient;
use super::edits::{
    acquire_mutation_locks, apply_prepared_with_held_locks, apply_workspace_edit,
    redact_structured_uris, resolve_inbound_path, resolve_inbound_path_allow_create, walk_uris,
    PreparedEdit,
};
use super::encoding::position_for_symbol;
use super::pool::LspLease;

pub(crate) const MAX_DIAGNOSTICS: usize = 50;
pub(crate) const MAX_WORKSPACE_SYMBOLS: usize = 200;
pub(crate) const MAX_REFERENCES: usize = 50;
pub(crate) const MAX_RENAME_PAIRS: usize = 1_000;
pub(crate) const MAX_GLOB_TARGETS: usize = 20;

pub const READ_REQUEST_METHODS: &[&str] = &[
    "textDocument/hover",
    "textDocument/definition",
    "textDocument/typeDefinition",
    "textDocument/implementation",
    "textDocument/references",
    "textDocument/documentSymbol",
    "textDocument/diagnostic",
    "workspace/symbol",
    "workspace/diagnostic",
];

pub struct ActionRequest {
    pub action: LspAction,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub symbol: Option<String>,
    pub query: Option<String>,
    pub new_name: Option<String>,
    pub apply: Option<bool>,
    pub payload: Option<String>,
    pub timeout: Option<u32>,
}

pub async fn dispatch(
    context: &ToolContext,
    lease: Option<&LspLease>,
    pool: &super::pool::LspPool,
    config: &super::LspToolConfig,
    servers: &[CatalogServer],
    workspace: &Path,
    req: ActionRequest,
) -> Result<String, ToolError> {
    match req.action {
        LspAction::Status => {
            let session_id = crate::tool_call_lifecycle::runtime::current_tool_runtime_context()
                .and_then(|scope| scope.session_id)
                .filter(|id| !id.is_empty())
                .unwrap_or_else(|| config.session_id.clone());
            let states = pool
                .inspect_session(&session_id, &config.behavior_id, workspace, &config.digest)
                .await;
            Ok(status_text(states, &config.servers, workspace))
        }
        LspAction::Reload => {
            let session_id = crate::tool_call_lifecycle::runtime::current_tool_runtime_context()
                .and_then(|scope| scope.session_id)
                .filter(|id| !id.is_empty())
                .unwrap_or_else(|| config.session_id.clone());
            let retired = pool
                .reload_snapshot(&session_id, &config.behavior_id, workspace, &config.digest)
                .await;
            Ok(format!(
                "retired {retired} language-server client(s) for the current snapshot"
            ))
        }
        LspAction::Capabilities => {
            let lease = lease.ok_or_else(|| unavailable_for_workspace(workspace, config))?;
            Ok(lease.client().capabilities().await.to_string())
        }
        action => {
            let lease = lease.ok_or_else(|| unavailable_for_workspace(workspace, config))?;
            run_file_action(context, lease.client(), config, servers, action, &req).await
        }
    }
}

async fn run_file_action(
    context: &ToolContext,
    client: &LspClient,
    config: &super::LspToolConfig,
    servers: &[CatalogServer],
    action: LspAction,
    req: &ActionRequest,
) -> Result<String, ToolError> {
    if matches!(action, LspAction::RequestRead | LspAction::RequestWrite) {
        return raw_request(context, client, req).await;
    }
    let file = req
        .file
        .as_deref()
        .ok_or_else(|| arg_invalid("file parameter required"))?;
    if file == "*" {
        return workspace_action(context, client, action, req).await;
    }
    if looks_like_glob(file) {
        return glob_action(
            context,
            client,
            config,
            servers,
            action,
            file,
            request_timeout(req),
        )
        .await;
    }
    let path = resolve_inbound_path(context, file).map_err(|err| policy(err))?;
    let text = std::fs::read_to_string(&path).map_err(|err| {
        ToolError::reported_failure(FailureClass::ArgumentInvalid, err.to_string())
    })?;
    let uri = super::uri::path_to_file_uri(&path);
    let lang = language_id(servers, &path);
    client
        .ensure_open(&uri, &lang, &text)
        .await
        .map_err(|err| ToolError::reported_failure(FailureClass::ToolReturnedError, err))?;
    let encoding = client.position_encoding().await;
    let pos = position_for_symbol(
        &text,
        encoding,
        req.line,
        req.symbol.as_deref().unwrap_or(""),
    )
    .or_else(|| {
        req.symbol.as_ref().map(|_| super::encoding::LspPosition {
            line: req.line.unwrap_or(1).saturating_sub(1),
            character: 0,
        })
    });
    let method = match action {
        LspAction::Hover => "textDocument/hover",
        LspAction::Definition => "textDocument/definition",
        LspAction::TypeDefinition => "textDocument/typeDefinition",
        LspAction::Implementation => "textDocument/implementation",
        LspAction::References => "textDocument/references",
        LspAction::Symbols => "textDocument/documentSymbol",
        LspAction::Diagnostics => "textDocument/diagnostic",
        LspAction::Rename => "textDocument/rename",
        LspAction::RenameFile => "workspace/willRenameFiles",
        LspAction::CodeActionsList | LspAction::CodeActionsApply => "textDocument/codeAction",
        _ => return Err(arg_invalid("unsupported action")),
    };
    let mut params = json!({
        "textDocument": { "uri": uri }
    });
    if let Some(pos) = pos {
        params["position"] = json!({ "line": pos.line, "character": pos.character });
    }
    if matches!(
        action,
        LspAction::CodeActionsList | LspAction::CodeActionsApply
    ) {
        let start = pos.unwrap_or(super::encoding::LspPosition {
            line: req.line.unwrap_or(1).saturating_sub(1),
            character: 0,
        });
        params["range"] = json!({
            "start": { "line": start.line, "character": start.character },
            "end": { "line": start.line, "character": start.character }
        });
        params["context"] = json!({
            "diagnostics": [],
            "triggerKind": 1
        });
    }
    if matches!(action, LspAction::References) {
        params["context"] = json!({ "includeDeclaration": true });
    }
    if matches!(action, LspAction::Rename) {
        let name = req
            .new_name
            .as_deref()
            .ok_or_else(|| arg_invalid("new_name required"))?;
        params["newName"] = json!(name);
    }
    if matches!(action, LspAction::RenameFile) {
        let dest = req
            .new_name
            .as_deref()
            .ok_or_else(|| arg_invalid("new_name required for rename_file"))?;
        let dest_path = resolve_inbound_path_allow_create(context, dest).map_err(policy)?;
        params = json!({
            "files": [{
                "oldUri": uri,
                "newUri": super::uri::path_to_file_uri(&dest_path)
            }]
        });
    }
    // Cold-started project servers can acknowledge initialize before their
    // first document index is queryable. A null documentSymbol response is
    // transient in that window just like a null hover/definition response.
    let retry_empty = matches!(
        action,
        LspAction::Hover
            | LspAction::Definition
            | LspAction::TypeDefinition
            | LspAction::Implementation
            | LspAction::References
            | LspAction::Symbols
    );
    let result = match request_maybe_retry(
        client,
        method,
        params,
        request_timeout(req),
        retry_empty,
    )
    .await
    {
        Ok(result) => result,
        Err(err) if matches!(action, LspAction::Diagnostics) => client
            .cached_diagnostics(&uri)
            .await
            .ok_or_else(|| ToolError::reported_failure(FailureClass::ToolReturnedError, err))?,
        Err(err) => {
            return Err(ToolError::reported_failure(
                FailureClass::ToolReturnedError,
                err,
            ))
        }
    };
    if matches!(
        action,
        LspAction::Rename | LspAction::RenameFile | LspAction::CodeActionsApply
    ) && req.apply != Some(false)
    {
        if !super::lsp_apply_authorized(
            config.lsp,
            config.file,
            super::LspMutationSource::ForegroundReturnedEdit,
        ) {
            return Err(policy("lsp apply is not authorized"));
        }
        let edit = if matches!(action, LspAction::CodeActionsApply) {
            resolve_code_action_edit(client, &result, req.query.as_deref()).await?
        } else {
            result.clone()
        };
        let mut applied = apply_workspace_edit(context, client, &edit).await?;
        if matches!(action, LspAction::RenameFile) {
            let dest = req
                .new_name
                .as_deref()
                .ok_or_else(|| arg_invalid("new_name required for rename_file"))?;
            let dest_path = resolve_inbound_path_allow_create(context, dest).map_err(policy)?;
            if path.exists() && path != dest_path {
                let prepared = vec![PreparedEdit {
                    path: dest_path,
                    new_bytes: Vec::new(),
                    expected_hash: None,
                    version: None,
                    rename_from: Some(path.clone()),
                }];
                let _guards = acquire_mutation_locks(&prepared).await;
                applied += apply_prepared_with_held_locks(context, client, &prepared).await?;
            }
        }
        return Ok(format!("Applied edit to {applied} file(s)"));
    }
    let mut output = truncate_model_output(format_result(context, action, result));
    if matches!(action, LspAction::Diagnostics) {
        if let Some(linter) =
            run_linter_diagnostics(config, servers, &path, request_timeout(req)).await
        {
            output = format!("{output}\n{linter}");
        }
    }
    Ok(output)
}

async fn workspace_action(
    context: &ToolContext,
    client: &LspClient,
    action: LspAction,
    req: &ActionRequest,
) -> Result<String, ToolError> {
    match action {
        LspAction::Symbols => {
            let query = req
                .query
                .as_deref()
                .ok_or_else(|| arg_invalid("query required for workspace symbols"))?;
            let result = client
                .request_with_timeout(
                    "workspace/symbol",
                    json!({ "query": query }),
                    request_timeout(req),
                )
                .await
                .map_err(|err| ToolError::reported_failure(FailureClass::ToolReturnedError, err))?;
            Ok(truncate_model_output(format_result(
                context, action, result,
            )))
        }
        LspAction::Diagnostics => {
            match client
                .request_with_timeout(
                    "workspace/diagnostic",
                    json!({ "identifier": "gents" }),
                    request_timeout(req),
                )
                .await
            {
                Ok(result) => Ok(truncate_model_output(format_result(
                    context, action, result,
                ))),
                Err(_) => Ok(
                    "workspace diagnostics require workspace/diagnostic; pass a file or glob"
                        .into(),
                ),
            }
        }
        _ => Err(arg_invalid(
            "file: * is only valid for diagnostics, symbols, or reload",
        )),
    }
}

async fn raw_request(
    context: &ToolContext,
    client: &LspClient,
    req: &ActionRequest,
) -> Result<String, ToolError> {
    let method = req
        .query
        .as_deref()
        .ok_or_else(|| arg_invalid("query (method) required for request"))?;
    let params = validate_raw_request(context, method, req.payload.as_deref())?;
    let result = client
        .request_with_timeout(method, params, request_timeout(req))
        .await
        .map_err(|err| ToolError::reported_failure(FailureClass::ToolReturnedError, err))?;
    Ok(truncate_model_output(format_result(
        context, req.action, result,
    )))
}

fn request_timeout(req: &ActionRequest) -> std::time::Duration {
    std::time::Duration::from_secs(req.timeout.unwrap_or(20).clamp(5, 300) as u64)
}

pub fn validate_raw_request(
    context: &ToolContext,
    method: &str,
    payload: Option<&str>,
) -> Result<Value, ToolError> {
    if method == "workspace/executeCommand" {
        return Err(arg_invalid("workspace/executeCommand is not supported"));
    }
    if !READ_REQUEST_METHODS.contains(&method) {
        return Err(arg_invalid(format!("unknown request method {method}")));
    }
    let params = match payload {
        Some(raw) => serde_json::from_str(raw).map_err(|err| arg_invalid(err.to_string()))?,
        None => json!({}),
    };
    if !params.is_object() {
        return Err(arg_invalid("request payload must be a JSON object"));
    }
    validate_known_request_shape(method, &params)?;
    validate_payload_uris(context, &params)?;
    Ok(params)
}

fn validate_known_request_shape(method: &str, params: &Value) -> Result<(), ToolError> {
    let allowed = match method {
        "textDocument/hover"
        | "textDocument/definition"
        | "textDocument/typeDefinition"
        | "textDocument/implementation"
        | "textDocument/documentSymbol"
        | "textDocument/diagnostic" => &["textDocument", "position", "workDoneToken"][..],
        "textDocument/references" => &["textDocument", "position", "context", "workDoneToken"][..],
        "workspace/symbol" => &["query", "workDoneToken"][..],
        "workspace/diagnostic" => &["identifier", "previousResultId", "workDoneToken"][..],
        _ => return Err(arg_invalid(format!("unknown request method {method}"))),
    };
    let obj = params
        .as_object()
        .ok_or_else(|| arg_invalid("request payload must be a JSON object"))?;
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(arg_invalid(format!("unknown field {key} for {method}")));
        }
    }
    match method {
        "workspace/symbol" => {
            if params.get("query").and_then(Value::as_str).is_none() {
                return Err(arg_invalid("query required for workspace/symbol"));
            }
        }
        m if m.starts_with("textDocument/") => {
            if params
                .pointer("/textDocument/uri")
                .and_then(Value::as_str)
                .is_none()
            {
                return Err(arg_invalid("textDocument.uri required"));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_payload_uris(context: &ToolContext, params: &Value) -> Result<(), ToolError> {
    for uri in walk_uris(params) {
        resolve_inbound_path(context, &uri).map_err(policy)?;
    }
    Ok(())
}

pub(crate) async fn request_maybe_retry(
    client: &LspClient,
    method: &str,
    params: Value,
    timeout: std::time::Duration,
    retry_empty: bool,
) -> Result<Value, String> {
    const MAX_EMPTY_ATTEMPTS: usize = 3;
    let deadline = std::time::Instant::now() + timeout;
    for attempt in 0..MAX_EMPTY_ATTEMPTS {
        if attempt > 0 {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Ok(Value::Null);
            }
            tokio::time::sleep(remaining.min(std::time::Duration::from_millis(400))).await;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Ok(Value::Null);
        }
        let result = client
            .request_with_timeout(method, params.clone(), remaining)
            .await?;
        if !retry_empty || !semantic_result_is_empty(&result) || attempt + 1 == MAX_EMPTY_ATTEMPTS {
            return Ok(result);
        }
    }
    unreachable!("bounded retry loop always returns")
}

fn semantic_result_is_empty(result: &Value) -> bool {
    result.is_null() || result.as_array().is_some_and(Vec::is_empty)
}

async fn resolve_code_action_edit(
    client: &LspClient,
    result: &Value,
    query: Option<&str>,
) -> Result<Value, ToolError> {
    let mut action = if let Some(arr) = result.as_array() {
        select_code_action(arr, query)?.clone()
    } else {
        result.clone()
    };
    if action.get("edit").is_none() && action.get("data").is_some() {
        action = client
            .request("codeAction/resolve", action.clone())
            .await
            .map_err(|err| arg_invalid(format!("code action resolve failed: {err}")))?;
    }
    if action.get("command").is_some() && action.get("edit").is_none() {
        return Err(arg_invalid("bare Command code actions are not executed"));
    }
    action
        .get("edit")
        .cloned()
        .ok_or_else(|| arg_invalid("code action has no edit"))
}

pub(crate) fn select_code_action<'a>(
    actions: &'a [Value],
    query: Option<&str>,
) -> Result<&'a Value, ToolError> {
    let selected = match query.map(str::trim).filter(|q| !q.is_empty()) {
        Some(query) => {
            if let Ok(index) = query.parse::<usize>() {
                actions
                    .get(index)
                    .or_else(|| index.checked_sub(1).and_then(|idx| actions.get(idx)))
            } else {
                let needle = query.to_ascii_lowercase();
                actions.iter().find(|item| {
                    item.get("title")
                        .and_then(Value::as_str)
                        .is_some_and(|title| title.to_ascii_lowercase().contains(&needle))
                })
            }
        }
        None => actions
            .iter()
            .find(|item| item.get("edit").is_some())
            .or_else(|| {
                actions
                    .iter()
                    .find(|item| item.get("data").is_some() && item.get("command").is_none())
            }),
    };
    selected.ok_or_else(|| arg_invalid("no matching CodeAction.edit to apply"))
}

pub(crate) fn truncate_model_output(text: String) -> String {
    crate::truncation::truncate_text(
        &text,
        crate::truncation::TruncationMode::Head,
        &crate::truncation::TruncationLimits::default(),
    )
    .0
}

pub(crate) fn format_result(context: &ToolContext, action: LspAction, result: Value) -> String {
    if result.is_null() {
        return match action {
            LspAction::Hover => "No hover information".into(),
            LspAction::Definition => "No definition found".into(),
            LspAction::Symbols => "No symbols found".into(),
            _ => "No result".into(),
        };
    }
    if let Some(contents) = result.pointer("/contents") {
        return flatten_hover(contents);
    }
    let (redacted, omitted) = redact_structured_uris(context, &result);
    if matches!(action, LspAction::Diagnostics) {
        return format_diagnostics(&redacted, omitted);
    }
    if let Some(arr) = redacted.as_array() {
        let cap = match action {
            LspAction::Diagnostics => MAX_DIAGNOSTICS,
            LspAction::Symbols => MAX_WORKSPACE_SYMBOLS,
            LspAction::References => MAX_REFERENCES,
            LspAction::Rename | LspAction::RenameFile => MAX_RENAME_PAIRS,
            _ => usize::MAX,
        };
        let mut lines = Vec::new();
        for item in arr {
            if lines.len() >= cap {
                break;
            }
            if let Some(uri) = item
                .pointer("/uri")
                .or_else(|| item.pointer("/targetUri"))
                .or_else(|| item.pointer("/location/uri"))
                .and_then(Value::as_str)
            {
                let line = item
                    .pointer("/range/start/line")
                    .or_else(|| item.pointer("/targetSelectionRange/start/line"))
                    .or_else(|| item.pointer("/location/range/start/line"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    + 1;
                let name = item.get("name").and_then(Value::as_str);
                let kind = item
                    .get("kind")
                    .and_then(Value::as_u64)
                    .map(symbol_kind_name);
                let container = item.get("containerName").and_then(Value::as_str);
                lines.push(match name {
                    Some(name) => format_symbol_location(name, kind, container, uri, line),
                    None => format!("{uri}:{line}"),
                });
            } else if item.get("name").is_some() {
                collect_document_symbol_lines(item, None, &mut lines, cap);
            } else if !item.is_null() {
                lines.push(item.to_string());
            }
        }
        return finish_location_lines(lines, omitted);
    }
    if let Some(uri) = redacted
        .pointer("/uri")
        .or_else(|| redacted.pointer("/targetUri"))
        .or_else(|| redacted.pointer("/location/uri"))
        .and_then(Value::as_str)
    {
        let line = redacted
            .pointer("/range/start/line")
            .or_else(|| redacted.pointer("/location/range/start/line"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + 1;
        return finish_location_lines(vec![format!("{uri}:{line}")], omitted);
    }
    let mut text = redacted.to_string();
    if omitted > 0 {
        text.push_str(&format!(
            "\nomitted {omitted} location(s) outside the allowed workspace"
        ));
    }
    text
}

fn collect_document_symbol_lines(
    item: &Value,
    container: Option<&str>,
    lines: &mut Vec<String>,
    cap: usize,
) {
    if lines.len() >= cap {
        return;
    }
    if let Some(name) = item.get("name").and_then(Value::as_str) {
        let line = item
            .pointer("/selectionRange/start/line")
            .or_else(|| item.pointer("/range/start/line"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + 1;
        let kind = item
            .get("kind")
            .and_then(Value::as_u64)
            .map(symbol_kind_name);
        let qualified = container
            .map(clean_symbol_container)
            .filter(|parent| !parent.is_empty())
            .map(|parent| format!("{parent}::{name}"))
            .unwrap_or_else(|| name.to_string());
        lines.push(match kind {
            Some(kind) => format!("{qualified} ({kind}):{line}"),
            None => format!("{qualified}:{line}"),
        });
    }
    if let Some(children) = item.get("children").and_then(Value::as_array) {
        for child in children {
            if lines.len() >= cap {
                break;
            }
            let next_container = item.get("name").and_then(Value::as_str).or(container);
            collect_document_symbol_lines(child, next_container, lines, cap);
        }
    }
}

fn clean_symbol_container(container: &str) -> &str {
    container.strip_prefix("impl ").unwrap_or(container).trim()
}

fn format_symbol_location(
    name: &str,
    kind: Option<&str>,
    container: Option<&str>,
    uri: &str,
    line: u64,
) -> String {
    let qualified = container
        .map(clean_symbol_container)
        .filter(|parent| !parent.is_empty())
        .map(|parent| format!("{parent}::{name}"))
        .unwrap_or_else(|| name.to_string());
    match kind {
        Some(kind) => format!("{qualified} ({kind}) {uri}:{line}"),
        None => format!("{qualified} {uri}:{line}"),
    }
}

fn symbol_kind_name(kind: u64) -> &'static str {
    const KINDS: &[&str] = &[
        "unknown",
        "file",
        "module",
        "namespace",
        "package",
        "class",
        "method",
        "property",
        "field",
        "constructor",
        "enum",
        "interface",
        "function",
        "variable",
        "constant",
        "string",
        "number",
        "boolean",
        "array",
        "object",
        "key",
        "null",
        "enum member",
        "struct",
        "event",
        "operator",
        "type parameter",
    ];
    KINDS.get(kind as usize).copied().unwrap_or("unknown")
}

fn format_diagnostics(value: &Value, omitted: usize) -> String {
    let mut diagnostics = Vec::new();
    collect_diagnostics(value, None, &mut diagnostics, MAX_DIAGNOSTICS);
    if diagnostics.is_empty() {
        return if omitted > 0 {
            format!("No diagnostics in the allowed workspace; omitted {omitted} outside-root location(s)")
        } else {
            "No diagnostics".into()
        };
    }
    let count = diagnostics.len();
    let mut text = format!("Found {count} diagnostic(s):\n{}", diagnostics.join("\n"));
    if omitted > 0 {
        text.push_str(&format!(
            "\nomitted {omitted} diagnostic location(s) outside the allowed workspace"
        ));
    }
    text
}

fn collect_diagnostics(
    value: &Value,
    inherited_uri: Option<&str>,
    out: &mut Vec<String>,
    cap: usize,
) {
    if out.len() >= cap {
        return;
    }
    match value {
        Value::Array(items) => {
            for item in items {
                if out.len() >= cap {
                    break;
                }
                collect_diagnostics(item, inherited_uri, out, cap);
            }
        }
        Value::Object(map) => {
            let uri = map.get("uri").and_then(Value::as_str).or(inherited_uri);
            if map.get("message").and_then(Value::as_str).is_some() {
                out.push(format_diagnostic(value, uri));
                return;
            }
            if let Some(items) = map
                .get("diagnostics")
                .or_else(|| map.get("items"))
                .and_then(Value::as_array)
            {
                for item in items {
                    if out.len() >= cap {
                        break;
                    }
                    collect_diagnostics(item, uri, out, cap);
                }
            }
        }
        _ => {}
    }
}

fn format_diagnostic(item: &Value, uri: Option<&str>) -> String {
    let line = item
        .pointer("/range/start/line")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        + 1;
    let location = uri
        .map(|uri| format!("{uri}:{line}"))
        .unwrap_or_else(|| format!("line {line}"));
    let severity = match item.get("severity").and_then(Value::as_u64) {
        Some(1) => "error",
        Some(2) => "warning",
        Some(3) => "information",
        Some(4) => "hint",
        _ => "diagnostic",
    };
    let source = item
        .get("source")
        .and_then(Value::as_str)
        .map(|source| format!(" [{source}]"))
        .unwrap_or_default();
    let code = item
        .get("code")
        .and_then(|code| {
            code.as_str()
                .map(str::to_string)
                .or_else(|| code.as_i64().map(|n| n.to_string()))
        })
        .map(|code| format!(" ({code})"))
        .unwrap_or_default();
    let message = item
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("missing diagnostic message");
    format!("{location}: {severity}{source}{code}: {message}")
}

fn finish_location_lines(lines: Vec<String>, omitted: usize) -> String {
    if lines.is_empty() && omitted > 0 {
        return format!("omitted {omitted} location(s) outside the allowed workspace");
    }
    let mut lines = lines;
    if omitted > 0 {
        lines.push(format!(
            "omitted {omitted} location(s) outside the allowed workspace"
        ));
    }
    if lines.is_empty() {
        "No result".into()
    } else {
        format!("Found {} result(s):\n{}", lines.len(), lines.join("\n"))
    }
}

fn flatten_hover(contents: &Value) -> String {
    if let Some(s) = contents.as_str() {
        return s.to_string();
    }
    if let Some(s) = contents.get("value").and_then(Value::as_str) {
        return s.to_string();
    }
    contents.to_string()
}

fn language_id(servers: &[CatalogServer], path: &std::path::Path) -> String {
    primary_for_file(servers, path)
        .and_then(|s| s.language_id.clone())
        .unwrap_or_else(|| super::catalog::language_id_for_path(path))
}

fn looks_like_glob(file: &str) -> bool {
    file.contains('*') || file.contains('?') || file.contains('{') || file.contains('[')
}

async fn glob_action(
    context: &ToolContext,
    client: &LspClient,
    config: &super::LspToolConfig,
    servers: &[CatalogServer],
    action: LspAction,
    pattern: &str,
    timeout: std::time::Duration,
) -> Result<String, ToolError> {
    if !matches!(action, LspAction::Diagnostics) {
        return Err(arg_invalid("globs are only valid for diagnostics"));
    }
    let runner = crate::toolset::native_runner::NativeFsRunner::new(context);
    let paths = runner.glob_paths(pattern, MAX_GLOB_TARGETS).await?;
    if paths.is_empty() {
        return Ok("no files matched the diagnostic glob".into());
    }
    let mut sections = Vec::new();
    let selected_server = servers
        .iter()
        .find(|server| server.name == client.server_name);
    for path in paths
        .into_iter()
        .filter(|path| {
            selected_server.is_none_or(|server| super::catalog::file_type_matches(server, path))
        })
        .take(MAX_GLOB_TARGETS)
    {
        let display = context.display_path(&path);
        let uri = super::uri::path_to_file_uri(&path);
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading matched diagnostic file {}", path.display()))?;
        client
            .ensure_open(&uri, &language_id(servers, &path), &text)
            .await
            .map_err(|err| ToolError::reported_failure(FailureClass::ToolReturnedError, err))?;
        let result = client
            .request(
                "textDocument/diagnostic",
                json!({ "textDocument": { "uri": uri } }),
            )
            .await
            .unwrap_or(Value::Null);
        let mut section = format!("{display}:\n{}", format_result(context, action, result));
        if let Some(linter) = run_linter_diagnostics(config, servers, &path, timeout).await {
            section = format!("{section}\n{linter}");
        }
        sections.push(section);
    }
    if sections.is_empty() {
        return Ok("no files matched the selected language server".into());
    }
    Ok(truncate_model_output(sections.join("\n")))
}

pub(crate) async fn run_linter_diagnostics(
    config: &super::LspToolConfig,
    servers: &[CatalogServer],
    path: &std::path::Path,
    timeout: std::time::Duration,
) -> Option<String> {
    let workspace = super::overlay_workspace_or(&config.workspace);
    let constraints = super::overlay_lsp_constraints(&config.constraints);
    let linter = servers.iter().find(|server| {
        server.is_linter
            && marker_matches(&workspace, &server.root_markers)
            && family_eligible(server, &workspace)
            && super::catalog::file_type_matches(server, path)
    })?;
    let argv = match linter.name.as_str() {
        "biome" => vec![
            linter.command.clone(),
            "check".into(),
            "--reporter=json".into(),
            path.to_string_lossy().into_owned(),
        ],
        "swiftlint" => vec![
            linter.command.clone(),
            "lint".into(),
            "--quiet".into(),
            path.to_string_lossy().into_owned(),
        ],
        _ => return None,
    };
    let (program, rest, env, _sandbox) =
        crate::toolset::prepare_managed_command(&workspace, &argv[0], &argv[1..], &constraints)
            .ok()?;
    let mut full = vec![program.to_string_lossy().into_owned()];
    full.extend(rest);
    let runtime = crate::tool_call_lifecycle::runtime::current_tool_runtime_context();
    let command_deadline = chrono::Utc::now()
        + chrono::Duration::from_std(timeout).unwrap_or_else(|_| chrono::Duration::days(36_500));
    let deadline_at = Some(
        runtime
            .as_ref()
            .and_then(|scope| scope.deadline_at)
            .map_or(command_deadline, |deadline| deadline.min(command_deadline)),
    );
    let cancellation_token = runtime
        .map(|scope| scope.cancellation_token)
        .unwrap_or_default();
    let outcome = crate::managed_exec::run_managed_exec(crate::managed_exec::ManagedExecRequest {
        argv: full,
        cwd: workspace,
        deadline_at,
        cancellation_token,
        max_output_bytes: 64 * 1024,
        stdin: Vec::new(),
        environment: Some(env),
        tool_name: Some("lsp".into()),
        live_output: None,
    })
    .await;
    match outcome {
        crate::managed_exec::ManagedExecOutcome::Exited { stdout, .. } => {
            let text = String::from_utf8_lossy(&stdout);
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(format!("{}: {trimmed}", linter.name))
            }
        }
        _ => None,
    }
}

fn status_text(
    states: std::collections::HashMap<String, super::pool::PoolServerState>,
    servers: &[CatalogServer],
    workspace: &std::path::Path,
) -> String {
    let names: Vec<String> = servers
        .iter()
        .map(|s| {
            use super::pool::PoolServerState;
            match states.get(&s.name) {
                Some(PoolServerState::Starting) => format!("{} (starting/indexing)", s.name),
                Some(PoolServerState::Ready) => format!("{} (ready)", s.name),
                Some(PoolServerState::Retiring) => format!("{} (retiring)", s.name),
                Some(PoolServerState::Failed(error)) => {
                    format!("{} (start failed; retry backoff active: {error})", s.name)
                }
                None => format!(
                    "{} ({})",
                    s.name,
                    super::catalog::server_unavailable_reason(workspace, s).unwrap_or_else(|| {
                        if s.is_linter {
                            "available on demand for diagnostics".into()
                        } else {
                            "not started — run a server-backed action such as hover or symbols"
                                .into()
                        }
                    })
                ),
            }
        })
        .collect();
    if names.is_empty() {
        "No language servers configured for this project".into()
    } else {
        format!("Language servers:\n- {}", names.join("\n- "))
    }
}

fn arg_invalid(text: impl Into<String>) -> ToolError {
    ToolError::reported_failure(FailureClass::ArgumentInvalid, text.into())
}

fn policy(text: impl Into<String>) -> ToolError {
    ToolError::reported_failure(FailureClass::PolicyDenied, text.into())
}

fn unavailable(text: impl Into<String>) -> ToolError {
    ToolError::reported_failure(FailureClass::ServiceUnavailable, text.into())
}

fn unavailable_for_workspace(workspace: &Path, config: &super::LspToolConfig) -> ToolError {
    unavailable(super::catalog::unavailable_servers_message(
        workspace,
        &config.servers,
        None,
    ))
}
