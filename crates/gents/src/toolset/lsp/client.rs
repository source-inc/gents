use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{oneshot, Mutex};

use crate::managed_exec::ManagedProcess;

const MAX_CONTENT_LENGTH: usize = 8 * 1024 * 1024;

const MAX_PENDING: usize = 32;
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_HEADER_BYTES: usize = 8 * 1024;

pub(crate) struct LspClient {
    stdin: Arc<Mutex<ChildStdin>>,
    pending: Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, String>>>>>,
    next_id: AtomicI64,
    pub server_name: String,
    process: Arc<Mutex<ManagedProcess>>,
    reader: tokio::task::JoinHandle<()>,
    stderr_task: tokio::task::JoinHandle<()>,
    encoding: Mutex<super::encoding::PositionEncoding>,
    capabilities: Mutex<Value>,
    workspace: std::path::PathBuf,
    init_options: Option<Value>,
    settings: Option<Value>,
    versions: Mutex<HashMap<String, i64>>,
    document_hashes: Mutex<HashMap<String, String>>,
    initialize_timeout: Duration,
    diagnostics: Arc<Mutex<HashMap<String, Value>>>,
    language_ids: Mutex<HashMap<String, String>>,
    server_status: Arc<Mutex<Option<ServerStatus>>>,
    progress: Arc<Mutex<HashSet<String>>>,
    alive: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
struct ServerStatus {
    quiescent: bool,
}

impl LspClient {
    #[cfg(test)]
    pub(crate) fn workspace(&self) -> &std::path::Path {
        &self.workspace
    }

    pub fn start(
        mut process: ManagedProcess,
        server_name: String,
        server: &super::catalog::CatalogServer,
        workspace: std::path::PathBuf,
    ) -> Result<Self, String> {
        let stdin = process.stdin.take().ok_or("process stdin missing")?;
        let stdout = process.stdout.take().ok_or("process stdout missing")?;
        let stderr = process.stderr.take();
        let pending: Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, String>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_reader = pending.clone();
        let stdin = Arc::new(Mutex::new(stdin));
        let stdin_reader = stdin.clone();
        let diagnostics: Arc<Mutex<HashMap<String, Value>>> = Arc::new(Mutex::new(HashMap::new()));
        let diagnostics_reader = diagnostics.clone();
        let server_status: Arc<Mutex<Option<ServerStatus>>> = Arc::new(Mutex::new(None));
        let server_status_reader = server_status.clone();
        let progress: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let progress_reader = progress.clone();
        let alive = Arc::new(AtomicBool::new(true));
        let alive_reader = alive.clone();
        let reader_workspace = workspace.clone();
        let settings = server.settings.clone();
        let reader = tokio::spawn(async move {
            let result = read_loop(
                stdout,
                ReaderShared {
                    pending: pending_reader.clone(),
                    stdin: stdin_reader,
                    diagnostics: diagnostics_reader,
                    workspace: reader_workspace,
                    settings,
                    server_status: server_status_reader,
                    progress: progress_reader,
                },
            )
            .await;
            alive_reader.store(false, Ordering::SeqCst);
            if let Err(error) = result {
                tracing::warn!(%error, "lsp reader exited");
                fail_pending(&pending_reader, &error).await;
            }
        });
        let stderr_task = tokio::spawn(async move {
            if let Some(mut stderr) = stderr {
                let mut buf = [0u8; 4096];
                loop {
                    match tokio::io::AsyncReadExt::read(&mut stderr, &mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            }
        });
        Ok(Self {
            stdin,
            pending,
            next_id: AtomicI64::new(1),
            server_name,
            process: Arc::new(Mutex::new(process)),
            reader,
            stderr_task,
            encoding: Mutex::new(super::encoding::PositionEncoding::Utf8),
            capabilities: Mutex::new(Value::Null),
            workspace,
            init_options: server.init_options.clone(),
            settings: server.settings.clone(),
            versions: Mutex::new(HashMap::new()),
            document_hashes: Mutex::new(HashMap::new()),
            initialize_timeout: server
                .warmup_timeout_ms
                .map(Duration::from_millis)
                .unwrap_or(INITIALIZE_TIMEOUT)
                .max(INITIALIZE_TIMEOUT)
                .min(Duration::from_secs(60)),
            diagnostics,
            language_ids: Mutex::new(HashMap::new()),
            server_status,
            progress,
            alive,
        })
    }

    pub async fn initialize(&self) -> Result<Value, String> {
        let root_uri = super::uri::path_to_file_uri(&self.workspace);
        let params = json!({
            "processId": null,
            "rootUri": root_uri,
            "rootPath": self.workspace,
            "workspaceFolders": [{
                "uri": root_uri,
                "name": self.workspace.file_name().and_then(|n| n.to_str()).unwrap_or("workspace")
            }],
            "initializationOptions": self.init_options.clone().unwrap_or(Value::Null),
            "capabilities": {
                "workspace": {
                    "applyEdit": false,
                    "workspaceEdit": {
                        "documentChanges": true,
                        "resourceOperations": ["create", "rename", "delete"]
                    },
                    "configuration": true
                },
                "textDocument": {
                    "hover": { "contentFormat": ["plaintext", "markdown"] },
                    "definition": { "linkSupport": true },
                    "typeDefinition": { "linkSupport": true },
                    "implementation": { "linkSupport": true },
                    "references": { "dynamicRegistration": false },
                    "documentSymbol": {
                        "dynamicRegistration": false,
                        "hierarchicalDocumentSymbolSupport": true,
                        "symbolKind": {
                            "valueSet": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]
                        }
                    },
                    "rename": {
                        "dynamicRegistration": false,
                        "prepareSupport": true
                    },
                    "synchronization": { "didSave": true }
                },
                "general": {
                    "positionEncodings": ["utf-8", "utf-16"]
                },
                "window": {
                    "workDoneProgress": true
                },
                "experimental": {
                    "serverStatusNotification": true
                }
            },
            "clientInfo": { "name": "gents", "version": "0" }
        });
        let result = self
            .request_with_timeout("initialize", params, self.initialize_timeout)
            .await?;
        let encodings = result
            .pointer("/capabilities/positionEncoding")
            .and_then(Value::as_str)
            .map(|enc| vec![enc.to_string()])
            .unwrap_or_else(|| {
                result
                    .pointer("/capabilities/general/positionEncodings")
                    .and_then(Value::as_array)
                    .map(|arr| {
                        arr.iter()
                            .filter_map(Value::as_str)
                            .map(ToOwned::to_owned)
                            .collect()
                    })
                    .unwrap_or_default()
            });
        *self.encoding.lock().await = super::encoding::negotiate(&encodings);
        *self.capabilities.lock().await = result.clone();
        let _ = self.notify("initialized", json!({})).await;
        if let Some(settings) = &self.settings {
            let _ = self
                .notify(
                    "workspace/didChangeConfiguration",
                    json!({ "settings": settings }),
                )
                .await;
        }
        Ok(result)
    }

    pub async fn wait_until_ready(&self, timings: Option<&Value>) {
        let budget = ready_wait(timings);
        if budget.is_zero() {
            return;
        }
        if self.server_name == "rust-analyzer" {
            self.wait_for_rust_analyzer(budget).await;
            return;
        }
        self.wait_for_server_status(budget).await;
    }

    async fn wait_for_rust_analyzer(&self, budget: Duration) {
        const MINIMUM_SETTLE: Duration = Duration::from_secs(2);
        let deadline = Instant::now() + budget;
        let started = Instant::now();
        let poll = Duration::from_millis(200);
        let status_timeout = Duration::from_millis(1_000);
        let mut seen_workspace = false;
        while Instant::now() < deadline {
            match self
                .request_with_timeout("rust-analyzer/analyzerStatus", json!({}), status_timeout)
                .await
            {
                Ok(Value::String(status)) if !status.starts_with("No workspaces") => {
                    if seen_workspace
                        && started.elapsed() >= MINIMUM_SETTLE
                        && self.progress.lock().await.is_empty()
                    {
                        return;
                    }
                    seen_workspace = true;
                }
                _ => {}
            }
            tokio::time::sleep(poll).await;
        }
    }

    async fn wait_for_server_status(&self, budget: Duration) {
        let deadline = Instant::now() + budget;
        while Instant::now() < deadline {
            if self.is_quiescent().await && self.progress.lock().await.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn is_quiescent(&self) -> bool {
        self.server_status
            .lock()
            .await
            .as_ref()
            .is_some_and(|status| status.quiescent)
    }

    pub async fn track_open(&self, uri: &str, version: i64) {
        self.versions.lock().await.insert(uri.to_string(), version);
    }

    pub async fn tracked_version(&self, uri: &str) -> Option<i64> {
        self.versions.lock().await.get(uri).copied()
    }

    pub async fn ensure_open(
        &self,
        uri: &str,
        language_id: &str,
        text: &str,
    ) -> Result<(), String> {
        let hash = crate::toolset::file_tools::content_hash(text.as_bytes());
        if self.versions.lock().await.contains_key(uri) {
            if self.document_hashes.lock().await.get(uri) == Some(&hash) {
                return Ok(());
            }
            self.sync_document(uri, language_id, text).await?;
            return Ok(());
        }
        self.open_document(uri, language_id, text, hash).await
    }

    async fn open_document(
        &self,
        uri: &str,
        language_id: &str,
        text: &str,
        hash: String,
    ) -> Result<(), String> {
        self.language_ids
            .lock()
            .await
            .insert(uri.to_string(), language_id.to_string());
        self.track_open(uri, 1).await;
        let opened = self
            .notify(
                "textDocument/didOpen",
                json!({
                    "textDocument": {
                        "uri": uri,
                        "languageId": language_id,
                        "version": 1,
                        "text": text
                    }
                }),
            )
            .await;
        if opened.is_ok() {
            self.document_hashes
                .lock()
                .await
                .insert(uri.to_string(), hash);
        } else {
            self.versions.lock().await.remove(uri);
            self.language_ids.lock().await.remove(uri);
        }
        opened
    }

    pub async fn sync_document(
        &self,
        uri: &str,
        language_id: &str,
        text: &str,
    ) -> Result<i64, String> {
        let version = {
            let mut versions = self.versions.lock().await;
            if let Some(current) = versions.get_mut(uri) {
                *current += 1;
                *current
            } else {
                drop(versions);
                let hash = crate::toolset::file_tools::content_hash(text.as_bytes());
                self.open_document(uri, language_id, text, hash).await?;
                return Ok(1);
            }
        };
        self.notify(
            "textDocument/didChange",
            json!({
                "textDocument": { "uri": uri, "version": version },
                "contentChanges": [{ "text": text }]
            }),
        )
        .await?;
        self.document_hashes.lock().await.insert(
            uri.to_string(),
            crate::toolset::file_tools::content_hash(text.as_bytes()),
        );
        Ok(version)
    }

    pub async fn close_document(&self, uri: &str) {
        if self.versions.lock().await.remove(uri).is_some() {
            self.language_ids.lock().await.remove(uri);
            self.document_hashes.lock().await.remove(uri);
            let _ = self
                .notify(
                    "textDocument/didClose",
                    json!({ "textDocument": { "uri": uri } }),
                )
                .await;
        }
    }

    pub async fn cached_diagnostics(&self, uri: &str) -> Option<Value> {
        self.diagnostics.lock().await.get(uri).cloned()
    }

    pub async fn position_encoding(&self) -> super::encoding::PositionEncoding {
        *self.encoding.lock().await
    }

    pub async fn capabilities(&self) -> Value {
        self.capabilities.lock().await.clone()
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        self.request_with_timeout(method, params, REQUEST_TIMEOUT)
            .await
    }

    pub async fn request_with_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, String> {
        if !self.is_alive() {
            return Err("language server has exited".into());
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            if pending.len() >= MAX_PENDING {
                return Err("pending LSP request cap reached".into());
            }
            pending.insert(id, tx);
        }
        let payload = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });
        if let Err(error) = write_message(&mut *self.stdin.lock().await, &payload).await {
            self.pending.lock().await.remove(&id);
            return Err(error);
        }
        let inflight = InflightRequest {
            stdin: self.stdin.clone(),
            pending: self.pending.clone(),
            id,
            completed: Arc::new(AtomicBool::new(false)),
        };
        let runtime = crate::tool_call_lifecycle::runtime::current_tool_runtime_context();
        let cancel = runtime.map(|scope| scope.cancellation_token);
        let outcome = tokio::select! {
            biased;
            _ = async {
                if let Some(token) = &cancel {
                    token.cancelled().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                self.cancel(id).await;
                self.pending.lock().await.remove(&id);
                Err(format!("LSP request {method} cancelled"))
            }
            result = tokio::time::timeout(timeout, rx) => {
                match result {
                    Ok(Ok(value)) => value,
                    Ok(Err(_)) => Err(format!("LSP request {method} dropped")),
                    Err(_) => {
                        self.cancel(id).await;
                        self.pending.lock().await.remove(&id);
                        Err(format!("LSP request {method} timed out"))
                    }
                }
            }
        };
        inflight.completed.store(true, Ordering::SeqCst);
        outcome
    }

    pub async fn notify(&self, method: &str, params: Value) -> Result<(), String> {
        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        });
        write_message(&mut *self.stdin.lock().await, &payload).await
    }

    pub async fn cancel(&self, id: i64) {
        let _ = self.notify("$/cancelRequest", json!({ "id": id })).await;
    }

    pub async fn shutdown_exit(&self) {
        let open: Vec<String> = self.versions.lock().await.keys().cloned().collect();
        for uri in open {
            self.close_document(&uri).await;
        }
        let _ = self.request("shutdown", Value::Null).await;
        let _ = self.notify("exit", Value::Null).await;
        self.alive.store(false, Ordering::SeqCst);
        let mut process = self.process.lock().await;
        process.terminate().await;
        self.reader.abort();
        self.stderr_task.abort();
        let mut pending = self.pending.lock().await;
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err("language server closed".into()));
        }
    }
}

struct InflightRequest {
    stdin: Arc<Mutex<ChildStdin>>,
    pending: Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, String>>>>>,
    id: i64,
    completed: Arc<AtomicBool>,
}

impl Drop for InflightRequest {
    fn drop(&mut self) {
        if self.completed.swap(true, Ordering::SeqCst) {
            return;
        }
        let stdin = self.stdin.clone();
        let pending = self.pending.clone();
        let id = self.id;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                pending.lock().await.remove(&id);
                let payload = json!({
                    "jsonrpc": "2.0",
                    "method": "$/cancelRequest",
                    "params": { "id": id }
                });
                let _ = write_message(&mut *stdin.lock().await, &payload).await;
            });
        }
    }
}

async fn write_message(stdin: &mut ChildStdin, value: &Value) -> Result<(), String> {
    let body = serde_json::to_vec(value).map_err(|err| err.to_string())?;
    if body.len() > MAX_CONTENT_LENGTH {
        return Err("JSON-RPC payload exceeds Content-Length cap".into());
    }
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    stdin
        .write_all(header.as_bytes())
        .await
        .map_err(|err| err.to_string())?;
    stdin
        .write_all(&body)
        .await
        .map_err(|err| err.to_string())?;
    stdin.flush().await.map_err(|err| err.to_string())
}

fn configuration_result(params: Option<&Value>, settings: &Option<Value>) -> Value {
    let items = params
        .and_then(|params| params.get("items"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if items.is_empty() {
        return json!([settings.clone().unwrap_or(Value::Null)]);
    }
    Value::Array(
        items
            .iter()
            .map(|_| settings.clone().unwrap_or(Value::Null))
            .collect(),
    )
}

fn ready_wait(timings: Option<&Value>) -> Duration {
    let Some(timings) = timings else {
        return Duration::ZERO;
    };
    let ms = timings
        .as_u64()
        .or_else(|| timings.get("initial").and_then(Value::as_u64))
        .or_else(|| timings.get("projectLoad").and_then(Value::as_u64))
        .unwrap_or(0);
    Duration::from_millis(ms).min(Duration::from_secs(60))
}

async fn fail_pending(
    pending: &Mutex<HashMap<i64, oneshot::Sender<Result<Value, String>>>>,
    error: &str,
) {
    let mut pending = pending.lock().await;
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(error.to_string()));
    }
}

struct ReaderShared {
    pending: Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, String>>>>>,
    stdin: Arc<Mutex<ChildStdin>>,
    diagnostics: Arc<Mutex<HashMap<String, Value>>>,
    workspace: std::path::PathBuf,
    settings: Option<Value>,
    server_status: Arc<Mutex<Option<ServerStatus>>>,
    progress: Arc<Mutex<HashSet<String>>>,
}

async fn read_loop(stdout: ChildStdout, shared: ReaderShared) -> Result<(), String> {
    let mut reader = BufReader::new(stdout);
    loop {
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            let n = reader
                .read_line(&mut line)
                .await
                .map_err(|err| err.to_string())?;
            if n == 0 {
                fail_pending(&shared.pending, "language server stdout closed").await;
                return Ok(());
            }
            if headers.len() + line.len() > MAX_HEADER_BYTES {
                fail_pending(&shared.pending, "LSP header exceeded bound").await;
                return Err("LSP header exceeded bound".into());
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
            headers.push_str(&line);
        }
        let length = headers
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length:"))
            .and_then(|v| v.trim().parse::<usize>().ok())
            .ok_or_else(|| "missing Content-Length".to_string())?;
        if length > MAX_CONTENT_LENGTH {
            fail_pending(&shared.pending, "incoming Content-Length exceeds cap").await;
            return Err("incoming Content-Length exceeds cap".into());
        }
        let mut body = vec![0u8; length];
        if let Err(error) = reader.read_exact(&mut body).await {
            fail_pending(&shared.pending, &error.to_string()).await;
            return Err(error.to_string());
        }
        let value: Value = match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(error) => {
                fail_pending(&shared.pending, &error.to_string()).await;
                return Err(error.to_string());
            }
        };
        if let Some(method) = value.get("method").and_then(Value::as_str) {
            handle_server_message(&shared, method, &value).await?;
            if value.get("id").is_some()
                && value.get("result").is_none()
                && value.get("error").is_none()
            {
                continue;
            }
            if value.get("id").is_none() {
                continue;
            }
        }
        if let Some(id) = value.get("id").and_then(Value::as_i64) {
            let result = if let Some(error) = value.get("error") {
                Err(error.to_string())
            } else {
                Ok(value.get("result").cloned().unwrap_or(Value::Null))
            };
            if let Some(tx) = shared.pending.lock().await.remove(&id) {
                let _ = tx.send(result);
            }
        }
    }
}

async fn handle_server_message(
    shared: &ReaderShared,
    method: &str,
    value: &Value,
) -> Result<(), String> {
    match method {
        "textDocument/publishDiagnostics" => {
            if let Some(uri) = value.pointer("/params/uri").and_then(Value::as_str) {
                shared.diagnostics.lock().await.insert(
                    uri.to_string(),
                    value.get("params").cloned().unwrap_or(Value::Null),
                );
            }
            return Ok(());
        }
        "experimental/serverStatus" => {
            let quiescent = value
                .pointer("/params/quiescent")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            *shared.server_status.lock().await = Some(ServerStatus { quiescent });
            return Ok(());
        }
        "$/progress" => {
            let token = progress_token(value.pointer("/params/token"));
            let kind = value
                .pointer("/params/value/kind")
                .and_then(Value::as_str)
                .unwrap_or("");
            let mut progress = shared.progress.lock().await;
            match kind {
                "begin" => {
                    progress.insert(token);
                }
                "end" => {
                    progress.remove(&token);
                }
                _ => {}
            }
            return Ok(());
        }
        _ => {}
    }
    let Some(id) = value.get("id").cloned() else {
        return Ok(());
    };
    let result = match method {
        "workspace/applyEdit" => json!({ "applied": false }),
        "workspace/configuration" => configuration_result(value.get("params"), &shared.settings),
        "workspace/workspaceFolders" => json!([{
            "uri": super::uri::path_to_file_uri(&shared.workspace),
            "name": shared.workspace.file_name().and_then(|n| n.to_str()).unwrap_or("workspace")
        }]),
        "window/workDoneProgress/create"
        | "client/registerCapability"
        | "client/unregisterCapability"
        | "window/showMessageRequest"
        | "workspace/semanticTokens/refresh"
        | "workspace/inlayHint/refresh"
        | "workspace/codeLens/refresh"
        | "workspace/codeAction/refresh"
        | "workspace/inlineValue/refresh"
        | "workspace/foldingRange/refresh"
        | "workspace/diagnostic/refresh" => json!(null),
        "window/showDocument" => json!({ "success": false }),
        _ => json!(null),
    };
    let reply = json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    });
    if let Err(error) = write_message(&mut *shared.stdin.lock().await, &reply).await {
        fail_pending(&shared.pending, &error).await;
        return Err(error);
    }
    Ok(())
}

fn progress_token(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(token)) => token.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => "progress".into(),
    }
}
