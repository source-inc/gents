use super::*;
use crate::tool_surface::{BehaviorToolConfig, FileToolMode, ToolCeiling, ToolSelection};
use crate::toolset::shared::ToolContext;
use crate::toolset::{CommandConstraints, CommandExecutionMode, CommandNetworkMode};

fn first_line_containing(path: &std::path::Path, needle: &str) -> u32 {
    let text = std::fs::read_to_string(path).unwrap_or_else(|err| {
        panic!("read {}: {err}", path.display());
    });
    text.lines()
        .position(|line| line.contains(needle))
        .map(|idx| u32::try_from(idx + 1).expect("line"))
        .unwrap_or_else(|| panic!("{}: missing `{needle}`", path.display()))
}

fn rustc_sysroot() -> Option<String> {
    let output = std::process::Command::new("rustc")
        .args(["--print", "sysroot"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sysroot = String::from_utf8(output.stdout).ok()?;
    let sysroot = sysroot.trim();
    if sysroot.is_empty() {
        None
    } else {
        Some(sysroot.to_string())
    }
}

fn write_isolated_rust_project(root: &std::path::Path) {
    let Some(sysroot) = rustc_sysroot() else {
        return;
    };
    let lib =
        std::fs::canonicalize(root.join("src/lib.rs")).unwrap_or_else(|_| root.join("src/lib.rs"));
    let doc = serde_json::json!({
        "sysroot": sysroot,
        "crates": [{
            "display_name": "lsp_demo_fixture",
            "root_module": lib,
            "edition": "2021",
            "deps": [],
            "is_workspace_member": true,
            "cfg": ["unix"]
        }]
    });
    std::fs::write(root.join("rust-project.json"), doc.to_string()).unwrap();
}

fn rust_analyzer_server(timings_ms: u64) -> CatalogServer {
    CatalogServer {
        name: "rust-analyzer".into(),
        command: "rust-analyzer".into(),
        args: vec![],
        file_types: vec![".rs".into()],
        root_markers: vec!["Cargo.toml".into(), "rust-project.json".into()],
        is_linter: false,
        priority: 1,
        language_id: Some("rust".into()),
        init_options: None,
        settings: Some(serde_json::json!({
            "rust-analyzer": { "checkOnSave": false }
        })),
        capabilities: None,
        workspace_ready_timings: Some(serde_json::json!({ "initial": timings_ms })),
        warmup_timeout_ms: Some(30_000),
    }
}

fn sample_config(
    workspace: std::path::PathBuf,
    file: FileToolMode,
    session_id: &str,
    servers: Vec<CatalogServer>,
) -> LspToolConfig {
    let constraints = CommandConstraints {
        allowed_argv_prefixes: Vec::new(),
        forbidden_argv_prefixes: Vec::new(),
        network_mode: CommandNetworkMode::Inherit,
        execution_mode: CommandExecutionMode::Unrestricted,
        sandbox: CommandExecutionMode::Unrestricted,
        deny_all_argv: false,
        deny_git_metadata_writes: false,
    };
    LspToolConfig {
        lsp: true,
        file,
        digest: config_digest(&workspace, &servers, &constraints),
        workspace,
        session_id: session_id.into(),
        behavior_id: "b1".into(),
        servers,
        constraints,
        format_on_write: false,
        diagnostics_on_write: false,
        diagnostics_on_edit: false,
        diagnostics_deduplicate: false,
        idle_timeout: std::time::Duration::from_secs(300),
    }
}

#[test]
fn advertised_only_when_enabled_and_file_tools_on() {
    assert!(lsp_advertised(true, FileToolMode::ReadOnly));
    assert!(lsp_advertised(true, FileToolMode::ReadWrite));
    assert!(!lsp_advertised(true, FileToolMode::Off));
    assert!(!lsp_advertised(false, FileToolMode::ReadWrite));
}

#[test]
fn primary_routing_uses_file_type_then_priority_with_catalog_order_ties() {
    let mut first = fixture_server(FIXTURE_PY.into());
    first.name = "first".into();
    first.file_types = vec![".ts".into()];
    first.priority = 20;
    let mut preferred = first.clone();
    preferred.name = "preferred".into();
    preferred.priority = 10;
    let mut unrelated = first.clone();
    unrelated.name = "rust".into();
    unrelated.file_types = vec![".rs".into()];
    unrelated.priority = 1;
    let servers = vec![first, preferred.clone(), unrelated];

    assert_eq!(
        super::catalog::primary_for_file(&servers, std::path::Path::new("src/app.ts"))
            .map(|server| server.name.as_str()),
        Some("preferred")
    );
    assert_eq!(
        super::catalog::primary_for_workspace(&servers).map(|server| server.name.as_str()),
        Some("rust")
    );

    let mut tied = preferred;
    tied.name = "later-tie".into();
    let tied_servers = vec![servers[1].clone(), tied];
    assert_eq!(
        super::catalog::primary_for_file(&tied_servers, std::path::Path::new("src/app.ts"))
            .map(|server| server.name.as_str()),
        Some("preferred"),
        "equal priority keeps catalog order"
    );
}

#[test]
fn tool_surface_includes_lsp_when_policy_allows() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"x\"\nversion=\"0.1.0\"\n",
    )
    .unwrap();
    let mut selection = ToolSelection::default();
    selection.enable_lsp = true;
    selection.file_tools = FileToolMode::ReadOnly;
    selection.file_tool_root = Some(root.path().to_path_buf());
    let ceiling = ToolCeiling::readonly_at(root.path());
    let config =
        BehaviorToolConfig::from_selection("beh", selection, &ceiling, Vec::new()).unwrap();
    let surface = config.resolve_with_subagent_tools_for_mcp_presence(false, Default::default());
    assert!(surface.tool_names().iter().any(|name| name == "lsp"));
}

#[test]
fn tool_surface_omits_lsp_when_disabled() {
    let root = tempfile::tempdir().unwrap();
    let mut selection = ToolSelection::default();
    selection.enable_lsp = false;
    selection.file_tools = FileToolMode::ReadOnly;
    selection.file_tool_root = Some(root.path().to_path_buf());
    let ceiling = ToolCeiling::readonly_at(root.path());
    let config =
        BehaviorToolConfig::from_selection("beh", selection, &ceiling, Vec::new()).unwrap();
    let surface = config.resolve_with_subagent_tools_for_mcp_presence(false, Default::default());
    assert!(!surface.tool_names().iter().any(|name| name == "lsp"));
}

#[tokio::test]
async fn inbound_uri_escape_is_policy_denied() {
    let root = tempfile::tempdir().unwrap();
    let context = ToolContext::new(root.path().to_path_buf(), false).unwrap();
    let err = edits::resolve_inbound_path(&context, "/etc/passwd").unwrap_err();
    assert!(err.contains("outside") || err.contains("allowed"), "{err}");
}

#[tokio::test]
async fn invalid_inbound_file_is_rejected_before_server_start() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname='t'\nversion='0.1.0'\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let tool = LspTool::new(
        sample_config(
            root.path().to_path_buf(),
            FileToolMode::ReadOnly,
            "invalid-before-start",
            vec![fixture_server(FIXTURE_PY.into())],
        ),
        pool.clone(),
    )
    .unwrap();
    let error = tool
        .call(LspArgs {
            action: "hover".into(),
            file: Some("untitled:outside".into()),
            line: Some(1),
            symbol: Some("outside".into()),
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: Some(5),
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unsupported URI"), "{error}");
    assert_eq!(pool.live_count().await, 0);
}

#[test]
fn self_config_rejects_settings_and_command() {
    let err = LspConfigDocument::parse_self_config(Some(
        r#"{"servers":{"rust-analyzer":{"command":"/tmp/evil"}}}"#,
    ))
    .unwrap_err();
    assert!(err.contains("command"), "{err}");
}

const FIXTURE_PY: &str = r#"
import json, sys
def read():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b'\r\n', b'\n'):
            break
        key, _, value = line.decode().partition(':')
        headers[key.strip().lower()] = value.strip()
    n = int(headers.get('content-length', '0'))
    return json.loads(sys.stdin.buffer.read(n) or b'null')
def write(obj):
    body = json.dumps(obj).encode()
    sys.stdout.buffer.write(f'Content-Length: {len(body)}\r\n\r\n'.encode())
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
while True:
    msg = read()
    if msg is None:
        break
    method = msg.get('method')
    mid = msg.get('id')
    if method == 'initialize':
        write({"jsonrpc":"2.0","id":mid,"result":{"capabilities":{},"serverInfo":{"name":"fixture"}}})
    elif method == 'shutdown':
        write({"jsonrpc":"2.0","id":mid,"result":None})
    elif method == 'textDocument/hover':
        write({"jsonrpc":"2.0","id":mid,"result":{"contents":"hello hover"}})
    elif method == 'textDocument/definition':
        uri = msg['params']['textDocument']['uri']
        write({"jsonrpc":"2.0","id":mid,"result":[{"uri":uri,"range":{"start":{"line":0,"character":0},"end":{"line":0,"character":1}}}]})
    elif method == 'textDocument/rename':
        uri = msg['params']['textDocument']['uri']
        write({"jsonrpc":"2.0","id":mid,"result":{"changes":{uri:[{"range":{"start":{"line":0,"character":0},"end":{"line":0,"character":1}},"newText":"Z"}]}}})
    elif method == 'exit':
        break
    elif mid is not None:
        write({"jsonrpc":"2.0","id":mid,"result":None})
"#;

#[tokio::test]
async fn fixture_hover_definition_and_rename_preview() {
    if std::process::Command::new("python3")
        .arg("-c")
        .arg("print(1)")
        .output()
        .map(|o| !o.status.success())
        .unwrap_or(true)
    {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("lib.rs");
    std::fs::write(&file, "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\nedition=\"2021\"\n",
    )
    .unwrap();
    let server = CatalogServer {
        name: "fixture".into(),
        command: "python3".into(),
        args: vec!["-c".into(), FIXTURE_PY.into()],
        file_types: vec![".rs".into()],
        root_markers: vec!["Cargo.toml".into()],
        is_linter: false,
        priority: 1,
        language_id: Some("rust".into()),
        init_options: None,
        settings: None,
        capabilities: None,
        workspace_ready_timings: None,
        warmup_timeout_ms: None,
    };
    let tool = LspTool::new(
        sample_config(
            root.path().to_path_buf(),
            FileToolMode::ReadWrite,
            "s1",
            vec![server],
        ),
        LspPool::new(),
    )
    .unwrap();
    let hover = tool
        .call(LspArgs {
            action: "hover".into(),
            file: Some("lib.rs".into()),
            line: Some(1),
            symbol: Some("x".into()),
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: None,
        })
        .await
        .expect("hover");
    assert!(hover.contains("hello hover"), "{hover}");
    let defn = tool
        .call(LspArgs {
            action: "definition".into(),
            file: Some("lib.rs".into()),
            line: Some(1),
            symbol: Some("x".into()),
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: None,
        })
        .await
        .expect("definition");
    assert!(defn.contains("lib.rs") || defn.contains("Found"), "{defn}");
    let preview = tool
        .call(LspArgs {
            action: "rename".into(),
            file: Some("lib.rs".into()),
            line: Some(1),
            symbol: Some("x".into()),
            query: None,
            new_name: Some("Z".into()),
            apply: Some(false),
            payload: None,
            timeout: None,
        })
        .await
        .expect("rename preview");
    assert!(!preview.is_empty(), "{preview}");
}

#[tokio::test]
async fn rename_file_allows_a_new_destination() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("old.rs");
    let destination = root.path().join("new.rs");
    std::fs::write(&source, "fn old() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let tool = LspTool::new(
        sample_config(
            root.path().to_path_buf(),
            FileToolMode::ReadWrite,
            "s-rename-file",
            vec![fixture_server(FIXTURE_PY.into())],
        ),
        LspPool::new(),
    )
    .unwrap();
    let output = tool
        .call(LspArgs {
            action: "rename_file".into(),
            file: Some("old.rs".into()),
            line: None,
            symbol: None,
            query: None,
            new_name: Some("new.rs".into()),
            apply: Some(true),
            payload: None,
            timeout: Some(5),
        })
        .await
        .expect("rename_file must admit a destination that does not exist yet");
    assert!(output.contains("Applied edit"), "{output}");
    assert!(!source.exists());
    assert_eq!(
        std::fs::read_to_string(destination).unwrap(),
        "fn old() {}\n"
    );
}

#[test]
fn workspace_edit_rename_allows_a_new_destination() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("old.rs");
    let destination = root.path().join("nested/new.rs");
    std::fs::write(&source, "fn old() {}\n").unwrap();
    let context = ToolContext::new(root.path().to_path_buf(), false).unwrap();
    let edit = serde_json::json!({
        "documentChanges": [{
            "kind": "rename",
            "oldUri": super::uri::path_to_file_uri(&source),
            "newUri": super::uri::path_to_file_uri(&destination)
        }]
    });
    let prepared = super::edits::prepare_workspace_edit(
        &context,
        &edit,
        super::encoding::PositionEncoding::Utf16,
    )
    .expect("newUri may name a not-yet-created path under the tool root");
    assert_eq!(prepared.len(), 1);
    let canonical_root = std::fs::canonicalize(root.path()).unwrap();
    assert_eq!(prepared[0].path, canonical_root.join("nested/new.rs"));
    assert_eq!(
        prepared[0].rename_from.as_deref(),
        Some(canonical_root.join("old.rs").as_path())
    );
}

#[tokio::test]
async fn workspace_edit_preflights_every_file_before_writing_any_file() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let first = root.path().join("first.rs");
    let stale = root.path().join("stale.rs");
    std::fs::write(&first, "fn first() {}\n").unwrap();
    std::fs::write(&stale, "fn stale() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname='t'\nversion='0.1.0'\n",
    )
    .unwrap();
    let server = fixture_server(FIXTURE_PY.into());
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-atomic-preflight",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-atomic-preflight".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: server.name.clone(),
        config_digest: config.digest.clone(),
    };
    let pool = LspPool::new();
    let lease = pool.get_or_start(key, &server, &config).await.unwrap();
    let prepared = vec![
        super::edits::PreparedEdit {
            path: std::fs::canonicalize(&first).unwrap(),
            new_bytes: b"fn rewritten() {}\n".to_vec(),
            expected_hash: Some(crate::toolset::file_tools::content_hash(b"fn first() {}\n")),
            version: None,
            rename_from: None,
        },
        super::edits::PreparedEdit {
            path: std::fs::canonicalize(&stale).unwrap(),
            new_bytes: b"fn also_rewritten() {}\n".to_vec(),
            expected_hash: Some("not-the-current-hash".into()),
            version: None,
            rename_from: None,
        },
    ];
    let context = ToolContext::new(root.path().to_path_buf(), false).unwrap();
    let _guards = super::edits::acquire_mutation_locks(&prepared).await;
    let error = super::edits::apply_prepared_with_held_locks(&context, lease.client(), &prepared)
        .await
        .expect_err("stale later file must reject the whole edit before writes");
    assert!(error.to_string().contains("changed between preflight"));
    assert_eq!(std::fs::read_to_string(first).unwrap(), "fn first() {}\n");
}

#[test]
fn text_edit_rejects_a_mid_codepoint_utf8_position() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("lib.rs");
    std::fs::write(&path, "a😀b\n").unwrap();
    let context = ToolContext::new(root.path().to_path_buf(), false).unwrap();
    let uri = super::uri::path_to_file_uri(&path);
    let edit = serde_json::json!({
        "changes": {
            (uri): [{
                "range": {
                    "start": {"line": 0, "character": 2},
                    "end": {"line": 0, "character": 2}
                },
                "newText": "x"
            }]
        }
    });
    let error = super::edits::prepare_workspace_edit(
        &context,
        &edit,
        super::encoding::PositionEncoding::Utf8,
    )
    .unwrap_err();
    assert!(error.contains("multibyte"), "{error}");
}

#[tokio::test]
async fn status_does_not_start_a_server() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname='t'\nversion='0.1.0'\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let tool = LspTool::new(
        sample_config(
            root.path().to_path_buf(),
            FileToolMode::ReadOnly,
            "s-status",
            vec![fixture_server(FIXTURE_PY.into())],
        ),
        pool.clone(),
    )
    .unwrap();
    let output = tool
        .call(LspArgs {
            action: "status".into(),
            file: None,
            line: None,
            symbol: None,
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: None,
        })
        .await
        .expect("status");
    assert!(
        output.contains("fixture (not started — run a server-backed action"),
        "{output}"
    );
    assert_eq!(pool.live_count().await, 0);
}

#[tokio::test]
async fn writethrough_does_not_start_a_client() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("lib.rs");
    std::fs::write(&path, "fn x() {}\n").unwrap();
    let pool = LspPool::new();
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-wt",
        vec![CatalogServer {
            name: "fixture".into(),
            command: "python3".into(),
            args: vec!["-c".into(), FIXTURE_PY.into()],
            file_types: vec![".rs".into()],
            root_markers: vec!["Cargo.toml".into()],
            is_linter: false,
            priority: 1,
            language_id: Some("rust".into()),
            init_options: None,
            settings: None,
            capabilities: None,
            workspace_ready_timings: None,
            warmup_timeout_ms: None,
        }],
    );
    let writethrough = LspWritethrough::new(pool.clone(), config);
    let _ = writethrough.after_mutation(&path).await;
    assert_eq!(pool.live_count().await, 0);
}

#[tokio::test]
async fn read_output_escape_redacts_and_completes() {
    if std::process::Command::new("python3")
        .arg("-c")
        .arg("print(1)")
        .output()
        .map(|o| !o.status.success())
        .unwrap_or(true)
    {
        return;
    }
    let fixture = r#"
import json, sys
def read():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b'\r\n', b'\n'):
            break
        key, _, value = line.decode().partition(':')
        headers[key.strip().lower()] = value.strip()
    n = int(headers.get('content-length', '0'))
    return json.loads(sys.stdin.buffer.read(n) or b'null')
def write(obj):
    body = json.dumps(obj).encode()
    sys.stdout.buffer.write(f'Content-Length: {len(body)}\r\n\r\n'.encode())
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
while True:
    msg = read()
    if msg is None:
        break
    method = msg.get('method')
    mid = msg.get('id')
    if method == 'initialize':
        write({"jsonrpc":"2.0","id":mid,"result":{"capabilities":{},"serverInfo":{"name":"fixture"}}})
    elif method == 'textDocument/definition':
        write({"jsonrpc":"2.0","id":mid,"result":[{"uri":"file:///etc/passwd","range":{"start":{"line":0,"character":0},"end":{"line":0,"character":1}}}]})
    elif method == 'shutdown':
        write({"jsonrpc":"2.0","id":mid,"result":None})
    elif method == 'exit':
        break
    elif mid is not None:
        write({"jsonrpc":"2.0","id":mid,"result":None})
"#;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let tool = LspTool::new(
        sample_config(
            root.path().to_path_buf(),
            FileToolMode::ReadOnly,
            "s-redact",
            vec![CatalogServer {
                name: "fixture".into(),
                command: "python3".into(),
                args: vec!["-c".into(), fixture.into()],
                file_types: vec![".rs".into()],
                root_markers: vec!["Cargo.toml".into()],
                is_linter: false,
                priority: 1,
                language_id: Some("rust".into()),
                init_options: None,
                settings: None,
                capabilities: None,
                workspace_ready_timings: None,
                warmup_timeout_ms: None,
            }],
        ),
        LspPool::new(),
    )
    .unwrap();
    let out = tool
        .call(LspArgs {
            action: "definition".into(),
            file: Some("lib.rs".into()),
            line: Some(1),
            symbol: Some("x".into()),
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: None,
        })
        .await
        .expect("definition completes");
    assert!(!out.contains("/etc/passwd"), "{out}");
    assert!(
        out.to_lowercase().contains("omitted") || out.to_lowercase().contains("redact"),
        "{out}"
    );
}

#[tokio::test]
async fn workspace_apply_edit_is_noop() {
    if std::process::Command::new("python3")
        .arg("-c")
        .arg("print(1)")
        .output()
        .map(|o| !o.status.success())
        .unwrap_or(true)
    {
        return;
    }
    let pwned = tempfile::tempdir().unwrap();
    let target = pwned.path().join("pwned.txt");
    let fixture = format!(
        r#"
import json, sys
def read():
    headers = {{}}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b'\r\n', b'\n'):
            break
        key, _, value = line.decode().partition(':')
        headers[key.strip().lower()] = value.strip()
    n = int(headers.get('content-length', '0'))
    return json.loads(sys.stdin.buffer.read(n) or b'null')
def write(obj):
    body = json.dumps(obj).encode()
    sys.stdout.buffer.write(f'Content-Length: {{len(body)}}\r\n\r\n'.encode())
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
sent = False
while True:
    msg = read()
    if msg is None:
        break
    method = msg.get('method')
    mid = msg.get('id')
    if method == 'initialize':
        write({{"jsonrpc":"2.0","id":mid,"result":{{"capabilities":{{}},"serverInfo":{{"name":"fixture"}}}}}})
        write({{"jsonrpc":"2.0","id":99,"method":"workspace/applyEdit","params":{{"edit":{{"changes":{{"file://{target}":[{{"range":{{"start":{{"line":0,"character":0}},"end":{{"line":0,"character":0}}}},"newText":"pwned"}}]}}}}}}}})
    elif method == 'textDocument/hover':
        write({{"jsonrpc":"2.0","id":mid,"result":{{"contents":"ok"}}}})
    elif method == 'shutdown':
        write({{"jsonrpc":"2.0","id":mid,"result":None}})
    elif method == 'exit':
        break
    elif mid is not None:
        write({{"jsonrpc":"2.0","id":mid,"result":None}})
"#,
        target = target.display()
    );
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let tool = LspTool::new(
        sample_config(
            root.path().to_path_buf(),
            FileToolMode::ReadWrite,
            "s-apply",
            vec![CatalogServer {
                name: "fixture".into(),
                command: "python3".into(),
                args: vec!["-c".into(), fixture],
                file_types: vec![".rs".into()],
                root_markers: vec!["Cargo.toml".into()],
                is_linter: false,
                priority: 1,
                language_id: Some("rust".into()),
                init_options: None,
                settings: None,
                capabilities: None,
                workspace_ready_timings: None,
                warmup_timeout_ms: None,
            }],
        ),
        LspPool::new(),
    )
    .unwrap();
    let _ = tool
        .call(LspArgs {
            action: "hover".into(),
            file: Some("lib.rs".into()),
            line: Some(1),
            symbol: Some("x".into()),
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: None,
        })
        .await;
    assert!(!target.exists(), "workspace/applyEdit must not write files");
}

#[tokio::test]
async fn request_cancel_does_not_kill_pooled_server() {
    if std::process::Command::new("python3")
        .arg("-c")
        .arg("print(1)")
        .output()
        .map(|o| !o.status.success())
        .unwrap_or(true)
    {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let server = CatalogServer {
        name: "fixture".into(),
        command: "python3".into(),
        args: vec!["-c".into(), FIXTURE_PY.into()],
        file_types: vec![".rs".into()],
        root_markers: vec!["Cargo.toml".into()],
        is_linter: false,
        priority: 1,
        language_id: Some("rust".into()),
        init_options: None,
        settings: None,
        capabilities: None,
        workspace_ready_timings: None,
        warmup_timeout_ms: None,
    };
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-cancel",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-cancel".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: "fixture".into(),
        config_digest: config.digest.clone(),
    };
    let lease = pool
        .get_or_start(key.clone(), &server, &config)
        .await
        .expect("start");
    let request_token = tokio_util::sync::CancellationToken::new();
    request_token.cancel();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(pool.live_count().await, 1);
    drop(lease);
    assert!(pool.has_ready(&key).await);
}

#[test]
fn tighter_ceiling_changes_digest() {
    let root = tempfile::tempdir().unwrap();
    let servers = vec![];
    let loose = CommandConstraints {
        allowed_argv_prefixes: Vec::new(),
        forbidden_argv_prefixes: Vec::new(),
        network_mode: CommandNetworkMode::Inherit,
        execution_mode: CommandExecutionMode::Unrestricted,
        sandbox: CommandExecutionMode::Unrestricted,
        deny_all_argv: false,
        deny_git_metadata_writes: false,
    };
    let tight = CommandConstraints {
        forbidden_argv_prefixes: vec![vec!["rust-analyzer".into()]],
        network_mode: CommandNetworkMode::Disabled,
        sandbox: CommandExecutionMode::WorkspaceWrite,
        ..loose.clone()
    };
    assert_ne!(
        config_digest(root.path(), &servers, &loose),
        config_digest(root.path(), &servers, &tight)
    );
}

#[test]
fn readonly_surface_uses_file_tool_root_not_cwd() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"x\"\nversion=\"0.1.0\"\n",
    )
    .unwrap();
    let mut selection = ToolSelection::default();
    selection.enable_lsp = true;
    selection.file_tools = FileToolMode::ReadOnly;
    selection.file_tool_root = Some(root.path().to_path_buf());
    let ceiling = ToolCeiling::readonly_at(root.path());
    let config =
        BehaviorToolConfig::from_selection("beh", selection, &ceiling, Vec::new()).unwrap();
    let surface = config.resolve_with_subagent_tools_for_mcp_presence(false, Default::default());
    let lsp = surface.lsp_config().expect("lsp advertised");
    assert_eq!(
        std::fs::canonicalize(&lsp.workspace).unwrap_or(lsp.workspace.clone()),
        std::fs::canonicalize(root.path()).unwrap()
    );
}

#[tokio::test]
async fn execute_command_is_argument_invalid() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    let tool = LspTool::new(
        sample_config(
            root.path().to_path_buf(),
            FileToolMode::ReadWrite,
            "s-exec",
            vec![],
        ),
        LspPool::new(),
    )
    .unwrap();
    let err = tool
        .call(LspArgs {
            action: "request".into(),
            file: None,
            line: None,
            symbol: None,
            query: Some("workspace/executeCommand".into()),
            new_name: None,
            apply: None,
            payload: Some(r#"{"command":"evil"}"#.into()),
            timeout: None,
        })
        .await
        .unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("executeCommand")
            || text.contains("unknown")
            || text.contains("not supported"),
        "{text}"
    );
}

fn python3_available() -> bool {
    std::process::Command::new("python3")
        .arg("-c")
        .arg("print(1)")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn fixture_server(args: String) -> CatalogServer {
    CatalogServer {
        name: "fixture".into(),
        command: "python3".into(),
        args: vec!["-c".into(), args],
        file_types: vec![".rs".into()],
        root_markers: vec!["Cargo.toml".into()],
        is_linter: false,
        priority: 1,
        language_id: Some("rust".into()),
        init_options: None,
        settings: None,
        capabilities: None,
        workspace_ready_timings: None,
        warmup_timeout_ms: None,
    }
}

#[tokio::test]
async fn simultaneous_first_calls_share_one_process() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let server = fixture_server(FIXTURE_PY.into());
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-singleflight",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-singleflight".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: "fixture".into(),
        config_digest: config.digest.clone(),
    };
    let first = pool.get_or_start(key.clone(), &server, &config);
    let second = pool.get_or_start(key.clone(), &server, &config);
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(first, second)
    })
    .await
    .expect("singleflight waiters must not lose the Ready notification");
    assert!(a.is_ok(), "first start failed");
    assert!(b.is_ok(), "second start failed");
    assert_eq!(pool.live_count().await, 1);
}

#[tokio::test]
async fn resolved_executable_path_is_subject_to_command_prefix_policy() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname='t'\nversion='0.1.0'\n",
    )
    .unwrap();
    let server = fixture_server(FIXTURE_PY.into());
    let admitted = super::admit::admit_command(&server.command, root.path()).unwrap();
    let mut config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadOnly,
        "s-resolved-prefix",
        vec![server.clone()],
    );
    config.constraints.forbidden_argv_prefixes = vec![vec![admitted.to_string_lossy().into()]];
    config.digest = config_digest(&config.workspace, &config.servers, &config.constraints);
    let key = PoolKey {
        session_id: "s-resolved-prefix".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: server.name.clone(),
        config_digest: config.digest.clone(),
    };
    let error = match LspPool::new().get_or_start(key, &server, &config).await {
        Ok(_) => panic!("absolute-path prefix must constrain a catalog bare command"),
        Err(error) => error,
    };
    assert!(error.to_ascii_lowercase().contains("forbidden"), "{error}");
}

#[tokio::test]
async fn semantic_retry_handles_empty_arrays_with_one_total_budget() {
    if !python3_available() {
        return;
    }
    let fixture = r#"
import json, sys
calls = 0
def read():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b'\r\n', b'\n'):
            break
        key, _, value = line.decode().partition(':')
        headers[key.strip().lower()] = value.strip()
    n = int(headers.get('content-length', '0'))
    return json.loads(sys.stdin.buffer.read(n) or b'null')
def write(obj):
    body = json.dumps(obj).encode()
    sys.stdout.buffer.write(f'Content-Length: {len(body)}\r\n\r\n'.encode())
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
while True:
    msg = read()
    if msg is None:
        break
    method = msg.get('method')
    mid = msg.get('id')
    if method == 'initialize':
        write({"jsonrpc":"2.0","id":mid,"result":{"capabilities":{}}})
    elif method == 'textDocument/documentSymbol':
        calls += 1
        result = [] if calls == 1 else [{"name":"retried","kind":12,"range":{"start":{"line":0,"character":0}},"selectionRange":{"start":{"line":0,"character":0}}}]
        write({"jsonrpc":"2.0","id":mid,"result":result})
    elif method == 'workspace/symbol':
        write({"jsonrpc":"2.0","id":mid,"result":[]})
    elif method == 'shutdown':
        write({"jsonrpc":"2.0","id":mid,"result":None})
    elif method == 'exit':
        break
    elif mid is not None:
        write({"jsonrpc":"2.0","id":mid,"result":None})
"#;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn retried() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname='t'\nversion='0.1.0'\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let server = fixture_server(fixture.into());
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadOnly,
        "s-empty-array",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-empty-array".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: server.name.clone(),
        config_digest: config.digest.clone(),
    };
    let lease = pool.get_or_start(key, &server, &config).await.unwrap();
    let result = super::actions::request_maybe_retry(
        lease.client(),
        "textDocument/documentSymbol",
        serde_json::json!({"textDocument":{"uri": super::uri::path_to_file_uri(&root.path().join("lib.rs"))}}),
        std::time::Duration::from_secs(2),
        true,
    )
    .await
    .expect("empty array should be retried");
    assert_eq!(result[0]["name"], "retried");

    let started = std::time::Instant::now();
    let empty = super::actions::request_maybe_retry(
        lease.client(),
        "workspace/symbol",
        serde_json::json!({"query":"missing"}),
        std::time::Duration::from_secs(5),
        true,
    )
    .await
    .expect("empty result at deadline is a semantic empty response");
    assert!(empty.is_null() || empty.as_array().is_some_and(Vec::is_empty));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "genuinely empty results must stop after the bounded retry count: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn ensure_open_resyncs_when_disk_text_changes_out_of_band() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("lib.rs");
    std::fs::write(&path, "fn before() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname='t'\nversion='0.1.0'\n",
    )
    .unwrap();
    let server = fixture_server(FIXTURE_PY.into());
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadOnly,
        "s-resync",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-resync".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: server.name.clone(),
        config_digest: config.digest.clone(),
    };
    let pool = LspPool::new();
    let lease = pool.get_or_start(key, &server, &config).await.unwrap();
    let uri = super::uri::path_to_file_uri(&path);
    lease
        .client()
        .ensure_open(&uri, "rust", "fn before() {}\n")
        .await
        .unwrap();
    lease
        .client()
        .ensure_open(&uri, "rust", "fn after() {}\n")
        .await
        .unwrap();
    assert_eq!(lease.client().tracked_version(&uri).await, Some(2));
}

#[tokio::test]
async fn exited_server_is_evicted_and_restarted_on_the_next_action() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname='t'\nversion='0.1.0'\n",
    )
    .unwrap();
    let counter = root.path().join("starts.txt");
    let fixture = format!(
        r#"
import json, sys
open({counter:?}, "a").write("x\n")
def read():
    headers = {{}}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b'\r\n', b'\n'):
            break
        key, _, value = line.decode().partition(':')
        headers[key.strip().lower()] = value.strip()
    n = int(headers.get('content-length', '0'))
    return json.loads(sys.stdin.buffer.read(n) or b'null')
def write(obj):
    body = json.dumps(obj).encode()
    sys.stdout.buffer.write(f'Content-Length: {{len(body)}}\r\n\r\n'.encode())
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
while True:
    msg = read()
    if msg is None:
        break
    method = msg.get('method')
    if method == 'initialize':
        write({{"jsonrpc":"2.0","id":msg.get('id'),"result":{{"capabilities":{{}}}}}})
    elif method == 'initialized':
        break
"#
    );
    let server = fixture_server(fixture);
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadOnly,
        "s-restart",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-restart".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: server.name.clone(),
        config_digest: config.digest.clone(),
    };
    let pool = LspPool::new();
    let first = pool
        .get_or_start(key.clone(), &server, &config)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while first.client().is_alive() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fixture server should exit");
    drop(first);
    let _second = pool.get_or_start(key, &server, &config).await.unwrap();
    let starts = std::fs::read_to_string(counter).unwrap();
    assert_eq!(starts.lines().count(), 2, "{starts:?}");
}

#[tokio::test]
async fn failed_initialize_wakes_waiters_and_backs_off() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let counter = root.path().join("starts.txt");
    let fixture = format!(
        r#"
import json, sys
open({counter:?}, "a").write("x\n")
def read():
    headers = {{}}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b'\r\n', b'\n'):
            break
        key, _, value = line.decode().partition(':')
        headers[key.strip().lower()] = value.strip()
    n = int(headers.get('content-length', '0'))
    return json.loads(sys.stdin.buffer.read(n) or b'null')
def write(obj):
    body = json.dumps(obj).encode()
    sys.stdout.buffer.write(f'Content-Length: {{len(body)}}\r\n\r\n'.encode())
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
while True:
    msg = read()
    if msg is None:
        break
    method = msg.get('method')
    mid = msg.get('id')
    if method == 'initialize':
        write({{"jsonrpc":"2.0","id":mid,"error":{{"code":-32000,"message":"nope"}}}})
    elif mid is not None:
        write({{"jsonrpc":"2.0","id":mid,"result":None}})
"#
    );
    let pool = LspPool::new();
    let server = fixture_server(fixture);
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-backoff",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-backoff".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: "fixture".into(),
        config_digest: config.digest.clone(),
    };
    let first = pool.get_or_start(key.clone(), &server, &config);
    let second = pool.get_or_start(key.clone(), &server, &config);
    let (a, b) = tokio::join!(first, second);
    assert!(a.is_err(), "first initialize should fail");
    assert!(b.is_err(), "waiter should see initialize failure");
    let during_backoff = pool.get_or_start(key.clone(), &server, &config).await;
    assert!(during_backoff.is_err(), "backoff should reject retry");
    let starts = std::fs::read_to_string(&counter).unwrap_or_default();
    assert_eq!(starts.matches('x').count(), 1, "{starts}");
    pool.expire_init_backoffs().await;
    let after = pool.get_or_start(key, &server, &config).await;
    assert!(
        after.is_err(),
        "retry after backoff should still fail initialize"
    );
    let starts = std::fs::read_to_string(&counter).unwrap_or_default();
    assert_eq!(starts.matches('x').count(), 2, "{starts}");
}

#[tokio::test]
async fn idle_sweep_retires_zero_lease_ready_clients() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let server = fixture_server(FIXTURE_PY.into());
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-idle",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-idle".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: "fixture".into(),
        config_digest: config.digest.clone(),
    };
    let lease = pool
        .get_or_start(key.clone(), &server, &config)
        .await
        .expect("start");
    drop(lease);
    pool.force_idle_timeout(&key, std::time::Duration::from_millis(1))
        .await;
    pool.force_last_used(
        &key,
        std::time::Instant::now() - std::time::Duration::from_secs(10),
    )
    .await;
    pool.sweep_idle().await;
    assert!(!pool.has_ready(&key).await);
    assert_eq!(pool.live_count().await, 0);
}

#[tokio::test]
async fn format_on_write_applies_without_starting_or_deadlocking() {
    if !python3_available() {
        return;
    }
    let fixture = r#"
import json, sys
def read():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b'\r\n', b'\n'):
            break
        key, _, value = line.decode().partition(':')
        headers[key.strip().lower()] = value.strip()
    n = int(headers.get('content-length', '0'))
    return json.loads(sys.stdin.buffer.read(n) or b'null')
def write(obj):
    body = json.dumps(obj).encode()
    sys.stdout.buffer.write(f'Content-Length: {len(body)}\r\n\r\n'.encode())
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
while True:
    msg = read()
    if msg is None:
        break
    method = msg.get('method')
    mid = msg.get('id')
    if method == 'initialize':
        write({"jsonrpc":"2.0","id":mid,"result":{"capabilities":{}}})
    elif method == 'textDocument/formatting':
        uri = msg['params']['textDocument']['uri']
        write({"jsonrpc":"2.0","id":mid,"result":[{"range":{"start":{"line":0,"character":0},"end":{"line":0,"character":4}},"newText":"fmt\n"}]})
    elif method == 'shutdown':
        write({"jsonrpc":"2.0","id":mid,"result":None})
    elif method == 'exit':
        break
    elif mid is not None:
        write({"jsonrpc":"2.0","id":mid,"result":None})
"#;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("lib.rs");
    std::fs::write(&path, "orig").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let server = fixture_server(fixture.into());
    let mut config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-fmt",
        vec![server.clone()],
    );
    config.format_on_write = true;
    let key = PoolKey {
        session_id: "s-fmt".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: "fixture".into(),
        config_digest: config.digest.clone(),
    };
    let _lease = pool
        .get_or_start(key, &server, &config)
        .await
        .expect("start");
    let writethrough = LspWritethrough::new(pool.clone(), config);
    let lock = super::super::file_tools::file_mutation_lock_for(&path);
    let _guard = lock.lock().await;
    let note = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        writethrough.after_mutation_under_lock(&path, MutationKind::Write),
    )
    .await
    .expect("format-on-write must not deadlock")
    .expect("format note");
    assert!(note.contains("format-on-write"), "{note}");
    drop(_guard);
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.starts_with("fmt"), "{body}");
}

#[test]
fn glob_match_parser_resolves_paths_from_the_effective_workspace_base() {
    let root = std::path::PathBuf::from("/tmp/ws");
    let base = root.join("sub");
    let raw = r#"{"matches":[{"path":"a.rs","entry_type":"file"}]}"#;
    let paths = super::super::native_runner::parse_glob_match_paths(raw, &base);
    assert_eq!(paths, vec![root.join("sub/a.rs")]);
}

#[tokio::test]
async fn rust_analyzer_starts_and_reports_ready() {
    if !std::process::Command::new("rust-analyzer")
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
    {
        return;
    }
    let src =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../demo/lsp-rust/workspace");
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("src")).unwrap();
    // rust-analyzer prefers Cargo.toml over rust-project.json. An isolated
    // temp crate has no lockfile/target, so cargo loading yields empty hover.
    std::fs::copy(src.join("src/lib.rs"), root.path().join("src/lib.rs")).unwrap();
    write_isolated_rust_project(root.path());
    assert!(
        rustc_sysroot().is_some(),
        "rustc --print sysroot is required for the isolated rust-project.json fixture"
    );
    let server = rust_analyzer_server(8_000);
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadOnly,
        "s-ra",
        vec![server.clone()],
    );
    let tool = LspTool::new(config, LspPool::new()).unwrap();
    let symbols = tool
        .call(LspArgs {
            action: "symbols".into(),
            file: Some("src/lib.rs".into()),
            line: None,
            symbol: None,
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: Some(40),
        })
        .await
        .expect("rust-analyzer must return document symbols after cold start");
    assert!(
        symbols.contains("add") && !symbols.contains("No symbols found"),
        "document symbols must include add, got: {symbols}"
    );
    let hover = tool
        .call(LspArgs {
            action: "hover".into(),
            file: Some("src/lib.rs".into()),
            line: Some(4),
            symbol: Some("add".into()),
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: Some(40),
        })
        .await
        .expect("rust-analyzer must accept hover after start");
    assert!(
        hover.contains("add")
            && (hover.contains("u32") || hover.contains("fn add") || hover.contains("left")),
        "hover must include the add signature, got: {hover}"
    );
    let status = tool
        .call(LspArgs {
            action: "status".into(),
            file: None,
            line: None,
            symbol: None,
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: None,
        })
        .await
        .expect("status");
    assert!(
        status.contains("rust-analyzer (ready)"),
        "expected a started rust-analyzer, got: {status}"
    );
}

/// rust-analyzer against this crate — the same facts the live e2e checks.
#[tokio::test]
#[ignore = "rust-analyzer on the gents crate graph; run with --ignored"]
async fn rust_analyzer_hover_on_gents_crate() {
    if !std::process::Command::new("rust-analyzer")
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
    {
        return;
    }
    let crate_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let auth = crate_root.join("src/toolset/lsp/auth.rs");
    let command = crate_root.join("src/toolset/shared/command.rs");
    let advertised_line = first_line_containing(&auth, "pub fn lsp_advertised");
    let meet_line = first_line_containing(&command, "pub fn meet(self, other: Self)");
    let tool = LspTool::new(
        sample_config(
            crate_root,
            FileToolMode::ReadOnly,
            "s-ra-gents",
            vec![rust_analyzer_server(45_000)],
        ),
        LspPool::new(),
    )
    .unwrap();

    for (file, symbol) in [
        ("src/toolset/lsp/auth.rs", "lsp_advertised"),
        ("src/toolset/shared/command.rs", "meet"),
    ] {
        let symbols = tool
            .call(LspArgs {
                action: "symbols".into(),
                file: Some(file.into()),
                line: None,
                symbol: None,
                query: None,
                new_name: None,
                apply: None,
                payload: None,
                timeout: Some(60),
            })
            .await
            .unwrap_or_else(|err| panic!("document symbols for {file}: {err}"));
        assert!(
            symbols.contains(symbol) && !symbols.contains("No symbols found"),
            "document symbols for {file} must include {symbol}, got: {symbols}"
        );
    }

    let advertised = tool
        .call(LspArgs {
            action: "hover".into(),
            file: Some("src/toolset/lsp/auth.rs".into()),
            line: Some(advertised_line),
            symbol: Some("lsp_advertised".into()),
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: Some(60),
        })
        .await
        .expect("hover lsp_advertised");
    assert!(
        advertised.contains("FileToolMode"),
        "lsp_advertised hover must name FileToolMode, got: {advertised}"
    );

    let meet = tool
        .call(LspArgs {
            action: "hover".into(),
            file: Some("src/toolset/shared/command.rs".into()),
            line: Some(meet_line),
            symbol: Some("meet".into()),
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: Some(60),
        })
        .await
        .expect("hover meet");
    assert!(
        meet.contains("Disabled") && meet.contains("Inherit"),
        "meet hover must document Disabled < Inherit, got: {meet}"
    );

    let status = tool
        .call(LspArgs {
            action: "status".into(),
            file: None,
            line: None,
            symbol: None,
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: None,
        })
        .await
        .expect("status");
    assert!(
        status.contains("rust-analyzer (ready)"),
        "expected a started rust-analyzer, got: {status}"
    );
}

#[test]
fn language_id_maps_extensions_to_protocol_ids() {
    assert_eq!(
        catalog::language_id_for_path(std::path::Path::new("src/lib.rs")),
        "rust"
    );
    assert_eq!(
        catalog::language_id_for_path(std::path::Path::new("main.py")),
        "python"
    );
    let rust = builtin_catalog()
        .into_iter()
        .find(|server| server.name == "rust-analyzer")
        .expect("rust-analyzer catalog entry");
    assert_eq!(rust.language_id.as_deref(), Some("rust"));
}

#[test]
fn code_action_query_selects_title_or_index() {
    let actions = vec![
        serde_json::json!({"title":"Extract function","edit":{"changes":{}}}),
        serde_json::json!({"title":"Inline variable","edit":{"changes":{"a":[]}}}),
        serde_json::json!({"title":"Need resolve","data":{"id":1}}),
    ];
    let by_title = super::actions::select_code_action(&actions, Some("inline")).unwrap();
    assert_eq!(by_title["title"], "Inline variable");
    let by_zero = super::actions::select_code_action(&actions, Some("0")).unwrap();
    assert_eq!(by_zero["title"], "Extract function");
    let by_one = super::actions::select_code_action(&actions, Some("1")).unwrap();
    assert_eq!(by_one["title"], "Inline variable");
    let unresolved = super::actions::select_code_action(&actions[2..], None).unwrap();
    assert_eq!(unresolved["title"], "Need resolve");
}

#[test]
fn action_caps_are_the_spec_values() {
    assert_eq!(super::actions::MAX_DIAGNOSTICS, 50);
    assert_eq!(super::actions::MAX_WORKSPACE_SYMBOLS, 200);
    assert_eq!(super::actions::MAX_REFERENCES, 50);
    assert_eq!(super::actions::MAX_RENAME_PAIRS, 1_000);
    assert_eq!(super::actions::MAX_GLOB_TARGETS, 20);
}

#[test]
fn redacts_single_location_workspace_symbol_and_nested_diagnostics() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    let context = ToolContext::new(root.path().to_path_buf(), false).unwrap();
    let inside = format!("file://{}/lib.rs", root.path().display());
    let (_, omitted) = edits::redact_structured_uris(
        &context,
        &serde_json::json!({"uri":"file:///etc/passwd","range":{"start":{"line":0}}}),
    );
    assert_eq!(omitted, 1);
    let (symbol, omitted) = edits::redact_structured_uris(
        &context,
        &serde_json::json!([{"name":"x","location":{"uri":"file:///etc/passwd"}}]),
    );
    assert_eq!(omitted, 1);
    assert!(!symbol.to_string().contains("/etc/passwd"), "{symbol}");
    let (diag, omitted) = edits::redact_structured_uris(
        &context,
        &serde_json::json!({"items":[{"uri":"file:///etc/passwd","items":[{"message":"boom"}]}]}),
    );
    assert_eq!(omitted, 1);
    assert!(!diag.to_string().contains("/etc/passwd"), "{diag}");
    let (kept, omitted) = edits::redact_structured_uris(
        &context,
        &serde_json::json!({"uri": inside, "range":{"start":{"line":2}}}),
    );
    assert_eq!(omitted, 0);
    assert!(kept.pointer("/uri").is_some());
}

#[test]
fn diagnostics_render_messages_for_push_pull_and_workspace_shapes() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    let context = ToolContext::new(root.path().to_path_buf(), false).unwrap();
    let uri = super::uri::path_to_file_uri(&root.path().join("lib.rs"));
    let diagnostic = serde_json::json!({
        "range": {"start": {"line": 2, "character": 1}},
        "severity": 1,
        "source": "rustc",
        "code": "E0001",
        "message": "useful diagnostic"
    });

    let push = super::actions::format_result(
        &context,
        LspAction::Diagnostics,
        serde_json::json!({"uri": uri, "diagnostics": [diagnostic.clone()]}),
    );
    assert!(push.contains("lib.rs:3"), "{push}");
    assert!(push.contains("useful diagnostic"), "{push}");
    assert!(push.contains("rustc") && push.contains("E0001"), "{push}");

    let pull = super::actions::format_result(
        &context,
        LspAction::Diagnostics,
        serde_json::json!({"kind": "full", "items": [diagnostic.clone()]}),
    );
    assert!(pull.contains("line 3"), "{pull}");
    assert!(pull.contains("useful diagnostic"), "{pull}");

    let workspace = super::actions::format_result(
        &context,
        LspAction::Diagnostics,
        serde_json::json!({"items": [{"uri": super::uri::path_to_file_uri(&root.path().join("lib.rs")), "items": [diagnostic]}]}),
    );
    assert!(workspace.contains("lib.rs:3"), "{workspace}");
    assert!(workspace.contains("useful diagnostic"), "{workspace}");
}

#[test]
fn document_symbol_cap_is_global_and_output_is_qualified() {
    let root = tempfile::tempdir().unwrap();
    let context = ToolContext::new(root.path().to_path_buf(), false).unwrap();
    let children = (0..=super::actions::MAX_WORKSPACE_SYMBOLS)
        .map(|index| {
            serde_json::json!({
                "name": format!("method_{index}"),
                "kind": 6,
                "selectionRange": {"start": {"line": index, "character": 0}},
                "range": {"start": {"line": index, "character": 0}}
            })
        })
        .collect::<Vec<_>>();
    let output = super::actions::format_result(
        &context,
        LspAction::Symbols,
        serde_json::json!([{
            "name": "impl Demo",
            "kind": 5,
            "selectionRange": {"start": {"line": 0, "character": 0}},
            "range": {"start": {"line": 0, "character": 0}},
            "children": children
        }]),
    );
    assert!(output.contains("Demo::method_0 (method):1"), "{output}");
    assert!(output.starts_with("Found 200 result(s):"), "{output}");
    assert!(!output.contains("method_199"), "{output}");
}

#[cfg(unix)]
#[tokio::test]
async fn linter_fallback_obeys_its_total_deadline() {
    use std::os::unix::fs::PermissionsExt;

    let workspace = tempfile::tempdir().unwrap();
    let host = tempfile::tempdir().unwrap();
    let linter_path = host.path().join("hung-biome");
    std::fs::write(&linter_path, "#!/bin/sh\nsleep 5\n").unwrap();
    let mut permissions = std::fs::metadata(&linter_path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&linter_path, permissions).unwrap();
    let source = workspace.path().join("source.js");
    std::fs::write(&source, "const x = 1;\n").unwrap();
    std::fs::write(workspace.path().join("package.json"), "{}").unwrap();
    let server = CatalogServer {
        name: "biome".into(),
        command: linter_path.to_string_lossy().into_owned(),
        args: Vec::new(),
        file_types: vec![".js".into()],
        root_markers: vec!["package.json".into()],
        is_linter: true,
        priority: 1,
        language_id: None,
        init_options: None,
        settings: None,
        capabilities: None,
        workspace_ready_timings: None,
        warmup_timeout_ms: None,
    };
    let config = sample_config(
        workspace.path().to_path_buf(),
        FileToolMode::ReadOnly,
        "s-linter-deadline",
        vec![server.clone()],
    );
    let started = std::time::Instant::now();
    let result = super::actions::run_linter_diagnostics(
        &config,
        &[server.clone()],
        &source,
        std::time::Duration::from_millis(200),
    )
    .await;
    assert!(result.is_none());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "hung linter exceeded its action deadline: {:?}",
        started.elapsed()
    );

    let cancellation = tokio_util::sync::CancellationToken::new();
    let trigger = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        trigger.cancel();
    });
    let started = std::time::Instant::now();
    let result = crate::tool_call_lifecycle::runtime::scope_request_tool_execution(
        None,
        cancellation,
        super::actions::run_linter_diagnostics(
            &config,
            &[server],
            &source,
            std::time::Duration::from_secs(5),
        ),
    )
    .await;
    assert!(result.is_none());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "cancelled linter did not stop promptly: {:?}",
        started.elapsed()
    );
}

#[test]
fn lsp_truncation_keeps_the_header() {
    let text = format!("Found 999 result(s):\n{}", "x\n".repeat(100_000));
    let output = super::actions::truncate_model_output(text);
    assert!(output.starts_with("Found 999 result(s):"), "{output}");
}

#[test]
fn sandbox_change_flips_digest() {
    let root = tempfile::tempdir().unwrap();
    let base = CommandConstraints {
        allowed_argv_prefixes: Vec::new(),
        forbidden_argv_prefixes: Vec::new(),
        network_mode: CommandNetworkMode::Disabled,
        execution_mode: CommandExecutionMode::ReadOnly,
        sandbox: CommandExecutionMode::Unrestricted,
        deny_all_argv: false,
        deny_git_metadata_writes: false,
    };
    let seated = CommandConstraints {
        sandbox: CommandExecutionMode::WorkspaceWrite,
        ..base.clone()
    };
    assert_ne!(
        config_digest(root.path(), &[], &base),
        config_digest(root.path(), &[], &seated)
    );
}

#[test]
fn readonly_request_rejects_unknown_payload_fields_before_start() {
    let root = tempfile::tempdir().unwrap();
    let context = ToolContext::new(root.path().to_path_buf(), false).unwrap();
    let err = super::actions::validate_raw_request(
        &context,
        "textDocument/hover",
        Some(r#"{"textDocument":{"uri":"file:///tmp/x"},"extra":true}"#),
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown field"), "{err}");
}

#[test]
fn readonly_request_validates_bare_and_non_file_uris() {
    let root = tempfile::tempdir().unwrap();
    let context = ToolContext::new(root.path().to_path_buf(), false).unwrap();
    for uri in [
        "/etc/passwd",
        "https://example.com/not-a-file",
        "untitled:outside",
    ] {
        let payload = serde_json::json!({"textDocument": {"uri": uri}}).to_string();
        let err = super::actions::validate_raw_request(
            &context,
            "textDocument/documentSymbol",
            Some(&payload),
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("outside")
                || text.contains("allowed")
                || text.contains("unsupported URI"),
            "{uri}: {text}"
        );
    }
}

#[tokio::test]
async fn cancelled_start_does_not_leave_starting_entry() {
    if !python3_available() {
        return;
    }
    let fixture = r#"
import json, sys, time
time.sleep(5)
def read():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b'\r\n', b'\n'):
            break
        key, _, value = line.decode().partition(':')
        headers[key.strip().lower()] = value.strip()
    n = int(headers.get('content-length', '0'))
    return json.loads(sys.stdin.buffer.read(n) or b'null')
def write(obj):
    body = json.dumps(obj).encode()
    sys.stdout.buffer.write(f'Content-Length: {len(body)}\r\n\r\n'.encode())
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
while True:
    msg = read()
    if msg is None:
        break
    if msg.get('method') == 'initialize':
        write({"jsonrpc":"2.0","id":msg.get("id"),"result":{"capabilities":{}}})
    elif msg.get('id') is not None:
        write({"jsonrpc":"2.0","id":msg.get("id"),"result":None})
"#;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let server = fixture_server(fixture.into());
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-start-cancel",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-start-cancel".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: "fixture".into(),
        config_digest: config.digest.clone(),
    };
    let start = pool.get_or_start(key.clone(), &server, &config);
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_millis(80)) => {}
        _ = start => {}
    }
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    assert_eq!(pool.live_count().await, 0);
}

#[tokio::test]
async fn dropped_hanging_request_sends_cancel_without_killing_server() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let cancel_file = root.path().join("cancelled");
    let fixture = format!(
        r#"
import json, sys
cancel_path = {cancel:?}
def read():
    headers = {{}}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b'\r\n', b'\n'):
            break
        key, _, value = line.decode().partition(':')
        headers[key.strip().lower()] = value.strip()
    n = int(headers.get('content-length', '0'))
    return json.loads(sys.stdin.buffer.read(n) or b'null')
def write(obj):
    body = json.dumps(obj).encode()
    sys.stdout.buffer.write(f'Content-Length: {{len(body)}}\r\n\r\n'.encode())
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
while True:
    msg = read()
    if msg is None:
        break
    method = msg.get('method')
    mid = msg.get('id')
    if method == 'initialize':
        write({{"jsonrpc":"2.0","id":mid,"result":{{"capabilities":{{}}}}}})
    elif method == '$/cancelRequest':
        open(cancel_path, 'w').write('cancelled')
    elif method == 'textDocument/hover':
        continue
    elif method == 'shutdown':
        write({{"jsonrpc":"2.0","id":mid,"result":None}})
    elif method == 'exit':
        break
    elif mid is not None:
        write({{"jsonrpc":"2.0","id":mid,"result":None}})
"#,
        cancel = cancel_file
    );
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let server = fixture_server(fixture);
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-drop-cancel",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-drop-cancel".into(),
        behavior_id: "b1".into(),
        workspace_root: root.path().to_path_buf(),
        server_name: "fixture".into(),
        config_digest: config.digest.clone(),
    };
    let lease = pool
        .get_or_start(key.clone(), &server, &config)
        .await
        .expect("start");
    let request = lease.client().request(
        "textDocument/hover",
        serde_json::json!({"textDocument":{"uri":"file://x"}}),
    );
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_millis(80)) => {}
        _ = request => {}
    }
    let seen = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if cancel_file.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(seen.is_ok(), "expected $/cancelRequest");
    assert!(pool.has_ready(&key).await);
}

#[test]
fn effective_disabled_network_is_not_replaced_by_lsp_default() {
    let mut bash = crate::tool_surface::ToolPolicyBash::off();
    bash.network_mode = CommandNetworkMode::Disabled;
    let omitted = constraints_from_effective_bash(&bash, None);
    assert_eq!(omitted.network_mode, CommandNetworkMode::Disabled);
    let explicit_enabled =
        constraints_from_effective_bash(&bash, Some(CommandNetworkMode::Enabled));
    assert_eq!(explicit_enabled.network_mode, CommandNetworkMode::Disabled);
    let explicit_inherit =
        constraints_from_effective_bash(&bash, Some(CommandNetworkMode::Inherit));
    assert_eq!(explicit_inherit.network_mode, CommandNetworkMode::Disabled);
}

#[test]
fn unrestricted_lsp_defaults_to_inherited_network() {
    let mut bash = crate::tool_surface::ToolPolicyBash::off();
    bash.execution_mode = CommandExecutionMode::Unrestricted;
    bash.network_mode = CommandNetworkMode::Inherit;
    let constraints = constraints_from_effective_bash(&bash, None);
    assert_eq!(constraints.sandbox, CommandExecutionMode::Unrestricted);
    assert_eq!(constraints.network_mode, CommandNetworkMode::Inherit);
}

#[test]
fn explicit_disabled_under_unrestricted_sandbox_is_unenforceable() {
    let root = tempfile::tempdir().unwrap();
    let constraints = CommandConstraints {
        allowed_argv_prefixes: Vec::new(),
        forbidden_argv_prefixes: Vec::new(),
        network_mode: CommandNetworkMode::Disabled,
        execution_mode: CommandExecutionMode::Unrestricted,
        sandbox: CommandExecutionMode::Unrestricted,
        deny_all_argv: false,
        deny_git_metadata_writes: false,
    };
    let command = if crate::toolset::admit_host_executable("true", root.path()).is_ok() {
        "true"
    } else {
        "/bin/true"
    };
    let err = crate::toolset::prepare_managed_command(root.path(), command, &[], &constraints)
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("disabled")
            || err.to_string().contains("unenforceable")
            || err.to_string().contains("network"),
        "{err}"
    );
}

#[test]
fn missing_host_executable_is_not_detected() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let missing = CatalogServer {
        name: "ghost-ls".into(),
        command: "definitely-not-an-lsp-binary-xyz".into(),
        args: vec![],
        file_types: vec![".rs".into()],
        root_markers: vec!["Cargo.toml".into()],
        is_linter: false,
        priority: 1,
        language_id: None,
        init_options: None,
        settings: None,
        capabilities: None,
        workspace_ready_timings: None,
        warmup_timeout_ms: None,
    };
    let message = catalog::unavailable_servers_message(root.path(), &[missing.clone()], None);
    assert!(message.contains("ghost-ls"), "{message}");
    assert!(message.contains("executable"), "{message}");
    let detected = catalog::detect_admitted_servers(root.path(), &[missing]);
    assert!(detected.is_empty());
}

#[tokio::test]
async fn reload_retires_current_snapshot_clients() {
    if !python3_available() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[package]\nname=\"t\"\nversion=\"0.0.1\"\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let server = fixture_server(FIXTURE_PY.into());
    let config = sample_config(
        root.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-reload",
        vec![server.clone()],
    );
    let tool = LspTool::new(config, pool.clone()).unwrap();
    let _ = tool
        .call(LspArgs {
            action: "hover".into(),
            file: Some("lib.rs".into()),
            line: Some(1),
            symbol: Some("x".into()),
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: None,
        })
        .await
        .expect("hover starts client");
    assert_eq!(pool.live_count().await, 1);
    let out = tool
        .call(LspArgs {
            action: "reload".into(),
            file: None,
            line: None,
            symbol: None,
            query: None,
            new_name: None,
            apply: None,
            payload: None,
            timeout: None,
        })
        .await
        .expect("reload");
    assert!(out.contains("retired"), "{out}");
    assert_eq!(pool.live_count().await, 0);
}

#[test]
fn readonly_bash_off_uses_platform_sandbox() {
    let constraints =
        constraints_from_effective_bash(&crate::tool_surface::ToolPolicyBash::off(), None);
    assert_eq!(
        constraints.sandbox,
        crate::toolset::lsp_sandbox_for_effective(CommandExecutionMode::ReadOnly)
    );
    assert_eq!(
        constraints.network_mode,
        crate::toolset::default_lsp_network_mode()
    );
}

#[tokio::test]
async fn overlay_read_write_meets_unrestricted_lsp_sandbox_to_workspace_write() {
    let constraints = CommandConstraints {
        allowed_argv_prefixes: Vec::new(),
        forbidden_argv_prefixes: Vec::new(),
        network_mode: CommandNetworkMode::Inherit,
        execution_mode: CommandExecutionMode::Unrestricted,
        sandbox: CommandExecutionMode::Unrestricted,
        deny_all_argv: false,
        deny_git_metadata_writes: false,
    };
    let met =
        crate::tool_call_lifecycle::runtime::scope_request_tool_execution_with_workspace_overlay(
            None,
            tokio_util::sync::CancellationToken::new(),
            crate::tool_call_lifecycle::runtime::ToolWorkspaceScope {
                workspace_cwd: None,
                workspace_root: Some(std::env::temp_dir()),
                workspace_authority: Some(crate::toolset::WorkspaceAuthority::ReadWrite),
            },
            None,
            None,
            None,
            Default::default(),
            false,
            async { super::overlay_lsp_constraints(&constraints) },
        )
        .await;
    assert_eq!(met.execution_mode, CommandExecutionMode::WorkspaceWrite);
    assert_eq!(met.sandbox, CommandExecutionMode::WorkspaceWrite);
}

#[tokio::test]
async fn start_client_initializes_with_pool_key_workspace() {
    if !python3_available() {
        return;
    }
    let baked = tempfile::tempdir().unwrap();
    let overlay = tempfile::tempdir().unwrap();
    std::fs::write(overlay.path().join("lib.rs"), "fn x() {}\n").unwrap();
    std::fs::write(
        overlay.path().join("Cargo.toml"),
        "[package]\nname='t'\nversion='0.1.0'\n",
    )
    .unwrap();
    let pool = LspPool::new();
    let server = fixture_server(FIXTURE_PY.into());
    let config = sample_config(
        baked.path().to_path_buf(),
        FileToolMode::ReadWrite,
        "s-overlay-root",
        vec![server.clone()],
    );
    let key = PoolKey {
        session_id: "s-overlay-root".into(),
        behavior_id: "b1".into(),
        workspace_root: overlay.path().to_path_buf(),
        server_name: server.name.clone(),
        config_digest: config.digest.clone(),
    };
    let lease = pool.get_or_start(key, &server, &config).await.unwrap();
    assert_eq!(lease.client().workspace(), overlay.path());
    assert_ne!(lease.client().workspace(), baked.path());
}
