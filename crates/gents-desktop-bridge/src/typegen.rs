//! TypeScript bridge-contract generation and bindings freshness gate.
//!
//! Decision: **ts-rs** (not typeshare).
//! Evidence:
//! - `serde-compat` honors `rename_all = "camelCase"` and tagged enums
//!   (`RenderedTimelineItem` with `tag = "kind"`) without extra attributes.
//! - Nested structs (`ToolServiceTestResult` → `ToolServiceToolView`) export
//!   transitively.
//! - `#[ts(type = "string")]` covers `&'static str` fields (`ClientUpdateEvent`).
//! - typeshare would require a separate CLI + attribute surface and has weaker
//!   serde-attribute coverage for our existing view models.
//!
//! Every production bridge-visible request, response, and event payload is
//! exported. Generated files land in the committed
//! `@source-inc/gents-desktop-client/src/generated/` directory.

use std::path::{Path, PathBuf};

use ts_rs::TS;

use crate::contract::BridgeContract;
use crate::error::{BridgeError, BridgeErrorCode};
use crate::tauri_commands::chat::RequestResendResultView;
use crate::tauri_commands::inference_setup::{
    CodexLoginRequest, CodexLoginResult, CodexLoginUrl, GrokLoginRequest, GrokLoginResult,
    GrokLoginUrl, InferenceProbeRequest, InferenceProbeResult, ProviderAccountDisconnectRequest,
    ProviderAccountView, ProviderAccountsRequest,
};
use crate::tauri_commands::lifecycle::DesktopObserverMetrics;
use crate::tauri_commands::workspace::WorkspaceListingView;
use crate::types::*;

fn bindings_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/gents-desktop-client/src/generated")
}

fn replace_typescript_identifier(source: &str, from: &str, to: &str) -> String {
    fn is_identifier_char(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
    }

    let bytes = source.as_bytes();
    let mut output = String::with_capacity(source.len());
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find(from) {
        let start = cursor + offset;
        let end = start + from.len();
        let left_is_identifier = start > 0 && is_identifier_char(bytes[start - 1]);
        let right_is_identifier = end < bytes.len() && is_identifier_char(bytes[end]);
        output.push_str(&source[cursor..start]);
        if left_is_identifier || right_is_identifier {
            output.push_str(from);
        } else {
            output.push_str(to);
        }
        cursor = end;
    }
    output.push_str(&source[cursor..]);
    output
}

fn normalize_generated_types(dir: &Path) -> Result<(), String> {
    for entry in std::fs::read_dir(dir).map_err(|e| e.to_string())? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("ts") {
            continue;
        }
        let source = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let normalized = replace_typescript_identifier(&source, "bigint", "number")
            .lines()
            .map(|line| {
                let trimmed = line.trim_end();
                if trimmed.starts_with("import type ") && trimmed.ends_with("\";") {
                    format!("{}.js\";", trimmed.trim_end_matches("\";"))
                } else {
                    trimmed.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        std::fs::write(path, normalized).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn bridge_visible_derived_types(source: &str, derive_marker: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut derive = String::new();
    let mut collecting_derive = false;
    let mut pending_match = false;

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#[derive(") {
            derive.clear();
            collecting_derive = true;
        }
        if collecting_derive {
            derive.push_str(trimmed);
            if trimmed.ends_with(")]") {
                collecting_derive = false;
                pending_match = derive
                    .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                    .any(|token| token == derive_marker);
            }
            continue;
        }
        if !pending_match {
            continue;
        }
        if trimmed.is_empty()
            || trimmed.starts_with("///")
            || trimmed.starts_with("#[")
            || trimmed.starts_with("//")
        {
            continue;
        }

        let declaration = trimmed
            .strip_prefix("pub struct ")
            .or_else(|| trimmed.strip_prefix("pub enum "))
            .or_else(|| trimmed.strip_prefix("pub(crate) struct "))
            .or_else(|| trimmed.strip_prefix("pub(crate) enum "));
        if let Some(declaration) = declaration {
            let name = declaration
                .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                .next()
                .unwrap_or_default();
            if !name.is_empty() {
                names.push(name.to_string());
            }
        }
        pending_match = false;
    }

    names
}

fn contract_source_paths() -> Vec<PathBuf> {
    fn collect_rust_files(directory: &Path, output: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(directory).expect("read contract source directory") {
            let path = entry.expect("contract source entry").path();
            if path.is_dir() {
                collect_rust_files(&path, output);
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
                output.push(path);
            }
        }
    }

    let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut paths = vec![
        source_root.join("contract.rs"),
        source_root.join("error.rs"),
        source_root.join("tauri_commands/chat.rs"),
        source_root.join("tauri_commands/inference_setup.rs"),
        source_root.join("tauri_commands/lifecycle.rs"),
        source_root.join("tauri_commands/workspace.rs"),
    ];
    collect_rust_files(&source_root.join("types"), &mut paths);
    paths.sort();
    paths
}

fn export_all(dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;

    macro_rules! export_types {
        ($($type:ty),+ $(,)?) => {
            $(
                <$type>::export_all_to(dir).map_err(|e| e.to_string())?;
            )+
        };
    }

    export_types!(
        DesktopInitRequest,
        ManagedServerStartRequest,
        PeerAddRequest,
        PeerStatusFetchRequest,
        PeerProbeRequest,
        BearerPairingRequest,
        ChatSendRequest,
        ConversationRenameRequest,
        AgentConfigSaveRequest,
        BehaviorSaveRequest,
        SkillDeleteRequest,
        TaskDeleteRequest,
        ScheduleDeleteRequest,
        EventTriggerDeleteRequest,
        BackendDeleteRequest,
        InferenceProfileDeleteRequest,
        ToolSelectionDeleteRequest,
        ToolServiceDeleteRequest,
        BehaviorDeleteRequest,
        BackendSaveRequest,
        InferenceProfileSaveRequest,
        ToolSelectionSaveRequest,
        ToolServiceSaveRequest,
        ToolServiceTestRequest,
        TaskSaveRequest,
        SkillSaveRequest,
        TaskRunRequest,
        ScheduleSaveRequest,
        ScheduleRunRequest,
        EventTriggerSaveRequest,
        DesktopOperationsSnapshotRequest,
        DesktopListSubagentTreeRequest,
        DesktopPreviewInterruptCascadeRequest,
        DesktopListHoldsRequest,
        DesktopResolveHoldRequest,
        DesktopInterruptRequest,
        DesktopProbeMcpServiceRequest,
        InferenceProbeRequest,
        CodexLoginRequest,
        GrokLoginRequest,
        ProviderAccountsRequest,
        ProviderAccountDisconnectRequest,
    );

    export_types!(
        BridgeErrorCode,
        BridgeError,
        DesktopClientSnapshot,
        ManagedServerState,
        ManagedServerStatus,
        PeerRemoveResponse,
        BearerPairingResponse,
        NetworkStatusView,
        ToolServiceTestResult,
        TaskRunResult,
        ChatSendResult,
        ClientUpdateEvent,
        DesktopOperationsSnapshot,
        CascadeCancelPreview,
        InterruptRequestResult,
        HeldToolCallView,
        ResolveHoldResult,
        BackendHealthView,
        MCPServiceHealthView,
        McpServiceProbeResult,
        DerivedCancelCauseView,
        DesktopSessionSnapshot,
        MessageView,
        ToolCallView,
        ToolResultView,
        BridgeContract,
        RequestResendResultView,
        WorkspaceListingView,
        DesktopObserverMetrics,
        InferenceProbeResult,
        CodexLoginResult,
        CodexLoginUrl,
        GrokLoginResult,
        GrokLoginUrl,
        ProviderAccountView,
    );

    normalize_generated_types(dir)?;
    Ok(())
}

fn list_ts_files(dir: &Path) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    if !dir.exists() {
        return Ok(names);
    }
    for entry in std::fs::read_dir(dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("ts") {
            names.push(path.file_name().unwrap().to_string_lossy().into_owned());
        }
    }
    names.sort();
    Ok(names)
}

#[test]
fn ts_rs_exports_tagged_enum_and_camel_case_structs() {
    let tmp = tempfile::tempdir().expect("tempdir");
    export_all(tmp.path()).expect("export");

    let timeline = std::fs::read_to_string(tmp.path().join("RenderedTimelineItem.ts"))
        .expect("RenderedTimelineItem.ts");
    assert!(
        timeline.contains("kind") && timeline.contains("userMessage"),
        "tagged enum should surface the serde tag + camelCase variant; got:\n{timeline}"
    );
    assert!(
        timeline.contains("itemKey"),
        "per-variant rename_all should camelCase enum fields; got:\n{timeline}"
    );

    let bootstrap = std::fs::read_to_string(tmp.path().join("DesktopBootstrapSummary.ts"))
        .expect("DesktopBootstrapSummary.ts");
    assert!(
        bootstrap.contains("defaultAgentHome"),
        "serde rename_all camelCase should be reflected; got:\n{bootstrap}"
    );
    let error = std::fs::read_to_string(tmp.path().join("BridgeError.ts")).expect("BridgeError.ts");
    assert!(
        error.contains("retryable"),
        "BridgeError fields; got:\n{error}"
    );

    assert!(
        !timeline.contains("bigint"),
        "serde IPC integers must be emitted as JavaScript numbers; got:\n{timeline}"
    );
}

#[test]
fn all_bridge_visible_contract_roots_are_generated() {
    let tmp = tempfile::tempdir().expect("tempdir");
    export_all(tmp.path()).expect("export");

    let files = list_ts_files(tmp.path())
        .expect("list generated")
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut wire_types = std::collections::BTreeSet::new();
    let mut typescript_types = std::collections::BTreeSet::new();
    for path in contract_source_paths() {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        wire_types.extend(bridge_visible_derived_types(&source, "Serialize"));
        wire_types.extend(bridge_visible_derived_types(&source, "Deserialize"));
        typescript_types.extend(bridge_visible_derived_types(&source, "TS"));
    }
    assert!(
        wire_types.is_subset(&typescript_types),
        "bridge-visible serialized/deserialized types without TS derive: {:?}",
        wire_types.difference(&typescript_types).collect::<Vec<_>>()
    );
    for expected in typescript_types.iter().map(|name| format!("{name}.ts")) {
        assert!(
            files.contains(&expected),
            "missing generated bridge contract {expected}"
        );
    }

    for inference_wire_type in [
        "InferenceProbeRequest.ts",
        "InferenceProbeResult.ts",
        "CodexLoginRequest.ts",
        "CodexLoginResult.ts",
        "CodexLoginUrl.ts",
        "GrokLoginRequest.ts",
        "GrokLoginResult.ts",
        "GrokLoginUrl.ts",
    ] {
        assert!(
            files.contains(inference_wire_type),
            "inference onboarding wire type {inference_wire_type} is not generated"
        );
    }
}

#[test]
fn bigint_normalization_only_rewrites_typescript_identifier_tokens() {
    let source = "type Wire = bigint | Array<bigint>; type bigintCounter = string;\n";
    assert_eq!(
        replace_typescript_identifier(source, "bigint", "number"),
        "type Wire = number | Array<number>; type bigintCounter = string;\n"
    );
}

#[test]
fn committed_bindings_match_regeneration() {
    let tmp = tempfile::tempdir().expect("tempdir");
    export_all(tmp.path()).expect("export");

    let expected_files = list_ts_files(tmp.path()).expect("list generated");
    let committed = bindings_dir();
    let actual_files = list_ts_files(&committed).unwrap_or_default();
    assert_eq!(
        actual_files, expected_files,
        "bindings file set drifted under {}. Regenerate with:\n  cargo test -p gents-desktop-bridge write_bindings -- --ignored",
        committed.display()
    );

    for name in &expected_files {
        let expected = std::fs::read_to_string(tmp.path().join(name)).expect("read generated");
        let actual = std::fs::read_to_string(committed.join(name)).unwrap_or_else(|_| {
            panic!("missing committed binding {name}; regenerate with write_bindings")
        });
        assert_eq!(
            actual,
            expected,
            "binding {name} drifted under {}. Regenerate with:\n  cargo test -p gents-desktop-bridge write_bindings -- --ignored",
            committed.display()
        );
    }
}

#[test]
#[ignore = "run explicitly to regenerate packages/gents-desktop-client/src/generated/"]
fn write_bindings() {
    let dir = bindings_dir();
    if dir.exists() {
        for entry in std::fs::read_dir(&dir).expect("read bindings") {
            let entry = entry.expect("entry");
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("ts") {
                let _ = std::fs::remove_file(path);
            }
        }
    }
    export_all(&dir).expect("export bindings");
    eprintln!("wrote bindings to {}", dir.display());
}
