use std::collections::BTreeMap;
use std::path::Path;

enum Home {
    Module(&'static str),
    WorkspaceTest(&'static str),
    Boundary(&'static str),
    Gap(&'static str),
}

fn model_homes() -> BTreeMap<&'static str, Home> {
    use Home::*;
    BTreeMap::from([
        ("ApplyReconcile", Module("conformance/apply_reconcile.rs")),
        ("Background", Module("conformance/background.rs")),
        ("BackendHealth", Module("conformance/backend_health.rs")),
        ("Client", Module("conformance/client_runtime.rs")),
        (
            "ClientShell",
            Boundary("projection theorems; desktop rendering is runtime-observed"),
        ),
        ("CodexShim", Module("conformance/codex_shim.rs")),
        ("CommandPolicy", Module("conformance/command_policy.rs")),
        ("Compaction", Module("conformance/streaming_compaction.rs")),
        ("CompletionRetry", Module("conformance/completion_retry.rs")),
        (
            "CancelPropagation",
            Module("conformance/cancel_propagation.rs"),
        ),
        (
            "CrossMachineComposed",
            Module("conformance/composed_invariants.rs"),
        ),
        ("DurableLineage", Module("conformance/background.rs")),
        ("DescendantGraph", Module("misc/descendant_graph.rs")),
        ("EditMatch", Module("conformance/edit_match.rs")),
        ("EventDelivery", Module("conformance/event_delivery.rs")),
        ("Fleet", Module("conformance/fleet.rs")),
        ("Goals", Module("conformance/goals.rs")),
        ("Identity", Module("conformance/identity.rs")),
        ("InferenceCall", Module("conformance/inference_call.rs")),
        ("ManagedExec", Module("conformance/managed_exec.rs")),
        ("MCPHealth", Module("conformance/mcp_health.rs")),
        (
            "Migration",
            WorkspaceTest("crates/gents-migration/tests/phase_b_steps.rs"),
        ),
        (
            "PairingReconcile",
            Module("conformance/pairing_reconcile.rs"),
        ),
        (
            "PeerRegistryDiscovery",
            Module("conformance/peer_registry_discovery.rs"),
        ),
        (
            "Persistence",
            Boundary("fail-open/closed policies are an accepted boundary (Boundaries.lean)"),
        ),
        (
            "P2PBackpressure",
            Boundary(
                "obligation model + operator surface for #630; not a flood-safety fence — queue-admission, retained JoinHandles, and durable pending-DAG recovery require defradb.rs work (boundary.p2p-backpressure.obligation-model)",
            ),
        ),
        ("Process", Module("conformance/process.rs")),
        ("PromptAssembly", Module("conformance/prompt_assembly.rs")),
        ("Recovery", Module("conformance/recovery_sweeps.rs")),
        (
            "RenderedCapture",
            Module("conformance/rendered_capture.rs"),
        ),
        ("Request", Module("conformance/request_lifecycle.rs")),
        ("RuntimeReconcile", Module("conformance/client_runtime.rs")),
        ("Scheduling", Module("conformance/scheduling.rs")),
        ("ScopeTemplates", Module("conformance/scope_templates.rs")),
        ("SelfConfig", Module("conformance/self_config.rs")),
        ("SessionRecovery", Module("conformance/session_recovery.rs")),
        (
            "Skills",
            Gap("#460 — implementation slices unshipped; fence lands with them"),
        ),
        (
            "StorageObservation",
            Boundary("daemon-visible classification is an accepted boundary (Boundaries.lean)"),
        ),
        (
            "StreamingResponse",
            Module("conformance/streaming_compaction.rs"),
        ),
        ("ToolExecution", Module("conformance/tool_execution.rs")),
        ("ToolPolicy", Module("conformance/tool_policy.rs")),
        ("Lsp", Module("conformance/lsp.rs")),
        ("Transcript", Module("conformance/transcript.rs")),
        ("Triggers", Module("conformance/triggers.rs")),
        (
            "ReversePairingHandlers",
            Module("conformance/pairing_reconcile.rs"),
        ),
    ])
}

fn proofs_models(root: &Path) -> Vec<String> {
    let proofs = root.join("crates/gents/proofs/Proofs");
    let mut models = Vec::new();
    for entry in std::fs::read_dir(&proofs).expect("read Proofs/").flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "lean") {
            let name = path.file_stem().unwrap().to_string_lossy().to_string();
            if matches!(name.as_str(), "Basic" | "Conformance") {
                continue;
            }
            models.push(name);
        }
    }
    models.sort();
    models
}

#[test]
fn every_lean_model_has_a_declared_conformance_home() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root")
        .to_path_buf();

    let homes = model_homes();
    let models = proofs_models(&root);

    let mut undeclared = Vec::new();
    let mut dangling: Vec<&str> = homes.keys().copied().collect();
    let mut gaps = Vec::new();

    for model in &models {
        match homes.get(model.as_str()) {
            None => undeclared.push(model.clone()),
            Some(home) => {
                dangling.retain(|name| name != model);
                match home {
                    Home::Module(path) => {
                        assert!(
                            root.join("crates/gents/tests").join(path).exists(),
                            "{model}: declared conformance module {path} does not exist"
                        );
                    }
                    Home::WorkspaceTest(path) => {
                        assert!(
                            root.join(path).exists(),
                            "{model}: declared workspace conformance test {path} does not exist"
                        );
                    }
                    Home::Boundary(rationale) => {
                        eprintln!("  BOUNDARY {model}: {rationale}");
                    }
                    Home::Gap(issue) => gaps.push(format!("{model}: {issue}")),
                }
            }
        }
    }

    if !gaps.is_empty() {
        eprintln!("declared conformance gaps ({}):", gaps.len());
        for gap in &gaps {
            eprintln!("  GAP {gap}");
        }
    }

    assert!(
        undeclared.is_empty(),
        "Lean models with NO declared conformance home (fence them, declare a \
         boundary, or declare a tracked gap in conformance/structure.rs):\n{}",
        undeclared.join("\n")
    );
    assert!(
        dangling.is_empty(),
        "conformance homes declared for Lean models that no longer exist \
         (remove from conformance/structure.rs):\n{}",
        dangling.join("\n")
    );
}
