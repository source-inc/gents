use std::collections::{BTreeMap, BTreeSet};

use gents::tool_surface::ToolPolicySurface;
use gents::tool_surface::{BashMode, EndpointScope, FileToolMode, ToolPolicyBash};
use gents::toolset::{CommandExecutionMode, CommandNetworkMode};

use crate::lean_vocab_test::{
    LeanToolPolicySurfaceView as View, LeanToolPolicyWriteGrant as WriteGrant,
};

fn file_from_rank(rank: u8) -> FileToolMode {
    match rank {
        0 => FileToolMode::Off,
        1 => FileToolMode::ReadOnly,
        2 => FileToolMode::ReadWrite,
        other => panic!("unknown file rank {other}"),
    }
}

fn file_rank(mode: FileToolMode) -> u8 {
    match mode {
        FileToolMode::Off => 0,
        FileToolMode::ReadOnly => 1,
        FileToolMode::ReadWrite => 2,
    }
}

fn bash_tool_from_exec_rank(rank: u8) -> BashMode {
    match rank {
        0 => BashMode::Off,
        1 => BashMode::ReadOnly,
        2 => BashMode::Unrestricted,
        other => panic!("unknown bash execution rank {other}"),
    }
}

fn exec_from_rank(rank: u8) -> CommandExecutionMode {
    match rank {
        0 => CommandExecutionMode::ReadOnly,
        1 => CommandExecutionMode::WorkspaceWrite,
        2 => CommandExecutionMode::Unrestricted,
        other => panic!("unknown command execution rank {other}"),
    }
}

fn exec_rank(mode: CommandExecutionMode) -> u8 {
    match mode {
        CommandExecutionMode::ReadOnly => 0,
        CommandExecutionMode::WorkspaceWrite => 1,
        CommandExecutionMode::Unrestricted => 2,
    }
}

fn net_from_rank(rank: u8) -> CommandNetworkMode {
    match rank {
        0 => CommandNetworkMode::Disabled,
        1 => CommandNetworkMode::Inherit,
        2 => CommandNetworkMode::Enabled,
        other => panic!("unknown command network rank {other}"),
    }
}

fn net_rank(mode: CommandNetworkMode) -> u8 {
    match mode {
        CommandNetworkMode::Disabled => 0,
        CommandNetworkMode::Inherit => 1,
        CommandNetworkMode::Enabled => 2,
    }
}

fn unit_scope_from_strings(kind: &str, keys: &[String]) -> EndpointScope<String, ()> {
    match kind {
        "none" => EndpointScope::None,
        "all" => EndpointScope::All,
        "only" => EndpointScope::<String, ()>::only_units(keys.iter().cloned()),
        other => panic!("unknown string scope kind {other:?}"),
    }
}

fn unit_scope_from_prefixes(kind: &str, keys: &[Vec<String>]) -> EndpointScope<Vec<String>, ()> {
    match kind {
        "none" => EndpointScope::None,
        "all" => EndpointScope::All,
        "only" => EndpointScope::<Vec<String>, ()>::only_units(keys.iter().cloned()),
        other => panic!("unknown prefix scope kind {other:?}"),
    }
}

fn pair_scope_from_keys(kind: &str, keys: &[String]) -> EndpointScope<(String, String), ()> {
    match kind {
        "none" => EndpointScope::None,
        "all" => EndpointScope::All,
        "only" => EndpointScope::<(String, String), ()>::only_units(keys.iter().map(|key| {
            let (did, behavior) = key.split_once("::").unwrap_or((key.as_str(), ""));
            (did.to_string(), behavior.to_string())
        })),
        other => panic!("unknown pair scope kind {other:?}"),
    }
}

fn encode_pair_keys(scope: &EndpointScope<(String, String), ()>) -> Vec<String> {
    scope
        .keys()
        .into_iter()
        .map(|(did, behavior)| format!("{did}::{behavior}"))
        .collect()
}

fn cli_scope_from_keys(kind: &str, keys: &[String]) -> EndpointScope<String, BTreeSet<String>> {
    match kind {
        "none" => EndpointScope::None,
        "all" => EndpointScope::All,
        "only" => EndpointScope::Only(
            keys.iter()
                .map(|key| (key.clone(), BTreeSet::new()))
                .collect::<BTreeMap<_, _>>(),
        ),
        other => panic!("unknown cli scope kind {other:?}"),
    }
}

fn write_scope_from_grants(
    kind: &str,
    grants: &[WriteGrant],
) -> EndpointScope<(String, String), BTreeSet<String>> {
    match kind {
        "none" => EndpointScope::None,
        "all" => EndpointScope::All,
        "only" => EndpointScope::Only(
            grants
                .iter()
                .map(|grant| {
                    (
                        (grant.tool.clone(), grant.collection.clone()),
                        grant.fields.iter().cloned().collect(),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        ),
        other => panic!("unknown write scope kind {other:?}"),
    }
}

fn grants_from_write_scope(
    scope: &EndpointScope<(String, String), BTreeSet<String>>,
) -> Vec<WriteGrant> {
    match scope {
        EndpointScope::Only(grants) => grants
            .iter()
            .map(|((tool, collection), fields)| WriteGrant {
                tool: tool.clone(),
                collection: collection.clone(),
                fields: fields.iter().cloned().collect(),
            })
            .collect(),
        EndpointScope::None | EndpointScope::All => Vec::new(),
    }
}

fn surface_from_view(view: &View) -> ToolPolicySurface {
    ToolPolicySurface {
        file: file_from_rank(view.file_rank),
        bash: ToolPolicyBash {
            tool: bash_tool_from_exec_rank(view.bash_mode),
            execution_mode: exec_from_rank(view.bash_mode),
            network_mode: net_from_rank(view.bash_net),
            sandbox: view.bash_sandbox,
            allowed_argv_prefixes: unit_scope_from_prefixes(
                &view.bash_allowed_kind,
                &view.bash_allowed_prefixes,
            ),
            forbidden_argv_prefixes: view.bash_forbidden.iter().cloned().collect(),
            read_only_allowlist: unit_scope_from_strings(
                &view.bash_read_only_kind,
                &view.bash_read_only_keys,
            ),
        },
        meta: view.meta,
        defra_query: view.defra_query,
        self_config: view.self_config,
        memory: view.memory,
        session_history: view.session_history,
        context_budget: view.context_budget,
        spawn: view.spawn,
        steering: view.steering,
        background: view.background,
        cross_deployment: view.cross_deployment,
        skills: view.skills,
        lsp: view.lsp,
        graph_dsl: false,
        cli_tools: cli_scope_from_keys(&view.cli_scope_kind, &view.cli_keys),
        mcp_services: unit_scope_from_strings(&view.mcp_scope_kind, &view.mcp_services),
        defra_collections: unit_scope_from_strings(
            &view.defra_collections_scope_kind,
            &view.defra_collections_keys,
        ),
        self_config_categories: unit_scope_from_strings(
            &view.self_config_categories_scope_kind,
            &view.self_config_categories_keys,
        ),
        subagent_targets: pair_scope_from_keys(
            &view.subagent_targets_scope_kind,
            &view.subagent_targets_keys,
        ),
        background_tools: unit_scope_from_strings(
            &view.background_tools_scope_kind,
            &view.background_tools_keys,
        ),
        write_tools: write_scope_from_grants(&view.write_scope_kind, &view.write_grants),
        query_tools: write_scope_from_grants(
            if view.query_scope_kind.is_empty() {
                "none"
            } else {
                &view.query_scope_kind
            },
            &view.query_grants,
        ),
    }
}

fn view_from_surface(
    surface: &ToolPolicySurface,
    mcp_probe: String,
    write_probe_tool: String,
    write_probe_collection: String,
) -> View {
    let write_probe = (write_probe_tool.clone(), write_probe_collection.clone());
    let write_fields = surface
        .write_tools
        .lookup(&write_probe)
        .map(|fields| fields.iter().cloned().collect())
        .unwrap_or_default();
    let query_probe = ("qt".to_string(), "coll".to_string());

    View {
        file_rank: file_rank(surface.file),
        meta: surface.meta,
        defra_query: surface.defra_query,
        self_config: surface.self_config,
        memory: surface.memory,
        session_history: surface.session_history,
        context_budget: surface.context_budget,
        spawn: surface.spawn,
        steering: surface.steering,
        background: surface.background,
        cross_deployment: surface.cross_deployment,
        skills: surface.skills,
        lsp: surface.lsp,
        bash_mode: exec_rank(surface.bash.execution_mode),
        bash_net: net_rank(surface.bash.network_mode),
        bash_sandbox: surface.bash.sandbox,
        bash_allowed_kind: surface.bash.allowed_argv_prefixes.kind().to_string(),
        bash_allowed_prefixes: surface.bash.allowed_argv_prefixes.keys(),
        bash_forbidden: surface
            .bash
            .forbidden_argv_prefixes
            .iter()
            .cloned()
            .collect(),
        bash_read_only_kind: surface.bash.read_only_allowlist.kind().to_string(),
        bash_read_only_keys: surface.bash.read_only_allowlist.keys(),
        cli_scope_kind: surface.cli_tools.kind().to_string(),
        cli_keys: surface.cli_tools.keys(),
        mcp_permits: surface.mcp_services.permits(&mcp_probe),
        mcp_probe,
        mcp_scope_kind: surface.mcp_services.kind().to_string(),
        mcp_services: surface.mcp_services.keys(),
        defra_collections_scope_kind: surface.defra_collections.kind().to_string(),
        defra_collections_keys: surface.defra_collections.keys(),
        self_config_categories_scope_kind: surface.self_config_categories.kind().to_string(),
        self_config_categories_keys: surface.self_config_categories.keys(),
        subagent_targets_scope_kind: surface.subagent_targets.kind().to_string(),
        subagent_targets_keys: encode_pair_keys(&surface.subagent_targets),
        background_tools_scope_kind: surface.background_tools.kind().to_string(),
        background_tools_keys: surface.background_tools.keys(),
        write_probe_tool,
        write_probe_collection,
        write_scope_kind: surface.write_tools.kind().to_string(),
        write_grants: grants_from_write_scope(&surface.write_tools),
        write_fields,
        query_probe_tool: query_probe.0.clone(),
        query_probe_collection: query_probe.1.clone(),
        query_scope_kind: surface.query_tools.kind().to_string(),
        query_grants: grants_from_write_scope(&surface.query_tools),
        query_fields: surface
            .query_tools
            .lookup(&query_probe)
            .map(|fields| fields.iter().cloned().collect())
            .unwrap_or_default(),
    }
}

pub(super) fn rederive(behavior: &View, ceiling: &View, runtime: &View) -> View {
    let behavior_policy = surface_from_view(behavior);
    let ceiling_policy = surface_from_view(ceiling);
    let runtime_policy = surface_from_view(runtime);
    let effective =
        ToolPolicySurface::effective(&behavior_policy, &ceiling_policy, &runtime_policy);
    view_from_surface(
        &effective,
        behavior.mcp_probe.clone(),
        behavior.write_probe_tool.clone(),
        behavior.write_probe_collection.clone(),
    )
}
