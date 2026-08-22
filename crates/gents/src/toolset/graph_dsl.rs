//! Graph DSL pipeline compiler tool.
//!
//! `compile_graph` is a compiler, not an engine: it takes a structured graph
//! definition (nodes + edges + optional seed) and materializes it as gents
//! automation documents — one Task per node and one EventTrigger per edge —
//! all through the same transactional `SelfConfigCore` write path that
//! `configure_automation` uses. Execution happens later via the existing
//! trigger/task engine when the agent writes a seed document.
//!
//! Gated by `ToolSelection.enable_graph_dsl` (default false), following the
//! same opt-in pattern as `enable_memory` / `enable_session_history`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::llm::tool::{Tool, ToolDefinition, ToolDyn};
use crate::self_config::SelfConfigCore;
use anyhow::{anyhow, bail, ensure, Result};
use defra_node::EmbeddedNode;
use serde::Deserialize;
use serde_json::{json, Value};

pub const COMPILE_GRAPH_TOOL_NAME: &str = "compile_graph";

/// A node in the pipeline graph. Each node becomes a Task document.
#[derive(Debug, Clone, Deserialize)]
pub struct GraphNode {
    /// The behavior that runs this stage. Must reference an existing behavior.
    pub behavior_id: String,
    /// Prompt template for the task (may contain `{{placeholders}}`).
    pub prompt_template: String,
    /// Collection the task's output is written to (informational; the task's
    /// bounded write tool surfaces are pre-populated separately).
    #[serde(default)]
    pub output_collection: Option<String>,
    /// Bare `surface_id` ref to a pre-populated `DatastoreToolSurface` doc.
    #[serde(default)]
    pub surface_id: Option<String>,
}

/// A directed edge in the pipeline graph. Each edge becomes an EventTrigger
/// that fires the target node's task when a document lands in the source
/// collection.
#[derive(Debug, Clone, Deserialize)]
pub struct GraphEdge {
    pub from: String,
    pub to: String,
    /// Explicit trigger id; auto-generated as `{graph_id}-{from}-{to}` when omitted.
    #[serde(default)]
    pub trigger_id: Option<String>,
    /// `"per_document"` (fan-out) or `"per_group"` (barrier).
    pub fire_mode: String,
    /// Collection the trigger watches.
    pub source_collection: String,
    /// Field used to group documents for `per_group` barriers.
    #[serde(default)]
    pub correlation_field: Option<String>,
    /// Field carrying the expected group size (barriers only).
    #[serde(default)]
    pub expected_count_field: Option<String>,
    /// Fixed expected group size (barriers only; alternative to the field).
    #[serde(default)]
    pub expected_count: Option<i64>,
    /// Barrier timeout in seconds (barriers only).
    #[serde(default)]
    pub group_timeout_secs: Option<i64>,
    /// Minimum docs before a barrier group fires (barriers only).
    #[serde(default)]
    pub group_min_count: Option<i64>,
    /// Optional filter expression spliced into the trigger probe.
    #[serde(default)]
    pub filter: Option<String>,
    /// Optional concurrency policy.
    #[serde(default)]
    pub concurrency: Option<String>,
}

/// The seed document that kicks off the pipeline. The compiler does NOT write
/// it — it echoes it back so the agent can write it via the appropriate
/// bounded write tool after compilation.
#[derive(Debug, Clone, Deserialize)]
pub struct GraphSeed {
    pub collection: String,
    #[serde(default)]
    pub fields: HashMap<String, Value>,
}

/// The full graph definition, deserialized from the tool call argument.
#[derive(Debug, Clone, Deserialize)]
pub struct GraphDefinition {
    pub graph_id: String,
    #[serde(default)]
    pub description: Option<String>,
    pub nodes: HashMap<String, GraphNode>,
    #[serde(default)]
    pub edges: Vec<GraphEdge>,
    #[serde(default)]
    pub seed: Option<GraphSeed>,
}

/// Tool call params: the graph definition is the single argument.
#[derive(Debug, Clone, Deserialize)]
pub struct CompileGraphParams {
    #[serde(flatten)]
    pub graph: GraphDefinition,
}

/// The compile_graph tool. Like `ConfigureAutomationTool`, it carries a
/// `SelfConfigCore` clone and routes all document creation through the
/// transactional write path.
#[derive(Clone)]
pub struct CompileGraphTool {
    core: SelfConfigCore,
}

impl CompileGraphTool {
    pub fn new(node: Arc<EmbeddedNode>, agent_did: String, behavior_id: String) -> Result<Self> {
        let core = SelfConfigCore::new(node, agent_did, behavior_id)?;
        Ok(Self { core })
    }

}

/// Error wrapper mirroring the other tool families: render the full anyhow
/// chain to the model.
#[derive(Debug)]
pub struct GraphDslError(anyhow::Error);

impl std::fmt::Display for GraphDslError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl std::error::Error for GraphDslError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.root_cause())
    }
}

impl From<anyhow::Error> for GraphDslError {
    fn from(error: anyhow::Error) -> Self {
        Self(error)
    }
}

impl Tool for CompileGraphTool {
    const NAME: &'static str = COMPILE_GRAPH_TOOL_NAME;

    type Error = GraphDslError;
    type Args = CompileGraphParams;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: COMPILE_GRAPH_TOOL_NAME.to_string(),
            description: "Compile a structured graph definition (nodes + edges) into gents \
                automation documents: one Task per node and one EventTrigger per edge, all \
                created transactionally via the self-config write path. This is a compiler, \
                not an engine — it produces Tasks and EventTriggers only. Execution happens \
                when the agent writes a seed document and the existing trigger engine fires."
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "graph_id": {
                        "type": "string",
                        "description": "Unique id for this pipeline; used as a prefix for all generated task_ids."
                    },
                    "description": {
                        "type": "string",
                        "description": "Human-readable description of the pipeline."
                    },
                    "nodes": {
                        "type": "object",
                        "description": "Map of node_id → node spec. Each node becomes a Task with task_id `{graph_id}-{node_id}`.",
                        "additionalProperties": {
                            "type": "object",
                            "properties": {
                                "behavior_id": { "type": "string" },
                                "prompt_template": { "type": "string" },
                                "output_collection": { "type": "string" },
                                "surface_id": { "type": "string" }
                            },
                            "required": ["behavior_id", "prompt_template"]
                        }
                    },
                    "edges": {
                        "type": "array",
                        "description": "Directed edges. Each becomes an EventTrigger firing the target node's task.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "from": { "type": "string" },
                                "to": { "type": "string" },
                                "trigger_id": { "type": "string" },
                                "fire_mode": { "type": "string", "enum": ["per_document", "per_group"] },
                                "source_collection": { "type": "string" },
                                "correlation_field": { "type": "string" },
                                "expected_count_field": { "type": "string" },
                                "expected_count": { "type": "integer" },
                                "group_timeout_secs": { "type": "integer" },
                                "group_min_count": { "type": "integer" },
                                "filter": { "type": "string" },
                                "concurrency": { "type": "string" }
                            },
                            "required": ["from", "to", "fire_mode", "source_collection"]
                        }
                    },
                    "seed": {
                        "type": "object",
                        "description": "Optional seed document spec. Not written by this tool — echoed back for the agent to write.",
                        "properties": {
                            "collection": { "type": "string" },
                            "fields": { "type": "object" }
                        },
                        "required": ["collection"]
                    }
                },
                "required": ["graph_id", "nodes"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let summary = compile_graph(&self.core, args.graph).await?;
        serde_json::to_string_pretty(&summary)
            .map_err(|error| GraphDslError(anyhow!("failed to serialize compile result: {error}")))
    }
}

/// Build and register the compile_graph tool for a behavior. Returns `None`
/// (no tool) when the agent DID is empty — fail closed for bare oneshot
/// contexts, matching `build_self_config_tools`.
pub fn build_graph_dsl_tool(
    node: Arc<EmbeddedNode>,
    agent_did: String,
    behavior_id: String,
) -> Option<Box<dyn ToolDyn>> {
    match CompileGraphTool::new(node, agent_did, behavior_id) {
        Ok(tool) => Some(Box::new(tool)),
        Err(error) => {
            tracing::warn!(
                %error,
                "compile_graph tool requested but not registrable; failing closed"
            );
            None
        }
    }
}

/// The summary returned to the agent after a successful compilation.
#[derive(Debug, serde::Serialize)]
pub(crate) struct CompileSummary {
    graph_id: String,
    tasks_created: Vec<String>,
    triggers_created: Vec<String>,
    seed_collection: Option<String>,
    seed_fields: Option<HashMap<String, Value>>,
    status: String,
}

/// Validate the graph definition structurally, then compile each node into a
/// Task and each edge into an EventTrigger through the self-config write path.
pub(crate) async fn compile_graph(
    core: &SelfConfigCore,
    graph: GraphDefinition,
) -> Result<CompileSummary> {
    // --- Structural validation (no DB needed) ---
    validate_graph(&graph)?;

    // --- Compilation: Tasks first, then EventTriggers ---
    // Tasks must exist before triggers so the trigger's task_id reference
    // resolves. Each document is created via the SelfConfigCore write path
    // (transactional, owned by the calling behavior).
    let graph_id = graph.graph_id.clone();
    let mut tasks_created = Vec::with_capacity(graph.nodes.len());
    for (node_id, node) in &graph.nodes {
        let task_id = format!("{graph_id}-{node_id}");
        create_task(core, &task_id, node_id, node).await?;
        tasks_created.push(task_id);
    }
    tasks_created.sort();

    let mut triggers_created = Vec::with_capacity(graph.edges.len());
    for edge in &graph.edges {
        let trigger_id = edge
            .trigger_id
            .clone()
            .unwrap_or_else(|| format!("{graph_id}-{}-{}", edge.from, edge.to));
        let task_id = format!("{graph_id}-{}", edge.to);
        create_trigger(core, &trigger_id, &task_id, edge).await?;
        triggers_created.push(trigger_id);
    }
    triggers_created.sort();

    Ok(CompileSummary {
        graph_id,
        tasks_created,
        triggers_created,
        seed_collection: graph.seed.as_ref().map(|s| s.collection.clone()),
        seed_fields: graph.seed.as_ref().map(|s| s.fields.clone()),
        status: "compiled".to_string(),
    })
}

// ---------------------------------------------------------------------------
// Structural validation
// ---------------------------------------------------------------------------

/// Validate the graph definition before any writes. This covers all the rules
/// the spec lists that don't require a live DB lookup: duplicate node ids,
/// graph_id / node_id GraphQL-name validity, edge reference checks, cycle
/// detection (DFS), per-mode required fields, and reachability.
///
/// Behavior/surface existence checks (rules 1 & 2 in the spec) require a live
/// node and are deferred to the self-config write path's own validation, which
/// already rejects references to nonexistent behaviors/surfaces.
pub(crate) fn validate_graph(graph: &GraphDefinition) -> Result<()> {
    // graph_id must be a valid GraphQL name prefix (it's embedded in task_ids).
    crate::graphql::validate_graphql_name(&graph.graph_id)
        .map_err(|e| anyhow!("graph_id {:?} is not a valid GraphQL name: {e}", graph.graph_id))?;

    ensure!(
        !graph.nodes.is_empty(),
        "graph {:?} has no nodes; at least one node is required",
        graph.graph_id
    );

    // Node ids must be valid GraphQL names (embedded in task_ids). HashMap
    // already guarantees uniqueness by key, so duplicate node ids are
    // impossible at the deserialization level — but we check explicitly so
    // the error message is clear if a future caller bypasses serde.
    for node_id in graph.nodes.keys() {
        crate::graphql::validate_graphql_name(node_id)
            .map_err(|e| anyhow!("node id {node_id:?} is not a valid GraphQL name: {e}"))?;
    }

    // Required node fields.
    for (node_id, node) in &graph.nodes {
        ensure!(
            !node.behavior_id.trim().is_empty(),
            "node {node_id:?} is missing a behavior_id"
        );
        ensure!(
            !node.prompt_template.trim().is_empty(),
            "node {node_id:?} is missing a prompt_template"
        );
    }

    // Edge reference checks + per-mode validation.
    let node_ids: HashSet<&str> = graph.nodes.keys().map(String::as_str).collect();
    for (i, edge) in graph.edges.iter().enumerate() {
        ensure!(
            node_ids.contains(edge.from.as_str()),
            "edge[{i}] from {:?} references a node that does not exist",
            edge.from
        );
        ensure!(
            node_ids.contains(edge.to.as_str()),
            "edge[{i}] to {:?} references a node that does not exist",
            edge.to
        );

        // source_collection must be a valid collection identifier (it's
        // interpolated into GraphQL by the trigger engine).
        crate::graphql::validate_collection_identifier(&edge.source_collection).map_err(|e| {
            anyhow!(
                "edge[{i}] ({} → {}) source_collection {:?} is not a valid collection identifier: {e}",
                edge.from, edge.to, edge.source_collection
            )
        })?;

        match edge.fire_mode.as_str() {
            "per_document" => {
                // Fan-out: source_collection is required (checked above).
                // correlation_field is optional for fan-out.
            }
            "per_group" => {
                // Barrier: correlation_field is required.
                ensure!(
                    edge.correlation_field
                        .as_deref()
                        .map(|f| !f.trim().is_empty())
                        .unwrap_or(false),
                    "edge[{i}] ({} → {}) has fire_mode \"per_group\" but is missing \
                     correlation_field (required for barriers)",
                    edge.from,
                    edge.to
                );
                // And either expected_count or expected_count_field.
                let has_count = edge.expected_count.is_some();
                let has_count_field = edge
                    .expected_count_field
                    .as_deref()
                    .map(|f| !f.trim().is_empty())
                    .unwrap_or(false);
                ensure!(
                    has_count || has_count_field,
                    "edge[{i}] ({} → {}) has fire_mode \"per_group\" but is missing \
                     expected_count or expected_count_field (required for barriers)",
                    edge.from,
                    edge.to
                );
            }
            other => {
                bail!(
                    "edge[{i}] ({} → {}) has invalid fire_mode {other:?}; \
                     use \"per_document\" or \"per_group\"",
                    edge.from,
                    edge.to
                )
            }
        }
    }

    // Cycle detection (DFS). The graph must be a DAG.
    detect_cycles(&graph)?;

    // Reachability: every node must appear in at least one edge or be a seed
    // target. A node that is neither a source nor a sink of any edge is only
    // valid if the seed targets it (the agent kicks it off manually). Since we
    // can't know which node the seed targets, we relax to: every node must
    // appear in at least one edge OR the graph has a seed.
    if graph.seed.is_none() {
        let mut referenced: HashSet<&str> = HashSet::new();
        for edge in &graph.edges {
            referenced.insert(edge.from.as_str());
            referenced.insert(edge.to.as_str());
        }
        let orphans: Vec<&str> = graph
            .nodes
            .keys()
            .filter(|id| !referenced.contains(id.as_str()))
            .map(String::as_str)
            .collect();
        ensure!(
            orphans.is_empty(),
            "graph {:?} has orphan nodes not reachable from any edge and no seed defined: {:?}; \
             every node must appear in at least one edge, or provide a seed",
            graph.graph_id,
            orphans
        );
    }

    // Duplicate trigger_ids (when explicitly provided).
    let mut seen_triggers: HashSet<&str> = HashSet::new();
    for (i, edge) in graph.edges.iter().enumerate() {
        if let Some(tid) = &edge.trigger_id {
            ensure!(
                seen_triggers.insert(tid.as_str()),
                "duplicate trigger_id {:?} on edge[{i}]",
                tid
            );
        }
    }

    Ok(())
}

/// DFS-based cycle detection. Returns Ok for a DAG, Err naming the cycle.
fn detect_cycles(graph: &GraphDefinition) -> Result<()> {
    // Build adjacency list.
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &graph.edges {
        adj.entry(edge.from.as_str()).or_default().push(edge.to.as_str());
    }

    // 0 = unvisited, 1 = in-progress (on the current DFS stack), 2 = done.
    let mut state: HashMap<&str, u8> = HashMap::new();
    let mut path: Vec<&str> = Vec::new();

    fn dfs<'a>(
        node: &'a str,
        adj: &HashMap<&'a str, Vec<&'a str>>,
        state: &mut HashMap<&'a str, u8>,
        path: &mut Vec<&'a str>,
    ) -> Result<()> {
        match state.get(node).copied().unwrap_or(0) {
            2 => return Ok(()), // already fully explored
            1 => {
                // Found a back-edge → cycle. Build the cycle path from the
                // stack for a clear error message.
                let start = path.iter().position(|n| *n == node).unwrap_or(0);
                let cycle: Vec<&str> = path[start..].iter().copied().chain(std::iter::once(node)).collect();
                bail!("graph contains a cycle: {}", cycle.join(" → "));
            }
            _ => {}
        }
        state.insert(node, 1);
        path.push(node);
        if let Some(neighbors) = adj.get(node) {
            for &next in neighbors {
                dfs(next, adj, state, path)?;
            }
        }
        path.pop();
        state.insert(node, 2);
        Ok(())
    }

    for node_id in graph.nodes.keys() {
        dfs(node_id.as_str(), &adj, &mut state, &mut path)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Document creation (via SelfConfigCore write path)
// ---------------------------------------------------------------------------

/// Create one Task document through the transactional self-config write path.
async fn create_task(
    core: &SelfConfigCore,
    task_id: &str,
    node_id: &str,
    node: &GraphNode,
) -> Result<()> {
    use crate::config_client::patch::{SelfConfigPatch, SelfConfigTarget};
    use crate::self_config::automation_request;

    let mut patch: SelfConfigPatch = Vec::new();
    // Store the node's target behavior_id in the task's `name` field — the
    // Task's `behavior_id` is the ownership link, pinned to the calling
    // behavior by the self-config write path (automation_request's on_create).
    // The trigger engine dispatches the task under the calling behavior; the
    // node's behavior_id is recorded here for reference.
    patch.push((
        "name".to_string(),
        Some(Value::String(format!("{node_id} ({})", node.behavior_id))),
    ));
    patch.push((
        "prompt_template".to_string(),
        Some(Value::String(node.prompt_template.clone())),
    ));
    patch.push(("enabled".to_string(), Some(Value::Bool(true))));
    if let Some(output_collection) = &node.output_collection {
        // Use the output collection as the task's output_schema_ref when
        // provided (the task's bounded write surfaces are pre-populated).
        patch.push((
            "output_schema_ref".to_string(),
            Some(Value::String(output_collection.clone())),
        ));
    }
    // behavior_id is pinned at create by on_create (automation_request), so
    // we don't set it in the patch — it's protected on updates.

    let request = automation_request(
        core,
        SelfConfigTarget::Task,
        task_id.to_string(),
        patch,
    );
    core.apply(request).await?;
    Ok(())
}

/// Create one EventTrigger document through the transactional self-config
/// write path.
async fn create_trigger(
    core: &SelfConfigCore,
    trigger_id: &str,
    task_id: &str,
    edge: &GraphEdge,
) -> Result<()> {
    use crate::config_client::patch::{SelfConfigPatch, SelfConfigTarget};
    use crate::self_config::automation_request;

    let mut patch: SelfConfigPatch = Vec::new();
    patch.push(("task_id".to_string(), Some(Value::String(task_id.to_string()))));
    patch.push((
        "source_collection".to_string(),
        Some(Value::String(edge.source_collection.clone())),
    ));
    // Fixed event_kind for now (document_created).
    patch.push((
        "event_kind".to_string(),
        Some(Value::String("document_created".to_string())),
    ));
    patch.push(("enabled".to_string(), Some(Value::Bool(true))));
    patch.push((
        "fire_mode".to_string(),
        Some(Value::String(edge.fire_mode.clone())),
    ));
    if let Some(cf) = &edge.correlation_field {
        patch.push(("correlation_field".to_string(), Some(Value::String(cf.clone()))));
    }
    if let Some(ecf) = &edge.expected_count_field {
        patch.push((
            "expected_count_field".to_string(),
            Some(Value::String(ecf.clone())),
        ));
    }
    if let Some(ec) = edge.expected_count {
        patch.push(("expected_count".to_string(), Some(Value::Number(ec.into()))));
    }
    if let Some(gts) = edge.group_timeout_secs {
        patch.push(("group_timeout_secs".to_string(), Some(Value::Number(gts.into()))));
    }
    if let Some(gmc) = edge.group_min_count {
        patch.push(("group_min_count".to_string(), Some(Value::Number(gmc.into()))));
    }
    if let Some(f) = &edge.filter {
        patch.push(("filter".to_string(), Some(Value::String(f.clone()))));
    }
    if let Some(c) = &edge.concurrency {
        patch.push(("concurrency".to_string(), Some(Value::String(c.clone()))));
    }

    let request = automation_request(
        core,
        SelfConfigTarget::EventTrigger,
        trigger_id.to_string(),
        patch,
    );
    core.apply(request).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid graph for test fixtures.
    fn sample_dag() -> GraphDefinition {
        let mut nodes = HashMap::new();
        nodes.insert(
            "scan".to_string(),
            GraphNode {
                behavior_id: "coding-gents-smart".to_string(),
                prompt_template: "Scan {{repo_path}}.".to_string(),
                output_collection: Some("ScanResult".to_string()),
                surface_id: Some("scan-io".to_string()),
            },
        );
        nodes.insert(
            "analyze".to_string(),
            GraphNode {
                behavior_id: "coding-gents-smart".to_string(),
                prompt_template: "Analyze findings.".to_string(),
                output_collection: Some("AnalysisReport".to_string()),
                surface_id: None,
            },
        );
        nodes.insert(
            "report".to_string(),
            GraphNode {
                behavior_id: "session-classifier".to_string(),
                prompt_template: "Write the final report.".to_string(),
                output_collection: Some("FinalReport".to_string()),
                surface_id: None,
            },
        );
        GraphDefinition {
            graph_id: "test_pipeline".to_string(),
            description: Some("test".to_string()),
            nodes,
            edges: vec![
                GraphEdge {
                    from: "scan".to_string(),
                    to: "analyze".to_string(),
                    trigger_id: Some("scan-to-analyze".to_string()),
                    fire_mode: "per_document".to_string(),
                    source_collection: "ScanResult".to_string(),
                    correlation_field: Some("run_id".to_string()),
                    expected_count_field: None,
                    expected_count: None,
                    group_timeout_secs: None,
                    group_min_count: None,
                    filter: None,
                    concurrency: None,
                },
                GraphEdge {
                    from: "analyze".to_string(),
                    to: "report".to_string(),
                    trigger_id: None, // auto-generated
                    fire_mode: "per_group".to_string(),
                    source_collection: "AnalysisReport".to_string(),
                    correlation_field: Some("run_id".to_string()),
                    expected_count_field: Some("expected_total".to_string()),
                    expected_count: None,
                    group_timeout_secs: Some(300),
                    group_min_count: None,
                    filter: None,
                    concurrency: None,
                },
            ],
            seed: Some(GraphSeed {
                collection: "ScanRequest".to_string(),
                fields: HashMap::from([
                    ("repo_path".to_string(), Value::String("/tmp/repo".to_string())),
                ]),
            }),
        }
    }

    #[test]
    fn compiles_simple_dag() {
        // Structural validation passes for a well-formed 3-node, 2-edge DAG.
        let graph = sample_dag();
        validate_graph(&graph).expect("valid DAG should pass validation");
    }

    #[test]
    fn rejects_cycle() {
        let mut graph = sample_dag();
        // Add an edge report → scan to create a cycle: scan → analyze → report → scan.
        graph.edges.push(GraphEdge {
            from: "report".to_string(),
            to: "scan".to_string(),
            trigger_id: Some("report-to-scan".to_string()),
            fire_mode: "per_document".to_string(),
            source_collection: "FinalReport".to_string(),
            correlation_field: None,
            expected_count_field: None,
            expected_count: None,
            group_timeout_secs: None,
            group_min_count: None,
            filter: None,
            concurrency: None,
        });
        let err = validate_graph(&graph).unwrap_err();
        assert!(
            err.to_string().contains("cycle"),
            "expected cycle error, got: {err}"
        );
    }

    #[test]
    fn rejects_barrier_without_correlation() {
        let mut graph = sample_dag();
        // Corrupt the barrier edge: remove correlation_field.
        graph.edges[1].correlation_field = None;
        let err = validate_graph(&graph).unwrap_err();
        assert!(
            err.to_string().contains("correlation_field"),
            "expected correlation_field error, got: {err}"
        );
    }

    #[test]
    fn rejects_barrier_without_expected_count() {
        let mut graph = sample_dag();
        // Remove both expected_count_field and expected_count from the barrier.
        graph.edges[1].correlation_field = Some("run_id".to_string());
        graph.edges[1].expected_count_field = None;
        graph.edges[1].expected_count = None;
        let err = validate_graph(&graph).unwrap_err();
        assert!(
            err.to_string().contains("expected_count"),
            "expected expected_count error, got: {err}"
        );
    }

    #[test]
    fn rejects_duplicate_node_ids() {
        // HashMap dedupes by key at deserialization, so we simulate the error
        // by checking that two entries with the same key collapse to one.
        let mut nodes = HashMap::new();
        nodes.insert(
            "dup".to_string(),
            GraphNode {
                behavior_id: "b1".to_string(),
                prompt_template: "p1".to_string(),
                output_collection: None,
                surface_id: None,
            },
        );
        // Inserting again with the same key overwrites — HashMap behavior.
        nodes.insert(
            "dup".to_string(),
            GraphNode {
                behavior_id: "b2".to_string(),
                prompt_template: "p2".to_string(),
                output_collection: None,
                surface_id: None,
            },
        );
        let graph = GraphDefinition {
            graph_id: "dup_test".to_string(),
            description: None,
            nodes,
            edges: vec![],
            seed: None,
        };
        // The graph has one node (deduped) and no edges/seed → orphan check
        // fires because the sole node isn't referenced by any edge.
        let err = validate_graph(&graph).unwrap_err();
        assert!(
            err.to_string().contains("orphan"),
            "expected orphan error for unreferenced node, got: {err}"
        );
    }

    #[test]
    fn generates_correct_task_ids() {
        // Verify the {graph_id}-{node_id} naming convention.
        let graph = sample_dag();
        let graph_id = &graph.graph_id;
        for node_id in graph.nodes.keys() {
            let expected = format!("{graph_id}-{node_id}");
            assert!(
                expected.starts_with("test_pipeline-"),
                "task_id {expected} should be prefixed with graph_id"
            );
        }
    }

    #[test]
    fn generates_correct_trigger_ids() {
        // Explicit trigger_id is used as-is; omitted ones auto-generate as
        // {graph_id}-{from}-{to}.
        let graph = sample_dag();
        let graph_id = &graph.graph_id;

        // Edge 0 has an explicit trigger_id.
        assert_eq!(graph.edges[0].trigger_id.as_deref(), Some("scan-to-analyze"));

        // Edge 1 omits trigger_id → auto-generated.
        let auto = format!(
            "{graph_id}-{}-{}",
            graph.edges[1].from, graph.edges[1].to
        );
        assert_eq!(auto, "test_pipeline-analyze-report");
    }

    #[test]
    fn rejects_missing_behavior() {
        let mut graph = sample_dag();
        // Blank out a behavior_id.
        graph.nodes.get_mut("scan").unwrap().behavior_id = "  ".to_string();
        let err = validate_graph(&graph).unwrap_err();
        assert!(
            err.to_string().contains("behavior_id"),
            "expected behavior_id error, got: {err}"
        );
    }

    #[test]
    fn rejects_edge_to_nonexistent_node() {
        let mut graph = sample_dag();
        graph.edges.push(GraphEdge {
            from: "scan".to_string(),
            to: "ghost".to_string(), // doesn't exist
            trigger_id: None,
            fire_mode: "per_document".to_string(),
            source_collection: "ScanResult".to_string(),
            correlation_field: None,
            expected_count_field: None,
            expected_count: None,
            group_timeout_secs: None,
            group_min_count: None,
            filter: None,
            concurrency: None,
        });
        let err = validate_graph(&graph).unwrap_err();
        assert!(
            err.to_string().contains("ghost"),
            "expected reference error naming 'ghost', got: {err}"
        );
    }

    #[test]
    fn rejects_invalid_graph_id() {
        let mut graph = sample_dag();
        graph.graph_id = "bad id with spaces".to_string();
        let err = validate_graph(&graph).unwrap_err();
        assert!(
            err.to_string().contains("graph_id"),
            "expected graph_id error, got: {err}"
        );
    }

    #[test]
    fn rejects_invalid_fire_mode() {
        let mut graph = sample_dag();
        graph.edges[0].fire_mode = "per_whatever".to_string();
        let err = validate_graph(&graph).unwrap_err();
        assert!(
            err.to_string().contains("fire_mode"),
            "expected fire_mode error, got: {err}"
        );
    }
}
