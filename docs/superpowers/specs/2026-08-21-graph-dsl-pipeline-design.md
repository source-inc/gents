# Graph DSL Pipeline Tool — Design Spec

## Goal

A single agent-callable tool (`compile_graph`) that takes a structured graph
definition and materializes it as gents automation documents — Tasks,
EventTriggers, and references to existing DatastoreToolSurface entries — so
an orchestrator agent (Amy) can author and execute multi-stage subagent
pipelines on the fly.

The tool is a **compiler**: graph definition → automation documents. It does
not execute the pipeline. Execution happens via the existing trigger/task
engine: the agent writes a seed document through a bounded write tool, triggers
fire, tasks run, barriers resolve.

## Background

PR #1162 (defending-code pack) proved the pattern: 14-stage pipeline using
`per_document` (fan-out) and `per_group` (barrier) event triggers, with bounded
typed tool surfaces per stage. That was a compiled pipeline declared in
manifest files. This DSL makes the same pattern agent-authorable.

The old workflow tool (`workflow.rs`, `orchestration.rs`) was torn out in favor
of this approach — one execution model (triggers/tasks), one lineage model
(document replication), one persistence model (DefraDB).

## The Tool

### Name: `compile_graph`

### Input Schema (structured tool call argument)

```json
{
  "graph_id": "security-sweep",
  "description": "Parallel security scan across two repos with synthesis",
  "nodes": {
    "scan_gents": {
      "behavior_id": "coding-gents-smart",
      "prompt_template": "Scan {{repo_path}} for vulnerabilities. Write findings via write_scan_result.",
      "output_collection": "ScanResult",
      "surface_id": "scan-io"
    },
    "scan_amyg": {
      "behavior_id": "coding-amygdala-smart",
      "prompt_template": "Scan {{repo_path}} for vulnerabilities. Write findings via write_scan_result.",
      "output_collection": "ScanResult",
      "surface_id": "scan-io"
    },
    "synthesize": {
      "behavior_id": "session-classifier",
      "prompt_template": "Synthesize findings from all scans into a prioritized report. Write via write_report.",
      "output_collection": "SynthesisReport",
      "surface_id": "report-io"
    }
  },
  "edges": [
    {
      "from": "scan_gents",
      "to": "synthesize",
      "trigger_id": "scan-gents-to-synth",
      "fire_mode": "per_document",
      "source_collection": "ScanResult",
      "correlation_field": "run_id"
    },
    {
      "from": "scan_amyg",
      "to": "synthesize",
      "trigger_id": "scan-amyg-to-synth",
      "fire_mode": "per_group",
      "source_collection": "ScanResult",
      "correlation_field": "run_id",
      "expected_count_field": "expected_total"
    }
  ],
  "seed": {
    "collection": "ScanRequest",
    "fields": {
      "repo_path": "/Users/admin/gents",
      "scope": "security"
    }
  }
}
```

### What the compiler produces

For each node:
1. A **Task** document (via the same write path as `configure_automation`):
   - `task_id`: `{graph_id}-{node_id}` (e.g. `security-sweep-scan_gents`)
   - `behavior_id`: from node spec
   - `prompt_template`: from node spec
   - `enabled`: true
   - `output_schema_ref`: optional, from node spec

For each edge:
2. An **EventTrigger** document:
   - `trigger_id`: from edge spec (or `{graph_id}-{from}-{to}` if omitted)
   - `task_id`: `{graph_id}-{to}` (references the target node's task)
   - `source_collection`: from edge spec
   - `event_kind`: `"document_created"` (fixed for now)
   - `fire_mode`: `"per_document"` (fan-out) or `"per_group"` (barrier)
   - `correlation_field`: from edge spec
   - `expected_count_field`: from edge spec (barriers only)
   - `group_timeout_secs`: from edge spec (optional, barriers only)
   - `enabled`: true

The compiler does NOT create:
- Behaviors (they already exist, referenced by ID)
- Tool selections (behaviors already own their tool selections)
- DatastoreToolSurface entries (referenced by `surface_id`, pre-populated)
- Collections/schemas (must exist or be created separately)

### Return value

```json
{
  "graph_id": "security-sweep",
  "tasks_created": ["security-sweep-scan_gents", "security-sweep-scan_amyg", "security-sweep-synthesize"],
  "triggers_created": ["scan-gents-to-synth", "scan-amyg-to-synth"],
  "seed_collection": "ScanRequest",
  "seed_fields": { "repo_path": "/Users/admin/gents", "scope": "security" },
  "status": "compiled"
}
```

The agent then writes the seed document via the appropriate bounded write tool
to kick off execution.

## Validation Rules

1. All `behavior_id` values must reference existing behaviors.
2. All `surface_id` values must reference existing DatastoreToolSurface docs.
3. No cycles in the edge graph (DAG only — loops are handled by the agent's
   goal mechanism, not by trigger cycles).
4. Barrier edges (`per_group`) must have `correlation_field` and either
   `expected_count` or `expected_count_field`.
5. Fan-out edges (`per_document`) must have `source_collection`.
6. Every node must be reachable from at least one edge or be a seed target.
7. Node IDs must be valid GraphQL names (they're embedded in task_ids).
8. `graph_id` must be a valid GraphQL name prefix.

## Implementation Plan

### File: `crates/gents/src/toolset/graph_dsl.rs`

New module containing:
- `GraphDefinition` struct (deserializes from the input schema above)
- `GraphNode`, `GraphEdge`, `GraphSeed` structs
- `compile_graph()` function that:
  1. Validates the graph (cycle detection, reference checks, required fields)
  2. For each node, creates a Task document via the SelfConfigCore write path
  3. For each edge, creates an EventTrigger document via the same path
  4. Returns the summary
- `CompileGraphTool` implementing the `Tool` trait
- `CompileGraphParams` deserializing the input

### Registration

Register `CompileGraphTool` in `toolset.rs` build function, gated behind a
new tool-selection flag `enable_graph_dsl` (default false). This follows the
same pattern as `enable_self_config`, `enable_memory`, etc.

### Key patterns to follow

- **Transactional writes**: Use `SelfConfigCore` / `ConfigApplyTxn` for all
  document creation. Same path as `configure_automation`. All writes in one
  transaction — if any fail, all roll back.
- **Ownership**: Tasks and triggers are owned by the calling behavior. The
  `behavior_id` field on the Task is the node's behavior_id; the EventTrigger's
  `task_id` references back to a task owned by the same graph.
- **Per-instance tool name**: Like `BoundedWriteTool`, the tool name is
  `compile_graph` (a single tool, not per-instance).
- **Error handling**: Return `anyhow::Error` with clear messages naming the
  offending node/edge. The agent needs to know what to fix.

### Tests

In `crates/gents/src/toolset/graph_dsl.rs` (or a sibling test file):

1. `compiles_simple_dag` — 3 nodes, 2 edges, verify task/trigger docs created
2. `rejects_cycle` — graph with a cycle, verify error
3. `rejects_missing_behavior` — node with nonexistent behavior_id
4. `rejects_barrier_without_correlation` — per_group edge without correlation_field
5. `rejects_duplicate_node_ids` — two nodes with same ID
6. `generates_correct_task_ids` — verify `{graph_id}-{node_id}` naming
7. `generates_correct_trigger_ids` — verify auto-generated IDs when omitted

## Non-Goals (for this cut)

- Collection/schema creation — collections must exist; the tool surfaces are
  pre-populated and referenced by ID.
- Pipeline execution — the tool compiles to documents only. Execution is via
  the existing trigger engine.
- Cleanup/teardown — no automatic disabling of tasks/triggers after pipeline
  completion. The agent can clean up via `configure_automation` with
  `enabled: false`.
- `map_over` fan-out from structured results — each trigger fires per document
  in the source collection; the agent doesn't need to parse a result list.
- Loop/cycle support — the graph must be a DAG. Loops are the agent's
  responsibility via the goal mechanism.
