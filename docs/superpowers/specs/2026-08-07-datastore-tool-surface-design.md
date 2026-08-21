# Datastore tool surface (design)

## Status

**Implemented on this PR** (schema + desired-state + snapshot expand + tests +
`pipeline-two-stage` surface migration). Companion to the EventTrigger graph
experiments design (`2026-08-07-event-trigger-graph-experiments-design.md`):
that work proved document-pipeline DAGs with **hand-authored** `write_tools`;
this feature makes granting those tools reusable without re-listing fields on
every tool selection, and makes the least-privilege stage agent — **whose only
tools are datastore reads and the discrete transitions we want it to make** —
a first-class, document-configured thing.

## Problem

Document-pipeline multi-agent work needs a tight loop:

1. Author domain collections (seed, findings, claims, …).
2. Give models **narrow create tools** for those collections.
3. Wire **Tasks** + **EventTriggers** so creates advance the graph.

Today step 2 is copy-paste: each `ToolSelection` carries a
`write_tools: [String]` column of JSON `WriteToolDecl`s (tool name, collection,
description, fields). That works — the shipped `pipeline-two-stage` arm uses
exactly one such decl (`write_experiment_finding` → `ExperimentFinding`) — but:

- Every experiment/arm re-declares the same field list already fixed by SDL.
- Sharing one "research write surface" across behaviors means duplicating the
  same decls on every tool selection.
- Authors who want another collection must know the `WriteToolDecl` shape, not
  just the schema they already wrote.

We do **not** want to expose full GraphQL / Defra to the model. The model should
see ordinary tools with JSON-schema args; the runtime performs one validated
create into one collection (existing `BoundedWriteTool` path).

## Decision

Add a small **config document** that names a reusable set of datastore create
tools, and **link it from `ToolSelection` by id**. At snapshot build, the
runtime expands the link into the same `WriteToolDecl` / `BoundedWriteTool`
machinery already used today.

```text
DatastoreToolSurface (apply-owned config doc)
  surface_id, agent_did, entries: [String] of WriteToolDecl JSON
           │
           ▼
ToolSelection.datastore_tool_surface_ids: [String]     ← bare-id refs
           │  resolved in document_view/snapshot.rs (like skills / subagent targets)
           ▼
decls ∪ inline write_tools → BehaviorToolConfig → BoundedWriteTool  (unchanged)
```

Three reuse commitments (these are the design, not implementation detail):

1. **Entries are create or query decls.** Create entries remain
   `WriteToolDecl`s (no `kind`, backward compatible). Query entries set
   `"kind": "query"` and deserialize as `QueryToolDecl`. The `entries`
   column is unchanged.
   (`document_config/tool_selection.rs`) including the existing dual-shape
   deserializer (JSON objects in manifests, JSON strings in the Defra
   `[String]` column) and the `graphql_string_list_field` encoder that
   already handles the empty-list→`null` rule.
2. **The link field copies the one true id-list precedent** on ToolSelection:
   `allowed_mcp_service_ids: [String]` — bare ids referencing another config
   collection. (Skills are *not* the precedent: `skill_refs` lives on
   `AgentBehavior`, and `write_tools` / `subagent_targets` are inline JSON,
   not refs.)
3. **Expansion happens where cross-doc refs already resolve:**
   `agent/document_view/snapshot.rs`, alongside `effective_skills` and
   `validate_subagent_targets_resolve`. The `tool_surface/` layer stays
   DB-free; it receives a fully-merged `Vec<WriteToolDecl>` exactly as today.

## Non-goals (v1)

- **Richer EventTrigger conditions** (status transitions, barriers/joins,
  `event_kind: updated`). Separate work; this design does not depend on it.
- Auto-tooling every collection on the node (explicit allowlist only).
- Update / delete tools. Single-collection **query** entries now live on the
  same `entries` column (`"kind": "query"`); the generic `defra_query` console
  remains available for multi-collection reads.
- Free-form GraphQL for models.
- Generating SDL from the surface (SDL stays the source of truth for types;
  the surface only references collections/fields).
- Per-field value constraints (enums / allowed values for transition fields).
  Today every `BoundedWriteTool` arg is string-typed in the model-visible
  JSON schema. Tightening that is a `WriteToolDecl`-level extension that
  would benefit inline `write_tools` equally — out of scope here.
- Changing EventTrigger or Task semantics.

## Shape

### New collection: `DatastoreToolSurface`

Apply-owned config: operators write it via desired-state apply or GraphQL;
the runtime never mutates it, and (see Self-config below) agents must not be
able to either.

| Field | Role |
| --- | --- |
| `surface_id` | Stable unique id (desired-state handle) |
| `agent_did` | Owner principal (same scoping as other agent config) |
| `display_name` | Optional |
| `enabled` | Soft disable without unlinking |
| `entries` | `[String]` of JSON-serialized `WriteToolDecl`s |

**Normative expand rule:** each entry of each enabled linked surface is a
`WriteToolDecl`; the merged decl list is `union(inline write_tools, surface
entries)` in deterministic order (inline first, then surfaces in link order,
entries in document order). The merged list feeds the existing build path
unchanged.

### Link from `ToolSelection`

```text
datastore_tool_surface_ids: [String]   # surface_id refs, same agent_did
```

- Empty / absent → no change from today (only inline `write_tools`).
- A **list**, not a single id: composing "research writes" + "ops writes" on
  one selection is the point of the feature.
- Expanded decls union with inline `write_tools`.

### Failure semantics (normative)

Two runtime precedents exist and they disagree: a dangling `skill_ref` is
**silently skipped** (`skills.rs::effective_skills` is a pure filter), while a
dangling `backend_id` / `inference_profile_id` **bails and marks the behavior
unavailable** (`snapshot.rs` → `unavailable_behaviors`, with a reason string).

This feature follows the **backend/profile precedent — fail closed**:

- Desired-state `validate` / `apply`: missing, foreign-`agent_did`, or
  disabled surface ref → hard error, apply refused.
- Runtime (config arrived via GraphQL or drifted): missing or disabled
  surface, or a merged-list name collision → behavior lands in
  `unavailable_behaviors` with a reason naming the surface id.

Rationale: for the stage agents this feature targets, the surface may be the
agent's **entire** tool set. Skill-style silent skip would produce a tool-less
agent that still claims requests and burns a model turn doing nothing — the
worst failure mode for a DAG. Never silently drop a surface.

### Name collisions

Reuse the existing machinery, run over the **merged** list:
`ToolSelectionDocument::validate()` already rejects duplicate `tool_name`s,
reserved built-in names (`bash`, `read_file`, `defra_query`, meta/subagent
tools, …), and `cli_tool_names` collisions. Surface entries get the identical
checks — at desired-state validate (manifest has all surfaces in hand) and at
snapshot build (runtime backstop → behavior unavailable). Do not silently
overwrite; do not add a second collision policy.

### Desired-state layout

```text
datastore-tool-surfaces/<surface_id>/object.json
```

`ToolSelection` object gains `datastore_tool_surface_ids: ["…"]`.

Apply order: **rank-0 leaf, same as `Skill`** — surfaces carry no outgoing
refs and must be written before the tool selections that reference them
(prune order is the reverse, and `prune.rs` gains the ref edge so a
referenced surface can't be deleted).

**v1 CLI scope:** manifest apply/validate/diff/export only, plus read-only
`gents config datastore-surface list|show`. No `set`/`create` CRUD command —
the precedent is `EventTrigger`, which is also writable only via apply. This
cuts the per-collection CLI surface roughly in half.

## What this actually touches (cost, verified)

"Small config doc" is honest about the runtime but not about the plumbing. A
new config collection touches, mechanically:

- **Lean first** (per the foundation flow — apply order is proof-fenced):
  `proofs/Proofs/ApplyReconcile/Collections.lean` (variant, `applyOrder`,
  `manifestAuthoritative`, parity theorem) and
  `ContractCases/{Types,Fixtures}.lean`. All total matches — the build fails
  until every one is extended. Mechanical; no new proof obligations expected.
- `collection.rs` (variant + 6 exhaustive fns + the hardcoded parity test);
  `CONFIG_APPLY_ORDER` / `CONFIG_PRUNE_ORDER` in `config_import.rs` (both
  `[Collection; 12]` → 13). Lean↔Rust parity is asserted by
  `lean_apply_write_boundary_tests.rs`.
- Schema registration: `gents-schemas` (`ALL`, name consts, self-enforcing
  file-count test), `gents-protocol/src/schemas.rs`, and a
  `DEFAULT_BASELINE` entry **with a pinned version CID** in
  `gents-migration/registry.rs` (new collections join the frozen baseline,
  not an `AddCollection` step — per the doc comment there).
- Desired-state parity with `Skill`: manifest struct + load/write/validate/
  normalize/diff/convert/prune (+ `shared.rs`, `config_bundle.rs`,
  export field consts) — ~12 files.
- Runtime visibility: `DocumentRuntimeView` field + `load.rs` list query +
  `apply.rs` incremental doc-id path + the `snapshot.rs` resolution itself.

The expand hook is genuinely a few dozen lines; the collection plumbing is
the real cost and it is all mechanical, pattern-following work. Accepted:
this is the price of config being documents, and it buys desired-state
diff/apply, replication, and lineage for free.

## Self-config (security)

"Apply-owned" is enforced by the self-config patch validator, not by ACP
(no config collection carries `@policy`; ACP is blocked upstream). Today
`write_tools` is already in the protected set ("operator/apply-managed and
protected", `self_config/mod.rs`). Therefore, normatively:

- `datastore_tool_surface_ids` joins `write_tools` in the protected,
  never-self-patchable fields of `ToolSelection`.
- **No self-config category is added** for `DatastoreToolSurface`.

Otherwise an agent with `tools`-category self-config could mint itself new
write tools, defeating the allowlist.

## The recipe this enables: datastore-only stage agents

The target persona: a subagent whose only capabilities are **reading pipeline
documents and making the discrete transitions we allow** — everything else
off. One surface + one selection. Prefer a bound query entry over
`defra_query` when the stage only needs one collection:

`datastore-tool-surfaces/experiment-io/object.json`:

```json
{
  "surface_id": "experiment-io",
  "agent_did": "did:key:PLACEHOLDER",
  "enabled": true,
  "entries": [
    {
      "tool_name": "record_experiment_finding",
      "collection": "ExperimentFinding",
      "description": "Record a finding document for the next pipeline stage.",
      "fields": [
        {"name": "job_id", "required": true},
        {"name": "finding_id", "required": true},
        {"name": "content", "required": true},
        {"name": "stage", "required": true}
      ]
    },
    {
      "kind": "query",
      "tool_name": "query_experiment_finding",
      "collection": "ExperimentFinding",
      "description": "Load findings for this run.",
      "fields": ["finding_id", "content", "stage"],
      "filter_fields": [
        {"name": "run_id", "fill": "correlation"}
      ]
    }
  ]
}
```

Existing create entries omit `kind` and stay writes. Query entries set
`"kind": "query"`. The model never names the collection; filled filter
fields are hidden and applied as `_eq`. Default row cap is 1000.

Tool selection (the interesting bits):

```json
{
  "selection_id": "exp-stage-datastore-only",
  "datastore_tool_surface_ids": ["experiment-io"],
  "enable_defra_query": false,
  "enable_file_tools": false,
  "enable_bash": false,
  "enable_meta_tools": false,
  "enable_context_budget": false,
  "enable_memory": false,
  "enable_session_history_tool": false,
  "enable_self_config": false,
  "subagent_spawn_enabled": false
}
```

Sharp edges the docs must carry (both exist today, independent of this
feature):

- `enable_meta_tools` and `enable_context_budget` are version-gated
  **default-true** — write `false` explicitly or the agent gets them.
- An **empty** `defra_query_collections` list means **all collections**.
  Least-privilege reads should use a surface query entry instead of
  `defra_query` whenever a single collection is enough.
- There is no `enable_write_tools` flag: a well-formed decl (inline or via
  surface) *is* the enablement.

These behaviors slot into `subagent_targets` unchanged — a parent references
`{name, agent_did, behavior_id}` exactly as today, so "spawn a subagent whose
whole world is the pipeline collections" is: one surface doc, one selection,
one behavior, one target entry.

## Validation

Static / apply-time (desired-state `validate`, extending
`validate_write_tools`):

- `surface_id` unique, non-empty; `agent_did` present and equal to the
  principal's (same rule as ToolSelection).
- Each entry: `WriteToolDecl::is_well_formed()` plus the existing field-name
  checks (non-empty, no duplicates within an entry); non-empty description
  preferred.
- No duplicate `tool_name` within a surface, across a selection's linked
  surfaces, against its inline `write_tools`, against reserved built-ins, or
  against `cli_tool_names`.
- Every `datastore_tool_surface_ids` entry resolves to a same-agent, enabled
  surface in the manifest.

Live validate (apply-time, same class as the EventTrigger source-collection
probe):

- `collection` exists on the node; optionally, each `fields[].name` is a
  field of that collection (collection existence is the v1 floor).

Surfaces may target app/experiment SDL registered on the node — same as
today's write tools and EventTrigger sources. No requirement to live in
product `gents-schemas`.

## Alternatives considered

| Alternative | Why not for v1 |
| --- | --- |
| Keep only inline `write_tools` | Works; poorly shareable, duplicates SDL, and every author re-learns the decl shape. |
| Share at authoring time (manifest include/templating) | No such mechanism exists in desired-state; would be invisible to live GraphQL-authored config and to replication. |
| Auto-tools for every collection | Too wide; models need an explicit grant. |
| Generate tools from SDL with no surface doc | No stable home for `tool_name`/description/allowlist; nothing to review in a desired-state diff. |
| Give models `defra_query` + raw mutations | Full API surface; wrong trust model. |
| Fold into `Skill` | Skills are instruction bundles resolved with **silent-skip** semantics and behavior-level refs; surfaces need fail-closed semantics and selection-level refs. A skill may *reference* a surface later. |

## PR plan (when implementing)

| PR | Contents |
| --- | --- |
| **This PR** | Design doc only (iterate). |
| A | The full vertical slice: Lean apply-order + `Collection` + schema/baseline registration + desired-state parity + `DocumentRuntimeView` loading + snapshot expand + fail-closed/unavailable semantics + collision checks, with unit/conformance tests (including: dangling ref ⇒ behavior unavailable; surface-equivalent-to-inline ⇒ identical tool surface). |
| B | Consumers: switch `pipeline-two-stage` stage-1 to `experiment-writes` (drop the inline decl, assert no behavior change), read-only `list|show` CLI, operator docs in `demo/README.md`. |

No "link stored, expand later" intermediate: shipping a reference field that
does nothing invites drift and silent no-op configs — the expand is the small
part.

## Success criteria (implementation)

- A surface applies as desired-state and links from a tool selection.
- Models on that selection see byte-identical tool definitions to the
  equivalent inline `write_tools` (same names, schemas, descriptions).
- Creates still execute through `BoundedWriteTool` — no second write path —
  and still fire EventTriggers (the `write_tool_trigger_e2e` contract).
- Validate rejects duplicate/reserved tool names and dangling, foreign, or
  disabled surface refs; at runtime the same conditions mark the behavior
  unavailable, never a silent skip.
- Self-config cannot create or link surfaces.
- `pipeline-two-stage` drops its inline finding decl for a shared surface
  with no behavior change.

## Related code (verified seams)

- `WriteToolDecl` / `WriteToolField`, dual-shape deserializer, collision
  checks: `crates/gents/src/document_config/tool_selection.rs`
- `BoundedWriteTool` (create-only `add_<Collection>`, arg validation,
  string-typed JSON schema): `crates/gents/src/defra_write/mod.rs`
- Expand seam + unavailable-behavior precedent:
  `crates/gents/src/agent/document_view/{load,apply,snapshot}.rs`
  (skills resolution and backend/profile bail-out live here)
- Surface build (stays DB-free): `crates/gents/src/tool_surface/`
- Link-field precedent: `allowed_mcp_service_ids` in
  `crates/gents-schemas/schemas/agent/tool_selection.graphql`
- Apply-order proofs: `crates/gents/proofs/Proofs/ApplyReconcile/`
- Migration baseline: `crates/gents-migration/src/registry.rs`
- Self-config protected fields: `crates/gents/src/self_config/mod.rs`
- Motivating usage: `demo/pipeline/` (canonical pack; stage-1 links
  `experiment-writes`), `docs/superpowers/specs/2026-08-07-event-trigger-graph-experiments-design.md`
