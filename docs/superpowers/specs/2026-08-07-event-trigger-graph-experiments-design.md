# Event-trigger graph experiments (design)

## Status (shipped)

This design describes the **shipped** experiment surface on branch/PR:

- **Canonical self-contained pack** at `experiments/pipeline/` (schemas +
  config): `ExperimentJob` → finding write → `ExperimentFinding`
- Stage-1 create tool via **`DatastoreToolSurface` `experiment-writes`**
  (not inline `write_tools`); stage-2 has **no tools**
- **`gents config apply --root experiments/pipeline`** applies pack-local
  `schemas/` first, then desired-state (surfaces, selections, triggers)
- No multi-arm shape matrix, no harness binary
- Inference example targets **DeepSeek V4 Flash** (`d4f`) via OpenAI-compatible
  chat completions on Tailscale peer **workstation-1**
  (`http://100.73.235.38:8000/v1`)

Earlier draft arms (`single-loop`, `fanout-on-job`) were removed in favor of the
pipeline pack. CI mock e2e remains an optional **follow-up**.

## Problem

We want a **repeatable, version-controlled way to compare multi-agent
topologies** on Gents — not as ephemeral in-loop orchestration, but as
document-driven graphs:

- **Nodes** = Tasks bound to behaviors (prompt + model + tools)
- **Edges** = EventTriggers on collection creates
- **Shared state** = source documents (`{{ doc.* }}` in prompt templates)
- **Kickoff** = a single GraphQL create of a seed document
- **Measurement** = `run_timeline` + adapter projections + `InferenceCall` tokens

Substantially all runtime infrastructure already exists. This workstream adds:

1. **Agent config** — desired-state root at `experiments/`, applied with
   `gents config apply --root experiments`.
2. **Operator documentation** — `experiments/README.md` for validate → schema
   apply → server → apply → wait for EventSource → single seed create →
   await lineage → timeline / token export.

No new harness code under `experiments/`.

This design deliberately **does not** use `fan_out_and_synthesize` for
experiment topology. Fan-out becomes "N EventTriggers on the same seed
create." Pipeline stages become "stage agents create next-collection docs."
Barrier / fan-in is **out of scope for v1** (see Non-goals).

## Constraints from the runtime (v1 EventTrigger)

These are product facts, not preferences:

| Constraint | Implication for graphs |
| --- | --- |
| `event_kind` is **`created` only** (first-seen) | Edges fire on **new documents**, never on in-place status updates |
| `AgentResponse` / `AgentRequest` are created then updated | Do **not** chain stages on "response completed" or lifecycle transitions |
| Filter is a GraphQL fragment on the source doc | Stage routing = fields on the seed / artifact docs |
| Concurrency is **per trigger** (`parallel` / `serial` / `latest_only`) | No multi-child barrier; join is not a trigger feature |
| Multiple triggers may match one create | Native **fan-out** |
| Materialized requests stamp `caused_by_trigger_id` + `caused_by_trigger_kind: "event"` | Measurement and await use trigger lineage |
| Event-trigger templates see `{{ doc.* }}` and `{{ event.* }}` (plus `node.node_did`, `node.behavior_id`, `ctx.now`); `{{ args.* }}` is **manual-run only** | Seed fields must carry `job_id`, prompt, arm labels |
| First-seen tracking seeds forward-only with a scan cap (`event_source.rs`) | Use fresh, experiment-only collections as trigger sources; **do not create the seed until EventSource logs that it is observing the collection** |

**Conclusion:** experiment graphs are **document pipelines**. Each stage that
should fire a later stage must **create** a document in a watched
collection.

## Architecture

A **pack** is a self-contained desired-state root: its own `schemas/`
alongside the config documents, so one `apply` registers collections and
config together.

```text
experiments/
  README.md                       what a pack is; index of packs
  pipeline/                       the shipped pack
    README.md                     operator guide (real apply path)
    schemas/                      SDL for ExperimentJob / ExperimentFinding
    agent-principal.json
    datastore-tool-surfaces/      experiment-writes (stage-1's create tool)
    tool-selections/  agent-behaviors/  tasks/  event_triggers/
    inference-backends/  inference-profiles/
    runs/                         gitignored scratch for trace exports
```

```text
 ExperimentJob create ──► EventTrigger exp-stage1 ──► stage-1 agent
                                                          │
                                     record_experiment_finding (surface tool)
                                                          ▼
                                              ExperimentFinding create
                                                          │
                                                          ▼
                                          EventTrigger exp-stage2 ──► stage-2 agent
```

Fan-out remains expressible — N EventTriggers on the same seed create —
but the shipped pack is the two-stage pipeline, because it is the shape
that exercises the write-tool edge.

### Seed document = experiment handle

One shared seed collection, `ExperimentJob`, holds:

| Field | Purpose |
| --- | --- |
| `job_id` | Stable run id; greppable in prompts and lineage queries |
| `prompt` | Task body for templates (`{{ doc.prompt }}`) |
| `suite` | Experiment suite name (e.g. `topology-ab`) |
| `arm` | Which pack was applied |

Kickoff is intentionally one mutation:

```graphql
mutation {
  create_ExperimentJob(input: {
    job_id: "exp-…"
    prompt: "…"
    suite: "topology-ab"
    arm: "pipeline"
  }) { _docID }
}
```

### The shipped pack: `experiments/pipeline`

| Stage | Trigger | Tools | Produces |
| --- | --- | --- | --- |
| stage-1 | `exp-stage1` on `ExperimentJob` created | `experiment-writes` surface only (`record_experiment_finding`) | `ExperimentFinding` docs |
| stage-2 | `exp-stage2` on `ExperimentFinding` created | none | response only |

Every kick goes through a trigger (never a direct `AgentRequest` create),
so the pack has one kick API: a single `create_ExperimentJob`.

Stage-1 is the datastore-only agent the tool-surface design targets — its
entire tool set is one create tool, granted by a `DatastoreToolSurface`
document rather than inline `write_tools`. Stage-2 has no tools at all,
proving the edge is the *document create*, not a lifecycle update.

Prompt templates exercise the full trigger template surface —
`{{ doc.* }}`, `{{ event.* }}` (`trigger_id`, `source_collection`,
`source_doc_id`, `fired_at`), `node.node_did` / `node.behavior_id`,
`ctx.now` — so a successful fire also proves template rendering.

The pack uses backend **`exp-deepseek`**:

- `endpoint`: `http://100.73.235.38:8000/v1`
- `provider_kind`: `OpenAiCompatible`
- `openai_wire_api`: `chat_completions`
- `model_name` / models: **`d4f`** (server id for DeepSeek-V4-Flash)

Tool selections set `orchestration_enabled: false`.

### Config surface

A pack is a desired-state root in the layout `gents config export` writes
and `apply` / `validate` / `diff` read, plus a pack-local `schemas/`:

```text
pipeline/
  schemas/*.graphql                              applied before config docs
  agent-principal.json
  inference-backends/<backend_id>/object.json
  inference-profiles/<profile_id>/object.json
  datastore-tool-surfaces/<surface_id>/object.json
  tool-selections/<selection_id>/object.json     orchestration_enabled: false
  agent-behaviors/<behavior_id>/object.json      (+ system_prompt.md sidecar)
  tasks/<task_id>/object.json                    (+ prompt.md sidecar)
  event_triggers/<trigger_id>/object.json        note: underscore dir name
```

```bash
gents config validate --root experiments/pipeline   # static, no server
gents config apply    --root experiments/pipeline --home <home> \
  --graphql http://127.0.0.1:<port>/api/v0/graphql \
  --bind-agent-did home --force-rebind-concrete-did
```

`config apply` registers `schemas/` on the node **before** the config
documents, so trigger source collections and surface targets exist by the
time live validation runs. `gents server --apply-root experiments/pipeline`
does the same against the in-process node at startup.

`--bind-agent-did home` rebinds the root's placeholder DID to the target
home; **`--force-rebind-concrete-did` is required** for the checked-in
placeholder principal. Layout reference:
`crates/gents-cli/tests/cli_config_validate.rs`.

### Running an experiment (operator path)

Full detail: **`experiments/pipeline/README.md`**. Summary:

1. `gents init` with `--inference-url http://100.73.235.38:8000/v1`,
   `--openai-wire-api chat-completions`, `--model-name d4f`
2. `gents server --apply-root experiments/pipeline` — one command: the pack's
   `schemas/` register on the in-process node, then the config applies with
   the home DID rebind. (Equivalent two-step: start `gents server`, then
   `gents config apply --root experiments/pipeline` with the rebind flags.)
3. Wait for log: `event source now observing source collection
   source_collection=ExperimentJob`
4. POST one `create_ExperimentJob` with a fresh `job_id`
5. Poll `AgentRequest(filter: { caused_by_trigger_id: { _eq: "…" } })`, then
   `gents trace timeline` / `project`

Trigger ids: `exp-stage1` / `exp-stage2`.

### Measurement

```bash
gents trace timeline --request-id <id> --home …
gents trace project --projection multi-agent --format eval-jsonl --request-id <id> --home …
```

Cost/structure metrics we trust (v1):

- request count / sibling count by `caused_by_trigger_id`
- inference call count and wall time from the timeline
- **token usage from `InferenceCall.prompt_tokens` / `completion_tokens` /
  `cached_input_tokens`** (query `InferenceCall` by `request_id` — timeline
  rows do not yet project these fields)

Do **not** use `AgentResponse.token_count` as a cost metric — it is a
streaming word-count proxy and can read 0 on recovered responses.

Quality scoring (LLM-as-judge, human rubrics) is **out of band**: export
eval-jsonl; score offline.

## Non-goals (v1 / this PR)

- New harness/runner code under `experiments/` — packs are config + docs
- **CI e2e** (`experiment_graph_e2e.rs`) or any new CI workflow for packs
  (optional follow-up only)
- Replacing or extending `fan_out_and_synthesize` barrier semantics
- `event_kind: updated` / "on lifecycle completed" triggers
- Claiming topology quality wins without a separate judge suite
- Cross-node P2P packs
- Promoting `ExperimentJob` into product `gents-schemas`

## Decisions

1. **Seed collections are pack-local SDL** in `experiments/pipeline/schemas/`;
   promote into `gents-schemas` only if productized.
2. **Stage-1 findings** come from a create tool granted by the
   `experiment-writes` `DatastoreToolSurface` rather than inline
   `write_tools`; operators may also inject `ExperimentFinding` docs via
   GraphQL when the model is weak.
3. **Every kick goes through a trigger**, never a direct `AgentRequest`
   create, so the pack has one kick API.
4. **Packs live at repo-root `experiments/`** so apply paths are short
   and runs are not mixed with design docs.
5. **Packs are self-contained** — `schemas/` lives inside the pack and is
   applied ahead of the config documents, so one command bootstraps a run.
6. **No CI e2e in this deliverable** — operator path + `gents config validate`
   are the gates; a mock e2e in `e2e_triggers` may be added later if desired.
7. **v1 needs no `{{ args.* }}` in trigger scope** — the seed document
   carries every run parameter. Exposing `args` to event-trigger templates
   is an accepted runtime follow-up, not a v1 blocker.

## Success criteria (this deliverable)

- The pipeline pack checks in as a desired-state root and passes
  `gents config validate --root experiments/pipeline`
- `gents server --apply-root experiments/pipeline` bootstraps schemas +
  config in one command against a fresh home
- Operator README documents the rebind flags, EventSource ordering, the
  DeepSeek endpoint, and InferenceCall token measurement
- Design/plan match the shipped tree

## Related code

- Trigger engine: `crates/gents/src/trigger_engine/` (first-seen semantics
  in `event_source.rs`)
- Existing event-trigger e2e patterns (reference only, not shipped for arms):
  `crates/gents/tests/e2e_triggers/event_trigger_e2e.rs`,
  `write_tool_trigger_e2e.rs`
- Desired state: `crates/gents-cli/src/desired_state/` (layout in
  `write.rs`, checks in `validate.rs`); worked fixture in
  `crates/gents-cli/tests/cli_config_validate.rs`
- Timeline / projections: `crates/gents/src/run_timeline.rs`,
  `adapter_projection.rs`
- CLI: `gents config {export,diff,apply,validate}`, `gents schema apply`,
  `gents trace {timeline,project}`
