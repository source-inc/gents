# Event-trigger graph experiments — implementation plan

**Spec:** `docs/superpowers/specs/2026-08-07-event-trigger-graph-experiments-design.md`  
**Operator guide:** `experiments/README.md`

## Status (shipped in this PR)

| Item | Status |
| --- | --- |
| Design doc | Shipped under `docs/superpowers/specs/` |
| Single productionized pipeline under `experiments/` | Shipped (no multi-arm matrix) |
| `DatastoreToolSurface` `experiment-writes` on stage-1 | Shipped |
| Least-privilege tool selections (stage-1 write only; stage-2 none) | Shipped |
| DeepSeek V4 Flash / workstation-1 backend | Shipped (`d4f` @ `http://100.73.235.38:8000/v1`) |
| Operator path + measurement docs | Shipped in `experiments/README.md` |
| `gents config validate --root experiments` | Passes |
| CI e2e / harness binary | **Not shipped** — deferred |

This PR ships experiment config + DatastoreToolSurface runtime. No multi-shape
A/B matrix and no new CI e2e.

## Goal (original)

Ship EventTrigger-driven graph shapes as desired-state arms — document
pipeline, single GraphQL seed kick, measure with `gents trace` /
`InferenceCall` tokens — without `fan_out_and_synthesize`.

## Architecture (reminder)

- Nodes = Tasks/behaviors  
- Edges = EventTriggers on `created` only  
- Kick = one `create_ExperimentJob` **after** EventSource observes the
  collection  
- Measure = trigger lineage + `gents trace timeline|project` + GraphQL
  `InferenceCall` token fields  

## Global constraints (still true)

- EventTrigger v1: `event_kind = "created"` only; no response-lifecycle edges;
  no fan-in barrier.
- Trigger templates: `doc` / `event` / `node` / `ctx` — not `args` (manual-run
  only).
- `orchestration_enabled: false` on experiment tool selections.
- Do not commit `experiments/runs/` artifacts or secrets.
- Placeholder principal requires
  `--bind-agent-did home --force-rebind-concrete-did`.
- Schema registration: prefer `gents schema apply experiments/schemas --home
  <home>` (local); GraphQL remote may 503 if collection management is off.

## Completed work (historical task map)

### Task 1 — `experiments/` layout — **done**

- `experiments/README.md`
- `experiments/schemas/experiment_job.graphql`
- `experiments/schemas/experiment_finding.graphql`
- `experiments/runs/.gitignore`

### Task 2 — Pipeline desired-state root — **done**

Single pack at `experiments/pipeline/` (two-stage pipeline + surface). Former draft
arms `single-loop` / `fanout-on-job` removed.

Validate:

```bash
gents config validate --root experiments/pipeline
```

### Task 3 — CI e2e wrapper — **deferred (not this PR)**

Was: `crates/gents/tests/e2e_triggers/experiment_graph_e2e.rs` + optional
`cli_experiment_shapes`. Explicitly **out of scope** for the shipped
config+docs PR. Existing patterns to copy later:

- `event_trigger_e2e.rs`
- `write_tool_trigger_e2e.rs`

### Task 4 — Measurement docs — **done**

In `experiments/README.md` and the design: timeline, multi-agent projection,
InferenceCall tokens; do not use `AgentResponse.token_count` for cost.

## Operator verification (manual)

See `experiments/README.md`. Live kick has been demonstrated against
workstation-1 DeepSeek: one seed create after EventSource observe →
`caused_by_trigger_id` / `caused_by_trigger_kind: event` request →
completed run on backend `exp-deepseek`.

## Explicitly deferred

- `experiment_graph_e2e` / `cli_experiment_shapes` / any CI workflow for arms
- `event_kind: updated`; barrier / join triggers
- Live-LLM quality A/B suite (judge scoring stays offline)
- Promoting `ExperimentJob` into product `gents-schemas`
- Cross-node P2P arms
- Runner/harness code under `experiments/`

## References

- Design: `docs/superpowers/specs/2026-08-07-event-trigger-graph-experiments-design.md`
- Operator: `experiments/README.md`
- E2E patterns (reference only): `crates/gents/tests/e2e_triggers/`
- Desired-state layout: `crates/gents-cli/src/desired_state/{write,validate}.rs`
- Config / schema / trace CLI: `gents config apply|diff|export|validate`,
  `gents schema apply`, `gents trace timeline|project`
