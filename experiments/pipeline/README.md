# Canonical pack: two-stage document pipeline

Self-contained Gents pack: domain SDL, `DatastoreToolSurface`, least-privilege
tool selections, tasks, and EventTriggers — all under this folder.

```text
create ExperimentJob
        │
        ▼  EventTrigger exp-stage1
   stage-1  ──write_experiment_finding (surface)──►  ExperimentFinding
                                                          │
                                                          ▼  EventTrigger exp-stage2
                                                     stage-2 (no tools)
```

## Layout

| Path | Role |
| --- | --- |
| `schemas/` | Pack-scoped SDL (`ExperimentJob`, `ExperimentFinding`) — applied by `config apply` |
| `datastore-tool-surfaces/experiment-writes/` | Create-tool grant → `ExperimentFinding` |
| `tool-selections/exp-tools-stage1/` | Links surface only; every other tool off |
| `tool-selections/exp-tools-stage2/` | Zero tools |
| `event_triggers/` | `exp-stage1` on job create; `exp-stage2` on finding create |
| `runs/` | Gitignored exports |

## Tools (least privilege)

| Behavior | Tools | Why |
| --- | --- | --- |
| stage-1 | `write_experiment_finding` only | Surface expands to one `BoundedWriteTool` create |
| stage-2 | none | Finding is already in the task prompt via `{{ doc.* }}` |

Surfaces name a **collection already on the node** (string). Pack apply
registers `schemas/` first so that name resolves.

## Run (anyone with a gents install + a model endpoint)

1. Init once (example uses DeepSeek V4 Flash on workstation-1):

   ```bash
   gents init --home <home> --inference-url http://100.73.235.38:8000/v1 \
     --backend-preset vllm --openai-wire-api chat-completions --model-name d4f \
     --tool-package minimal
   ```

2. **Validate + start server with pack apply** (recommended):

   ```bash
   gents config validate --root experiments/pipeline
   gents server --home <home> --http-port 19191 --p2p-transport none --no-codex-shim \
     --apply-root experiments/pipeline --apply-prune
   ```

   After ready, the server applies this pack against the **in-process** node
   (`schemas/` first, then desired-state; home DID rebind). The serving JSON
   includes an `apply_root` field with the apply report.

   Equivalent without folding into server:

   ```bash
   gents server --home <home> --http-port 19191 --p2p-transport none --no-codex-shim
   gents config apply --root experiments/pipeline --home <home> \
     --graphql http://127.0.0.1:19191/api/v0/graphql \
     --bind-agent-did home --force-rebind-concrete-did --prune
   ```

3. Wait for EventSource logs: observing `ExperimentJob` **before** seeding
   (created/first-seen only). Apply-root registers triggers; still wait for
   the observe log before the first seed.

4. Kick:

   ```graphql
   mutation {
     create_ExperimentJob(input: {
       job_id: "exp-…"
       prompt: "Your research question"
       suite: "pipeline"
       arm: "pipeline"
     }) { _docID job_id }
   }
   ```

5. Await `caused_by_trigger_id` `exp-stage1` then `exp-stage2`; export with
   `gents trace timeline --request-id <id> --home <home>`.

## Design

- Topology: `docs/superpowers/specs/2026-08-07-event-trigger-graph-experiments-design.md`
- Surfaces: `docs/superpowers/specs/2026-08-07-datastore-tool-surface-design.md`
