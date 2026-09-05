# Grok background-task projection

The shim projects runtime state, not another process lifecycle. The server
passes its existing BackgroundExecutionRegistry to each connection. Task
cancellation uses the runtime process-control API after checking registered
sessions or canonical, controllable child edges.

Running processes expose the full retained runtime window (currently 256 KiB),
not merely the database's 4 KiB observation tail. Eviction is reported as
truncation. Terminal output uses persisted completion evidence and its capture
limits. Output already discarded is not recoverable; model page budgets are
unchanged.

Native task registration precedes cumulative tool_call_update output. Delivery
fingerprints commit only after successful sends. During overlapping turns,
background activity and child streams continue; parent text and its cursors
stay pending. Background parent updates omit prompt/timing metadata so the
pager cannot adopt an old turn or reset the active timer. Only the canonical
request lifecycle can terminalize a child, not an interrupt marker alone.

## Stock UI audit

In the local xai-org/grok-build checkout, xai-grok-pager's
app/acp_handler/mod.rs handles SessionMatch::Child by passing ordinary tool
calls/updates to the child session tracker and scrollback. Bash tools are not
intentionally hidden.

app/acp_handler/background.rs handles task start/completion via
resolve_target_view, selecting the child's background-task store. Root
ToolCallUpdate handling calls route_bg_task_stdout to replace cumulative task
output. The child branch bypasses that helper and calls session.handle_update
directly. Therefore the current cumulative-output projection alone does not
prove that child task cards render output.

The stock client is the compatibility target: no client changes are in scope.
Its existing `x.ai/monitor_event` handler routes appended background output into
child task stores. Child projections use that channel; root projections retain
cumulative `tool_call_update` output. Delivery receipts hold byte offsets and
hashes, not another output buffer. They commit only after successful sends.
Evicted bytes are explicitly identified. When final persisted capture differs
from the live interleaved stream, it is labelled as a captured snapshot rather
than guessed to be a continuation.

Stock child-pane task cancellation addresses the parent session. The shim
resolves the task's actual owner through that session's authorized canonical
descendants, rejecting ambiguous IDs before mutation. The runtime rechecks
the resolved session/principal/requester scope before cancellation.

## Live wire check

With spawn_process and bash_unrestricted enabled on the served behavior:

```sh
python3 demo/grok-tui-port/scripts/grok_background_probe.py \
  --socket /path/to/grok.sock --cwd "$PWD" \
  --model GLM-5.3-Flash-NVFP4
```

The probe launches one bounded process, verifies a >100 KB output snapshot
after task registration, and cancels only that process through the native
task RPC. This complements the runtime and shim regression tests; it is not
a stock-pager rendering test.

Add `--child` when a `control-worker` subagent target is configured to exercise
the same large-output and native cancellation checks in a child session.

## Rebased live verification

The updated production binary was served from a fresh database, with the
configured GLM-5.3-Flash-NVFP4 backend, 524,288-token context and high reasoning:

- Root and child background processes both delivered the >100 KB retained
  snapshot after registration, accepted native task cancellation, returned
  `already_exited` on repeat cancellation, and emitted cancelled task completion.
- Cross-turn model-facing list/read/steer and native subagent get/list/cancel
  passed. The cancelled child became durably interrupted and its bash call
  cancelled; the queued steering request completed with the expected marker.
- Child tool/text delivery followed pane creation and preceded its finish
  event. The child reply appeared once; successful bash completion included
  its output, while automatic wake echoes carried hidden-scrollback metadata.

The unchanged Grok 1.0.13 fullscreen client also displayed a running background
bash task inside a completed child's pane. Opening its task-output viewer
showed the generated `CHILD_OUTPUT_VISIBLE_6142` marker. This exercises actual
rendering, in addition to the framed-ACP assertions above.

A second stock-client session exercised the native child task kill action:
selecting the running task and pressing `x` immediately rendered
`Task failed ... (cancelled)`, and the corresponding runtime tool row became
`cancelled`. The parent-addressed and explicit-child-addressed framed controls
also passed. No client source or installed client binary was modified.
