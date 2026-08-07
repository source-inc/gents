# Harbor / Terminal-Bench

This adapter runs Gents as a Harbor custom agent inside each task container.
The Gents process sees the same `/app` working tree as the verifier, uses native
write and shell tools, persists the complete request lifecycle, and writes an
ATIF v1.7 trajectory to `/logs/agent/trajectory.json`.

Run commands from the Gents repository root so Harbor can import
`scripts.harbor.gents_agent:GentsAgent`.

## Requirements

- Harbor with Docker access.
- A Linux `gents` binary containing the ATIF projection from PR #988. During
  development, pass its host path with `GENTS_BINARY_PATH`. Once this lands in
  a release, `GENTS_RELEASE_URL` may point at the x86_64 Linux release tarball.
- For a full Terminal-Bench run, a Bullseye glibc compatibility bundle matching
  the Linux binary architecture. Some task images are musl-based, so they cannot
  execute a dynamically linked glibc binary without its loader and libraries. Pass the bundle with
  `GENTS_GLIBC_BUNDLE_PATH`; when it is set the adapter always installs the
  bundle and runs Gents through its loader. Leave it unset only when every task
  image already ships a glibc loader — the adapter checks for one and fails the
  trial if it is missing.
- An OpenAI-compatible chat-completions endpoint reachable from task
  containers.

The adapter intentionally does not build Gents inside every benchmark task.
That would add Rust compilation time and network variance to the agent score.
Build the compatibility bundle once with:

```sh
./scripts/harbor/build_glibc_bundle.sh \
  /absolute/path/to/gents-glibc-bullseye-x86_64.tar.gz
```

For native Linux/arm64 task containers on an Apple Silicon controller, use:

```sh
./scripts/harbor/build_glibc_bundle_aarch64.sh \
  /absolute/path/to/gents-glibc-bullseye-aarch64.tar.gz
```

## DeepSeek V4 Flash on workstation-1

The workstation service exposes model ID `d4f`. The official DeepSeek code-agent
evaluation uses `reasoning_effort=max`, `temperature=1.0`, and `top_p=0.95`;
those are the adapter defaults.

```sh
DOCKER_DEFAULT_PLATFORM=linux/amd64 PYTHONPATH="$PWD" \
  HARBOR_BIN=/absolute/path/to/harbor \
  HARBOR_HEALTHCHECK_URL=http://100.73.235.38:8000/v1/models \
  ./scripts/harbor/run_with_cleanup.sh run \
  -d terminal-bench/terminal-bench-2-1 \
  --agent scripts.harbor.gents_agent:GentsAgent \
  --model d4f \
  --n-concurrent 16 \
  --n-concurrent-agents 16 \
  --timeout-multiplier 1000 \
  --agent-timeout-multiplier 1000 \
  --max-retries 0 \
  --delete \
  --allow-agent-host 100.73.235.38 \
  --ae GENTS_BINARY_PATH=/absolute/path/to/gents \
  --ae GENTS_GLIBC_BUNDLE_PATH=/absolute/path/to/gents-glibc-bullseye-x86_64.tar.gz \
  --ae GENTS_INFERENCE_URL=http://100.73.235.38:8000/v1 \
  --ae GENTS_DOCKER_PLATFORM=linux/amd64 \
  --ae GENTS_CONTEXT_WINDOW=458752 \
  --ae GENTS_MAX_OUTPUT=393216 \
  --ae GENTS_MAX_TOTAL=100000 \
  --ae GENTS_MAX_TURNS=1000 \
  --ae GENTS_REQUEST_TIMEOUT_SECS=86400
```

The `1000` multipliers effectively disable Harbor's environment-build, agent
setup, agent execution, and verifier deadlines. The explicit 24-hour request
deadline serves the same purpose inside the runtime. Foreground commands are
deliberately bounded far below it (#1018): each command defaults to a 600-second
timeout and the model may explicitly request up to 3,600 seconds per command. A
command that hits its timeout is killed as a process group and returns a normal
`status: "timeout"` tool outcome with partial output, so the model can recover
or narrow the command instead of silently occupying the benchmark slot; longer
work belongs in `spawn_process`, which has a 10-hour background lifetime budget.
Both values are advertised to the model in the bash tool schema, logged at
server startup in `gents-server.log`, and recorded per call as `timeout_ms` in
the persisted command result. The Studio Two controller has
32 CPUs and a 256 GiB OrbStack memory allocation. Concurrency 16 keeps the
workstation inference service busy while leaving enough aggregate KV-cache
headroom for typical Terminal-Bench trajectories. The workstation's measured
2,638,151-token KV pool provides about 164,884 live tokens per request at c=16,
or 5.15 fully populated 512K sequences; unusually long generations will queue
or preempt rather than behaving like infinite KV.

`run_with_cleanup.sh` contains controller-level cancellation: Harbor normally
runs `docker compose down` after each trial, and the wrapper removes only new
Harbor `__env` Compose projects if the controller is interrupted. Each Gents
attempt also gets a UUID-suffixed `/tmp` home, so a cancelled attempt's RocksDB
lock cannot affect a retry. When `HARBOR_HEALTHCHECK_URL` is set, the wrapper
also checks the inference service every 15 seconds and stops Harbor after three
consecutive failures; both values can be changed with
`HARBOR_HEALTHCHECK_INTERVAL_SECS` and `HARBOR_HEALTHCHECK_FAILURE_LIMIT`.

Within each trial, `run_gents.sh` supervises the Gents server and response
waiter as one lifecycle. If the server exits during an active request, the
waiter is cancelled and the runner reports whether `wait(2)` observed an exit
code or a terminating signal. Before cleanup removes the UUID-scoped home, the
runner attempts a bounded status query, partial response, timeline and ATIF
projection, captures a server-log tail and process tree, inventories the local
store, and archives the home only when it fits the configured size ceiling.
GraphQL failure is retained explicitly rather than replacing the server-loss
cause with a connection error. Compaction failures that exhaust both guided
decoding and the strict non-guided JSON fallback are classified separately as
`compaction_provider_error` in `gents-outcome.json`.

The adapter installs an explicit `gents-fs-runner` shim on every trial and
verifies it during setup. The shim exists for images that need the glibc
compatibility loader: there, Linux reports the loader rather than the wrapped
Gents executable as `/proc/self/exe`, so Gents cannot discover its embedded
filesystem runner by basename. Installing it unconditionally keeps setup
identical across image types.

Harbor isolates agent-phase network access. Keep `--allow-agent-host` aligned
with the inference host or the task container will not be able to reach the
model endpoint.

`PYTHONPATH` keeps the repository importable after Harbor changes into the job
directory. For a deterministic smoke run, add a fully qualified filter such as
`--include-task-name terminal-bench/write-compressor`. To take the first
matching task instead, add `--n-tasks 1`. Start the complete 89-task run only
after the smoke task produces a valid trajectory and verifier result.

Useful overrides:

| Variable | Default | Purpose |
|---|---:|---|
| `GENTS_TEMPERATURE` | `1.0` | Request sampling temperature |
| `GENTS_TOP_P` | `0.95` | Request nucleus sampling |
| `GENTS_TOP_K` | unset | Optional request top-k |
| `GENTS_SEED` | unset | Optional non-negative sampling seed, persisted on the request and Harbor result metadata |
| `GENTS_REASONING_EFFORT` | `max` | DeepSeek thinking effort (`low`, `high`, or `max`) |
| `GENTS_MODEL` | Harbor `--model` | Model ID sent to the inference endpoint |
| `GENTS_DOCKER_PLATFORM` | unset | Force task images/builds, e.g. `linux/amd64` |
| `GENTS_GLIBC_BUNDLE_PATH` | unset | glibc loader/library bundle for musl task images |
| `GENTS_MAX_OUTPUT` | `393216` | Per-turn output ceiling, matching DeepSeek's 384K (384 × 1024) `high`/`max` recommendation. Each completion clamps this ceiling to the context remaining after its assembled input. The name deliberately avoids Harbor's secret-key `TOKEN` heuristic. |
| `GENTS_MAX_TOTAL` | required | Positive provider-reported token allowance for the whole durable request. Every completed provider call is charged, including tool turns, compaction, and later-retracted attempts; optional title inference is disabled for budgeted requests. Missing usage and observed overruns fail closed. The name avoids Harbor's secret-key `TOKEN` heuristic. |
| `GENTS_CONTEXT_WINDOW` | `458752` | Gents prompt/compaction budget. The 75% compaction threshold admits up to 344,064 estimated input tokens; the per-turn output clamp preserves the combined-context invariant and the difference from D4F's 512K server limit leaves 53,248 tokens of tokenizer-accounting headroom. |
| `GENTS_MAX_TURNS` | `1000` | Agent completion-loop turn ceiling |
| `GENTS_RETRY_MAX_TRANSPORT` | `3` | Transient inference retry ceiling |
| `GENTS_REQUEST_TIMEOUT_SECS` | `86400` | Durable request and Harbor exec timeout |
| `GENTS_COMMAND_TIMEOUT_SECS` | `600` | Foreground command timeout applied when the model omits `timeout_secs` |
| `GENTS_COMMAND_TIMEOUT_MAX_SECS` | `3600` | Foreground cap for explicit `timeout_secs` requests; kept far below `GENTS_REQUEST_TIMEOUT_SECS` so a pathological command returns control to the model (#1018) |
| `GENTS_DIAGNOSTIC_TIMEOUT_SECS` | `10` | Per-command ceiling while capturing failure diagnostics |
| `GENTS_DIAGNOSTIC_HOME_MAX_BYTES` | `67108864` | Maximum runtime-home size eligible for the diagnostic archive; larger homes retain an inventory and an archive-skipped note |
| `GENTS_TRACE_SETTLE_TIMEOUT_SECS` | `30` | Maximum wait for root inference-call terminal usage persistence before accepting the ATIF export |
| `GENTS_SUPERVISION_POLL_SECS` | `1` | Poll interval for the server/response-waiter lifecycle supervisor |
| `GENTS_TOOL_ROOT` | `/app` | Filesystem and shell tool root |

Jack Bench's retained controller sets `GENTS_JACK_BENCH_ATTESTATION=1` and
`GENTS_HARBOR_CONTROLLER_BINARY_SHA256=<sha256>`. These are not general run
overrides. In that mode the pinned adapter independently rehashes the complete
private task package, Harbor task content, adapter sources, Gents binary, and
executing Harbor entry point; verifies the installed Gents commit and Harbor
version; and writes an exclusive `jack-bench-runtime-attestation.json` beside
the trial's `agent/` directory before inference. Any mismatch fails agent setup.

Each trial retains:

- `trajectory.json` — Harbor-native ATIF v1.7 trajectory
- `request.json`, `request.stdout.json`, and `response.json` — request and response
- `gents-init.json`, `gents-profile.json`, `gents-tools.json`,
  `gents-tools-explain.json`, `gents-status.json`, and
  `gents-server.log` — runtime evidence
- On runner failure, `gents-diagnostic.json`, `gents-server-exit.json` when the
  server was reaped, `gents-server-tail.txt`, `process-tree.txt`, final status
  or `graphql-unavailable.txt`, partial response/timeline/ATIF output, and
  `gents-home-inventory.txt`. `gents-home.tar.gz` is also retained when the
  home fits `GENTS_DIAGNOSTIC_HOME_MAX_BYTES`.

`trajectory.json.final_metrics` carries Harbor's standard
`total_prompt_tokens`, `total_completion_tokens`, and `total_cached_tokens`.
Those totals include every provider call owned by the root request, including
compaction. The adapter copies the values into the Harbor `AgentContext`
counters. It waits for every root `InferenceCall` row to leave
`queued`/`running` before accepting the export, so asynchronous terminal
persistence cannot silently undercount the last provider call. Exact turn or
aggregate-token exhaustion after at least one charged call is returned to the
verifier as a scoreable budget outcome; missing usage, provider-reported
overrun, exhaustion before any provider call retained chargeable usage, and
other runtime/provider failures remain agent errors.
