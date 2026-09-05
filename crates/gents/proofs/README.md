# gents Lean Proofs

This directory contains the Lean 4 model for `gents`.

The goal is not to prove math in isolation. The goal is to make the runtime
state machines explicit enough that:

- lifecycle invariants are written down once
- Rust changes can be checked against an executable model
- unresolved Rust/spec mismatches are isolated as deviations, while intentional
  product boundaries are documented explicitly

The proofs are strongest where the runtime is a state machine:

- request, process, and persistence lifecycle transitions
- request execution leases with opaque generations, semantic-progress renewal,
  atomic request/response terminalization, and recovery race exclusion (#1341)
- daemon-visible storage observation assumptions at the persistence boundary
- inference-call lifecycle, cancellation transitions, and slot reconstruction
- scheduler and fleet slot accounting from persisted call rows
- session retry/reissue
- tenant-safe, on-demand session hydration admission and document selection
- runtime reconcile generation publication
- desired-state apply ordering and field ownership
- task, schedule, and event-trigger dispatch
- isolated workspace lifecycle, append-only bindings, seal/owner routing, authority meet, and callback invocation journals (`Proofs/Workspace`, `Proofs/Callback`)
- client turn projection and desktop shell workflow state
- command/tool execution policy for bash argv, network, sandbox, and shell env
- MCP/tool execution preflight and retry eligibility boundaries
- managed native executor deadline/cancel liveness and tool composition
- mailbox owner stamping, open-row idempotence, terminal transitions, and
  separation of attention status from graph progress (`Proofs/Mailbox`)
- canonical descendant visibility, materialization authorization, and
  direct-parent control authority (`DescendantGraph`, #836)
- provider-input narrowing and prompt-layer assembly (`PromptAssembly`,
  #448 / #992): soundness/fixpoint/idempotence/split-stability over the
  permissive transcript, loop-threading validity (the `run_loop_stream`
  chokepoint), the fixed layer order of the assembled request, and the
  provider-input threshold and dynamic per-turn output clamp checked before
  every owned-loop completion dispatch. Later tool turns can grow beyond a
  safe entry input, so `PromptAssembly/Budget.lean` proves the per-turn guard
  and `input + effectiveOutput ≤ context` over the whole dispatch trace and
  drives a generated regression where only a later turn crosses the budget.
  `PromptAssembly/AggregateBudget.lean` separately proves the request-wide
  ledger: every reported completion (including a later-retracted attempt)
  charges monotonically, each dispatch fits its remaining allowance, and an
  exhausted ledger cannot dispatch again. Generated witnesses fence the Rust
  clamp and fail-closed charge classifications.
  `Provider.sanitizeForProvider` models the full three-stage composition
  production runs (`normalize_assistant_content_order ∘
  drop_unpaired_tool_calls ∘ drop_orphaned_tool_results`); the coarser
  row-only `sanitize` is related to it by a *conditional* refinement,
  `project_sanitizeForProvider_eq_sanitize`, which holds on assistant rows
  whose content is nothing but tool calls. The two genuinely differ outside
  that fragment: on an assistant message carrying text alongside a tool call
  that never resolved, production keeps the message and its text while
  `sanitize` drops the row. The model follows production there — see the
  `Proofs/PromptAssembly/Provider.lean` module docstring.

They model daemon storage observations, but do not prove DefraDB storage-engine
correctness, network delivery, provider behavior, UI rendering, external tool
behavior, or host sandbox implementation details. Those are explicit model
boundaries.

**Obligation models without a Rust conformance bridge** (design notes /
local lemmas only — not listed as fenced proven areas):

- P2P backpressure/admission (`Proofs/P2PBackpressure.lean` + TLA
  `P2PBackpressure`): one-wave success-ack backing, pending capacity, and
  strict outbound push-slot release. **Operator mitigation / obligation
  model only** — not a flood-safety fence. The pinned DefraDB implementation
  now admits compact jobs into a bounded queue before spawning worker work,
  defers overflow to its persisted retry ladder, and durably stores
  push-originated pending-DAG registrations before success acknowledgement.
  Those newer multi-wave and restart properties are not yet modeled or
  conformance-fenced here. See
  `boundary.p2p-backpressure.obligation-model`.

## Quick Start

```bash
# Install Lean 4 if needed.
curl https://elan.lean-lang.org/elan-init.sh -sSf | sh -s -- -y

# Build all proofs.
cd crates/gents/proofs
lake build

# Print the Rust conformance contract JSON used by Rust tests.
lake build Proofs.Conformance.Contracts
lake env lean --run Proofs/Conformance/Contracts.lean
```

## What Is Proven

The current proof suite covers twenty practical areas:

1. Request/process/persistence state transitions
2. Daemon storage-observation assumptions that refine persistence
3. Inference-call lifecycle, request linkage, and cancellation terminality
4. Scheduler slot accounting and admission/release
5. Session recovery and retry/reissue semantics
6. Runtime reconcile generation publication and visibility
7. Desired-state apply ordering, reference closure, and apply/runtime field separation
8. Trigger dispatch for manual, schedule, and event-driven tasks
9. Client turn-state derivation from replicated request/response documents
10. Client-shell workflow rules for selection, submission, and transport decoupling
11. Command/tool execution policy: argv prefixes, read-only allowlists,
    disabled-network fail-closed behavior, sandbox selection, and filtered env
12. Tool execution boundary rules: schema/health preflight and MCP retry
    eligibility before future idempotent tool retries are enabled
13. Managed native executor liveness: deadline/cancel transitions signal the
    executor and compose to a terminal timed-out/cancelled tool outcome
14. Backend probe health (#640): the scheduled prober's per-runtime hysteresis
    machine — demotion at exactly K consecutive failures, no flap below K,
    single-success promotion, and effective availability as
    intent ∧ ¬measured-unhealthy
15. Provider-input and token-budget enforcement: prompt assembly and
    sanitization, per-turn context clamps, and the request-wide aggregate
    ledger across tool turns and retracted attempts
16. Canonical descendant graphs: durable pending bridge visibility,
    logical-plus-physical materialization authorization, behavior/deployment/
    await-mode independence, replicated authorization equivalence,
    and the separation between ancestor visibility and direct-parent control
17. Agent self-configuration writes: per-collection writable/protected field
    partitions, patch-merge identity immutability and containment,
    transactional accept/reject totality, and no-lockout recoverability
18. Graph-pipeline publication and terminal runs: whole-graph validation before
    activation, complete-artifact visibility, immutable revision identity,
    active-pointer alignment, atomic running-run plus seed creation, run pinning,
    result-contract-gated success, durable cancellation intent, and single-winner
    terminal transitions
19. Human-attention mailbox: requester/agent identity stamping, owner-only
    dismissal, at-most-one open item per owner/source tuple, fresh occurrence
    allocation after terminal rows, terminal-state immutability, deadline
    expiry, and proof that mailbox close states do not create graph edges
20. Request execution leases (#1341): opaque fresh ownership generations,
    claim deadlines, renewal only from persisted semantic response/tool/
    transcript progress, expiry/drop recovery, matching-generation terminal
    CAS, atomic request/response agreement, and at-most-one winner-owned goal
    continuation/token-charge effect

Separately, **obligation models** (no Rust refinement tests yet):

- **P2P backpressure/admission (#630):** local Lean lemmas + TLA+ hub
  model. Proves what *must* hold for one-wave admission safety/liveness
  (success-ack backing, pending capacity, timeout frees the push
  semaphore). Does **not** prove multi-wave hub stability, Bitswap stall
  recovery, gossip send-loop health, or that the pinned `p2p` crate
  implements these transitions. Operator knobs on `gents server`
  expose the production bounds these models talk about.

The proof boundary matters:

- Lean proves invariants from the point where runtime state is visible to the
  model.
- Rust conformance tests check that persisted DefraDB-visible states refine
  that model (for the fenced areas above — not for obligation models).
- External assumptions such as "DefraDB eventually makes an acked mutation
  visible" or "provider streamed bytes" are not proven here.

## Cross-node TLA+ specs

The `tla/` sibling directory contains TLA+ specifications for cross-node properties beyond per-node Lean coverage. See `tla/README.md`.

Currently:
- `ReversePairing` — control-plane convergence of reverse-pairing subscriptions; first concrete artifact under issue #155's cross-boundary verification strategy.
- `PairingTransport` — connection/install liveness for one directed pairing edge.
- `P2PBackpressure` — bounded hub fan-in/fan-out admission and push-worker liveness obligations for issue #630 (obligation model; not a fleet stability proof).
- `ReplicatedRequestConvergence` — replicated terminal-state convergence under
  persisted bounded owner re-drive plus bounded full replay when a configured
  peer reconnects after a longer partition.

## Why This Matters

The proof work is intended to prevent the class of bugs we have already hit in
practice:

- illegal lifecycle transitions
- recovery/claim races
- scheduler slot leaks
- broken retry/reissue semantics
- reconcile publication races
- apply operations clobbering runtime-owned fields
- disabled or serial triggers accepting work incorrectly
- "ready" or "completed" states that were not actually earned
- clients repairing replicated state from the render path

When the model cannot cover something, that boundary should be named explicitly
and either tested at the Rust boundary or treated as an external assumption.

## Structure

Provider-input assembly for Claude: the body's `system[]` order and tools omission, and SSE tool-block accumulation, are modelled and witnessed; byte framing, usage and headers are Rust-tested.

| File | Contents |
|------|----------|
| `Proofs/Basic.lean` | Shared opaque ids, `Time`, and terminal-state helpers |
| `Proofs/DescendantGraph.lean` | Canonical descendant-edge visibility/read/control authorization, materialization and scope properties (#836) |
| `Proofs/Process.lean` | Process lifecycle model plus executable `Action`, `step?`, and `replay?` |
| `Proofs/Request.lean` | Barrel for request state, transitions, executable semantics, and local properties |
| `Proofs/RequestExecutionLease.lean` | Barrel for the #1341 execution-lease state, executable transitions, stale-owner exclusion, atomic terminal agreement, and bounded terminal effects |
| `Proofs/InferenceCall.lean` | Barrel for inference-call state, transitions, slot accounting, cancellation properties, and in-memory controller bookkeeping (#1001) |
| `Proofs/Persistence.lean` | Persistence lifecycle model plus executable `Action`, `step?`, and `replay?` |
| `Proofs/StorageObservation.lean` | Daemon-visible storage observation model and persistence bridge |
| `Proofs/CrossMachineComposed.lean` | Cross-machine composition and guards; global `WellFormed` (list-level coherence, detached persistence/linkage, unique call ids, no early tools, invFG) established at `initial` and preserved by every transition (#555) |
| `Proofs/Scheduling.lean` | Scheduler/backend slot state |
| `Proofs/Fleet.lean` | Barrel for fleet state, transitions, executable semantics, and slot accounting |
| `Proofs/SessionRecovery.lean` | Retry/reissue model for session-linked requests |
| `Proofs/SessionHydration/` | Exact applied peer/requester/agent route admission plus selected-network verified membership; exact requester/agent/session document selection; terminality; idempotent crash re-drive; pairing non-interference; and resettable session-scoped receiver progress (#1142). Fence: `tests/conformance/session_hydration.rs`. The reconciler consumes the selected set through DefraDB's bounded peer-targeted document pusher. |
| `Proofs/CompletionRetry.lean` | Barrel for per-completion retry state, transitions, executable semantics, and budget/deadline/effects properties |
| `Proofs/RuntimeReconcile.lean` | Barrel for runtime reconcile state, relational transitions, and executable semantics |
| `Proofs/ApplyReconcile.lean` | Barrel for desired-state apply, prefix safety, runtime bridge, and convergence |
| `Proofs/SelfConfig.lean` | Barrel for agent self-configuration patch semantics: field partitions, merge, write step, and guardrails (#654) |
| `Proofs/Triggers.lean` | Barrel for trigger types, dispatch, reachability, serial, latest-only, and lineage proofs |
| `Proofs/Workspace.lean` | Isolated workspace lifecycle, append-only bindings, seal/owner routing, and authority meet |
| `Proofs/Callback.lean` | Callback invocation lifecycle, action-journal prefix, and claim uniqueness |
| `Proofs/Client.lean` | Barrel for client turn-state derivation and client theorems |
| `Proofs/ClientShell.lean` | Barrel for multi-session shell workflow modules |
| `Proofs/CommandPolicy.lean` | Barrel for command/tool execution policy validation, sandbox, env, and safety proofs |
| `Proofs/ToolExecution.lean` | MCP/tool preflight and retry eligibility boundary model |
| `Proofs/ManagedExec.lean` | Barrel for managed native executor state, executable transitions, liveness properties, and tool composition |
| `Proofs/GraphPipeline.lean` | Model-callable graph publication and run lifecycle: validation/materialization gates, active-pointer alignment, immutable revision identity, atomic seed/run start, cancellation suppression, result-commit-gated success, and terminal CAS safety |
| `Proofs/PromptAssembly/` | Provider-view sanitation and prompt assembly, per-turn context budgeting, and the request-wide aggregate token ledger. Fences: generated cases consumed by `agent::loop_stream::tests`. |
| `Proofs/PromptAssembly/ClaudeMap.lean` | Claude tool-name map and Messages provider-input assembly: `splitSystem_partition`, `systemBlocks_head`, `systemBlocks_tail_verbatim`, `toolsField_empty`, `accumulate_ignores_start_when_streamed`, `runStream_*`. Fences: `tests/conformance/prompt_assembly.rs::generated_claude_{map,stream,body}_cases_*`; the identity pin lives in `claude_messages::tests`. |
| `Proofs/P2PBackpressure.lean` | Obligation model (no conformance bridge): success-ack backing, pending-DAG capacity, strict push-slot release on timeout |
| `Proofs/PeerRegistryDiscovery/DirectoryProjection.lean` | Agent directory projection (machine index v1): source-owned membership, foreign-row preservation, idempotent convergence, write-free settled fixpoint, retraction soundness. Fence: `tests/conformance/directory_projection.rs`. |
| `Proofs/Background/` | Subagent/background bridge model: `BridgedState` (parent/child composed pair, `SecondLeg` subagent-vs-tool vocabulary), six bridge transitions, completion-notification/continuation composition, and property modules (B1/B2 projection, B3/B3′ cascade/detach, B4 depth, B5 link symmetry, B6 foreground blocking, B7 budget, INV-UNIQUE, delegation graph) |
| `Proofs/Recovery/` | Recovery sweep contracts (`RecoverySweep`, outcome accounting #693, equivalence-to-uninterrupted), the registered sweep registry, per-collection sweeps including subagent liveness (#465) and the startup restart-disposition classifier (#937), and the startup sweep ordering contract (`StartupOrder.lean`, #1001: the parent-gated inference-call sweep converges only after request repair; #1341 adds startup-and-periodic inference cadence and proves a live-lease startup defers both rows until an expired ordered periodic pass converges them) |
| `Proofs/Session/` | Session queue model: queue sources (`background_completion`, steering), coalesce policy/keys, automated wake-up drain |
| `Proofs/Compaction/` | Transcript reduction (#993) plus durable request-local provider reduction (#1127): canonical provider-view sanitation, pair-safe split correspondence, immutable create-and-compare identity, persist-before-activate, and exact crash restoration. Fences: `tests/conformance/streaming_compaction.rs` and `tests/conformance/durable_reduction.rs`. |
| `Proofs/RenderedCapture.lean` | Persist-before-send at the provider boundary (#840/#523): the five-component capture key, the opaque canonical request, `assembled → durablyCaptured → sent`, and the capture decision (fresh / idempotent / rejected). Proves `sent_implies_durably_captured`, `sent_requires_a_capture_step`, `capture_key_determines_request`, `capture_idempotent`, `capture_rejects_rebinding`, and `capture_failure_blocks_send`. The key's third component is the exact signed request document identity plus provider-call scope, encoded as the injective pair `[request_doc_id, capture_scope]`, because one request runs several completion loops and each starts its turn and attempt counters at zero. Fences: `agent::loop_stream::tests::generated_rendered_capture_cases_fence_persist_before_send` (ordering, driven through the real owned loop), `tests/conformance/rendered_capture.rs` (key identity), and `tests/e2e_runtime/rendered_request_capture.rs` (the persisted payload equals the body a real HTTP backend received, and a failing sink issues zero provider requests). Scope: `boundary.rendered-capture.assembled-request-artifact`, `boundary.rendered-capture.key-encoding-injectivity`. |
| `Proofs/DurableLineage.lean` | DefraDB ingest boundary for request provenance: logical/physical document-edge coherence, root/bridge/control-continuation shapes, per-row rejection that cannot poison later admissible work, steering normalization that clears both halves of the spawn tool edge, and message-before-steering-request publication. The R4C generated steering witness fences these values in Rust conformance tests. |
| `Proofs/Properties/Safety.lean` | Request/process/persistence safety properties S1-S6 |
| `Proofs/Properties/Liveness.lean` | Request/process liveness properties L1-L3 |
| `Proofs/Properties/SchedulingSafety.lean` | Scheduler/fleet safety properties S7-S9 |
| `Proofs/Properties/SchedulingLiveness.lean` | Scheduler/fleet liveness properties |
| `Proofs/Properties/Decidable.lean` | Finite-state exhaustive checks |
| `Proofs/Conformance/Gents.lean` | Mapping from Lean state to Rust/DefraDB state |
| `Proofs/Conformance/Boundaries.lean` | Intentional product policies and external assumptions at the Rust/Lean boundary |
| `Proofs/Conformance/Deviations.lean` | Active unresolved Rust/spec mismatches |
| `Proofs/Conformance/SchedulerConformance.lean` | Scheduler-specific conformance notes |
| `Proofs/Conformance/CoverageLedger.lean` | Checked ledger mapping every emitted conformance domain to a Rust consumer, accepted boundary, or accepted follow-up |
| `Proofs/Conformance/Contracts.lean` | Test-time JSON extraction surface for Rust vocabularies, finite state counts, transition tables, legal/illegal transition pairs, witness rows, ClientShell cases, and the coverage ledger |
| `Proofs/Conformance/ClientShell/Contracts.lean` | Generated finite ClientShell step/projection cases for frontend and desktop shell tests |
| `Proofs/Conformance/Triggers.lean` | Barrel for trigger lifecycle/materialization conformance |

Semantic submodules:

| Barrel | Submodules |
|--------|------------|
| `Proofs.Request` | `State`, `Transition`, `Executable`, `Properties` |
| `Proofs.RequestExecutionLease` | `State`, `Transition`, `Properties` |
| `Proofs.InferenceCall` | `State`, `Transition`, `Executable`, `Properties`, `SlotAccounting`, `ControllerBookkeeping` |
| `Proofs.RuntimeReconcile` | `State`, `Transition`, `Executable` |
| `Proofs.ApplyReconcile` | `Collections`, `Manifest`, `Diff`, `Apply`, `ApplyProperties`, `Prefix`, `RuntimeBridge`, `Convergence` |
| `Proofs.Triggers` | `Types`, `Dispatch`, `Reachability`, `SerialSupport`, `Serial`, `LatestOnly`, `Lineage` |
| `Proofs.Triggers.SerialSupport` | `Counting`, `Preservation` |
| `Proofs.Client` | `Types`, `Lifecycle`, `Terminal`, `Replacement` |
| `Proofs.ClientShell` | `Types`, `Submission`, `Transition`, `Projection`, `Theorems` |
| `Proofs.CommandPolicy` | `Types`, `Validation`, `Sandbox`, `Env`, `Theorems` |
| `Proofs.ToolExecution` | standalone health/schema preflight and retry eligibility model |
| `Proofs.ManagedExec` | `State`, `Transition`, `Executable`, `Properties`, `Composed` |
| `Proofs.BackendHealth` | `State`, `Transition`, `Properties`, `Executable` |
| `Proofs.Fleet` | `State`, `Transition`, `Executable`, `Properties` |
| `Proofs.CompletionRetry` | `State`, `Transition`, `Executable`, `Properties` |
| `Proofs.Conformance.Triggers` | `Lifecycle`, `Materialization`, `Trace` |

The top-level barrel imports remain the stable entry points for downstream code.

Related implementation-facing doc:

- `client-state-machine.md`: client turn observation protocol for app implementers

## Rust Conformance Extraction

Rust conformance tests do not hand-maintain separate Lean parity tables for the
core executable machines. This includes the aggregate-budget dispatch and
charge cases consumed at the owned completion-loop boundary. The test helper
in `src/lean_vocab_test.rs` runs:

```bash
cd crates/gents/proofs
lake build Proofs.Conformance.Contracts
lake env lean --run Proofs/Conformance/Contracts.lean
```

The emitted JSON is printed between `---BEGIN GENTS LEAN CONTRACT JSON---` and
`---END GENTS LEAN CONTRACT JSON---` sentinel lines so Rust can reject unrelated
stdout. It is generated from Lean constructors, `toDefraDB` functions, terminal
predicates, executable `step?` functions, and finite witness contexts. It
currently covers:

- `Request`
- `RequestExecutionLease` one-step and recovery/race traces: 34 one-step cases
  cover live authorization and exact-observation Dead/Superseded revocation.
  `generated_request_execution_lease_cases_fence_production_policy` exercises
  the production authorization seam for begin, progress, finalize, and revocation.
  Two generated provider-EOF cases also fence the production requirement for an
  explicit provider final event before successful turn completion.
  Abstract claim/recovery and database race traces retain explicit coverage
  follow-ups; no standalone Rust reference machine is counted as a consumer.
- `Process`
- `Persistence.failClosed`
- `Persistence.failOpen`
- `StorageObservation.failClosed`
- `StorageObservation.failOpen`
- `RuntimeReconcile`
- `SessionRecovery`
- `InferenceCall`
- `ManagedExec`

`RuntimeReconcile`, `SessionRecovery`, and `ClientShell` also emit deterministic
witness rows. Those rows keep the JSON small while pinning generation
publication/router observation/request admission, session retry guards for
deadline closure, retry budget, latest-request status, duplicate new request
ids, and identity preservation, and shell projection behavior for selection
preservation, stale and matching request observation, stale workflow cleanup,
transport no-op, submit gates, and terminal follow-up allowance. ClientShell
rows are split into a frontend list consumed by the TypeScript chat projection
test and a desktop list consumed by the Rust session-snapshot bridge test.

It also emits `ToolExecution` preflight and retry witness rows, plus the
`ToolRetryDisposition` vocabulary, so Rust tests can reject accidental MCP
preflight or `call_tool` retry drift before idempotency metadata changes.

ManagedExec exports its state vocabulary, legal transition table, and
deadline/cancel liveness witness rows. Rust consumes those contracts in the
managed-exec unit tests and `state_machine_conformance`. A generated native
subprocess inventory also requires `managedExecProcessGroupBoundary`,
process-tree termination, and bounded output drain for `list_files`, `glob`,
`grep`, `bash`, and `bash_unrestricted`.

The Lean `pendingSpawn` state is intentionally one step finer than the Rust
registry surface: Rust records an active executor only after `Command::spawn`
returns a child pid, which corresponds to the model's `running` state. Spawn
failure is still modeled explicitly through `spawnFailed`.

The same JSON includes `coverage_ledger`, maintained in
`Proofs/Conformance/CoverageLedger.lean`. The Rust test
`lean_contract_coverage_ledger_accounts_for_every_emitted_domain` compares that
ledger against every emitted vocabulary domain, state-machine domain,
trigger-case group, runtime witness group, session-recovery witness group,
frontend and desktop ClientShell case groups, ToolExecution case group, and
follow-up hook. Each entry must name a Rust/TypeScript consumer or an accepted
product-boundary/follow-up, so adding a Lean contract also requires making its
runtime coverage explicit.

Coverage consumers are registered in
`crates/gents/tests/support/conformance_consumers.rs`. Add a registry
entry in the same change as a new `consumerCoverage` ledger row: Rust entries
name the package, source file, module path, and `#[test]`/`#[tokio::test]`
function, while TypeScript entries name the app, source file, suite, and test.
The registry is validated by
`lean_contract_coverage_ledger_accounts_for_every_emitted_domain`, so a renamed
or deleted consumer test makes the ledger fail instead of silently leaving a
stale string.

When a Lean vocabulary, terminal partition, action, or legal transition changes,
the generated JSON changes on the next Rust test run. The Rust tests then fail
unless the runtime behavior or the documented product-boundary assertions are
updated to match.

`ToolExecution` exports executable preflight/retry contract cases. Before adding
idempotent MCP retries, extend that Lean model first with the metadata source,
retry budget, and replay/idempotency assumptions, then update the Rust contract
that ties advertised MCP metadata to the widened retry rule.

Future executable `ToolExecution` or other contracts should extend
`Proofs.Conformance.Contracts` and add matching coverage-ledger entries in the
same PR. If the runtime side is intentionally not executable yet, the entry must
point to `Proofs.Conformance.Boundaries` or to an accepted follow-up rather than
leaving the Lean contract advisory-only.

## Core Model

### Layer 1: Process Lifecycle

States:

- `uninitialized`
- `recovering`
- `ready`
- `shuttingDown`
- `shutdown`

Operational meaning:

- `recovering` means the runtime is not yet allowed to accept fresh work
- `ready` means the runtime passed startup validation and can accept work
- `shuttingDown` means no new work should enter and existing work is draining

### Layer 2: Request Lifecycle

States:

- `workspaceBindingPending`
- `pending`
- `claimed`
- `processing`
- `inputRequired`
- `completed`
- `failed`
- `superseded`
- `dead`
- `interrupted`

Operational meaning:

- `workspaceBindingPending` is created bound to a workspace and not yet
  claimable; `bindWorkspace` moves it to `pending` once the WorkspaceBinding
  document is materialized
- `pending` has not been claimed by a backend slot yet
- `claimed` owns admission but has not started inference
- `processing` is actively executing
- `inputRequired` is reserved for a blocked external-input cycle; current Rust
  runtime code does not emit it because autonomous tool calls run inline, and
  active runtime filters exclude it until that loop is modeled
- `dead` is persisted by the request machine only for stale pre-claim TTL
  expiry; post-claim provider failure, retry exhaustion, tool failure, and
  deadline expiry are terminal `failed`. The subagent-liveness recovery sweep
  is the one exception: it terminalizes an expired `claimed`/`processing` child
  as `dead` (`Proofs/Recovery/`), published in the contract as
  `recoveryReachable` under `boundary.request.recovery-sweep-reachable`
- `interrupted` models operator cancellation and releases admission
- terminal states are `completed`, `failed`, `superseded`, `dead`, and `interrupted`

`AgentRequest.lifecycle_state` is the only persisted request state column;
`RequestState.toDefraDB` is its vocabulary. Lean `AdmissionState` is bridged
through runtime-owned `InferenceCall` rows, not a request column.

### Layer 3: Persistence Lifecycle

States:

- `uncommitted`
- `committing`
- `committed`
- `lost`

Operational meaning:

- this layer models whether durable state is actually recorded before terminal
  outcomes are considered valid; Rust currently treats this as an operational
  storage boundary rather than a persisted per-token state document

### Layer 3b: Storage Observation Boundary

States:

- `noMutation`
- `inFlight`
- `successAcknowledged`
- `mutationFailed`
- `staleObserved`
- `readVisible`
- `lostAcknowledged`

Operational meaning:

- this layer models only what the daemon observed around storage writes and
  follow-up reads/events
- a successful mutation ack refines to `PersistenceState.committed`
- a failed mutation does not refine to committed; fail-closed retries from
  uncommitted, while fail-open acknowledges lost output
- stale reads or stale/missing events can happen after a success ack, but the
  model keeps that separate from DefraDB's internal correctness

### Layer 4: Inference Call Lifecycle

States:

- `queued`
- `running`
- `cancelled`
- `completed`
- `failed`

Operational meaning:

- `queued` is persisted before a backend semaphore permit is available
- `queued` does not hold a backend slot
- `running` owns exactly one backend permit and is waiting for or consuming provider work
- `cancelled` records terminal cancellation without provider completion,
  including request interrupts and backend lifecycle cancellation
- `cancelled`, `completed`, and `failed` release backend capacity and do not
  contribute to reconstructed slot counts
- terminal call states are `cancelled`, `completed`, and `failed`

The core request/process/persistence state space remains `9 x 5 x 4 = 180`
states. The call layer adds a separate 5-state persisted lifecycle linked to a
request by `request_id` and bound to a backend by `backend_id`.

## Plain-English Property Summary

### Request/Process Safety

| ID | Property | Why it matters | Theorem |
|----|----------|----------------|---------|
| S1 | Terminal requests stay terminal | A completed, failed, superseded, dead, or interrupted request cannot silently re-enter processing | `terminal_irreversibility` |
| S3 | `progressSeq` never decreases | Clients can treat progress as monotonic and avoid rewind bugs | `progress_monotonic` |
| S4 | Completion cannot be a hidden deadline violation | A request that reaches `completed` did not get there through deadline expiry | `completed_not_deadline_expired`, `deadline_structural_bound` |
| S5 | Recovery blocks claims | New work is not accepted while recovery is still repairing stuck state | `recovery_blocks_claims` |
| S6 | Completion implies persistence | The model does not allow `completed` without a committed durable state | `persistence_before_completion` |

The historical numbering skips `S2` in the current Lean files. There is no
separate theorem labeled `S2` today; the gap is intentional rather than a
missing build artifact.

Request-local field monotonicity uses local labels instead of scheduler safety
numbers: `R-Int` for `interrupt_monotonicity` and `R-TTL` for
`valid_until_monotonicity`.

Deadline and TTL conformance is now explicit on both sides: the request model
requires `ttlOpen` before claim (`claim_requires_ttl_open`,
`claim_with_ttl_bounds_time`), and session retry/reissue requires the source
request deadline to remain open (`reissue_source_deadline_open`,
`reissue_latest_deadline_open`). Rust mirrors this by converting stale
pre-claim requests to `dead/Stale` and by bounding inference retry sleeps and
stream waits by the claimed deadline. Once work is claimed, retry exhaustion
and deadline expiry remain ordinary terminal `failed` outcomes rather than
being reclassified as `dead`.

### Storage Observation Safety

`Proofs/StorageObservation.lean` separates daemon-observed storage facts from
DefraDB correctness. The bridge theorems state that starting a mutation,
observing mutation success, and observing fail-open/fail-closed mutation
failure refine the existing `PersistenceState` transitions
(`begin_refines_persistence`, `success_refines_persistence`,
`failure_failClosed_refines_persistence`, and
`failure_failOpen_refines_persistence`).

Local observation theorems also record the daemon assumptions Rust relies on:
`success_acknowledged_committed`, `mutation_failed_uncommitted`,
`mutation_failed_ne_committed`,
`stale_observation_preserves_success_commit`,
`terminal_write_observed_committed`,
`readYourWrites_visibility_path`, `successful_mutation_eventual_visibility_path`,
`failClosed_failed_mutation_retry_path`, `failOpen_failed_mutation_lost_path`,
`staleRead_eventual_visibility_path`, and `staleEvent_eventual_visibility_path`.

### Liveness taxonomy

"Liveness" theorems in this suite fall into four tiers. **Almost all Lean
results are tier 1.** Reading an `*_eventually_*` or `*_convergence` name as
fair-scheduler or wall-clock progress is a misread (#557).

| Tier | Meaning | Where it lives |
|------|---------|----------------|
| **1. Existential reachability** | There exists a finite legal path from pre to a good post (`∃ post, Trace …` / `∃ actions, …`) | Lean (default): `claimed_eventually_terminal`, `recovery_convergence`, `accepted_work_eventually_releases`, `D1_delivery_convergence`, `streamIdle_eventually_terminal`, … |
| **2. Fair-scheduler liveness** | Under weak/strong fairness, enabled progress steps fire | Primarily `tla/` (WF/SF annotations); Lean does not assume a fair scheduler |
| **3. Bounded phase / measure progress** | Each relevant step decreases a `Nat` measure (or is otherwise step-bounded) — not wall-clock latency | Rare in Lean; example: L1 `phase_change_decreases_measure` (termination measure on phase change). Not distributed N-tick latency. |
| **4. Operational watchdog** | Runtime-enforced deadline/timeout/recovery | Rust (request deadlines, stream idle timeouts, recovery sweeps) — not Lean |

Naming convention going forward: prefer `*_reachable` for pure tier-1 results;
keep historical `*_eventually_*` names for continuity. Cross-node temporal
load (pairing, transport, reverse-pairing) is carried by `tla/`, not by
per-node Lean machines.

### Request/Process Liveness

| ID | Property | Tier | Why it matters | Theorem |
|----|----------|------|----------------|---------|
| L1 | Real current-product phase changes decrease a termination measure | 3 (bounded phase progress) | The model rules out endless phase churn that never gets closer to terminal state | `phase_change_decreases_measure` |
| L2 | Claimed work has a constructive path to terminal state | 1 (`∃ post, Trace`) | A claimed request is not modeled as stuck forever before inference begins | `claimed_eventually_terminal` |
| L3 | Recovery has a same-length terminal-result list | 1′ (list witness, **not** a Trace) | For any stuck list there exists a same-length list of terminal contexts; does *not* prove a transition path from each stuck input to its result | `recovery_convergence` |

### Scheduler Safety and Liveness

Numeric tiers (1–4) apply to **liveness** rows only. Safety invariants are marked
`— (safety)` rather than assigned a liveness tier.

| ID | Property | Tier | Why it matters | Theorem |
|----|----------|------|----------------|---------|
| S7 | Capacity invariants are preserved | — (safety) | Running-slot counts stay within backend limits | `capacity_invariant_preserved`, `reconstructedSlotCount_bounded_by_max_concurrent` |
| S8 | Slot accounting is preserved | — (safety) | Scheduler running counts stay aligned with per-request admission state and persisted running call rows | `slot_accounting_preserved`, `scheduler_running_reconstructed_from_inference_calls` |
| S9 | Terminal work releases capacity; unavailable backends cannot acquire | — (safety) | Slots are not leaked and unrunnable backends do not accept new work | `terminal_implies_released`, `permitDrop_terminalization_not_counted`, `unavailable_blocks_acquire` |
| L | Capacity-available work can acquire | 1 | A waiting request is not artificially blocked when slots exist | `acquire_when_capacity_available` |
| L | Accepted work eventually releases | 1 | The model has a constructive path from accepted work to released capacity | `accepted_work_eventually_releases` |

The scheduling-liveness theorem was intentionally renamed to
`accepted_work_eventually_releases`; the old name used the previous acceptance
vocabulary and is not kept as an alias so the proof-tree hygiene search stays
unambiguous.

`Proofs/InferenceCall/SlotAccounting.lean` is the production-facing slot model:
queued rows contribute zero slots, running rows contribute one slot on their
`backend_id`, terminal rows contribute zero slots, permit-drop terminalization
cannot leave a row counted, and live linked queued/running calls have a model
path to a non-slot-holding terminal state.

`Proofs/InferenceCall/Persistence.lean` refines the database writer guards:
start requires the current row to be queued; completion/failure require running;
cancellation accepts queued/running. A terminal winner preserves its outcome
and stamp against later lifecycle writes, while late provider usage can update
the usage observation without reopening the call. Usage observation does not
itself charge the aggregate budget. These laws support the admission persistence
reverse-race regressions; no new legal lifecycle transitions are introduced.

### Session Recovery

`Proofs/SessionRecovery.lean` proves that retry/reissue behavior preserves the
session boundary:

- reissued requests stay in the same session
- behavior identity is preserved
- latest-request semantics are updated coherently
- retry counts advance monotonically and stay bounded

This is the formal version of "retry creates a new request without corrupting
session history."

### Completion Retry

`Proofs/CompletionRetry.lean` models retry of a single request's completion
inside the owned loop: transport backoff, resample and one-shot repair on
vLLM parse-400s, turn-close-and-continue after effects, and budget/deadline
exhaustion. It is executable in Lean through
`Proofs/CompletionRetry/Executable.lean`, which defines `Action`, `step?`,
`step_sound`, and `transition_complete`; a `preStreamFail` action carries the
observed `FailureClass` and the selected wake time, so `step?` genuinely
consumes both the classification and the fail-fast (overshoot) decision.
`OutputObligation.lean` additionally proves that an unsatisfied durable-write
minimum or incomplete dynamic closed set cannot take the terminal transition,
that exact closed sets complete, and that inconsistent or overfull sets reject.
Successful writes advance monotonically toward completion. Trigger-scoped
obligations follow automated trigger lineage rather than the broader
scheduled-request classification.

The key guarantees (`Proofs/CompletionRetry/Properties.lean`) are:

- **N1** — a re-issued or repaired completion never faces un-accounted tool
  executions, so retry never re-executes tools
  (`n1_reissue_requires_no_open_effects`, carried by `ReissueInv` /
  `reissue_inv_preserved`)
- **N2** — a partial render is retracted only before any effect this turn;
  closing-and-continuing starts a new turn rather than retracting the old one
  (`n2_retract_only_before_effects`)
- **N3** — retry budgets advance monotonically and stay within their ladders,
  and repair happens at most once
  (`n3_budget_monotone_bounded`, `n3_repair_at_most_once`)
- **N4** — every backoff wake fits the claimed deadline, never moves the clock
  backwards, and retry never extends the deadline
  (`n4_backoff_fits_deadline`)
- **N5** — a turn retains at most one rendered instance
  (`n5_rendered_at_most_one`)

This is the formal reason retrying a failed completion is safe: tools are not
re-run, renders are not double-counted, and a retry can neither exceed its
budget nor sleep past its deadline.

### Runtime Reconcile

`Proofs/RuntimeReconcile.lean` is the model for live runtime generation swaps.
It is executable in Lean through `Proofs/RuntimeReconcile/Executable.lean`,
which defines `Action`, `step?`, `replay?`, `step_sound`,
`transition_complete`, `replay_sound`, and `trace_complete`.
The same module exposes executable helper corollaries for generation
monotonicity, coherent preservation, publish well-formedness, request binding,
router observed-generation readiness/liveness, and in-flight retirement safety.

The key guarantees are:

- generations only move forward
- sessions stay pinned by behavior identity, not by mutable default selection
- request acceptance and its owned session projection are one atomic transition
- accepted request identities are monotone and cannot be admitted twice
- publication is separate from resolution
- a generation is not retired while in-flight work still depends on it
- coherent snapshots stay coherent across transitions

This is the formal reason Rust separates resolved snapshots from active
snapshots.

### Apply/Reconcile

`Proofs/ApplyReconcile.lean` models the operator/CLI apply path:

- collection apply order is explicit
- desired-state references must be closed and point to earlier apply ranks
- apply steps write only `DesiredFields`
- runtime-owned `LiveFields` are structurally untouched by apply
- partial apply is modeled as any prefix of the sorted diff
- every well-formed prefix preserves live-owned fields and keeps the full
  desired projection reference-closed; an explicit corollary scopes that to
  already-written referrers
- retrying from any prefix by recomputing diff converges to the same manifest
  desired projection
- after convergence, another diff/apply pass is idempotent
- `t_conv_runnable` is the apply-sensitive result: after a well-formed apply,
  every manifest behavior id is runnable
- `t_conv` and `t_conv_published` are coverage corollaries over the resolved and
  published snapshot carrier sets

This is the formal contract behind manifest diff/apply and per-agent manifest
roots.

### Triggers

`Proofs/Triggers.lean` models the trigger engine and proves:

- disabled triggers cannot accept work
- serial triggers accept at most one active request
- `T3_latest_only_convergence` proves latest-only supersession directly from
  `dispatchStep`; `latestOnlyFireTransition_convergence` is only the abstract
  relation unwrapping lemma
- `T4_lineage_completeness` is the definitional lineage shape theorem for
  `consistentLineage`; the materialization substance is
  `dispatch_materializedTriggerRequest_consistentLineage`, which connects
  actual `dispatch` output, manual lineage-id normalization, and the execution
  origin assigned to the materialized request

`Proofs/Conformance/Triggers.lean` records the Rust/DefraDB shape used by the
runtime trigger implementation. `Proofs/Conformance/Triggers/Contracts.lean`
also emits finite trigger dispatch cases into the Rust conformance JSON. The
in-crate Rust trigger-engine test consumes those generated cases and checks
manual dispatch, schedule/event reachability, tuple-sensitive serial behavior,
latest-only supersession, parallel bypass of in-flight gates, lineage, and
execution-origin projection against the real `TriggerEngine::dispatch` path.
For latest-only cases, Rust seeds the spy materializer with Lean's
`superseded_prior_ids` and asserts those concrete request ids were superseded.

Operational trigger-source behavior remains covered in Rust: DefraDB event
delivery, control-watcher debounce, schedule tick cadence, subscription
reconciliation timing, template parser failures, and persistence writeback
shapes are integration/persistence concerns rather than Lean dispatch facts.

**Projection boundary (#605):** `SystemState.requests` is a single agent's
view — `TriggerKey` is only unique per agent, so the Rust queries that
materialize it scope by the dispatching behavior's `agent_did`, and a
claimed/processing row past its claim deadline (+grace) projects as terminal
(the owning loop enforces the same deadline in-memory, so such a row is a
wedged orphan, not an in-flight run). Both halves are fenced by the
scheduling conformance tests (`serial_gate_is_scoped_by_agent_did`,
`serial_gate_ignores_expired_claims`,
`supersede_only_touches_own_agent_requests`); see the docstrings on
`Proofs/Triggers/Types.lean`'s `AgentRequest.isTerminal` and `SystemState`.

### Backgrounding: subagent bridges and native background tools

Background work comes in two kinds that share the `AgentToolCall` bridge row
vocabulary (`await_mode`, `cancel_policy`) but have deliberately different
durable state and restart outcomes. The models keep them distinct; do not
generalize one lane's fixtures to the other.

| | Background **subagent** (R5) | Native background **tool** (R6) |
|---|---|---|
| Row shape | `await_mode="background"`, `child_request_id` set | `await_mode="background"`, `child_request_id` empty |
| Durable state | Bridge row + child `AgentRequest` (lineage, depth, interrupt flag) + notification message + coalesced wake row | Tool row (result, cancel_cause) + notification message + coalesced wake row |
| Volatile state | Foreground waiter state in the owned loop | Execution registries and the live output ring buffer |
| Restart, live parent | **Leave bridge running**; project when the child terminal is durable | **Interrupt**: terminalize `cancelled`, notification reason `interrupted_on_restart`, one coalesced wake |
| Restart, terminal parent | Preserve the durable bridge for child-terminal projection | **Fail**: terminalize `failed`, notification reason `parent_terminal`, one coalesced wake |
| Completion path | `project_background_subagent_completion` / recovery child-precedence | Native tool completion / startup and periodic ownership recovery |

Model → conformance → Rust bindings:

- **Bridge lifecycle and properties** — `Proofs/Background/*` (B1–B7,
  INV-LINK/UNIQUE/DEPTH, delegation graph) → `r6_backgrounding_cases`,
  `r6_background_theorem_witnesses`, `subagent_delegation_graph_cases`, and
  `r4c_background_work_cases` in the contract JSON → driven by
  `tests/conformance/background.rs` against the real hook and
  `ToolCallLifecycle`.
- **Startup restart disposition (#937)** —
  `Proofs/Recovery/Sweeps/BackgroundRestart.lean` models the classifier in
  `recover_stuck_running_tool_calls` as a total function
  (`restartDisposition`) with exhaustive characterizations
  (`restart_interrupt_iff_native_background_live_parent`,
  `leave_running_iff_preserved_shapes`,
  `notification_iff_terminalized_native_background`,
  `deadline_precedes_restart_interrupt`). The `restart_disposition_cases`
  rows are **computed from the model** and driven through the real
  `ToolCallLifecycle::recover_all` by
  `conformance::generated_restart_disposition_cases_drive_recover_all`,
  including the leave-running rows (background subagent bridge under a live
  parent, detached bridge under an interrupted parent, child-linked bridge
  under a cleanly completed parent) and the notification + coalesced-wake
  side effects with idempotence under a second pass.
- **Recovery sweeps** — `Proofs/Recovery/Sweeps/*` (tool calls, detached
  bridges, subagent liveness #465, terminal-parent owned tools #837, and
  orphaned native-background ownership repair, including volatile execution
  reservations and retryable completion-notification/wake obligations) →
  `recovery_sweep_cases` → `tests/conformance/recovery_sweeps.rs`.
- **Partial output (#937)** — `Proofs/Background/ToolOutput.lean` models the
  three-way `read_tool_output` dispatch (terminal → persisted completion;
  running + live snapshot → ring-buffer tail; running + no snapshot — the
  post-restart shape — → empty), the retained-window paging contract
  (contiguity, eviction detectability, progress, `has_more`), and ring tail
  retention. The `r4c.read_tool_output.dispatch_by_state` witness values and
  the `tool_output_paging_cases` rows are computed from `readDispatch` /
  `readSlice`; the dispatch is driven against the real hook by
  `conformance::generated_read_tool_output_witness_drives_hook_dispatch` and
  the paging rows against `read_retained_output_slice` by
  `background_tools::tests::generated_tool_output_paging_cases_match_slice_function`.
  UTF-8 boundary snapping is a Rust representation detail below the byte
  model.
- **Executable bridge step (#937)** — `Proofs/Background/Executable.lean`
  now executes the bridge-local events on the subagent leg
  (`bridge_complete`, `bridge_failure`, `bridge_cancel_cascade`) with a
  non-vacuous `step_refines_transition`. The `bridge_step_cases` rows are
  computed by running `step` on concrete fixtures (pinned at Lean build time
  by `bridgeStepCases_pinned`) and driven by
  `conformance::generated_bridge_step_cases_drive_bridge_lifecycle` through
  `project_background_subagent_completion` (which owns the complete/failure
  guards — Rust `bridge_complete` itself is a caller-trust boundary) and
  `ToolCallLifecycle::bridge_cancel_cascade`.
- **Native tool leg (boundary)** — the childless R6 row's lifecycle is the
  single-row `ToolExecution` machine (executable via
  `ToolExecution.Executable.step?`, including `background`, `foreground`, and
  `detach` mode/policy actions): Rust `bridge_complete` on a
  `new_background_tool` row refines `ToolExecution.Transition.complete`,
  `bridge_failure(Interrupted)` refines `cancelDuringRun`, and
  `bridge_failure(Dead/Failed)` refines `fail` at the same persistence seam
  (`is_bridge()` admits both kinds). `SecondLeg.tool` in
  `Proofs/Background/Bridge.lean` carries only the terminal-projection
  vocabulary for that leg; the paired `BridgedState` transitions are
  subagent-only by design.
- **Terminal completion → next agent turn (#937)** —
  `Proofs/Background/CompletionContinuation.lean` composes the terminal
  parent-visible tool state, ordinary user-role transcript append, canonical
  `background_completion:<session>` coalesced wake, and FIFO claim. Its
  `claimed_continuation_sees_terminal_notification` theorem makes the
  provider-facing acceptance property explicit: a continuation can only be
  built after notification persistence, and claiming it retains that message
  in the parent transcript. The executable canonical path emits
  `terminal_completion_message_precedes_claimed_continuation` in
  `r6_backgrounding_cases`; the Rust consumer projects a real background
  subagent completion, verifies the message and wake, releases the active
  parent, claims the wake through `DefraWatcher`, and verifies the message is
  still present.
- **Wake coalescing** — `Proofs/Session/*` queue model
  (`background_completion` source, coalesce keys, automated drain) →
  queue-source rows in `r6_backgrounding_cases` and the R4c steering
  witnesses.
- **Cross-node subagent completion/cancel** — `tla/SubagentCompletion.tla`
  and `tla/SubagentCancelPropagation.tla` (subagent lane only; native tool
  backgrounding is single-node and carried by Lean).

### Compaction

`Proofs/Compaction` models transcript reduction — the one place where the
durable transcript and the provider view diverge on purpose, and therefore the
place where "everything persisted can be projected back out" is most at risk.

Before #993 the model was vacuous: `stubMessageKind` was literally
`| .toolResult callId key => .toolResult callId key`, so every preservation
property quantified over `id` and proved that doing nothing preserves meaning.
The current model quantifies over the production policy:

- **`strip`** rewrites a tool result's payload into a pointer stub and touches
  nothing else — never a constructor, never a call id. `strip_idempotent` is
  earned by production recognizing an existing stub rather than re-stubbing it.
- **`providerView = sanitize ∘ strip`** is the single narrowing both the
  compaction writer and the request reader index.
  `strip_sanitize_commute` settles the question #993 raised as unproven —
  stripping first does *not* change which pairs `sanitize` considers orphaned —
  and settling it affirmatively is what licenses reordering the compacted-prefix
  drop past sanitization.
- **`providerView_append`** proves the provider view of a longer history begins
  with the provider view of the shorter one, given the suffix contributes no
  result for a call announced in the prefix. Two checkable sufficient conditions
  are provided; production satisfies the second, since a new request appends its
  user prompt before anything else. `compacted_prefix_correspondence` is the
  theorem the runtime fix rests on: the count the writer records names exactly
  the rows the reader drops.
- **`summarize`** is parameterised over the token-budget split index rather than
  pinning a token function — what must hold is that *whatever* index the budget
  picks, the reducer stays sound. `pairSafeBoundary` retreats that index to the
  nearest turn boundary, and `raw_split_can_orphan` witnesses that the retreat is
  load-bearing: an unadjusted split leaves a tool result whose call was dropped.

`IsValidReducer` carries whole-view coherence (`preservesCoherent`) rather than
pair closure alone, and `ViewCoherent` includes provider-validity and the
assistant-role structure of announcements. Pair closure of a compacted
transcript is not preserved by an arbitrary drop; the previous, narrower fields
sufficed only because the modelled reducer was `id`.

The runtime counterpart of `safeToReduce` is `compaction::safe_to_reduce`,
resolved at session scope — see
`boundary.compaction.safe-to-reduce-session-scope` for the refinement and its
accepted failure mode.

### Client Turn Projection

`Proofs/Client.lean` models how clients derive a turn state from replicated
`AgentRequest` and `AgentResponse` snapshots:

- derivation is total for every non-empty attempt chain
- server lifecycle and response advances do not decrease client rank
- terminal client states line up with effectively terminal server observations
- retry replacement derives from the new tip, with retry restart as the one
  allowed rank decrease

The implementation-facing version is `client-state-machine.md`.

The Codex shim reuses this projection directly. Its adapter only maps the
generic `ClientTurnState` into Codex wire phases and applies the acknowledged
local-interrupt override; request/response precedence, lifecycle monotonicity,
and terminal coherence stay owned by `Proofs/Client.lean`. Generated Codex
conformance rows are evaluated from that composition rather than restating a
parallel Codex-specific state machine.

### Client Shell Workflow

`Proofs/ClientShell.lean` sits above the per-turn projection and models the
desktop-style multi-session shell:

- snapshots never mutate the user's selected deployment/session
- transport health is a non-mutating input
- local session switching is transport-independent
- a new conversation is ephemeral until its first request is submitted
- the submitted request selects the session returned by the runtime
- follow-up submission safety is independent from transport health
- an awaiting submission only retires after the matching tip is observed

`Proofs/Conformance/ClientShell/Contracts.lean` turns those properties into
finite executable cases emitted through `Proofs.Conformance.Contracts`. The
frontend `projectChatShell` test consumes all generated frontend projection
fields, and desktop Rust snapshot tests consume the selected-session subset
with generated observed/preferred request fields. Rows without a selected
session are frontend-owned submit-gate cases. This is the formal guard against
render-time "repair" logic corrupting local UI state.

## Executable Model

The core Lean layers are executable, not just relational. This includes
request, process, persistence, storage observation, session recovery, fleet,
and runtime reconcile:

- `Action`: legal transition vocabulary
- `step?`: executable one-step transition
- `replay?`: bounded trace replay over actions
- soundness/completeness theorems connecting `step?` back to `Transition`

That gives Rust a crisp contract: legal transitions come from Lean, and Rust
must refine them through DB-visible state updates.

## Rust Conformance Strategy

- Lean defines the legal state machines and trace structure.
- Rust tests assert that persisted DefraDB state matches those legal traces.
- Small unit tests still cover isolated pure helpers.
- Binary E2E tests are useful smoke coverage, but they are not the primary
  state-machine proof boundary.

The main conformance files are:

- `crates/gents/tests/state_machine_conformance.rs`
- `crates/gents/src/admission/tests.rs`
- `crates/gents-protocol/src/client_protocol/tests.rs`
- `crates/gents-cli/src/desired_state/tests.rs`
- `Proofs/Conformance/Gents.lean`
- `Proofs/Conformance/Boundaries.lean`
- `Proofs/Conformance/CoverageLedger.lean`
- `Proofs/Conformance/SchedulerConformance.lean`
- `Proofs/Conformance/Triggers.lean`
- `Proofs/Conformance/Deviations.lean`

The Rust/Lean vocabulary checks compare Rust-visible strings against Lean
`toDefraDB` definitions for request lifecycle states, execution origins,
process lifecycle states, runtime reconcile phases, trigger kinds,
inference-call states, and the closed set of system-generated inference-call
terminal reasons.

Trigger conformance additionally consumes generated Lean dispatch cases. These
cases are not a second hand-written table: Lean computes the pre/post request
counts, expected materialization, supersede calls, lineage, and target
non-terminal counts from `dispatch`/`dispatchStep`, and Rust checks the live
engine against those values.

Admission tests also reconstruct held backend slots from persisted
`InferenceCall` rows during contention, queueing, completion, failure,
cancellation, permit-drop, backend-gone, and queue-full paths. These tests
assert that only `call_state = "running"` holds capacity and that the
reconstructed count never exceeds backend `max_concurrent`.

## Decidable Exhaustive Checks

The finite-state checks currently establish:

- generated Request transition cases enumerate the full 9x9 state square as
  legal, illegal, or product-unreachable, with `inputRequired` pairs classified
  as reserved current-product vocabulary
- generated Process transition cases enumerate the full 5x5 state square as
  legal or illegal
- every active current-product non-terminal request state has at least one
  successor; reserved `inputRequired` remains vocabulary-only
- every non-terminal process state has at least one successor
- every non-terminal persistence state has at least one successor
- every non-terminal storage-observation state has at least one successor
- every non-terminal inference-call state has at least one successor
- admission-state invariants line up with request state
- the trailing `#eval` cardinalities print sanity counts for reviewers: 9
  request, 5 process, 4 persistence, 7 storage-observation, 5 call, and 180
  core composed states

These checks are useful because they catch structural model regressions quickly,
even before theorem-level reasoning matters. Rust consumes the generated
Request and Process transition cases directly: legal cases are driven through
deterministic lifecycle/status paths, ordinary illegal cases must have no Rust
writer path, and reserved cases must cite their boundary. The `Fintype` instances
structurally pin the finite vocabularies; the cardinality output is diagnostic
and is not itself a separate proof obligation beyond those instances and the
theorems established in `Proofs/Properties/Decidable.lean`.

## Boundaries And Deviations

`Proofs/Conformance/Boundaries.lean` records intentional product policies,
reserved vocabulary, closed historical items, and external assumptions. These
are not deviations.

Current boundaries:

- `inputRequired` is reserved persisted/client vocabulary. Rust parses it as
  non-terminal client vocabulary if observed, but active runtime lifecycle
  filters use only `pending`, `claimed`, and `processing` until external input
  is modeled.
- `dead` is current product behavior only for stale pre-claim TTL expiry.
  Post-claim provider failure, retry exhaustion, tool failure, and deadline
  expiry remain terminal `failed`.
- Tool failures are permanent until tools expose retry-safe health,
  idempotency, and side-effect metadata.
- Fleet aggregate slot state is reconstructed from `InferenceCall` rows rather
  than persisted as a single `FleetState` document. Only rows with
  `call_state = "running"` hold slots; queued and terminal rows do not.
- `StorageObservation` records daemon-level storage assumptions: success acks,
  failed mutations, stale reads/events, and minimum visibility paths. DefraDB
  storage-engine correctness remains external.

`Proofs/Conformance/Deviations.lean` is reserved only for real unresolved
Rust/spec mismatches. Active deviations are expected to name an accepted failure
mode or a follow-up tracker. Current entries cover live event-source rescan
gaps.

## Known Limitations

### Apply Storage Atomicity

`gents-cli config apply` today is best-effort: if a write fails partway
through the ordered apply sequence, the database is left in a durable prefix and
there is no rollback. `Proofs/ApplyReconcile/Prefix.lean` covers this non-atomic
case: every prefix preserves runtime/live-owned fields, already-written
referrers remain reference-closed, and rerunning `apply` from the prefix
converges to the same manifest desired projection. Production realizes the
model's "recompute diff after a prefix" retry step by rebuilding the live diff
at the start of each `config apply` attempt and applying selected documents via
unique-field upserts or equivalent override writers. The storage assumption is
only that a reported successful mutation is durable before the next retry.

### Interrupted Inference Calls

`Proofs/InferenceCall.lean` models queued, running, cancelled, completed, and
failed call states. `Proofs/CrossMachineComposed.lean` proves
`ComposedState.interrupted_request_cancels_live_linked_call`: when a request is
interrupted, any queued or running call linked by `request_id` has a valid model
path to `cancelled`.

The broader `cancelled` call state is not interrupt-only. Rust also uses it for
backend-gone and controller-drain cases; those are modeled as ordinary terminal
call transitions rather than request-interrupt composition.

Rust covers this bridge at the admission/permit level and with a full
`BehaviorDaemon` mock-stream fixture: mid-stream interruption preserves partial
response content, persists the linked inference call as `cancelled`, and leaves
unrelated concurrent calls live.

System-generated `InferenceCall.failure_reason` values used by admission and
interrupt/drop paths are mirrored by `InferenceCallTerminalReason`; provider
error strings remain open and are not treated as a closed Lean vocabulary.

## What Is Not Proven

These proofs do not establish:

- DefraDB read-your-writes semantics beyond the modeled daemon assumption
- DefraDB CRDT merge or event-delivery correctness
- network reliability
- provider/model correctness
- MCP or external tool availability
- desktop rendering correctness
- OS sandbox behavior
- wall-clock skew / real-time monotonicity (`Time := Nat` is abstract; #558)
- ID-namespace collision freedom or cross-node identity uniqueness for
  `RequestId` / `PeerId` / `AgentDid` collapsed to `Nat` (#558;
  `boundary.model.nat-typed-ids-time`)
- fair-scheduler or bounded-latency temporal liveness for distributed
  delivery (tier 2/3; see § Liveness taxonomy and `tla/`)

Those are handled through explicit assumptions, Rust integration tests,
operational diagnostics, TLA+ specs, or platform-specific tests.
