# Isolated workspaces for parallel write work

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Spec lives with the plan: this document *is* the design. After approval, copy it to `docs/superpowers/specs/2026-08-21-isolated-workspaces-design.md` before implementation.

**Goal:** Give EventTrigger graphs and subagents a runtime-enforced isolated write environment — provisioned before the request starts, bound as the request’s real file-tool root / shell CWD / LSP root, left inspectable for review, and merged only by a typed integrator host action.

**Architecture:** Isolation is three documents, not a path on one row. A work-unit create fires a callback that emits a typed ActionPlan. The host journals granted actions. That produces a replicated `IsolatedWorkspace` identity, a local-only `WorkspacePlacement`, and later a replicated `WorkspaceReceipt`. Downstream requests bind through append-only `WorkspaceBinding` rows. After the writer seals, review/validation/integration all verify the same tree/diff hash. Host execution is deployment-owned and never claim-then-fail on a replica.

**Tech Stack:** Lean 4 + conformance, DefraDB documents (branchable vs local-only split matching the schema audit), existing Wasmtime/Lens fixture pattern, pluggable host adapters, existing `CommandExecutionMode::WorkspaceWrite` (macOS Seatbelt today).

**Spec / issues:** #1133 (WASM callbacks), #728 (AGENTS.md), #834 (coordinator isolation), #378/#832 (orchestration, not this feature). Acceptance: DefensePatchAssignment. The defending-code pack can keep temporary local validation clones until these edges land, then switch patch/validate/review to durable workspace references.

**Global constraints**
- Lean → conformance → Rust for legal transitions, invariants, or provider-input changes. Zero `sorry`s.
- `graphql::escape_graphql_string()` for interpolated GraphQL. Never emit `[]` in DefraDB mutations — emit `null`.
- Gate with `cargo test -p gents` and `cargo check --workspace --all-targets`.
- EventTrigger still fires on **created** only. Do not chain stages on in-place status updates.
- `build_tools` runs once per behavior. Per-request isolation overlays at claim/tool-scope time; it does not mutate `ToolSelection.file_tool_root`.
- Operator `--tool-root` / existing local `WorkspaceRoot` remain the ceiling. Placement paths must sit under that ceiling.
- Replicated rows never carry host paths, remotes-with-credentials, or absolute `git-common-dir` paths.
- Workspace-bound requests are **routed to the owning deployment before claim**, not claimed and failed on a replica.
- `effective command/file policy = behavior policy ∧ workspace authority`. ReadWrite is `WorkspaceWrite`, never Unrestricted.
- Host ActionPlans are journaled per action. Validate-then-execute is not filesystem atomicity.

---

## 1. Two layers, one acceptance case

This is **not** “add worktrees to the maintenance pack.” It is a general execution-environment feature. DefensePatchAssignment is the first graph that is not allowed to cheat.

### General problem

Demo packs already fan out **read** work well: EventTrigger + correlation + `per_group` barriers. Parallel **write** work has no runtime primitive.

Today a write-capable behavior gets one `ToolSelection.file_tool_root`, baked into an `Arc<ToolSet>` at behavior start (`crates/gents/src/tool_surface/build.rs`). `workspace_cwd` only changes the relative **base** (`crates/gents/src/toolset/shared/context.rs`); the sandbox **root** stays fixed. Changing bash CWD does not confine writes: Unrestricted bash can still write `/tmp`, sibling repos, the main checkout, or absolute paths.

The repo-maintenance pack therefore:

- puts the operator ceiling at the *parent* of the source repo
- tells the model, in the prompt, to run `make worktree BRANCH=… DIR=… BASE=…`
- serializes all commits onto one shared tree because package-document arrival is not an execution callback
- never actually rebinds file tools, bash CWD, or LSP to that worktree — the path is prose

That is host orchestration pretending to be model reasoning.

Related general needs, all the same primitive:

- Models should `cwd` inside an isolation root.
- Subagents should inherit or bind a workspace by document id.
- Reviewers attach to the same logical workspace with read-only authority, possibly concurrently.
- Only a typed integrator host action mutates shared Git metadata / trunk.
- Failed/rejected trees stay inspectable. Cleanup is explicit.

### Specific use case (acceptance)

A `DefensePatchAssignment` must:

1. Provision one isolated workspace **before** its triggered `AgentRequest` starts.
2. Make that workspace the request’s **effective file-tool root, shell CWD, and LSP root** via request-scoped overlay — not a path in the prompt.
3. Record immutable **logical** repository id / base SHA / branch / work-unit id / owner `deployment_id` / creation policy on a replicated row; record **local** path, adapter, dirty-base observation, and provisioning state on a non-replicated placement row.
4. Be idempotent across retries/restarts: an existing local target succeeds only when logical identity matches; otherwise hard-fail with no overwrite and no cleanup.
5. Bind downstream patch / validate / review requests by workspace id + append-only binding documents. Reviewers are ReadOnly. Integration is a typed host action, not a shell.
6. Seal after the writer receipt: revoke further write bindings, persist a tree/diff hash. Validation, review, and integration all verify that hash.
7. Freeze instruction provenance from the immutable base revision. Writer-edited `AGENTS.md` is patch data, not new controlling instructions for reviewers.
8. Leave cleanup as a separate explicit lifecycle action.

WASM remains the general planner: user modules emit typed ActionPlans with granted capabilities, fuel/memory/IO limits, and no ambient WASI. Repository-specific creation, artifact cloning, and merge policy stay pluggable host actions.

If DefensePatchAssignment cannot run without prompt-level `git worktree` / `cd` / `git commit`, the general design has failed.

---

## 2. What exists today (Gents)

| Piece | Reality | Gap |
| --- | --- | --- |
| EventTrigger graphs | Document pipelines; `created` only; correlation; `per_group` fan-in | No pre-request host edge |
| `ToolSelection.file_tool_root` | Per-behavior, clamped to operator ceiling, baked at `build_tools` | Cannot differ per request/work-unit |
| `ToolContext` | `root` = sandbox ceiling; `base` = relative-path CWD | `workspace_cwd` only overrides **base** |
| Request `workspace_cwd` | Metadata `codex_shim.cwd` or `workspace_cwd`; installed into `TOOL_RUNTIME_SCOPE` | Shim-only; not a workspace document; not the file-tool root |
| `WorkspaceRoot` | Operator allowlist of local dirs; **not** `@branchable`; never replicates | Ceiling, not a work-unit workspace |
| `CommandExecutionMode::WorkspaceWrite` | macOS Seatbelt `sandbox-exec` with `WRITABLE_ROOT`; off-macOS `workspace_write_sandbox_enforced() == false` and admission fails closed | Must be the ReadWrite workspace meet; not Unrestricted |
| Git argv policy | ReadOnly allowlists `status/diff/show/log/...`; write subcommands are not a separate WorkspaceWrite git policy | Linked worktrees share refs — `git commit` mutates the main repo |
| LSP pool key | `(session, behavior, workspace_root, server, digest)` | Uses behavior tool root, not a bound work-unit |
| AGENTS.md | **Not loaded** (#728) | Must not load live writer-tree instructions for reviewers |
| `spawn_subagent` | `name`, `prompt`, `await_mode`, `deadline` | No isolation / cwd / workspace_id |
| WASM | Lenses: inline bytes, content-addressed `LensModule::from_bytes`, Wasmtime | Transform-only; no ActionPlan, no fuel/signer policy |
| `make worktree` | `scripts/worktree-bootstrap.sh`: git worktree + APFS clonefile of `target/` and proofs `.lake` | Invoked by humans/prompts, not the runtime |
| Schema audit | Shared vs Local vs Projection (`docs/defradb-schema-audit.md`); `WorkspaceRoot` is Local | New types must follow that split |
| Workflow primitives (#378) | `fan_out_and_synthesize` | Does not provision filesystems |

Load-bearing implementation facts:

- `tool_surface/build.rs` resolves `file_tool_root` once per behavior.
- `toolset/shared/context.rs:181` — `workspace_cwd` changes `effective_base` only.
- `toolset/shared/command.rs` — `WorkspaceWrite` is a real sandbox mode; Unrestricted is host-user permissions. `select_sandbox_for_policy` already refuses WorkspaceWrite without Seatbelt.
- `meet_execution_mode` already infimums command modes. Workspace authority must compose through that meet, not bypass it.
- EventTrigger materializer snapshots source fields onto `caused_by_trigger_context`.
- Watcher claim is the wrong place to discover “this workspace is owned elsewhere.” Routing happens at materialization.

---

## 3. What the peers actually do

Steal mechanisms, not process-local architecture. Gents’ control plane is documents + identity + P2P; Codex / Grok / oh-my-pi are process-local CLIs.

### Codex

- CWD is a thread property. Gents already projects this through the Codex shim.
- AGENTS.md walks cwd → project root, concatenates root→cwd, injects as a wrapped user message, 32 KiB budget.
- Trust for linked worktrees resolves to the **main git root** — evidence that a worktree is not an independent Git database.
- Hooks are process-local config.

**Steal:** cwd as first-class request property; instruction discovery bounded by isolation root.
**Do not steal:** loading live mutable instructions as the reviewer contract; session-only state; shell hooks.

### Grok Build

- `spawn_subagent(isolation=worktree)` plus optional cwd; trees preserved after completion.
- `xai-fast-worktree`: CoW clone, dirty-file policy, artifact copy, later a pool.
- Persistent shell cwd; conversation path remapping worktree ↔ root.
- Hooks: ambient shell scripts, fail-open.

**Steal:** CoW + artifact clone as a host adapter; preserve trees after completion; dirty-base observation on the **local** placement.
**Do not steal:** silent worktree on spawn; shell-script hooks; treating CWD change as write confinement.

### oh-my-pi

- Isolation PAL (apfs/btrfs/overlay/rcopy/…). Isolated runs capture patch or branch artifacts.
- Host tools vs agent tools: the agent cannot `git push`; only host tools mutate shared GitHub/git.
- Review is read-only on a PR worktree. Deterministic paths. Isolated agents are torn down at completion; default merge can cherry-pick.

**Steal:** host-action vs agent-action split; reviewer read-only; receipts (patch/base SHA); workers do not integrate.
**Do not steal:** auto-merge on completion; tearing down isolation at success; worker `git commit` on a linked worktree.

### Synthesis

| Need | Codex | Grok | oh-my-pi | Gents (this design) |
| --- | --- | --- | --- | --- |
| Isolation unit | thread cwd | spawn worktree | spawn isolation PAL | replicated `IsolatedWorkspace` + local `WorkspacePlacement` |
| Concurrent use | one session | one spawn | one spawn | append-only `WorkspaceBinding` |
| Write confinement | sandbox policy | OS sandbox | PAL + cwd | `WorkspaceWrite` meet, not Unrestricted |
| Extension | shell hooks | shell hooks | host tools | WASM → ActionPlan → journaled host actions |
| Merge | human | human / optional | auto patch/cherry-pick | typed `integrate_workspace` after **seal** |
| Cleanup | human | preserved | destroy on complete | explicit later action |
| Replication | none | none | none | identity + receipts replicate; placement does not |

---

## 4. Approaches

### A. Prompt-only coordinator (#834 as the whole fix)

**Reject.** Today’s maintenance pack.

### B. Grok-style `spawn_subagent(isolation=worktree)`

**Reject as the primary design.** Triggered tasks never call spawn. No durable identity, no concurrent reviewer bindings, no seal hash, no P2P routing. Spawn may later *create* an `IsolatedWorkspace` internally.

### C. Hard-code worktree provision inside EventTrigger

**Reject as the long-term ABI.** Every new host action becomes another trigger column.

### D. Split identity / placement / binding + ActionPlan host executor + WASM emitters (recommended)

1. **Logical workspace + local placement + append-only bindings** — the product primitive.
2. **Journaled host action executor** — typed, capability-gated, deployment-owned. First actions: provision, seal/receipt, cleanup, integrate.
3. **Callback engine** — bindings that run WASM (or a first-party emitter) to produce ActionPlans. No WASI. `CallbackResult` is created only after required result documents are durable.

DefensePatchAssignment is one graph over those three, not a special case.

---

## 5. Recommended architecture

```text
WorkUnit (DefensePatchAssignment, …)
  │ created
  ▼
CallbackBinding → CallbackInvocation  Pending → Claimed → Running → Succeeded|Failed|Denied
  │ WASM / builtin emitter → ActionPlan (no FS/net/process)
  ▼
Host executor on owning deployment_id only
  │ journal: Validated → Executing N → EffectObserved → ResultDocsWritten
  ▼
IsolatedWorkspace (Ready, replicated, no host path)
WorkspacePlacement (local-only: path, adapter, dirty-base, provisioning)
  │ then CallbackResult created  →  EventTrigger
  ▼
AgentRequest { workspace_id, workspace_authority, workspace_owner_deployment_id }
  │ materialized only on the owner; watcher elsewhere does not claim
  │ WorkspaceBinding created (append-only)
  ▼
Request-scoped overlay
  │ root = placement.path under WorkspaceWrite (or ReadOnly)
  │ cwd  = that path
  │ LSP  = that path
  │ instructions = frozen base-revision manifest, not live tree
  ▼
Writer finishes → WorkspaceReceipt + host seal (tree/diff hash)
IsolatedWorkspace Ready → Sealed
  │ no further ReadWrite bindings
  ▼
Validate / review bindings (ReadOnly, concurrent OK) must match sealed hash
Integrator: typed integrate_workspace host action (not bash against trunk)
Later: explicit cleanup_workspace → Cleaning → Cleaned
```

EventTrigger stays the orchestrator. Callbacks are a new document edge that can run *before* any LLM request.

### Why not run WASM inside EventTrigger

Different lifecycle, ownership, retry, and journal. Callbacks produce documents the trigger engine already understands.

### First-party emitter

ActionPlan is the ABI from day one. A builtin emitter can produce `CreateWorkspace` from work-unit fields. User WASM is the extensible planner. DefensePatchAssignment can ship on the builtin; a fixture `cdylib` proves the WASM path in the same release train.

---

## 6. Document model

Follow the schema audit: Shared/branchable vs Local/non-branchable. Do not overload `WorkspaceRoot` (operator ceiling).

### `IsolatedWorkspace` (`@branchable`) — logical identity only

No host path. No remotes. No `git-common-dir`. Replicates.

| Field | Notes |
| --- | --- |
| `workspace_id` | Unique logical id. Immutable. |
| `work_unit_id` | Assignment / package id. Immutable. |
| `repository_id` | Configured **logical** repo id (see `RepositoryPlacement`). Immutable. Never a URL. |
| `base_sha` | Immutable provision target. |
| `branch` | Immutable logical branch name. |
| `creation_policy` | `git_worktree_diff` (v1 default) or `isolated_clone`. Immutable. |
| `adapter` | Host adapter id: `make_worktree`, `git_worktree`, later CoW. Immutable. |
| `owner_deployment_id` | Opaque host id, **not** an agent principal DID. Immutable. |
| `writer_principal` / `integrator_principal` | Agent DIDs. Reviewer set is not required here; bindings carry authority. |
| `instruction_manifest` | JSON string: paths + hashes of instruction files read from `base_sha` at provision. Immutable. |
| `seal_hash` | Null until Sealed; then the tree and/or captured-diff hash. |
| `lifecycle_state` | See below. |
| `caused_by_invocation_id` | CallbackInvocation that provisioned it. |
| `caused_by_correlation` | Copied from the work-unit spine. |

Lifecycle (Lean-fenced). This is provisioning / sealing / cleanup — **not** active-request counting, **not** work success/rejection:

```text
Provisioning → Ready | ProvisionFailed
Ready → Sealed
Sealed → Cleaning → Cleaned
```

- `Provisioning`: identity durable; no bindings.
- `Ready`: owner has a matching local placement; ReadWrite and ReadOnly bindings allowed.
- `ProvisionFailed`: terminal for provision; no bindings; disk left as-is for inspection; no implicit cleanup.
- `Sealed`: writer receipt exists; `seal_hash` set; **no new ReadWrite bindings**; ReadOnly and Integrate bindings allowed if they carry/verify `seal_hash`.
- `Cleaning → Cleaned`: explicit cleanup only.

Work outcome lives on `WorkspaceReceipt` and review documents. A rejected patch does not move the workspace to a Rejected state.

Do **not** EventTrigger on these status updates. Downstream agents watch **creates**: `CallbackResult`, `WorkspaceReceipt`, review docs.

### `WorkspacePlacement` — local only, **not** `@branchable`

Same posture as `WorkspaceRoot` / `InferenceBackend`: host path, adapter observation, provisioning state. Never replicates.

| Field | Notes |
| --- | --- |
| `workspace_id` | Join to logical row. |
| `deployment_id` | This host. Must equal `IsolatedWorkspace.owner_deployment_id`. |
| `host_path` | Absolute canonical path under operator ceiling / `WorkspaceRoot`. |
| `repository_placement_id` | Local checkout this worktree/clone was created from. |
| `adapter` / `adapter_version` | What actually ran. |
| `dirty_base` | Bool + bounded summary. Observed at provision. |
| `provisioning_state` | Local mirror of provision progress for crash reconcile (path exists, git worktree registered, artifacts cloned, …). |
| `observed_tree_hash` | Last observed tree hash (for seal reconcile). |

Idempotent provision: if `host_path` exists or git already has a worktree there, succeed only when local observation matches `(workspace_id, repository_id, base_sha, branch, work_unit_id)`. Mismatch → `ProvisionFailed`, no overwrite, no cleanup.

### `RepositoryPlacement` — local only, **not** `@branchable`

Maps `repository_id` → this host’s checkout path. Operator-configured. The replicated workspace stores only `repository_id`.

Never derive `repository_id` from a credential-bearing remote or from `git rev-parse --git-common-dir` (absolute, host-specific, sometimes a file pointing at shared metadata).

### `WorkspaceBinding` (`@branchable`) — append-only lease

Concurrent reviewers, retries, and validation requests are normal. Do not squeeze them into a single `Bound` state.

| Field | Notes |
| --- | --- |
| `binding_id` | Immutable. |
| `workspace_id` | Immutable. |
| `request_id` + `request_doc_id` | Immutable. Same dual-id posture as other lineage edges. |
| `authority` | `ReadOnly` \| `ReadWrite` \| `Integrate`. Immutable. |
| `deployment_id` | Owner that must run the request. Immutable. |
| `seal_hash` | Required for bindings created after Sealed; must equal workspace `seal_hash`. |
| `lifecycle_state` | `Active` → `Released` \| `Denied`. Terminal when the request terminals or seal/authority check fails. |

Rules:

- ReadWrite bindings only while workspace is `Ready` (not Sealed, not Provisioning).
- At most one Active ReadWrite binding per workspace (writer exclusivity). Retries release the previous binding first.
- Many Active ReadOnly bindings are allowed (reviewers + validators).
- Integrate bindings only while `Sealed`.
- Materializer creates the binding **before** the request becomes claimable. A replica without matching `deployment_id` never sees a claimable request for that workspace (see §8 routing).

### `WorkspaceReceipt` (`@branchable`) — portable result

| Field | Notes |
| --- | --- |
| `receipt_id` | Immutable. |
| `workspace_id` | Immutable. |
| `produced_by_request_id` / `produced_by_request_doc_id` | Immutable. |
| `kind` | `writer` \| `integrator`. |
| `base_sha` | |
| `seal_hash` | Tree and/or captured-diff hash. For `git_worktree_diff` this is the sealed diff identity, not a worker commit SHA. |
| `head_sha` | Only meaningful for `isolated_clone` (worker commits) or after integrator action. Null for sealed diffs. |
| `changed_files` | JSON string, bounded; spill via #722 if needed. Empty → `null` in GraphQL, not `[]`. |
| `diff_artifact` | Bounded text or content-addressed blob reference. |
| `checks_run` | JSON string. |
| `unresolved_conflicts` | JSON string or null. |
| `integration_instructions` | Text for the integrator. |

Writer completion creates a `writer` receipt; the host then seals. Integrator completion creates an `integrator` receipt. Review success/failure is a review document, not a workspace lifecycle state.

### Callback collections (from #1133, tightened)

- **`CallbackModule`** (`@branchable` config): inline WASM bytes, ABI version, JSON args, content-addressed `module_id` from canonical `(bytes, canonical_args, abi)`, signer/provenance, enabled, resource limits (fuel, memory pages, max input bytes, max output bytes). Desired-state apply/diff/prune.
- **`CallbackBinding`**: source collection / `event_kind: created` / filter, `module_id` **or** `builtin_emitter`, principal DID, `capability_set`, retry policy, `owner_deployment_id`. Filter/source-field projection **must not** include secret-bearing field names (same family as `is_secret_env_name`: KEY/SECRET/TOKEN/PASSWORD).
- **`CallbackInvocation`**: source doc/version, idempotency key, lifecycle `Pending → Claimed → Running → Succeeded | Failed | Denied`, attempts, canonical ActionPlan JSON, **action journal** (see §8), error. Crash-safe like AgentRequest. Claimable only on `owner_deployment_id`.
- **`CallbackResult`**: created **only after** all required result documents for that plan are durable (`IsolatedWorkspace`, `WorkspacePlacement`, and any action-local receipts). This is the EventTrigger edge. Creating it earlier is a spec bug.

Capabilities (WASM cannot mint them): `create_workspace`, `observe_dirty_base`, `clone_artifacts`, `seal_workspace`, `cleanup_workspace`, `integrate_workspace`.

### `AgentRequest` additions

```graphql
workspace_id: String @index @immutable
workspace_authority: String @immutable              # ReadOnly | ReadWrite | Integrate
workspace_owner_deployment_id: String @index @immutable
workspace_seal_hash: String @immutable              # empty/null for pre-seal writer; required post-seal
```

Stamped at materialization. Subagent/internal derived requests inherit unless the spawn explicitly binds another workspace. Unbound requests (`workspace_id` null) keep today’s behavior-level `file_tool_root`.

---

## 7. Request-scoped execution environment

### Overlay, do not rebuild ToolSet

On the **owning** deployment, when a request with `workspace_id` is about to run:

1. Load `IsolatedWorkspace`. Fail closed if missing or not in a bindable state for the requested authority.
2. Load local `WorkspacePlacement` for `(workspace_id, this deployment_id)`. Fail closed if missing (replicas never get here — see routing).
3. Canonicalize `host_path`; require it under operator `--tool-root` and an enabled `WorkspaceRoot` if that allowlist is in use.
4. If workspace is `Sealed`, require `request.workspace_seal_hash == workspace.seal_hash == placement.observed_tree_hash` (or the captured-diff hash, depending on creation policy). Mismatch → deny, do not run.
5. Ensure an Active `WorkspaceBinding` exists for this request. If materializer already wrote it, reuse; never create a ReadWrite binding on a Sealed workspace.
6. Install into `TOOL_RUNTIME_SCOPE`:
   - `workspace_root` = placement path (new; today only `workspace_cwd` exists)
   - `workspace_cwd` = that path (or persisted chdir under it)
   - `workspace_authority`
7. File tools, bash, native fs-runner, and LSP use `workspace_root` as `ToolContext.root`.
8. Apply the authority meet (§7.1).

### 7.1 Authority meet

```text
effective file mode    = meet(behavior file mode, authority_file(authority))
effective command mode = meet(behavior command mode, authority_command(authority))
```

using existing `meet_execution_mode` ranks: ReadOnly < WorkspaceWrite < Unrestricted.

| Authority | File tools | Command mode | Git metadata | Host actions |
| --- | --- | --- | --- | --- |
| ReadOnly | ReadOnly | ReadOnly | no | none |
| ReadWrite | ReadWrite rooted at placement | **WorkspaceWrite** rooted at placement | see creation policy | none |
| Integrate | ReadOnly on the sealed tree (inspect) | ReadOnly | **none via bash** | `integrate_workspace` only |

**ReadWrite never meets to Unrestricted.** A behavior configured `bash_mode: Unrestricted` / `command_execution_policy: unrestricted` still becomes WorkspaceWrite for a ReadWrite workspace binding. Changing CWD is not confinement; Seatbelt `WRITABLE_ROOT` (or a future Linux equivalent) is.

**Integrate never means unrestricted shell against trunk.** Trunk/source checkout is outside the worker root. Integrate is a typed host action with its own adapter (merge / rebase / cherry-pick / apply-diff).

### 7.2 Platform restriction for WorkspaceWrite

Today:

- macOS: `/usr/bin/sandbox-exec` implements WorkspaceWrite (`docs/macos-bash-sandbox.md`, `select_sandbox_for_policy`).
- non-macOS: `workspace_write_sandbox_enforced() == false`; WorkspaceWrite admission already bails.

v1 of ReadWrite workspace bindings **keeps that fail-closed behavior**. Do not admit a ReadWrite `WorkspaceBinding` on a host that cannot enforce WorkspaceWrite. Linux landlock/bwrap is an explicit later PR, not a silent Unrestricted fallback.

File-tool writes (`write_file` / `edit_file`) already path-confine via `ToolContext.ensure_allowed`. That is necessary and not sufficient for bash.

### 7.3 Creation policy and whether workers may commit

Linked git worktrees share object/ref storage with the main repository. `git add` / `git commit` in the worktree **mutates shared Git metadata**, which contradicts integrator-only Git mutation.

Two honest modes:

| Policy | Git database | Worker file edits | Worker `git add/commit` | Seal artifact |
| --- | --- | --- | --- | --- |
| `git_worktree_diff` (v1, defending-code) | shared (linked worktree) | yes, under WorkspaceWrite | **denied** (argv policy + no integrate cap) | host captures diff / tree hash; no worker commit |
| `isolated_clone` (later) | separate `.git` | yes | allowed inside the clone | worker commits are local to the clone; integrator still applies via host action |

DefensePatchAssignment uses **`git_worktree_diff`**. The integrator consumes the sealed diff receipt. `make worktree` remains a valid **adapter** for creating the linked tree and cloning `target/` + `.lake`; it does not grant the worker Git-write.

Extend command policy for WorkspaceWrite + `git_worktree_diff` so `git add`, `git commit`, `git merge`, `git rebase`, `git push`, `git update-ref`, `git symbolic-ref`, etc. are denials. Read git (`status`, `diff`, `log`, `rev-parse`) stays allowed.

### 7.4 Seal before review

After the writer request terminals successfully:

1. Host `seal_workspace` action: freeze the tree, compute `seal_hash` (tree hash and/or canonical diff identity), persist on `IsolatedWorkspace` and the writer `WorkspaceReceipt`, update placement `observed_tree_hash`.
2. Release the writer’s ReadWrite binding.
3. Transition `Ready → Sealed`. Further ReadWrite bindings are illegal.
4. Every subsequent ReadOnly / Integrate binding copies `seal_hash` onto the request and re-verifies it at start (and before integrate). If the placement hash has drifted, fail closed — a reviewed workspace must not change between review and integration.

A writer failure does not seal. The workspace may remain `Ready` for a retry (after the failed ReadWrite binding is Released) or be left for inspection and explicit cleanup. It does not become a fake “Rejected” workspace state.

### 7.5 Frozen instruction provenance (#728, tightened)

Do **not** load mutable `AGENTS.md` from the writer’s modified tree as controlling instructions for anyone after provision.

At provision, from the **immutable `base_sha`** (the source checkout / blob at that revision, not the live worktree):

- discover instruction files (`AGENTS.override.md`, `AGENTS.md`, configured fallbacks) at the repo root (and cwd-within-root if we later persist chdir — still against base_sha blobs)
- hash them, store `instruction_manifest` on `IsolatedWorkspace`

Prompt assembly for **every** workspace-bound request (writer, validator, reviewer, integrator) injects that frozen manifest into the history-stripped `<context>` message.

If the writer edits `AGENTS.md`, that is patch data in the diff/receipt. Reviewers see it as a changed file, not as new system-like instructions. This is the load-bearing difference from Codex’s live walk.

Unbound requests (no `workspace_id`) may still do the live cwd→tool-root walk from #728.

### 7.6 Models changing cwd

Persist cwd on the request, always under `workspace_root`. Explicit `chdir` tool in v1. Escape is a policy denial. Unbound requests clamp to behavior `file_tool_root`.

### 7.7 LSP

Pool key already includes `workspace_root`. Bound requests pass the placement path so a reviewer and a writer on different trees do not share rust-analyzer. Digest already hashes effective tool root / command constraints.

---

## 8. WASM callbacks, journaled host execution, identity

### WASM contract

Mirror lenses (`crates/gents-lenses/fixture_*`, `LensModule::from_bytes`):

- Fixture crate: `cdylib` + host-testable `rlib`.
- Stable `module_id` from bytes + **canonical JSON args** + ABI (deterministic serialization: sorted object keys, no NaN, no host paths).
- Wasmtime, **no WASI**.
- Resource limits on the module/binding: fuel, memory pages, max input bytes, max output bytes. Exceed → `Denied`, no host actions.
- Signer/provenance required; desired-state apply validates signer policy (only trusted principals may install modules).
- Input: source document JSON **after secret-field stripping**, binding args, granted capability set, `deployment_id` **not** included as a confused-deputy host path.
- Output: ActionPlan JSON, schema-validated. Unknown action types → entire plan `Denied`.

WASM is a planner. If it cannot express an action in the typed plan, it cannot do it.

### ActionPlan (v1)

```json
{
  "abi": 1,
  "actions": [
    {
      "type": "create_workspace",
      "work_unit_id": "…",
      "repository_id": "…",
      "base_sha": "…",
      "branch": "…",
      "creation_policy": "git_worktree_diff",
      "adapter": "make_worktree",
      "clone_artifacts": ["target/", "crates/gents/proofs/.lake"]
    }
  ]
}
```

No destination absolute path in the plan. The host chooses a deterministic child of the operator workspace parent from `(workspace_id, branch)` after looking up `RepositoryPlacement`. Hints, if added later, are relative and rejected outside the ceiling.

Later types: `seal_workspace`, `cleanup_workspace`, `integrate_workspace`.

### Journaled host execution (not “atomic”)

Validate the whole plan against capabilities and operator policy **before** the first action. That is validation atomicity, not filesystem atomicity.

Each action has a durable journal entry on `CallbackInvocation`:

```text
Validated → Executing → EffectObserved → ResultDocsWritten
```

- **Validated:** plan accepted; this action is next.
- **Executing:** host started the adapter (may have crashed mid-command).
- **EffectObserved:** host looked at the world (path exists, git worktree list, tree hash, adapter-specific markers) and reconciled it with the idempotency key. Mismatch identity → fail the invocation, **do not** delete or overwrite.
- **ResultDocsWritten:** the documents that action must persist are durable (`IsolatedWorkspace` + `WorkspacePlacement` for create; receipt + `seal_hash` for seal; etc.).

Recovery: if the process dies in `Executing`, restart **observes** the existing effect and either completes `EffectObserved` + docs or fails closed. Never blindly re-run a create against a path.

`CallbackResult` is written only when every action is `ResultDocsWritten` (or the invocation is terminal Failed/Denied with no partial result edge). EventTrigger must not fire on a half-provisioned workspace.

Prefix semantics: action N+1 does not enter `Executing` until action N is `ResultDocsWritten`.

### Identity and ownership

- **`deployment_id`:** opaque, stable host identifier. Distinct from any agent principal DID. Do not reuse `agent_did` / template `node.node_did` as workspace owner. A local `HostDeployment` (or equivalent) record holds this id; it is what placements and bindings key on.
- **`repository_id`:** operator-configured logical id. Local `RepositoryPlacement` maps it to a checkout. Never a remote URL (credentials, rewrite rules, mirrors).
- **Routing before claim:** materializer of a workspace-bound `AgentRequest` stamps `workspace_owner_deployment_id` and only that deployment’s watcher is eligible to claim. Other deployments may replicate the row for audit; they do not claim and then fail. Same spirit as `(did, behavior)` single-deployment, implemented as an explicit owner field rather than an after-the-fact check.
- CallbackInvocation claim uses the same owner field.

### Secret-bearing source fields

Callback and trigger projections that feed WASM or ActionPlans drop or deny fields whose names match the existing secret-env heuristic. Bindings that require such a field fail closed at apply time.

---

## 9. Authority, integration, cleanup

| Role | Binding authority | What they can do |
| --- | --- | --- |
| Writer | ReadWrite, only while `Ready` | Edit files under placement via file tools + WorkspaceWrite bash; **no** `git add/commit` in `git_worktree_diff`; no integrate action |
| Validator / reviewer | ReadOnly, `Sealed`, concurrent | Read the sealed tree; cannot write, stage, commit, or merge |
| Integrator | Integrate, `Sealed` | Typed `integrate_workspace` host action only (apply sealed diff / cherry-pick / merge-to-trunk per adapter policy) |

Cleanup:

- Never implicit on request terminal, review reject, or process exit.
- Explicit `cleanup_workspace` after operator/integrator ack.
- `ProvisionFailed` and sealed-but-rejected work still occupy disk until cleanup.

---

## 10. Formal model

New Lean machines, fenced like Triggers / Request / ToolCall. Zero `sorry`s.

### `Proofs/Callback/`

- Invocation lifecycle.
- Claim uniqueness per `(owner_deployment_id, invocation_id)`.
- Journal prefix: action N+1 not executing until N’s result docs are written.
- Recovery from Executing observes effects; identity mismatch cannot delete.
- Denied (capability, limits, validation, secret fields) never executes host actions.
- `CallbackResult` exists only in the Succeeded case after result docs.

### `Proofs/Workspace/`

- Workspace lifecycle as in §6 (no Bound, no Completed/Failed/Rejected as workspace states).
- Bindings: ReadWrite only in Ready; unique Active ReadWrite; ReadOnly concurrent; Integrate only in Sealed; seal_hash agreement.
- Request with `workspace_id` has effective root = that workspace’s **placement on the owner** (abstract path id in the model).
- Authority meet: ReadWrite ↛ Unrestricted; Integrate is not a bash write.
- `git_worktree_diff` worker cannot take Git-metadata write transitions.
- Cleanup is a separate action; Sealed ↛ Cleaned.
- Owner routing: a request with `workspace_owner_deployment_id ≠ local` is not claimable.

### PromptAssembly / ToolPolicy / Lsp

- Frozen `instruction_manifest` enters through `assembleWithContext`; tail-order and strip-from-replayed-history still hold. Live-tree AGENTS.md is not in the provider input for bound requests.
- ToolPolicy meet includes workspace authority.
- LSP pool identity includes placement root.

Conformance: `tests/conformance/callback_lifecycle.rs`, `tests/conformance/workspace_binding.rs`, plus host-idempotency filesystem tests (match vs mismatch existing target).

---

## 11. Acceptance graph: DefensePatchAssignment

External harness: `sourcenetwork/defending-code-reference-harness`. Until this lands, that pack may keep temporary local validation clones. After, patch/validate/review edges bind durable workspace references.

```text
DefensePatchAssignment created
  → CallbackBinding (builtin or fixture WASM)
      → CreateWorkspace ActionPlan (git_worktree_diff + make_worktree adapter)
      → journaled host provision
      → IsolatedWorkspace Ready + local WorkspacePlacement + CallbackResult
  → EventTrigger patch-execute (source: CallbackResult)
      → AgentRequest routed to owner_deployment_id
      → WorkspaceBinding ReadWrite
      → worker edits files (WorkspaceWrite); git commit denied
      → WorkspaceReceipt (writer) + seal_workspace → Sealed
  → EventTrigger patch-validate (source: WorkspaceReceipt)
      → ReadOnly bindings, seal_hash verified, frozen instruction_manifest
  → EventTrigger patch-review (per_group if N reviewers)
      → concurrent ReadOnly bindings, same seal_hash
  → EventTrigger integrate (source: review-approved doc)
      → Integrate binding + typed integrate_workspace (apply sealed diff to trunk)
  → later: cleanup_workspace
```

Must-hold checks (automated):

- Worker file write outside placement path is denied.
- Worker Unrestricted bash is not admitted; WorkspaceWrite Seatbelt root is the placement (macOS). Off-macOS ReadWrite binding is refused until Linux containment exists.
- `git commit` / `git add` in the worker is denied (`git_worktree_diff`).
- `pwd` / relative file tools / LSP root are the placement path.
- Provider input contains frozen base-revision instructions, not a writer-modified `AGENTS.md` as controlling text.
- A second validator request can bind concurrently; a second writer cannot while the first ReadWrite is Active; no writer after Sealed.
- Review and integrate fail closed if placement hash ≠ `seal_hash`.
- Restart during provision observes existing effect; mismatch identity does not overwrite.
- Replica with replicated `IsolatedWorkspace` does not claim the writer request and does not run `git worktree add`.
- Disk remains after writer failure / review reject until explicit cleanup.

The maintenance pack should then drop prompt-level `make worktree` and bind execute/review the same way.

---

## 12. Subagents (not v1-critical for DefensePatchAssignment)

`spawn_subagent` today: `{ name, prompt, await_mode, deadline }`.

Add `workspace: inherit | { id } | provision { policy }`:

- Default when parent has `workspace_id`: **inherit** (same workspace, authority infimum — child cannot outrank parent).
- `{ id }`: bind an existing Ready/Sealed workspace the caller may use.
- `provision`: create a child work-unit + host CreateWorkspace, then spawn. This is how Grok `isolation=worktree` is expressed without a side channel.

v1 of DefensePatchAssignment does not need spawn provision. Do not silently create worktrees inside spawn.

#834 disjoint path allowlists on a shared index remain later.

---

## 13. Implementation slices (PR plan)

Each PR independently reviewable. Lean/conformance before runtime where legality changes.

### PR 1 — Lean + conformance: workspace, bindings, callback journal

- `Proofs/Workspace/*`, `Proofs/Callback/*`, generated contract JSON, `tests/conformance/workspace_binding.rs`, `callback_lifecycle.rs`.
- Model: split identity/placement, binding rules, seal, owner routing, journal prefix, authority meet, `git_worktree_diff` Git-write denial.
- Depends on: nothing.

### PR 2 — Schemas

- New: `isolated_workspace.graphql` (`@branchable`), `workspace_placement.graphql` (local), `repository_placement.graphql` (local), `workspace_binding.graphql` (`@branchable`), `workspace_receipt.graphql` (`@branchable`), callback module/binding/invocation/result.
- `agent_request.graphql`: `workspace_id`, `workspace_authority`, `workspace_owner_deployment_id`, `workspace_seal_hash`.
- Opaque `deployment_id` on a local host-deployment record if one does not exist yet.
- Breaking store cut (same posture as correlation): no in-place backfill.
- Depends on: PR 1 types.

### PR 3 — Request-scoped ToolContext overlay + authority meet

- `tool_call_lifecycle/runtime.rs` (`workspace_root`, authority), `agent/daemon/inference.rs`, `toolset/shared/context.rs`, `native_runner.rs`, command-policy meet with workspace authority, tests.
- Bound ReadWrite → WorkspaceWrite at placement; ReadOnly denies writes; Unrestricted is unreachable for bound requests.
- Fail closed when WorkspaceWrite cannot be enforced (current non-macOS behavior).
- Unbound requests unchanged.
- Depends on: PR 2.

### PR 4 — Host executor: CreateWorkspace, journaled, `git_worktree_diff`

- `crates/gents/src/workspace/` adapters (`make_worktree`, `git_worktree`), placement writes, dirty-base observation, existing-target match/mismatch, journal recover-from-Executing, git argv denials for add/commit under this policy.
- Builtin emitter from structured fields. No WASM yet.
- Depends on: PR 1–2.

### PR 5 — Callback engine + owner routing

- `crates/gents/src/callback/` scan/claim/run, desired-state apply, recovery sweep.
- `CallbackResult` only after result docs durable.
- Watcher/materializer: workspace-bound requests claimable only on `workspace_owner_deployment_id`.
- Depends on: PR 4.

### PR 6 — WASM planner

- Fixture crate `crates/gents-callbacks/fixture_create_workspace/`, Wasmtime invoke, module_id stability, fuel/memory/IO limits, signer policy, secret-field stripping, denial paths.
- Depends on: PR 5.

### PR 7 — Writer receipt, seal, frozen instructions

- Writer terminal → `WorkspaceReceipt` + `seal_workspace` journaled action → `Sealed`.
- Unique Active ReadWrite; no ReadWrite after Sealed.
- `instruction_manifest` captured at provision from `base_sha`; prompt assembly uses it for bound requests.
- Depends on: PR 3–5.

### PR 8 — Concurrent ReadOnly bindings + integrator host action

- Validators/reviewers bind concurrently with `seal_hash` check.
- `integrate_workspace` typed action (apply sealed diff / cherry-pick / merge) — no bash against trunk.
- Explicit `cleanup_workspace`.
- Depends on: PR 7.

### PR 9 — Unbound AGENTS.md live walk (#728 remainder)

- Live cwd→tool-root discovery **only** for requests without `workspace_id`. Bound requests already use the frozen manifest from PR 7.
- Depends on: PR 3, 7.

### PR 10 — Acceptance pack + maintenance-pack migration

- Pack modeling DefensePatchAssignment; defending-code pack switches patch/validate/review to workspace references.
- Rewrite `demo/repo-maintenance` execute/review off prompt `make worktree`.
- Depends on: PR 5–8 (WASM fixture optional if builtin emitter covers the pack).

### PR 11 — `spawn_subagent` workspace parameter

- inherit / bind-id / provision. Default inherit, authority infimum.
- Depends on: PR 3–5.

### PR 12 (later, not v1 acceptance) — Linux WorkspaceWrite + `isolated_clone`

- Landlock/bwrap (or equivalent) so ReadWrite bindings can admit off macOS.
- Separate-git-db clone mode if a work-unit truly needs worker commits.

---

## 14. Testing

- Lean + generated conformance for lifecycles, binding rules, seal_hash agreement, owner routing, journal prefix, idempotency.
- Unit: existing-target match vs mismatch; dirty-base on placement not on replicated row; ceiling escape; ReadWrite↛Unrestricted; `git commit` denied in `git_worktree_diff`; seal drift fails review; frozen vs live AGENTS.md.
- Integration: callback → placement → triggered request overlay (file write, bash `pwd`, LSP root, Seatbelt WRITABLE_ROOT).
- Crash: kill during Executing; restart observes effect; no overwrite.
- Concurrency: two ReadOnly bindings Active; second ReadWrite denied; post-seal ReadWrite denied.
- Routing: non-owner watcher does not claim.
- Pack: DefensePatchAssignment graph; maintenance execute no longer shells out to `make worktree` from the model.
- Gate: `cargo test -p gents` and `cargo check --workspace --all-targets`.

---

## 15. Non-goals (this program)

- Auto-merge to trunk on worker success.
- Ambient WASI or arbitrary shell callbacks.
- Mutating `ToolSelection.file_tool_root` per request.
- EventTrigger-on-status-update.
- Putting `host_path` on a `@branchable` collection.
- A single Bound workspace state or encoding review outcome as workspace lifecycle.
- Unrestricted bash as “ReadWrite.”
- Worker commits on linked worktrees.
- Live writer-tree `AGENTS.md` as reviewer instructions.
- Claiming a workspace-bound request on a non-owner and then failing.
- Grok worktree pool / Btrfs snapshot (later adapter).
- Linux WorkspaceWrite in v1 (fail closed; PR 12).
- `isolated_clone` in v1 (PR 12).
- #834 shared-index disjoint path allowlists.
- #378 new workflow primitives.

---

## 16. Key decisions

1. **Split logical workspace from local placement.** Replicated rows never carry host paths.
2. **Append-only `WorkspaceBinding` for concurrent use.** Workspace lifecycle is provision / seal / cleanup only.
3. **ReadWrite meets to `WorkspaceWrite`, never Unrestricted.** Integrate is a typed host action. v1 ReadWrite requires an enforceable sandbox (macOS Seatbelt); otherwise fail closed.
4. **v1 creation policy is `git_worktree_diff`.** Workers edit files; they do not commit. Integrator applies the sealed diff.
5. **Seal before review.** `seal_hash` is the contract between writer, validators, reviewers, and integrator.
6. **Instruction provenance is frozen at `base_sha`.** Writer-edited instruction files are patch data.
7. **Host execution is journaled per action.** `CallbackResult` only after result documents are durable.
8. **Opaque `deployment_id` + logical `repository_id`.** Route to owner before claim. No credential-bearing remotes in replicated identity.
9. **WASM is a resource-limited planner** (fuel, memory, IO, signer, canonical JSON, no secret fields). Host adapters do creation, clone, seal, merge, cleanup.
10. **Builtin emitter shares the ActionPlan ABI** so DefensePatchAssignment is not blocked on user blobs.
11. **Existing `WorkspaceRoot` stays the local operator ceiling.**

---

## 17. Open questions (now mostly closed)

Implementers follow these defaults:

1. **Store cut** for `AgentRequest` workspace fields: breaking cut, no backfill (correlation posture).
2. **Destination path:** host chooses a deterministic child of the operator workspace parent from `(workspace_id, branch)` via `RepositoryPlacement`. No absolute path in ActionPlan.
3. **Receipt size:** bounded JSON on the row; spill through #722 if needed.
4. **chdir:** explicit `chdir` tool in v1.
5. **Builtin vs WASM for the first pack:** builtin emitter; fixture WASM in tests.
6. **Linux containment / `isolated_clone`:** not v1; tracked as PR 12.
7. **`deployment_id` minting:** new local host-deployment record; do not alias agent principal DID even if a node identity DID exists.

No remaining product forks for v1. DefensePatchAssignment is the acceptance test; the defending-code pack keeps temporary local clones until PR 10 switches those edges over.
