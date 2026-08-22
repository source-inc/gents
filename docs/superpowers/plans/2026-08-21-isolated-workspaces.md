# Isolated Workspaces Implementation Plan (PR 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the Lean machines and generated conformance witnesses for isolated workspaces and callback invocations — no host filesystem yet.

**Architecture:** Model workspace lifecycle, append-only bindings, seal, owner routing, authority meet, `git_worktree_diff` Git-write denial, callback invocation + per-action journal. Emit JSON witnesses consumed by Rust tests. Follow the Workflow conformance pattern.

**Tech Stack:** Lean 4 (existing proofs package), `gents-lean-contract` JSON snapshot, `crates/gents/tests/conformance`.

**Spec:** `docs/superpowers/specs/2026-08-21-isolated-workspaces-design.md` — especially §6 document model, §7.1–7.4 authority/seal/git, §8 journal/identity, §10 formal model.

## Global Constraints

- Lean → conformance → Rust. Zero `sorry`s.
- Do not emit host paths, remotes, or `git-common-dir` on the logical workspace type.
- Workspace lifecycle is only `Provisioning → Ready | ProvisionFailed`, `Ready → Sealed`, `Sealed → Cleaning → Cleaned`. No Bound / Completed / Failed / Rejected workspace states.
- Bindings are append-only; unique Active ReadWrite; concurrent ReadOnly; Integrate only when Sealed.
- ReadWrite never meets to Unrestricted.
- Journal prefix: action N+1 cannot Execute until N is ResultDocsWritten.
- CallbackResult exists only after Succeeded + result docs.
- `graphql` / DefraDB mutation rules do not apply to this Lean-only slice.
- Gate: `cd crates/gents/proofs && lake build Proofs` and `cargo test -p gents --test conformance --test misc` (the new tests plus snapshot parse).
- Do not implement schemas, host adapters, or WASM in this task.

---

### Task 1: Lean Workspace + Callback machines and conformance witnesses

**Files:**
- Create: `crates/gents/proofs/Proofs/Workspace.lean`
- Create: `crates/gents/proofs/Proofs/Workspace/Types.lean`
- Create: `crates/gents/proofs/Proofs/Workspace/Transition.lean`
- Create: `crates/gents/proofs/Proofs/Workspace/Properties.lean`
- Create: `crates/gents/proofs/Proofs/Workspace/Conformance.lean`
- Create: `crates/gents/proofs/Proofs/Callback.lean`
- Create: `crates/gents/proofs/Proofs/Callback/Types.lean`
- Create: `crates/gents/proofs/Proofs/Callback/Transition.lean`
- Create: `crates/gents/proofs/Proofs/Callback/Properties.lean`
- Create: `crates/gents/proofs/Proofs/Callback/Conformance.lean`
- Create: `crates/gents/proofs/Proofs/Conformance/Contracts/Json/Workspace.lean`
- Create: `crates/gents/proofs/Proofs/Conformance/Contracts/Json/Callback.lean`
- Create: `crates/gents/tests/conformance/workspace_binding.rs`
- Create: `crates/gents/tests/conformance/callback_lifecycle.rs`
- Modify: `crates/gents/proofs/Proofs.lean` (import Workspace + Callback)
- Modify: `crates/gents/proofs/Proofs/Conformance/Contracts/Json/Snapshot.lean` (emit new JSON keys)
- Modify: `crates/gents/proofs/Proofs/Conformance/Contracts/Machines/Catalog.lean` if adding state-machine vocabularies
- Modify: `crates/gents/src/lean_vocab_test/support.rs` (snapshot structs + accessors; `#[serde(default)]` on new fields)
- Modify: `crates/gents/tests/conformance.rs` (mod declarations)
- Modify: `crates/gents/tests/misc.rs` if a snapshot-parse test belongs there (prefer conformance.rs)
- Modify: `crates/gents/proofs/Proofs/Conformance/CoverageLedger.lean` with consumer entries for the new tests
- Modify: `crates/gents/proofs/README.md` — one bullet that these machines exist
- Spec already at: `docs/superpowers/specs/2026-08-21-isolated-workspaces-design.md`

**Interfaces:**
- Consumes: existing `Proofs.CommandPolicy.ExecutionMode` (readOnly < workspaceWrite < unrestricted) if a meet lemma is needed; otherwise define a local `Authority` inductive and a `meet` function with the same ranks.
- Produces: Lean types and `native_decide` witness lists; JSON keys `workspace_cases`, `workspace_binding_cases`, `callback_cases` on the contract snapshot; Rust accessors `lean_workspace_cases()`, `lean_workspace_binding_cases()`, `lean_callback_cases()`.

- [ ] **Step 1: Write Lean types (Workspace)**

In `Proofs/Workspace/Types.lean`, define (names may match these exactly):

```lean
inductive WorkspaceState
  | provisioning | ready | provisionFailed | sealed | cleaning | cleaned
  deriving DecidableEq, Repr

inductive BindingAuthority | readOnly | readWrite | integrate
inductive BindingState | active | released | denied
inductive CreationPolicy | gitWorktreeDiff | isolatedClone

structure IsolatedWorkspace where
  workspaceId : String
  workUnitId : String
  repositoryId : String
  baseSha : String
  branch : String
  creationPolicy : CreationPolicy
  ownerDeploymentId : String
  sealHash : Option String
  state : WorkspaceState

structure WorkspaceBinding where
  bindingId : String
  workspaceId : String
  requestId : String
  authority : BindingAuthority
  deploymentId : String
  sealHash : Option String
  state : BindingState
```

Logical workspace **must not** contain a host path field.

`WorkspaceState.toDefraDB` / `fromDefraDB?` with a round-trip theorem like RequestState.

Terminal workspace states: `provisionFailed`, `cleaned` (and only those as `HasTerminal` if you use that class). `sealed` is not terminal — cleanup still exists.

- [ ] **Step 2: Write Workspace transitions and properties**

Legal transitions only:

- provisioning → ready
- provisioning → provisionFailed
- ready → sealed (requires `sealHash.isSome`)
- sealed → cleaning
- cleaning → cleaned

Binding rules as `Prop`s with `Decidable` instances:

- `ReadWriteOk w b`: `b.authority = readWrite → w.state = ready ∧ b.state = active`
- `UniqueActiveReadWrite`: at most one active readWrite binding per workspaceId
- `ReadOnlyConcurrent`: many active readOnly allowed when `w.state = ready ∨ w.state = sealed`
- `IntegrateOk`: integrate only when `w.state = sealed` and `b.sealHash = w.sealHash`
- `OwnerClaimable deploymentId w`: claimable iff `w.ownerDeploymentId = deploymentId` and state is bindable
- `GitMetadataWriteOk policy authority`: false when `policy = gitWorktreeDiff` and authority is readWrite (worker cannot git-add/commit)
- `AuthorityMeet`: readWrite meets command mode to `workspaceWrite`, never `unrestricted`

Prove the obvious theorems (cases on the inductives; `native_decide` only for finite witness lists). Zero `sorry`s.

- [ ] **Step 3: Write Callback types, journal, transitions**

```lean
inductive InvocationState | pending | claimed | running | succeeded | failed | denied
inductive ActionJournalState | validated | executing | effectObserved | resultDocsWritten

structure ActionJournalEntry where
  index : Nat
  state : ActionJournalState

structure CallbackInvocation where
  invocationId : String
  ownerDeploymentId : String
  state : InvocationState
  journal : List ActionJournalEntry
  resultEmitted : Bool
```

Journal prefix: for all `i`, if entry `i+1` is `executing` or later, entry `i` is `resultDocsWritten`.

`resultEmitted = true` only if `state = succeeded` and every journal entry is `resultDocsWritten`.

Claim uniqueness: at most one claimed/running invocation per `(ownerDeploymentId, invocationId)` — model as a predicate on a list of invocations.

Denied/failed never execute host actions: journal stays empty or all entries remain `validated` without advancing to `executing`. Pick one and prove it; empty journal on Denied is simpler.

Transitions: pending→claimed→running→succeeded|failed|denied; claimed→denied; running→failed.

- [ ] **Step 4: Conformance witness lists with `native_decide`**

Follow `Proofs/Workflow/Conformance.lean`:

Workspace cases covering at least:

- provision success (provisioning→ready)
- provision fail (no bind)
- seal requires hash
- readWrite after sealed is illegal
- second active readWrite is illegal
- two active readOnly after seal is legal
- integrate before seal is illegal
- integrate with mismatched sealHash is illegal
- non-owner deployment cannot claim
- gitWorktreeDiff + readWrite Git-metadata write is illegal
- authority meet readWrite ↛ unrestricted

Callback cases covering at least:

- happy journal prefix
- action 1 executing while action 0 not resultDocsWritten is illegal
- resultEmitted while running is illegal
- resultEmitted on succeeded with complete journal is legal
- denied with empty journal is legal
- denied with executing journal is illegal

Theorems: `workspaceCases.all caseLegalCorrect = true` and same for callback, proved by `native_decide`.

- [ ] **Step 5: JSON + Rust snapshot + tests**

Mirror `Proofs/Conformance/Contracts/Json/Workflow.lean`. Add keys to `snapshotJson` **before the last field**, keep valid JSON.

Rust `LeanContractSnapshot` new fields with `#[serde(default)]`:

```rust
#[serde(default)]
pub(crate) workspace_cases: Vec<LeanWorkspaceCase>,
#[serde(default)]
pub(crate) workspace_binding_cases: Vec<LeanWorkspaceBindingCase>,
#[serde(default)]
pub(crate) callback_cases: Vec<LeanCallbackCase>,
```

Define case structs to match the JSON you emit.

Tests (`tests/conformance/workspace_binding.rs`, `callback_lifecycle.rs`):

- snapshot lists are non-empty
- every case's `legal` flag agrees with a Rust reimplementation of the Lean predicate (copy the predicate; do not call host git)
- required named cases from Step 4 exist

Register mods in `tests/conformance.rs`.

Add CoverageLedger consumer entries pointing at those test function paths (see existing workflow/trigger entries for the string shape).

- [ ] **Step 6: Build and test**

```bash
cd crates/gents/proofs && lake build Proofs
cargo test -p gents --test conformance workspace_binding -- --nocapture
cargo test -p gents --test conformance callback_lifecycle -- --nocapture
```

Also run the snapshot parse path: `cargo test -p gents --test conformance lean_executable_contracts_cover_initial_domains` if it still compiles after snapshot field additions.

Expected: lake success, new tests pass, no `sorry`.

- [ ] **Step 7: Commit**

```bash
git add docs/superpowers/specs/2026-08-21-isolated-workspaces-design.md \
  docs/superpowers/plans/2026-08-21-isolated-workspaces.md \
  crates/gents/proofs crates/gents/src/lean_vocab_test/support.rs \
  crates/gents/tests/conformance crates/gents/tests/conformance.rs
git commit -m "feat: Lean workspace and callback isolation machines (#1133)"
```

Include the spec and this plan in the commit if they are untracked.

Later PRs (schemas, overlay, host executor, WASM, seal, integrator, packs) are specified in the design doc §13 and are **not** this task.
