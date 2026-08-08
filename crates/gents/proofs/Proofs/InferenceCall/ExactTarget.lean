import Proofs.InferenceCall.Executable

/-!
# Exact-target persistence for inference calls

`InferenceCall.Transition` defines which lifecycle edges are legal.  This file
models the storage fence required to make those edges true of a physical
DefraDB document: a write names the `_docID` returned by creation and the state
the writer observed.  A successful write changes only that document and only
through `InferenceCall.step?`.

Logical `callId` is deliberately absent from `FencedUpdate`.  Two physical
documents may carry the same logical values after replication; a correctness
write must not fan out across them.
-/

namespace InferenceCall

abbrev DocumentId := Nat
abbrev OwnerToken := Nat
abbrev Epoch := Nat

structure PersistedCall where
  call : InferenceCall
  owner : OwnerToken
  epoch : Epoch
  deriving Repr

/-- A database projection indexed by physical DefraDB document identity. -/
abbrev Store := DocumentId → Option PersistedCall

/-- The compare-and-set input for one existing `InferenceCall` document. -/
structure FencedUpdate where
  target : DocumentId
  expectedState : InferenceCallState
  expectedOwner : OwnerToken
  expectedEpoch : Epoch
  action : Action
  deriving DecidableEq, Repr

/-- Apply one exact-document, expected-state-fenced lifecycle write.

`none` means the physical document was absent, its observed state did not match
the writer's expected source state, or the requested lifecycle edge was
illegal.  In all three cases the database must remain unchanged.
-/
def applyFenced (store : Store) (write : FencedUpdate) : Option Store :=
  match store write.target with
  | none => none
  | some pre =>
      if pre.call.state = write.expectedState ∧
          pre.owner = write.expectedOwner ∧
          pre.epoch = write.expectedEpoch then
        match step? pre.call write.action with
        | none => none
        | some post =>
            some fun docId =>
              if docId = write.target then
                some { pre with call := post }
              else
                store docId
      else
        none

/-- Exact CAS protects one physical row; it does not resolve replicated
logical-id conflicts. Admission therefore supplies the complete visible set of
physical documents for the logical call and proceeds only when it is exactly
the selected target. -/
def applyAdmitted
    (store : Store)
    (visibleLogicalDocuments : List DocumentId)
    (write : FencedUpdate) : Option Store :=
  if visibleLogicalDocuments = [write.target] then
    applyFenced store write
  else
    none

theorem logical_conflict_rejects_admission
    {store : Store} {write : FencedUpdate} {sibling : DocumentId} :
    applyAdmitted store [write.target, sibling] write = none := by
  simp [applyAdmitted]

theorem unique_visible_target_refines_to_exact_cas
    {store : Store} {write : FencedUpdate} :
    applyAdmitted store [write.target] write = applyFenced store write := by
  simp [applyAdmitted]

/-- Strict compare-and-set and idempotent observation are different outcomes.
`applyFenced` remains strict: replaying a successful write with its old expected
state returns `none`.  A caller may then reload the exact document and accept an
already-observed desired state without issuing another mutation. -/
inductive FencedDisposition where
  | applied
  | observedDesired
  | rejected
  deriving DecidableEq, Repr

structure FencedObservation where
  disposition : FencedDisposition
  store : Store

/-- Apply the strict CAS, or classify the exact current row as an idempotent
observation when the declared `(expectedState, action)` is itself a legal edge
and the row has already reached that edge's desired state. -/
def applyOrObserve (store : Store) (write : FencedUpdate) : FencedObservation :=
  match applyFenced store write with
  | some post => ⟨.applied, post⟩
  | none =>
      match store write.target with
      | none => ⟨.rejected, store⟩
      | some current =>
          if current.owner = write.expectedOwner ∧
              current.epoch = write.expectedEpoch then
            let expected := { current.call with state := write.expectedState }
            match step? expected write.action with
            | none => ⟨.rejected, store⟩
            | some desired =>
                if current.call.state = desired.state then
                  ⟨.observedDesired, store⟩
                else
                  ⟨.rejected, store⟩
          else
            ⟨.rejected, store⟩

def applyAdmittedOrObserve
    (store : Store)
    (visibleLogicalDocuments : List DocumentId)
    (write : FencedUpdate) : FencedObservation :=
  if visibleLogicalDocuments = [write.target] then
    applyOrObserve store write
  else
    ⟨.rejected, store⟩

theorem logical_conflict_rejects_observation
    {store : Store} {write : FencedUpdate} {sibling : DocumentId} :
    applyAdmittedOrObserve store [write.target, sibling] write =
      ⟨.rejected, store⟩ := by
  simp [applyAdmittedOrObserve]

theorem missing_target_rejected
    {store : Store} {write : FencedUpdate}
    (h_missing : store write.target = none) :
    applyFenced store write = none := by
  simp [applyFenced, h_missing]

theorem stale_expected_state_rejected
    {store : Store} {write : FencedUpdate} {pre : PersistedCall}
    (h_row : store write.target = some pre)
    (h_stale : pre.call.state ≠ write.expectedState) :
    applyFenced store write = none := by
  simp [applyFenced, h_row, h_stale]

theorem stale_owner_rejected
    {store : Store} {write : FencedUpdate} {pre : PersistedCall}
    (h_row : store write.target = some pre)
    (h_stale : pre.owner ≠ write.expectedOwner) :
    applyFenced store write = none := by
  simp [applyFenced, h_row, h_stale]

theorem stale_epoch_rejected
    {store : Store} {write : FencedUpdate} {pre : PersistedCall}
    (h_row : store write.target = some pre)
    (h_stale : pre.epoch ≠ write.expectedEpoch) :
    applyFenced store write = none := by
  simp [applyFenced, h_row, h_stale]

theorem observed_desired_is_non_mutating
    {store : Store} {write : FencedUpdate}
    (h_observed : (applyOrObserve store write).disposition =
      .observedDesired) :
    (applyOrObserve store write).store = store := by
  rcases h_apply : applyFenced store write with _ | post
  · simp only [applyOrObserve, h_apply]
    split
    · rfl
    · split
      · split
        · rfl
        · split <;> rfl
      · rfl
  · simp [applyOrObserve, h_apply] at h_observed

/-- A successful write cannot affect any sibling physical document. -/
theorem sibling_isolation
    {store post : Store} {write : FencedUpdate}
    (h_apply : applyFenced store write = some post)
    {sibling : DocumentId}
    (h_distinct : sibling ≠ write.target) :
    post sibling = store sibling := by
  unfold applyFenced at h_apply
  split at h_apply
  · contradiction
  · rename_i pre h_row
    split at h_apply
    · rename_i h_fence
      split at h_apply
      · contradiction
      · rename_i next h_step
        simp only [Option.some.injEq] at h_apply
        subst post
        simp [h_distinct]
    · contradiction

/-- Success implies that the targeted row existed in the expected source
state and took a legal `InferenceCall.Transition`. -/
theorem successful_write_is_legal
    {store post : Store} {write : FencedUpdate}
    (h_apply : applyFenced store write = some post) :
    ∃ pre : PersistedCall, ∃ next : InferenceCall,
      store write.target = some pre ∧
      pre.call.state = write.expectedState ∧
      pre.owner = write.expectedOwner ∧
      pre.epoch = write.expectedEpoch ∧
      step? pre.call write.action = some next ∧
      Transition pre.call next ∧
      post write.target = some { pre with call := next } := by
  unfold applyFenced at h_apply
  split at h_apply
  · contradiction
  · rename_i pre h_row
    split at h_apply
    · rename_i h_fence
      split at h_apply
      · contradiction
      · rename_i next h_step
        simp only [Option.some.injEq] at h_apply
        subst post
        refine ⟨pre, next, h_row, h_fence.1, h_fence.2.1, h_fence.2.2,
          h_step, step_sound h_step, ?_⟩
        simp
    · contradiction

/-- Terminal rows cannot be reopened or rewritten to a different terminal
outcome, even when a caller supplies the terminal row's current state as its
expected state. -/
theorem terminal_irreversible
    {store : Store} {write : FencedUpdate} {pre : PersistedCall}
    (h_row : store write.target = some pre)
    (h_terminal : isTerminal pre.call.state) :
    applyFenced store write = none := by
  rcases write with ⟨target, expectedState, expectedOwner, expectedEpoch, action⟩
  cases h_state : pre.call.state <;> cases action <;>
    simp [applyFenced, h_row, step?, h_state,
      HasTerminal.isTerminal, InferenceCallState.instHasTerminal] at h_terminal ⊢

/-- Physical identity, not logical `callId`, scopes a successful mutation. -/
theorem duplicate_logical_id_sibling_unchanged
    {store post : Store} {write : FencedUpdate}
    {target sibling : PersistedCall} {siblingId : DocumentId}
    (h_sibling : store siblingId = some sibling)
    (h_same_logical_id : target.call.callId = sibling.call.callId)
    (h_distinct : siblingId ≠ write.target)
    (h_apply : applyFenced store write = some post) :
    post siblingId = some sibling ∧ target.call.callId = sibling.call.callId := by
  have h_unchanged := sibling_isolation h_apply h_distinct
  exact ⟨h_unchanged.trans h_sibling, h_same_logical_id⟩

end InferenceCall
