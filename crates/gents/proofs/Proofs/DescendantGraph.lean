import Proofs.Basic

/-!
# Canonical descendant graph

`AgentToolCall` is the parent-authored durable edge receipt.  A child request
may materialize later (and on another deployment), so visibility is defined
from the receipt and the root ownership tuple.  Once a child is materialized,
the logical and physical request/tool-call links must corroborate the receipt.

Visibility and control are intentionally separate.  An ancestor can inspect a
verified descendant edge without thereby acquiring the direct parent's
steer/cancel authority.
-/

namespace DescendantGraph

abbrev PrincipalId := Nat
abbrev DeploymentId := Nat
abbrev ToolCallId := Nat
abbrev LineageId := Nat
abbrev Cursor := ToolCallId × RequestId

inductive AwaitMode where
  | foreground
  | background
  deriving DecidableEq, Repr

inductive Materialization where
  | pending
  | local
  | replicated
  deriving DecidableEq, Repr

inductive Lifecycle where
  | pending
  | running
  | completed
  | failed
  | cancelled
  deriving DecidableEq, Repr

inductive Scope where
  | direct
  | descendants
  deriving DecidableEq, Repr

structure Viewer where
  rootRequestId : RequestId
  rootPrincipal : PrincipalId
  rootSessionId : SessionId
  lineageId : LineageId
  deriving DecidableEq, Repr

structure Edge where
  rootRequestId : RequestId
  rootSessionId : SessionId
  parentRequestId : RequestId
  parentToolCallId : ToolCallId
  childRequestId : RequestId
  childSessionId : Option SessionId
  ownerPrincipal : PrincipalId
  controlPrincipal : PrincipalId
  childPrincipal : PrincipalId
  behaviorId : BehaviorId
  deploymentId : DeploymentId
  lineageId : LineageId
  awaitMode : AwaitMode
  materialization : Materialization
  lifecycle : Lifecycle
  bridgeDurable : Bool
  physicalCorroborated : Bool
  directFromRoot : Bool
  deriving DecidableEq, Repr

def materializationAuthorized (edge : Edge) : Bool :=
  match edge.materialization with
  | .pending => true
  | .local | .replicated => edge.physicalCorroborated

/- The durable bridge receipt is enumerable by its owning lineage even when a
   materialized child fails physical corroboration. That diagnostic visibility
   never grants access to the child document itself. -/
def visible (viewer : Viewer) (edge : Edge) : Bool :=
  edge.bridgeDurable &&
    edge.rootRequestId == viewer.rootRequestId &&
    edge.ownerPrincipal == viewer.rootPrincipal &&
    edge.rootSessionId == viewer.rootSessionId &&
    edge.lineageId == viewer.lineageId

def readable (viewer : Viewer) (edge : Edge) : Bool :=
  visible viewer edge &&
    materializationAuthorized edge &&
    edge.materialization != .pending

def terminal : Lifecycle → Bool
  | .pending | .running => false
  | .completed | .failed | .cancelled => true

/-- Only an absent child can converge through retry. A child that exists but
    rejects the bridge's physical lineage is a permanent authorization result.
    A terminal bridge cannot converge further even if no child materialized. -/
def retryable (viewer : Viewer) (edge : Edge) : Bool :=
  visible viewer edge && edge.materialization == .pending && !terminal edge.lifecycle

/-- The ordinary active-child view retains every owned, nonterminal bridge,
    including a rejected child diagnostic. -/
def listedByDefault (viewer : Viewer) (edge : Edge) : Bool :=
  visible viewer edge && !terminal edge.lifecycle

/-- Control is narrower than visibility: only the direct owning principal may
    steer/cancel through this edge. -/
def controllable (viewer : Viewer) (edge : Edge) : Bool :=
  readable viewer edge &&
    edge.directFromRoot &&
    edge.controlPrincipal == viewer.rootPrincipal

def inScope (scope : Scope) (edge : Edge) : Bool :=
  match scope with
  | .direct => edge.directFromRoot
  | .descendants => true

/-- A page cursor is derived only from durable edge identity, never from the
    edge's mutable lifecycle/materialization projection. -/
def cursor (edge : Edge) : Cursor :=
  (edge.parentToolCallId, edge.childRequestId)

/-- Cursor lookup runs over the stable scoped edge sequence. Volatile filters
    such as `includeTerminal` are applied only to the returned suffix. -/
def afterCursor (target : Cursor) : List Edge → Option (List Edge)
  | [] => none
  | edge :: rest =>
      if cursor edge == target then some rest else afterCursor target rest

theorem behavior_change_preserves_visibility
    (viewer : Viewer) (edge : Edge) (behavior : BehaviorId) :
    visible viewer { edge with behaviorId := behavior } = visible viewer edge := by
  rfl

theorem deployment_change_preserves_visibility
    (viewer : Viewer) (edge : Edge) (deployment : DeploymentId) :
    visible viewer { edge with deploymentId := deployment } = visible viewer edge := by
  rfl

theorem await_change_preserves_visibility
    (viewer : Viewer) (edge : Edge) (mode : AwaitMode) :
    visible viewer { edge with awaitMode := mode } = visible viewer edge := by
  rfl

theorem lifecycle_change_preserves_cursor
    (edge : Edge) (lifecycle : Lifecycle) :
    cursor { edge with lifecycle := lifecycle } = cursor edge := by
  rfl

theorem terminal_transition_preserves_cursor_anchor
    (edge next : Edge) :
    afterCursor (cursor edge) [{ edge with lifecycle := .completed }, next] =
      some [next] := by
  simp [afterCursor, lifecycle_change_preserves_cursor]

theorem pending_bridge_visible_without_child
    (viewer : Viewer) (edge : Edge)
    (hBridge : edge.bridgeDurable = true)
    (hRoot : edge.rootRequestId = viewer.rootRequestId)
    (hOwner : edge.ownerPrincipal = viewer.rootPrincipal)
    (hSession : edge.rootSessionId = viewer.rootSessionId)
    (hLineage : edge.lineageId = viewer.lineageId) :
    visible viewer { edge with
      materialization := .pending
      physicalCorroborated := false } = true := by
  simp [visible, materializationAuthorized, hBridge, hRoot, hOwner, hSession, hLineage]

theorem unrelated_principal_cannot_see
    (viewer : Viewer) (edge : Edge)
    (h : edge.ownerPrincipal ≠ viewer.rootPrincipal) :
    visible viewer edge = false := by
  simp [visible, h]

theorem unrelated_principal_cannot_read
    (viewer : Viewer) (edge : Edge)
    (h : edge.ownerPrincipal ≠ viewer.rootPrincipal) :
    readable viewer edge = false := by
  simp [readable, unrelated_principal_cannot_see viewer edge h]

theorem unrelated_root_request_cannot_see
    (viewer : Viewer) (edge : Edge)
    (h : edge.rootRequestId ≠ viewer.rootRequestId) :
    visible viewer edge = false := by
  simp [visible, h]

theorem unrelated_session_cannot_see
    (viewer : Viewer) (edge : Edge)
    (h : edge.rootSessionId ≠ viewer.rootSessionId) :
    visible viewer edge = false := by
  simp [visible, h]

theorem unrelated_lineage_cannot_see
    (viewer : Viewer) (edge : Edge)
    (h : edge.lineageId ≠ viewer.lineageId) :
    visible viewer edge = false := by
  simp [visible, h]

theorem uncorroborated_materialized_edge_is_visible_but_unreadable
    (viewer : Viewer) (edge : Edge)
    (hBridge : edge.bridgeDurable = true)
    (hRoot : edge.rootRequestId = viewer.rootRequestId)
    (hOwner : edge.ownerPrincipal = viewer.rootPrincipal)
    (hSession : edge.rootSessionId = viewer.rootSessionId)
    (hLineage : edge.lineageId = viewer.lineageId)
    (h : edge.materialization ≠ .pending)
    (hPhysical : edge.physicalCorroborated = false) :
    visible viewer edge = true ∧ readable viewer edge = false := by
  have hMaterialization : materializationAuthorized edge = false := by
    generalize hKind : edge.materialization = kind at h ⊢
    cases kind <;> simp_all [materializationAuthorized, hPhysical]
  constructor
  · simp [visible, hBridge, hRoot, hOwner, hSession, hLineage]
  · simp [readable, hMaterialization]

theorem rejected_materialization_is_not_retryable
    (viewer : Viewer) (edge : Edge)
    (h : edge.materialization ≠ .pending) :
    retryable viewer edge = false := by
  simp [retryable, h]

theorem terminal_pending_bridge_is_not_retryable
    (viewer : Viewer) (edge : Edge)
    (hTerminal : terminal edge.lifecycle = true) :
    retryable viewer { edge with materialization := .pending } = false := by
  simp [retryable, hTerminal]

theorem owned_running_rejection_remains_listed
    (viewer : Viewer) (edge : Edge)
    (hBridge : edge.bridgeDurable = true)
    (hRoot : edge.rootRequestId = viewer.rootRequestId)
    (hOwner : edge.ownerPrincipal = viewer.rootPrincipal)
    (hSession : edge.rootSessionId = viewer.rootSessionId)
    (hLineage : edge.lineageId = viewer.lineageId)
    (hRunning : edge.lifecycle = .running) :
    listedByDefault viewer edge = true := by
  simp [listedByDefault, terminal, visible, hBridge, hRoot, hOwner, hSession, hLineage,
    hRunning]

theorem replicated_materialization_preserves_authorization
    (viewer : Viewer) (edge : Edge) :
    visible viewer { edge with materialization := .replicated } =
      visible viewer { edge with materialization := .local } := by
  rfl

theorem visibility_does_not_grant_ancestor_control
    (viewer : Viewer) (edge : Edge)
    (_hVisible : visible viewer edge = true)
    (hNested : edge.directFromRoot = false) :
    controllable viewer edge = false := by
  simp [controllable, hNested]

theorem control_implies_visibility
    (viewer : Viewer) (edge : Edge)
    (hControl : controllable viewer edge = true) :
    visible viewer edge = true := by
  simp [controllable] at hControl
  have hReadable := hControl.1
  unfold readable at hReadable
  simp at hReadable
  exact hReadable.1.1.1

theorem direct_scope_excludes_nested
    (edge : Edge) (hNested : edge.directFromRoot = false) :
    inScope .direct edge = false := by
  simp [inScope, hNested]

theorem descendants_scope_includes_every_edge (edge : Edge) :
    inScope .descendants edge = true := by
  rfl

end DescendantGraph
