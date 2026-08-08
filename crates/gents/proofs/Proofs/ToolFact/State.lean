import Proofs.RenderedCapture

/-!
# Exact durable tool facts (#1073)

Tool calls, complete results, and approval decisions are immutable signed facts.
Every edge names the exact DefraDB `_docID`, composite CID, and verified signer;
logical keys are only conflict domains and never sufficient for a join.
-/

namespace ToolFact

open RenderedCapture

abbrev LogicalKey := Nat
abbrev PayloadHash := Nat
abbrev SignerDid := Nat

/-- Exact physical document version plus the signer verified from its commit. -/
structure SignedRef where
  version : DocumentVersionRef
  signerDid : SignerDid
  signatureValid : Bool
  deriving DecidableEq, Repr

def versionExact (version : DocumentVersionRef) : Bool :=
  version.docId != 0 && version.compositeCommitCid != 0

def SignedRef.authoritative (ref : SignedRef) : Bool :=
  versionExact ref.version && ref.signerDid != 0 && ref.signatureValid

structure ToolCallFact where
  key : LogicalKey
  signed : SignedRef
  argsHash : PayloadHash
  deriving DecidableEq, Repr

structure ToolCallIntent where
  key : LogicalKey
  argsHash : PayloadHash
  deriving DecidableEq, Repr

structure ToolResultIntent where
  key : LogicalKey
  call : SignedRef
  outputHash : PayloadHash
  fullOutput : Bool
  deriving DecidableEq, Repr

structure ToolResultFact where
  key : LogicalKey
  signed : SignedRef
  call : SignedRef
  outputHash : PayloadHash
  fullOutput : Bool
  deriving DecidableEq, Repr

inductive ApprovalDecision where
  | approved
  | denied
  deriving DecidableEq, Repr

structure ToolApprovalIntent where
  key : LogicalKey
  call : SignedRef
  decision : ApprovalDecision
  reasonHash : PayloadHash
  deriving DecidableEq, Repr

structure ToolApprovalFact where
  key : LogicalKey
  signed : SignedRef
  call : SignedRef
  decision : ApprovalDecision
  reasonHash : PayloadHash
  deriving DecidableEq, Repr

abbrev Store (α : Type) := Nat → Option α

namespace Store

def empty {α : Type} : Store α := fun _ => none

def bind {α : Type} (store : Store α) (docId : Nat) (value : α) : Store α :=
  fun probe => if probe = docId then some value else store probe

@[simp] theorem bind_self {α : Type} (store : Store α) (docId : Nat) (value : α) :
    bind store docId value docId = some value := by
  simp [bind]

@[simp] theorem bind_other {α : Type} (store : Store α) (docId probe : Nat) (value : α)
    (h : probe ≠ docId) : bind store docId value probe = store probe := by
  simp [bind, h]

end Store

abbrev ToolCallStore := Store ToolCallFact
abbrev ToolResultStore := Store ToolResultFact
abbrev ToolApprovalStore := Store ToolApprovalFact

structure State where
  calls : ToolCallStore
  results : ToolResultStore
  approvals : ToolApprovalStore

def State.empty : State :=
  { calls := Store.empty, results := Store.empty, approvals := Store.empty }

def ToolCallFact.forIntent (intent : ToolCallIntent) (signed : SignedRef) : ToolCallFact :=
  { key := intent.key, signed := signed, argsHash := intent.argsHash }

def ToolResultFact.forIntent (intent : ToolResultIntent) (signed : SignedRef) :
    ToolResultFact :=
  { key := intent.key
  , signed := signed
  , call := intent.call
  , outputHash := intent.outputHash
  , fullOutput := intent.fullOutput }

def ToolApprovalFact.forIntent (intent : ToolApprovalIntent) (signed : SignedRef) :
    ToolApprovalFact :=
  { key := intent.key
  , signed := signed
  , call := intent.call
  , decision := intent.decision
  , reasonHash := intent.reasonHash }

def exactCall? (store : ToolCallStore) (ref : SignedRef) : Option ToolCallFact :=
  match store ref.version.docId with
  | some fact =>
      if fact.signed = ref ∧ ref.authoritative = true then some fact else none
  | none => none

def exactResult? (store : ToolResultStore) (ref : SignedRef) : Option ToolResultFact :=
  match store ref.version.docId with
  | some fact =>
      if fact.signed = ref ∧ ref.authoritative = true ∧ fact.fullOutput = true then
        some fact
      else none
  | none => none

def exactApproval? (store : ToolApprovalStore) (ref : SignedRef) : Option ToolApprovalFact :=
  match store ref.version.docId with
  | some fact =>
      if fact.signed = ref ∧ ref.authoritative = true then some fact else none
  | none => none

end ToolFact
