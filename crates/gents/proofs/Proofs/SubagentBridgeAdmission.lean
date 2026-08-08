/-!
# Signed subagent bridge admission

A bridge row is authority to materialize a child only when the runtime consumes
one exact current composite head, verifies that head's signature, binds the
signer to both the immutable bridge author and the admitted parent owner, and
reads the parent/tool/child edge from that same signed snapshot.

The carried DID columns remain useful routing data. They are not evidence until
the exact bridge commit has passed this predicate.
-/

namespace SubagentBridgeAdmission

abbrev Did := Nat
abbrev Cid := Nat

structure Evidence where
  bridgeSignatureValid : Bool
  bridgeSignerDid : Did
  bridgeAuthorDid : Did
  admittedParentDid : Did
  bridgeHeadCount : Nat
  observedBridgeCid : Cid
  currentBridgeCid : Cid
  parentRequestMatches : Bool
  parentToolCallMatches : Bool
  childRequestMatches : Bool
  deriving DecidableEq, Repr

def admitted (evidence : Evidence) : Bool :=
  evidence.bridgeSignatureValid &&
    (evidence.bridgeSignerDid == evidence.bridgeAuthorDid &&
    (evidence.bridgeSignerDid == evidence.admittedParentDid &&
    (evidence.bridgeHeadCount == 1 &&
    (evidence.observedBridgeCid == evidence.currentBridgeCid &&
    (evidence.parentRequestMatches &&
    (evidence.parentToolCallMatches && evidence.childRequestMatches))))))

theorem admitted_iff (evidence : Evidence) :
    admitted evidence = true ↔
      evidence.bridgeSignatureValid = true ∧
      evidence.bridgeSignerDid = evidence.bridgeAuthorDid ∧
      evidence.bridgeSignerDid = evidence.admittedParentDid ∧
      evidence.bridgeHeadCount = 1 ∧
      evidence.observedBridgeCid = evidence.currentBridgeCid ∧
      evidence.parentRequestMatches = true ∧
      evidence.parentToolCallMatches = true ∧
      evidence.childRequestMatches = true := by
  simp [admitted]

theorem admitted_binds_signer_to_parent (evidence : Evidence)
    (h : admitted evidence = true) :
    evidence.bridgeSignatureValid = true ∧
    evidence.bridgeSignerDid = evidence.bridgeAuthorDid ∧
    evidence.bridgeSignerDid = evidence.admittedParentDid := by
  have accepted := (admitted_iff evidence).mp h
  exact ⟨accepted.1, accepted.2.1, accepted.2.2.1⟩

theorem admitted_pins_exact_current_head (evidence : Evidence)
    (h : admitted evidence = true) :
    evidence.bridgeHeadCount = 1 ∧
    evidence.observedBridgeCid = evidence.currentBridgeCid := by
  have accepted := (admitted_iff evidence).mp h
  exact ⟨accepted.2.2.2.1, accepted.2.2.2.2.1⟩

theorem admitted_binds_parent_edge (evidence : Evidence)
    (h : admitted evidence = true) :
    evidence.parentRequestMatches = true ∧
    evidence.parentToolCallMatches = true ∧
    evidence.childRequestMatches = true := by
  have accepted := (admitted_iff evidence).mp h
  exact ⟨accepted.2.2.2.2.2.1, accepted.2.2.2.2.2.2.1,
    accepted.2.2.2.2.2.2.2⟩

inductive Outcome where
  | rejected
  | childMaterialized
  deriving DecidableEq, Repr

namespace Outcome

def toContract : Outcome → String
  | .rejected => "rejected"
  | .childMaterialized => "childMaterialized"

end Outcome

def evaluate (evidence : Evidence) : Outcome :=
  if admitted evidence then .childMaterialized else .rejected

theorem child_materialized_iff_admitted (evidence : Evidence) :
    evaluate evidence = .childMaterialized ↔ admitted evidence = true := by
  cases h : admitted evidence <;> simp [evaluate, h]

end SubagentBridgeAdmission
