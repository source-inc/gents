import Proofs.Conformance.ContractCases.Types
import Proofs.SubagentBridgeAdmission

namespace Conformance.ContractCases

open SubagentBridgeAdmission

structure SubagentBridgeAdmissionCase where
  name : String
  bridgeSignatureValid : Bool
  bridgeSignerDid : Nat
  bridgeAuthorDid : Nat
  admittedParentDid : Nat
  bridgeHeadCount : Nat
  observedBridgeCid : Nat
  currentBridgeCid : Nat
  parentRequestMatches : Bool
  parentToolCallMatches : Bool
  childRequestMatches : Bool
  admitted : Bool
  outcome : String
  deriving Repr

private def bridgeAdmissionCase (name : String) (evidence : Evidence) :
    SubagentBridgeAdmissionCase :=
  { name := name
  , bridgeSignatureValid := evidence.bridgeSignatureValid
  , bridgeSignerDid := evidence.bridgeSignerDid
  , bridgeAuthorDid := evidence.bridgeAuthorDid
  , admittedParentDid := evidence.admittedParentDid
  , bridgeHeadCount := evidence.bridgeHeadCount
  , observedBridgeCid := evidence.observedBridgeCid
  , currentBridgeCid := evidence.currentBridgeCid
  , parentRequestMatches := evidence.parentRequestMatches
  , parentToolCallMatches := evidence.parentToolCallMatches
  , childRequestMatches := evidence.childRequestMatches
  , admitted := admitted evidence
  , outcome := (evaluate evidence).toContract
  }

private def valid : Evidence :=
  { bridgeSignatureValid := true
  , bridgeSignerDid := 7
  , bridgeAuthorDid := 7
  , admittedParentDid := 7
  , bridgeHeadCount := 1
  , observedBridgeCid := 101
  , currentBridgeCid := 101
  , parentRequestMatches := true
  , parentToolCallMatches := true
  , childRequestMatches := true
  }

def subagentBridgeAdmissionCases : List SubagentBridgeAdmissionCase :=
  [ bridgeAdmissionCase "valid_signed_bridge" valid
  , bridgeAdmissionCase "invalid_bridge_signature"
      { valid with bridgeSignatureValid := false }
  , bridgeAdmissionCase "bridge_signer_not_declared_author"
      { valid with bridgeSignerDid := 9 }
  , bridgeAdmissionCase "bridge_signer_not_admitted_parent"
      { valid with admittedParentDid := 9 }
  , bridgeAdmissionCase "ambiguous_bridge_heads"
      { valid with bridgeHeadCount := 2 }
  , bridgeAdmissionCase "stale_bridge_snapshot"
      { valid with observedBridgeCid := 100 }
  , bridgeAdmissionCase "parent_request_edge_mismatch"
      { valid with parentRequestMatches := false }
  , bridgeAdmissionCase "parent_tool_edge_mismatch"
      { valid with parentToolCallMatches := false }
  , bridgeAdmissionCase "child_request_edge_mismatch"
      { valid with childRequestMatches := false }
  ]

theorem subagentBridgeAdmissionCases_pinned :
    subagentBridgeAdmissionCases.map
      (fun row => (row.name, row.admitted, row.outcome)) =
      [ ("valid_signed_bridge", true, "childMaterialized")
      , ("invalid_bridge_signature", false, "rejected")
      , ("bridge_signer_not_declared_author", false, "rejected")
      , ("bridge_signer_not_admitted_parent", false, "rejected")
      , ("ambiguous_bridge_heads", false, "rejected")
      , ("stale_bridge_snapshot", false, "rejected")
      , ("parent_request_edge_mismatch", false, "rejected")
      , ("parent_tool_edge_mismatch", false, "rejected")
      , ("child_request_edge_mismatch", false, "rejected")
      ] := by
  rfl

end Conformance.ContractCases
