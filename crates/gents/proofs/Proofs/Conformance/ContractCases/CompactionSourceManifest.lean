import Proofs.Conformance.ContractCases.Types
import Proofs.Compaction.SourceManifest

namespace Conformance.ContractCases

open Compaction.SourceManifest

structure CompactionSourceManifestCase where
  name : String
  disposition : String
  visibleLogicalTwins : Nat
  manifestValid : Bool
  sourcesCurrent : Bool
  durableRows : Nat
  deriving Repr

private def exact (ordinal docId cid signerDid : Nat) : ExactRef :=
  { ordinal, docId, compositeCommitCid := cid, signerDid, signatureValid := true }

private def transcript := [exact 1 101 1001 7, exact 2 102 1002 7]
private def config := [exact 1 201 2001 7, exact 2 202 2002 7]

private def validManifest : Manifest :=
  { version := 1
  , sessionId := 1
  , behaviorId := 2
  , transcript
  , config
  , priorCompactions := []
  , providerViewCount := 2
  , priorCompactedCount := 0
  , compactorInputCount := 2 }

private def fact : FinalFact :=
  { logicalKey := 1, sequence := 1, sourceManifest := validManifest, summaryHash := 99 }

private def dispositionString : Disposition → String
  | .applied => "applied"
  | .idempotent => "idempotent"
  | .rejected => "rejected"

private def caseOf
    (name : String) (visibleLogicalTwins : Nat)
    (manifestValid sourcesCurrent : Bool) (observation : Observation) :
    CompactionSourceManifestCase :=
  { name
  , disposition := dispositionString observation.disposition
  , visibleLogicalTwins
  , manifestValid
  , sourcesCurrent
  , durableRows := observation.rows.length }

private def malformed :=
  { fact with sourceManifest := { validManifest with transcript := [] } }
private def conflicting := { fact with summaryHash := 100 }
private def changedTranscript := [exact 1 101 9001 7, exact 2 102 1002 7]
private def changedConfig := [exact 1 201 9002 7, exact 2 202 2002 7]
private def unsigned :=
  { fact with sourceManifest :=
      { validManifest with transcript :=
          [{ (exact 1 101 1001 7) with signatureValid := false }, exact 2 102 1002 7] } }

def compactionSourceManifestCases : List CompactionSourceManifestCase :=
  [ caseOf "fresh_exact_manifest_applied" 0 true true
      (finalize [] fact transcript config [])
  , caseOf "identical_replay_is_idempotent" 1 true true
      (finalize [fact] fact transcript config [])
  , caseOf "malformed_empty_transcript_rejected" 0 false true
      (finalize [] malformed [] config [])
  , caseOf "unsigned_source_rejected" 0 false true
      (finalize [] unsigned unsigned.sourceManifest.transcript config [])
  , caseOf "conflicting_final_fact_rejected" 1 true true
      (finalize [fact] conflicting transcript config [])
  , caseOf "logical_twins_rejected" 2 true true
      (finalize [fact, conflicting] fact transcript config [])
  , caseOf "mutated_transcript_rejected" 0 true false
      (finalize [] fact changedTranscript config [])
  , caseOf "mutated_config_rejected" 0 true false
      (finalize [] fact transcript changedConfig []) ]

end Conformance.ContractCases
