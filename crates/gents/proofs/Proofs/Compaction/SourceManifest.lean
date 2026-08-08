/-!
Exact, immutable source manifests for finalized compaction facts.

The runtime may track mutable compaction progress elsewhere.  A finalized
summary is create-only and names the ordered, signed document versions from
which it was derived.  Logical twins and source rebinding fail closed.
-/

namespace Compaction.SourceManifest

structure ExactRef where
  ordinal : Nat
  docId : Nat
  compositeCommitCid : Nat
  signerDid : Nat
  signatureValid : Bool
  deriving BEq, DecidableEq, Repr

def ExactRef.complete (ref : ExactRef) : Bool :=
  ref.docId != 0 && ref.compositeCommitCid != 0 && ref.signerDid != 0 && ref.signatureValid

def exactOrder : List ExactRef → Bool
  | [] | [_] => true
  | left :: right :: rest => left.ordinal < right.ordinal && exactOrder (right :: rest)

structure Manifest where
  version : Nat
  sessionId : Nat
  behaviorId : Nat
  transcript : List ExactRef
  config : List ExactRef
  priorCompactions : List ExactRef
  providerViewCount : Nat
  priorCompactedCount : Nat
  compactorInputCount : Nat
  deriving BEq, DecidableEq, Repr

def Manifest.valid (manifest : Manifest) : Bool :=
  manifest.version == 1
    && manifest.sessionId != 0
    && manifest.behaviorId != 0
    && !manifest.transcript.isEmpty
    && manifest.transcript.all ExactRef.complete
    && exactOrder manifest.transcript
    && !manifest.config.isEmpty
    && manifest.config.all ExactRef.complete
    && manifest.priorCompactions.all ExactRef.complete
    && exactOrder manifest.priorCompactions
    && manifest.priorCompactedCount <= manifest.providerViewCount
    && manifest.compactorInputCount
      <= manifest.providerViewCount - manifest.priorCompactedCount

structure FinalFact where
  logicalKey : Nat
  sequence : Nat
  sourceManifest : Manifest
  summaryHash : Nat
  deriving BEq, DecidableEq, Repr

inductive Disposition where
  | applied
  | idempotent
  | rejected
  deriving BEq, DecidableEq, Repr

structure Observation where
  rows : List FinalFact
  disposition : Disposition
  deriving BEq, DecidableEq, Repr

/-- Create-and-compare over every visible logical candidate. -/
def commit (visible : List FinalFact) (desired : FinalFact) : Observation :=
  if desired.sourceManifest.valid then
    match visible with
    | [] => { rows := [desired], disposition := .applied }
    | [current] =>
        if current = desired then
          { rows := visible, disposition := .idempotent }
        else
          { rows := visible, disposition := .rejected }
    | _ => { rows := visible, disposition := .rejected }
  else
    { rows := visible, disposition := .rejected }

abbrev sourceStillCurrent (pinned current : List ExactRef) : Prop := pinned = current

def finalize
    (visible : List FinalFact)
    (desired : FinalFact)
    (currentTranscript currentConfig currentPrior : List ExactRef) : Observation :=
  if sourceStillCurrent desired.sourceManifest.transcript currentTranscript
      ∧ sourceStillCurrent desired.sourceManifest.config currentConfig
      ∧ sourceStillCurrent desired.sourceManifest.priorCompactions currentPrior then
    commit visible desired
  else
    { rows := visible, disposition := .rejected }

theorem malformed_manifest_rejected
    (visible : List FinalFact) (desired : FinalFact)
    (invalid : desired.sourceManifest.valid = false) :
    (commit visible desired).disposition = .rejected := by
  simp [commit, invalid]

theorem identical_replay_is_idempotent
    (desired : FinalFact) (valid : desired.sourceManifest.valid = true) :
    (commit [desired] desired).disposition = .idempotent := by
  simp [commit, valid]

theorem conflicting_replay_preserves_final_fact
    (current desired : FinalFact)
    (valid : desired.sourceManifest.valid = true)
    (different : current ≠ desired) :
    commit [current] desired = { rows := [current], disposition := .rejected } := by
  simp [commit, valid, different]

theorem logical_twins_rejected
    (left right desired : FinalFact)
    (valid : desired.sourceManifest.valid = true) :
    (commit [left, right] desired).disposition = .rejected := by
  simp [commit, valid]

theorem mutated_transcript_rejected
    (visible : List FinalFact) (desired : FinalFact)
    (currentTranscript : List ExactRef)
    (changed : desired.sourceManifest.transcript ≠ currentTranscript) :
    (finalize visible desired currentTranscript desired.sourceManifest.config
      desired.sourceManifest.priorCompactions).disposition = .rejected := by
  simp [finalize, sourceStillCurrent, changed]

theorem mutated_config_rejected
    (visible : List FinalFact) (desired : FinalFact)
    (currentConfig : List ExactRef)
    (changed : desired.sourceManifest.config ≠ currentConfig) :
    (finalize visible desired desired.sourceManifest.transcript currentConfig
      desired.sourceManifest.priorCompactions).disposition = .rejected := by
  simp [finalize, sourceStillCurrent, changed]

theorem mutated_prior_compaction_rejected
    (visible : List FinalFact) (desired : FinalFact)
    (currentPrior : List ExactRef)
    (changed : desired.sourceManifest.priorCompactions ≠ currentPrior) :
    (finalize visible desired desired.sourceManifest.transcript
      desired.sourceManifest.config currentPrior).disposition = .rejected := by
  simp [finalize, sourceStillCurrent, changed]

end Compaction.SourceManifest
