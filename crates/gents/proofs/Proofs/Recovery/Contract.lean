import Proofs.Basic

namespace Recovery

inductive RecoveryCadence where
  | startup
  | periodic
  deriving DecidableEq, Repr

namespace RecoveryCadence

def toContract : RecoveryCadence → String
  | .startup => "startup"
  | .periodic => "periodic"

end RecoveryCadence

inductive RecoveryImplementationStatus where
  | implemented
  | obligation
  deriving DecidableEq, Repr

namespace RecoveryImplementationStatus

def toContract : RecoveryImplementationStatus → String
  | .implemented => "implemented"
  | .obligation => "obligation"

end RecoveryImplementationStatus

inductive PersistedRecoveryCollection where
  | agentRequest
  | agentResponse
  | agentToolCall
  | inferenceCall
  deriving DecidableEq, Repr

namespace PersistedRecoveryCollection

def toContract : PersistedRecoveryCollection → String
  | .agentRequest => "AgentRequest"
  | .agentResponse => "AgentResponse"
  | .agentToolCall => "AgentToolCall"
  | .inferenceCall => "InferenceCall"

def all : List PersistedRecoveryCollection :=
  [ .agentRequest
  , .agentResponse
  , .agentToolCall
  , .inferenceCall
  ]

theorem all_complete (collection : PersistedRecoveryCollection) :
    collection ∈ all := by
  cases collection <;> simp [all]

end PersistedRecoveryCollection

structure RecoverySweep where
  Row : Type
  collection : PersistedRecoveryCollection
  sweepId : String
  rustFunction : String
  cadence : RecoveryCadence
  implementationStatus : RecoveryImplementationStatus
  stale : Row → Prop
  recover : Row → Row
  terminal : Row → Prop
  measure : Row → Nat
  h_stale_positive : ∀ row, stale row → measure row > 0
  h_recover_terminal : ∀ row, stale row → terminal (recover row)
  h_recover_zero : ∀ row, stale row → measure (recover row) = 0

namespace RecoverySweep

def recoveredRows (sweep : RecoverySweep) (rows : List sweep.Row) : List sweep.Row :=
  rows.map sweep.recover

def aggregateMeasure (sweep : RecoverySweep) (rows : List sweep.Row) : Nat :=
  rows.foldr (fun row acc => sweep.measure row + acc) 0

theorem recover_decreases_measure
    (sweep : RecoverySweep)
    (row : sweep.Row)
    (h_stale : sweep.stale row) :
    sweep.measure (sweep.recover row) < sweep.measure row := by
  rw [sweep.h_recover_zero row h_stale]
  exact sweep.h_stale_positive row h_stale

theorem recoveredRows_length
    (sweep : RecoverySweep)
    (rows : List sweep.Row) :
    (sweep.recoveredRows rows).length = rows.length := by
  simp [recoveredRows]

theorem recoveredRows_terminal
    (sweep : RecoverySweep)
    {rows : List sweep.Row}
    (h_all_stale : ∀ row, row ∈ rows → sweep.stale row) :
    ∀ row, row ∈ sweep.recoveredRows rows → sweep.terminal row := by
  intro row h_mem
  unfold recoveredRows at h_mem
  rcases List.mem_map.mp h_mem with ⟨pre, h_pre_mem, h_row⟩
  rw [← h_row]
  exact sweep.h_recover_terminal pre (h_all_stale pre h_pre_mem)

theorem aggregateMeasure_recovered_zero
    (sweep : RecoverySweep)
    {rows : List sweep.Row}
    (h_all_stale : ∀ row, row ∈ rows → sweep.stale row) :
    sweep.aggregateMeasure (sweep.recoveredRows rows) = 0 := by
  induction rows with
  | nil =>
      simp [aggregateMeasure, recoveredRows]
  | cons hd tl ih =>
      have h_hd : sweep.measure (sweep.recover hd) = 0 :=
        sweep.h_recover_zero hd (h_all_stale hd (by simp))
      have h_tl : ∀ row, row ∈ tl → sweep.stale row := by
        intro row h_mem
        exact h_all_stale row (by simp [h_mem])
      simp [aggregateMeasure, recoveredRows, h_hd]
      exact ih h_tl

theorem finite_stale_rows_converge
    (sweep : RecoverySweep)
    (rows : List sweep.Row)
    (h_all_stale : ∀ row, row ∈ rows → sweep.stale row) :
    ∃ results : List sweep.Row,
      results.length = rows.length ∧
      sweep.aggregateMeasure results = 0 ∧
      ∀ row, row ∈ results → sweep.terminal row := by
  refine ⟨sweep.recoveredRows rows, ?_, ?_, ?_⟩
  · exact sweep.recoveredRows_length rows
  · exact sweep.aggregateMeasure_recovered_zero h_all_stale
  · exact sweep.recoveredRows_terminal h_all_stale

end RecoverySweep

structure RecoveryEquivalence (sweep : RecoverySweep) where
  uninterrupted : sweep.Row → sweep.Row
  h_recover_eq_uninterrupted :
    ∀ row, sweep.stale row → sweep.recover row = uninterrupted row

namespace RecoveryEquivalence

def uninterruptedRows
    {sweep : RecoverySweep}
    (equivalence : RecoveryEquivalence sweep)
    (rows : List sweep.Row) : List sweep.Row :=
  rows.map equivalence.uninterrupted

theorem recoveredRows_eq_uninterruptedRows
    {sweep : RecoverySweep}
    (equivalence : RecoveryEquivalence sweep)
    {rows : List sweep.Row}
    (h_all_stale : ∀ row, row ∈ rows → sweep.stale row) :
    sweep.recoveredRows rows = equivalence.uninterruptedRows rows := by
  induction rows with
  | nil =>
      simp [RecoverySweep.recoveredRows, uninterruptedRows]
  | cons hd tl ih =>
      have h_hd :
          sweep.recover hd = equivalence.uninterrupted hd :=
        equivalence.h_recover_eq_uninterrupted hd (h_all_stale hd (by simp))
      have h_tl : ∀ row, row ∈ tl → sweep.stale row := by
        intro row h_mem
        exact h_all_stale row (by simp [h_mem])
      change
        sweep.recover hd :: sweep.recoveredRows tl =
          equivalence.uninterrupted hd :: equivalence.uninterruptedRows tl
      rw [h_hd, ih h_tl]

theorem finite_stale_rows_converge_to_uninterrupted
    {sweep : RecoverySweep}
    (equivalence : RecoveryEquivalence sweep)
    (rows : List sweep.Row)
    (h_all_stale : ∀ row, row ∈ rows → sweep.stale row) :
    ∃ results : List sweep.Row,
      results = equivalence.uninterruptedRows rows ∧
      results.length = rows.length ∧
      sweep.aggregateMeasure results = 0 ∧
      ∀ row, row ∈ results → sweep.terminal row := by
  refine ⟨sweep.recoveredRows rows, ?_, ?_, ?_, ?_⟩
  · exact recoveredRows_eq_uninterruptedRows equivalence h_all_stale
  · exact sweep.recoveredRows_length rows
  · exact sweep.aggregateMeasure_recovered_zero h_all_stale
  · exact sweep.recoveredRows_terminal h_all_stale

end RecoveryEquivalence

end Recovery
