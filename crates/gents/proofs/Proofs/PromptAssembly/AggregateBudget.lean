import Proofs.Basic

/-!
# Request-wide provider token budget

One durable agent request may issue several provider calls: tool turns and
completed attempts which are later retracted all consume provider tokens. This
model gives those calls one monotone ledger. The runtime must constrain every
dispatch from the ledger and charge every non-zero provider usage report.

The provider tokenizer and the truthfulness of its usage report remain external
assumptions. Production checks the report after each call and fails closed when
it is absent or would put the ledger over its declared limit.
-/

namespace PromptAssembly.AggregateBudget

structure Usage where
  inputTokens : Nat
  outputTokens : Nat
  reportedTotal : Nat
  deriving DecidableEq, Repr

/-- Charge the larger of the provider's total and its reported input/output
sum. An internally inconsistent total can therefore never undercharge. -/
def Usage.chargedTotal (usage : Usage) : Nat :=
  max usage.reportedTotal (usage.inputTokens + usage.outputTokens)

structure Ledger where
  limit : Nat
  used : Nat
  deriving DecidableEq, Repr

def Ledger.remaining (ledger : Ledger) : Nat :=
  ledger.limit - ledger.used

/-- Constrain a call's configured output ceiling by the request-wide budget
remaining after its assembled-input estimate. -/
def effectiveOutputBudget
    (ledger : Ledger) (inputTokens configuredMaxOutputTokens : Nat) : Nat :=
  min configuredMaxOutputTokens (ledger.remaining - inputTokens)

def CanDispatch
    (ledger : Ledger) (inputTokens configuredMaxOutputTokens : Nat) : Prop :=
  0 < effectiveOutputBudget ledger inputTokens configuredMaxOutputTokens

instance (ledger : Ledger) (inputTokens configuredMaxOutputTokens : Nat) :
    Decidable (CanDispatch ledger inputTokens configuredMaxOutputTokens) := by
  unfold CanDispatch
  infer_instance

def Ledger.charge (ledger : Ledger) (usage : Usage) : Ledger :=
  { ledger with used := ledger.used + usage.chargedTotal }

inductive ChargeResult where
  | missing
  | within (ledger : Ledger)
  | exhausted (ledger : Ledger)
  | overrun (ledger : Ledger)
  deriving DecidableEq, Repr

inductive PostChargeAction where
  | continue
  | succeed
  | fail
  deriving DecidableEq, Repr

/-- Post-call legality. Exact exhaustion may publish only an already-valid
terminal response; it cannot admit a tool turn, empty response, or failed
structured-output contract. Missing usage and observed overrun always fail. -/
def postChargeAction (result : ChargeResult) (terminalValid : Bool) : PostChargeAction :=
  match result with
  | .missing | .overrun _ => .fail
  | .within _ => if terminalValid then .succeed else .continue
  | .exhausted _ => if terminalValid then .succeed else .fail

/-- A missing or all-zero provider usage report is not enforceable. Otherwise
the post-call ledger distinguishes remaining capacity, exact exhaustion, and
an observed overrun. -/
def chargeReported (ledger : Ledger) (usage : Option Usage) : ChargeResult :=
  match usage with
  | none => .missing
  | some report =>
      if report.chargedTotal = 0 then
        .missing
      else
        let next := ledger.charge report
        if next.used > next.limit then
          .overrun next
        else if next.used = next.limit then
          .exhausted next
        else
          .within next

theorem charged_total_covers_components (usage : Usage) :
    usage.inputTokens + usage.outputTokens ≤ usage.chargedTotal := by
  exact Nat.le_max_right _ _

theorem charge_monotone (ledger : Ledger) (usage : Usage) :
    ledger.used ≤ (ledger.charge usage).used := by
  simp [Ledger.charge]

/-- If the ledger starts within its limit, the dispatch clamp keeps the
estimated input plus requested output within the unspent request budget. -/
theorem dispatch_respects_remaining
    {ledger : Ledger} {inputTokens configuredMaxOutputTokens : Nat}
    (hwithin : ledger.used ≤ ledger.limit)
    (hdispatch : CanDispatch ledger inputTokens configuredMaxOutputTokens) :
    ledger.used + inputTokens +
      effectiveOutputBudget ledger inputTokens configuredMaxOutputTokens ≤
        ledger.limit := by
  have hinput : inputTokens ≤ ledger.remaining := by
    unfold CanDispatch effectiveOutputBudget at hdispatch
    omega
  unfold effectiveOutputBudget Ledger.remaining at hinput ⊢
  omega

theorem exhausted_cannot_dispatch
    {ledger : Ledger} {inputTokens configuredMaxOutputTokens : Nat}
    (hexhausted : ledger.limit ≤ ledger.used) :
    ¬ CanDispatch ledger inputTokens configuredMaxOutputTokens := by
  unfold CanDispatch effectiveOutputBudget Ledger.remaining
  omega

theorem exhausted_succeeds_iff_terminal_valid
    (ledger : Ledger) (terminalValid : Bool) :
    postChargeAction (.exhausted ledger) terminalValid = .succeed ↔
      terminalValid = true := by
  cases terminalValid <;> simp [postChargeAction]

theorem missing_usage_fails (terminalValid : Bool) :
    postChargeAction .missing terminalValid = .fail := by
  simp [postChargeAction]

/-- Charging two calls is additive and order-independent. This is the key
retry property: a completed attempt that is later retracted still consumes the
same ledger as the replacement attempt. -/
theorem two_charges_are_additive
    (ledger : Ledger) (first second : Usage) :
    ((ledger.charge first).charge second).used =
      ledger.used + first.chargedTotal + second.chargedTotal := by
  rfl

end PromptAssembly.AggregateBudget
