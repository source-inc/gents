import Proofs.ToolFact.Transition

namespace ToolFact

theorem call_twin_rejected
    (state : State) (intent : ToolCallIntent) (evidence : SignedRef)
    (h_args : intent.argsHash ≠ 0)
    (h_evidence : evidence.authoritative = true)
    (left right : Nat) :
    (commitCall state [left, right] intent evidence).disposition = .rejected := by
  simp [commitCall, h_args, h_evidence]
  split <;> rfl

theorem call_replay_idempotent
    (state : State) (intent : ToolCallIntent) (evidence : SignedRef)
    (h_args : intent.argsHash ≠ 0)
    (h_evidence : evidence.authoritative = true)
    (h_existing : state.calls evidence.version.docId =
      some (ToolCallFact.forIntent intent evidence)) :
    commitCall state [evidence.version.docId] intent evidence =
      ⟨.observedIdentical, state⟩ := by
  simp [commitCall, h_args, h_evidence, h_existing]

theorem result_twin_rejected
    (state : State) (intent : ToolResultIntent) (evidence : SignedRef)
    (call : ToolCallFact)
    (h_call : exactCall? state.calls intent.call = some call)
    (h_full : intent.fullOutput = true)
    (h_output : intent.outputHash ≠ 0)
    (h_evidence : evidence.authoritative = true)
    (left right : Nat) :
    (commitResult state [left, right] intent evidence).disposition = .rejected := by
  simp [commitResult, h_call, h_full, h_output, h_evidence]
  split <;> rfl

theorem incomplete_result_rejected
    (state : State) (visible : List Nat) (intent : ToolResultIntent)
    (evidence : SignedRef) (h_incomplete : intent.fullOutput = false) :
    (commitResult state visible intent evidence).disposition = .rejected := by
  simp [commitResult, h_incomplete]
  split <;> rfl

theorem result_replay_idempotent
    (state : State) (intent : ToolResultIntent) (evidence : SignedRef)
    (call : ToolCallFact)
    (h_call : exactCall? state.calls intent.call = some call)
    (h_full : intent.fullOutput = true)
    (h_output : intent.outputHash ≠ 0)
    (h_evidence : evidence.authoritative = true)
    (h_existing : state.results evidence.version.docId =
      some (ToolResultFact.forIntent intent evidence)) :
    commitResult state [evidence.version.docId] intent evidence =
      ⟨.observedIdentical, state⟩ := by
  simp [commitResult, h_call, h_full, h_output, h_evidence, h_existing]

theorem result_payload_rebinding_rejected
    (state : State) (intent : ToolResultIntent) (evidence : SignedRef)
    (call : ToolCallFact) (existing : ToolResultFact)
    (h_call : exactCall? state.calls intent.call = some call)
    (h_full : intent.fullOutput = true)
    (h_output : intent.outputHash ≠ 0)
    (h_evidence : evidence.authoritative = true)
    (h_existing : state.results evidence.version.docId = some existing)
    (h_conflict : existing ≠ ToolResultFact.forIntent intent evidence) :
    (commitResult state [evidence.version.docId] intent evidence).disposition = .rejected := by
  simp [commitResult, h_call, h_full, h_output, h_evidence, h_existing, h_conflict]

theorem approval_replay_idempotent
    (state : State) (intent : ToolApprovalIntent) (evidence : SignedRef)
    (call : ToolCallFact)
    (h_call : exactCall? state.calls intent.call = some call)
    (h_evidence : evidence.authoritative = true)
    (h_existing : state.approvals evidence.version.docId =
      some (ToolApprovalFact.forIntent intent evidence)) :
    commitApproval state [evidence.version.docId] intent evidence =
      ⟨.observedIdentical, state⟩ := by
  simp [commitApproval, h_call, h_evidence, h_existing]

theorem approval_twin_rejected
    (state : State) (intent : ToolApprovalIntent) (evidence : SignedRef)
    (call : ToolCallFact)
    (h_call : exactCall? state.calls intent.call = some call)
    (h_evidence : evidence.authoritative = true)
    (left right : Nat) :
    (commitApproval state [left, right] intent evidence).disposition = .rejected := by
  simp [commitApproval, h_call, h_evidence]
  split <;> rfl

theorem projection_exact_result_pins_call
    {state : State} {join : TranscriptJoin} {projection : Projection}
    (h_project : projectExact state join = some projection) :
    projection.result.call = join.call := by
  cases h_call : exactCall? state.calls join.call with
  | none => simp [projectExact, h_call] at h_project
  | some call =>
      cases h_result : exactResult? state.results join.result with
      | none => simp [projectExact, h_call, h_result] at h_project
      | some result =>
          by_cases h_pins : result.call = join.call
          · cases h_approval : join.approval with
            | none =>
                simp [projectExact, h_call, h_result, h_pins, h_approval] at h_project
                subst projection
                exact h_pins
            | some approvalRef =>
                cases h_exact : exactApproval? state.approvals approvalRef with
                | none =>
                    simp [projectExact, h_call, h_result, h_pins, h_approval,
                      h_exact] at h_project
                | some approval =>
                    by_cases h_approval_pins : approval.call = join.call
                    · simp [projectExact, h_call, h_result, h_pins, h_approval,
                        h_exact, h_approval_pins] at h_project
                      subst projection
                      exact h_pins
                    · simp [projectExact, h_call, h_result, h_pins, h_approval,
                        h_exact, h_approval_pins] at h_project
          · simp [projectExact, h_call, h_result, h_pins] at h_project

theorem projection_exact_approval_pins_call
    {state : State} {join : TranscriptJoin} {projection : Projection}
    {approval : ToolApprovalFact}
    (h_project : projectExact state join = some projection)
    (h_approval : projection.approval = some approval) :
    approval.call = join.call := by
  cases h_call : exactCall? state.calls join.call with
  | none => simp [projectExact, h_call] at h_project
  | some call =>
      cases h_result : exactResult? state.results join.result with
      | none => simp [projectExact, h_call, h_result] at h_project
      | some result =>
          by_cases h_result_pins : result.call = join.call
          · cases h_ref : join.approval with
            | none =>
                simp [projectExact, h_call, h_result, h_result_pins, h_ref] at h_project
                subst projection
                simp at h_approval
            | some approvalRef =>
                cases h_exact : exactApproval? state.approvals approvalRef with
                | none =>
                    simp [projectExact, h_call, h_result, h_result_pins, h_ref,
                      h_exact] at h_project
                | some resolved =>
                    by_cases h_pins : resolved.call = join.call
                    · simp [projectExact, h_call, h_result, h_result_pins, h_ref,
                        h_exact, h_pins] at h_project
                      subst projection
                      simp at h_approval
                      subst approval
                      exact h_pins
                    · simp [projectExact, h_call, h_result, h_result_pins, h_ref,
                        h_exact, h_pins] at h_project
          · simp [projectExact, h_call, h_result, h_result_pins] at h_project

end ToolFact
