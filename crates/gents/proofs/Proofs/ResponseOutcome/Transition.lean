import Proofs.ResponseOutcome.State

namespace ResponseOutcome

inductive Step : Machine → Machine → Prop where
  | updateLive {pre post : Machine} (tailPresent : Bool)
      (h_active : pre.live.stage = .active)
      (h_post : post =
        { pre with live := { pre.live with
            revision := pre.live.revision + 1
          , tailPresent := tailPresent } }) :
      Step pre post
  | bindMessage {pre post : Machine} (message : MessageEvidence)
      (h_active : pre.live.stage = .active)
      (h_exact : message.exactFor pre.live.request = true)
      (h_post : post =
        { live := { pre.live with
            revision := pre.live.revision + 1,
            materializedMessage := some message }
        , responsePresent := pre.responsePresent
        , outcomes := pre.outcomes
        , requestTerminal := pre.requestTerminal
        , cut := .messageDurable }) :
      Step pre post
  | publishComplete {pre post : Machine} (fact : OutcomeFact)
      (h_cut : pre.cut = .messageDurable)
      (h_message : pre.live.materializedMessage = fact.finalMessage)
      (h_request : fact.request = pre.live.request)
      (h_kind : fact.kind = .complete)
      (h_fresh : publish pre.outcomes fact = (.fresh, fact :: pre.outcomes))
      (h_post : post =
        { pre with outcomes := fact :: pre.outcomes
                 , cut := .outcomeDurable }) :
      Step pre post
  | publishFailure {pre post : Machine} (fact : OutcomeFact)
      (h_present : pre.responsePresent = true)
      (h_request : fact.request = pre.live.request)
      (h_kind : fact.kind = .error ∨ fact.kind = .interrupted)
      (h_fresh : publish pre.outcomes fact = (.fresh, fact :: pre.outcomes))
      (h_post : post =
        { pre with outcomes := fact :: pre.outcomes
                 , cut := .outcomeDurable }) :
      Step pre post
  | recoverMissingResponse {pre post : Machine}
      (evidence : ClaimCommitEvidence)
      (provenance : ExecutionProvenance)
      (fact : OutcomeFact)
      (h_cut : pre.cut = .claimDurable)
      (h_missing : pre.responsePresent = false)
      (h_reconstructed : reconstructExecutionProvenance evidence = some provenance)
      (h_provenance : fact.provenance = provenance)
      (h_request : fact.request = provenance.claim)
      (h_kind : fact.kind = .error ∨ fact.kind = .interrupted)
      (h_fresh : publish pre.outcomes fact = (.fresh, fact :: pre.outcomes))
      (h_post : post =
        { pre with outcomes := fact :: pre.outcomes
                 , cut := .outcomeDurable }) :
      Step pre post
  | observeIdempotentOutcome {pre post : Machine} (fact : OutcomeFact)
      (h_observed : publish pre.outcomes fact = (.idempotent, pre.outcomes))
      (h_post : post = pre) :
      Step pre post
  | rejectConflictingOutcome {pre post : Machine} (fact : OutcomeFact)
      (h_rejected : publish pre.outcomes fact = (.rejected, pre.outcomes))
      (h_post : post = pre) :
      Step pre post
  | terminalizeRequest {pre post : Machine}
      (h_outcome : pre.cut = .outcomeDurable)
      (h_not_terminal : pre.requestTerminal = false)
      (h_post : post = { pre with requestTerminal := true, cut := .requestTerminal }) :
      Step pre post
  | supersedeLive {pre post : Machine}
      (h_terminal : pre.requestTerminal = true)
      (h_active : pre.live.stage = .active)
      (h_post : post =
        { live := { pre.live with stage := .superseded }
        , responsePresent := pre.responsePresent
        , outcomes := pre.outcomes
        , requestTerminal := pre.requestTerminal
        , cut := .liveSuperseded }) :
      Step pre post

inductive Trace : Machine → Machine → Prop where
  | refl {machine : Machine} : Trace machine machine
  | step {pre next post : Machine} : Step pre next → Trace next post → Trace pre post

end ResponseOutcome
