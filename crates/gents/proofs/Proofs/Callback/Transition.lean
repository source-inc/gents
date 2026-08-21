import Proofs.Callback.Types

namespace CallbackInvocation

inductive Transition : CallbackInvocation → CallbackInvocation → Prop where
  | claim {pre post : CallbackInvocation} :
      pre.state = .pending →
      post = { pre with state := .claimed } →
      Transition pre post
  | run {pre post : CallbackInvocation} :
      pre.state = .claimed →
      post = { pre with state := .running } →
      Transition pre post
  | succeed {pre post : CallbackInvocation} :
      pre.state = .running →
      pre.journal.all (fun e => decide (e.state = .resultDocsWritten)) = true →
      post = { pre with state := .succeeded, resultEmitted := true } →
      Transition pre post
  | fail {pre post : CallbackInvocation} :
      pre.state = .running →
      pre.journal = [] →
      post = { pre with state := .failed, resultEmitted := false } →
      Transition pre post
  | deny_claimed {pre post : CallbackInvocation} :
      pre.state = .claimed →
      pre.journal = [] →
      post = { pre with state := .denied, resultEmitted := false } →
      Transition pre post
  | deny_running {pre post : CallbackInvocation} :
      pre.state = .running →
      pre.journal = [] →
      post = { pre with state := .denied, resultEmitted := false } →
      Transition pre post

end CallbackInvocation
