import Proofs.Workspace.Types

namespace IsolatedWorkspace

inductive Transition : IsolatedWorkspace → IsolatedWorkspace → Prop where
  | provision_ready {pre post : IsolatedWorkspace} :
      pre.state = .provisioning →
      post = { pre with state := .ready } →
      Transition pre post
  | provision_failed {pre post : IsolatedWorkspace} :
      pre.state = .provisioning →
      post = { pre with state := .provisionFailed } →
      Transition pre post
  | seal {pre post : IsolatedWorkspace} :
      pre.state = .ready →
      pre.sealHash.isSome = true →
      post = { pre with state := .sealed } →
      Transition pre post
  | begin_cleanup {pre post : IsolatedWorkspace} :
      pre.state = .sealed →
      post = { pre with state := .cleaning } →
      Transition pre post
  | finish_cleanup {pre post : IsolatedWorkspace} :
      pre.state = .cleaning →
      post = { pre with state := .cleaned } →
      Transition pre post

def transitionLegal (pre : IsolatedWorkspace) (postState : WorkspaceState) : Bool :=
  match pre.state, postState with
  | .provisioning, .ready => true
  | .provisioning, .provisionFailed => true
  | .ready, .sealed => pre.sealHash.isSome
  | .sealed, .cleaning => true
  | .cleaning, .cleaned => true
  | _, _ => false

def step? (pre : IsolatedWorkspace) (postState : WorkspaceState) :
    Option IsolatedWorkspace :=
  if transitionLegal pre postState then
    some { pre with state := postState }
  else
    none

end IsolatedWorkspace
