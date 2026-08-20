import Proofs.ClientShell.Types

structure SubmitContext where
  clientAvailable   : Bool
  composerNonEmpty  : Bool
  requestedBehavior : Option BehaviorId
  deriving Repr

def behaviorMismatch
    (store : LocalStore) (sid : SessionId)
    (requested : Option BehaviorId) : Bool :=
  match requested, (store.find sid).bind (·.behaviorId) with
  | some r, some e => decide (r ≠ e)
  | _, _           => false

def trustworthyForFollowUp
    (s : ShellState) (store : LocalStore)
    (requestedBehavior : Option BehaviorId) : Bool :=
  match s.selection.session with
  | none     => true
  | some sid =>
    match store.find sid with
    | none     => false
    | some obs =>
      let tipCoherent :=
        match obs.latestObservedRequest, obs.latestTurn with
        | some _, some _ => true
        | none,   none   => true
        | _,      _      => false
      let noMismatch := ¬ behaviorMismatch store sid requestedBehavior
      tipCoherent && noMismatch

def canSubmit
    (s : ShellState) (store : LocalStore) (ctx : SubmitContext) : Bool :=
  if ¬ ctx.clientAvailable then false
  else if s.selection.agent.isNone then false
  else if ¬ ctx.composerNonEmpty then false
  else
    match s.workflow with
    | .submitting _ _ | .awaiting _ _ | .blocked _ => false
    | .idle =>
      match s.selection.session with
      | none     => true
      | some sid =>
        match store.find sid with
        | none     => false
        | some obs =>
          let tipTerminalOrUnstarted :=
            match obs.latestObservedRequest, obs.latestTurn with
            | some _, some t => t.isTerminal
            | none,   none   => true
            | _,      _      => false
          let noMismatch := ¬ behaviorMismatch store sid ctx.requestedBehavior
          tipTerminalOrUnstarted && noMismatch
