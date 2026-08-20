import Proofs.ClientShell.Submission

def workflowAfterSelectSession
    (sid : SessionId) : SubmissionWorkflow → SubmissionWorkflow
  | .blocked _        => .idle
  | .awaiting sid' req =>
      if sid' = sid then .awaiting sid' req else .idle
  | w                 => w

def snapshotAdvanceWorkflow
    (w : SubmissionWorkflow) (store : LocalStore) : SubmissionWorkflow :=
  match w with
  | .awaiting sid req =>
    match store.find sid with
    | some obs =>
      if obs.latestObservedRequest = some req then .idle else w
    | none     => w
  | w' => w'

def step
    (s : ShellState) (input : ShellInput)
    (store : LocalStore) (_transport : TransportHealth)
    (ctx : SubmitContext) : ShellState :=
  match input with
  | .user .requestNewConversation =>
      { s with
          selection := { s.selection with session := none },
          workflow  := .idle }
  | .user (.selectDeployment p a) =>
      { s with
          selection := { s.selection with peer := some p, agent := some a, session := none },
          workflow  := .idle }
  | .user (.selectSession sid) =>
      let cleared := workflowAfterSelectSession sid s.workflow
      { s with
          selection := { s.selection with session := some sid },
          workflow  := cleared }
  | .user .startSubmit =>
      if canSubmit s store ctx then
        match s.selection.agent with
        | none   => s
        | some a => { s with workflow := .submitting a s.selection.session }
      else s
  | .user .acknowledgeBlocker =>
      match s.workflow with
      | .blocked _ => { s with workflow := .idle }
      | _          => s
  | .snapshot store' =>
      { s with workflow := snapshotAdvanceWorkflow s.workflow store' }
  | .mutation (.submitted sid req) =>
      { s with
          selection := { s.selection with session := some sid },
          workflow  := .awaiting sid req }
  | .mutation (.failed r) =>
      { s with workflow := .blocked r }
  | .transport _ => s
