import Proofs.Transcript.State

/-!
# Claude content-block map (Track B / B2)

Anthropic Messages HTTP is the only Claude wire. Its `tool_use` /
`tool_result` content blocks are mapped here onto existing `ToolCallId`s /
`MessageKind` rows. This file does not model the HTTP transport, the agent's subscription credential
token read, or oat.

Unmapped names (including Claude-native `Bash`) and `tool_use` on an empty
gents surface fail closed: the block does not become a call. No aliases.
Text-only turns (no `tool_use`) succeed even on an empty surface.

Id identity: a successful map returns the same `ToolCallId` it was given.
The runtime's `toolu_*` → `Nat` injection is a separate plumbing obligation
(`UniqueCallIds`); this map does not remap.
-/

namespace PromptAssembly.ClaudeMap

open ToolExecution (ToolCallId)
open Transcript (MessageKind)

/-- One Claude content block. `name` is the on-the-wire tool name. -/
inductive Block where
  | text
  | toolUse (id : ToolCallId) (name : String)
  | toolResult (id : ToolCallId)
  deriving DecidableEq, Repr

/-- Gents names advertised this turn. Empty = A2b text-only fence. -/
abbrev Surface := Finset String

inductive MapError where
  | emptySurface
  | unmappedName (name : String)
  | duplicateId (id : ToolCallId)
  | overlappingBlock (id : ToolCallId)
  deriving DecidableEq, Repr

def errorName : MapError → String
  | .emptySurface => "emptySurface"
  | .unmappedName name => "unmappedName:" ++ name
  | .duplicateId id => "duplicateId:" ++ toString id
  | .overlappingBlock id => "overlappingBlock:" ++ toString id

def blockTag : Block → String
  | .text => "text"
  | .toolUse id name => s!"toolUse:{id}:{name}"
  | .toolResult id => s!"toolResult:{id}"

/-- Map one `tool_use`. Empty surface or a name not on it fails closed. -/
def mapToolUse (surface : Surface) (id : ToolCallId) (name : String) :
    Except MapError ToolCallId :=
  if surface = ∅ then
    .error .emptySurface
  else if name ∈ surface then
    .ok id
  else
    .error (.unmappedName name)

theorem mapToolUse_empty (id : ToolCallId) (name : String) :
    mapToolUse ∅ id name = .error .emptySurface := by
  simp [mapToolUse]

theorem mapToolUse_ok_mem {surface : Surface} {id : ToolCallId} {name : String}
    (h : mapToolUse surface id name = .ok id) : name ∈ surface := by
  unfold mapToolUse at h
  by_cases hEmpty : surface = ∅
  · simp [hEmpty] at h
  · by_cases hMem : name ∈ surface
    · exact hMem
    · simp [hEmpty, hMem] at h

theorem mapToolUse_ok_nonempty {surface : Surface} {id : ToolCallId} {name : String}
    (h : mapToolUse surface id name = .ok id) : surface ≠ ∅ := by
  intro hempty
  simp [mapToolUse, hempty] at h

theorem mapToolUse_preserves_id {surface : Surface} {id id' : ToolCallId}
    {name : String} (h : mapToolUse surface id name = .ok id') : id' = id := by
  unfold mapToolUse at h
  by_cases hEmpty : surface = ∅
  · simp [hEmpty] at h
  · by_cases hMem : name ∈ surface
    · simp [hEmpty, hMem] at h
      exact h.symm
    · simp [hEmpty, hMem] at h

theorem mapToolUse_unmapped {surface : Surface} {id : ToolCallId} {name : String}
    (hne : surface ≠ ∅) (hmem : name ∉ surface) :
    mapToolUse surface id name = .error (.unmappedName name) := by
  simp [mapToolUse, hne, hmem]

/-- `Bash` is not `bash`. No aliases. -/
theorem mapToolUse_no_bash_alias (id : ToolCallId) :
    mapToolUse {("bash" : String)} id "Bash" = .error (.unmappedName "Bash") := by
  apply mapToolUse_unmapped
  · simp
  · simp

def toolUsePairs : List Block → List (ToolCallId × String)
  | [] => []
  | .toolUse id name :: rest => (id, name) :: toolUsePairs rest
  | _ :: rest => toolUsePairs rest

def mapPairs (surface : Surface) :
    List (ToolCallId × String) → Except MapError (Finset ToolCallId)
  | [] => .ok ∅
  | (id, name) :: rest =>
    match mapPairs surface rest with
    | .error e => .error e
    | .ok acc =>
      if id ∈ acc then
        .error (.duplicateId id)
      else
        match mapToolUse surface id name with
        | .error e => .error e
        | .ok _ => .ok (insert id acc)

/-- Assistant-turn map: `tool_use` blocks → native call-id set, or fail closed. -/
def mapTurn (surface : Surface) (blocks : List Block) :
    Except MapError (Finset ToolCallId) :=
  mapPairs surface (toolUsePairs blocks)

theorem mapTurn_text_only (surface : Surface) :
    mapTurn surface [.text] = .ok ∅ := rfl

theorem mapTurn_empty_surface_text :
    mapTurn ∅ [.text] = .ok ∅ := rfl

theorem mapTurn_empty_surface_tool_use (id : ToolCallId) :
    mapTurn ∅ [.toolUse id "bash"] = .error .emptySurface := by
  simp [mapTurn, mapPairs, toolUsePairs, mapToolUse]

theorem mapTurn_mapped (name : String) (id : ToolCallId) :
    name ∈ ({name} : Surface) →
    mapTurn {name} [.toolUse id name] = .ok {id} := by
  intro hmem
  simp [mapTurn, mapPairs, toolUsePairs, mapToolUse, hmem]

theorem mapTurn_unmapped (id : ToolCallId) :
    mapTurn {("bash" : String)} [.toolUse id "Bash"] =
      .error (.unmappedName "Bash") := by
  simp [mapTurn, mapPairs, toolUsePairs, mapToolUse_no_bash_alias]

theorem mapTurn_duplicate (id : ToolCallId) :
    mapTurn {("bash" : String)}
        [.toolUse id "bash", .toolUse id "bash"] =
      .error (.duplicateId id) := by
  have hmem : ("bash" : String) ∈ ({("bash" : String)} : Surface) := by simp
  simp [mapTurn, mapPairs, toolUsePairs, mapToolUse, hmem]

/-- A successful turn's call ids are exactly the `tool_use` ids, as `MessageKind`. -/
def mappedKind (ids : Finset ToolCallId) : MessageKind :=
  if ids = ∅ then .ordinary else .assistantToolCalls ids

theorem mappedKind_text : mappedKind ∅ = .ordinary := by
  simp [mappedKind]

theorem mappedKind_calls (id : ToolCallId) :
    mappedKind {id} = .assistantToolCalls {id} := by
  simp [mappedKind]

/-! ## System assembly (single-wire Messages HTTP)

`Transcript.MessageRole` has no `system`; the wire-side row type here carries
just what assembly needs: a `System` row's text, or "some other row". -/

inductive Msg where
  | system (text : String)
  | other (tag : String)
  deriving DecidableEq, Repr

/-- The Claude Code identity block. `system[0]` on every request; the agent's
subscription credential routes on it. Checked against Rust `CLAUDE_CODE_IDENTITY`
by the vocab test. -/
def identity : String := "You are Claude Code, Anthropic's official CLI for Claude."

/-- Pull `System` rows out in order; everything else is untouched. Rust also
trims a whitespace-only preamble and drops blank `System` rows before this
split; the model does not represent that and no witness covers it. -/
def splitSystem : List Msg → List String × List Msg
  | [] => ([], [])
  | .system t :: rest =>
    let (sys, others) := splitSystem rest
    (t :: sys, others)
  | m :: rest =>
    let (sys, others) := splitSystem rest
    (sys, m :: others)

/-- `system[]` on the wire: identity first, then the preamble, then the `System`
rows verbatim (blank-row dropping happens in Rust before `rows` is built). -/
def systemBlocks (preamble : Option String) (rows : List String) : List String :=
  identity :: (preamble.toList ++ rows)

theorem systemBlocks_head (preamble : Option String) (rows : List String) :
    (systemBlocks preamble rows).head? = some identity := rfl

theorem systemBlocks_tail_verbatim (preamble : Option String) (rows : List String) :
    (systemBlocks preamble rows).tail = preamble.toList ++ rows := rfl

def isSystem : Msg → Bool
  | .system _ => true
  | .other _ => false

/-- The remaining list contains no `System` row, and the split loses nothing:
the system texts are exactly the `System` rows in order. -/
theorem splitSystem_partition (msgs : List Msg) :
    (splitSystem msgs).2.all (fun m => !isSystem m) = true ∧
    (splitSystem msgs).1 = (msgs.filter isSystem).map (fun m =>
      match m with | .system t => t | .other _ => "") ∧
    (splitSystem msgs).2 = msgs.filter (fun m => !isSystem m) := by
  induction msgs with
  | nil => simp [splitSystem]
  | cons m rest ih =>
    obtain ⟨h1, h2, h3⟩ := ih
    cases m with
    | system t => simp [splitSystem, isSystem, List.filter, h1, h2, h3]
    | other tag => simp [splitSystem, isSystem, List.filter, h1, h2, h3]

/-- `tools` is absent from the wire for an empty surface. -/
def toolsField : List String → Option (List String)
  | [] => none
  | tools => some tools

theorem toolsField_empty : toolsField [] = none := rfl

theorem toolsField_nonempty (t : String) (rest : List String) :
    toolsField (t :: rest) = some (t :: rest) := rfl

/-! ## Tool-block accumulation (SSE)

One `tool_use` block arrives as `content_block_start` (with a usually-empty
`input`), zero or more `input_json_delta` fragments, and `content_block_stop`.
Defect C1 seeded the start input and appended deltas (`{}{...}`). Here the
deltas are the arguments whenever any arrived. -/

inductive StreamEvent where
  | text (t : String)
  | start (id : ToolCallId) (name : String) (input : Option String)
  | delta (fragment : String)
  | stop
  deriving DecidableEq, Repr

def accumulate (start : Option String) (deltas : List String) : String :=
  match deltas with
  | [] => start.getD "{}"
  | _ => String.join deltas

theorem accumulate_ignores_start_when_streamed (start : Option String)
    (deltas : List String) (h : deltas ≠ []) :
    accumulate start deltas = String.join deltas := by
  cases deltas with
  | nil => exact absurd rfl h
  | cons d rest => rfl

theorem accumulate_uses_start_when_no_deltas (start : Option String) :
    accumulate start [] = start.getD "{}" := rfl

structure Pending where
  id : ToolCallId
  name : String
  start : Option String
  /-- Deltas in reverse arrival order (consed); flushed as `deltas.reverse`. -/
  deltas : List String
  deriving Repr

structure StreamState where
  pending : Option Pending
  seen : List ToolCallId
  out : List (ToolCallId × String)
  deriving Repr

def StreamState.init : StreamState := { pending := none, seen := [], out := [] }

/-- Flush the pending block: duplicate id, then surface map, then arguments. -/
def flush (surface : Surface) (st : StreamState) : Except MapError StreamState :=
  match st.pending with
  | none => .ok st
  | some p =>
    if p.id ∈ st.seen then
      .error (.duplicateId p.id)
    else
      match mapToolUse surface p.id p.name with
      | .error e => .error e
      | .ok _ =>
        .ok { pending := none
            , seen := p.id :: st.seen
            , out := st.out ++ [(p.id, accumulate p.start p.deltas.reverse)] }

def step (surface : Surface) (st : StreamState) : StreamEvent → Except MapError StreamState
  | .text _ => .ok st
  | .start id name input =>
    match st.pending with
    | some _ => .error (.overlappingBlock id)
    | none => .ok { st with pending := some { id := id, name := name, start := input, deltas := [] } }
  | .delta fragment =>
    match st.pending with
    | none => .ok st
    | some p => .ok { st with pending := some { p with deltas := fragment :: p.deltas } }
  | .stop => flush surface st

/-- Left to right, first error wins; end of stream flushes an unterminated block. -/
def runStream (surface : Surface) (events : List StreamEvent) :
    Except MapError (List (ToolCallId × String)) :=
  (events.foldlM (step surface) StreamState.init >>= flush surface) |>.map (·.out)

/-- `Except` ships no `DecidableEq`; the `runStream` witnesses below decide
equality on `Except MapError (List (ToolCallId × String))`. -/
local instance instDecidableEqExcept {ε α : Type} [DecidableEq ε] [DecidableEq α] :
    DecidableEq (Except ε α)
  | .error a, .error b =>
    if h : a = b then .isTrue (h ▸ rfl) else .isFalse (fun e => h (Except.error.inj e))
  | .error _, .ok _ => .isFalse nofun
  | .ok _, .error _ => .isFalse nofun
  | .ok a, .ok b =>
    if h : a = b then .isTrue (h ▸ rfl) else .isFalse (fun e => h (Except.ok.inj e))

theorem runStream_text_only (surface : Surface) :
    runStream surface [.text "hi"] = .ok [] := rfl

theorem runStream_deltas_win :
    runStream {("echo" : String)}
      [.start 1 "echo" (some "{}"), .delta "{\"text\":", .delta " \"hi\"}", .stop] =
      .ok [(1, "{\"text\": \"hi\"}")] := by
  native_decide

theorem runStream_start_input_without_deltas :
    runStream {("echo" : String)} [.start 1 "echo" (some "{\"a\":1}"), .stop] =
      .ok [(1, "{\"a\":1}")] := by
  native_decide

theorem runStream_overlap :
    runStream {("echo" : String)} [.start 1 "echo" none, .start 2 "echo" none, .stop] =
      .error (.overlappingBlock 2) := by
  native_decide

theorem runStream_duplicate :
    runStream {("echo" : String)}
      [.start 1 "echo" none, .stop, .start 1 "echo" none, .stop] =
      .error (.duplicateId 1) := by
  native_decide

theorem runStream_unterminated_flushes :
    runStream {("echo" : String)} [.start 1 "echo" none, .delta "{}"] = .ok [(1, "{}")] := by
  native_decide

end PromptAssembly.ClaudeMap
