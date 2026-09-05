import Proofs.Conformance.ContractCases.Types
import Proofs.PromptAssembly

/-!
# PromptAssembly contract cases

Witness rows for the provider-input sanitizer, the assembled layer order, and
tool-argument repair.

**Every expected value in this file is computed by running the Lean model**, not
written by hand. `expected` is literally `Provider.sanitizeForProvider input`;
`expectedTwice` is the model applied twice; `splits` are the model applied to
each suffix. That is what makes the Rust fence mechanical rather than social: a
change to either the model or the Rust sanitizer breaks the equality, and no
human transcription sits in between.

Provider-validity of each expected output is *not* emitted as data, because it
is a theorem — `Provider.sanitizeForProvider_sound`. If production reproduces
the emitted output exactly, it inherits that validity. This is why the Rust
fence needs no hand-rolled pairing oracle.
-/

namespace Conformance.ContractCases

open PromptAssembly.Content (Item)
open PromptAssembly.Provider (ProviderRow)

/-- A content item, flattened for emission. `value` is the text/reasoning index
for `text`/`other`, and the tool-call id for `call`. -/
structure PromptAssemblyItemCase where
  item : String
  value : Nat
  deriving Repr

structure PromptAssemblyRowCase where
  role : String
  kind : String
  callIds : List Nat
  content : List PromptAssemblyItemCase
  deriving Repr

structure PromptAssemblySplitCase where
  index : Nat
  expected : List PromptAssemblyRowCase
  deriving Repr

structure PromptAssemblySanitizeCase where
  name : String
  input : List PromptAssemblyRowCase
  expected : List PromptAssemblyRowCase
  expectedTwice : List PromptAssemblyRowCase
  splits : List PromptAssemblySplitCase
  deriving Repr

structure PromptAssemblyLayerCase where
  name : String
  skillCount : Nat
  summaryCount : Nat
  conversationLen : Nat
  slots : List String
  deriving Repr

structure PromptAssemblyRepairCase where
  name : String
  input : String
  expected : String
  expectedTwice : String
  payloadOnly : Bool
  deriving Repr

structure PromptAssemblyBudgetCase where
  name : String
  contextWindow : Nat
  maxOutputTokens : Nat
  thresholdBasisPoints : Nat
  configuredThresholdBudget : Nat
  promptTokens : Nat
  requestTokens : Nat
  effectiveInputBudget : Nat
  effectiveOutputTokens : Nat
  shouldCompact : Bool
  providerSafe : Bool
  canDispatch : Bool
  deriving Repr

structure PromptAssemblyTurnBudgetCase where
  name : String
  contextWindow : Nat
  maxOutputTokens : Nat
  thresholdBasisPoints : Nat
  configuredThresholdBudget : Nat
  effectiveInputBudget : Nat
  turnInputTokens : List Nat
  turnOutputTokens : List Nat
  turnShouldCompact : List Bool
  turnCanDispatch : List Bool
  deriving Repr

structure PromptAssemblyRetentionCase where
  name : String
  configuredKeepRecent : Nat
  effectiveInputBudget : Nat
  fixedInput : Nat
  availableInput : Nat
  retentionTarget : Nat
  summaryMaxOutput : Nat
  effectiveSummaryOutput : Nat
  rollingSummaryInputBudget : Nat
  deriving Repr

/-! ## Building witness rows -/

private def itemCase : Item → PromptAssemblyItemCase
  | .text index => { item := "text", value := index }
  | .other index => { item := "other", value := index }
  | .call callId => { item := "call", value := callId }

/-- The announced call ids, read off the content in content order.

`Finset` has no computable ordered projection here (`Finset.toList` is
noncomputable and the contract generator must actually run), so the emitted list
comes from the content instead. Coherence makes that the same set: for an
assistant row, `Content.callsOf content = callIds` by `Coherent`, which
`witnessesAreCoherent` below discharges for every input witness and
`Provider.allCoherent_sanitizeForProvider` propagates to every output. -/
private def announcedIds (content : List Item) : List Nat :=
  content.filterMap Item.callId?

private def roleName : Transcript.MessageRole → String
  | .user => "user"
  | .assistant => "assistant"

private def rowCase (pr : ProviderRow) : PromptAssemblyRowCase :=
  match pr.row.kind with
  | .ordinary =>
    { role := roleName pr.row.role
    , kind := "ordinary"
    , callIds := []
    , content := pr.content.map itemCase }
  | .assistantToolCalls _ =>
    { role := roleName pr.row.role
    , kind := "assistantToolCalls"
    , callIds := announcedIds pr.content
    , content := pr.content.map itemCase }
  | .toolResult callId _ =>
    { role := roleName pr.row.role
    , kind := "toolResult"
    , callIds := [callId]
    , content := pr.content.map itemCase }

private def rowCases (rows : List ProviderRow) : List PromptAssemblyRowCase :=
  rows.map rowCase

/-! ## The witness transcripts

Constructed so that every row is `Coherent`: an assistant row's announced call
set is exactly the set of `call` items in its content. -/

private def mkRow (sequence : Nat) (role : Transcript.MessageRole)
    (kind : Transcript.MessageKind) (content : List Item) : ProviderRow :=
  { row := ⟨sequence, 0, sequence, role, kind⟩, content := content }

/-- An assistant turn announcing `ids`, optionally carrying prose first. -/
private def assistantCalls (sequence : Nat) (ids : List Nat)
    (prose : List Item := []) : ProviderRow :=
  mkRow sequence .assistant (.assistantToolCalls ids.toFinset)
    (prose ++ ids.map Item.call)

/-- A tool result closing `callId`. Production threads exactly one result per
user message (`loop_stream.rs`), so one row is one result. -/
private def toolResult (sequence : Nat) (callId : Nat) : ProviderRow :=
  mkRow sequence .user (.toolResult callId ⟨0, callId, 0⟩) []

/-- Ordinary user prose. -/
private def userText (sequence : Nat) (index : Nat) : ProviderRow :=
  mkRow sequence .user .ordinary [Item.text index]

/-- Assistant prose with no tool calls. -/
private def assistantText (sequence : Nat) (index : Nat) : ProviderRow :=
  mkRow sequence .assistant .ordinary [Item.text index]

private def witnessTranscripts : List (String × List ProviderRow) :=
  [ ("empty", [])
  , ("clean-paired-turn",
      [ userText 0 0
      , assistantCalls 1 [1, 2]
      , toolResult 2 1
      , toolResult 3 2
      , userText 4 1 ])
  , ("orphaned-result-before-its-call",
      [ toolResult 0 1
      , assistantCalls 1 [1] ])
  , ("unpaired-call-is-dropped",
      [ userText 0 0
      , assistantCalls 1 [1, 2]
      , toolResult 2 1
      , assistantCalls 3 [3] ])
  , ("result-after-conversation-resumes",
      [ assistantCalls 0 [1]
      , userText 1 0
      , toolResult 2 1 ])
  , ("loop-threaded-turn-is-a-fixpoint",
      [ assistantCalls 0 [1, 2]
      , toolResult 1 1
      , toolResult 2 2 ])
  , -- The case the row-only model cannot express: assistant prose rides along
    -- with a call that never resolved. Production keeps the message and its
    -- prose; the row is demoted to `.ordinary`.
    ("assistant-prose-survives-its-unpaired-call",
      [ userText 0 0
      , assistantCalls 1 [1] [Item.text 7]
      , userText 2 1 ])
  , ("assistant-prose-with-mixed-paired-and-unpaired-calls",
      [ assistantCalls 0 [1, 2] [Item.text 7, Item.other 8]
      , toolResult 1 1 ])
  , -- Content arriving out of canonical order: text after the calls. Stage 3
    -- reorders it; the announced call set is unchanged.
    ("content-order-is-normalized",
      [ mkRow 0 .assistant (.assistantToolCalls [1].toFinset)
          [Item.call 1, Item.other 5, Item.text 6]
      , toolResult 1 1 ])
  , -- Empty messages: Rust drops them, asymmetrically across the two stages
    -- (an empty user message goes in stage 1, an empty assistant message is
    -- carried through and pruned in stage 2). The row-only model kept both.
    ("empty-messages-are-dropped",
      [ mkRow 0 .assistant .ordinary []
      , mkRow 1 .user .ordinary []
      , userText 2 0 ])
  , -- An empty message does *not* end the active turn: Rust clears pending
    -- calls only on plain content. The pair must survive it intact.
    ("empty-message-does-not-break-an-open-turn",
      [ assistantCalls 0 [1]
      , mkRow 1 .user .ordinary []
      , toolResult 2 1 ])
  , ("empty-assistant-message-between-paired-turns",
      [ assistantCalls 0 [1]
      , toolResult 1 1
      , mkRow 2 .assistant .ordinary []
      , userText 3 0 ])
  , ("interleaved-blocks",
      [ assistantCalls 0 [1]
      , toolResult 1 1
      , assistantText 2 3
      , assistantCalls 3 [2, 4]
      , toolResult 4 2
      , toolResult 5 4
      , userText 6 9 ])
  ]

/-- Every witness row is `Coherent` — its announced call set is exactly the
`call` items in its content. This is what licenses `announcedIds` to read the
emitted call ids off the content, and it is the hypothesis
`Provider.sanitizeForProvider_sound` and `_idempotent` need. Checked by
`decide`, so a witness that drifts out of coherence fails the build. -/
theorem witnessesAreCoherent :
    ∀ witness ∈ witnessTranscripts, PromptAssembly.Provider.AllCoherent witness.2 := by
  decide

/-- The *other* premise of `Provider.sanitizeForProvider_sound` and
`_idempotent`. Without this the contract's claim — that production reproducing
an emitted output thereby inherits provider-validity — does not actually follow,
because the theorem would be quantified over inputs the witnesses need not
satisfy. Checked by `decide`, so a witness that reuses a call id across rows
fails the build. -/
theorem witnessesHaveUniqueCallIds :
    ∀ witness ∈ witnessTranscripts,
      PromptAssembly.UniqueCallIds
        (PromptAssembly.Provider.project witness.2) := by
  decide

/-- Both premises together, so the soundness the emitted rows rest on is
discharged for every witness rather than asserted in a comment. -/
theorem witnessOutputsAreProviderValid :
    ∀ witness ∈ witnessTranscripts,
      PromptAssembly.ProviderValid
        (PromptAssembly.Provider.project
          (PromptAssembly.Provider.sanitizeForProvider witness.2)) := by
  intro witness hwitness
  exact PromptAssembly.Provider.sanitizeForProvider_sound
    (witnessesHaveUniqueCallIds witness hwitness)
    (witnessesAreCoherent witness hwitness)

private def splitCases (rows : List ProviderRow) : List PromptAssemblySplitCase :=
  (List.range (rows.length + 1)).map fun index =>
    { index := index
    , expected := rowCases (PromptAssembly.Provider.sanitizeForProvider (rows.drop index)) }

private def sanitizeCase (witness : String × List ProviderRow) :
    PromptAssemblySanitizeCase :=
  let rows := witness.2
  let once := PromptAssembly.Provider.sanitizeForProvider rows
  { name := witness.1
  , input := rowCases rows
  , expected := rowCases once
  , expectedTwice := rowCases (PromptAssembly.Provider.sanitizeForProvider once)
  , splits := splitCases rows }

def promptAssemblySanitizeCases : List PromptAssemblySanitizeCase :=
  witnessTranscripts.map sanitizeCase

/-! ## Layer order

Emitted from `PromptAssembly.Template.assembleWithContext`, whose
`assembleWithContext_tail` theorem fixes the tail as `contextPreamble, prompt`. -/

private def slotName : PromptAssembly.Slot → String
  | .preamble => "preamble"
  | .summaryReminder => "summaryReminder"
  | .skillReminder index => s!"skillReminder:{index}"
  | .conversation index => s!"conversation:{index}"
  | .contextPreamble => "contextPreamble"
  | .prompt => "prompt"

private def layerShapes : List (String × Nat × Nat × Nat) :=
  [ ("bare", 0, 0, 0)
  , ("conversation-only", 0, 0, 3)
  , ("summary-and-conversation", 0, 1, 2)
  , ("skills-summary-and-conversation", 2, 1, 2)
  , ("skills-only", 3, 0, 0)
  ]

def promptAssemblyLayerCases : List PromptAssemblyLayerCase :=
  layerShapes.map fun shape =>
    { name := shape.1
    , skillCount := shape.2.1
    , summaryCount := shape.2.2.1
    , conversationLen := shape.2.2.2
    , slots :=
        (PromptAssembly.Template.assembleWithContext
          shape.2.1 shape.2.2.1 shape.2.2.2).map slotName }

/-! ## Tool-argument repair

Emitted from `PromptAssembly.ToolArgs.repairArgs`, fencing
`repair_is_payload_only` (repair rewrites argument payloads only) and
`repair_idempotent` (a second pass is a no-op). -/

/-- A stand-in payload type whose leaf sanitizer is idempotent, matching the
shape of the Rust repair: normalize to an object, then sanitize leaves. -/
private inductive Payload where
  | empty
  | raw
  | sanitized
  deriving DecidableEq, Repr

private def sanitizePayload : Payload → Payload
  | .empty => .empty
  | .raw => .sanitized
  | .sanitized => .sanitized

private instance : PromptAssembly.LeafSanitizer Payload where
  sanitize := sanitizePayload
  idempotent := by intro p; cases p <;> rfl

private def argsName : PromptAssembly.ToolArgs Payload → String
  | .object .empty => "object:empty"
  | .object .raw => "object:raw"
  | .object .sanitized => "object:sanitized"
  | .str none => "str:unparsed"
  | .str (some .empty) => "str:object:empty"
  | .str (some .raw) => "str:object:raw"
  | .str (some .sanitized) => "str:object:sanitized"
  | .array => "array"
  | .scalar => "scalar"
  | .null => "null"

private def repairVectors : List (String × PromptAssembly.ToolArgs Payload) :=
  [ ("object-passes-through", .object .raw)
  , ("empty-object-passes-through", .object .empty)
  , ("stringified-object-salvages", .str (some .raw))
  , ("unparsable-string-collapses", .str none)
  , ("array-collapses", .array)
  , ("scalar-collapses", .scalar)
  , ("null-collapses", .null)
  ]

/-- Whether the repair rewrote only the payload — i.e. the result is an object
whose payload is the leaf-sanitized original. True exactly on object inputs,
which is what `repair_is_payload_only` states. -/
private def isPayloadOnly : PromptAssembly.ToolArgs Payload → Bool
  | .object _ => true
  | _ => false

def promptAssemblyRepairCases : List PromptAssemblyRepairCase :=
  repairVectors.map fun vector =>
    let once := PromptAssembly.repairArgs Payload.empty vector.2
    { name := vector.1
    , input := argsName vector.2
    , expected := argsName once
    , expectedTwice := argsName (PromptAssembly.repairArgs Payload.empty once)
    , payloadOnly := isPayloadOnly vector.2 }

/-! ## Provider input and dynamic output budgets

The old daemon trigger reserved the full configured output ceiling on every
turn, which reduced D4F's effective input to 65,536 tokens. These cases are
computed from `PromptAssembly.Budget`; the observed 118,785 + 393,216 = 512,001
provider rejection now demonstrates why the output ceiling must be clamped to
the remaining context on each dispatch.
-/

private structure BudgetWitness where
  name : String
  contextWindow : Nat
  maxOutputTokens : Nat
  thresholdBasisPoints : Nat
  promptTokens : Nat
  requestTokens : Nat

private def budgetWitnesses : List BudgetWitness :=
  [ -- Degenerate and one-token contexts pin saturating subtraction and the
    -- strict positive-output dispatch boundary.
    { name := "zero-context-zero-output"
    , contextWindow := 0, maxOutputTokens := 0, thresholdBasisPoints := 10000
    , promptTokens := 0, requestTokens := 0 }
  , { name := "zero-context-positive-output"
    , contextWindow := 0, maxOutputTokens := 1, thresholdBasisPoints := 10000
    , promptTokens := 0, requestTokens := 0 }
  , { name := "one-context-zero-output"
    , contextWindow := 1, maxOutputTokens := 0, thresholdBasisPoints := 10000
    , promptTokens := 0, requestTokens := 0 }
  , { name := "one-context-one-output-empty-input"
    , contextWindow := 1, maxOutputTokens := 1, thresholdBasisPoints := 10000
    , promptTokens := 0, requestTokens := 0 }
  , { name := "one-context-input-equals-context"
    , contextWindow := 1, maxOutputTokens := 1, thresholdBasisPoints := 10000
    , promptTokens := 1, requestTokens := 0 }
  , { name := "one-context-input-one-over-context"
    , contextWindow := 1, maxOutputTokens := 1, thresholdBasisPoints := 10000
    , promptTokens := 2, requestTokens := 0 }
  , { name := "output-one-below-context"
    , contextWindow := 1000, maxOutputTokens := 999, thresholdBasisPoints := 10000
    , promptTokens := 0, requestTokens := 0 }
  , { name := "output-equals-context"
    , contextWindow := 1000, maxOutputTokens := 1000, thresholdBasisPoints := 10000
    , promptTokens := 0, requestTokens := 0 }
  , { name := "output-one-above-context"
    , contextWindow := 1000, maxOutputTokens := 1001, thresholdBasisPoints := 10000
    , promptTokens := 0, requestTokens := 0 }
  , { name := "configured-threshold-budget-minus-one"
    , contextWindow := 10000, maxOutputTokens := 1000, thresholdBasisPoints := 7500
    , promptTokens := 7499, requestTokens := 0 }
  , { name := "configured-threshold-boundary"
    , contextWindow := 10000, maxOutputTokens := 1000, thresholdBasisPoints := 7500
    , promptTokens := 7500, requestTokens := 0 }
  , { name := "configured-threshold-one-over"
    , contextWindow := 10000, maxOutputTokens := 1000, thresholdBasisPoints := 7500
    , promptTokens := 7501, requestTokens := 0 }
  , { name := "d4f-profile-safe-boundary"
    , contextWindow := 510976, maxOutputTokens := 393216, thresholdBasisPoints := 7500
    , promptTokens := 383232, requestTokens := 0 }
  , { name := "d4f-profile-one-over"
    , contextWindow := 510976, maxOutputTokens := 393216, thresholdBasisPoints := 7500
    , promptTokens := 383233, requestTokens := 0 }
  , { name := "d4f-observed-provider-rejection"
    , contextWindow := 512000, maxOutputTokens := 393216, thresholdBasisPoints := 7500
    , promptTokens := 118785, requestTokens := 0 }
  , { name := "incoming-request-crosses-boundary"
    , contextWindow := 10000, maxOutputTokens := 4000, thresholdBasisPoints := 7500
    , promptTokens := 7493, requestTokens := 8 }
  , { name := "output-reserves-entire-context-empty-input"
    , contextWindow := 1000, maxOutputTokens := 1000, thresholdBasisPoints := 7500
    , promptTokens := 0, requestTokens := 0 }
  , { name := "output-reserves-entire-context-one-token"
    , contextWindow := 1000, maxOutputTokens := 1000, thresholdBasisPoints := 7500
    , promptTokens := 1, requestTokens := 0 }
  , { name := "validated-summary-output-maximum"
    , contextWindow := 65536, maxOutputTokens := 32768, thresholdBasisPoints := 7500
    , promptTokens := 32768, requestTokens := 0 }
    -- Thresholds that are *not* exactly representable in binary. Computing the
    -- budget as `contextWindow × threshold` in floating point and truncating
    -- lands one token low on each of these, so before #1008 they would have
    -- failed the contract. Both sides now divide basis points as integers.
  , { name := "non-dyadic-threshold-57pct-boundary"
    , contextWindow := 10000, maxOutputTokens := 1000, thresholdBasisPoints := 5700
    , promptTokens := 5700, requestTokens := 0 }
  , { name := "non-dyadic-threshold-57pct-one-over"
    , contextWindow := 10000, maxOutputTokens := 1000, thresholdBasisPoints := 5700
    , promptTokens := 5701, requestTokens := 0 }
  , { name := "non-dyadic-threshold-69pct-boundary"
    , contextWindow := 10000, maxOutputTokens := 1000, thresholdBasisPoints := 6900
    , promptTokens := 6900, requestTokens := 0 }
  , { name := "non-dyadic-threshold-69pct-one-over"
    , contextWindow := 10000, maxOutputTokens := 1000, thresholdBasisPoints := 6900
    , promptTokens := 6901, requestTokens := 0 }
  , { name := "non-dyadic-threshold-29pct-large-window-boundary"
    , contextWindow := 200000, maxOutputTokens := 1000, thresholdBasisPoints := 2900
    , promptTokens := 58000, requestTokens := 0 }
  , { name := "non-dyadic-threshold-29pct-large-window-one-over"
    , contextWindow := 200000, maxOutputTokens := 1000, thresholdBasisPoints := 2900
    , promptTokens := 58001, requestTokens := 0 }
  ]

private def budgetCase (witness : BudgetWitness) : PromptAssemblyBudgetCase :=
  let configured := PromptAssembly.Budget.configuredThresholdBudget
    witness.contextWindow witness.thresholdBasisPoints
  let effective := PromptAssembly.Budget.effectiveInputBudget configured
    witness.contextWindow
  let inputTokens := witness.promptTokens + witness.requestTokens
  let outputTokens := PromptAssembly.Budget.effectiveOutputBudget inputTokens
    witness.contextWindow witness.maxOutputTokens
  { name := witness.name
  , contextWindow := witness.contextWindow
  , maxOutputTokens := witness.maxOutputTokens
  , thresholdBasisPoints := witness.thresholdBasisPoints
  , configuredThresholdBudget := configured
  , promptTokens := witness.promptTokens
  , requestTokens := witness.requestTokens
  , effectiveInputBudget := effective
  , effectiveOutputTokens := outputTokens
  , shouldCompact := decide (PromptAssembly.Budget.ExceedsInputBudget
      witness.promptTokens witness.requestTokens configured witness.contextWindow)
  , providerSafe := decide
      (inputTokens + outputTokens ≤ witness.contextWindow)
  , canDispatch := decide (PromptAssembly.Budget.CanDispatch inputTokens
      witness.contextWindow witness.maxOutputTokens) }

def promptAssemblyBudgetCases : List PromptAssemblyBudgetCase :=
  budgetWitnesses.map budgetCase

/-! The owned loop must apply the same gate to every provider completion, not
only to the entry turn. These traces cross the boundary after one or more safe
turns so a first-turn-only implementation cannot satisfy the generated fence. -/

private structure TurnBudgetWitness where
  name : String
  contextWindow : Nat
  maxOutputTokens : Nat
  thresholdBasisPoints : Nat
  turnInputTokens : List Nat

private def turnBudgetWitnesses : List TurnBudgetWitness :=
  [ { name := "owned-loop-later-turn-crosses-budget"
    , contextWindow := 458752
    , maxOutputTokens := 393216
    , thresholdBasisPoints := 7500
    , turnInputTokens := [48000, 65536, 65537, 344064, 344065] }
  , { name := "owned-loop-every-turn-safe"
    , contextWindow := 10000
    , maxOutputTokens := 2000
    , thresholdBasisPoints := 7500
    , turnInputTokens := [1000, 4000, 7500] }
  , { name := "owned-loop-dispatch-capacity-boundaries"
    , contextWindow := 10000
    , maxOutputTokens := 1
    , thresholdBasisPoints := 10000
    , turnInputTokens := [9999, 10000, 10001] }
  , { name := "owned-loop-zero-configured-output"
    , contextWindow := 10000
    , maxOutputTokens := 0
    , thresholdBasisPoints := 7500
    , turnInputTokens := [0, 7499, 7500, 7501] }
  ]

private def turnBudgetCase
    (witness : TurnBudgetWitness) : PromptAssemblyTurnBudgetCase :=
  let configured := PromptAssembly.Budget.configuredThresholdBudget
    witness.contextWindow witness.thresholdBasisPoints
  let effective := PromptAssembly.Budget.effectiveInputBudget configured
    witness.contextWindow
  { name := witness.name
  , contextWindow := witness.contextWindow
  , maxOutputTokens := witness.maxOutputTokens
  , thresholdBasisPoints := witness.thresholdBasisPoints
  , configuredThresholdBudget := configured
  , effectiveInputBudget := effective
  , turnInputTokens := witness.turnInputTokens
  , turnOutputTokens := witness.turnInputTokens.map fun inputTokens =>
      PromptAssembly.Budget.effectiveOutputBudget inputTokens witness.contextWindow
        witness.maxOutputTokens
  , turnShouldCompact := witness.turnInputTokens.map fun inputTokens =>
      decide (PromptAssembly.Budget.ExceedsInputBudget inputTokens 0 configured
        witness.contextWindow)
  , turnCanDispatch := witness.turnInputTokens.map fun inputTokens =>
      decide (PromptAssembly.Budget.CanDispatch inputTokens witness.contextWindow
        witness.maxOutputTokens) }

def promptAssemblyTurnBudgetCases : List PromptAssemblyTurnBudgetCase :=
  turnBudgetWitnesses.map turnBudgetCase

/-! ## Static-overhead-aware compaction retention

These rows keep the configured cap, saturating fixed-layer subtraction, and
exact three-quarter reservation under one Lean-owned calculation. -/

private structure RetentionWitness where
  name : String
  configuredKeepRecent : Nat
  effectiveInputBudget : Nat
  fixedInput : Nat
  summaryMaxOutput : Nat

private def retentionWitnesses : List RetentionWitness :=
  [ { name := "zero-capacity", configuredKeepRecent := 0
    , effectiveInputBudget := 0, fixedInput := 0, summaryMaxOutput := 0 }
  , { name := "one-token-capacity", configuredKeepRecent := 1
    , effectiveInputBudget := 1, fixedInput := 0, summaryMaxOutput := 1 }
  , { name := "configured-cap-wins", configuredKeepRecent := 2
    , effectiveInputBudget := 100, fixedInput := 0, summaryMaxOutput := 2 }
  , { name := "quarter-boundary-minus-one", configuredKeepRecent := 100
    , effectiveInputBudget := 3, fixedInput := 0, summaryMaxOutput := 1 }
  , { name := "quarter-boundary-exact", configuredKeepRecent := 100
    , effectiveInputBudget := 4, fixedInput := 0, summaryMaxOutput := 4 }
  , { name := "quarter-boundary-one-over", configuredKeepRecent := 100
    , effectiveInputBudget := 5, fixedInput := 0, summaryMaxOutput := 5 }
  , { name := "fixed-input-leaves-one", configuredKeepRecent := 100
    , effectiveInputBudget := 100, fixedInput := 99, summaryMaxOutput := 25 }
  , { name := "fixed-input-consumes-capacity", configuredKeepRecent := 100
    , effectiveInputBudget := 100, fixedInput := 100, summaryMaxOutput := 100 }
  , { name := "fixed-input-exceeds-capacity", configuredKeepRecent := 100
    , effectiveInputBudget := 100, fixedInput := 101, summaryMaxOutput := 101 }
  , { name := "large-uncapped-json-safe", configuredKeepRecent := 200000
    , effectiveInputBudget := 200000, fixedInput := 1, summaryMaxOutput := 32768 }
  , { name := "large-configured-cap", configuredKeepRecent := 20000
    , effectiveInputBudget := 200000, fixedInput := 58000, summaryMaxOutput := 200000 }
  , { name := "small-context-large-summary-ceiling", configuredKeepRecent := 20
    , effectiveInputBudget := 2000, fixedInput := 0, summaryMaxOutput := 32768 }
  , { name := "summary-ceiling-smaller-than-quarter", configuredKeepRecent := 20
    , effectiveInputBudget := 6000, fixedInput := 0, summaryMaxOutput := 512 }
  ]

private def retentionCase (witness : RetentionWitness) : PromptAssemblyRetentionCase :=
  let summaryOutput := PromptAssembly.Budget.summaryOutputCeiling
    witness.summaryMaxOutput witness.effectiveInputBudget
  { name := witness.name
  , configuredKeepRecent := witness.configuredKeepRecent
  , effectiveInputBudget := witness.effectiveInputBudget
  , fixedInput := witness.fixedInput
  , availableInput := witness.effectiveInputBudget - witness.fixedInput
  , retentionTarget := PromptAssembly.Budget.compactionRetentionTarget
      witness.configuredKeepRecent witness.effectiveInputBudget witness.fixedInput
  , summaryMaxOutput := witness.summaryMaxOutput
  , effectiveSummaryOutput := summaryOutput
  , rollingSummaryInputBudget := PromptAssembly.Budget.rollingSummaryInputBudget
      witness.effectiveInputBudget summaryOutput
  }

def promptAssemblyRetentionCases : List PromptAssemblyRetentionCase :=
  retentionWitnesses.map retentionCase

/-! ## Claude content-block map (Track B / B2)

Every `outcome` / `ids` field is `mapTurn` (or its error), not a hand oracle.
The Completer parser must reproduce these results. -/

structure PromptAssemblyClaudeMapCase where
  name : String
  surface : List String
  blocks : List String
  outcome : String
  ids : List Nat
  deriving Repr

private def surfaceOf (names : List String) : PromptAssembly.ClaudeMap.Surface :=
  names.toFinset

private def claudeMapCase (name : String) (surface : List String)
    (blocks : List PromptAssembly.ClaudeMap.Block) : PromptAssemblyClaudeMapCase :=
  let tagged := blocks.map PromptAssembly.ClaudeMap.blockTag
  match PromptAssembly.ClaudeMap.mapTurn (surfaceOf surface) blocks with
  | .ok _ =>
    { name := name
    , surface := surface
    , blocks := tagged
    , outcome := "ok"
    , ids := (PromptAssembly.ClaudeMap.toolUsePairs blocks).map (·.1) }
  | .error e =>
    { name := name
    , surface := surface
    , blocks := tagged
    , outcome := PromptAssembly.ClaudeMap.errorName e
    , ids := [] }

def promptAssemblyClaudeMapCases : List PromptAssemblyClaudeMapCase :=
  [ claudeMapCase "text-only-empty-surface" [] [.text]
  , claudeMapCase "mapped-echo" ["echo"] [.toolUse 1 "echo"]
  , claudeMapCase "bash-is-not-bash" ["bash"] [.toolUse 1 "Bash"]
  , claudeMapCase "empty-surface-tool-use" [] [.toolUse 1 "echo"]
  , claudeMapCase "duplicate-id" ["echo"]
      [.toolUse 1 "echo", .toolUse 1 "echo"]
  ]

/-! ## Claude Messages body (single wire)

`system` is `systemBlocks preamble (splitSystem rows).1`; `toolsPresent` is
`(toolsField tools).isSome`. Rows are tagged `system:<text>` / `other:<tag>`. -/

structure PromptAssemblyClaudeBodyCase where
  name : String
  preamble : Option String
  rows : List String
  tools : List String
  system : List String
  toolsPresent : Bool
  deriving Repr

private def msgTag : PromptAssembly.ClaudeMap.Msg → String
  | .system t => "system:" ++ t
  | .other tag => "other:" ++ tag

private def claudeBodyCase (name : String) (preamble : Option String)
    (rows : List PromptAssembly.ClaudeMap.Msg) (tools : List String) :
    PromptAssemblyClaudeBodyCase :=
  let (sys, _) := PromptAssembly.ClaudeMap.splitSystem rows
  { name := name
  , preamble := preamble
  , rows := rows.map msgTag
  , tools := tools
  , system := PromptAssembly.ClaudeMap.systemBlocks preamble sys
  , toolsPresent := (PromptAssembly.ClaudeMap.toolsField tools).isSome }

def promptAssemblyClaudeBodyCases : List PromptAssemblyClaudeBodyCase :=
  [ claudeBodyCase "identity-only" none [.other "user"] []
  , claudeBodyCase "preamble-after-identity" (some "You are helpful.") [.other "user"] ["echo"]
  , claudeBodyCase "system-rows-follow-preamble" (some "P")
      [.system "S1", .other "user", .system "S2", .other "assistant"] ["echo"]
  , claudeBodyCase "system-rows-without-preamble" none
      [.system "workspace context", .other "user"] []
  , claudeBodyCase "empty-tools-omitted" (some "P") [.other "user"] []
  , claudeBodyCase "two-tools-present" none [.other "user"] ["echo", "list_files"]
  ]

/-! ## Claude Messages SSE stream (single wire)

`outcome` / `calls` are `runStream`, not a hand oracle. Event tags:
`text:<t>`, `start:<id>:<name>:<input>` (empty input = none), `delta:<partial>`, `stop`. -/

structure PromptAssemblyClaudeStreamCase where
  name : String
  surface : List String
  events : List String
  outcome : String
  calls : List String
  deriving Repr

private def streamEventTag : PromptAssembly.ClaudeMap.StreamEvent → String
  | .text t => "text:" ++ t
  | .start id name input => s!"start:{id}:{name}:{input.getD ""}"
  | .delta fragment => "delta:" ++ fragment
  | .stop => "stop"

private def claudeStreamCase (name : String) (surface : List String)
    (events : List PromptAssembly.ClaudeMap.StreamEvent) : PromptAssemblyClaudeStreamCase :=
  match PromptAssembly.ClaudeMap.runStream (surfaceOf surface) events with
  | .ok calls =>
    { name := name, surface := surface, events := events.map streamEventTag
    , outcome := "ok", calls := calls.map (fun (id, args) => s!"{id}={args}") }
  | .error e =>
    { name := name, surface := surface, events := events.map streamEventTag
    , outcome := PromptAssembly.ClaudeMap.errorName e, calls := [] }

def promptAssemblyClaudeStreamCases : List PromptAssemblyClaudeStreamCase :=
  [ claudeStreamCase "text-only" [] [.text "hi"]
  , claudeStreamCase "deltas-win-over-start" ["echo"]
      [.start 1 "echo" (some "{}"), .delta "{\"text\":", .delta " \"hi\"}", .stop]
  , claudeStreamCase "start-input-without-deltas" ["echo"]
      [.start 1 "echo" (some "{\"a\":1}"), .stop]
  , claudeStreamCase "no-input-is-empty-object" ["echo"] [.start 1 "echo" none, .stop]
  , claudeStreamCase "overlapping-block" ["echo"]
      [.start 1 "echo" none, .start 2 "echo" none, .stop]
  , claudeStreamCase "duplicate-id" ["echo"]
      [.start 1 "echo" none, .stop, .start 1 "echo" none, .stop]
  , claudeStreamCase "unmapped-name" ["bash"] [.start 1 "Bash" none, .stop]
  , claudeStreamCase "empty-surface-tool-use" [] [.start 1 "echo" none, .stop]
  , claudeStreamCase "unterminated-flushes" ["echo"] [.start 1 "echo" none, .delta "{}"]
  , claudeStreamCase "two-blocks-in-order" ["echo", "list_files"]
      [.start 1 "echo" none, .delta "{}", .stop, .text "then", .start 2 "list_files" none, .delta "{\"path\":\".\"}", .stop]
  ]

end Conformance.ContractCases
