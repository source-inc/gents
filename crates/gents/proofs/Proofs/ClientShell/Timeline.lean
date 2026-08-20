import Proofs.Client

/-!
# Client Shell Timeline Ordering (#608 parity)

Every client shell renders a session's transcript in the same order: messages
interleaved with their tool groups, with a pending turn immediately before the
first durable message from its request (or at the tail), then orphan tool
groups and the live-assistant overlay. While an unmaterialized foreground tool
is running, the overlay is placed immediately before that tool's orphan group,
without jumping ahead of earlier historical orphan groups. Otherwise the
overlay remains at the tail. The *order and the message↔tool-group partition* are semantics a
second shell must reproduce exactly; only the pixels are presentation.

This models `gents_protocol::timeline::build_timeline_order`. The Rust
function is structured in the same three phases this model concatenates:

    buildOrder = placePending pending body ++ placedOrphanTail

where `body` interleaves each surviving (deduped) message with the tool group it
owns, `orphans` are the tool groups no surviving message owns, and the final
phase inserts the visible overlay at the tail or immediately before one
identified orphan sequence.

Model boundary: the input message list is taken **already sorted** by the
shell's total sequence order (the Rust sorts first; sort correctness is a
standard fact, not re-derived here). The fence is the interleave / partition /
tail discipline *on top of* that order — which is exactly the part a second
shell re-implements and can get wrong.
-/

namespace ClientShell.Timeline

inductive Role
  | user
  | assistant
  deriving DecidableEq, Repr

/-- The ordering-relevant projection of one transcript message. `seq` is the
sort key and the tool-group attach key; `emitsItem` says whether it contributes
a visible slot; `token` is an opaque presentation-dedup token. -/
structure Msg where
  key : Nat
  seq : Int
  role : Role
  emitsItem : Bool
  token : Option Nat
  deriving DecidableEq, Repr

/-- One ordered slot. A shell maps each to a rich item; the order and identity
of the slots is the shared contract. -/
inductive Slot
  | message (key : Nat) (seq : Int) (role : Role)
  | toolGroup (seq : Int)
  | pending
  | overlay
  deriving DecidableEq, Repr

/-- First-wins dedup by `key`, then by `token`. Threads the seen sets in order,
so the SURVIVING messages keep their input order. -/
def dedup (seenKeys : List Nat) (seenTokens : List Nat) : List Msg → List Msg
  | [] => []
  | m :: rest =>
      if m.key ∈ seenKeys then
        dedup seenKeys seenTokens rest
      else
        match m.token with
        | some t =>
            if t ∈ seenTokens then
              dedup (m.key :: seenKeys) seenTokens rest
            else
              m :: dedup (m.key :: seenKeys) (t :: seenTokens) rest
        | none =>
            m :: dedup (m.key :: seenKeys) seenTokens rest

/-- The kept (deduped) messages, in input order. -/
def kept (msgs : List Msg) : List Msg := dedup [] [] msgs

/-- Does sequence `s` have a tool group? -/
def hasGroup (groups : List Int) (s : Int) : Bool := s ∈ groups

/-- Body slots: each kept message emits its slot (when `emitsItem`) immediately
followed by the tool group it owns (when it owns one). Mirrors the per-message
loop; the `attached` accumulator prevents a repeated sequence from re-emitting a
group. -/
def bodyGo (groups : List Int) (attached : List Int) : List Msg → List Slot
  | [] => []
  | m :: rest =>
      let msgSlots := if m.emitsItem then [Slot.message m.key m.seq m.role] else []
      if hasGroup groups m.seq ∧ m.seq ∉ attached then
        msgSlots ++ Slot.toolGroup m.seq :: bodyGo groups (m.seq :: attached) rest
      else
        msgSlots ++ bodyGo groups attached rest

def body (groups : List Int) (msgs : List Msg) : List Slot :=
  bodyGo groups [] (kept msgs)

/-- The sequences a surviving message attaches a group to. -/
def attachedSeqs (groups : List Int) (msgs : List Msg) : List Int :=
  ((kept msgs).map Msg.seq).filter (fun s => hasGroup groups s)

/-- Orphan groups: those attached to no surviving message. -/
def orphans (groups : List Int) (msgs : List Msg) : List Int :=
  groups.filter (fun s => s ∉ attachedSeqs groups msgs)

/-- Live overlay placement. A running foreground tool identifies the exact
orphan sequence immediately before which its reasoning belongs. -/
inductive OverlayPlacement
  | tail
  | beforeOrphan (seq : Int)
  deriving DecidableEq, Repr

structure Overlay where
  hasDurableOwner : Bool
  placement : OverlayPlacement
  deriving DecidableEq, Repr

/-- A request-owned pending user turn normally sits at the body tail. During
partial replication, a later message from the same request may arrive first;
then the pending turn is inserted immediately before that message. -/
inductive PendingPlacement
  | tail
  | beforeMessage (seq : Int)
  deriving DecidableEq, Repr

def insertPendingBefore (target : Int) : List Slot → List Slot
  | [] => [Slot.pending]
  | slot :: rest =>
      match slot with
      | .message _ seq _ =>
          if seq = target then Slot.pending :: slot :: rest
          else slot :: insertPendingBefore target rest
      | .toolGroup seq =>
          if seq = target then Slot.pending :: slot :: rest
          else slot :: insertPendingBefore target rest
      | _ => slot :: insertPendingBefore target rest

def placePending : Option PendingPlacement → List Slot → List Slot
  | none, body => body
  | some .tail, body => body ++ [Slot.pending]
  | some (.beforeMessage target), body => insertPendingBefore target body

theorem pending_before_matching_message_head
    (target : Int) (key : Nat) (role : Role) (rest : List Slot) :
    placePending (some (.beforeMessage target))
        (Slot.message key target role :: rest) =
      Slot.pending :: Slot.message key target role :: rest := by
  simp [placePending, insertPendingBefore]

theorem pending_before_matching_tool_group_head
    (target : Int) (rest : List Slot) :
    placePending (some (.beforeMessage target))
        (Slot.toolGroup target :: rest) =
      Slot.pending :: Slot.toolGroup target :: rest := by
  simp [placePending, insertPendingBefore]

/-- Insert `visibleOverlay` immediately before the first matching orphan.
If the target is absent (a partial-sync race), keep the overlay at the tail. -/
def insertOverlayBefore (target : Int) (visibleOverlay : List Slot) : List Int → List Slot
  | [] => visibleOverlay
  | seq :: rest =>
      if seq = target then
        visibleOverlay ++ Slot.toolGroup seq :: rest.map Slot.toolGroup
      else
        Slot.toolGroup seq :: insertOverlayBefore target visibleOverlay rest

def placeOrphanTail (placement : OverlayPlacement) (visibleOverlay : List Slot)
    (orphanSeqs : List Int) : List Slot :=
  match placement with
  | .tail => orphanSeqs.map Slot.toolGroup ++ visibleOverlay
  | .beforeOrphan target => insertOverlayBefore target visibleOverlay orphanSeqs

/-- The three-phase timeline order. -/
def buildOrder (groups : List Int) (msgs : List Msg)
    (pending : Option PendingPlacement) (overlay : Option Overlay) : List Slot :=
  let visibleOverlay :=
    match overlay with
    | some o => if o.hasDurableOwner then [] else [Slot.overlay]
    | none => []
  let placedOrphanTail :=
    match overlay with
    | some o => placeOrphanTail o.placement visibleOverlay (orphans groups msgs)
    | none => (orphans groups msgs).map Slot.toolGroup
  placePending pending (body groups msgs)
    ++ placedOrphanTail

/-! ## Slot-membership helpers -/

/-- No `Slot.overlay` is produced by the body phase: the interleave emits only
message and tool-group slots. -/
theorem overlay_not_in_bodyGo (groups : List Int) (attached : List Int) (ms : List Msg) :
    Slot.overlay ∉ bodyGo groups attached ms := by
  induction ms generalizing attached with
  | nil => simp [bodyGo]
  | cons m rest ih =>
      unfold bodyGo
      by_cases hg : hasGroup groups m.seq ∧ m.seq ∉ attached
      · simp only [hg, if_true]
        cases m.emitsItem <;> simp [List.mem_append, ih]
      · simp only [hg, if_false]
        cases m.emitsItem <;> simp [List.mem_append, ih]

theorem overlay_not_in_body (groups : List Int) (msgs : List Msg) :
    Slot.overlay ∉ body groups msgs :=
  overlay_not_in_bodyGo groups [] (kept msgs)

theorem overlay_mem_insertPendingBefore_iff (target : Int) (slots : List Slot) :
    (Slot.overlay ∈ insertPendingBefore target slots) ↔ Slot.overlay ∈ slots := by
  induction slots with
  | nil => simp [insertPendingBefore]
  | cons slot rest ih =>
      cases slot with
      | message key seq role =>
          by_cases h : seq = target <;> simp [insertPendingBefore, h, ih]
      | toolGroup seq =>
          by_cases h : seq = target <;> simp [insertPendingBefore, h, ih]
      | pending => simp [insertPendingBefore, ih]
      | overlay =>
          change (Slot.overlay ∈ Slot.overlay :: insertPendingBefore target rest) ↔ _
          simp

theorem overlay_mem_placePending_iff (pending : Option PendingPlacement)
    (slots : List Slot) :
    (Slot.overlay ∈ placePending pending slots) ↔ Slot.overlay ∈ slots := by
  cases pending with
  | none => rfl
  | some placement =>
      cases placement with
      | tail => simp [placePending]
      | beforeMessage target => exact overlay_mem_insertPendingBefore_iff target slots

theorem overlay_not_in_orphans (groups : List Int) (msgs : List Msg) :
    Slot.overlay ∉ (orphans groups msgs).map Slot.toolGroup := by
  simp

theorem overlay_mem_insertOverlayBefore_iff (target : Int) (visible : List Slot)
    (orphanSeqs : List Int) :
    (Slot.overlay ∈ insertOverlayBefore target visible orphanSeqs) ↔
      Slot.overlay ∈ visible := by
  induction orphanSeqs with
  | nil => simp [insertOverlayBefore]
  | cons seq rest ih =>
      by_cases htarget : seq = target
      · simp [insertOverlayBefore, htarget]
      · simp [insertOverlayBefore, htarget, ih]

theorem pending_mem_insertOverlayBefore_iff (target : Int) (visible : List Slot)
    (orphanSeqs : List Int) :
    (Slot.pending ∈ insertOverlayBefore target visible orphanSeqs) ↔
      Slot.pending ∈ visible := by
  induction orphanSeqs with
  | nil => simp [insertOverlayBefore]
  | cons seq rest ih =>
      by_cases htarget : seq = target
      · simp [insertOverlayBefore, htarget]
      · simp [insertOverlayBefore, htarget, ih]

/-! ## Overlay: shown iff live, and precisely placed -/

/-- The overlay is emitted exactly when it is present and no durable assistant
turn already owns the same request-local content. -/
theorem overlay_shown_iff (groups : List Int) (msgs : List Msg)
    (pending : Option PendingPlacement) (o : Overlay) :
    (Slot.overlay ∈ buildOrder groups msgs pending (some o)) ↔ o.hasDurableOwner = false := by
  unfold buildOrder
  have hb := overlay_not_in_body groups msgs
  have ho := overlay_not_in_orphans groups msgs
  have hp := overlay_mem_placePending_iff pending (body groups msgs)
  cases hm : o.hasDurableOwner <;>
    cases hplace : o.placement with
    | tail => simp [hp, hm, hplace, placeOrphanTail, List.mem_append, hb, ho]
    | beforeOrphan target =>
        simp [hp, hm, hplace, placeOrphanTail, List.mem_append, hb,
          overlay_mem_insertOverlayBefore_iff]

/-- No overlay slot appears when the overlay is absent. -/
theorem no_overlay_when_absent (groups : List Int) (msgs : List Msg)
    (pending : Option PendingPlacement) :
    Slot.overlay ∉ buildOrder groups msgs pending none := by
  unfold buildOrder
  have hb := overlay_not_in_body groups msgs
  have ho := overlay_not_in_orphans groups msgs
  have hp := overlay_mem_placePending_iff pending (body groups msgs)
  simp [hp, List.mem_append, hb, ho]

/-! ## Partition: every tool group is placed exactly once -/

/-- A sequence that a surviving message attaches a group to is a real group. -/
theorem attachedSeqs_subset_groups (groups : List Int) (msgs : List Msg) {s : Int}
    (h : s ∈ attachedSeqs groups msgs) : s ∈ groups := by
  unfold attachedSeqs at h
  rw [List.mem_filter] at h
  simpa [hasGroup] using h.2

/-- **Partition (completeness).** Every tool group is either attached to a
surviving message or an orphan — none is dropped. -/
theorem group_attached_or_orphan (groups : List Int) (msgs : List Msg) {s : Int}
    (h : s ∈ groups) : s ∈ attachedSeqs groups msgs ∨ s ∈ orphans groups msgs := by
  by_cases ha : s ∈ attachedSeqs groups msgs
  · exact Or.inl ha
  · refine Or.inr ?_
    unfold orphans
    rw [List.mem_filter]
    exact ⟨h, by simpa using ha⟩

/-- **Partition (disjointness).** No tool group is both attached and an orphan —
none is placed twice. -/
theorem group_not_both (groups : List Int) (msgs : List Msg) {s : Int}
    (ha : s ∈ attachedSeqs groups msgs) : s ∉ orphans groups msgs := by
  unfold orphans
  rw [List.mem_filter]
  simp [ha]

/-! ## Tail structure -/

theorem pending_not_in_body (groups : List Int) (msgs : List Msg) :
    Slot.pending ∉ body groups msgs := by
  unfold body
  generalize (kept msgs) = ks
  generalize ([] : List Int) = acc
  induction ks generalizing acc with
  | nil => simp [bodyGo]
  | cons m rest ih =>
      unfold bodyGo
      by_cases hg : hasGroup groups m.seq ∧ m.seq ∉ acc
      · simp only [hg, if_true]; cases m.emitsItem <;> simp [List.mem_append, ih]
      · simp only [hg, if_false]; cases m.emitsItem <;> simp [List.mem_append, ih]

theorem pending_mem_insertPendingBefore (target : Int) (slots : List Slot) :
    Slot.pending ∈ insertPendingBefore target slots := by
  induction slots with
  | nil => simp [insertPendingBefore]
  | cons slot rest ih =>
      cases slot with
      | message key seq role =>
          by_cases h : seq = target <;> simp [insertPendingBefore, h, ih]
      | toolGroup seq =>
          by_cases h : seq = target <;> simp [insertPendingBefore, h, ih]
      | pending =>
          change Slot.pending ∈ Slot.pending :: insertPendingBefore target rest
          exact List.mem_cons_self Slot.pending _
      | overlay => simp [insertPendingBefore, ih]

theorem pending_mem_placePending_iff (pending : Option PendingPlacement)
    (slots : List Slot) (habsent : Slot.pending ∉ slots) :
    (Slot.pending ∈ placePending pending slots) ↔ pending.isSome := by
  cases pending with
  | none => simp [placePending, habsent]
  | some placement =>
      cases placement with
      | tail => simp [placePending]
      | beforeMessage target => simp [placePending, pending_mem_insertPendingBefore]

/-- The pending turn appears exactly when a turn is pending. -/
theorem pending_shown_iff (groups : List Int) (msgs : List Msg)
    (pending : Option PendingPlacement) (overlay : Option Overlay) :
    (Slot.pending ∈ buildOrder groups msgs pending overlay) ↔ pending.isSome := by
  unfold buildOrder
  have hb := pending_not_in_body groups msgs
  have hp := pending_mem_placePending_iff pending (body groups msgs) hb
  have ho : Slot.pending ∉ (orphans groups msgs).map Slot.toolGroup := by simp
  cases overlay with
  | none => simpa [List.mem_append, ho] using hp
  | some o =>
      cases hm : o.hasDurableOwner <;> cases hplace : o.placement with
      | tail =>
          simpa [hm, hplace, placeOrphanTail, List.mem_append, ho] using hp
      | beforeOrphan target =>
          simpa [hm, hplace, placeOrphanTail, List.mem_append, ho,
            pending_mem_insertOverlayBefore_iff] using hp

/-- **Tail overlay is last.** When no running orphan tool needs the live
reasoning placed before it, an emitted overlay remains the final slot. -/
theorem overlay_is_last (groups : List Int) (msgs : List Msg)
    (pending : Option PendingPlacement) (o : Overlay) (hshow : o.hasDurableOwner = false)
    (htail : o.placement = .tail) :
    (buildOrder groups msgs pending (some o)).getLast? = some Slot.overlay := by
  unfold buildOrder
  simp [hshow, htail, placeOrphanTail, List.getLast?_append]

/-- Prefix groups remain before the overlay when it targets a later orphan. -/
theorem insertOverlayBefore_prefix (target : Int) (visible : List Slot)
    (earlier suffix : List Int) (hnot : target ∉ earlier) :
    insertOverlayBefore target visible (earlier ++ target :: suffix) =
      earlier.map Slot.toolGroup
        ++ visible
        ++ Slot.toolGroup target :: suffix.map Slot.toolGroup := by
  induction earlier with
  | nil => simp [insertOverlayBefore]
  | cons seq rest ih =>
      simp only [List.mem_cons, not_or] at hnot
      have hseq : seq ≠ target := Ne.symm hnot.1
      simp [insertOverlayBefore, hseq, ih hnot.2, List.append_assoc]

/-- **Running-tool overlay shape.** The emitted overlay appears immediately
before its target orphan while all earlier historical orphans stay earlier. -/
theorem overlay_before_target_shape (groups : List Int) (msgs : List Msg)
    (pending : Option PendingPlacement) (o : Overlay) (target : Int) (earlier suffix : List Int)
    (hshow : o.hasDurableOwner = false)
    (hplace : o.placement = .beforeOrphan target)
    (horphans : orphans groups msgs = earlier ++ target :: suffix)
    (hnot : target ∉ earlier) :
    buildOrder groups msgs pending (some o) =
      placePending pending (body groups msgs)
        ++ earlier.map Slot.toolGroup
        ++ [Slot.overlay]
        ++ Slot.toolGroup target :: suffix.map Slot.toolGroup := by
  unfold buildOrder
  simp [hshow, hplace, placeOrphanTail, horphans,
    insertOverlayBefore_prefix target [Slot.overlay] earlier suffix hnot]

/-! ## Dedup: no message is shown twice -/

/-- The surviving messages have distinct keys, and none was already seen. First-
wins dedup is what stops a shell from rendering the same message twice. -/
theorem dedup_keys_nodup (msgs : List Msg) :
    ∀ seenKeys seenTokens,
      ((dedup seenKeys seenTokens msgs).map Msg.key).Nodup ∧
        ∀ k ∈ (dedup seenKeys seenTokens msgs).map Msg.key, k ∉ seenKeys := by
  induction msgs with
  | nil => intro _ _; simp [dedup]
  | cons m rest ih =>
      intro seenKeys seenTokens
      by_cases hk : m.key ∈ seenKeys
      · simp only [dedup, hk, if_true]
        exact ih seenKeys seenTokens
      · match hmt : m.token with
        | some t =>
            by_cases ht : t ∈ seenTokens
            · simp only [dedup, hk, if_false, hmt, ht, if_true]
              obtain ⟨hnd, hns⟩ := ih (m.key :: seenKeys) seenTokens
              exact ⟨hnd, fun k hkm => (hns k hkm) ∘ List.mem_cons_of_mem _⟩
            · simp only [dedup, hk, if_false, hmt, ht, if_false, List.map_cons, List.nodup_cons]
              obtain ⟨hnd, hns⟩ := ih (m.key :: seenKeys) (t :: seenTokens)
              refine ⟨⟨fun hmem => hns m.key hmem (List.mem_cons_self ..), hnd⟩, ?_⟩
              intro k hk'
              rcases List.mem_cons.mp hk' with h | h
              · subst h; exact hk
              · exact (hns k h) ∘ List.mem_cons_of_mem _
        | none =>
            simp only [dedup, hk, if_false, hmt, List.map_cons, List.nodup_cons]
            obtain ⟨hnd, hns⟩ := ih (m.key :: seenKeys) seenTokens
            refine ⟨⟨fun hmem => hns m.key hmem (List.mem_cons_self ..), hnd⟩, ?_⟩
            intro k hk'
            rcases List.mem_cons.mp hk' with h | h
            · subst h; exact hk
            · exact (hns k h) ∘ List.mem_cons_of_mem _

/-- **Each surviving message key is unique.** -/
theorem kept_keys_nodup (msgs : List Msg) :
    ((kept msgs).map Msg.key).Nodup :=
  (dedup_keys_nodup msgs [] []).1

end ClientShell.Timeline
