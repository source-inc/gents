import Proofs.EventDelivery.Contract

namespace EventDelivery

def pendingWork (w : World) : Nat :=
  (w.persistentSet.filter (fun d => d ∉ w.processedSet)).length

def Fair (inst : SourceInstance) (actions : List Action) : Prop :=
  ∀ i : Nat, i + inst.rescanBoundedBy < actions.length →
    ∃ j : Nat, i ≤ j ∧ j ≤ i + inst.rescanBoundedBy ∧
      (actions[j]?).map Action.isRescan = some true

theorem Fair.nil (inst : SourceInstance) : Fair inst [] := by
  intro i h_lt
  simp at h_lt

theorem Fair.singleton_rescanTick
    (inst : SourceInstance) (h_pos : 0 < inst.rescanBoundedBy) :
    Fair inst [.rescanTick] := by
  intro i h_lt
  simp only [List.length_singleton] at h_lt
  have h_i : i = 0 := by omega
  have h_b : inst.rescanBoundedBy = 0 := by omega
  exact absurd h_b (Nat.ne_zero_of_lt h_pos)

inductive TraceOf : World → List Action → World → Prop where
  | nil  {w : World} : TraceOf w [] w
  | cons {w₁ a w₂ as w₃} :
      Transition w₁ a w₂ → TraceOf w₂ as w₃ → TraceOf w₁ (a :: as) w₃

theorem D1_delivery_convergence
    (inst : SourceInstance)
    (w₀ : World) (d : DocId)
    (h_persisted : d ∈ w₀.persistentSet)
    (h_unprocessed : d ∉ w₀.processedSet)
    (h_inst_pos : 0 < inst.rescanBoundedBy) :
    ∃ (actions : List Action) (w' : World),
      TraceOf w₀ actions w' ∧
      Fair inst actions ∧
      d ∈ w'.handled := by
  let w₁ : World :=
    { w₀ with subscriptionQueue :=
        (w₀.persistentSet.filter (fun x => x ∉ w₀.processedSet)) ++ w₀.subscriptionQueue }
  let w₂ : World :=
    { w₁ with handled := d :: w₁.handled
            , processedSet := d :: w₁.processedSet
            , subscriptionQueue := w₁.subscriptionQueue.erase d }
  refine ⟨[.rescanTick, .handle d], w₂, ?_, ?_, ?_⟩
  ·
    have h_rescan : Transition w₀ .rescanTick w₁ := Transition.rescanTick w₀
    have h_mem_q : d ∈ w₁.subscriptionQueue := by
      show d ∈ (w₀.persistentSet.filter (fun x => x ∉ w₀.processedSet)) ++ w₀.subscriptionQueue
      apply List.mem_append.mpr
      left
      apply List.mem_filter.mpr
      refine ⟨h_persisted, ?_⟩
      simp [h_unprocessed]
    have h_unproc₁ : d ∉ w₁.processedSet := h_unprocessed
    have h_handle : Transition w₁ (.handle d) w₂ := Transition.handle w₁ d h_mem_q h_unproc₁
    exact TraceOf.cons h_rescan (TraceOf.cons h_handle TraceOf.nil)
  ·
    intro i h_window
    have h_len : ([Action.rescanTick, Action.handle d]).length = 2 := rfl
    rw [h_len] at h_window
    have h_i : i = 0 := by omega
    have h_b : inst.rescanBoundedBy = 1 := by omega
    subst h_i
    refine ⟨0, Nat.le_refl 0, ?_, ?_⟩
    ·
      omega
    ·
      rfl
  ·
    show d ∈ d :: w₁.handled
    exact List.mem_cons_self _ _

theorem D2_fair_delivery_latency
    (w₀ : World) (d : DocId)
    (h_persisted : d ∈ w₀.persistentSet)
    (h_unprocessed : d ∉ w₀.processedSet) :
    ∃ (actions : List Action) (w' : World),
      TraceOf w₀ actions w' ∧
      d ∈ w'.handled ∧
      Action.rescanTick ∉ actions := by
  let w₁ : World :=
    { w₀ with subscriptionQueue := d :: w₀.subscriptionQueue }
  let w₂ : World :=
    { w₁ with handled := d :: w₁.handled
            , processedSet := d :: w₁.processedSet
            , subscriptionQueue := w₁.subscriptionQueue.erase d }
  refine ⟨[.enqueue d, .handle d], w₂, ?_, ?_, ?_⟩
  ·
    have h_enq : Transition w₀ (.enqueue d) w₁ := Transition.enqueue w₀ d h_persisted
    have h_mem_q : d ∈ w₁.subscriptionQueue := List.mem_cons_self _ _
    have h_handle : Transition w₁ (.handle d) w₂ :=
      Transition.handle w₁ d h_mem_q h_unprocessed
    exact TraceOf.cons h_enq (TraceOf.cons h_handle TraceOf.nil)
  ·
    show d ∈ d :: w₁.handled
    exact List.mem_cons_self _ _
  ·
    intro h
    simp at h

/-- A lossy subscription wake is never the correctness boundary. If the
document remains durable and unprocessed, dropping its queued wake followed by
one complete durable rescan makes it handleable again. -/
theorem D3_dropped_wake_recovered_by_rescan
    (w₀ : World) (d : DocId)
    (h_persisted : d ∈ w₀.persistentSet)
    (h_queued : d ∈ w₀.subscriptionQueue)
    (h_unprocessed : d ∉ w₀.processedSet) :
    ∃ (w₁ w₂ w₃ : World),
      Transition w₀ (.drop d) w₁ ∧
      Transition w₁ .rescanTick w₂ ∧
      Transition w₂ (.handle d) w₃ ∧
      d ∈ w₃.handled := by
  let w₁ : World :=
    { w₀ with subscriptionQueue := w₀.subscriptionQueue.erase d }
  let w₂ : World :=
    { w₁ with subscriptionQueue :=
        (w₁.persistentSet.filter (fun x => x ∉ w₁.processedSet)) ++
          w₁.subscriptionQueue }
  let w₃ : World :=
    { w₂ with handled := d :: w₂.handled
            , processedSet := d :: w₂.processedSet
            , subscriptionQueue := w₂.subscriptionQueue.erase d }
  refine ⟨w₁, w₂, w₃, Transition.drop w₀ d h_queued,
    Transition.rescanTick w₁, ?_, ?_⟩
  · apply Transition.handle
    · show d ∈
        (w₁.persistentSet.filter (fun x => x ∉ w₁.processedSet)) ++
          w₁.subscriptionQueue
      apply List.mem_append.mpr
      left
      apply List.mem_filter.mpr
      exact ⟨h_persisted, by simpa [w₁] using h_unprocessed⟩
    · simpa [w₂, w₁] using h_unprocessed
  · exact List.mem_cons_self _ _

theorem C1_processed_set_excludes_handle
    (w : World) (d : DocId) (a : Action) (w' : World)
    (h_processed : d ∈ w.processedSet)
    (h : Transition w a w') :
    a ≠ .handle d := by
  intro h_eq
  rw [h_eq] at h
  cases h with
  | handle _ _ h_unprocessed =>
    exact h_unprocessed h_processed

end EventDelivery
