import Proofs.SessionHydration.Executable

namespace SessionHydration

theorem terminalFor_insert_self (st : State) (r : Request) (outcome : Outcome) (count : Nat) :
    terminalFor { st with terminals := insert (terminal r outcome count) st.terminals } r.key := by
  exact ⟨terminal r outcome count, Finset.mem_insert_self _ _, rfl⟩

/-- A request that fails admission never delivers a document. -/
theorem hydration_request_grants_nothing (cat : Catalog) (st : State) (r : Request)
    (hnot : ¬ admits cat r) : (applyStep cat st r).delivered = st.delivered := by
  unfold applyStep
  split <;> simp_all

/-- Every selected document is scoped to the requester's lineage. -/
theorem selected_tenancy_sound (cat : Catalog) (r : Request) (doc : Document)
    (hdoc : doc ∈ selectedDocuments cat r) : doc.requester = r.requester := by
  simp [selectedDocuments, eligible] at hdoc
  exact hdoc.2.2.1

/-- Selection also preserves the exact agent/session ownership tuple. -/
theorem selected_session_sound (cat : Catalog) (r : Request) (doc : Document)
    (hdoc : doc ∈ selectedDocuments cat r) :
    doc.agent = r.agent ∧ doc.session = r.session := by
  simp [selectedDocuments, eligible] at hdoc
  exact ⟨hdoc.2.2.2.1, hdoc.2.2.2.2⟩

/-- Hydration can only select the seven transcript collections named by #1142. -/
theorem selected_collection_sound (cat : Catalog) (r : Request) (doc : Document)
    (hdoc : doc ∈ selectedDocuments cat r) : doc.collection ∈ transcriptCollections := by
  simp [selectedDocuments, eligible] at hdoc
  exact hdoc.2.1

/-- An unknown or mismatched session owner cannot cause delivery. -/
theorem session_ownership_required (cat : Catalog) (st : State) (r : Request)
    (howner : ownedSession r ∉ cat.sessions) :
    (applyStep cat st r).delivered = st.delivered := by
  apply hydration_request_grants_nothing
  intro hadmits
  exact howner hadmits.2.2

/-- One sweep always records a terminal outcome for a previously pending key. -/
theorem pending_reaches_terminal (cat : Catalog) (st : State) (r : Request)
    (hpending : ¬ terminalFor st r.key) :
    terminalFor (applyStep cat st r) r.key := by
  unfold applyStep
  rw [if_neg hpending]
  split
  · apply terminalFor_insert_self
  · apply terminalFor_insert_self

theorem terminal_request_is_noop (cat : Catalog) (st : State) (r : Request)
    (hterminal : terminalFor st r.key) : applyStep cat st r = st := by
  simp [applyStep, hterminal]

/-- Re-serving after a crash or duplicate event is idempotent. -/
theorem applyStep_idempotent (cat : Catalog) (st : State) (r : Request) :
    applyStep cat (applyStep cat st r) r = applyStep cat st r := by
  by_cases hterminal : terminalFor st r.key
  · rw [terminal_request_is_noop cat st r hterminal]
    exact terminal_request_is_noop cat st r hterminal
  · have hafter := pending_reaches_terminal cat st r hterminal
    exact terminal_request_is_noop cat (applyStep cat st r) r hafter

/-- Hydration is not a scope/template transition and cannot flap pairing. -/
theorem pairing_noninterference (cat : Catalog) (st : State) (r : Request) :
    (applyStep cat st r).pairingState = st.pairingState := by
  unfold applyStep
  by_cases hterminal : terminalFor st r.key
  · rw [if_pos hterminal]
  · rw [if_neg hterminal]
    by_cases hadmits : admits cat r
    · rw [if_pos hadmits]
    · rw [if_neg hadmits]

end SessionHydration
