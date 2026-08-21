import Proofs.SessionHydration.State

namespace SessionHydration

def decideAdmits (cat : Catalog) (r : Request) : Bool := decide (admits cat r)

theorem decideAdmits_agrees (cat : Catalog) (r : Request) :
    decideAdmits cat r = true ↔ admits cat r := by
  unfold decideAdmits
  exact decide_eq_true_iff

def decideSelected (r : Request) (doc : Document) : Bool := decide (eligible r doc)

theorem decideSelected_agrees (r : Request) (doc : Document) :
    decideSelected r doc = true ↔
      doc.collection ∈ transcriptCollections ∧ doc.requester = r.requester ∧
        doc.agent = r.agent ∧ doc.session = r.session := by
  simp [decideSelected, eligible]

end SessionHydration
