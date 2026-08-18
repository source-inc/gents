import Proofs.Basic
import Mathlib.Data.Finset.Basic
import Mathlib.Data.Finset.Card

/-!
# Session hydration state

The model separates admission from document selection. Pairing, membership,
and session ownership admit a request; requester/session/agent predicates then
select the exact transcript documents eligible for replay. `pairingState` is
opaque state owned by PairingReconcile and is carried here solely to prove that
hydration never changes pairing filters or generations.
-/

namespace SessionHydration

structure Request where
  key : String
  peer : String
  requester : String
  agent : String
  session : String
  deriving DecidableEq, Repr

structure SessionOwner where
  session : String
  requester : String
  agent : String
  deriving DecidableEq, Repr

structure Document where
  collection : String
  id : String
  requester : String
  agent : String
  session : String
  deriving DecidableEq, Repr

structure Catalog where
  pairedPeers : Finset String
  activeMembers : Finset String
  sessions : Finset SessionOwner
  documents : Finset Document
  deriving DecidableEq

inductive Outcome where
  | served
  | rejected
  deriving DecidableEq, Repr

structure Terminal where
  key : String
  outcome : Outcome
  servedCount : Nat
  deriving DecidableEq, Repr

structure State where
  delivered : Finset Document
  terminals : Finset Terminal
  pairingState : Finset String
  deriving DecidableEq

def transcriptCollections : Finset String :=
  ["AgentRequest", "AgentResponse", "AgentMessage", "AgentToolCall",
   "AgentToolResult", "AgentToolApproval", "CompactionEntry"].toFinset

def ownedSession (r : Request) : SessionOwner :=
  { session := r.session, requester := r.requester, agent := r.agent }

def admits (cat : Catalog) (r : Request) : Prop :=
  r.peer ∈ cat.pairedPeers ∧
  r.requester ∈ cat.activeMembers ∧
  ownedSession r ∈ cat.sessions

instance (cat : Catalog) (r : Request) : Decidable (admits cat r) := by
  unfold admits
  infer_instance

def eligible (r : Request) (doc : Document) : Prop :=
  doc.collection ∈ transcriptCollections ∧
  doc.requester = r.requester ∧ doc.agent = r.agent ∧ doc.session = r.session

instance (r : Request) (doc : Document) : Decidable (eligible r doc) := by
  unfold eligible
  infer_instance

def selectedDocuments (cat : Catalog) (r : Request) : Finset Document :=
  cat.documents.filter (eligible r)

def terminalFor (st : State) (key : String) : Prop :=
  ∃ terminal ∈ st.terminals, terminal.key = key

instance (st : State) (key : String) : Decidable (terminalFor st key) := by
  unfold terminalFor
  infer_instance

def terminal (r : Request) (outcome : Outcome) (servedCount : Nat) : Terminal :=
  { key := r.key, outcome, servedCount }

def applyStep (cat : Catalog) (st : State) (r : Request) : State :=
  if terminalFor st r.key then st
  else if admits cat r then
    let docs := selectedDocuments cat r
    { st with
      delivered := st.delivered ∪ docs
      terminals := insert (terminal r .served docs.card) st.terminals }
  else
    { st with terminals := insert (terminal r .rejected 0) st.terminals }

end SessionHydration
