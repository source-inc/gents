import Proofs.SessionHydration
import Proofs.Conformance.Contracts.Json.Helpers
import Proofs.Conformance.ContractCases.Types

namespace Conformance.Contracts

open Conformance.ContractCases

structure SessionHydrationDecisionCase where
  name : String
  paired : Bool
  activeMember : Bool
  ownsSession : Bool

def hydrationRequest : SessionHydration.Request :=
  { key := "peer-1:session-1"
  , peer := "peer-1"
  , requester := "did:key:requester-1"
  , agent := "did:key:agent-1"
  , session := "session-1" }

def hydrationOwnedDocument : SessionHydration.Document :=
  { collection := "AgentMessage", id := "owned"
  , requester := hydrationRequest.requester
  , agent := hydrationRequest.agent
  , session := hydrationRequest.session }

def hydrationForeignDocument : SessionHydration.Document :=
  { collection := "AgentMessage", id := "foreign"
  , requester := "did:key:requester-2"
  , agent := hydrationRequest.agent
  , session := hydrationRequest.session }

def hydrationWrongCollectionDocument : SessionHydration.Document :=
  { collection := "AgentSession", id := "wrong-collection"
  , requester := hydrationRequest.requester
  , agent := hydrationRequest.agent
  , session := hydrationRequest.session }

def hydrationCatalog (w : SessionHydrationDecisionCase) : SessionHydration.Catalog :=
  { pairedPeers := if w.paired then [hydrationRequest.peer].toFinset else ∅
  , activeMembers := if w.activeMember then [hydrationRequest.requester].toFinset else ∅
  , sessions := if w.ownsSession then [SessionHydration.ownedSession hydrationRequest].toFinset else ∅
  , documents := [hydrationOwnedDocument, hydrationForeignDocument,
      hydrationWrongCollectionDocument].toFinset }

def sessionHydrationDecisionCases : List SessionHydrationDecisionCase :=
  [ { name := "admitted", paired := true, activeMember := true, ownsSession := true }
  , { name := "unpaired", paired := false, activeMember := true, ownsSession := true }
  , { name := "inactive_member", paired := true, activeMember := false, ownsSession := true }
  , { name := "unowned_session", paired := true, activeMember := true, ownsSession := false } ]

def sessionHydrationDecisionCaseJson (w : SessionHydrationDecisionCase) : String :=
  let cat := hydrationCatalog w
  "{"
    ++ "\"name\":" ++ jsonString w.name ++ ","
    ++ "\"paired\":" ++ boolString w.paired ++ ","
    ++ "\"active_member\":" ++ boolString w.activeMember ++ ","
    ++ "\"owns_session\":" ++ boolString w.ownsSession ++ ","
    ++ "\"expected_admit\":" ++ boolString (SessionHydration.decideAdmits cat hydrationRequest) ++ ","
    ++ "\"expected_selected_count\":" ++
      toString (SessionHydration.selectedDocuments cat hydrationRequest).card
    ++ "}"

def sessionHydrationDecisionCasesJson : String :=
  jsonArray (sessionHydrationDecisionCases.map sessionHydrationDecisionCaseJson)

end Conformance.Contracts
