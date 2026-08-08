import Proofs.Basic
import Mathlib.Data.Finset.Basic
import Mathlib.Data.List.Basic

namespace ScopeTemplates

abbrev TemplateId := String
abbrev Did := String

inductive Delivery where
  | push
  | replicate
  deriving DecidableEq, Repr

inductive DidSource where
  | localDid
  | peerDid
  | homeDid
  deriving DecidableEq, Repr

structure CollectionRule where
  collection : String
  field : String
  source : DidSource
  deriving DecidableEq, Repr

inductive Scope where
  | peerDid (field : String)
  | unscoped
  | perCollection (rules : List CollectionRule)
  deriving DecidableEq, Repr

structure ScopeFilterKey where
  field : String
  value : Did
  deriving DecidableEq, Repr

structure CollectionScopeFilter where
  collection : String
  field : String
  value : Did
  deriving DecidableEq, Repr

structure Template where
  id : TemplateId
  collections : Finset String
  scope : Scope
  delivery : Delivery
  deriving DecidableEq

abbrev Catalog := List Template

def conversationTranscriptCollections : List String :=
  ["AgentRequest", "AgentResponse", "AgentResponseOutcome", "AgentMessage", "AgentToolCall",
   "AgentToolResult", "AgentToolApproval", "AgentSession", "AgentConversation", "CompactionEntry",
   "BearerPairingReady"]

def agentConfigCollections : List String :=
  ["AgentBehavior", "ToolSelection", "InferenceBackend", "InferenceProfile",
   "ToolServiceRegistry", "Skill"]

def conversationCollections : List String :=
  conversationTranscriptCollections ++ agentConfigCollections

def machineCollections : List String :=
  conversationCollections ++ ["AgentDirectoryEntry"]

def discoveryCollections : List String :=
  ["AgentNetwork", "NetworkMembership", "PeerEndpoint", "NetworkJoinRequest",
   "AgentBehavior", "ToolSelection", "InferenceBackend", "InferenceProfile",
   "ToolServiceRegistry", "Skill"]

def networkControlCollections : List String :=
  ["AgentNetwork", "NetworkMembership", "PeerEndpoint", "NetworkJoinRequest"]

def subagentHostCollections : List String :=
  ["AgentRequest", "AgentResponse", "AgentResponseOutcome", "AgentMessage", "AgentToolCall",
   "AgentToolApproval"]

def schedulerOwnerCollections : List String :=
  ["EventTriggerActivation", "EventDeliveryAdmission"]

def conversationRules : List CollectionRule :=
  [ { collection := "AgentRequest",      field := "requester_did", source := .peerDid }
  , { collection := "AgentResponse",     field := "requester_did", source := .peerDid }
  , { collection := "AgentResponseOutcome", field := "requester_did", source := .peerDid }
  , { collection := "AgentMessage",      field := "requester_did", source := .peerDid }
  , { collection := "AgentToolCall",     field := "requester_did", source := .peerDid }
  , { collection := "AgentToolResult",   field := "requester_did", source := .peerDid }
  , { collection := "AgentToolApproval", field := "requester_did", source := .peerDid }
  , { collection := "AgentSession",      field := "requester_did", source := .peerDid }
  , { collection := "AgentConversation", field := "requester_did", source := .peerDid }
  , { collection := "CompactionEntry",   field := "requester_did", source := .peerDid }
  , { collection := "BearerPairingReady", field := "claimant_did", source := .peerDid } ]

def machineRules : List CollectionRule :=
  conversationRules ++
    [ { collection := "AgentDirectoryEntry", field := "source_did", source := .homeDid } ]

def subagentCoordinatorRules : List CollectionRule :=
  [ { collection := "AgentToolCall", field := "spawn_target_did", source := .peerDid } ]

def subagentHostRules : List CollectionRule :=
  [ { collection := "AgentRequest",      field := "requester_did", source := .peerDid }
  , { collection := "AgentResponse",     field := "requester_did", source := .peerDid }
  , { collection := "AgentResponseOutcome", field := "requester_did", source := .peerDid }
  , { collection := "AgentMessage",      field := "requester_did", source := .peerDid }
  , { collection := "AgentToolCall",     field := "requester_did", source := .peerDid }
  , { collection := "AgentToolApproval", field := "requester_did", source := .peerDid } ]

def schedulerOwnerRules : List CollectionRule :=
  [ { collection := "EventTriggerActivation", field := "agent_did", source := .localDid }
  , { collection := "EventDeliveryAdmission", field := "agent_did", source := .localDid } ]

def conversationTemplate : Template :=
  { id := "conversation"
  , collections := conversationCollections.toFinset
  , scope := .perCollection conversationRules
  , delivery := .push }

def machineTemplate : Template :=
  { id := "machine"
  , collections := machineCollections.toFinset
  , scope := .perCollection machineRules
  , delivery := .push }

def agentConfigTemplate : Template :=
  { id := "agent-config"
  , collections := agentConfigCollections.toFinset
  , scope := .unscoped
  , delivery := .replicate }

def backupTemplate : Template :=
  { id := "backup"
  , collections := conversationTranscriptCollections.toFinset
  , scope := .unscoped
  , delivery := .replicate }

def discoveryTemplate : Template :=
  { id := "discovery"
  , collections := discoveryCollections.toFinset
  , scope := .unscoped
  , delivery := .replicate }

def networkControlTemplate : Template :=
  { id := "network-control"
  , collections := networkControlCollections.toFinset
  , scope := .unscoped
  , delivery := .replicate }

def subagentCoordinatorTemplate : Template :=
  { id := "subagent-coordinator"
  , collections := ["AgentToolCall"].toFinset
  , scope := .perCollection subagentCoordinatorRules
  , delivery := .push }

def subagentHostTemplate : Template :=
  { id := "subagent-host"
  , collections := subagentHostCollections.toFinset
  , scope := .perCollection subagentHostRules
  , delivery := .push }

def schedulerOwnerTemplate : Template :=
  { id := "scheduler-owner"
  , collections := schedulerOwnerCollections.toFinset
  , scope := .perCollection schedulerOwnerRules
  , delivery := .replicate }

def appCollectionsTemplate : Template :=
  { id := "app-collections"
  , collections := (∅ : Finset String)
  , scope := .unscoped
  , delivery := .replicate }

def builtinCatalog : Catalog :=
  [ conversationTemplate
  , machineTemplate
  , agentConfigTemplate
  , backupTemplate
  , discoveryTemplate
  , networkControlTemplate
  , subagentCoordinatorTemplate
  , subagentHostTemplate
  , schedulerOwnerTemplate
  , appCollectionsTemplate ]

end ScopeTemplates
