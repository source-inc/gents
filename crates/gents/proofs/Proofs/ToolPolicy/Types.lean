import Mathlib.Data.Finset.Basic
import Mathlib.Data.Finset.Lattice.Basic

namespace ToolPolicy

abbrev ToolId := String

inductive FileCap where
  | off
  | readOnly
  | readWrite
  deriving DecidableEq, Repr

structure ValueMeet (V : Type) where
  vmeet : V → V → V
  vle : V → V → Prop
  vle_refl : ∀ a, vle a a
  vmeet_le_left : ∀ a b, vle (vmeet a b) a
  vmeet_le_right : ∀ a b, vle (vmeet a b) b

inductive EndpointScope (K V : Type) where
  | none
  | only (keys : Finset K) (val : K → V)
  | all

inductive ExecMode where
  | readOnly
  | workspaceWrite
  | unrestricted
  deriving DecidableEq, Repr

inductive NetMode where
  | disabled
  | inherit
  | enabled
  deriving DecidableEq, Repr

structure BashPolicy where
  mode : ExecMode
  network : NetMode
  forbidden : Finset (List String)
  allowed : EndpointScope (List String) Unit
  readOnly : EndpointScope String Unit
  sandbox : Bool

structure Surface where
  file : FileCap
  bash : BashPolicy
  meta : Bool
  defraQuery : Bool
  selfConfig : Bool
  memory : Bool
  sessionHistory : Bool
  contextBudget : Bool
  spawn : Bool
  steering : Bool
  background : Bool
  crossDeployment : Bool
  skills : Bool
  lsp : Bool
  cliTools : EndpointScope ToolId (Finset String)
  mcpServices : EndpointScope ToolId Unit
  defraCollections : EndpointScope ToolId Unit
  selfConfigCategories : EndpointScope ToolId Unit
  subagentTargets : EndpointScope (String × String) Unit
  backgroundTools : EndpointScope ToolId Unit
  writeTools : EndpointScope (String × String) (Finset String)
  queryTools : EndpointScope (String × String) (Finset String)

abbrev Avail := Surface

end ToolPolicy
