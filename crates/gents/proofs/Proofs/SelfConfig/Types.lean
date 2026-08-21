import Mathlib.Data.List.Basic

namespace SelfConfig

inductive Target where
  | agentBehavior
  | toolSelection
  | inferenceProfile
  | inferenceBackend
  | toolServiceRegistry
  | task
  | schedule
  | eventTrigger
  deriving DecidableEq, Repr

def allTargets : List Target :=
  [ .agentBehavior
  , .toolSelection
  , .inferenceProfile
  , .inferenceBackend
  , .toolServiceRegistry
  , .task
  , .schedule
  , .eventTrigger
  ]

def Target.collectionName : Target → String
  | .agentBehavior => "AgentBehavior"
  | .toolSelection => "ToolSelection"
  | .inferenceProfile => "InferenceProfile"
  | .inferenceBackend => "InferenceBackend"
  | .toolServiceRegistry => "ToolServiceRegistry"
  | .task => "Task"
  | .schedule => "Schedule"
  | .eventTrigger => "EventTrigger"

def Target.uniqueField : Target → String
  | .agentBehavior => "behavior_id"
  | .toolSelection => "selection_id"
  | .inferenceProfile => "profile_id"
  | .inferenceBackend => "backend_id"
  | .toolServiceRegistry => "service_id"
  | .task => "task_id"
  | .schedule => "schedule_id"
  | .eventTrigger => "trigger_id"

def Target.category : Target → String
  | .agentBehavior => "behavior"
  | .toolSelection => "tools"
  | .inferenceProfile => "profile"
  | .inferenceBackend => "backend"
  | .toolServiceRegistry => "mcp_service"
  | .task => "automation"
  | .schedule => "automation"
  | .eventTrigger => "automation"

def selfConfigCategories : List String :=
  ["behavior", "tools", "profile", "backend", "mcp_service", "automation", "persona"]

def defaultCategories : List String :=
  ["behavior", "tools", "profile"]

abbrev FieldKey := String

def allFields : Target → List FieldKey
  | .agentBehavior =>
      [ "behavior_id", "agent_did", "display_name", "description", "summary"
      , "system_prompt", "request_context_template", "backend_id", "model_name"
      , "tool_selection_id", "inference_profile_id", "compaction_strategy"
      , "compaction_threshold", "enabled", "skill_refs", "skill_excludes"
      , "created_at", "updated_at" ]
  | .toolSelection =>
      [ "selection_id", "agent_did", "display_name", "tool_policy_version"
      , "enable_file_tools", "file_tools_mode", "file_tool_root", "enable_bash"
      , "bash_mode", "command_execution_policy", "command_allowed_argv_prefixes"
      , "command_forbidden_argv_prefixes", "read_only_command_allowlist"
      , "command_network_mode", "cli_tool_names", "enable_meta_tools"
      , "allowed_mcp_service_ids", "delegate_to", "backgroundable_tool_names"
      , "approval_required_tools", "subagent_targets", "subagent_spawn_enabled"
      , "orchestration_enabled"
      , "subagent_steering_enabled", "subagent_background_enabled"
      , "subagent_default_await_mode", "subagent_allow_cross_deployment"
      , "cross_deployment_spawn_timeout_seconds", "enable_memory"
      , "enable_session_history_tool", "enable_context_budget"
      , "enable_defra_query", "defra_query_collections", "write_tools"
      , "datastore_tool_surface_ids"
      , "enable_self_config", "self_config_categories"
      , "self_config_no_lockout", "self_config_dry_run", "enable_lsp"
      , "lsp_config", "updated_at" ]
  | .inferenceProfile =>
      [ "profile_id", "display_name", "context_window", "max_output_tokens"
      , "max_turns", "temperature", "top_p", "top_k", "seed", "min_p", "frequency_penalty", "presence_penalty", "repetition_penalty", "reasoning_effort", "stream_batch_ms"
      , "stream_liveness_timeout_secs", "deadline_duration_secs"
      , "retry_max_transport", "retry_backoff_ms", "retry_max_resample"
      , "retry_allow_repair", "retry_interactive_max", "updated_at" ]
  | .inferenceBackend =>
      [ "backend_id", "name", "provider_kind", "openai_wire_api", "endpoint"
      , "api_key", "api_key_env_var", "max_concurrent", "max_queue_depth"
      , "enabled", "models", "last_probe", "probe_status", "updated_at" ]
  | .toolServiceRegistry =>
      [ "service_id", "display_name", "description", "hostname", "tailscale_ip"
      , "lan_ip", "mcp_port", "mcp_path", "send_agent_did", "status", "version"
      , "updated_at" ]
  | .task =>
      [ "task_id", "name", "description", "behavior_id", "prompt_template"
      , "enabled", "output_schema_ref", "created_at", "updated_at" ]
  | .schedule =>
      [ "schedule_id", "task_id", "interval_secs", "cron", "timezone"
      , "missed_run_policy", "enabled", "concurrency", "next_run_at"
      , "last_attempt_at", "last_status", "last_error", "fire_count"
      , "created_at", "updated_at" ]
  | .eventTrigger =>
      [ "trigger_id", "task_id", "source_collection", "event_kind", "filter"
      , "enabled", "concurrency", "correlation_field", "fire_mode"
      , "expected_count", "expected_count_field", "group_timeout_secs"
      , "group_min_count", "created_at", "updated_at", "last_attempt_at"
      , "last_fired_source_doc_id", "last_status", "last_error", "fire_count" ]

def writableFields : Target → List FieldKey
  | .agentBehavior =>
      [ "display_name", "description", "summary", "system_prompt"
      , "request_context_template", "backend_id", "model_name"
      , "tool_selection_id", "inference_profile_id", "compaction_strategy"
      , "compaction_threshold", "enabled", "skill_refs", "skill_excludes" ]
  | .toolSelection =>
      [ "display_name", "enable_file_tools", "file_tools_mode", "file_tool_root"
      , "enable_bash", "bash_mode", "command_execution_policy"
      , "command_allowed_argv_prefixes", "command_forbidden_argv_prefixes"
      , "read_only_command_allowlist", "command_network_mode", "cli_tool_names"
      , "enable_meta_tools", "allowed_mcp_service_ids"
      , "backgroundable_tool_names", "approval_required_tools", "subagent_targets"
      , "subagent_spawn_enabled"
      , "subagent_steering_enabled", "subagent_background_enabled"
      , "subagent_default_await_mode", "subagent_allow_cross_deployment"
      , "cross_deployment_spawn_timeout_seconds", "enable_memory"
      , "enable_session_history_tool", "enable_context_budget"
      , "enable_defra_query", "defra_query_collections", "enable_self_config"
      , "self_config_categories", "self_config_no_lockout"
      , "self_config_dry_run", "enable_lsp", "lsp_config" ]
  | .inferenceProfile =>
      [ "display_name", "context_window", "max_output_tokens", "max_turns"
      , "temperature", "top_p", "top_k", "seed", "min_p", "frequency_penalty", "presence_penalty", "repetition_penalty", "reasoning_effort", "stream_batch_ms", "stream_liveness_timeout_secs"
      , "deadline_duration_secs", "retry_max_transport", "retry_backoff_ms"
      , "retry_max_resample", "retry_allow_repair", "retry_interactive_max" ]
  | .inferenceBackend =>
      [ "name", "provider_kind", "openai_wire_api", "endpoint"
      , "api_key_env_var", "max_concurrent", "max_queue_depth", "enabled"
      , "models" ]
  | .toolServiceRegistry =>
      [ "display_name", "description", "hostname", "tailscale_ip", "lan_ip"
      , "mcp_port", "mcp_path", "send_agent_did", "status" ]
  | .task =>
      [ "name", "description", "prompt_template", "enabled"
      , "output_schema_ref" ]
  | .schedule =>
      [ "task_id", "interval_secs", "cron", "timezone", "missed_run_policy"
      , "enabled", "concurrency" ]
  | .eventTrigger =>
      [ "task_id", "source_collection", "event_kind", "filter", "enabled"
      , "concurrency", "correlation_field", "fire_mode", "expected_count"
      , "expected_count_field", "group_timeout_secs", "group_min_count" ]

def protectedFields (t : Target) : List FieldKey :=
  (allFields t).filter (fun k => decide (k ∉ writableFields t))

theorem writable_subset_all :
    ∀ t ∈ allTargets, ∀ k ∈ writableFields t, k ∈ allFields t := by
  native_decide

theorem all_fields_nodup : ∀ t ∈ allTargets, (allFields t).Nodup := by
  native_decide

theorem writable_fields_nodup : ∀ t ∈ allTargets, (writableFields t).Nodup := by
  native_decide

theorem unique_field_protected :
    ∀ t ∈ allTargets, t.uniqueField ∈ protectedFields t := by
  native_decide

theorem agent_did_never_writable :
    ∀ t ∈ allTargets, "agent_did" ∉ writableFields t := by
  native_decide

theorem api_key_protected :
    "api_key" ∈ protectedFields .inferenceBackend
      ∧ "api_key_env_var" ∈ writableFields .inferenceBackend := by
  native_decide

theorem backend_probe_fields_protected :
    "probe_status" ∈ protectedFields .inferenceBackend
      ∧ "last_probe" ∈ protectedFields .inferenceBackend := by
  native_decide

theorem automation_runtime_fields_protected :
    (["next_run_at", "last_attempt_at", "last_status", "last_error",
      "fire_count"]).all (fun k => decide (k ∈ protectedFields .schedule))
      ∧ (["last_attempt_at", "last_fired_source_doc_id", "last_status",
          "last_error", "fire_count"]).all
          (fun k => decide (k ∈ protectedFields .eventTrigger)) := by
  native_decide

theorem recreate_identity_field_protected :
    ∀ t ∈ allTargets,
      "updated_at" ∈ allFields t → "updated_at" ∈ protectedFields t := by
  native_decide

theorem categories_well_formed :
    (∀ t ∈ allTargets, t.category ∈ selfConfigCategories)
      ∧ ∀ c ∈ defaultCategories, c ∈ selfConfigCategories := by
  native_decide

end SelfConfig
