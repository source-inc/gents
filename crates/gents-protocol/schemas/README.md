# Schema Data Model

This directory contains the non-agent DefraDB GraphQL schemas used by
`gents`. The agent collection schemas live in the dependency-free
`gents-schemas` crate so external document-peer consumers can depend on
the same collection contract without pulling in runtime/protocol dependencies.

The schema is intentionally document-oriented. The runtime resolves behavior and
execution state from documents, then publishes operational state back into
documents for inspection and debugging.

This file is the quick map of what each collection means, how collections relate
to each other, and which subsystem writes them.

Collection changes are governed by the repository-wide
[DefraDB schema design guide](../../../docs/defradb-schema-guide.md) and tracked
in the [schema decision ledger](../../../docs/schema-decision-ledger.md). The
guide defines identity, branchability, replication, ACP, provenance, and
retention requirements that are intentionally broader than this operational
map.

## High-Level Shape

```text
AgentPrincipal
  -> default_behavior_id -> AgentBehavior

AgentBehavior
  -> backend_id -> InferenceBackend
  -> tool_selection_id -> ToolSelection
  -> inference_profile_id -> InferenceProfile

AgentPrincipal / AgentBehavior
  -> runtime publication -> AgentRuntime

Interactive execution:
  AgentRequest -> AgentResponse
               -> InferenceCall
               -> AgentSession
               -> AgentConversation
               -> AgentMessage
               -> AgentToolCall
               -> AgentToolResult
               -> CompactionEntry
               -> RenderedRequest

Scheduled and event-driven execution:
  Task         -> behavior_id -> AgentBehavior
  Schedule     -> task_id     -> Task
  EventTrigger -> task_id     -> Task

Remote tool discovery:
  ToolServiceRegistry
```

## Collection Groups

### Agent Configuration

These documents describe what the agent is and how it should run.

| Collection | Key fields | References | Written by | Read by |
|------------|------------|------------|------------|---------|
| `AgentPrincipal` | `agent_did`, `default_behavior_id`, `enabled` | `default_behavior_id -> AgentBehavior.behavior_id` | `init`, config/bootstrap code | document boot, reconcile, scheduler/task defaulting |
| `AgentBehavior` | `behavior_id`, `agent_did`, `backend_id`, `model_name`, `tool_selection_id`, `inference_profile_id`, `enabled` | `backend_id -> InferenceBackend.backend_id`, `tool_selection_id -> ToolSelection.selection_id`, `inference_profile_id -> InferenceProfile.profile_id` | `init`, `config behavior set`, library builder/document bootstrap | runtime resolution, request routing, scheduler |
| `ToolSelection` | `selection_id`, `agent_did`, file/bash/meta/delegate fields, MCP service allowlist, command execution policy fields (`command_allowed_argv_prefixes` extends via argv prefixes; `read_only_command_allowlist` replaces/narrows the ReadOnly base — see `docs/macos-bash-sandbox.md`) | selected by `AgentBehavior.tool_selection_id` | `init`, `config tools set` | tool-surface resolution |
| `InferenceBackend` | `backend_id`, `provider_kind`, `endpoint`, `api_key`, `api_key_env_var`, `max_concurrent`, `max_queue_depth`, `enabled`, `models`, `probe_status` | selected by `AgentBehavior.backend_id` | `init`, `config backend set`, desired-state manifests, health/probe updates | startup readiness, runtime execution, scheduler execution |
| `InferenceProfile` | `profile_id`, context/output/temperature/deadline fields | selected by `AgentBehavior.inference_profile_id` | `config profile set` | runtime resolution |

### Runtime Observability

These documents expose what the runtime is doing right now.

| Collection | Key fields | Meaning | Written by | Read by |
|------------|------------|---------|------------|---------|
| `AgentRuntime` | `agent_did`, `process_state`, `reconcile_phase`, `active_generation`, `router_generation`, `last_reconcile_result` | current runtime/reconcile state for one agent principal | runtime startup/reconcile/shutdown code | `status`, `show runtime`, debugging |

### Interactive Conversation State

These documents record user requests, assistant output, and conversation history.

| Collection | Key fields | References | Written by | Read by |
|------------|------------|------------|------------|---------|
| `AgentRequest` | `request_id`, `agent_did`, `behavior_id`, `session_id`, sampling overrides, `metadata`, `status`, `lifecycle_state`, `backend_id`, `failure_reason`, `interrupt_requested_at`, `valid_until` | belongs to an agent/session/behavior | `chat`, `request submit`, lifecycle transitions | router, CLI inspection, recovery |
| `InferenceCall` | `call_id`, `request_id`, `backend_id`, `call_kind`, `call_state`, queue/timing/token fields | belongs to a request/backend | admission controller at terminal call state | benchmarking, RL reward shaping, debugging |
| `AgentResponse` | `request_id`, `agent_did`, `behavior_id`, `session_id`, `status`, `content`, `reasoning`, `error_message`, `progress_seq`, `materialized_message_sequence`, `interrupted_at` | latest response for a request; also the in-flight streaming overlay until committed into transcript | streaming/runtime code | `chat`, `response show`, `response wait`, TUI, rich clients |
| `AgentSession` | `session_id`, `behavior_id`, `status`, `started`, `ended` | ties a sequence of requests to one behavior | session manager | `chat`, inspection, recovery |
| `AgentConversation` | `session_id`, `agent_did`, `behavior_id`, `title`, `preview_text`, `status`, `latest_request_id` | high-level conversation summary per session | session/conversation layer | UI and inspection |
| `AgentMessage` | `message_key`, `session_id`, `sequence`, `role`, `content`, `timestamp` | ordered transcript entries | session/history layer | chat history, TUI, debugging |
| `AgentToolCall` | `tool_call_key`, `session_id`, `tool_name`, `tool_call_id`, `args`, `result`, `status`, trace enrichment fields | concrete tool invocation records within a session | runtime/tool persistence | chat progress, TUI, diagnostics |
| `AgentToolResult` | `agent_did`, `session_id`, `tool_name`, `tool_input`, `output_text`, `truncated`, `discarded_because_interrupted` | normalized tool result persistence | tool persistence hook | compaction and later inspection |
| `CompactionEntry` | `compaction_key`, `session_id`, `summary`, `messages_compacted`, token counts | persisted compaction summaries | compaction layer | session reconstruction and debugging |
| `RenderedRequest` | `capture_key`, `request_doc_id`, source/claim commit CIDs and verified signer DIDs, `request_id`, `session_id`, `capture_scope`, `turn_index`, `attempt`, `request_json`, `prompt_hash`, `tools_hash`, `provenance_json` | one durable fact per provider attempt: the exact HTTP request body and exact signed source-to-claim request chain, persisted before send | `rendered_request::transport::RenderedRequestCapturingHttpClient`, the innermost transport in every provider stack, through `rendered_request::sink::DefraRenderedRequestSink` (installed by default) | trace projections, capture reconstruction (still `CapturedOnly` until all transcript/config versions and later policy evidence are pinned) |

### Tasks, Schedules, and Event Triggers

| Collection | Key fields | References | Written by | Read by |
|------------|------------|------------|------------|---------|
| `Task` | `task_id`, `name`, `behavior_id`, `prompt_template`, `enabled`, `output_schema_ref` | `behavior_id -> AgentBehavior.behavior_id` | desired-state apply | trigger engine |
| `Schedule` | `schedule_id`, `task_id`, `interval_secs`, `enabled`, `concurrency`, `next_run_at`, `last_attempt_at`, `last_status`, `fire_count` | `task_id -> Task.task_id` | desired-state apply, trigger engine status updates | trigger engine |
| `EventTrigger` | `trigger_id`, `task_id`, `source_collection`, `event_kind`, `filter`, `enabled`, `concurrency`, `last_attempt_at`, `last_fired_source_doc_id`, `last_status`, `fire_count` | `task_id -> Task.task_id` | desired-state apply, trigger engine status updates | event source / trigger engine |

`Task.behavior_id` is concrete and mandatory. A `Schedule` references the `Task`
it fires; the trigger engine materializes `AgentRequest` rows from due
`Schedule`s.

An `EventTrigger` also references a `Task`, but fires from document events on a
declared `source_collection`. Desired-state validation probes the live DefraDB
schema for the source collection, validates the trigger filter, and resolves
`doc.*` template references in the target task before apply succeeds.

### Tool Service Discovery

| Collection | Key fields | Meaning | Written by | Read by |
|------------|------------|---------|------------|---------|
| `ToolServiceRegistry` | `service_id`, `hostname`, `tailscale_ip`, `lan_ip`, `mcp_port`, `mcp_path`, `status`, `version`, `updated_at` | registry entries for discoverable MCP-style tool services | desired-state apply, service registry writers | meta-tools and discovery flows |

`ToolServiceRegistry` desired-state owns the identity and endpoint fields:
`service_id`, `display_name`, `description`, `hostname`, `tailscale_ip`,
`lan_ip`, `mcp_port`, and `mcp_path`. Desired-state apply normalizes missing or
null address fields to empty strings, defaults missing or empty `mcp_path` to
`/mcp`, and creates rows with `status: "online"` so they are discoverable.
`version` and `updated_at` are runtime-owned and may be null on rows created by
apply. Tool lists are discovered from the MCP service at runtime; the schema does
not expose a persisted `tools` relation.

## Operational Relationships

### Configuration Resolution

The runtime resolves a runnable behavior by following this chain:

1. Load `AgentPrincipal` for `agent_did`
2. Choose the principal’s `default_behavior_id` or an explicit `behavior_id`
3. Load `AgentBehavior`
4. Load `InferenceBackend`
5. Load `ToolSelection`
6. Optionally load `InferenceProfile`
7. Intersect behavior-selected tools with the operator `ToolCeiling`
8. Publish `AgentRuntime`

If the backend is missing, disabled, or unhealthy, the behavior is unrunnable.

### Interactive Request Flow

The normal CLI path is:

1. `chat` or `request submit` writes `AgentRequest`
2. runtime claims and executes the request
3. an explicitly configured rendered-request capture sink may observe each provider-bound request in memory before streaming starts
4. streaming writes `AgentResponse`
5. transcript/session layers write `AgentSession`, `AgentConversation`, `AgentMessage`
6. once the final assistant message is committed, `AgentResponse.materialized_message_sequence`
   points at the committed `AgentMessage.sequence`
7. tool activity writes `AgentToolCall` and `AgentToolResult`

### Reconcile

Live reconcile is driven by changes to configuration documents:

- `AgentPrincipal`
- `AgentBehavior`
- `ToolSelection`
- `InferenceProfile`
- referenced `InferenceBackend`

The runtime republishes `AgentRuntime` as it resolves, applies, and activates a
new generation.

## Branchable vs Non-Branchable

Several operational collections are marked `@branchable`:

- `AgentConversation`
- `AgentMessage`
- `AgentRequest`
- `AgentResponse`
- `AgentRuntime`
- `AgentSession`
- `AgentToolCall`
- `AgentToolResult`
- `CompactionEntry`
- `RenderedRequest`
- `Task`
- `Schedule`

These are the documents where preserving observable history matters most.

`@branchable` is not what gives a field its content address — DefraDB writes
per-field and composite commit blocks unconditionally, and `is_branchable` gates
only the extra collection-level block. It is irreversible (DefraDB rejects every
patch that flips it, and a populated collection cannot be dropped and
recreated), and it is the precondition for branchable collection sync and for
collection-scoped ACP read decisions. Choose it when the collection is created
or never.

The core configuration collections are not branchable:

- `AgentPrincipal`
- `AgentBehavior`
- `ToolSelection`
- `InferenceBackend`
- `InferenceProfile`

Those are treated as current desired state rather than append-only history.

## Source of Truth Boundaries

Some boundaries are deliberate:

- `ToolSelection` is the behavior-selected tool surface.
- `ToolSelection.allowed_mcp_service_ids` optionally narrows meta-tools to a
  behavior-specific set of MCP service IDs. Missing or empty means all online
  `ToolServiceRegistry` services remain visible for backward compatibility.
- `ToolCeiling` is not stored here; it is an operator safety cap applied at
  runtime.
- Command execution policy lives on `ToolSelection`: `command_execution_policy`
  accepts `read_only`, `workspace_write`/`managed_write`, or `unrestricted`;
  `command_allowed_argv_prefixes` and `command_forbidden_argv_prefixes` refine
  argv-level allow/deny behavior; `command_network_mode` is an optional network
  policy hint. In `read_only` mode, an allowed argv prefix can authorize an
  operator-configured diagnostic command outside the built-in read-only
  allowlist. When the allowed-prefix list is non-empty, it remains a global argv
  gate for all commands. Runtime enforcement still depends on the selected bash
  mode and host platform. On macOS, `workspace_write` uses the seatbelt sandbox
  and only permits same-sandbox process introspection; `unrestricted` is
  unsandboxed and is the policy to use for host-diagnostics stewards that need
  `ps` or broad `lsof`.
- backend credentials may currently be stored either directly in
  `InferenceBackend.api_key` or indirectly via `InferenceBackend.api_key_env_var`.
- backend capability metadata is not stored in `InferenceBackend`; provider
  behavior is delegated to rig and deprecated manifest/import fields are
  ignored during config migration.
- `AgentRuntime` is the runtime’s published observability surface, not desired
  configuration.

## Where Schemas Are Registered

The runtime registers schemas from `crates/gents/src/schema.rs`, via
`gents_protocol::schemas`.

That file is the authoritative list of which SDL files are loaded into the
embedded DefraDB node.
