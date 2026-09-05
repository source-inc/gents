# Claude subscription backend — design as shipped

**Date:** 2026-09-05
**Status:** shipped (`ClaudeCliSubscription`). Operator guide: `docs/backends.md` § Claude subscription. How the design got here, including the decisions that were reversed: [`claude-subscription-history.md`](./claude-subscription-history.md).

## Goal

A Claude Max / Claude.ai subscription seat is a first-class inference backend on the same owned completion loop as OpenAI, ChatGPT Codex and Grok. Gents owns the transcript, tool execution, audit and recovery; Claude is a completer that may *request* gents tools. Nothing Claude Code ships (its CLI, its built-in tools, its agent loop) is part of the runtime.

```text
owned loop (run_loop_stream, unchanged chokepoint)
  ├─ OpenAiCompatible / OpenRouter → OpenAI HTTP client
  ├─ ChatGptCodex                  → Codex client   (agent-scoped OAuthCredential)
  ├─ XaiGrokOAuth                  → Grok client    (agent-scoped OAuthCredential)
  └─ ClaudeCliSubscription         → Messages HTTP  (agent-scoped OAuthCredential)
```

## The wire

One wire for every turn, tool-capable or text-only: `POST https://api.anthropic.com/v1/messages`, streamed (SSE), parsed incrementally by `claude_messages`. The URI is pinned; the backend document keeps the placeholder endpoint `claude-cli://subscription`.

- `system[0]` is the Claude Code identity block (`CLAUDE_CODE_IDENTITY`); the assembled gents preamble and `Message::System` rows follow as `system[1..]`. There is no `system:` user block.
- Two `cache_control: ephemeral` breakpoints: the last `system` block and the last content block.
- `tools` carries the behavior's resolved gents surface and is omitted when that surface is empty, never sent as `[]`. Title calls carry no tools.
- No sampling keys (`temperature`, `top_p`, effort, thinking): the live model rejects them on this scope. `max_tokens` is 32768 for inference and 24 for title calls.
- Model ids come from `InferenceBackend.models[]` (preset default `claude-sonnet-5`; see Discovery).
- Tool blocks parse fail-closed: an unknown name, a duplicate `toolu_*` id, a malformed block or an overlapping block is a classified error, never a silent drop and never an execution by Claude Code. A mapped `tool_use` becomes an ordinary `AgentToolCall` through `hook.on_tool_call`; gents runs it (`dispatch_tool`, policy, approvals, subagent bridge) and threads the result back as a native `tool_result`.
- Persist-before-send holds: every request is captured as `RenderedRequestSource::ClaudeCliSubscription` (`capture_seam = transport_body`) before the bytes leave.

## Auth

The credential is an agent-scoped `OAuthCredential` document: `provider = "claude-subscription"`, `credential_id = "claude-subscription:<agent_did>"`, one row per agent DID, replicated under the same rule as the Codex and Grok credentials.

- `gents claude-login` (crate `gents-claude-login`) runs the PKCE flow itself against `claude.com/cai/oauth/authorize` and `platform.claude.com/v1/oauth/token`, with Claude Code's public client id and scope set. Default is a loopback listener; `--manual` accepts a pasted `code#state`; `--no-browser` prints the URL. It writes the document and prints the result with both tokens redacted. The `claude` binary is never invoked.
- The bearer is the shared single-flight `DbCredentialBearer` with `OAuthRefreshKind::Claude`: cache-first, refreshes owner-only within five minutes of expiry against the token endpoint, persists the rotated row, and a `401` invalidates it once so the next request refreshes. After a failed refresh the bearer stays forced and serves the classified error for 60 s without another token-endpoint call (`REFRESH_FAILURE_COOLDOWN`, shared by all three OAuth kinds); it never re-serves the rejected token.
- Tokens never reach logs, captures, `Debug` output or `gents query` (`access_token` is a restricted field). Raw GraphQL on the loopback endpoint can read them; that surface is unauthenticated by design and documented as such.

## Health, readiness, diagnose

- The prober treats the three agent-scoped OAuth kinds alike (`probe_oauth_credential`): it reads the credential document and never refreshes, never spawns, never calls the provider. A fresh token promotes `unknown` to `healthy`; an enabled credential whose access token is stale but has a refresh token still passes, because the next request refreshes it; a missing or disabled credential is a `ProbeFail` and demotes after K failures with the `gents claude-login --agent-did <did>` hint.
- A behavior whose backend is `ClaudeCliSubscription` with no enabled credential is not runnable (`CredentialsRequired`) and names the login command.
- `gents diagnose` reports `checks.claude_auth` (credential id, expiry, ok) and degrades `status` only when no credential can be read. It reads the document, not the live bearer, so it can lag a turn that just refreshed.

## Discovery

`gents config backend discover-models` fetches `GET /v1/models?limit=100` with the credential bearer, `anthropic-version: 2023-06-01` and `anthropic-beta: oauth-2025-04-20`. It is explicit only: no login runs it and automatic discovery skips agent-scoped kinds. `--write` replaces the backend's `models[]` (models-only mutation, all three OAuth kinds, skipped when zero ids come back). `gents init` reseeds `models[]` to its single `--model-name`, so rerun `--write` after an init. Codex's `/model` picker reads `models[]`.

## Lean fence

Prompt assembly and stream accumulation for this wire are modelled in `crates/gents/proofs/Proofs/PromptAssembly/ClaudeMap.lean` (`systemBlocks`, `splitSystem`, `toolsField`, stream accumulation), with contract cases, JSON encoders and snapshot keys; Rust reads the generated witnesses in the conformance suite and pins `CLAUDE_CODE_IDENTITY` to the witness head. Anything that changes provider input or tool-call legality starts in Lean, then conformance, then Rust. Zero `sorry`.

## Invariants

- Gents executes tools; Claude only requests them. Claude Code's built-ins (`Bash`, `Read`, `Task`, MCP) are never enabled and never aliased onto gents names.
- A Claude-owned agent loop (MCP servers or `Task` driving gents) is rejected.
- One wire; no process seat; no Claude binary at runtime.
- Live Claude traffic during development is a numbered, human-approved write request; tokens are never printed.

## Known limitations

Listed once, in `docs/backends.md` § Claude subscription → Known limitations.

## Open

- Spawn / subagent tools requested by Claude must take the bridge path, never `complete_native`; not in the first product.
- `FailureClass` for an unmapped provider tool: reuse `policyDenied` / `external`, or a new class (Lean first).
- Partial `tool_use` deltas are not mapped; the loop waits for the complete block. Parallel `tool_use` blocks in one message map 1:1 like OpenAI.
- Reasoning effort / thinking on the wire, and advertising efforts to the Codex shim.
- A revoked refresh token is only seen on the request path (bounded by the cooldown); nothing feeds request-path auth failures back into health.

## Lean / Rust map

| Concern | Lean | Rust |
|---|---|---|
| Claude wire shape, stream accumulation | `Proofs/PromptAssembly/ClaudeMap.lean` | `claude_messages.rs`, `claude_subscription.rs` |
| Sanitize / pairing | `Proofs/PromptAssembly/{Provider,Executable,Properties}.lean` | `compaction::sanitize_history_for_provider`, `loop_stream` entry |
| Tool args object normal form | `Proofs/PromptAssembly/ToolArgs.lean` | `tool_use.input` → `ToolFunction.arguments` |
| Budgets | `Proofs/PromptAssembly/{Budget,AggregateBudget}.lean` | `loop_stream` clamp + charge |
| ToolCall machine | `Proofs/ToolExecution/*`, `Conformance/Contracts/Machines/ToolCall.lean` | `hook.on_tool_call`, `dispatch_tool`, `tool_call_lifecycle` |
| Unique ids | `CrossMachineComposed/UniqueCallIds.lean` | `toolu_*` → `ToolCall.id` / `internal_call_id` |
| Capture order | `Proofs/RenderedCapture.lean` | `rendered_request/transport.rs`, `RenderedRequestSource::ClaudeCliSubscription` |
| Recovery | `Proofs/Recovery/Sweeps/ToolCalls.lean` | existing sweeps |
| Credential, refresh, cooldown | — | `oauth_credential.rs`, `claude_oauth.rs`, `claude_oauth_refresh.rs`, `crates/gents-claude-login` |
| Health probe, readiness gate, diagnose | — | `backend_health.rs`, `agent/document_view/snapshot.rs`, `gents-cli commands/diagnose` |
| Discovery | — | `backend_provider.rs`, `gents-cli commands/config/backend.rs` |
