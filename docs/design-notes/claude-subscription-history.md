# Claude subscription backend — how the design got here

**Date:** 2026-09-05. Companion to [`claude-subscription.md`](./claude-subscription.md) (the design as shipped).

Between 2026-08-30 and 2026-09-05 the Claude backend went through five designs, each reversing part of the last. The original notes (the spike, phases 0–6, A2a, A2b, A2c and the Track B PR-stack note) were removed in the commit that added this file; `git log --all -- docs/design-notes/SPEC-claude-a2c-tool-bridging.md` finds them. This page keeps what still matters: the timeline, the decisions, and which of them survived.

## Timeline

| When | Step | Outcome |
|---|---|---|
| 2026-08-30 | Spike, phases 0–5 | `claude -p` driven as a text-only completer (`--tools ""`, fail closed on `tool_use`) behind a loopback OpenAI Chat Completions proxy, from an isolated config dir and gents home. Phase 5 verdict **Go**: the traffic billed the subscription plan meter, not the Console API. |
| 2026-08-30 | Phase 6, "Path A" packaging | Rust proxy (`gents claude-proxy`) and `gents claude-login` wrapping `claude auth login`; stock `OpenAiCompatible` backend pointed at the proxy with model slug `claude-plan`. Hard rule: no Anthropic token in DefraDB. |
| 2026-08-31 → 09-01 | A2a | `gents server` supervises the proxy so one prod home lists Grok and Claude in the Codex `/model` picker. |
| 2026-09-01 | A2b | New `BackendProviderKind::ClaudeCliSubscription`, placeholder endpoint `claude-cli://subscription`, in-process CLI completer authenticated by a process seat (`gents server --claude-config-dir`, refuse-closed `--claude-write-approved`). Proxy deleted. |
| 2026-09-02 | A2c / Track B, B1 evidence | Tool bridging inside the owned loop. Four wire options: C1 (CLI stream-json as a function-calling wire) killed by evidence, since `--tools` only names built-ins the CLI executes; C3 (prompt-stuffed JSON) not taken; C4 (Claude-owned MCP / `Task` loop) rejected; **C2 (Anthropic Messages HTTP) locked**, authenticated by reading the seat's `.credentials.json` / Keychain. |
| 2026-09-03 | Single wire | Every turn, tool-capable or not, over Messages HTTP; the process CLI completer deleted; Claude Code identity block as `system[0]`; Lean `ClaudeMap` fence with conformance witnesses. |
| 2026-09-04 | OAuth credential parity | The seat is gone. Auth is an agent-scoped `OAuthCredential` written by a first-party PKCE `gents claude-login`; the shared bearer refreshes it; the credential-expiry health probe, readiness gate and `checks.claude_auth` follow the Codex/Grok pattern. The `claude` binary is no longer a dependency. Same day: a stale-but-refreshable credential stays routable; 60 s refresh-failure cooldown. |
| 2026-09-04 → 05 | Discovery; re-port | `/v1/models` discovery with the credential bearer and `discover-models --write`; the whole track re-ported onto `main` as logical commits with no seat era. |

## Decisions

| Decision | First locked | Status |
|---|---|---|
| Gents owns the loop, the documents and tool execution; Claude only completes and may request tools | spike; A2c | **kept** |
| Text-only completer, fail closed on any `tool_use` | spike | superseded 2026-09-03 by tool bridging; fail-closed **kept** for unmapped / Claude-native names and empty surfaces |
| No Anthropic token in DefraDB; no `OAuthCredential` for Claude; `is_agent_scoped_oauth()` false | spike, phase 6, A2b, A2c C2 auth lock | **reversed 2026-09-04**: agent-scoped `OAuthCredential` like Codex/Grok; `is_agent_scoped_oauth()` true |
| Process seat in an explicit config dir; `claude` binary as the login dependency | phase 6, A2b | **reversed 2026-09-04**: no seat, no binary; first-party PKCE login |
| Loopback OpenAI proxy (standalone, then server-managed) | spike, phase 6, A2a | **reversed 2026-09-01** (A2b) and deleted |
| Server write-gate flag `--claude-write-approved` | A2b | **removed 2026-09-04**; the numbered-approval rule for live traffic during development stays a process rule, not code |
| `ClaudeCliSubscription` as the kind name; `claude-cli://subscription` placeholder endpoint | A2b | **kept** (renaming explicitly out of scope) |
| Client-facing model slug `claude-plan`; later a hard-coded four-id catalog | spike, A2a | **reversed**: real ids via `/v1/models` discovery; preset seeds `claude-sonnet-5` |
| C1 CLI wire / C3 prompt JSON / C4 Claude-owned loop | A2c | C1 dead by evidence; C3 not taken; C4 rejected; **C2 kept** |
| `.credentials.json` and macOS Keychain readers | A2c, single wire | **deleted 2026-09-04** |
| Two wires (process CLI for empty surfaces, HTTP for tools) | A2c B3 | **superseded 2026-09-03** by the single wire |
| No aliases from Claude-native tool names to gents tools | A2c | **kept** |
| Spawn / subagent via the bridge path (B4) | A2c | **still later** |
| Lean → conformance → Rust for provider-input and tool-legality changes | A2b, A2c | **kept** |

## Evidence

Each step was verified live on a Claude Max seat under numbered write approvals; that evidence lives in the developer's gitignored `.scratch/claude-spike/logs/` and is summarised in the PR descriptions. The shipped behaviour is covered in-tree by the `claude_` unit tests, the Lean-witnessed conformance suite, the `gents-claude-login` tests and the CLI suites.
