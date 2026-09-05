# CLAUDE.md

## What this is

Gents is a Rust agent runtime where the database is the control plane. It is built on DefraDB, and it leans on what DefraDB uniquely provides — DID-based cryptographic identity, document-level access control, and P2P replication — to give agents properties a normal stack can't: verifiable identity, least-privilege permission boundaries, and gossip-based event propagation across nodes.

Everything is a document. Configuration, requests, responses, sessions, tool calls, triggers, schedules — all DefraDB documents. You configure an agent by writing documents, trigger work by writing documents, and debug by reading documents. The runtime watches for request documents, drives them through a formally specified lifecycle, and writes response documents back.

**The `gents` runtime crate is the core of this repository.** The CLI, the desktop app, the protocol crate, the schemas — all of it exists to operate, observe, or share the vocabulary of that runtime. When you are deciding where logic belongs or what matters in a tradeoff, the runtime's integrity wins.

## How we build: the foundation flow

This project is built upward from a formal foundation, and every substantive change follows the same flow:

**Lean models → conformance tests → implementation.**

1. **Start in the Lean spec** (`crates/gents/proofs/`). Understand the current model. Make the change there first, and prove it doesn't break the safety/liveness properties you care about. Zero `sorry`s is the standard.
2. **Drive the conformance tests from the spec.** The spec change defines what the tests expect; the tests are the fence between the model and the code.
3. **Make the Rust satisfy the tests.** Then make it clean — the code on top of this foundation is meant to read as well as it verifies.

Not every change touches the spec — plumbing, tooling, and infrastructure don't. But anything that changes *what transitions are legal*, *what invariants hold*, or *what the model feeds the provider* starts in Lean. When a proof obligation is hard to discharge, treat that as information: sketching the soundness argument has caught real bugs here before the code existed.

The proven core (see `crates/gents/proofs/README.md` for the full map): request/process/persistence lifecycles, tool-call and subagent lifecycles, scheduler and fleet slot accounting, recovery convergence, trigger dispatch, transcript reduction and compaction, and provider-input assembly (sanitization soundness, idempotence, split-stability).

## The system, held in your head

The shape that should ground any work here:

- **Request flow:** a watcher claims request documents → the request lifecycle (Pending → Claimed → Processing → terminal states, all Lean-fenced) → the **owned completion loop** (`agent/loop_stream.rs`) drives model turns and tool execution → the hook persists every observable step as documents → a stream processor materializes the live response. Request state is one column, `lifecycle_state`, owned by `gents_protocol::request_lifecycle::RequestLifecycleState`; nothing else names its strings.
- **Provider-input correctness is a property, not a discipline.** Every completion request in the system is born in the owned loop, which sanitizes loaded history at entry (the PromptAssembly model proves this sound). The durable transcript is deliberately permissive; the provider boundary narrows it.
- **Identity is layered:** a *principal* (DID) is the permission and audit boundary; *behaviors* (prompt/tools/model) are reusable interfaces on a principal; *deployments* place principals on hosts. Each `(did, behavior)` runs on exactly one deployment.
- **Tools are documents too:** tool selections, MCP services, subagent targets, and skills are all configured as documents and resolved into a per-behavior tool surface at reconcile time. Subagents are first-class: children are requests, bridged back to the parent's tool call.
- **Automation is document-driven:** Tasks, Schedules, and EventTriggers fire requests through the trigger engine, with lineage stamped on every request.
- **Everything persisted can be projected back out:** the run timeline (`run_timeline.rs`) reconstructs a request's event stream from persisted rows, and adapter projections (`adapter_projection.rs`, CLI `trace timeline|project`) export it in external-framework shapes (OpenAI-Codex, LangGraph, multi-agent) with redaction modes enforced by ACP.
- **Client sync has one observation owner and no synthetic lifecycle.** DefraDB's strict P2P status is adapted once in `gents::p2p_observability`; `ClientSyncStateOwner` combines that observation with transport and configured-peer facts; `project_sync_health` derives the four product states; the bridge transports that projection; and `projectSyncOperationalStatus` supplies UI copy/actions. `AgentBehaviorReadiness` owns runtime process/admission readiness only—it is never a database-sync source. Do not recreate `stalled`, elapsed-clock heuristics, pairing-derived global sync, optional wire defaults, or another UI-local projector. See `docs/superpowers/specs/2026-08-17-mobile-session-sync-design.md` for the compact ownership contract.
- **External code is held at arm's length:** rig-core is the provider/streaming client only ("Layer A"); native messages cross through `llm::rig_compat`, while `provider_input` is the narrow wire-projection seam used to account the exact provider DTO. The message family is native and byte-compatible with what's persisted (`gents-protocol::message`). Full rig removal is staged (#438/#439). DefraDB itself comes from the public `sourcenetwork/defradb.rs` repository, pinned in the workspace `Cargo.toml` — look there when node, schema, or identity behavior is in question. Claude subscriptions are reached over Anthropic Messages HTTP with an agent-scoped `OAuthCredential` written by `gents claude-login` and refreshed by gents; the `claude` binary is not a dependency.

## Sharp edges

The few things you can't discover until they bite:

- **Always `graphql::escape_graphql_string()`** for anything interpolated into a GraphQL string. Queries are built inline by convention.
- **Never emit `[]` in a DefraDB mutation** — an empty list literal types as `JsonArray` and corrupts nillable array columns. Emit `null`.
- **Gate with the full package suite** (`cargo test -p gents`), not `--lib` — integration tests are separate compile units and `--lib` will happily pass while they don't build.
- **Compile the whole workspace before pushing** (`cargo check --workspace --all-targets`) — the test gate above still skips examples, the desktop crates, and many test targets, so a new required struct field breaks their construction sites silently until CI (or the next unrelated PR). CI enforces this in `rust-and-cli`; run it locally first.
- **Flaky tests are defects.** The formal-verification investment exists to eliminate that class; capture, file, and fix — never shrug.
- **Create worktrees with `make worktree BRANCH=<branch>`**, not bare `git worktree add`. It clones `target/` and `proofs/.lake` from the current checkout via APFS clonefile; a cold worktree costs ~40 min of recompilation (sccache can't cache build scripts, proc-macro expansion, or linking) and hours of Mathlib. Note `cargo check` artifacts don't warm `cargo test` — warm with `cargo build --tests -p <crate>` if tests are the goal.
- `tracing`, never `println`.
