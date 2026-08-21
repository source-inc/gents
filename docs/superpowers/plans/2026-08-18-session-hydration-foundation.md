# Session Hydration Foundation (#1142) Implementation Plan

**Goal:** Define the tenant-safe, idempotent `SessionHydrationRequest` lifecycle and register its client-authored control collection without inventing a second P2P delivery path.

**Architecture:** Follow the foundation flow: model admission, document selection, terminality, crash re-drive, and pairing non-interference in Lean; fence the executable decision in Rust; then register the branchable schema in the shared catalog, client-authored migration fence, and `machine` template. This PR deliberately stops before the server sweep: neither the pinned DefraDB revision nor current `main` exposes its peer-targeted historical replay/doc-pusher through `P2POperations`. A follow-up must expose that existing primitive with its bounded replay and persisted retry behavior.

**Spec:** `docs/superpowers/specs/2026-08-17-mobile-session-sync-design.md`

## Review clarifications

- `request_key` remains `"{peer_id}:{session_id}"`; the lifecycle consumes a parsed peer id instead of treating the opaque key as authorization.
- Admission requires a paired peer, active requester membership, and an exact `(session_id, requester_did, agent_did)` ownership tuple.
- Selection independently filters every candidate document by the seven transcript collection names, requester, session, and agent. Admission alone never turns an unscoped candidate set into a grant.
- Re-drive is set-idempotent: a crash after document delivery but before the terminal write can repeat the same selection without widening it.
- Hydration state does not modify pairing state or template filters.
- Do not implement hydration by global document subscriptions, no-op document rewrites, a manifest collection, or replicator teardown/reinstall.

## Tasks

1. Add `Proofs/SessionHydration/{State,Executable,Properties}.lean` and a barrel import. Prove no push before admission, tenancy/session soundness, idempotent re-drive, terminality, and pairing non-interference with zero `sorry`s.
2. Add a small pure Rust mirror in `p2p_reconcile/session_hydration.rs` and conformance tests for the admission matrix and exact selected document set.
3. Add the branchable `SessionHydrationRequest` SDL to `gents-schemas` and `gents-protocol`; classify it as client-authored so fresh-apply parity remains enforced.
4. Add the collection and requester-scoped rule to the Lean and Rust `machine` catalogs, including exact catalog assertions.
5. Run `lake build`, focused schema/template/conformance tests, `cargo test -p gents`, and `cargo check --workspace --all-targets`.

## Follow-up prerequisite

Expose a peer-targeted document replay operation from DefraDB's existing `DocPusher`/`TransportDocPusher` through the node API. The operation must accept explicit document references, enforce the peer's persisted replication filters/ACP, use the existing replay admission bounds, and persist incomplete sends into the existing retry ledger. Only then should the server sweep enumerate transcript rows and mark a request `served` after that operation accepts the exact document set.
