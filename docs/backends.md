# Backends

This page is the committed backend support matrix for Gents. It tracks
which providers are supported, which wire API each provider uses, what request
and response shaping Gents applies, and whether a provider has an offline
wire fixture replay fence.

The runtime owns provider-input assembly before any provider-specific client is
called. Provider-specific shaping should stay small, explicit, and tested at the
HTTP seam because live provider bugs tend to appear in headers, unsupported
parameters, response content types, and tool schema details rather than in the
agent loop itself.

## Support Matrix

| Provider kind | Wire API | Auth | Streaming | Tools | Reasoning | Request shaping | Response shaping | Fixture status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `OpenAiCompatible` | OpenAI Responses by default; Chat Completions fallback for compatible local servers | API key or local/no-auth | SSE | Function tools through rig | Responses inherit the model default; local Chat Completions sends `chat_template_kwargs.enable_thinking` | Adds cache-scope `user` when available; does not assume every Responses model accepts `reasoning.effort` | Standard rig OpenAI handling | Planned by #545 |
| `ChatGptCodex` | ChatGPT Codex Responses endpoint | `OAuthCredential` document, refreshed by owner runtime | SSE | Function tools, forced `strict: false` to match Codex CLI | `reasoning.effort` currently fixed at `medium` | Strips unsupported `max_output_tokens`, `temperature`, `top_p`; injects instructions/store/stream defaults; adds Codex `version` and `Accept` headers | Adds missing `Content-Type: text/event-stream` only when the backend omits it; synthesizes completion body from SSE for non-streaming probes | Unit-pinned in #530; replay corpus planned by #545 |
| `XaiGrokOAuth` | Grok CLI subscription proxy (`cli-chat-proxy.grok.com`); Responses by default, `openai_wire: chat_completions` honored (the proxy serves both; the official client picks per model) | `OAuthCredential` document (`provider=xai-oauth`), refreshed by owner runtime | SSE | Function tools through rig | Not forced (several Grok models reject `reasoning.effort`) | Sets `store: false` when absent (Responses); injects Grok-CLI identity headers (`x-xai-token-auth`, `x-authenticateresponse`, `x-grok-client-*`, User-Agent) + bearer on every wire | Adds missing SSE `Content-Type` when omitted | Unit tests for headers/bearer/wire; live replay planned by #545 |
| `OpenRouter` | Chat Completions | API key | SSE | Function tools through rig | Provider-dependent | Adds OpenRouter provider preference `require_parameters: true` | Standard rig OpenRouter handling | Planned by #545 |
| local OpenAI-compatible servers | Responses or Chat Completions depending on server support | Usually none/local key | SSE varies by server | Function tools when server supports them | Reasoning parser support varies; Chat Completions sends `enable_thinking` for vLLM-style servers | Same as `OpenAiCompatible`; operators may need Chat Completions fallback for servers without `/v1/responses` | Standard rig OpenAI handling | Planned by #545 |
| `ClaudeCliSubscription` | Anthropic Messages HTTP (`POST /v1/messages`), the single Claude wire; `/v1/models` discovery with the credential bearer (endpoint placeholder `claude-cli://subscription`) | `OAuthCredential` document (`provider=claude-subscription`), written by `gents claude-login`, refreshed by owner runtime | Messages SSE, parsed incrementally into the owned loop | Tool-capable: gents tools map onto `tool_use`; unmapped names fail closed; `tools` omitted when the surface is empty | No sampling keys sent | `system[0]` = Claude Code identity block, then preamble and System rows; two `cache_control` breakpoints; Claude model IDs on `InferenceBackend.models[]`; a behavior with no enabled credential is not runnable; health = credential-expiry read (not fleet HTTP) | `text_delta` / `tool_use` blocks → owned completion/stream path | Unit + SSE fixtures, Lean-generated body/stream witnesses; live only with a logged-in credential |

## Probe lifecycle and health (#640)

Backend availability composes two signals, and they deliberately live in
different places:

- **Operator/bootstrap intent** — the fleet-replicated `InferenceBackend`
  document's `enabled` and `probe_status`. The startup ratchet promotes
  `unknown → healthy` for reachable backends and stamps `last_probe`; the
  scheduled prober keeps that promotion recurring with fresh `last_probe`, and
  `gents config backend set --probe-status ...` remains the manual
  override. Nothing ever writes `unhealthy` here: reachability is
  observer-relative, and 16 runtimes stomping one document would replicate
  churn and conflicting opinions.
- **Measured health** — each runtime's scheduled prober (default: every 60s,
  10s timeout) probes the models endpoint of every enabled, probeable backend
  and keeps an in-memory `BackendHealthMap`. Hysteresis is K=3 consecutive
  failures to demote to `unhealthy`, one success to promote back (formal
  model: `crates/gents/proofs/Proofs/BackendHealth/`).
  Agent-scoped OAuth backends (`ChatGptCodex`, `XaiGrokOAuth`,
  `ClaudeCliSubscription`) skip the HTTP round-trip; instead each cycle reads
  the enabled `OAuthCredential` document for the runtime principal's DID and
  that backend's provider (`probe_oauth_credential`; at most one exists, since
  `credential_id` is derived from that pair). A fresh access token (more than
  the 5-minute refresh skew from `access_token_expires_at`) passes. A stale
  access token also passes as long as the document carries a refresh token:
  it is debug-logged as stale and the next request refreshes it. A missing
  document fails with that provider's login hint (`gents codex-login` /
  `grok-login` / `claude-login --agent-did <did>`); a read error or a document
  with no refresh token also fails. The probe never refreshes and never spawns
  anything. K=3 demotes in
  `BackendHealthMap` only — the replicated document is not stamped
  `unhealthy`; a document born `unknown` is promoted to `healthy` on the first
  passing cycle. The measured detail (the login hint, or the read error)
  lives in the runtime's health snapshot; the replicated document only gets
  `probe_status` / `last_probe`.

Effective availability is `intent AND NOT measured-unhealthy`: a measured
demotion removes the backend from admission and marks dependent behaviors
unavailable within `probe_interval × K + reconcile debounce`, and one
successful probe restores routing. Measured state resets on restart (a dead
backend is doc-available again for up to K probe intervals until re-demoted).

The `gents_backend_probe_status{backend_id,status}` metric reports the
MEASURED state with value 1 iff healthy — it genuinely reads 0 during an
outage — and `gents_backend_last_probe_seconds` reports probe freshness.
Both fall back to document values for backends the prober has no opinion on.

## Wire Fixture Policy

Provider fixture replay is tracked in #545. Recorded fixtures live under
`crates/gents/tests/fixtures/providers/` and must be safe to commit.

Rules:

- No access tokens, refresh tokens, API keys, account ids, or bearer strings in
  fixtures.
- Redaction happens before writing fixtures to disk.
- Fixture replay should assert every recorded request is consumed exactly once.
- Fixture refresh is a live/operator action; CI should replay committed fixtures
  offline.

The fixture directory has a regression test that scans committed fixture files
for common credential patterns. The scanner is intentionally conservative: if a
new provider introduces another credential shape, add it to the scanner before
committing fixtures.

## ChatGPT subscription (ChatGptCodex, OAuth)

Use a ChatGPT/Codex subscription instead of an API key. The credential is stored
as an `OAuthCredential` DefraDB document scoped by `agent_did` and provider
(`chatgpt-codex`), not in `~/.codex`.

### Setup

1. Configure a backend with `provider_kind = ChatGptCodex`.
2. Sign in and write the credential document:

   ```sh
   gents codex-login --agent-did did:key:...
   ```

   Add `--device-auth` for headless login, and `--graphql` to write to a running
   node instead of the local home.
3. Verify:

   ```sh
   gents codex-auth-probe --agent-did did:key:...
   ```

### Models

- **Default:** the `chatgpt-codex` preset defaults the model to **`gpt-5.5`**, so
  `gents init --backend-preset chatgpt-codex` works without `--model-name`.
- **Use plain `gpt-5.x` slugs, not `-codex` variants.** A ChatGPT subscription serves
  models like `gpt-5.5`; the `-codex` variants (`gpt-5.2-codex`, …) return
  *"not supported when using Codex with a ChatGPT account"*.
- **List your account's models:**

  ```sh
  gents config backend discover-models \
    --graphql <url> --backend-id <id> --agent-did did:key:...
  ```

  Add `--write` to replace the backend's `models[]` with the returned set; nothing
  is written without it, and an empty result is never written (`models[]` is left
  unchanged). The returned set is what the account can actually use — it is gated server-side by
  plan and by the advertised Codex client version (see below). An empty list usually
  means a stale client version.
- **Change the model:** pass `--model-name <slug>` to `init`, or update the behavior
  with `gents config behavior set --backend-id <id> --model-name <slug>`.
- **Client version gate.** The backend gates model availability on the Codex client
  version Gents advertises (currently `0.138.0`, on both the request `version`
  header and the `/models` `client_version` query param). If a newer floor is required,
  set `GENTS_CHATGPT_CODEX_CLIENT_VERSION` — one knob moves it everywhere.
- **Reasoning effort** is currently fixed at `medium`; per-behavior effort selection
  (e.g. `xhigh`) is tracked in #540.

### Wire-shaping guarantees

The ChatGPT Codex path is stricter than hosted OpenAI Responses in several
places. Regression tests pin these details:

- unsupported top-level params are stripped: `max_output_tokens`, `temperature`,
  `top_p`
- function tools are sent as `strict: false`
- the Codex client version is sourced from one accessor and used for both the
  request `version` header and `/models?client_version=...`
- `Accept: text/event-stream, application/json` is sent
- a missing SSE `Content-Type` is filled as `text/event-stream`, while a
  backend-supplied content type is preserved

### Credential storage

- `gents codex-login` uses Codex's OAuth flow with an ephemeral in-memory
  store, then writes the resulting access token, refresh token, id token, account
  id, plan, FedRAMP flag, and expiry into `OAuthCredential`.
- The runtime reads `OAuthCredential` for the behavior's `agent_did`; it does not
  read `CODEX_HOME` or `~/.codex` for ChatGPT backend auth.
- v1 stores token fields as plaintext document fields, matching the current
  `InferenceBackend.api_key` precedent. Filtered replication must scope the
  credential to the owning `agent_did`; encrypted token fields are the next slice.

### Fleet / remote

- OAuth refresh rotates the refresh token, so only the owner node for the
  `(agent_did, behavior)` should refresh and write the document. Replicas can use
  the current access token and receive the rotated document through replication.
- Owner election across nodes is not yet wired: every runtime currently builds
  the bearer as the owner, so the single-writer guarantee relies on the routing
  model placing each `(agent_did, behavior)` on exactly one deployment. Do not
  replicate an `OAuthCredential` to a second node that also runs the same
  `(agent_did, behavior)` until owner derivation lands (a later slice).
- When replicating credentials, use an agent-scoped filter such as
  `OAuthCredential:agent_did=did:key:...`; do not include `OAuthCredential` in an
  unfiltered config replicator.
- The single-node/local demo path treats the local runtime as the owner.
- A remote frontend (`gents codex`) does not need local ChatGPT credentials;
  the server-side runtime uses the replicated `OAuthCredential` document.

### Token refresh

- The ChatGPT HTTP client asks a `DbCredentialBearer` for a bearer before every
  request. All clients for one `credential_id` share a single bearer (cache and
  refresh lock) per process, so the rotating refresh token has exactly one
  in-process writer.
- If the access token is near expiry and this runtime is the owner, it posts the
  refresh token to OpenAI's token endpoint, writes the rotated tokens back to
  `OAuthCredential`, then sends the request. A refresh that fails is not
  retried for 60 seconds: the bearer returns the same failure without another
  POST, though a re-login is picked up when the cooldown lapses (≤60 s).
- If the provider rejects a live request with 401/403, the bearer is invalidated
  so the next request forces a refresh rather than replaying a clock-fresh but
  server-revoked token. Runtime errors still tell the operator to rerun
  `gents codex-login` when a refresh cannot recover.

### Diagnostics

- `gents codex-auth-probe` reads the credential document (read-only; it
  never refreshes — the owning runtime is the single refresh writer, so a second
  writer would trip the provider's reuse-detection), probes `/models`, and prints
  account, plan, expiry, and reachable models.
- `gents diagnose` reports `checks.chatgpt_auth` as structured JSON with
  `credential_id` and `expires_at`, or an actionable `gents codex-login`
  guidance string when the document is missing or expired.

## Grok subscription (XaiGrokOAuth, OAuth)

Use a SuperGrok or eligible X Premium+ subscription instead of minting a
`console.x.ai` API key. The credential is an `OAuthCredential` document scoped
by `agent_did` and provider (`xai-oauth`), parallel to ChatGPT Codex.

Spike facts (auth endpoints, public client id, proxy headers) live in
[`docs/design-notes/xai-grok-oauth-spike.md`](design-notes/xai-grok-oauth-spike.md).

### Setup

1. Configure a backend with `provider_kind = XaiGrokOAuth` (preset `xai-oauth`
   / `grok-oauth`). Default endpoint:
   `https://cli-chat-proxy.grok.com/v1`. Default model: `grok-4.5`.
2. Sign in (device-code; works over SSH without a loopback callback):

   ```sh
   gents grok-login --agent-did did:key:...
   ```

   Aliases: `gents xai-login`. Use `--graphql` to write to a running node.
3. Verify (read-only; does not refresh):

   ```sh
   gents grok-auth-probe --agent-did did:key:...
   ```

Model discovery and the probe query the proxy's `/models-v2` catalog (the
path the official Grok CLI uses); entries are identified by `model` /
`modelId`. `gents config backend discover-models --write` replaces the backend's
`models[]` with that catalog; nothing is written without `--write`. The wire API
defaults to Responses; if a model turns out to be
Chat-Completions-only, set `openai_wire: chat_completions` on the backend
document — unlike `ChatGptCodex`, the setting is honored for this provider.

### Endpoint choice

| Path | Base URL | Auth |
| --- | --- | --- |
| **Subscription OAuth (this provider)** | `https://cli-chat-proxy.grok.com/v1` | SuperGrok / X Premium+ OAuth bearer + Grok-CLI identity headers |
| **API key (existing OpenAI-compatible)** | `https://api.x.ai/v1` | `XAI_API_KEY` on a generic `OpenAiCompatible` backend |

Do **not** send a subscription OAuth bearer to `api.x.ai` expecting free quota —
that surface commonly returns **402** spending-limit / **403** tier errors for
subscription tokens. Use the CLI chat proxy for OAuth.

### Failure modes

| Symptom | Meaning | Fix |
| --- | --- | --- |
| Login succeeds, inference **401** | Expired / revoked grant | `gents grok-login` again |
| Login succeeds, inference or refresh **403** tier / permission | Account not entitled to OAuth API | Not fixed by re-login; use `XAI_API_KEY` + `api.x.ai`, or check SuperGrok tier |
| Inference **402** on `api.x.ai` with OAuth token | Wrong base URL for subscription | Point the backend at `cli-chat-proxy.grok.com` |
| Proxy **402/426** without client headers | Client not recognized as CLI | Gents injects Grok-CLI identity headers automatically |

### Credential storage & fleet

Same document model and owner-only refresh rules as ChatGPT Codex
(`OAuthCredential`, agent-scoped filter, rotating refresh token), including the
60 s refresh-failure cooldown: a failed refresh is served without another POST
until it lapses, and a re-login is picked up when it does. ChatGPT-only
fields (`chatgpt_plan_type`, `is_fedramp`) stay null/false for Grok.

### Diagnostics

- `gents grok-auth-probe` / `xai-auth-probe` is read-only.
- `gents diagnose` reports `checks.xai_auth` parallel to `checks.chatgpt_auth`.

## Claude subscription (`ClaudeCliSubscription`, OAuth)

Use a Claude Max / Claude.ai subscription seat over a **single Anthropic
Messages HTTP wire** (`POST /v1/messages`). The credential is an
`OAuthCredential` document scoped by `agent_did` and provider
(`claude-subscription`), the same document model ChatGPT Codex and Grok use.
The `claude` binary is not involved at any point and is not a dependency.

This is **not** Anthropic Console API-key billing: every Claude-backed turn
spends the subscription seat. The backend document keeps the placeholder
endpoint `claude-cli://subscription`; `gents config backend discover-models`
lists the seat's models (`GET /v1/models` with the credential bearer). Turns are
tool-capable: gents tools map onto `tool_use` and unmapped names fail closed.
`system[0]` is the Claude Code identity block, followed by the gents preamble
and System rows, with two `cache_control` breakpoints; `tools` is omitted when
the surface is empty, and no sampling keys are sent. Claude model IDs are
advertised from `InferenceBackend.models[]`: the preset default is
`claude-sonnet-5`; run `gents config backend discover-models --write` to load
what the subscription serves. The Messages URI is pinned to `api.anthropic.com`
regardless of the backend document's endpoint; only discovery honours an
`http(s)://` endpoint override (a test seam; the presets write the placeholder).

Design notes:

- [`docs/design-notes/claude-subscription.md`](design-notes/claude-subscription.md)
  (the design as shipped: wire, auth, health, discovery, Lean fence, invariants)
- [`docs/design-notes/claude-subscription-history.md`](design-notes/claude-subscription-history.md)
  (timeline and the decisions that were kept or reversed on the way here)

### Setup

1. Configure a backend with `provider_kind = ClaudeCliSubscription` (preset
   `claude-cli-subscription`; aliases `claude-subscription`, `claude-cli`).
   No `:8787`, no dummy API key:

   ```sh
   gents config backend set \
     --graphql http://127.0.0.1:9191/api/v0/graphql \
     --backend-id <did>:backend \
     --name "Claude Max subscription" \
     --backend-preset claude-cli-subscription \
     --max-concurrent 1

   # `backend set` seeds models[] with the literal `default`; only
   # `gents init --backend-preset claude-cli-subscription` seeds a slug (the
   # preset default, claude-sonnet-5). After step 2 (login), fill the catalog
   # from the seat (GET /v1/models with the credential bearer); without
   # --write nothing is written:
   gents config backend discover-models --write \
     --graphql http://127.0.0.1:9191/api/v0/graphql \
     --backend-id <did>:backend --agent-did <did>
   gents config behavior set \
     --graphql http://127.0.0.1:9191/api/v0/graphql \
     --agent-did <did> --behavior-id <id> \
     --backend-id <did>:backend --model-name claude-sonnet-5
   ```

   Migrating an older A2a `OpenAiCompatible` + `http://127.0.0.1:8787/v1` row:

   ```sh
   gents config backend set \
     --graphql http://127.0.0.1:9191/api/v0/graphql \
     --backend-id "$CLAUDE_BACKEND_ID" \
     --name "Claude Max subscription" \
     --backend-preset claude-cli-subscription \
     --max-concurrent 1
   # Then `discover-models --write` as above; clear openai_wire_api / api_key.
   # `gents config backend set --backend-preset claude-cli-subscription`
   # clears sticky openai_wire_api on update.
   ```

2. Sign in and write the credential document. The command runs the PKCE
   loopback flow itself and opens your browser:

   ```sh
   gents claude-login --agent-did did:key:...
   ```

   The login URL is always printed; `--no-browser` only skips opening it in a
   browser.
   `--manual` is the paste-the-code fallback with no localhost callback (use
   it over SSH). `--graphql <url>` writes to a running node instead of the
   local `--home`. The command prints the upserted row with both tokens
   redacted:

   ```json
   {
     "login": "completed",
     "doc_id": "...",
     "credential_id": "claude-subscription:did:key:...",
     "agent_did": "did:key:...",
     "provider": "claude-subscription",
     "access_token_expires_at": "2026-09-04T20:00:00Z",
     "last_refresh": "2026-09-04T12:00:00Z",
     "enabled": true,
     "access_token": "<redacted>",
     "refresh_token": "<redacted>"
   }
   ```

3. Verify (read-only; never refreshes):

   ```sh
   gents diagnose   # checks.claude_auth.ok == true
   ```

4. Start the server. There is no Claude-specific server flag: a Claude-backed
   behavior whose principal has an enabled credential is live, and every
   Claude-backed turn bills the seat.

   ```sh
   gents server
   ```

### Endpoint / billing choice

| Path | Endpoint / transport | Auth / billing |
| --- | --- | --- |
| **Claude Max seat (this recipe)** | `ClaudeCliSubscription` + `claude-cli://subscription` (Anthropic Messages HTTP) | `OAuthCredential` (`claude-subscription`) written by `gents claude-login`; plan meter |
| **Anthropic Console API key** | Anthropic API / Console-billed usage | API key / Console billing — out of scope for this backend; do not confuse with the Max seat |

### Failure modes

| Symptom | Meaning | Fix |
| --- | --- | --- |
| Behavior not runnable: `… has no enabled OAuthCredential for agent <did>; run gents claude-login --agent-did <did>` (agent snapshot gate; `gents diagnose` reports the same condition as `checks.claude_auth.guidance` with different wording) | No enabled `claude-subscription` credential for the behavior's principal | `gents claude-login --agent-did <did>` |
| `gents diagnose` reports `checks.claude_auth.ok == false` with `Claude OAuth credential for agent <did> … is expired or revoked. Re-authenticate with gents claude-login --agent-did <did>`, while its overall `status` stays `ok` and health stays `healthy` | Access token past `access_token_expires_at` on a credential that still has a refresh token (the ordinary state of an idle agent; Anthropic sets the lifetime via `expires_in`, and gents falls back to 1 h when it is absent); `diagnose` reports freshness itself but only a credential it cannot read degrades `status`, and the probe counts the credential healthy (it never refreshes) | Nothing: the next request refreshes. If that refresh fails, see the "Refresh fails" row |
| Health fails with `reading OAuthCredential: OAuthCredential missing refresh_token`; K=3 demotes | Credential has no refresh token (the probe never refreshes); this is the decoder's read error and carries no login hint | `gents claude-login --agent-did <did>`; the next passing cycle promotes the backend |
| Refresh fails at the token endpoint | Refresh grant expired or revoked | `gents claude-login --agent-did <did>`; until then the failure is served for 60 s at a time without another POST, and the new credential is used on the next request |
| Inference **401** | Anthropic rejected the bearer | The bearer is invalidated once and that request fails; the next request refreshes. A 401 that survives a refresh means re-login |
| Unexpected Claude spend | A Claude-backed behavior with an enabled credential on a running server | Disable the backend document for dry runs. Setting the credential `enabled: false` also stops spend, but the lookup filters `enabled = true`, so the credential then reads as missing: K=3 demotes the backend to `unhealthy` and `gents diagnose` reports `status: degraded` |
| Turn fails closed on `tool_use` (unmapped name) | Claude emitted a tool that is not on the behavior's gents surface | Add the tool to the behavior's surface or leave it off; names are never aliased |
| `discover-models` fails with `requires an OAuthCredential document` or a 401/403 from `api.anthropic.com/v1/models` | No enabled credential for the agent DID, or Anthropic rejected the bearer | `gents claude-login --agent-did <did>`, then rerun |
| Fleet probe demotes Claude over HTTP | Old binary still HTTP-probes the placeholder | Rebuild/restart; `ClaudeCliSubscription` skips fleet HTTP probes |

### Credential storage & fleet

Same document model and owner-only refresh rules as ChatGPT Codex and Grok:
`OAuthCredential` with `credential_id = claude-subscription:<agent_did>`,
plaintext token fields (the `InferenceBackend.api_key` precedent), rotating
refresh token, and the agent-scoped replication filter
`OAuthCredential:agent_did=did:key:...` — Claude rows need no new rule.
ChatGPT-only fields (`chatgpt_plan_type`, `is_fedramp`, `id_token`,
`account_id`) stay null/false. Nothing is read from `~/.claude` or any Claude
CLI configuration. The token fields are plaintext: anyone who can reach the
node's GraphQL endpoint can read them (`gents query` refuses the token fields;
raw GraphQL does not).

### Token refresh

- The Messages client asks the shared single-flight `DbCredentialBearer`
  (`OAuthRefreshKind::Claude`) for a bearer before every request; all clients
  for one `credential_id` share one bearer per process.
- Within 5 minutes of `access_token_expires_at`, the owner runtime posts the
  refresh token to Anthropic's token endpoint (JSON body), writes the rotated
  tokens and `last_refresh` back to `OAuthCredential`, then sends the request.
  Replicas use the current access token and receive the rotated document
  through replication.
- A refresh that fails (revoked grant, endpoint error) is not retried for
  60 seconds: the bearer returns the same failure without another POST, and a
  re-login is picked up when the cooldown lapses (≤60 s).
- A `401` invalidates the cached bearer once; that request fails and the next
  one refreshes rather than replaying a clock-fresh but server-revoked token.
- An agent idle past `access_token_expires_at` stays routable: the health probe
  counts a stale credential that still has a refresh token as healthy, and the
  first request of the next turn refreshes it before sending. No operator
  action is needed.

### Health

`ClaudeCliSubscription` is an agent-scoped OAuth kind: the prober reads the
enabled `claude-subscription` credential for the runtime principal's DID each
cycle (see "Probe lifecycle and health"; at most one exists per DID). Fresh
token → healthy, and a document born `unknown` is promoted. Stale token with a
refresh token → still healthy (debug-logged as stale; the next request
refreshes it). Missing credential → failure carrying the
`gents claude-login --agent-did <did>` hint; a read error or a document with
no refresh token also fails, carrying the read error and no hint. K=3 demotes
the in-memory map only. The probe never refreshes and never spawns anything. One
consequence: a credential whose refresh token has been revoked looks healthy to
the prober until a request actually fails; `gents diagnose` still reports
staleness independently because it checks token freshness itself.

### Diagnostics

- `gents diagnose` reports `checks.claude_auth` (`ok`, `credential_id`,
  `provider`, `expires_at`, or a `guidance` string naming
  `gents claude-login --agent-did <did>`). Whenever an enabled
  `ClaudeCliSubscription` backend is configured, a credential that cannot be
  read (no `credential_id`) degrades the overall status; a stale one (`ok ==
  false` with a `credential_id`) does not, since the next request refreshes it.
- `gents config backend discover-models --graphql <url> --backend-id <id>
  --agent-did <did>` GETs `https://api.anthropic.com/v1/models` with the
  credential bearer (plus `anthropic-version` / `anthropic-beta`) and prints
  `discovered_models`; with `--write` it replaces `models[]` with them
  (`models_written`), superseding the single `--model-name` that `gents init`
  seeds (`backend set` seeds the literal `default`). Without `--write` nothing
  is written. 401/403
  appends `gents claude-login` guidance.
  `gents diagnose` does not run discovery for this backend.
- There is no `claude-auth-probe`; `gents diagnose` is the read-only check.

### Known limitations

- The runtime is cache-first: a process serves turns from its in-memory
  bearer and reads the `OAuthCredential` document only when that bearer is
  missing or stale, while `gents diagnose` reads the document. When the
  write-back after a refresh fails (it is logged, not retried by the turn),
  `diagnose` can report a stale document while turns keep succeeding.
- A revoked refresh token does not demote health: the probe never refreshes,
  so the credential stays `healthy` until a request fails, and each failed
  refresh is then served from the 60 s cooldown without another POST.
  Re-login is the only fix.
- `gents init` reseeds `models[]` to the single `--model-name`, also on an
  existing backend; rerun `gents config backend discover-models --write`
  afterwards to restore the seat's catalog.
- Effort / thinking is not sent on the Claude wire: the Messages body carries
  no sampling keys, so reasoning-effort settings have no effect on Claude
  turns.
- `/v1/models` is fetched with `?limit=100` and `has_more` is ignored, so a
  catalog over 100 models would be truncated.
- `discover-models --write` skips the write when discovery returns zero models
  (`write_skipped` in the output); `models[]` is never wiped.

### Unified prod suite

Fold Claude into the **prod** home so Codex `/model` lists Grok **and** Claude
together:

1. Register a `ClaudeCliSubscription` backend on prod `~/.gents` with endpoint
   `claude-cli://subscription` and the model IDs `discover-models` returns.
   Keep the existing Grok/`XaiGrokOAuth` backend. Do **not** point Claude at
   `http://127.0.0.1:8787/v1`.
2. Sign in for the prod agent: `gents claude-login --agent-did <prod DID>`.
3. Start the server:

   ```sh
   gents server
   ```

   Keep the **default behavior** on Grok unless you explicitly point a
   behavior at the Claude backend; a Claude-backed behavior with an enabled
   credential spends the seat on every turn.

4. Chat with one surface:

   ```sh
   gents codex --remote ws://127.0.0.1:9292/
   ```

`/model` should show Grok models and the Claude Max IDs. Claude turns are
tool-capable over the Messages wire. There is no HTTP `claude-proxy` / `:8787`
path anymore.
