# Reusable desktop packages

_Design spec — 2026-07-27. Issue [#877](https://github.com/source-inc/gents/issues/877)._

**Implementation status (PR #878, honest):**

| Phase                | Status                 | Notes                                                                                              |
| -------------------- | ---------------------- | -------------------------------------------------------------------------------------------------- |
| 1 Crate extract      | **done**               | `gents-desktop-bridge`                                                                             |
| 2 Contract prep      | **done**               | full ts-rs request/view generation, fingerprint, error taxonomy, freshness gates                   |
| 3 Plugin-ization     | **done** (native)      | plugin, permissions, path strip, peer id rekey, BridgeError, projection seam                       |
| 4 Fixture host       | **done** (composition) | host shell/home/capabilities, file-backed domain plugin, package surfaces, Rust + frontend CI      |
| 5 Client package     | **done**               | typed transport, one subscription per store, generated public types, testing seam                  |
| 6 Tokens + UI        | **done**               | semantic token contract, conformance fence, shared primitives                                      |
| 7 Chat package       | **done**               | projection, transcript/composer/cancel/tool/denial components, styles, tests                       |
| 8 Fleet package      | **done**               | remote fleet surface + opt-in local-runtime subpath, brand slots, styles, tests                    |
| 9 Operations package | **done**               | focused holds, health, trace, and workspace surfaces, styles, tests                                |
| 10 Release           | **wired**              | packed clean-consumer gate, exact pins, tag workflow; first real tag remains release-time evidence |

**v1 snapshot projection model (accepted deviation):** grants are
**process-wide** via `BridgeConfig.snapshot_grants` (host-declared, fail-closed
default `core_only`). Not per-caller Tauri capability introspection. Hosts must
keep capability files ⊆ that profile; consistency is a host obligation + fixture
tests, not runtime ACL reflection. Per-caller projection is a filed follow-up.

**Recorded follow-ups:** `bridge_runner` remains in `gents-desktop-tauri`
(not yet `test-harness` on the bridge crate). The reusable store owns the shared
snapshot subscription/coalescing boundary; Gents Desktop retains its mature
session polling/restart coordinator in `useDesktopShell` and passes package
components data + callbacks. Each `DesktopClient` now owns a full
transport-bound `client.api`; operations providers and fleet package-owned reads
accept that adapter, while the global API seam remains only for compatibility.
Package CSS is semantic-token-only but preserves the existing class names to
keep the extraction behavior-identical; a future collision-hardening release
may prefix them. Live-lane verification of the serde camelCase fix is fenced by
a wire-format assertion in `test:live:chat` (`itemKey` must arrive camelCase over
real IPC). External Amygdala authentication/Cargo-fetch
and the first real tag publish remain release-environment evidence, not changes
that can be proven inside this repository. The in-tree fixture proves package
and plugin composition plus native Gents-home isolation; it does **not** run a
second domain DefraDB node or an automated pairing/chat/domain journey. Amygdala
must retain or extract the app-private session polling/restart coordinator before
it can claim Gents-equivalent chat recovery.

Design approved 2026-07-27 after three review passes; security-hardened across
PR #878 rounds. Base: [#875](https://github.com/source-inc/gents/pull/875)
(`1a5e23d5`).

Gents Desktop already keeps its reusable runtime behavior in
`crates/gents-desktop-core`, but everything above that — the Tauri command bridge, the
typed TypeScript transport, the view models, and the chat/fleet/operations workflows —
lives inside `apps/gents-desktop`. A downstream Tauri/React distribution therefore has
to fork the app or copy private source. This document specifies a first-party package
boundary that lets a downstream app own its identity, shell, storage, and domain while
reusing Gents chat, fleet, and operator behavior through versioned dependencies.

The motivating downstream is **Amygdala**, a separately branded household-operations
app (see the Amygdala App Platform design, 2026-07-26, in the Amygdala repository).
Its kitchen-inventory domain stays out of Gents; the extension seam it needs lives
here. Amygdala's architecture pairs with two authoritative peers — a Kitchen Gents
runtime for chat and `kitchen-mcp` for inventory collections — and its v1 keeps the
Kitchen domain in a **separate client store** under the Amygdala home. The v1 contract
supports that model (§ Domain storage and co-resident plugins), but the in-tree
fixture does not reproduce or validate the complete two-node topology. Gents Desktop
itself becomes the first-party consumer of every extracted seam, so the package
boundary is exercised on every CI run.

Related docs: [gents.md](gents.md) (platform architecture),
[operations.md](operations.md) (pairing and desktop operation),
[../apps/gents-desktop/README.md](../apps/gents-desktop/README.md) (desktop app),
[../apps/gents-desktop/tests/AGENT_BROWSER.md](../apps/gents-desktop/tests/AGENT_BROWSER.md)
(semantic browser harness), [../crates/gents/proofs/README.md](../crates/gents/proofs/README.md)
(proven core).

## Baseline state that drove the design

This section records the pre-extraction state observed when the design was
approved. The implementation-status table above is authoritative for PR #878.

The boundary proposed below follows the seams the code already has. The load-bearing
facts:

**Rust.** `crates/gents-desktop-core` has no Tauri dependency; it exposes `client`
(`ClientCore`, store, queries, mutations, bearer pairing), `local_runtime`, and
`remote_admin`, and owns identity (`PrincipalIdentity::load_or_create`), storage
layout (`DesktopPaths`, keyed off `GENTS_DESKTOP_HOME`), schema registration
(`ensure_runtime_schemas` + `subscribe_all_collections` inside `ClientCore::start`),
and the embedded DefraDB node. All Tauri coupling lives in
`apps/gents-desktop/src-tauri/src/bridge/`:

- `mod.rs` builds the `tauri::Builder` and registers **55 commands** in one
  `generate_handler!` list; there is no `.setup()` hook (client start is the lazy
  `desktop_client_start` command), one managed state type
  (`DesktopAppState { bridge: Mutex<DesktopBridge> }`), one plugin
  (`tauri-plugin-opener`), and exactly **one event name**,
  `desktop://client-updated`, with payload `{ reason }` where reason ∈
  `{store, health, lifecycle, config}`.
- `mod.rs` also installs process-global infrastructure: a 32 MiB-stack Tokio runtime
  set via `tauri::async_runtime::set()` (iOS DefraDB replay needs the stack), and
  `logging.rs` initializes tracing. These globals matter for plugin coexistence
  (§ Domain storage and co-resident plugins).
- `tauri_commands/*` are thin `#[tauri::command]` wrappers; the logic beneath them —
  `commands/*`, `snapshot/*`, `types/*` (view models), `cascade.rs`,
  `cause_derivation.rs` — is already Tauri-agnostic.
- The bridge depends on `gents-desktop-core`, and directly on the `gents` runtime
  crate (backend registry, tool-surface explain, graphql helpers) and
  `gents-protocol` (bearer tokens).
- The debug-only native-E2E commands (`desktop_native_e2e_config`,
  `desktop_native_e2e_status`) are registered unconditionally but double-gated:
  `#[cfg(debug_assertions)]` bodies plus a `GENTS_NATIVE_E2E=1` runtime check;
  release builds compile them to inert stubs.

**Frontend.** One private npm package (`gents-desktop-tauri`, Vite 7 + React 19, no
router, no state library). The Tauri transport is hard-coded in exactly three modules:
`src/lib/desktop-api.ts` (a `DesktopApiAdapter` object of ~50 typed methods over
`invoke`, with a test-only override `setDesktopApiAdapterForTests`),
`src/lib/desktop-events.ts` (one `listen` wrapper, same override pattern), and
`src/lib/nativeSimulatorE2e.ts` (the in-app native-E2E driver). View-model types are
hand-written mirrors of the Rust `bridge/types/views/*` structs (comment-enforced,
no codegen, no drift gate) and are imported by 49 files through the `lib/types`
barrel. `useDesktopShell` is a single god-hook, but it is also the system's only
**refresh coordinator**: it owns the snapshot/session caches, sequencing refs, the
trailing refresh queue, active-session polling, restart/backoff, and the P2P
auto-restart cooldown (`desktopShellRuntime.ts`, `desktopShellEffects.ts`). Its
action factories are already partitioned by domain (`desktopShellChatActions`,
`...PeerActions`, `...ConfigActions`, `...TaskActions`), and pure projection logic
(`chat-shell.ts`, `conversation-selection.ts`, `fleetMetrics.ts`, `lineageModel.ts`)
is separable. Only 11 components call `desktop-api` directly; the rest are
prop-driven presentation. Styling is global CSS under `@layer` with semantic tokens
in `styles/tokens.css`, `[data-theme]` switching, and one primary breakpoint
(`max-width: 760px`) repeated as a literal in ~10 files. Branding is not centralized:
name strings and logo live in `components/fleet/BrandLockup.tsx`, brand colors in
`tokens.css`.

**Tests.** Unit/component suites and the deterministic browser harness import app
internals through deliberate seams (`setDesktopApiAdapterForTests`,
`setDesktopClientUpdatedListenerFactoryForTests`,
`setDesktopShellTimingConfigForTests`); the external lanes — `tests/agent-browser.mjs`
(deterministic and live modes, `iphone` default viewport) and
`tests/run-ios-simulator-e2e.mjs` + `tests/ios/GentsUITests.swift` — drive only public
surfaces: the Vite-served harness, the `bridge_runner` binary's JSON-ready protocol,
`data-testid` selectors, the `com.source-inc.gents` bundle id, and the
`native-e2e-status.json` temp-file contract. The iPhone branch added mobile bearer
pairing, chat recovery/reconnect/interrupt routing, responsive layout, the agent
browser, and the native Simulator lane; all of these are contracts this design must
keep working, and most of them already point at the seams a package boundary needs.

**Workspace and releases.** Cargo versions are workspace-inherited (`0.8.0`), the npm
version is kept in lockstep manually, releases are git tags `vX.Y.Z` validated against
`workspace.package.version` by `release-macos.yml`. Nothing is published to crates.io
or npm today — and the DefraDB dependencies are git-pinned
(`ssh://…/sourcenetwork/defradb.rs.git`), which makes crates.io publication of any
crate in this dependency cone **impossible**, a hard constraint on the release design
(§ Compatibility and release contract).

## Package and dependency graph

### Rust

One new crate, extracted from `apps/gents-desktop/src-tauri/src/bridge/`:

```
gents (runtime)        gents-protocol
      ▲                     ▲
      │                     │
      └──── gents-desktop-core ◄──────────────┐
                    ▲                         │
                    │                         │
            gents-desktop-bridge  (new; depends on tauri, gents,
                    ▲              gents-desktop-core, gents-protocol)
        ┌───────────┴───────────┐
 gents-desktop-tauri      <downstream host crate, e.g. Amygdala>
 (app binary; owns          (owns its own Builder, identity, domain
  Builder + branding)        plugins/stores, extra commands)
```

- **`crates/gents-desktop-bridge`** takes the entire `bridge/` tree: the
  Tauri-agnostic logic (`commands/*`, `snapshot/*`, `types/*`, `cascade.rs`,
  `cause_derivation.rs`, `logging.rs`), the `#[tauri::command]` wrappers, the managed
  state, and the update pump. It exposes a Tauri **plugin** (§ Native composition
  contract) rather than a builder. It also gains ownership of the `bridge_runner`
  test binary (behind a `test-harness` cargo feature) so the live and agent-browser
  lanes exercise the extracted crate, not the app.
- **`gents-desktop-core` is unchanged in role**: no Tauri, no view models. It gains
  additive host-policy options (§ Native composition contract): a `HomePolicy` in
  place of the implicit `GENTS_DESKTOP_HOME`/platform-data-dir defaults.
- **`gents-desktop-tauri` shrinks to an app shell**: `tauri::Builder`,
  `generate_context!` (bundle identity `com.source-inc.gents`, icons, window,
  capabilities), the plugin registration, and any Gents-app-specific commands. Its
  `bridge/` module is deleted.

Dependency direction rules, enforced structurally: `gents-desktop-bridge` must not
depend on `gents-desktop-tauri` (crate graph makes this a compile error once the
extraction lands); view models live only in the bridge crate; the app crate must not
re-declare or fork them. The bridge's direct dependency on the `gents` runtime crate
is accepted and explicit — the bridge is an operator surface over the runtime, and
hiding that behind `gents-desktop-core` re-exports would add indirection without
adding a boundary.

### Frontend

Six published packages plus the private apps, managed as npm workspaces:

```
@source-inc/gents-desktop-tokens     semantic CSS contract
@source-inc/gents-desktop-ui         shared accessible primitives
@source-inc/gents-desktop-client     transport, shared store, canonical types
        ▲   ▲   ▲
        │   │   └── @source-inc/gents-desktop-chat        chat state + components
        │   └────── @source-inc/gents-desktop-fleet       discovery/pairing/health/peers
        └────────── @source-inc/gents-desktop-operations  holds, traces, health panels
                          ▲
                          │ (all packages consumed by)
              apps/gents-desktop  (private shell: App, Sidebar, navigation,
                                   branding, theme choice, config workspace)
              apps/fixture-host   (independent consumer + domain plugin)
```

- **`@source-inc/gents-desktop-client`** — the only package that knows a transport
  exists, and the owner of shared client state. It defines the `DesktopApiAdapter`
  interface (already present in `desktop-api.ts`), a `TauriTransport` default
  implementation, the **shared store and refresh coordinator** (§ Frontend
  composition contract), the canonical TypeScript view-model and request types
  (moved out of `src/lib/types/`, henceforth generated — see drift gate), and a
  `/testing` subpath export carrying the deterministic in-memory adapter seam that
  `tests/ui-harness/desktopHarness.ts` implements today. The npm scope is
  `@source-inc` because GitHub Packages requires scope = org; the issue's
  `@gents/*` names were illustrative.
- **`@source-inc/gents-desktop-ui`** — the truly shared `CopyButton`,
  `ConfirmDialog`, clipboard helper, and timestamp formatter. It depends only on
  React and semantic tokens; it contains no Gents brand values.
- **`@source-inc/gents-desktop-chat`** — headless first: `chat-shell.ts` projection
  (`projectChatShell`, `ChatWorkflowState`, turn/send state), conversation selection,
  reusable interrupt/cascade helpers, `useMasterDetail`, and the presentational
  components (`ChatComposer`,
  `ChatHeader`, `ChatTranscriptPanel`, `Transcript`, `cancelUx/*`, `slashSkills`).
  Hosts own orchestration and pass snapshots/actions into the prop-driven surface;
  task/schedule actions stay **app-private**.
- **`@source-inc/gents-desktop-fleet`** — `FleetDashboard`, `FleetRow`,
  `AddPeerForm`, `QrScannerDialog`, `peerConnectionImport`, `NetworkPanel`,
  `fleetMetrics`, and `peerConnectionErrors` formatting.
  `BrandLockup` does **not** move — the dashboard takes a `brand` slot/prop.
- **`@source-inc/gents-desktop-operations`** — focused operator panels:
  `HoldsPanel`/`useToolCallHolds`, `RequestTracePanel`, `backendHealth/*`,
  `mcpHealth/*`, and `WorkspaceTreePanel`. Tool lifecycle, background work, and
  subagent progress are conversation-owned in `-chat`.
- **Stays app-private**: `App.tsx`, `Sidebar` and sidebar widgets, hand-rolled view
  switching and shortcuts, theme persistence choice, branding assets and strings, the
  config workspace (`ConfigWorkspace` and the `config/*` panels), and
  `useDesktopShell` itself — now composing the package views while retaining the
  app's production orchestration policies. The config
  authoring surface is deliberately not packaged in v1 (see Unresolved decisions).

Chat/fleet/operations packages depend only on `-client`, `-ui`, and React (plus
their direct rendering/decoder libraries); they never import each other or the
app. `react` and `react-dom` are declared as **peer dependencies**
in every package (never bundled), and the packed-artifact gate (§ Migration and
validation) installs the packages into a clean dependency tree to prove the peer
ranges and `exports` maps are right. Enforcement of the import boundary is
mechanical, not conventional: package `exports` fields hide internals, and a
`check-desktop-package-boundaries.mjs` CI gate forbids
`apps/gents-desktop/src` imports from packages, cross-domain package imports,
host-private CSS tokens, and non-exact lockstep pins. This is the fence that
keeps private imports from silently returning.

Design tokens are a contract, not a package of components:
`@source-inc/gents-desktop-tokens` ships **semantic** custom properties
(`--color-bg/surface/text/accent`, spacing, radii, fonts) that packaged components
reference exclusively, and Gents **brand** values (`--source-green`, brand fonts,
logo) that stay in the app. The existing `design-system-conformance.test.ts` moves
alongside the tokens and becomes the gate that packaged CSS uses only semantic vars.
The semantic/brand split lands **before** any UI package is extracted (§ Migration,
phase 6), so branded CSS is never extracted and revisited.

## Native composition contract

### Snapshot grants (v1 process-wide profile)

The aggregate snapshot is projected at the shared builder seam by
`SnapshotGrants` on `BridgeConfig`. **v1 is single-profile-per-process:** every
webview of a host process sees the same projected sections. The host sets
`snapshot_grants` to match the maximum capability profile it grants any window
(Gents Desktop: `SnapshotGrants::all()`; fixture: chat+fleet+operations bits).
`BridgeConfig::default()` uses `core_only()` (fail closed).

This deliberately does **not** introspect Tauri ACL per invoke. That was the
original multi-webview package-profile ideal; shipping it requires a supported
capability-query API and is tracked as a follow-up. Until then, hosts that
grant different profiles to different webviews must not share one process, or
must accept the more permissive projection.

### A Tauri plugin, not a builder

`gents-desktop-bridge` exposes:

```rust
pub struct BridgeConfig {
    /// Where the client's storage home comes from.
    pub home: HomePolicy,
    /// Host-side ceiling on LOCAL RUNTIME provisioning — and only that.
    /// desktop_client_start performs client-store bootstrap in the resolved
    /// home (create dirs, mint/load the principal identity, open the embedded
    /// node), which clean-install pairing requires; it never provisions or
    /// attaches a local Gents runtime. Local-runtime provisioning goes
    /// exclusively through desktop_init_local_standard (permission set
    /// `runtime-admin`); LocalRuntimeAllowed permits that command,
    /// PairedRemoteOnly makes it fail with BridgeError::Unsupported even when
    /// the permission is granted.
    pub bootstrap: BootstrapPolicy,
    /// Host identity metadata for logs/diagnostics (not payloads): app name, version.
    pub app_meta: AppMeta,
}

pub enum HomePolicy {
    /// Delegates to the existing DesktopPaths::discover() behavior, verbatim:
    /// GENTS_DESKTOP_HOME env override, else dirs::data_local_dir()/gents/desktop
    /// (client/paths.rs). Delegation — not a re-specification — is the contract:
    /// any deviation would make existing installs appear fresh and mint a new
    /// principal identity.
    Default,
    /// Resolved via the Tauri AppHandle path resolver into the host's app-data
    /// directory (sandbox/macOS/iOS-safe). Recommended for downstream hosts.
    AppDataDir { subdirectory: &'static str },
    /// Exact root. Primarily for tests and fixtures.
    FixedRoot(PathBuf),
}

pub fn init<R: tauri::Runtime>(config: BridgeConfig) -> tauri::plugin::TauriPlugin<R>

/// Host-called helpers for process-global setup the plugin must NOT own:
/// installs the large-stack Tokio runtime (tauri::async_runtime::set must run
/// before the Builder) and, optionally, tracing. See coexistence rules below.
pub fn install_runtime();
/// Convenience only; takes an explicit log path/filter. The current app derives
/// its log path through Gents' DesktopPaths (bridge/logging.rs), which would
/// conflict with AppDataDir homes and host-owned logging — so the bridge never
/// infers a log location. Hosts with their own subscriber skip this entirely.
pub fn init_tracing(config: TracingConfig);
```

A downstream host composes it the ordinary Tauri way:

```rust
gents_desktop_bridge::install_runtime();   // before the builder, once per process
tauri::Builder::default()
    .plugin(gents_desktop_bridge::init(BridgeConfig { .. }))
    .plugin(tauri_plugin_opener::init())
    .plugin(kitchen_client::plugin())      // host domain plugin, host-owned store
    .invoke_handler(tauri::generate_handler![amygdala_inventory_list, ..])
    .run(tauri::generate_context!())       // host's context: bundle id, icons, windows
```

**Native policy is authoritative over storage — the webview can never supply a
path.** The client home is resolved exactly once, from `BridgeConfig.home`, at
plugin init, and every command — start, initialization, bootstrap summary,
reset/overwrite — operates on that one resolved home. This is a deliberate contract
change: today's `DesktopInitRequest` accepts **two** path fields, `desktop_home`
_and_ `agent_home` (`bridge/types/requests.rs:7`), which `desktop_init_local_standard`
uses directly (`tauri_commands/lifecycle.rs`) — so a `runtime-admin` webview could
point initialization, including the reset/overwrite flags, at arbitrary filesystem
paths, bypassing `AppDataDir`/`FixedRoot` entirely. Plugin-ization removes **both**
fields (and every other filesystem-path field) from IPC payloads: the client home
comes from `HomePolicy`, and the local runtime home comes from
`BootstrapPolicy::LocalRuntimeAllowed { agent_home: AgentHomePolicy }` (`Default` =
the existing `~/.gents` conventions, or `Fixed(PathBuf)`), resolved natively. Gents
Desktop's env-based flexibility survives on the native side via the `Default`
policies.

The resolved policies bind **every** agent-home consumer, not just initialization.
Today the workspace browser calls `default_agent_home()` independently
(`tauri_commands/workspace.rs:99`), as does tool-surface explanation
(`tools_explain.rs:118`) — under `Fixed` or `PairedRemoteOnly` those would expose
the global `~/.gents` workspace instead of the host-authorized location. Under the
plugin, all three existing consumers — bootstrap summary, workspace listing, and
tool-surface explanation — resolve the agent home from bridge state, and where no
local runtime exists (`PairedRemoteOnly`) workspace listing and tool-surface
explanation **fail closed** with a typed `BridgeError` rather than falling back to
a global default.

Why a plugin instead of an exported handler list: Tauri 2's supported cross-crate
composition mechanism is the plugin API — it carries its own `generate_handler!`,
manages its own state, declares its own permissions, and composes with any number of
host plugins and commands without touching the host's invoke handler. The cost is
that invoke paths change from `desktop_chat_send` to
`plugin:gents-desktop-bridge|desktop_chat_send`. That rename is contained entirely
inside the transport layer — the only code that speaks command strings —
which is `desktop-api.ts` when the rename lands (phase 3) and
`@source-inc/gents-desktop-client` once it is extracted (phase 5); it ships in the
same PR that plugin-izes the bridge, so no consumer outside that layer ever
observes it.

### Capability-scoped permissions

One blanket `default` permission over ~53 commands would erase the security value of
the package boundary at exactly the layer where it matters most. The plugin instead
declares **capability-scoped permission sets**, aligned with the frontend package
split, and hosts opt into each in their capability files:

| Permission set      | Commands (grouped)                                                                                                                                                                                                                    |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `core`              | `desktop_bridge_contract`, `desktop_bootstrap_summary` (**lifecycle-projected** — see below), `desktop_client_snapshot` (**permission-projected** — sections require the matching read grants, see below), `desktop_observer_metrics` |
| `client-lifecycle`  | `desktop_client_start`, `desktop_client_shutdown`, `desktop_set_selected_agent`                                                                                                                                                       |
| `runtime-admin`     | `desktop_init_local_standard`                                                                                                                                                                                                         |
| `session-read`      | session snapshot                                                                                                                                                                                                                      |
| `trace-read`        | request timeline                                                                                                                                                                                                                      |
| `tool-surface-read` | tool-surface explain                                                                                                                                                                                                                  |
| `chat-write`        | send, conversation rename, session fork                                                                                                                                                                                               |
| `resend-control`    | request resend                                                                                                                                                                                                                        |
| `fleet-read`        | peer status fetch (**by saved peer id only** — see below), network status                                                                                                                                                             |
| `workspace-read`    | workspace list                                                                                                                                                                                                                        |
| `fleet-admin`       | peer add/remove/rename, bearer pairing, P2P repair (all address-accepting flows live only here)                                                                                                                                       |
| `operations-read`   | operations snapshot, subagent tree, backend/MCP health lists, MCP probe                                                                                                                                                               |
| `interrupt-read`    | interrupt-cascade preview                                                                                                                                                                                                             |
| `interrupt-control` | interrupt request                                                                                                                                                                                                                     |
| `holds-read`        | tool-call hold list                                                                                                                                                                                                                   |
| `holds-control`     | tool-call hold resolve                                                                                                                                                                                                                |
| `config-read`       | config sections of the projected snapshot (backends, profiles, tools, skills, behaviors, tasks, schedules, triggers)                                                                                                                  |
| `config-write`      | all 17 config save/delete/test commands                                                                                                                                                                                               |
| `tasks`             | task/schedule/event-trigger save + run                                                                                                                                                                                                |
| `native-e2e`        | the two debug-only E2E commands; never part of any default                                                                                                                                                                            |

The shipped `default` set is minimal: `core` + `client-lifecycle`. Notably it
excludes `runtime-admin` — a webview should not be able to provision a local
runtime unless the host grants that explicitly. `desktop_client_shutdown` sits in
`client-lifecycle`, not `runtime-admin`, deliberately: the restart/backoff loop the
client package owns calls shutdown-then-start (`useDesktopShell.ts:208` today), and
the worst a webview can do with shutdown is stop its own client — denial of its own
service, which any webview can already achieve, not an escalation. The line
`desktop_client_start` walks is precise: it performs **client-store bootstrap**
(create the home dirs, mint/load the principal identity, open the embedded node) —
without which a clean-install paired-remote host could never start, pair, or chat —
but it never provisions or attaches a local Gents runtime. Runtime provisioning
lives exclusively in `desktop_init_local_standard` (`runtime-admin`), with
`BootstrapPolicy` as the host-side ceiling on that command (§ contract in
`BridgeConfig` above).

**The aggregate snapshot is permission-projected.** Today
`desktop_client_snapshot` returns every domain's state in one payload —
conversation previews, system prompts, backend endpoints, tool selections, skills,
tasks, schedules, triggers, and fleet data — so putting it in a "minimal" default
would hand `-client` all of that without any read grant and make `config-read`
meaningless. Under the plugin, command _availability_ comes from `core`, but the
payload is **projected by grant**: each read set carries a Tauri permission scope
entry for its snapshot sections (`session-read` → conversations/sessions,
`fleet-read` → peers/network, `config-read` → the config sections listed in the
table, `operations-read` → operations state), the snapshot builder emits only the
sections the calling webview's grants cover, and a default-only webview receives
just lifecycle/runtime status.

Crucially, the projection applies at the **snapshot builder seam, not the command**:
the aggregate snapshot leaks through many responses, not one. `desktop_client_start`
and `desktop_client_shutdown` return a `DesktopClientSnapshot`
(`tauri_commands/lifecycle.rs:59`), every peer/config/task mutation returns a fresh
snapshot through the shared refresh helper (`tauri_commands.rs:22`), and pairing and
peer-removal responses nest one. Because projection lives in the one builder that
all of these call, every snapshot-bearing payload — direct, lifecycle, mutation
refresh, or nested — is projected by the caller's grants. The gate matches the
contract: the bridge projection suite runs **per grant profile** and asserts every
snapshot-bearing response contains exactly the granted sections, not just the
direct snapshot command under default grants. The fixture-host test separately
checks that its declared `SnapshotGrants` match its capability file.

Three refinements keep the projection honest without forcing broad grants:

- **Purpose-built redacted projections.** Chat needs behavior selection and
  slash-skill suggestions (`ChatWorkspace.tsx:104`), and fleet rows render tool
  icons and per-deployment metrics — but neither package should hold `config-read`
  for that. "Ids and labels" is not enough: slash-skill selection consumes behavior
  `skillRefs`/`skillExcludes` and skill `scope`/`enabled` (`slashSkills.ts:26`);
  fleet tool icons consume capability modes and service/CLI identifiers
  (`fleetMetrics.ts:85`); `FleetRow` derives task, backend, conversation, and
  runtime metrics. The `session-read` and `fleet-read` scopes therefore carry
  **purpose-built projection types** (e.g. `ChatBehaviorProjection`,
  `ChatSkillProjection`, `FleetDeploymentProjection`) whose field lists are
  enumerated from the actual consumers at extraction time under one redaction
  rule: identity, selection, capability-shape, and count/metric fields are
  projectable; authored content — system prompts, skill instruction bodies,
  backend endpoints, credentials — is `config-read` only. The projection types are
  part of the generated view-model contract and the fingerprint, so widening one
  is a visible, versioned contract change.
- **`desktop_bootstrap_summary` is lifecycle-projected the same way.** Its current
  payload (`bridge/types/views/bootstrap.rs`) includes filesystem paths, the tool
  root, saved peer addresses, GraphQL endpoints, and agent identity — far more
  than `core` should hand out. Under the plugin, the base (`core`) response is
  init/run state, the local principal DID (the app's own public identity), and
  `app_meta`; the `fleet-read` scope adds saved-peer summaries; the
  `runtime-admin` scope adds filesystem paths, tool root, and endpoint URLs.
- **No arbitrary addresses in read grants.** `desktop_peer_status_fetch` today
  performs a native HTTP fetch against a webview-provided address
  (`tauri_commands/peers.rs:49`) — an SSRF/LAN-scanning primitive if it sat in a
  read grant. Under the plugin it is re-keyed to a **saved peer id**; the native
  side resolves the address from the peer directory. The invariant, stated
  precisely: **read-only commands never accept arbitrary addresses**. Admin flows
  necessarily do — adding a peer or pairing takes an address by nature — which is
  exactly why those commands live in `fleet-admin`, a grant a host extends only to
  surfaces that administer the fleet.

Fine-grained sets are the enforcement unit, but the _supported setup_ unit is the
**package grant profile** — the sets a frontend package's full surface needs. A host
following a package's documented install must never hit permission-denied:

| Package       | Required permission sets                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `-client`     | `default` (`core` + `client-lifecycle`, which covers the restart/backoff loop's shutdown-then-start)                                                                                                                                                                                                                                                                                                                                             |
| `-chat`       | `default` + `session-read` + `chat-write` + `resend-control` (retry) + `interrupt-read` (cascade preview for the cancel dialog) + `interrupt-control` (interrupt is part of the promised chat UX). **Never the hold sets** — enumerating or approving held tool executions is not a chat capability                                                                                                                                              |
| `-fleet`      | `default` + `fleet-read`; add `fleet-admin` for the pairing surfaces (`AddPeerForm`, QR pairing). The local-runtime onboarding affordance ("Connect Local Agent", today inside `FleetDashboard`) is extracted as an **opt-in subcomponent** with its own declared requirement of `runtime-admin` + `BootstrapPolicy::LocalRuntimeAllowed`; the base fleet profile never includes `runtime-admin`, and paired-remote hosts simply don't render it |
| `-operations` | `default` + `operations-read` + `interrupt-read` + `interrupt-control` + `holds-read` + `holds-control` + `trace-read` (`RequestTracePanel` timelines) + `resend-control` (backgrounded-tools resume) + `workspace-read` (`WorkspaceTreePanel`)                                                                                                                                                                                                  |

The app-private config workspace (not packaged in v1) uses `config-read` +
`config-write` + `tool-surface-read` + `tasks` — `tool-surface-read` belongs to the
config family, not chat, because `BehaviorToolSurface` is a config panel.

Each package ships a machine-readable manifest of the commands its code can invoke;
a contract test checks manifest ⊆ declared profile (using the fingerprint's
command→set mapping), opt-in surfaces (like the local-runtime onboarding
subcomponent) carry their own manifests and declared extras, and the fixture host
runs with **exactly** its profile grants so an undeclared command surfaces as a CI
failure, not a downstream bug report.
Gents Desktop grants everything except `native-e2e` (production builds), plus
`native-e2e` in its E2E capability overlay (below). The exact command-to-set
assignment is finalized in the plugin-ization PR from the inventory above; the
review rule is that no set may mix read with mutate.

### Domain storage and co-resident plugins

This is the v1 extension model, resolved explicitly (it was the largest open risk in
the first draft of this design):

**The downstream domain owns its own node, storage, commands, and events.** Amygdala's
Kitchen module runs as its own Tauri plugin (`kitchen_client::plugin()` above) with
its own client store under the Amygdala application home, paired to its own
authoritative peer (`kitchen-mcp`), with its own schemas, ACP policies, commands, and
event names. The Gents bridge neither hosts nor sees domain collections. This matches
the Amygdala platform design's data path (two authoritative peers; separate Kitchen
client store first; same-node co-location only as a later, measured optimization).

**`gents-desktop-bridge` guarantees safe coexistence** with such plugins. Concretely,
the plugin must not own process-wide resources, and the contract enumerates every
global it touches:

- **Async runtime**: the 32 MiB-stack Tokio runtime is _not_ installed by the plugin
  (Tauri requires `async_runtime::set` before the builder anyway). The host calls
  `install_runtime()` once; the helper is idempotent and documented as required on
  iOS (DefraDB history replay overflows default stacks — iPhone-branch evidence).
- **Tracing**: the plugin never initializes tracing; hosts bring their own
  subscriber or call `init_tracing(TracingConfig)` with an explicit log path —
  the helper never derives one from Gents' `DesktopPaths` defaults.
- **Environment**: the plugin reads env vars only under `HomePolicy::Default`
  (`GENTS_DESKTOP_HOME`) and the debug-only `GENTS_NATIVE_E2E`/`GENTS_E2E_*` family;
  `AppDataDir`/`FixedRoot` hosts get zero env coupling.
- **Filesystem**: everything lives under the `HomePolicy`-resolved home; the only
  exception is the debug-only `native-e2e-status.json` in the temp dir, which is
  namespaced and feature-gated.
- **Managed state and events**: `DesktopAppState` is plugin-private state under the
  plugin's namespace; event names stay under the `desktop://` prefix. Domain plugins
  choose their own state types and event prefixes; nothing collides.
- **Ports/network**: the embedded node's P2P/iroh listeners bind per-store under the
  bridge's home; a co-resident domain node binds its own. No fixed-port assumptions.

**Same-node `host_schemas` is deferred, not designed halfway.** Registering domain
schemas into the Gents-managed node is attractive for mobile resource use, but it is
only safe with a stable `BridgeHandle` / document-store API through which downstream
commands access the shared `ClientCore` (locking discipline, ACP scoping, schema
namespace rules, lifecycle ordering). That API is real work with runtime-integrity
implications and is explicitly **out of v1**; the earlier draft's
`BridgeConfig.host_schemas` is withdrawn. If mobile measurements later justify
co-location, the `BridgeHandle` design becomes its own issue with its own review
(owner: maintainers + Amygdala, evidence: mobile resource profiles from the two-node
fixture).

**The fixture proves composition and Gents-home isolation, not the complete
Amygdala topology.** The downstream fixture (§ Migration, phase 4) runs the Gents
bridge plugin and a co-resident fixture-domain Tauri plugin with a separate
file-backed JSON document home, command namespace, and event prefix. It consumes all
six frontend packages, loads real bridge session snapshots, and CI builds the host,
runs its grant-composition test, and runs a native test with two concurrent
`ClientCore` stores under distinct homes. The fixture-domain plugin is a
`BTreeMap` persisted as JSON, not a second embedded DefraDB node, and CI does not
drive bearer pairing, chat, and domain writes through a complete Tauri journey.
That two-node product journey remains downstream/Amygdala integration evidence.

### Ownership split

The **bridge owns**: `DesktopAppState` (the `ClientCore` lifecycle), the update pump
that turns `store_updates()`/`p2p_health_updates()` into `desktop://client-updated`
events, all command implementations, view-model serialization, snapshot builders,
and interrupt-cascade preview/execution.

The **host owns**: the `tauri::Builder` and `generate_context!` (bundle identifier,
product name, icons, plists, entitlements, windows, title-bar style, CSP), the
capability files and which permission sets they grant, every non-Gents plugin
(including its domain plugins and their stores), its own commands and managed state,
process-global setup via the provided helpers, and — through `BridgeConfig` — the
storage home and bootstrap policy. Identity is _derived from_ the storage home:
`PrincipalIdentity::load_or_create` in the home the host chose means each host app
install mints its own principal DID. Amygdala installs are distinct principals from
Gents Desktop installs by construction; nothing in the extraction lets one app assume
another's identity.

### Stable contracts

The bridge's public contract, versioned as one unit (§ Compatibility):

- **Commands**: the 53 production commands in the permission table above, plus
  `desktop_bridge_contract` returning
  `{ contract_version, package_version }` so the TS client can fail fast on a
  mismatched host instead of failing weirdly later.
- **Contract versioning semantics**: `contract_version` is `MAJOR.MINOR`. MINOR
  increments for **additive** changes (new commands, new optional fields, new event
  reasons, new error codes); MAJOR increments for **breaking** changes (removal,
  rename, shape or meaning change of anything existing). The TS client accepts a
  bridge with the same MAJOR and a MINOR ≥ the client's build-time requirement, and
  surfaces a structured startup error otherwise. The contract-fingerprint gate
  (§ Frontend composition contract) classifies each diff as additive or breaking and
  fails CI when the version bump doesn't match the classification. Because an older
  client may legitimately face a _newer_ MINOR, forward compatibility is a client
  obligation, not an accident: a generated closed TypeScript union cannot model
  future additive variants by itself, so the client wraps generated types in a parse
  layer that maps unrecognized event reasons and error codes to explicit
  `unknown`-carrying variants (rendered as safe fallbacks, never crashes), and a
  **forward-compatibility fixture test** runs the client against a fingerprint with
  an extra reason, error code, and optional field.
- **Events**: `desktop://client-updated` with `reason ∈ {store, health, lifecycle,
config}`. The coarse ping-then-refetch model is the contract; fine-grained
  streaming events are explicitly out of scope for v1.
- **Errors**: commands currently return stringly errors, and the frontend already
  string-matches them (`peerConnectionErrors.ts`). The plugin-ization PR — the one
  breaking change window — moves to a serialized
  `BridgeError { code, message, retryable }` with `code` as a closed enum, and the
  client package maps codes to presentation. The code taxonomy is inventoried
  _before_ that window (§ Migration, phase 2) so names, errors, and generated types
  change together, once.
- **View models**: the serialized shapes in `bridge/types/views/*` and
  `bridge/types/requests/*`, with no Gents branding, shell, or navigation assumptions
  in any payload (true today; the contract makes it a rule and the fixture app makes
  it testable).
- **Debug-only E2E surface**: `desktop_native_e2e_config`/`_status` stay in the
  bridge crate (module `e2e`) behind a **`native-e2e` cargo feature**. Cargo features
  are additive and cannot be tied to build profiles, so the gating is explicit, not
  automatic: the feature is never in `default`; the app crate forwards it
  (`native-e2e = ["gents-desktop-bridge/native-e2e"]`); the E2E launchers
  (`run-ios-simulator-e2e.mjs`, the live harness) pass `--features native-e2e` to
  their build steps; release packaging builds without it, and a release gate asserts
  the commands are absent. Compilation alone is not activation: the webview must
  also be _granted_ the plugin's `native-e2e` permission. That grant lives in an
  E2E-only capability file (`capabilities/native-e2e.json`) — and a pitfall makes
  explicit enumeration mandatory: when `app.security.capabilities` is omitted or
  empty, Tauri includes **every** file under `capabilities/`, and today's
  `tauri.conf.json` omits it, so merely not referencing the file would still
  activate it. The contract therefore requires production config to enumerate
  `"capabilities": ["default"]`, the E2E overlay (`--config` merge, alongside
  `--features native-e2e`) to enumerate `"capabilities": ["default", "native-e2e"]`,
  and the release gate to inspect the **effective compiled capability set** in the
  build artifacts — not merely the source config — so a production build neither
  compiles the commands nor grants the permission. The existing runtime double-gate
  (`#[cfg(debug_assertions)]` + `GENTS_NATIVE_E2E=1`) is retained as
  defense-in-depth, so even a mistaken feature enable ships inert stubs in a release
  profile. The commands are documented as an unsupported test contract: present so
  any host can run the native E2E lane, never part of the production API.

Sharp edges propagate to downstream hosts: the bridge crate's docs must carry the
repo's DefraDB rules (`graphql::escape_graphql_string()` for every interpolation;
never emit `[]` in a mutation — emit `null`) because host domain plugins run their
own embedded DefraDB clients in the same process and inherit the same failure modes.

## Frontend composition contract

### Injected transport

`@source-inc/gents-desktop-client` inverts today's hard-coding. The package exports:

```ts
interface DesktopTransport {
  invoke<T>(command: string, args?: unknown): Promise<T>;
  listenClientUpdated(handler: (e: ClientUpdateEvent) => void): Promise<Unlisten>;
}
createDesktopClient(transport?: DesktopTransport): DesktopClient  // typed commands
tauriTransport(): DesktopTransport      // default; the only @tauri-apps/api import
```

The app and downstream hosts pass nothing and get the Tauri transport;
`DesktopClient`/`DesktopStore` tests pass a fake through constructor injection.
The `/testing` subpath exports that deterministic memory transport. The client
also exposes a full `client.api` adapter over the same transport. Operations
providers and fleet components accept it explicitly, so multi-client hosts and
non-Tauri tests do not need process-global overrides. The mature Gents Desktop
harness retains `setDesktopApiAdapterForTests` as a compatibility seam. All
direct Tauri imports remain centralized in `transport.ts`.

### One store, one subscription, one refresh coordinator

Chat, fleet, and operations must not each subscribe to `desktop://client-updated`
and refetch independently: three listeners on one coarse event means duplicate
snapshot fetches, races between overlapping refreshes, and cross-domain views built
from different snapshot generations. Today's god-hook is ugly, but it is also the
thing that prevents all of that — its sequencing refs, trailing refresh queue,
active-session polling, restart/backoff loop, and P2P auto-restart cooldown are
coordination behavior that must survive extraction.

The aggregate refresh portion of that coordination moves into `-client` as a
**shared client store**:

```ts
createDesktopStore(client: DesktopClient, timing?: TimingConfig): DesktopStore
// - owns the single client-updated subscription
// - owns the latest aggregate snapshot, generation counter, debounce, and
//   trailing-refresh queue
// - exposes subscribe/getState for useSyncExternalStore consumers
```

The independent fixture consumes this store through `useSyncExternalStore`; package
tests prove that each store owns one update subscription and coalesces a burst of
events into one refresh. Domain components remain transport-agnostic and prop-driven:
hosts may project this store themselves or supply data/callbacks from another state
layer. Gents Desktop deliberately retains the established session caches, active-turn
polling, restart/backoff, and P2P cooldown policy in `useDesktopShell`; moving those
policies was not required to make the package boundary composable and would have made
the behavior-preserving extraction materially riskier. Therefore a downstream such
as Amygdala requires additional coordinator work to inherit equivalent active-chat
polling and recovery behavior; recreating it independently is not implied by this PR.
`TimingConfig` covers refresh debounce for deterministic host tests.

### Canonical types and the drift gate

Canonical types are **generated from Rust**. The bridge crate's view-model and
request structs derive TS bindings with `ts-rs` (selected by the phase-2 spike),
emitted into `@source-inc/gents-desktop-client/src/generated/`. Two CI
gates replace today's "1:1 mirror — keep in sync" comments:

1. **Codegen freshness**: CI regenerates and fails on diff (generated output is
   committed, reviewable, and versioned with the package).
2. **Contract fingerprint**: a generated `contracts/desktop-bridge.json` — command
   names, permission sets, event names, error codes, and type schemas — is
   snapshot-checked. Any diff is classified additive vs breaking and must be matched
   by the corresponding `contract_version` MINOR or MAJOR bump in the same PR, plus
   a changelog entry. This is how breaking bridge/view-model changes are _identified_
   rather than noticed.

This deliberately does not touch the Lean JSON contract machinery
(`proofs/Proofs/Conformance/Contracts/Json/*`): that fences runtime semantics; this
fences serialization shape between the bridge and its TS consumers. They are
different layers with different authorities.

### Headless state vs presentation

Every domain package is layered: a headless core (pure projections and reusable
hooks, with no host-shell imports) and a component layer on top. The
composition contract for a host shell:

- **State**: hosts may consume the shared snapshot store above or use another state
  layer; package components accept typed data and callbacks.
- **Presentation**: components take data + callbacks; reusable hooks invoke only the
  public client API. Slots/props replace
  hard-coded chrome: `FleetDashboard` takes a `brand` slot; panels take
  label/asset overrides where Gents strings exist today.
- **Navigation**: packages never navigate. Hosts own routes/views (Gents Desktop's
  hand-rolled `workspaceView` state is one valid host; a router-based host is
  another) and mount package surfaces wherever they choose.
- **Responsive ownership**: packages own component-level responsiveness — each ships
  its own media queries at the documented narrow breakpoint (`760px`, published as an
  exported constant `NARROW_BREAKPOINT_PX` and used to end the magic-number drift;
  CSS custom properties can't parameterize `@media`, so the constant is the contract
  and the literal is generated/documented, not ad-hoc). Hosts own **shell-level**
  responsive behavior: the mobile master/detail pane switching currently in `App.tsx`
  moves into a headless `useMasterDetail` helper in `-chat` so hosts get the iPhone
  branch's behavior without adopting Gents' layout.
- **Accessibility**: the existing roles/labels/testids are promoted to contract:
  every interactive packaged component documents its `data-testid` and ARIA surface,
  and the agent-browser's semantic targeting (role/label strategies) doubles as the
  a11y smoke across packages. Testids (`composer-input`, `fleet-pair-*`,
  `assistant-message`, …) are stable API — the native E2E driver, Playwright,
  Bombadil, and XCUITest all depend on them.
- **CSS**: packages ship compiled ESM + `.d.ts` + plain CSS files (no CSS-in-JS, no
  shadow DOM), keeping today's `@layer`-ordered global-CSS model. The
  behavior-preserving extraction keeps existing class names; class names remain
  non-contractual (testids are the contract),
  semantic tokens are the theming API, and `[data-theme]` switching keeps working
  with host-supplied token values.

## Compatibility and release contract

**One version train, lockstep, exact pinning.** All desktop crates and npm packages
release together at `workspace.package.version`, tagged `vX.Y.Z` exactly as today
(`release-macos.yml` already validates tag = workspace version; the npm workspace
versions join that check). Lockstep is the honest choice for packages that share one
serialized contract and one repo: independent versioning would manufacture a
compatibility matrix with only one supported diagonal. The bridge
`contract_version` (MAJOR.MINOR, semantics in § Stable contracts) moves independently
of the release version and is what compatibility decisions key on.

**Distribution:**

- **Rust: git-tag pinning, not crates.io.** The DefraDB git dependencies make
  registry publication impossible for this dependency cone, so the supported
  mechanism is `gents-desktop-bridge = { git = "ssh://git@github.com/source-inc/gents.git", tag = "vX.Y.Z" }`
  (downstream needs repo access — true today for any consumer of this private repo).
  Note the transitive cost: the consumer's Cargo must also fetch the ssh-pinned
  DefraDB revisions, so external access is validated from Amygdala CI in phase 5
  (by exact revision, since no crate-bearing tag exists yet) and by release tag in
  phase 10 — not assumed.
- **npm: GitHub Release tarball URLs.** Org-registry authentication was not made a
  prerequisite for downstream CI, so the implemented release workflow uses the
  documented fallback: it uploads each clean-install-verified `npm pack` tarball as
  a GitHub Release asset, and downstream pins the asset URL per package
  (`"@source-inc/gents-desktop-client": "https://github.com/source-inc/gents/releases/download/vX.Y.Z/source-inc-gents-desktop-client-X.Y.Z.tgz"`).
  npm git dependencies are **not** a viable fallback — a Git URL installs only the
  repository-root package and explicitly does not install its workspaces, so it
  cannot address nested `@source-inc/gents-desktop-*` packages. What downstream
  installs is the packed artifact, and the packed artifact is what
  CI tests (§ Migration).

**Compatibility matrix.** `CHANGELOG.md` has one row per release: tag, bridge
crate version, npm package versions,
**bridge contract version**, minimum `gents` runtime the bridge speaks to. The
runtime handshake command plus the client's startup check turn version skew into a
clear error at boot.

**Changelog and downstream update workflow.** The root `CHANGELOG.md` has a
"Bridge contract" section per release listing every
contract-fingerprint change, marked additive or breaking. The supported downstream
update is: bump the git tag and the npm pins to the same `vX.Y.Z`, read the
Bridge-contract section, run the downstream's contract + e2e + visual gates (the
fixture app below is the template for those gates), merge. Renovate/Dependabot-style
automation is possible against GitHub Packages but out of scope here.

## Migration and validation

### Extraction sequence

The numbered phases are the review sequence; PR #878 intentionally carries the
complete in-repository sequence so the boundary lands usable rather than scaffolded.
Each phase keeps `apps/gents-desktop` behavior-identical unless stated. The
ordering follows two review directives: contract inputs (generated types, error
codes) land **before** the one breaking window so command names, errors, and types
change together; and the downstream fixture guards the external boundary from its
creation onward. Phases 3 and 4 are a **stacked PR pair** to make that guard real:
the fixture (phase 4) is developed on top of the plugin-ization PR (phase 3), and
phase 3 does not merge until phase 4 is green atop it — so an incomplete external
boundary is discovered before the breaking change lands, not in a later phase. From
phase 4 onward the fixture grows with every package.

Entry criterion for every phase: previous phase merged — with the one stated
exception that phase 4 enters when phase 3's PR is _open and green_, since it is
developed stacked on that branch and the pair merges in order (3, then 4) once
phase 4 is green atop it. Standing exit criteria for
every phase: `cargo check --workspace --all-targets`, `cargo test -p gents`, affected
desktop Rust suites, `npm run test:ui` (format, build, unit, Playwright e2e, short
fuzz), and `test:ui:agent --backend deterministic --viewport iphone`; from phase 4
onward, the fixture-host gates; from phase 5 onward, the packed-artifact gate; phases
that touch the live bridge or native surface add the live/iOS lanes named below.

1. **Crate move, no behavior change.** Create `crates/gents-desktop-bridge`
   containing the Tauri-agnostic bridge logic (`commands/`, `snapshot/`, `types/`,
   `cascade.rs`, `cause_derivation.rs`, `logging.rs`); the app's `tauri_commands/*`
   wrappers stay put and import it. Cross-crate `generate_handler!` gymnastics are
   deliberately avoided by leaving the `#[tauri::command]` layer in the app until
   phase 3. Exit: app compiles against the new crate; no `bridge::` module remains
   for moved code; live suites (`test:live:chat`, `test:live:fleet`,
   `test:live:cascade`) pass unchanged.
2. **Contract prep (non-breaking).** The type-generation spike on real view models
   decides `ts-rs` vs `typeshare` (enum representations, serde attrs, chrono/uuid
   handling are the differentiators); generation + the contract-fingerprint snapshot
   are wired against the _current_ bridge types and command list. The
   `BridgeError.code` taxonomy is inventoried from today's string-matched failure
   modes (`peerConnectionErrors.ts`, live sad-path suites) and reviewed. Exit: CI
   drift gates run (and go red on a synthetic change — test the fence itself);
   error-code enum agreed; nothing user-visible changed.
3. **Plugin-ization — the one breaking window.** Move `tauri_commands/*` and
   `state.rs` into the bridge crate behind `gents_desktop_bridge::init(BridgeConfig)`
   with `HomePolicy` (Default/AppDataDir/FixedRoot), `BootstrapPolicy`, and the
   `install_runtime()`/`init_tracing()` host helpers; resolve the home once from
   `BridgeConfig` and strip every filesystem-path field (`desktop_home` **and**
   `agent_home`) from IPC request payloads, re-keying `desktop_peer_status_fetch`
   to saved peer ids and routing every agent-home consumer (bootstrap summary,
   workspace listing, tool-surface explanation) through bridge state with
   fail-closed behavior under `PairedRemoteOnly`;
   declare the capability-scoped permission sets and implement snapshot
   permission-projection at the shared builder seam (covering lifecycle,
   mutation-refresh, and nested pairing/removal responses); introduce
   `BridgeError` and `desktop_bridge_contract`; move
   `bridge_runner` into the bridge crate behind `test-harness` (**status: deferred** —
   runner remains in `gents-desktop-tauri`; launchers still target that package);
   put the `e2e` module
   behind the explicit `native-e2e` feature with app-crate forwarding, the
   E2E-launcher `--features` wiring, and the E2E-only capability overlay
   (`capabilities/native-e2e.json`; production config enumerates
   `"capabilities": ["default"]` explicitly — required because omitting the field
   includes every capability file — and the overlay enumerates
   `["default", "native-e2e"]`). Update the app: builder shrinks to helpers +
   plugin +
   context; `desktop-api.ts` command strings and error handling update in the same
   PR, consuming the phase-2 generated types. Exit:
   `test:ui:agent --backend live --viewport iphone` and `test:ui:live:e2e` green
   against the relocated `bridge_runner`; `test:ui:ios:e2e` green (native surface
   changed, built with `--features native-e2e` + the E2E capability overlay);
   release build verified to exclude both the E2E commands and the `native-e2e`
   grant by inspecting the effective compiled capability set; permission sets
   reviewed against the no-read/mutate-mixing rule; the projection tests green
   **per grant profile and per snapshot-bearing response shape** (direct snapshot,
   lifecycle returns, mutation refreshes, nested pairing/removal responses — a
   default-only caller receives no session/fleet/config/operations sections in any
   of them); the phase-4 fixture green atop this PR (stacked-pair rule) before
   merge.
4. **Minimal downstream fixture host — composition proof (stacked on phase 3;
   phase 3 merges only with this green atop it).** `apps/fixture-host`
   (name open): a minimal Tauri app with a different bundle id, product name, icon,
   and `HomePolicy::AppDataDir` home, granting only `default + session-read +
chat-write + fleet-read + fleet-admin + operations-read` permissions (no
   `runtime-admin`, no `config-write`) and configuring the paired-remote policy,
   registering the Gents
   bridge plugin **and** a file-backed fixture-domain plugin with its own command
   and event prefix. CI builds the host and runs its grant-composition test.
   Exit delivered in this PR: fixture compiles in CI; a Rust integration test boots
   two `ClientCore` stores simultaneously and asserts their resolved roots and
   identity files do not collide; grant declarations match the capability file.
   A second domain DefraDB node and automated pairing/chat/domain Tauri journey are
   explicitly not claimed by this fixture.
5. **npm workspaces + `@source-inc/gents-desktop-client`.** Workspace bootstrap;
   extract transport interface, injected client, the shared store/refresh
   coordinator for aggregate snapshots (the app-private active-session and restart
   policies remain an explicit downstream requirement),
   generated types, `/testing` adapter contract; peer-dependency declarations;
   dependency-lint fence on. The ui-harness switches from test-only setters to
   public injection. The **packed-artifact gate** starts here: CI runs `npm pack` on
   each package and installs the tarballs into the fixture host's clean dependency
   tree (no workspace links), catching missing `exports`, undeclared deps, and
   dropped CSS assets. Distribution access is validated **end to end from Amygdala
   CI** here: GitHub Packages auth for npm (or the registry decision flips to the
   fallback) _and_ an external Cargo fetch of the private Rust chain — a clean
   environment outside this workspace resolving `gents-desktop-bridge` by **exact
   commit revision** (no release tag carrying the crate exists yet; tag consumption
   is proven at phase 10), including the transitive `gents` and ssh-pinned DefraDB
   revisions. The in-workspace fixture and the npm-pack gate cannot prove that;
   this check can.
   Exit: zero imports of `src/lib/desktop-api` outside the app-shell composition
   layer; fixture consumes `-client` from a packed tarball; single-subscription
   property tested (N update events → one coalesced refresh); forward-compatibility
   fixture test green (client tolerates an extra event reason, error code, and
   optional field).
6. **Tokens/theming split — before any UI extraction.** Semantic vs brand token
   separation in the app's CSS; `design-system-conformance` becomes the
   semantic-only gate for everything that will be extracted. Exit: app renders
   identically (visual suite); a token-override smoke shows retheming without
   patching components; no `--source-*` reference remains in CSS slated for
   extraction.
7. **`@source-inc/gents-desktop-chat`.** Headless projection + prop-driven
   components; `useMasterDetail`; semantic-only moved CSS; visual
   baselines re-approved. Delivered evidence: chat unit suites run from the
   package, Gents Desktop retains its deterministic/live journeys, and the fixture
   renders fetched bridge session snapshots. Not delivered here: a fixture iOS
   project or automated fixture pairing/chat/recovery journey.
8. **`@source-inc/gents-desktop-fleet`.** Same shape; `BrandLockup` stays in-app via
   `brand` slot; the fixture host consumes the package surface. Delivered evidence:
   package tests/build and the existing Gents Desktop pairing/fleet lanes. An
   automated fixture clean-install pairing journey remains downstream evidence.
9. **`@source-inc/gents-desktop-operations`.** Focused holds, health, trace, and
   workspace panels. Background and subagent presentation moved into the chat
   timeline so hosts do not have to compose a second tool-state UI.
10. **Release wiring.** Publish on tag (GitHub Packages or fallback per phase-5
    evidence); `CHANGELOG.md` + compat matrix; tag-validation extended to npm
    versions; documented downstream update workflow. Exit: a dry-run tag publishes
    all packages at one version; the fixture app consumes them by exact pin through
    a rehearsed pin-bump PR; and external Cargo consumption **by the release tag**
    is proven from Amygdala CI (completing the phase-5 by-revision validation).

### How the existing lanes keep working

- **`test:ui:agent`** (deterministic/live, `iphone` default): the harness keeps
  driving the real app shell; its adapter injection goes through the public
  `-client` API after phase 5, and its live mode targets the relocated
  `bridge_runner` after phase 3. Because it never imported app internals, its JSONL
  protocol, semantic targeting, and viewport presets are unchanged — and the fixture
  app reuses it wholesale by pointing it at a different harness entry.
- **`test:ui:ios:e2e`**: the mint-invite → clean-install → pair → chat → stability
  flow is untouched; the bundle id and app-bundle path become parameters (defaulting
  to Gents values), the build step adds `--features native-e2e` plus the E2E
  capability overlay, and the `native-e2e-status.json` contract and staged status
  reporting stay. The XCUITest
  OCR lane needs no change beyond bundle-id parameterization.
- **Unit/component suites** move with their code into the packages they test;
  app-level suites keep covering composition. The `playwright-fixture-guard` pattern
  extends to the new packages (specs go through shared fixtures only).

### Traceability

| #877 acceptance criterion                                                                       | Package / API                                                                                                                             | Phase       | Verification gate                                                                                                                |
| ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ----------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Minimal downstream app owns binary, identity, storage home, schema registration, extra commands | `gents-desktop-bridge::init(BridgeConfig)` + `HomePolicy`; domain plugins own their stores/schemas (co-residence contract)                | 3–4         | Fixture build + grant test; native concurrent-`ClientCore` home-isolation test. A second domain node remains downstream evidence |
| Working chat surface: streaming, retry, interrupt, reconnect, recovery — no copied source       | `@source-inc/gents-desktop-chat` projection/components over the typed `-client` contract                                                  | 7           | Gents agent-browser/live chat lanes; fixture renders session snapshots. Downstream recovery coordinator/journey remains required |
| Fleet pairing, health, peer management via package API                                          | `@source-inc/gents-desktop-fleet` (+ bridge `fleet-*` permission sets)                                                                    | 8           | Gents `test:live:fleet` and QR/bearer journeys; fixture composes the surface. Downstream clean-install pairing remains required  |
| Operator holds/traces/cancellation via package API                                              | `@source-inc/gents-desktop-operations` (+ `operations-read`, interrupt, and hold permission sets)                                         | 9           | `test:live:operations`/`interrupt`/`cascade` and deterministic operations scenarios                                              |
| Own branding, semantic theme, navigation, domain routes without patching components             | Semantic tokens contract (split before extraction), `brand` slots, host-owned navigation                                                  | 6–9         | Token-override smoke, fixture-host distinct branding + domain module, visual suite                                               |
| Gents Desktop builds and passes its checks consuming the extracted packages                     | App consumes all four packages + plugin                                                                                                   | every phase | Standing exit gates on each phase (app is the first consumer throughout)                                                         |
| Documented version-bump/update workflow                                                         | Lockstep train, exact pins, `CHANGELOG.md`, compat matrix, contract handshake with additive/breaking semantics                            | 10          | Dry-run tag publish + fixture pin-bump rehearsal; packed-artifact gate from phase 5                                              |
| Non-goals: no plugin marketplace; no Amygdala domain code upstream; no weakened Gents semantics | Extension = co-resident plugins, slots/registry, config only; fixture's domain stays in fixture; runtime authority unchanged (§ Security) | —           | Review fence: dependency-lint + crate graph + permission-set review; no runtime-semantic diffs in extraction PRs                 |

## Security and runtime integrity

**The runtime stays the semantic authority.** Every bridge command already delegates
to `gents-desktop-core` and the `gents` runtime; extraction moves code across crate
boundaries without changing what transitions are legal, what invariants hold, or what
the provider is fed. Interrupt cascades, request lifecycles, tool-call holds, and
recovery all remain runtime-owned; the packages render and request, they never
decide. Downstream hosts get no API to override lifecycle behavior — that absence is
the design, not an oversight.

**Permissions make the boundary enforceable at the webview line.** The
capability-scoped sets mean a compromised or merely buggy host webview granted the
chat profile cannot invoke pairing, config mutation, or runtime provisioning; it
holds the interrupt sets deliberately (interrupt is part of the chat contract) but
never the hold sets — enumerating or approving privileged held tool executions is
an operator capability, not a chat one. The blanket-default alternative was
rejected during review for exactly this reason. Runtime provisioning is additionally double-gated: the
`runtime-admin` permission at the webview boundary and `BootstrapPolicy` as the
host-side ceiling, with `desktop_client_start` limited to client-store bootstrap
and structurally unable to provision a runtime. And the webview never chooses
where any of it happens: the storage home is resolved once from
`BridgeConfig.home`, and plugin-ization strips the `desktop_home` request fields
that today would let a `runtime-admin` webview aim initialization — with its
reset/overwrite flags — at an arbitrary path.

**Identity and ACP boundaries stay explicit.** Principals are minted per storage
home by `PrincipalIdentity`; bearer pairing keeps its full verification chain
(issuer signature, freshness, network-admin check, signed behavior binding, ticket
peer id) inside `gents-desktop-core`, untouched by packaging. Domain plugins run
their own clients in their own homes under their own ACP policies; the co-residence
contract gives them **no supported API path** into the bridge's store, and the
deferred `BridgeHandle` is the only future mechanism that would — which is precisely
why it is deferred to its own reviewed design rather than sketched here. Be clear
about what that boundary is: native Rust plugins share one trusted process and
filesystem, so a co-resident plugin is trusted code by construction. Tauri
permissions protect the webview boundary, not one native plugin from another; the
protection against a hostile domain plugin is code review of what the host links,
not this contract.

**Lean obligations.** This is a packaging design: it moves seams, adds additive
config, and renames invoke paths. It does not change legal runtime transitions,
invariants, or provider inputs, so it requires no speculative proof changes. Two
watchpoints where implementation could drift into Lean territory, called out so
follow-up PRs treat them correctly: (1) `BootstrapPolicy` must only _gate_ the
existing `init_standard_local_runtime` path behind the `runtime-admin` command —
never add a new lifecycle and never let `desktop_client_start` become a
_runtime-provisioning_ path (the client-store bootstrap inside `ClientCore::start`
is existing behavior and stays); (2) any temptation to enrich the event contract beyond the coarse
`client-updated` ping into semantic lifecycle events would put event ordering into
the contract and must go through the Lean model → conformance test → Rust flow
before shipping. The future `BridgeHandle`/document-store API is a third: shared
access to the bridge's `ClientCore` from host commands touches persistence-lifecycle
invariants and gets the full foundation flow when it is designed.

## Rejected alternatives

- **Copying or git-subtree sharing app source.** Rejected: no versioned contract, no
  drift detection, and every upstream chat-stability fix becomes a manual merge — the
  exact failure mode #877 exists to end. Subtrees also copy private internals
  wholesale, erasing the public/private line this design draws.
- **One monolithic desktop UI package.** Rejected: it couples chat consumers to
  fleet/operations churn, makes semver signals meaningless (everything breaks
  everything), and forecloses hosts that want chat without the operator surface.
  The cost of four packages is low because they share one version train.
- **Bridge crate owning a complete `tauri::Builder`.** Rejected: the builder is where
  host identity lives (`generate_context!`, bundle id, icons, windows, capabilities,
  host plugins). A bridge-owned builder would either hardcode Gents identity or grow
  a config surface that re-implements Tauri. The plugin boundary composes instead.
- **Same-node `host_schemas` in v1.** Withdrawn after review: it competes with the
  domain-client model Amygdala actually specifies (separate Kitchen store paired to
  its own authoritative peer), and it is unsafe without a designed
  `BridgeHandle`/document-store API for downstream access to the shared
  `ClientCore`. Deferred behind that future design, gated on mobile resource
  measurements from a future downstream two-node journey.
- **One blanket plugin permission.** Rejected: a single `default` covering pairing,
  config mutation, and cancellation alongside reads would make the package split
  cosmetic at the native security boundary. Capability-scoped sets with a minimal
  default replace it.
- **Per-domain event subscriptions in the frontend.** Rejected: three independent
  listeners on one coarse event reintroduce the duplicate-refetch and
  snapshot-skew bugs the god-hook currently prevents. The shared store/refresh
  coordinator in `-client` is the boundary-preserving replacement.
- **Duplicated Rust and TS view models without a drift gate.** Rejected — this is the
  status quo, held together by "keep in sync" comments across 49 importing files. It
  already costs a hand-written normalizer (`normalizeInitSummary`) and will silently
  corrupt downstream apps the first time a field renames. Generated types + contract
  fingerprint are non-negotiable in this design.
- **Letting downstreams override Gents runtime semantics.** Rejected per #877
  non-goals and the repo's foundation: the proven lifecycle/invariant core is the
  product. Extension is additive (co-resident plugins, commands, panels, tokens);
  semantic override would turn every downstream bug into a Gents trust problem.
- **Keeping global (non-plugin) command registration to preserve invoke names.**
  Considered for phase 3: cross-crate `generate_handler!` re-export tricks avoid the
  `plugin:` rename but are unsupported, fragile across Tauri versions, and leave
  permissions/capabilities unmodeled. One contained rename inside the client package
  is cheaper than a permanently awkward composition seam.

## Unresolved decisions

Stated openly rather than buried as implementation detail:

1. **npm scope and final names** (`@source-inc/gents-desktop-*` proposed; separate
   `-tokens` package or tokens-in-client). Owner: maintainers, at design review.
   GitHub Packages forces the `@source-inc` scope if that registry is chosen.
2. **Distribution access: GitHub Packages vs release-tarball URLs for npm, and
   external Cargo access to the private git chain.** Recommended: GitHub Packages,
   **decided
   by the phase-5 validation from Amygdala CI** — neither the registry nor the
   git-tag Rust story is committed to until Amygdala CI has authenticated to the
   npm registry _and_ fetched `gents-desktop-bridge` (with transitive `gents` +
   ssh-pinned DefraDB revisions) via Cargo from outside this workspace. Owner:
   maintainers + Amygdala.
3. **Type-generation tool: `ts-rs` vs `typeshare`.** Decided by the phase-2 spike on
   real view models (enum representations, `serde` attrs, chrono/uuid handling are
   the known differentiators), before the breaking window. Owner: phase-2
   implementer, with the spike diff as evidence.
4. **Config workspace packaging.** The agent/behavior/backend authoring surface
   stays app-private in v1. If Amygdala needs config authoring (not just consuming
   configured agents), a `-config` package is a follow-up with the same layering —
   its permission set (`config-write`) already exists. Owner: Amygdala requirements;
   revisit after phase 9.
5. **Final permission-set granularity.** The table in § Capability-scoped
   permissions is the proposal; the phase-3 PR finalizes command-to-set assignment
   under the no-read/mutate-mixing rule (e.g., whether `desktop_set_selected_agent`
   belongs in `client-lifecycle` or `chat-write`). Owner: phase-3 implementer +
   reviewer.
6. **`BridgeHandle`/document-store API for same-node domain schemas.** Deferred
   entirely; opened as its own issue only if mobile resource measurements from the
   downstream two-node measurements justify co-location. Owner: maintainers +
   Amygdala; evidence: mobile profiles.
7. **Fixture-host location and iOS project generation** (`apps/fixture-host` with
   committed generated Xcode project vs XcodeGen-on-demand like the main app).
   Owner: phase-4/7 implementers; constraint: the lane must stay runnable on the
   self-hosted macOS runner.

## References

Issue: [#877](https://github.com/source-inc/gents/issues/877). Base series:
[#875](https://github.com/source-inc/gents/pull/875) (squash-merged as `1a5e23d5`,
formerly `agent/iphone-amy-bearer-pairing`) — mobile bearer pairing with
cryptographically verified reciprocal readiness and relaunch revalidation, chat
recovery/interrupt routing, responsive shell, agent-browser harness, native
Simulator E2E. Downstream design: _Amygdala App Platform — Downstream Product on
Gents_ (2026-07-26, Amygdala repository,
`docs/superpowers/specs/2026-07-26-amygdala-app-platform-design.md`) — two
authoritative peers, separate Kitchen client store, composition API. Key code
anchors: `apps/gents-desktop/src-tauri/src/bridge/mod.rs` (builder + 55-command
handler), `crates/gents-desktop-core/src/client/` (`core/bearer_pairing.rs`,
`paths.rs`, `principal_identity.rs`, `schema.rs`),
`apps/gents-desktop/src/lib/desktop-api.ts`, `src/lib/types/`,
`src/hooks/useDesktopShell.ts` + `desktopShell*`,
`apps/gents-desktop/tests/{agent-browser.mjs,run-ios-simulator-e2e.mjs,ios/GentsUITests.swift,ui-harness/}`.
