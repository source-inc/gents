# Stock Grok UI hydration completion audit

The unchanged stock Grok TUI is the compatibility target. All work stays in
PR #1363; #1324 is superseded. Green CI for the background fixes is not proof
that the broader hydration goal is complete. Never merge automatically.

## Verified background baseline

Commit a8a9d5bd passed all 11 CI jobs, 307 local shim regressions, and the full
workspace check. The runtime suite passed 2,705 tests (3 ignored). Live GLM
and unchanged Grok 1.0.13 checks cover child task output, native bash kill,
native subagent stop/cascade, later-turn steering, and automatic wakeups.
See grok-background-projection.md and PR verification comments for evidence.

## Verification chronology

### User-test follow-up: goal creation

The user found that `/goal <objective>` was rejected, while the suggested
`create_goal` workaround was disabled on the demo behavior. Goal management
and rendering did not establish complete slash-command compatibility.
The follow-up routes creation through the runtime's existing atomic
`submit_goal_backed_request_local` owner (Goal + claim + signed first request),
preserving the ordinary prompt/cancellation/delivery path and prompt metadata.
It matches stock trailing `--budget <positive integer>` parsing. No runtime
state machine or provider-input assembly rule changes are required; the
existing `GoalAutomation.submissionSafe` contract remains the fence.
All 331 shim tests, the workspace all-target check and binary build passed
(`/tmp/grok-goal-create-{suite,check,build}.log`). The dedicated live probe
`demo/grok-tui-port/scripts/grok_goal_create_probe.py` passed on an isolated
GLM server: session `grok-edge-fcd34529fa274274`, initial signed request
`6e617ff5-9034-4735-80cf-b24bc2734b0d`, budget 100,000, goal complete. It verified
the native goal update, prompt identity, stripped objective, real inference and
runtime completion. Existing operator demo sessions were not interrupted.

The final review follow-ups passed all 330 shim tests, the full workspace
all-target check, and the binary build. Both independent final reviewers
(Claude and Grok) reported no blockers. The dated checkpoints below preserve
the investigation history; intermediate “pending” statements are superseded
by their subsequent verification results. PR #1363 records final-tip CI.

### Latest review checkpoint

The final native Grok 4.6 read-only review also completed with no blockers
(`/tmp/grok-final-native-review.log`). It checked viewer identity/cancellation,
scoping, replay matching, unknown-identity retention, goal observation, and
lock ordering. Two nonblocking correctness observations received small local
follow-ups: install the cancellation target before sending a visible viewer
echo, and compare the rendered goal observation so elapsed time advances even
when persisted goal fields and usage are unchanged. Tests/check/build passed in
`/tmp/grok-review-polish-{tests,check,build}.log`.

Other Grok observations do not require a new runtime owner: native synthetic
prompt-ID families remain reserved; explicitly addressed, authorized observed
requests can be cancelled even when not the current visible wake; live/replay
echo metadata follows their different client paths. Empty prompt content has
no meaningful rendered text. Long-session query scaling is tracked in #1386.

Commit `38848104` passed all 11 CI checks (run `33952659240`), including the
goal accounting and replay chronology fixes documented below. A focused
read-only Claude review of those changes completed with no blockers
(`/tmp/grok-final-projection-claude-review.log`). It independently checked
ordered-assignment correctness, fail-closed binding, durable timing, and
read-only usage accounting. Nonblocking findings still require disposition:
an unmatched live tail can prevent binding a closed prefix during reconnect;
unknown response identity can temporarily discard delivery/timing evidence;
goal usage scans at the 200 ms observation cadence are costly for old sessions.

Local review follow-ups now preserve delivery/timing evidence on an unknown
response identity, distinguish a known replacement, and bind a uniquely
identified closed prefix even while an unmatched, unstamped live tip remains
open. The latter has an embedded reconnect regression with two identical
closed segments plus a still-streaming third segment. Missing interior,
divergent, stamped, or genuinely ambiguous segments are not relaxed.
Goal accounting observation now runs once per second; token/tool streaming
retains its existing cadence. All 330 shim tests, the workspace all-target
check, and binary build passed (`/tmp/grok-review-final-{tests,check,build}.log`).
On the rebuilt server, the persisted goal-continuation replay probe passed
again (279 exact assistant characters, historical timestamps bounded by the
durable completion), and the goal control/clear/recreate probe passed on
`grok-edge-4f9bef9dfb204ea6` with the coarser observation cadence.

The broader runtime indexing/materialization work is tracked in
https://github.com/gents-ai/gents/issues/1386. It must preserve complete history,
exact ownership, and late background events, not introduce a shim database or
silently stop observing old sessions. This is a scaling follow-up, not a claim
that the current read scans have constant cost.

The remaining multi-client finding has a local follow-up: discovered human
turns re-read authorized submission metadata, echo the human prompt under its
original identity, and reuse the stock `turn_completed` viewer finalizer. No
parallel legacy `prompt_complete` rail is needed. Runtime
wakes keep their hidden synthetic identity. Viewer cancellation matches the
active observed request's persisted prompt identity and requests the runtime
interrupt; only runtime acknowledgement terminalizes it. The new DB regression
checks visible echo, wrong-target rejection, cancellation intent, acknowledgement,
and completion identity. Existing wake fixtures now explicitly carry the
runtime source kind instead of masquerading as human submissions. This slice
still needs passing broad gates and a two-client stock/live check; it is not
included in the green published tip.

The viewer follow-up now passed all 328 shim tests, the workspace check, and
the binary build (`/tmp/grok-viewer-canonical-*.log`). Two-client live checks
used the unchanged Grok 1.0.13 viewer on `grok-edge-7f5059b3e1804437` and a
separate leader-socket driver:

- `peer-viewer-live-1` / request `af9c1bca-2e38-48c8-811d-34166841af34`:
  human prompt visible, `PEER_VIEWER_OK` streamed, native Worked-for marker,
  idle viewer and driver `end_turn`; runtime request completed without tools.
- `peer-viewer-cancel-1` / request `8cee0be6-085d-4299-9e66-213b061a7fa1`:
  viewer displayed foreground `sleep 120`; Escape in that viewer requested
  cancellation. Driver returned `cancelled`, runtime request became interrupted,
  and its bash tool became cancelled with `tool call cancelled`. Stock displayed
  `Turn cancelled by user in 24s` and cleared the spinner. No client patches,
  synthetic terminal writes, or duplicate legacy completion rail were needed.

Published commit `76c022fa` passed all 11 CI checks (run `33948163091`).
The chronological implementation notes below retain intermediate test states;
those are not the current status of that published commit.

The independent Claude read-only review completed against the stock 1.0.13
source and found live user-echo `promptIndex`/`hideFromScrollback` in the wrong
wire location. They belong in `ContentChunk._meta`, beside `content`, as replay
already emits them. The follow-up changes that shape and its regression test.

Gents code-review run `e40f1888-272a-4d4b-8356-0f201e8c24ff` completed all graph
stages with 12 confirmed records, including duplicate reports of the same
issues. Its "pending gates" finding describes an earlier audit checkpoint;
the actual published-tip checks above supersede it. Its code findings still
require individual disposition, not dismissal based on green CI.

Follow-up work in progress:

- Replay no longer holds the connection registry during DB reads or outbound
  delivery. An abort-safe, connection-local reservation rejects concurrent
  loads of the same session. The stalled-output/abort regression and all 318
  shim tests passed before the subsequent echo/history changes.
- History discovery folds bounded request pages into one summary per session.
  The non-paginated stock roster scans once, rather than repeating the full
  history scan for every picker page. Search, first-prompt attribution, actual
  activity ordering, and complete history remain intact. This reduces memory
  and repeated scans; it does **not** make the remaining full-history DB scan
  constant-cost. A persisted activity index would belong to the runtime, not
  a shim-owned cache.
- Remaining review triage includes late-event polling, repeated context-owner
  reads, legacy partial session-info behavior,
  and visibility of human turns submitted through another attached client.
- Descendant accounting now resolves readable child identities in batches of
  128, preserving exact requester identity and rejecting ambiguous aliases.
  All 320 shim tests and the workspace all-target check passed. The final
  duplicate-alias extension also passed its separate targeted rerun.

Additional native hydration gap identified during the capability inventory:
stock `extensions/notification.rs::SessionUpdate::GoalUpdated` feeds the
pager's goal panel (`app/acp_handler/session_notification.rs`). Gents already
persists `Goal` objective/status/token budget/token usage/active time and has
`goal::GoalDocument` helpers. The published checkpoint had no GoalUpdated
producer; the local follow-up projects this canonical state without adding a
todo list or second orchestrator.

The follow-up now adds a read-only `goals.rs` projector to the attached session
observer. It uses `goal::load_canonical_goal` and the runtime active-time helper,
not a fresh goal query/selection policy or a usage mutation. Its delivery cursor
commits after successful sends, suppresses unchanged observations, and emits
native `cleared` when the last observed goal disappears. The stock parser
explicitly renders unfamiliar statuses as paused, so `usage_limited` remains
the real runtime status with an explanatory pause message, rather than claiming
an infrastructure failure. Stock worker/verifier phases and round counters are
not mapped from Gents continuation attempts.

Both initial goal tests passed: numeric/status projection and DB-backed exact
agent/session scoping, unchanged DB records, failed-send retry, no duplicate
delivery, and deletion. The stock goal-detail view advertises
`/goal status | pause | resume | clear`; those resolve in the stock shell's
`slash_commands.rs`, not an `x.ai/goal/*` RPC. The shim does not yet implement
that command path in the published checkpoint. The local follow-up now
intercepts those exact single-text prompt commands after attached-session
authorization. Status reads the canonical goal; pause/resume use runtime
`set_goal`; clear uses `delete_goals_for_session`, including its replicated
twins/creation-claim cleanup. No model request is submitted for these controls.
The stock shell's command output uses `ContentChunk._meta.hostTurn`; the
follow-up uses the same marker. Arbitrary `/goal <objective>` creation remains
an explicit unsupported command, rather than pretending to submit an atomic
goal-backed run. Goals can be created through the runtime CLI or through
`create_goal` when that tool is enabled for the behavior.

All 324 shim regressions and the all-target workspace check passed before the
final hostTurn marker adjustment. The targeted marker/empty-metadata regression,
live binary build, and another workspace check subsequently passed too.

Live fixture checks now passed on `grok-edge-7f5059b3e1804437`:

- `grok_goal_probe.py` verified paused-goal updates plus status/pause/clear over
  the real leader socket. It refuses to replace existing goals. A distinct
  retained paused fixture was used for rendering, because Defra correctly
  refuses to recreate an identically addressed deleted document.
- Unchanged Grok 1.0.13 dashboard resume displayed **Goal: Paused**, 123/1k
  tokens, 12%, and 7 seconds. Opening the native goal detail panel displayed
  the same objective, status, budget bar, and command hints. `/goal status`
  rendered the persisted values as host-command output.
- `/goal resume` reached the runtime owner, which refreshed the fixture's
  synthetic usage to the session's real 14,315 tokens and marked its 1,000-token
  budget exhausted. The native panel updated to **Budget**. No new inference
  request ran. `/goal clear` removed the fixture goal and the visible panel.
- Trying to pause that budget-limited goal exposed an undesirable retry-style
  RPC error. The final follow-up now asks the runtime's existing
  `apply_operator_status_transition` whether the action is legal and returns
  an explanatory host-command response when it is not; it does not change
  the transition rules or write an illegal state. A DB-backed regression
  asserts the goal remains budget-limited. All 324 shim tests and the workspace
  all-target check passed for this last adjustment. Publication remains pending.

The focused Claude review completed and identified a clear/recreate blocker:
stock permanently suppresses a cleared `goal_id`, while Gents reuses its
logical goal ID for a session. The follow-up now derives the wire display ID
from the durable physical Goal document identity (`gents:<_docID>`), leaving
the canonical logical ID in `update._meta.gents/goalId`. No connection-local
generation counter or runtime identity change is introduced. Clear emits the
last delivered physical incarnation's ID. The DB regression recreates the
same logical ID and asserts that its next wire ID differs; status/usage changes
within one document retain the same wire ID. All 324 shim tests, the binary
build, and workspace all-target check passed (`/tmp/grok-goal-incarnation-*.log`).
The live probe verified distinct physical IDs after clearing and recreating
the same logical goal. The same attached stock Grok 1.0.13 client then showed
the recreated goal and its detail panel; clearing it removed the panel again.

A separate inference check created a paused goal through the runtime CLI
(not a raw fixture mutation) in `grok-edge-7f5059b3e1804437`. Stock `/goal resume`
caused runtime request
`goal-cont-00000000000000000001-a9d52eddbae78403903c3b1098c305ad` to run.
Its only tools were `get_goal` and `update_goal`, both completed. The canonical
goal became complete, and stock rendered `GOAL_RESUME_OK`, the completed goal,
and the ended turn. A raw rendering fixture without `continuation_sequence`
is not a valid runtime-created goal and must not be used to test admission.

This check exposed an unresolved accounting gap: after completion, the stored
Goal still reports 14,315 tokens. The continuation's three completed physical
InferenceCalls additionally report 7,281+105, 7,616+55, and 7,690+66 tokens
(22,813 total). The current projector faithfully displays the stored counter,
so final goal accounting is not yet verified. The runtime goal source refreshes
usage for active/budget-limited candidates, not completed goals. Resolve the
accounting owner/read projection before declaring the overall audit complete;
do not introduce observer-side database writes to mask stale materialization.

The next local correction uses the existing read-only runtime
`goal::session_token_usage` calculation for both the panel observation and
`/goal status`. Goal fields are changed only in the in-memory projection input,
never persisted by observation. The delivery comparison includes this derived
usage, so a late InferenceCall update can refresh a completed panel even when
the Goal row has not changed. A DB regression covers completion, late usage,
status output, and an unchanged persisted Goal snapshot. Verification of this
correction is pending; it adds runtime accounting reads to goal observation.

The correction now passed 319 shim-module tests plus 5 serve/shim flag tests,
the workspace all-target check, and the binary build. Live socket validation
on the completed session reports 37,128 tokens through both GoalUpdated and
`/goal status`, matching the inference ledger. The before/after persisted Goal
snapshot remains unchanged at its 14,315-token scheduler checkpoint. Stock
Grok 1.0.13 dashboard resume displays **Goal: Done, 37.1k/100k tokens**.
The clear/recreate wire probe also passed again on fresh session
`grok-edge-bba19f2ea7b046a7` with ledger-derived usage.

That rendered replay exposed a separate unresolved timing defect: the brief
completed goal continuation displays **Thought for 13m5s** when resumed about
13 minutes later. Investigate historical event timestamps versus live arrival
fallback in `send_projection_event`/`RequestUpdateTiming`; do not treat this
as verified replay fidelity or a model that actually reasoned for 13 minutes.

The replay follow-up carries the response's persisted terminal timestamp into
retained-tail projection, rather than substituting reconnect time. Wire
inspection also reproduced a duplicate `GOAL_RESUME_OK`: locally ambiguous
prefix matches discarded an otherwise unique ordered assignment across the
complete transcript. The matcher now compares earliest/latest feasible
strictly increasing assignments; it binds only when they coincide. Exhaustive
three-segment/four-row cases fence ambiguity and inversion behavior.
All 327 shim tests passed for these changes; workspace/build/live checks remain
pending. `scripts/grok_replay_probe.py` is a read-only regression that compares
replayed assistant text with persisted rows and rejects timestamps after the
request's durable completion. It reproduces the old server's timestamp failure.

The workspace check and build subsequently passed. The replay probe now passes
against the rebuilt server for the same goal continuation: 279 assistant
characters exactly match its persisted messages, and every historical chunk
timestamp is at or before `2026-09-05T07:05:01.547144+00:00`. Unchanged Grok
1.0.13 dashboard resume shows **Thought for 1.1s** before `get_goal`, followed
by the two actual model text segments (before and after `update_goal`), not
the extra retained-history copy. Goal usage remains 37.1k/100k. The two real
segments both begin `GOAL_RESUME_OK`; suppressing either would lose model text.

Commits `676ef953` and `f5c4c65a` are now published on PR #1363. CI run
`33951867228` is checking that tip; the accounting correction above is not yet
included in it.

The review otherwise verified the native DTO, scoping, runtime transition
ownership, retry cursor, and transactional clear. Nonblocking observations:
host-command exchanges are ephemeral (not inserted into model history), the
200 ms goal observation cadence adds one read per attached session, and a
concurrent runtime transition can still turn a successful preflight into a
typed runtime error. These are not reasons to write a second transcript,
transition owner, or synthetic goal generation registry in the shim.

The legacy-accounting recommendation needs more than omitting numeric fields:
stock `acp_types.rs::ContextInfo` defaults missing fields to zero, and
`SessionInfoData` requires numeric `turns` plus a non-optional context object.
The context renderer prints those numbers directly. It does not interpret the
shim's `gents/partialContext` metadata as "unknown." Do not turn missing
accounting into a successful zero-usage/zero-turn display just to satisfy a
review suggestion. Known component estimates can still be partial; completely
missing observations need an honest unavailable path or canonical recovery.

These follow-up changes still require their own verification and published-tip
CI. The overall goal remains open.

| Surface | Current evidence | Completion evidence required |
| --- | --- | --- |
| Session resume | Stock dashboard replay/continuation passed; expanded cancelled child bash and its result now render | Maintain regression coverage for read-only replay and active handoff; final review pending |
| History picker/search | Authorized search/pagination and stock F3 listing passed; native dashboard attach passed | CLI/F3 selection still has a stock local-file preflight for database-only sessions; use the native leader dashboard |
| Context usage | Live capture-comparison probe and rendered counter/breakdowns passed; stale/decreasing context regressions pass | Final review pending; unsupported provider breakdowns remain explicitly partial |
| Cumulative token accounting | Physical parent/child aggregation, retry deduplication, duration, native billing absence, and rendered usage verified | Reasoning/cache-creation/per-model pricing are not persisted and cannot be recovered as exact breakdowns; final review pending |
| Other native controls/data | Goal panel/status/clear/resume-to-budget-limit verified in stock TUI; model/mode and unsupported-operation inventory below | Final goal-control review and publication gates remain; do not claim unsupported runtime operations exist |
| Final gate | Runtime: 2,711 passed, 3 ignored (unchanged by follow-up); shim: 324 passed; workspace passed; stock live goal and earlier background/accounting checks passed | Focused goal review, remaining review dispositions, and green CI on the final published tip remain required |

### Native capability boundaries

- Background bash and subagents: runtime-owned output, lifecycle, cancellation,
  and notifications feed native tool/pane updates; live checks are documented
  in `grok-background-projection.md`. Data already discarded by the runtime's
  output-retention limit cannot be reconstructed by the shim.
- History/resume: native leader dashboard attachment and replay work. The stock
  local-file history preflight is not a database session loader; no synthetic
  local transcript files or client patches are created to bypass it.
- Models: the catalog describes the behavior's bound model. `session/set_model`
  accepts that model and rejects unsupported model/effort overrides. A visual
  picker change must not pretend to reconfigure provider input.
- Modes: `session/set_mode` round-trips the pager's client mode metadata; runtime
  principal/tool permissions remain the enforcement owner.
- Context/compaction: persisted inference accounting, captured breakdowns, and
  compaction records are projected into context/session-info. Manual
  `x.ai/compact_conversation` is explicitly unsupported; this shim does not own
  an operator-triggered compaction transition. Automatic runtime compaction
  remains visible through its persisted observations.
- Interjection: `x.ai/interject` is explicitly unsupported. Existing queued
  steering and background wakeups are delivered by their runtime owners;
  writing a detached message would not implement in-turn provider injection.
- Goals: project persisted goals and route management commands through the
  existing runtime APIs. The user-test follow-up also routes `/goal <objective>`
  through atomic goal-backed request admission, with optional trailing
  `--budget <tokens>`. There is no stock worker/verifier orchestrator in the shim.
- Todos/workflows/schedules: do not fabricate corresponding stock UI state from
  unrelated Goal records or generic tool names. Only persisted events with a
  supported semantic mapping are eligible for projection.
- Billing: native billing absence is returned. Dollar prices, per-model spend,
  reasoning-token breakdowns, and cache-creation counters are not invented when
  no corresponding persisted accounting exists.

### Performance review disposition still to close

History listing is now a single bounded-page scan retaining one summary per
session; the roster no longer repeats that scan per picker page. It is still
linear in history size. A persisted session-activity index belongs in the
runtime and needs separate design, not a shim-side cache or silent history cap.
Likewise, retiring terminal replay cursors without a durable late-event boundary
would lose the background notices this work explicitly fixes. The current
observer limits request delivery to 32 per sweep tick and retains send cursors;
any optimization must preserve late append visibility. These are performance
follow-ups, not permission to drop old sessions or terminal-request events.
Repeated context-owner reads and the small duplicated occupancy expression are
remaining cleanup findings; their exact-scope conservative behavior must remain
intact when consolidated. Final review disposition is still required.

## Ownership and implementation constraints

- Resume replays persisted observations; it must not regenerate a conversation,
  create replacement requests, or invent runtime completion transitions.
- Reuse the existing projection leaves and send-success cursors. Fence the
  replay/live handoff so active requests and late background events are not
  duplicated, missed, or terminalized ahead of their canonical owners.
- Apply exact session/principal/requester authorization before exposing any
  history, child edges, usage, or controls. The existing runtime history helper
  is principal-scoped, so it is not by itself a requester-scoped UI boundary.
- AgentSession has no persisted cwd/model/usage fields. Do not report the
  current client directory as historical execution evidence or invent session
  settings that do not affect the runtime.
- InferenceCall has no session_id. Resolve physically owned requests first;
  use its `context_accounting_json` and provider usage through the existing
  accounting owner. Avoid an unrelated agent-wide latest-call estimate.
- ContextAccounting is captured from the exact RenderedRequest before
  provider dispatch. The runtime `context_budget` and `session_history`
  modules already parse/account these records; extend/reuse those seams.
- Changes to legal runtime transitions or provider input start in Lean, then
  conformance tests, then implementation. Read-only UI replay is not a new
  model-execution lifecycle.

## Source map

- Gents: `grok_shim/acp.rs`, `projection.rs`, `turn.rs`;
  `gents/src/toolset/session_history.rs`, `context_budget.rs`;
  protocol `InferenceCall` schema and runtime AgentSession schema.
- Stock client: `app/effects/mod.rs` (LoadSession, FetchSessionList),
  `app/effects/helpers.rs` (picker DTO parsing), `app/session_startup.rs`,
  `app/acp_handler/mod.rs` (context usage and replay/live delivery).

The next implementation slice is persisted session observation/accounting
with requester scoping, followed by list/load replay and real stock-client
resume verification. The complete objective remains all rows above, not just
the already verified background baseline.

## Accounting implementation progress

`toolset::load_session_inference_observation` now supplies exact
session/principal/requester-scoped usage and latest context. It joins inference
calls by physical request document identity, batches request predicates, and
reuses the existing usage aggregation. Compaction calls contribute to usage
but cannot replace the conversation's inference context. Missing requester
means exact null identity, not unrestricted access.

All 9 session-history tests passed, including new foreign-requester/physical
ownership and context-decrease-after-compaction regressions. The bounded
request observation API is now wired into live metadata, ordered by persisted
dispatch timestamp/sequence/identity. A newer inference can lower context;
polling an older request cannot replace it. Obsolete per-request cumulative
token cursor plumbing has been removed.

All 308 shim regressions passed before that final plumbing cleanup. The live
`grok_context_probe.py` passed on session `grok-edge-730ed1622a554e1a`:
calls `9bff3176-734d-401f-91e5-5fd18f8009e8` and
`64760abb-ce79-4eee-b95e-8303b317e7f9` projected 7,854 and 7,891 context
tokens, respectively, exactly matching persisted accounting plus completion
tokens. Each turn generated only 6 tokens, demonstrating the distinction
from cumulative generated-token spend. This is wire verification, not yet a
resume check. A subsequent unchanged Grok 1.0.13 fullscreen test displayed
`STOCK_CONTEXT_OK` and the native `7.86k` counter in session
`123b5940-5e14-4adf-918b-84f5b132d741`. Its exit banner offered `--resume`,
which reinforces why session/load must be implemented rather than left as a
shaped stub. Full package/workspace gates and the remaining requirements
above are still open.

The resume prerequisite audit also found root session discovery scoped only
by session ID. It now requires the bound principal as both agent and requester
(matching root submission), with a regression covering foreign agent,
foreign requester, and missing requester rows. Child session discovery keeps
its separately authorized child identity. This is a read-only scope fix, not
a new runtime lifecycle.

## Session history and resume progress

- `sessions.rs` reads exact principal/requester-scoped request pages, checks
  persisted session owners/behaviors in batches, and derives history from
  those rows. It provides search, activity ordering, and keyset page cursors;
  no current cwd/model is invented as historical metadata. Child sessions
  remain under their parent. New AgentSession rows now persist requester_did;
  older null-requester sessions require actual authorized request history.
- `session/load` uses the existing projection and its send-success cursors,
  marking historical deliveries with isReplay. Live observation starts after
  the load response, from the attachment time captured before history reads.
  Runtime execution and session lifecycle are not restarted or rewritten.
- 312 shim tests passed before the subsequent active-handoff test and roster
  additions. The 70-message replay regression covers multiple DB pages,
  one prompt echo, and no new requests. All 9 scoped accounting tests pass.
- After restarting the server, `grok_resume_probe.py` loaded persisted session
  `123b5940-5e14-4adf-918b-84f5b132d741`: request count remained 1 during
  load, and exactly one new live continuation recalled `STOCK_CONTEXT_OK`.
- Actual stock 1.0.13 F3 lists/searches server history. Its selection path,
  like CLI --resume, requires local Grok session files before ACP; selecting
  a database-only session reports "Session not found locally". This is not
  a successful stock resume check. No client files have been fabricated and
  no client source has been changed to bypass it.
- The stock leader dashboard has a distinct roster attach path that directly
  issues session/load. `x.ai/sessions/list` now projects that native shape;
  actual rendered attach verification is next. This is the intended native
  server-hosted path, not relabeling coding sessions as cloud chat entries.
- The active-handoff regression found that fractional attachment timestamps
  exclude subsequently created requests within the same second because
  request submission persists whole seconds. Discovery now includes the
  full attachment second; existing cursors deduplicate already replayed rows.
  All 313 shim tests passed, including that active-handoff regression
  (`/tmp/grok-roster-tests-fixed.log`).

### Stock dashboard evidence and usage followup

The unchanged Grok 1.0.13 dashboard (`Ctrl+\\`, expand Inactive, select a
roster row) attached to `123b5940-5e14-4adf-918b-84f5b132d741` and displayed
replayed content. A subsequent live prompt returned `DASHBOARD_RESUME_OK`
in that same session. Native history fidelity still needs the tool/child
resume cases and a repeat rendered check after preserving original human
prompt IDs: the stock client hides the `notifications-*` family, so assigning
that family to every historical request hid human prompt text. The fix
preserves persisted promptId for replay while keeping the existing live
observation/cancellation identity for a resumed active request. All 313 shim
tests passed after that change (`/tmp/grok-resume-origin-tests.log`).

`x.ai/session/usage` is now wired to the shared runtime accounting aggregation,
plus physically authorized descendant traversal. Each exact child session
scope is counted once even if multiple edges reach it. Open/missing/unreadable
usage is marked incomplete. Costs are absent and marked partial, never
assumed free; unavailable model/duration/reasoning/cache-creation breakdowns
are identified in metadata. Distinct main-loop rounds exclude compaction and
retry attempts using persisted dispatch coordinates. This implementation
now passes embedded physical-lineage coverage: parent and child usage are
included despite aliased logical request IDs, while a foreign requester's
usage is excluded. All 314 shim tests passed in
`/tmp/grok-info-usage-regressions.log`. Live stock-panel verification remains
outstanding.

`x.ai/session/info` now projects the native nested response envelope, current
context accounting categories, main-loop turn count, and physically scoped
compaction counts. Breakdown fields absent from runtime accounting remain
explicitly partial rather than invented. The whole-workspace check passed
in `/tmp/grok-hydration-workspace-check.log`; subsequent accounting hardening
still requires a fresh gate. Turn counting now uses physical request identity
and marks unknown call kinds/ownership unavailable. A new regression covers
retry deduplication, separate physical requests sharing a logical alias, and
compaction exclusion (`/tmp/grok-accounting-physical-rounds.log`, pending).

Remaining: stock dashboard resume including tool/child histories, the local
history-picker limitation assessment, cumulative usage/session-info and the
broader native-control inventory, and full runtime/workspace/final-PR CI gates.

### Latest live panel evidence and remaining fidelity gaps

- Full runtime gate completed: 2,707 passed, 3 ignored, no failures
  (`/tmp/grok-hydration-full-runtime.log`). The physical-round accounting
  slice subsequently passed all 10 tests; it still needs rerunning after
  explicitly excluding the runtime's `oneoff` calls alongside compaction.
- Extended live context probe passed on `grok-edge-13d16b43358448fe`:
  context 7,854 then 7,891; cumulative input 14,298, output 18, total 14,316,
  three billable calls including one `oneoff` call, two inference rounds.
  Every wire total was compared against physically owned database rows.
- Stock 1.0.13 dashboard replay now visibly includes both human prompts and
  answers. `/session-info` renders the model/backend, turn index, and context;
  its Context usage and Usage limit tabs display the same persisted data.
  Direct `/usage` opened a client subscription question; the session-info
  modal's tabs expose the actual accounting without client modifications.
- Rendered fidelity is NOT complete: stock ignores our custom partial-field
  metadata and displays absent system-token/tool-count/reasoning/duration
  fields as zero. The exact `RenderedRequest.request_json` capture exists and
  should supply supported context details through a runtime observation
  owner. Existing inference telemetry should supply available durations.
  Do not treat passing wire totals as proof these breakdowns are correct.
- The usage tab also tries `x.ai/billing`, currently method-not-found. Audit
  the native local-provider billing shape rather than inventing cloud credits.
- Stock dashboard roster polling is already built into its event loop while
  the dashboard is open; no additional push-state owner is necessary.

### Duration and local billing implementation

The next accounting owner extension aggregates `InferenceCall.started_at` to
`ended_at` durations, excluding queue time. Missing, reversed, invalid, or
overflowing intervals remain unknown rather than known zero. The shim sums
available durations across its existing authorized scopes and marks partial
duration explicitly. Tests cover closed intervals, missing/reversed timestamps,
empty histories, and the native duration field. These additions are not yet
live-verified; `/tmp/grok-billing-duration-shim-tests.log` is the new gate.

`x.ai/billing` now returns the native nullable config envelope with no cloud
balance or subscription and on-demand billing disabled. This matches the
stock modal's "No billing data available" path; it does not fabricate a
credit allowance or claim local inference is free. A route regression is
included. Rendered verification after rebuilding remains required.

The preceding shim suite completed with 315 passing tests. Its result does
not cover these newest duration/billing changes or the still-missing detailed
context hydration. The shared live probe now expects loadSession=true.

### Captured context details (implementation, verification pending)

`toolset::load_session_context_details` now resolves the exact authorized
physical request and its inference turn/attempt capture, then checks the
provenance admission call ID. Only numeric details leave this helper. It
derives OpenAI chat system/developer estimates and actual message/tool counts,
using the same runtime JSON byte estimator and preserving the total message
partition. Unsupported provider shapes, document-injected partitions, missing
captures, and ambiguous ownership remain unavailable rather than guessed.
The shim wires those fields into session-info. A pure decomposition test and
extended live capture-comparison probe were added; embedded authorization
coverage and actual rendered verification remain outstanding.

Current checks: `/tmp/grok-context-details-check.log` and
`/tmp/grok-context-details-runtime-tests.log`. The preceding accounting slice
passed 10 tests; that result predates the new capture helper and duration test.

The capture-owner regression has now been added to the embedded session
history fixture. It verifies foreign principal/requester/session rejection,
admission-call mismatch rejection, and physical ownership despite a misleading
capture request alias. Current runs are `/tmp/grok-context-ownership-tests.log`
and `/tmp/grok-context-decomposition-tests.log`; results remain pending.
The new dev build is `/tmp/grok-context-details-live-build.log`.

Latest remote check: fetching main still leaves zero main commits missing
from this branch. PR #1363 remains clean and its existing a8a9d5bd tip has all
11 checks passing. That is baseline evidence only; none of the current dirty
hydration changes have been pushed or covered by final-tip CI yet.

The full session-history slice now passes 13 tests, including captured-context
ownership/admission rejection and numeric decomposition
(`/tmp/grok-context-ownership-tests.log`). The dev binary is rebuilding with
the capture helper. Fresh broad gates are queued for this source state:
`/tmp/grok-hydration-final-workspace.log`,
`/tmp/grok-hydration-final-runtime.log`, and
`/tmp/grok-hydration-final-shim.log`. These are pending, not green evidence.

### Updated binary: live and rendered verification

The expanded probe passed on `grok-edge-7f5059b3e1804437`
(`/tmp/grok-context-details-live.log`): context 7,854 then 7,891; final usage
14,296 input + 19 output = 14,315 total; three calls, two inference rounds,
1,635 ms provider duration. Counts matched captured provider bodies, usage
matched physical inference rows, duration matched timestamps, and billing
returned its native no-data shape. The final workspace check also passed.

Unchanged Grok 1.0.13 dashboard resumed that session. Its context panel now
shows 480 system tokens and 21 tool definitions; session usage shows 14,315
tokens and 1.6s API time. The billing panel displays "No billing data
available", without the prior unsupported-method error.

The same client also resumed old child session parent
`0d5413ba-3ebc-490c-b3cd-366d8a7aa3cf`: original human prompt, subagent call,
parent answer, and later interruption acknowledgement rendered from history.
Detailed expanded child-output inspection remains pending; do not infer it
from the parent acknowledgement. Final runtime/shim gates are still running.

Expanded child inspection found a wire defect: the cancelled bash call was
persisted and emitted on the correct child session, but its tool `content`
used bare `{type:"text",text:...}` instead of ACP ToolCallContent's
`{type:"content",content:{type:"text",text:...}}` envelope. The unchanged
client displayed the child's reasoning but not that tool. The shared result
projection is now corrected, with golden coverage and the live tool-update
expectation updated. Retest the rendered child before claiming the cause
fully resolved. Builds/tests: `/tmp/grok-tool-content-build.log` and
`/tmp/grok-tool-content-shim-tests.log`. The preceding shim gate passed 316
tests but did not validate this envelope with the stock decoder.
Reference: https://github.com/agentclientprotocol/agent-client-protocol/blob/main/schema/v1/schema.json

The corrected binary was rechecked in unchanged Grok 1.0.13: expanding the
same resumed child now shows `Run sleep 180`, and opening that tool displays
`tool call cancelled`. This confirms the missing child tool was the content
envelope defect, not missing database state or a client modification.
The workspace gate passed again (`/tmp/grok-tool-content-workspace.log`).
The full runtime gate completed with 2,711 passed and 3 ignored. The shim
rerun found one old test reading the former `/text` path; it now asserts the
correct `/content/text` path and is rerunning in
`/tmp/grok-tool-content-final-tests.log` (not yet passed).
