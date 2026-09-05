Run {{ event.correlation }} finished full combined review.

status={{ doc.status }} head={{ doc.head_sha }} findings={{ doc.confirmed_findings }}
implement_surfaces={{ doc.implement_surface_count }} expected_results={{ doc.live_result_count }}
<untrusted_review_summary>
{{ doc.summary }}
</untrusted_review_summary>

Call `read_grok_port_job`, `read_port_final_review_report`, and
`read_port_surface`. For each surface, interpret the complete wire packet as
`grok_wire` followed verbatim by optional `grok_wire_continuation`. If review
status is not `green`, write exactly one
`surface_id=none`, `status=blocked` result explaining the review block and
stop. Do not fabricate an environment proof on this path: the proof write is
deliberately not a trigger-scoped output obligation, and the grouped reviewer
will turn its absence into failed coverage before publication.

Require current HEAD to equal the report's exact `head_sha`. Use the job's
`gents_root`, `live_model`, `live_endpoint`, `live_home`, `live_graphql`, and
`live_socket`. Never reuse the orchestration home or its running process.
Build the integrated CLI, initialize the run-owned live home against the
declared model endpoint, and launch the integrated Grok leader/shim on the
run-owned GraphQL port and socket, explicitly binding Grok turns to the
`port-live` behavior. Verify that this bound behavior's effective model and
context window are `live_model` and 524288 before accepting the catalog
advertisement. Discover the exact launch flags from the integrated `--help`
and implementation; do not invent a protocol substitute.
Every `gents demo provision` or `gents config apply` invocation used to build
the live home must export
`GENTS_GROK_PORT_ENDPOINT_1="<job live_endpoint>"`. Never use the obsolete
`GENTS_GROK_PORT_ENDPOINT` name and never rely on the pack's localhost
fallback.

First run `demo/grok-tui-port/scripts/grok_edge_probe.py --edge offline` and
`demo/grok-tui-port/scripts/grok_stock_pty_probe.py self-test`; require both
embedded validator matrices to pass without a socket. Before starting the server, run
`grok_stock_pty_probe.py preflight` for `live_graphql` and `live_socket` and
save its fresh vacancy proof under the run directory.
Wait for both HTTP readiness and socket readiness. Record the PID of the
actual process listening on `live_graphql`, not a wrapper shell PID, verify it
belongs to the launched run-owned server, and clean up only that PID when
probes finish. Require the listener and socket to be gone afterward.
The checked-in probes preserve and report the job's exact `live_socket`
identity while bridging an over-limit Unix `connect(2)` pathname through a
short absolute alias in a private temporary directory; do not bypass that
portable-path handling in a deep worktree.

Immediately after readiness, run
`demo/grok-tui-port/scripts/grok_stock_pty_probe.py run` against the stock
`grok` binary and `live_socket`, passing the exact job endpoint,
backend ID `grok-port-backend-ws1`, run-owned live home, preflight JSON,
actual listener PID, GraphQL URL, and repository cwd, and save its structured
JSON under the run directory. Invoke this foreground command with the full
120-second tool timeout and pass `--ready-timeout 15 --timeout 90
--total-timeout 95`; do not wrap it in another timeout, pipe its output, or
otherwise mask its exit status. Require both a zero process exit and the saved
proof before continuing. The script queries the live-home
`InferenceBackend` itself and requires its endpoint to equal the job endpoint.
It launches stock interactive
`grok --leader --leader-socket <live_socket>` in a PTY, uses a fresh random
marker, then requires its exact echo in the correlated durable assistant
message. A second distinct challenge must complete in the same stock-client
session while the pager process remains alive, proving the client returned to
usable input. Terminal repaint bytes and local input echo are not model-output
evidence; the framed probe below owns exact ACP output-wire verification. `grok -p`
bypasses leader mode and is never evidence for this port.

Then run the edge probe against `live_socket` and `live_graphql`, passing
`--model live_model`, one edge at a time: handshake, prompt, tool, subprocess,
subagent, and cancel. The probe must pass its wire and document assertions,
including the subprocess command and output on both the Grok wire and the
persisted AgentToolCall. The subagent edge must also prove the foreground spawn
contract end to end: the parent's early standard-rail `task`-titled tool_call
carrying `_meta.subagentBackground: false` (the pager-local foreground wait
marker), the exact live extension-rail `x.ai/session_notification`
spawned/progress/finished lifecycle for the `port-live-worker` target, the
child-session identity, the exact `subagent_finished` DTO (snake_case variant
fields under the camelCase `sessionId/update/_meta` envelope: required
`subagent_id`, `child_session_id`, `status`, `tool_calls`, `turns`,
`duration_ms`, `tokens_used`, `will_wake` with `will_wake: false` for the
foreground child, optional `error`/`output`, never `parent_session_id`), the
terminal `tool_call_update` reusing the same toolCallId, and — via
`live_graphql` — the durable parent/child AgentRequest linkage
(`caused_by_parent_request_id`) plus the persisted child
AgentToolCall/AgentMessage rows for the spawned child session.

Map those observed edges to every surface whose verdict is `implement` or
`shaped-stub`. A shaped stub passes only when its explicit error/not-found
contract and absence of fabricated documents match `live_expect`. Do not treat
fixture replay, direct provider HTTP, or the outer orchestration server as a
pass. Query `live_graphql` for the correlated Gents documents required by each
`live_expect`.

If preflight, launch, stock PTY, or a global probe dependency fails, do not
invent a proof. Clean up the exact listener if it exists, then write one
truthful `blocked` or `failed` result for every non-ignore surface so the
grouped reviewer can close the ledger and fail publication.

After all probes, stop the script-verified actual listener PID. Run
`grok_stock_pty_probe.py cleanup` against the saved proof and require both the
listener/port and socket to be absent. Then call
`write_port_live_environment_proof` exactly once from the final structured
JSON, with `final_review_head` set to the reviewed HEAD and all proof booleans
true. Preserve the complete JSON under the datastore string ceiling by
splitting the compact JSON verbatim into exactly two non-empty strings, each
at most 2,000 UTF-8 bytes, stored in `proof_json` and
`proof_json_continuation`; concatenation must reproduce the original JSON
exactly. Write this singleton
before any surface result so the grouped reviewer cannot start without it.

Call `write_port_live_result` once per non-ignore surface (or one sentinel
`surface_id=none` / `status=blocked` if none exist). The runtime fills
`expected_total` from the reviewed ledger; do not supply or reinterpret it.
`status` is `passed` only when both `grok_wire_observed` and
`gents_docs_observed` satisfy `live_expect`. Do not supply `run_id`.

After every required live-result row (or the single permitted blocked sentinel)
is durably written, call `update_goal` with `status="complete"`. Never complete
the goal while required surface coverage remains unrecorded.
