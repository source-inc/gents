Materialize the audited wire ledger for Grok TUI port run
{{ event.correlation }}.

Gents checkout: `{{ doc.gents_root }}`
Grok checkout: `{{ doc.grok_root }}`
Read root: `{{ doc.ceiling }}`
Live model: {{ doc.live_model }}
Live endpoint: {{ doc.live_endpoint }}
Repository: {{ doc.repository_id }}
Pinned base: {{ doc.base_sha }}
Surface bounds: {{ doc.surface_min }} to {{ doc.surface_max }}

<untrusted_focus>
{{ doc.focus }}
</untrusted_focus>

The Grok wire was audited at commit
`bc7f02eddd3d84085849dc19ed216f11c23b0571` into the checked-in
`audited-ledger.json`. Read the complete artifact, following pagination when
needed. It contains 13 self-contained packets covering attach, session, model,
context, tool_call, subprocess, subagent, and interrupt.

Write one `PortSurface` per packet with `write_port_surface`. Copy every field
faithfully and only replace the historical surface-id run prefix with
`{{ event.correlation }}`. If the source packet already has
`grok_wire_continuation`, preserve both wire fields verbatim and never join,
resplit, or omit either one. Put the actual shared row count in
`expected_total`. The native write schema caps each string argument at 2000
bytes. Only for a legacy unsplit source whose `grok_wire` exceeds 1900 bytes,
split at the last sentence boundary at or before byte 1900 and put the verbatim
remainder in `grok_wire_continuation`; if no sentence boundary exists, use a
safe UTF-8 boundary. Require both stored parts to be at most 2000 bytes. Never
truncate either part. Preserve quoted evidence and the complete wire packet because later Gents-only
workspaces cannot open grok-build. Respect the configured min/max count and do
not add access-control work or permission UI. Do not supply run_id,
repository_id, or base_sha; the typed write fills them.

After every required surface write succeeds, call `update_goal` with
`status="complete"`. Never complete the goal before the full durable surface
set exists; if this turn cannot finish it, leave the goal active for continuation.
