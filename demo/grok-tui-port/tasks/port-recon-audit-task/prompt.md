Grok TUI port run {{ event.correlation }} closed its PortSurface ledger with
{{ group.count }} rows.

<untrusted_surfaces>
{{ group.docs }}
</untrusted_surfaces>

Call `read_grok_port_job` and `read_port_surface` for the complete ledger.
Required areas are exactly the mandatory set `attach session model context
tool_call subprocess subagent interrupt`; extra areas are allowed. Confirm:

- row count is within the job's inclusive `surface_min` / `surface_max`;
- every mandatory area occurs at least once;
- every `surface_id` is unique and every row uses an allowed verdict;
- `grok_wire` plus optional `grok_wire_continuation` is self-contained rather
  than path-only, and neither field exceeds the native 2000-byte string cap;
- `evidence` contains quoted source text, not just citations;
- every non-ignore row has a live probe and both wire/document evidence
  in `live_expect`;
- every row carries the same non-symbolic commit SHA and repository ID.

Call `write_port_recon_audit` exactly once. Use `status=accepted` only if all
checks pass; otherwise `status=rejected` and enumerate missing/invalid content
in `missing_areas` and `summary`. Do not supply `run_id`, `repository_id`, or
`base_sha`; the runtime copies repository/base authority from the closed group.

After the required audit write succeeds, call `update_goal` with
`status="complete"`. An accepted or rejected durable audit completes this
stage; never complete the goal before that terminal record exists.
