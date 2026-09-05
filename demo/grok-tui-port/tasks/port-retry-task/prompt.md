Repair rejected Grok port attempt `{{ doc.work_unit_id }}` for logical unit
`{{ doc.logical_unit_id }}` in run {{ event.correlation }}. The rejected
workspace `{{ doc.workspace_id }}` is sealed and must remain untouched.

Call `read_port_work_unit` and `read_port_review`. Select the exact prior work
unit and its exact blocked review. Fail closed rather than mixing attempts or
logical units.

Parse the prior numeric `attempt`, add one, and call `write_port_work_unit`
exactly once to create the new attempt:

- Keep the same `logical_unit_id`, surface ids, area, title, structured
  `owned_paths`, ownership instructions, compact surface-id evidence indexes,
  repository, pinned base,
  and expected total. The authoritative complete evidence stays on the mapped
  `PortSurface` rows, including `grok_wire_continuation`.
- Set `work_unit_id=<logical_unit_id>:attempt-<next>`, `attempt=<next>`,
  `status=ready`, and `verdict=implement`.
- Use a new Git-safe branch ending in `-attempt-<next>`; never reuse the sealed
  branch.
- Set `prior_work_unit_id` to `{{ doc.work_unit_id }}`.
- Put the exact review evidence and notes, including every `path:line`
  finding, in `repair_context`.
- Set `prior_diff` to `regenerate from pinned base and review evidence`; core
  workspace receipts remain runtime-owned and are not projected through this
  portable pack tool. The new writer regenerates the owned files from the
  immutable contract and exact findings.

Do not write a closure or integration result. The normal create-workspace,
writer, seal, and independent-review edges process the new attempt. Do not
supply `run_id` or `caused_by_correlation`.

After the replacement work unit is durably written, call `update_goal` with
`status="complete"`. Never complete the goal before that new attempt exists.
