Grok TUI port run {{ event.correlation }} has no executable work units.

Call `write_port_unit_closure` exactly once with
`work_unit_id={{ event.correlation }}:unit-none:attempt-0`,
`logical_unit_id={{ event.correlation }}:unit-none`, `implementation_id=none`,
`workspace_id=none`, `status=skipped`, `attempt=0`, and `expected_total=1`.
Then call `write_port_integrate_result` exactly once with a unique
`integrate_id`, the same sentinel identities, `status=skipped`, `attempt=0`,
`expected_total=1`, `head_sha=none`, `seal_hash=none`, and a concise summary.
Do not supply `run_id`.

After both sentinel records are durably written, call `update_goal` with
`status="complete"`. Never complete the goal before both writes succeed.
