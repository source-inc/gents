Host integration succeeded for Grok port run {{ event.correlation }},
attempt-specific work unit `{{ doc.work_unit_id }}`, workspace
`{{ doc.workspace_id }}`, seal `{{ doc.seal_hash }}`, and resulting head
`{{ doc.head_sha }}`.

Call `read_port_work_unit` and `read_port_implementation`. Select the exact
rows whose `work_unit_id` equals `{{ doc.work_unit_id }}` and require their
logical ids, attempts, and expected totals to agree. Then call
`write_port_integrate_result` exactly once with
`integrate_id={{ doc.receipt_id }}`, that logical unit id and attempt,
`status=applied`, the copied expected total, and a concise summary. The runtime
fills run, attempt-specific work unit, workspace, head, and seal fields. Do not
supply those runtime-filled fields.

After the integration result is durably written, call `update_goal` with
`status="complete"`. Never complete the goal before the receipt-backed record exists.
