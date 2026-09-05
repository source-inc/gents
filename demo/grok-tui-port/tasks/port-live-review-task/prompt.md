Run {{ event.correlation }} closed its live probe ledger.

{{ group.docs }}

Call `read_grok_port_job`, `read_port_final_review_report`,
`read_port_surface`, `read_port_live_result`, and
`read_port_live_environment_proof` for the complete ledgers. Require exactly
one environment proof. Fail closed unless its head equals the reviewed head;
its expected and applied endpoints both equal the job `live_endpoint`; its
backend is exactly `grok-port-backend-ws1`; its endpoint variable is exactly
`GENTS_GROK_PORT_ENDPOINT_1`; its fresh preflight nonce is 32 lowercase
hexadecimal characters and both preflight booleans are true; its endpoint and
listener booleans are true; its live-home/GraphQL/socket fields equal the job
values; its PID is positive and listener command names that exact run-owned
home, HTTP port, socket, and Gents server; its PTY expected answer is exactly
`GENTS_STOCK_` followed by the 24-character lowercase hexadecimal challenge;
its prompt SHA-256 is 64
lowercase hexadecimal characters; its pre-submit byte count is nonnegative;
its second expected answer uses the same prefix and its distinct 24-character
lowercase hexadecimal idle challenge; its two
terminal request IDs are non-empty and distinct and share one non-empty stock
session; its PTY
idle/verified booleans are true; and both cleanup booleans are true. Require
both proof strings to be non-empty and at most 2,000 bytes, concatenate
`proof_json` plus `proof_json_continuation`, parse the result, and require every
duplicated value to equal the structured field.
Missing, duplicate, malformed, or inconsistent global proof increments
`failed_count` and makes `coverage_complete=false`, independently of the
surface rows.

Build the exact set of
surface IDs whose verdict is `implement` or `shaped-stub`. Reject duplicate,
missing, or extra probe surface IDs; a green review with zero non-ignore
surfaces must have the single
blocked `surface_id=none` sentinel. Count a `passed`
row as failed when `grok_wire_observed` or `gents_docs_observed` is empty or
does not match that surface's `live_expect`.

Call `write_port_live_report` once with counts, `expected_count` from the
final review, actual `observed_count`, `coverage_complete=true` only for exact
unique coverage, and `final_review_head` from the report. Any coverage defect
must increment `failed_count`. Do not supply `run_id`.

After the live-review report is durably written, call `update_goal` with
`status="complete"`. Passing and blocked coverage reports both complete this
stage; never complete the goal before the terminal report exists.
