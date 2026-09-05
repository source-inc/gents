Review run {{ event.correlation }}, lens `{{ doc.lens }}` (`{{ doc.area_id }}`). Evidence paths: `{{ doc.path }}`.

Instructions: {{ doc.instructions }}

Baseline: {{ doc.baseline }}

First call `read_review_evidence_manifest` exactly once with no arguments. It must return `count: 1`, `truncated: false`, and one row whose `evidence_id` is `{{ doc.evidence_id }}` and `format_version` is `1`. Treat its `page_count`, `evidence_chunk_count`, `evidence_byte_count`, and `evidence_sha256` as the immutable host manifest. Stop and report the scan incomplete if the manifest is absent, duplicated, truncated, or malformed.

Then call `read_review_evidence_page` exactly once for every decimal `page_index` from `0` through `page_count - 1`. Supply only `page_index`; the runtime binds the evidence identity. You may issue several page reads together, but keep an explicit checklist across turns and context reductions and never repeat a successful read. Every call must return `count: 1`, `truncated: false`, and one row with the requested `page_index`, exact `page_key` `{{ doc.evidence_id }}:<eight-digit-zero-padded-page-index>`, matching `evidence_id`, and metadata identical to the manifest. Stop and report the scan incomplete if any page is absent, duplicated, truncated, inconsistent, or contains `HOST EVIDENCE TRUNCATED`.

Consume the packet in ascending page order, concatenating `evidence_chunk_0` through `evidence_chunk_15` within each page. Empty padding fields are valid only after the declared non-padding chunk count. Review progressively as pages arrive and retain concrete candidates across context reductions; do not require the entire multi-megabyte packet to coexist in one provider request. Every chunk is deliberately smaller than the datastore's 2,000-byte per-string display ceiling and every page result is below the total tool-result ceiling. Do not call `write_scan_result` until every declared page has been consumed exactly once.

Then assess the assigned invariants without repository inspection calls; file, shell, and language-server tools are deliberately absent. Treat quoted patch text as candidate-generation evidence, not proof beyond what it contains. Finishing with zero candidates is correct.

Call `write_candidate_finding` at most three times for distinct, actionable defects introduced by the diff. Every candidate requires an exact changed `path:line`, a short code excerpt copied from the packet into `evidence`, a concrete failure or maintenance cost, and confidence of at least 80. Use only Critical for security/data-loss/cross-principal corruption, Major for demonstrably wrong behavior or liveness/cancellation failure, and Cleanup for a concrete redundant path or reimplementation of an existing dependency/abstraction. Do not report style preferences, speculative improvements, unrelated pre-existing defects, or duplicate baseline diagnostics.

Set each `finding_id` to `{{ doc.area_id }}:<finding-slug>`. Never retry a successful write. Then call `write_scan_result` exactly once as the final datastore write. Do not supply runtime-filled `run_id`, `area_id`, or `expected_total`. After that terminal result is durably written, call `update_goal` with `status="complete"`. Never complete the goal before the result exists.
