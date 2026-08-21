Defense run {{ group.correlation_value }} has {{ group.count }} durable
verification completions (complete={{ group.complete }}):

{{ group.docs }}

Use `read_defense_candidate` and `read_defense_verdict` to load the bounded
candidate and verdict ledgers. A
`:no-candidates` completion has no corresponding candidate or verdict; it is
the empty-set sentinel. Every completion must carry the same
`scan_ledger_status`; copy that exact value to the triage summary. If completion
statuses disagree, record a `count_mismatch:` value naming the disagreement.

For a non-empty candidate ledger, compare the candidate and verdict identity
sets by `finding_id`. A verdict is promotable only when
`verdict=confirmed`. Verifiers do not dedupe; root-cause clustering is the sole
consequence-collapse stage. For each promotable verdict call
`write_defending_finding` with every adjudicated field, including attacker
identity and control, control source, entry point, sink, privileges, guard and
fail-closed behavior, impact, and `verdict=confirmed`. Do not perform source
verification yourself.

Finally call `write_defense_triage_summary` exactly once as the last write.
Use these exact formulas over real verdict rows:

- `confirmed_count = count(verdict == "confirmed")`
- `refuted_count = count(verdict == "refuted")`
- `duplicate_count = count(duplicate_of != "" && duplicate_of != "none")` and
  must be zero for this pipeline version
- `candidate_count = confirmed_count + refuted_count`
- `promoted_count = count(DefendingFinding writes)` and it must equal
  `confirmed_count`

If the identity sets or counts disagree, still close the stage: promote only
confirmed verdict rows with an exact candidate identity, prefix
`scan_ledger_status` with `count_mismatch:`, and name every missing or extra id
in `summary`. The completion sentinel is absent from both candidate and verdict ledgers and
every count. Do not subtract duplicates from `refuted_count`. Do not supply
runtime-filled `run_id` or `repository_path`. Never retry successful writes or
call subagent tools.
