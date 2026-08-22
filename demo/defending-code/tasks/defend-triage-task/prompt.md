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
sets by `finding_id`. Require exact agreement on `finding_id`, `area_id`,
`source_revision`, and `source_tree_state` before joining a pair. A verdict is
promotable only when that provenance agrees, `verdict=confirmed`,
`adjudicated_claim_kind=vulnerability`, and `severity=HIGH|MEDIUM|LOW`.
Verifiers do not dedupe; root-cause clustering is the sole consequence-collapse stage.
For each promotable verdict call
`write_defending_finding` by joining the exact candidate and verdict with that
`finding_id`. Preserve the candidate's `root_cause_key`, `category`, path/line,
title/description/exploit scenario, recommendation, and `threat_ids`. Set final
`claim_kind` from the verdict's `adjudicated_claim_kind`, and use the verdict's
provenance, security boundary, exploitability gates, impact, contract surface, `severity`, `confidence`, `evidence`,
`verification`, `preconditions`, and `access_level`; never promote the
candidate's `claimed_severity`. Set `verdict=confirmed` and derive a concise
`owner_hint` from the affected component/path. Do not perform source
verification yourself or rewrite either stage's evidence.

Finally call `write_defense_triage_summary` exactly once as the last write.
Use these exact formulas over the durable candidate/verdict join:

- `candidate_count = count(candidate rows)`
- `confirmed_count = count(verdict == "confirmed")`
- `refuted_count = count(verdict == "refuted")`
- `duplicate_count = 0`; only the later root-cause stage collapses consequences
- `eligible_confirmed_count = count(confirmed vulnerability verdicts with
  HIGH|MEDIUM|LOW severity, an exact candidate identity, and matching provenance)`
- `promoted_count = count(DefendingFinding writes)` and it must equal
  `eligible_confirmed_count`

`candidate_count` must equal `confirmed_count + refuted_count` for a consistent
ledger; preserve the real counts when it does not.

If the identity sets, provenance, or counts disagree, still close the stage:
promote only otherwise-eligible confirmed verdicts satisfying every criterion
above, prefix
`scan_ledger_status` with `count_mismatch:`, and name every missing or extra id
or provenance disagreement in `summary`. The completion sentinel is absent
from both candidate and verdict ledgers and every count. Do not subtract
duplicates from `refuted_count`. Do not supply runtime-filled `run_id` or
`repository_path`. Never retry successful writes or call subagent tools.
