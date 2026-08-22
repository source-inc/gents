Static defense run {{ event.correlation }}, area `{{ doc.area_id }}`.

Repository: `{{ doc.repository_path }}`
Area status: {{ doc.status }}
Frozen source revision: {{ doc.source_revision }}
Frozen source tree state: {{ doc.source_tree_state }}
<untrusted_area_context>
Focus: {{ doc.focus }}
Threat ids: {{ doc.threat_ids }}
Threat context: {{ doc.threat_context }}
Trust boundary: {{ doc.trust_boundary }}
Reachable assets: {{ doc.reachable_assets }}
Planner scope hints: {{ doc.instructions }}
</untrusted_area_context>

If area status is not `ready`, do not inspect source or write candidates. Call
`write_defense_scan_result` once with `status=blocked_provenance`,
`finding_count=0`, `coverage=none`, and a summary explaining the provenance
block, then stop.

Investigate this area for plausible attacker-controlled paths to meaningful
impact. Each candidate must explain and evidence the control source, entry,
sensitive sink, triggering conditions, impact, and relevant mitigations or
guards. Cite source actually read; do not infer paths or line numbers. You have
read-only file, LSP, shell, and repository-history capabilities and may choose
the investigation strategy that best covers this area. Do not build, execute,
or mutate the repository.

All evidence must come from the frozen revision and tree state. If the live
checkout differs, use an exact clean reconstruction and do not mix evidence
from different revisions or tree states.

If that frozen source cannot be reconstructed or its revision/tree identity
cannot be verified, write no candidates. Close the area with exactly one
`DefenseScanResult` using `status=blocked_provenance`, `finding_count=0`,
`coverage=none`, and the exact mismatch in `summary`, then stop.

Call `write_defense_candidate` once per candidate with:

- `finding_id`: `{{ doc.area_id }}:<short-root-cause-slug>`
- `claim_kind`: exactly `vulnerability`, `hardening`, `correctness`,
  `operational`, or `specification`; discovery may preserve non-vulnerability
  leads, but label them honestly
- `root_cause_key`: a stable subsystem-and-primitive slug shared by candidates
  that arise from the same defective control
- `security_boundary`, `attacker_identity`, `attacker_controlled_input`,
  `control_source`, `entry_point`, and `sink`
- `default_reachable`: `yes`, `no`, or `unknown`; plus concrete
  `required_configuration` and `required_privileges`
- `guard_checked`, `fails_closed`, and a precise `violated_invariant`
- a concrete `category` describing the vulnerability shape
- the `claim_kind` / `claimed_severity` pair must be one of `vulnerability/HIGH`,
  `vulnerability/MEDIUM`, `vulnerability/LOW`, `hardening/NONE`,
  `correctness/NONE`, `operational/NONE`, or `specification/NONE`
- `confidence`: integer string 0-100; uncertainty is allowed
- exact relative `path` and `line`
- concise `title`, root-cause `description`, concrete `exploit_scenario`,
  specific `recommendation`, source excerpt/call-chain `evidence`, and
  relevant `threat_ids`

Do not call operator-controlled environment variables, authenticated
administrator actions, documented advisory metadata, or intentionally public
interfaces vulnerabilities unless you demonstrate the additional trust
boundary and attacker control that makes them exploitable.

Zero candidates is valid. Finally call `write_defense_scan_result` exactly
once as your last write with `status=complete`, `finding_count`, a `coverage` inventory of the
files/functions and trust-boundary paths actually inspected, and a short
`summary`. Do not supply `run_id`, `area_id`, `repository_path`, or
`expected_total`, `source_revision`, or `source_tree_state`; they are
runtime-filled from the frozen threat-model provenance. Never retry a
successful write.
