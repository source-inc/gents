import { postGraphql } from "../live/graphql.ts";
import type { DefenseSnapshot } from "./types.ts";

type DefenseData = {
  DefendingCodeJob?: DefenseSnapshot["jobs"];
  DefenseThreatModel?: DefenseSnapshot["threats"];
  DefenseReviewArea?: DefenseSnapshot["areas"];
  DefenseScanResult?: DefenseSnapshot["scans"];
  DefenseCandidateFinding?: DefenseSnapshot["candidates"];
  DefenseFindingVerdict?: DefenseSnapshot["verdicts"];
  DefendingFinding?: DefenseSnapshot["findings"];
  DefenseTriageSummary?: DefenseSnapshot["triage"];
  DefensePatchAssignment?: DefenseSnapshot["assignments"];
  DefensePatchCandidate?: DefenseSnapshot["patches"];
  DefensePatchReview?: DefenseSnapshot["reviews"];
  DefenseReport?: DefenseSnapshot["reports"];
  AgentRequest?: DefenseSnapshot["requests"];
  InferenceCall?: DefenseSnapshot["calls"];
};

export async function loadDefenseSnapshot(): Promise<DefenseSnapshot> {
  const data = await postGraphql<DefenseData>(`{
    DefendingCodeJob { _docID run_id repository_path focus area_min area_max engagement_context }
    DefenseThreatModel {
      _docID run_id repository_path focus area_min area_max source_revision source_tree_state
      provenance_status system_context assets entry_points threats deprioritized open_questions
      mitigations provenance
    }
    DefenseReviewArea {
      _docID run_id area_id repository_path source_revision source_tree_state status focus
      threat_ids threat_context trust_boundary reachable_assets instructions expected_total
    }
    DefenseScanResult {
      _docID run_id area_id repository_path status expected_total finding_count coverage summary
    }
    DefenseCandidateFinding {
      _docID run_id finding_id area_id source_revision source_tree_state claim_kind root_cause_key
      security_boundary attacker_identity attacker_controlled_input control_source entry_point
      sink default_reachable required_configuration required_privileges guard_checked
      fails_closed violated_invariant category claimed_severity confidence path line title
      description exploit_scenario recommendation evidence threat_ids
    }
    DefenseFindingVerdict {
      _docID run_id finding_id area_id verdict source_revision source_tree_state claim_kind
      root_cause_key security_boundary attacker_identity attacker_controlled_input control_source
      entry_point sink attacker_control default_reachable required_configuration
      required_privileges guard_checked fails_closed violated_invariant impact severity confidence
      contract_surface category path line title description exploit_scenario recommendation
      evidence verification duplicate_of preconditions access_level owner_hint threat_ids
    }
    DefendingFinding {
      _docID run_id finding_id area_id source_revision source_tree_state claim_kind root_cause_key
      security_boundary attacker_identity attacker_controlled_input control_source entry_point sink
      attacker_control default_reachable required_configuration required_privileges guard_checked
      fails_closed violated_invariant impact category severity confidence path line title description
      exploit_scenario recommendation evidence verification preconditions access_level owner_hint
      threat_ids verdict
    }
    DefenseTriageSummary {
      _docID run_id scan_ledger_status candidate_count confirmed_count refuted_count
      duplicate_count promoted_count summary
    }
    DefensePatchAssignment {
      _docID run_id assignment_id cluster_id finding_id member_finding_ids contract_review_id
      contract_disposition skip_reason repository_path base_revision base_tree_state status
      expected_total
    }
    DefensePatchCandidate {
      _docID run_id patch_id cluster_id finding_id member_finding_ids contract_review_id
      contract_disposition status repository_path base_revision base_tree_state
      workspace_requirement path line category diff diff_sha256 rationale variants_checked bypass_considered
      test_note validation_plan expected_total
    }
    DefensePatchReview {
      _docID run_id patch_id cluster_id finding_id validation_id reviewed_base_revision
      reviewed_base_tree_state reviewed_diff_sha256 receipt_match verdict quality_status
      out_of_scope_hunks new_surface reason expected_total
    }
    DefenseReport {
      _docID run_id audit_status candidate_count confirmed_count refuted_count root_cause_count
      actionable_cluster_count patch_count mechanically_valid_patch_count
      maintainer_accepted_patch_count security_accepted_patch_count accepted_patch_count
      rejected_patch_count severity_counts top_risks summary human_actions
    }
    AgentRequest {
      request_id session_id behavior_id status lifecycle_state caused_by_trigger_id
      caused_by_correlation caused_by_source_doc_id caused_by_parent_request_id
      caused_by_parent_tool_call_id subagent_depth content created_at
    }
    InferenceCall { request_id prompt_tokens completion_tokens }
  }`);
  let verificationAssignments: DefenseSnapshot["verificationAssignments"] = [];
  let verificationCompletions: DefenseSnapshot["verificationCompletions"] = [];
  let clusters: DefenseSnapshot["clusters"] = [];
  let contractReviews: DefenseSnapshot["contractReviews"] = [];
  let validations: DefenseSnapshot["validations"] = [];
  let securityReviews: DefenseSnapshot["securityReviews"] = [];
  let contractPipelineAvailable = false;
  try {
    const optional = await postGraphql<{
      DefenseVerificationAssignment?: DefenseSnapshot["verificationAssignments"];
      DefenseVerificationCompletion?: DefenseSnapshot["verificationCompletions"];
    }>(`{
      DefenseVerificationAssignment {
        _docID run_id assignment_id finding_id area_id repository_path status
        scan_ledger_status expected_total
      }
      DefenseVerificationCompletion {
        _docID run_id assignment_id finding_id repository_path status scan_ledger_status
        expected_total
      }
    }`);
    verificationAssignments = optional.DefenseVerificationAssignment ?? [];
    verificationCompletions = optional.DefenseVerificationCompletion ?? [];
  } catch {
    // Older live runs predate the assignment schema. Keep their visualizer usable.
  }
  try {
    const optional = await postGraphql<{
      DefenseRootCauseCluster?: DefenseSnapshot["clusters"];
      DefenseContractReview?: DefenseSnapshot["contractReviews"];
      DefensePatchValidation?: DefenseSnapshot["validations"];
      DefensePatchSecurityReview?: DefenseSnapshot["securityReviews"];
    }>(`{
      DefenseRootCauseCluster {
        _docID run_id cluster_id repository_path base_revision base_tree_state status primary_finding_id
        member_finding_ids consequence_finding_ids canonical_title canonical_root_cause claim_kind
        severity security_boundary affected_paths remediation_scope expected_total
      }
      DefenseContractReview {
        _docID run_id review_id cluster_id status disposition spec_impact
        required_foundation_flow required_proof_files compatibility_constraints
        recommended_fix_boundary required_human_decision evidence expected_total
      }
      DefensePatchValidation {
        _docID run_id validation_id patch_id cluster_id finding_id status
        validated_base_revision base_tree_state validated_diff_sha256 observed_head_revision
        result_tree_hash workspace_mode workspace_identity changed_files provenance_match
        applies_cleanly format_status compile_status test_status proof_status commands evidence
        expected_total
      }
      DefensePatchSecurityReview {
        _docID run_id security_review_id patch_id cluster_id finding_id validation_id
        reviewed_base_revision reviewed_base_tree_state reviewed_diff_sha256 receipt_match verdict
        original_path_closed sibling_variants_checked bypass_found contract_alignment evidence
        expected_total
      }
    }`);
    clusters = optional.DefenseRootCauseCluster ?? [];
    contractReviews = optional.DefenseContractReview ?? [];
    validations = optional.DefensePatchValidation ?? [];
    securityReviews = optional.DefensePatchSecurityReview ?? [];
    contractPipelineAvailable = true;
  } catch {
    // Older live runs predate the contract-aware patch pipeline.
  }
  return {
    contractPipelineAvailable,
    jobs: data.DefendingCodeJob ?? [],
    threats: data.DefenseThreatModel ?? [],
    areas: data.DefenseReviewArea ?? [],
    scans: data.DefenseScanResult ?? [],
    candidates: data.DefenseCandidateFinding ?? [],
    verificationAssignments,
    verificationCompletions,
    verdicts: data.DefenseFindingVerdict ?? [],
    findings: data.DefendingFinding ?? [],
    triage: data.DefenseTriageSummary ?? [],
    clusters,
    contractReviews,
    assignments: data.DefensePatchAssignment ?? [],
    patches: data.DefensePatchCandidate ?? [],
    validations,
    reviews: data.DefensePatchReview ?? [],
    securityReviews,
    reports: data.DefenseReport ?? [],
    requests: data.AgentRequest ?? [],
    calls: data.InferenceCall ?? [],
  };
}

export function emptyDefenseSnapshot(): DefenseSnapshot {
  return {
    contractPipelineAvailable: false,
    jobs: [],
    threats: [],
    areas: [],
    scans: [],
    candidates: [],
    verificationAssignments: [],
    verificationCompletions: [],
    verdicts: [],
    findings: [],
    triage: [],
    clusters: [],
    contractReviews: [],
    assignments: [],
    patches: [],
    validations: [],
    reviews: [],
    securityReviews: [],
    reports: [],
    requests: [],
    calls: [],
  };
}
