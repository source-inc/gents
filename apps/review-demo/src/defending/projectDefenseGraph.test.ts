import { describe, expect, it } from "vitest";

import { emptyDefenseSnapshot } from "./pollDefending.ts";
import { projectDefenseGraph } from "./projectDefenseGraph.ts";

describe("projectDefenseGraph", () => {
  it("shows a compact expected campaign before a job exists", () => {
    const graph = projectDefenseGraph(emptyDefenseSnapshot());
    expect(graph.runId).toBeNull();
    expect(graph.nodes.filter((node) => node.kind === "area")).toHaveLength(8);
    expect(graph.nodes.filter((node) => node.kind === "scan")).toHaveLength(0);
    expect(graph.nodes.at(-1)).toMatchObject({
      kind: "report",
      state: "expected",
    });
  });

  it("shows the requested area fan-out while planning is live", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({
      _docID: "job-1",
      run_id: "defense-1",
      area_min: "8",
      area_max: "8",
    });
    snapshot.threats.push({ _docID: "threat-1", run_id: "defense-1" });
    snapshot.requests.push(
      {
        request_id: "threat-request",
        caused_by_correlation: "defense-1",
        caused_by_trigger_id: "defend-threat-model",
        caused_by_source_doc_id: "job-1",
        lifecycle_state: "completed",
      },
      {
        request_id: "plan-request",
        caused_by_correlation: "defense-1",
        caused_by_trigger_id: "defend-plan",
        caused_by_source_doc_id: "threat-1",
        lifecycle_state: "processing",
      },
    );

    const graph = projectDefenseGraph(snapshot);
    expect(graph.nodes.find((node) => node.kind === "threat")?.state).toBe(
      "done",
    );
    expect(graph.nodes.find((node) => node.kind === "plan")).toMatchObject({
      state: "live",
      requestId: "plan-request",
      badges: ["0/8 areas"],
    });
    expect(graph.nodes.filter((node) => node.kind === "area")).toHaveLength(8);
    expect(graph.nodes.filter((node) => node.kind === "scan")).toHaveLength(0);
    expect(graph.nodes.find((node) => node.kind === "triage")?.state).toBe(
      "waiting-group",
    );
  });

  it("keeps expected slots visible while the planner streams area writes", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ run_id: "defense-1", area_min: "3", area_max: "3" });
    snapshot.areas.push({
      _docID: "area-1",
      run_id: "defense-1",
      area_id: "defense-1:area-01",
      expected_total: "3",
    });
    snapshot.requests.push({
      request_id: "scan-request",
      caused_by_correlation: "defense-1",
      caused_by_trigger_id: "defend-scan",
      caused_by_source_doc_id: "area-1",
      lifecycle_state: "processing",
    });
    snapshot.candidates.push({
      _docID: "candidate-early",
      run_id: "defense-1",
      finding_id: "finding-early",
    });

    const graph = projectDefenseGraph(snapshot);
    const areas = graph.nodes.filter((node) => node.kind === "area");
    expect(areas).toHaveLength(3);
    expect(areas.map((node) => node.state)).toEqual([
      "done",
      "expected",
      "expected",
    ]);
    expect(areas[0]).not.toHaveProperty("requestId");
    expect(graph.nodes.filter((node) => node.kind === "scan")).toHaveLength(1);
    expect(graph.nodes.find((node) => node.kind === "scan")).toMatchObject({
      state: "live",
      requestId: "scan-request",
    });
    expect(graph.nodes.filter((node) => node.kind === "candidate")).toHaveLength(
      0,
    );
    expect(graph.nodes.find((node) => node.kind === "triage")).toMatchObject({
      state: "waiting-group",
      badges: ["0/3 scans", "1 candidates queued"],
    });
  });

  it("nests real verifier requests under candidate documents", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ _docID: "job-1", run_id: "defense-1", area_min: "1" });
    snapshot.areas.push({
      _docID: "area-1",
      run_id: "defense-1",
      area_id: "defense-1:area-01",
      expected_total: "1",
    });
    snapshot.scans.push({
      _docID: "scan-1",
      run_id: "defense-1",
      area_id: "defense-1:area-01",
      expected_total: "1",
    });
    snapshot.candidates.push({
      _docID: "candidate-1",
      run_id: "defense-1",
      finding_id: "finding-1",
      title: "Candidate claim",
    });
    snapshot.verdicts.push({
      _docID: "verdict-1",
      run_id: "defense-1",
      finding_id: "finding-1",
      verdict: "confirmed",
    });
    snapshot.requests.push(
      {
        request_id: "triage-request",
        caused_by_correlation: "defense-1",
        caused_by_trigger_id: "defend-triage",
        lifecycle_state: "processing",
      },
      {
        request_id: "verifier-request",
        session_id: "verifier-session",
        behavior_id: "defend-verifier",
        caused_by_correlation: "defense-1",
        caused_by_parent_request_id: "triage-request",
        content: "Verify exactly `finding-1` (finding_id: finding-1).",
        lifecycle_state: "completed",
      },
    );

    const graph = projectDefenseGraph(snapshot);
    expect(graph.nodes.find((node) => node.kind === "candidate")).toMatchObject(
      {
        sourceDocId: "candidate-1",
        state: "done",
        badges: ["verified"],
      },
    );
    expect(graph.nodes.find((node) => node.kind === "verifier")).toMatchObject({
      requestId: "verifier-request",
      sessionId: "verifier-session",
      state: "done",
      badges: ["verified"],
    });
    expect(graph.nodes.find((node) => node.kind === "verdict")).toMatchObject({
      sourceDocId: "verdict-1",
      state: "done",
    });
  });

  it("does not invent verifier agents for a legacy serial triage request", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ _docID: "job-1", run_id: "defense-1", area_min: "1" });
    snapshot.areas.push({
      _docID: "area-1",
      run_id: "defense-1",
      area_id: "defense-1:area-01",
      expected_total: "1",
    });
    snapshot.scans.push({
      _docID: "scan-1",
      run_id: "defense-1",
      area_id: "defense-1:area-01",
      expected_total: "1",
    });
    snapshot.candidates.push({
      _docID: "candidate-1",
      run_id: "defense-1",
      finding_id: "finding-1",
    });
    snapshot.requests.push({
      request_id: "triage-request",
      caused_by_correlation: "defense-1",
      caused_by_trigger_id: "defend-triage",
      content: "Verify every candidate in stable order.",
      lifecycle_state: "processing",
    });

    const graph = projectDefenseGraph(snapshot);
    expect(graph.nodes.find((node) => node.kind === "triage")).toMatchObject({
      state: "live",
      badges: [
        "1/1 scans",
        "serial triage",
        "active candidate untracked",
      ],
    });
    expect(graph.nodes.find((node) => node.kind === "candidate")?.badges).toEqual([
      "activity untracked",
    ]);
    expect(graph.nodes.filter((node) => node.kind === "verifier")).toHaveLength(0);
  });

  it("projects assignment-triggered verifier requests without a parent coordinator", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ _docID: "job-1", run_id: "defense-1", area_min: "1" });
    snapshot.areas.push({
      _docID: "area-1",
      run_id: "defense-1",
      area_id: "defense-1:area-01",
      expected_total: "1",
    });
    snapshot.scans.push({
      _docID: "scan-1",
      run_id: "defense-1",
      area_id: "defense-1:area-01",
      expected_total: "1",
    });
    snapshot.candidates.push({
      _docID: "candidate-1",
      run_id: "defense-1",
      finding_id: "finding-1",
    });
    snapshot.verificationAssignments.push({
      _docID: "verify-assignment-1",
      run_id: "defense-1",
      assignment_id: "finding-1:verify",
      finding_id: "finding-1",
      status: "ready",
      expected_total: "1",
    });
    snapshot.requests.push(
      {
        request_id: "plan-request",
        caused_by_correlation: "defense-1",
        caused_by_trigger_id: "defend-verification-plan",
        lifecycle_state: "completed",
      },
      {
        request_id: "verifier-request",
        session_id: "verifier-session",
        behavior_id: "defend-verifier",
        caused_by_correlation: "defense-1",
        caused_by_trigger_id: "defend-verifier",
        caused_by_source_doc_id: "verify-assignment-1",
        lifecycle_state: "processing",
      },
    );

    const graph = projectDefenseGraph(snapshot);
    expect(graph.nodes.find((node) => node.kind === "verification-plan")).toMatchObject({
      state: "done",
      requestId: "plan-request",
      badges: ["1 assignments", "document fan-out"],
    });
    expect(
      graph.nodes.find((node) => node.kind === "verification-assignment"),
    ).toMatchObject({ state: "done", sourceDocId: "verify-assignment-1" });
    expect(graph.nodes.find((node) => node.kind === "verifier")).toMatchObject({
      state: "live",
      requestId: "verifier-request",
      badges: ["running"],
    });
    expect(graph.nodes.find((node) => node.kind === "triage")).toMatchObject({
      state: "waiting-group",
      badges: ["0/1 complete", "0/1 verdicts", "1 running", "0 queued"],
    });
  });

  it("shows the empty candidate assignment and verifier as an explicit lane", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ run_id: "defense-empty", area_min: "1" });
    snapshot.verificationAssignments.push({
      _docID: "empty-assignment",
      run_id: "defense-empty",
      assignment_id: "defense-empty:no-candidates",
      finding_id: "none",
      status: "skipped",
      expected_total: "1",
    });
    snapshot.verificationCompletions.push({
      run_id: "defense-empty",
      assignment_id: "defense-empty:no-candidates",
      finding_id: "none",
      status: "skipped",
      expected_total: "1",
    });
    snapshot.requests.push({
      request_id: "empty-verifier",
      caused_by_correlation: "defense-empty",
      caused_by_trigger_id: "defend-verifier",
      caused_by_source_doc_id: "empty-assignment",
      behavior_id: "defend-verifier",
      lifecycle_state: "completed",
    });

    const graph = projectDefenseGraph(snapshot);
    expect(graph.nodes.find((node) => node.id === "candidate:none")).toMatchObject({
      state: "done",
      badges: ["empty-set sentinel"],
    });
    expect(
      graph.nodes.find((node) => node.id === "verification-assignment:none"),
    ).toMatchObject({ sourceDocId: "empty-assignment", badges: ["skipped"] });
    expect(graph.nodes.find((node) => node.id === "verifier:none")).toMatchObject({
      requestId: "empty-verifier",
      state: "done",
      badges: ["skipped"],
    });
  });

  it("shows a verifier's durable blocked completion without inventing a verdict", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ run_id: "defense-blocked", area_min: "1" });
    snapshot.candidates.push({
      run_id: "defense-blocked",
      finding_id: "finding-blocked",
    });
    snapshot.verificationAssignments.push({
      _docID: "assignment-blocked",
      run_id: "defense-blocked",
      assignment_id: "finding-blocked:verify",
      finding_id: "finding-blocked",
      status: "ready",
      expected_total: "1",
    });
    snapshot.verificationCompletions.push({
      run_id: "defense-blocked",
      assignment_id: "finding-blocked:verify",
      finding_id: "finding-blocked",
      status: "blocked_provenance",
      expected_total: "1",
    });
    snapshot.requests.push({
      request_id: "verifier-blocked",
      caused_by_correlation: "defense-blocked",
      caused_by_trigger_id: "defend-verifier",
      caused_by_source_doc_id: "assignment-blocked",
      behavior_id: "defend-verifier",
      lifecycle_state: "completed",
    });

    const graph = projectDefenseGraph(snapshot);
    expect(
      graph.nodes.find((node) => node.id === "verifier:finding-blocked"),
    ).toMatchObject({
      state: "done",
      badges: ["blocked provenance"],
    });
    expect(graph.nodes.some((node) => node.kind === "verdict")).toBe(false);
  });

  it("does not let a verdict mask a contradictory blocked completion", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ run_id: "defense-corrupt", area_min: "1" });
    snapshot.candidates.push({
      run_id: "defense-corrupt",
      finding_id: "finding-corrupt",
    });
    snapshot.verificationAssignments.push({
      _docID: "assignment-corrupt",
      run_id: "defense-corrupt",
      assignment_id: "finding-corrupt:verify",
      finding_id: "finding-corrupt",
      expected_total: "1",
    });
    snapshot.verificationCompletions.push({
      run_id: "defense-corrupt",
      assignment_id: "finding-corrupt:verify",
      finding_id: "finding-corrupt",
      status: "blocked_handoff",
      expected_total: "1",
    });
    snapshot.verdicts.push({
      run_id: "defense-corrupt",
      finding_id: "finding-corrupt",
      verdict: "confirmed",
    });

    expect(
      projectDefenseGraph(snapshot).nodes.find(
        (node) => node.id === "candidate:finding-corrupt",
      )?.badges,
    ).toEqual(["blocked handoff"]);
  });

  it("shows an actual reducer request even when the local ledger looks inconsistent", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ run_id: "defense-live-reducer", area_min: "1" });
    snapshot.requests.push({
      request_id: "triage-live",
      caused_by_correlation: "defense-live-reducer",
      caused_by_trigger_id: "defend-triage",
      lifecycle_state: "processing",
    });

    expect(
      projectDefenseGraph(snapshot).nodes.find((node) => node.kind === "triage"),
    ).toMatchObject({ state: "live", requestId: "triage-live" });
  });

  it("renders orphan verification work and its running agent", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ run_id: "defense-orphan", area_min: "1" });
    snapshot.verificationAssignments.push({
      _docID: "assignment-orphan",
      run_id: "defense-orphan",
      assignment_id: "missing-finding:verify",
      finding_id: "missing-finding",
      status: "ready",
      expected_total: "1",
    });
    snapshot.requests.push({
      request_id: "verifier-orphan",
      caused_by_correlation: "defense-orphan",
      caused_by_trigger_id: "defend-verifier",
      caused_by_source_doc_id: "assignment-orphan",
      behavior_id: "defend-verifier",
      lifecycle_state: "processing",
    });

    const graph = projectDefenseGraph(snapshot);
    expect(
      graph.nodes.find((node) => node.id === "verification-assignment:orphan:assignment-orphan"),
    ).toMatchObject({ badges: ["orphan/duplicate ledger row", "ready"] });
    expect(
      graph.nodes.find((node) => node.id === "verifier:orphan:assignment-orphan"),
    ).toMatchObject({ state: "live", requestId: "verifier-orphan" });
  });

  it("renders a validation agent launched from an orphan patch", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.contractPipelineAvailable = true;
    snapshot.jobs.push({ run_id: "defense-orphan-patch", area_min: "1" });
    snapshot.patches.push({
      _docID: "patch-orphan",
      run_id: "defense-orphan-patch",
      patch_id: "missing-assignment:patch",
      status: "drafted",
    });
    snapshot.validations.push({
      _docID: "validation-orphan-doc",
      run_id: "defense-orphan-patch",
      validation_id: "missing-assignment:patch:validation",
      patch_id: "missing-assignment:patch",
      status: "partial",
    });
    snapshot.requests.push({
      request_id: "validation-orphan",
      caused_by_correlation: "defense-orphan-patch",
      caused_by_trigger_id: "defend-patch-validation",
      caused_by_source_doc_id: "patch-orphan",
      lifecycle_state: "processing",
    });

    const graph = projectDefenseGraph(snapshot);
    expect(
      graph.nodes.find((node) => node.id === "patch:orphan:patch-orphan"),
    ).toMatchObject({ state: "done", sourceDocId: "patch-orphan" });
    expect(
      graph.nodes.find(
        (node) => node.id === "validation:orphan-request:patch-orphan",
      ),
    ).toMatchObject({ state: "live", requestId: "validation-orphan" });
  });

  it("keeps discovery blocked when ledger totals disagree", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ run_id: "defense-bad-total", area_min: "1" });
    snapshot.areas.push({
      run_id: "defense-bad-total",
      area_id: "area-1",
      expected_total: "1",
    });
    snapshot.scans.push({
      run_id: "defense-bad-total",
      area_id: "area-1",
      expected_total: "2",
    });

    const graph = projectDefenseGraph(snapshot);
    expect(graph.nodes.find((node) => node.kind === "triage")).toMatchObject({
      state: "waiting-group",
      badges: ["1/1 scans", "inconsistent discovery ledger"],
    });
  });

  it("closes both fan-outs and marks the report complete", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ _docID: "job-1", run_id: "defense-1", area_min: "1" });
    snapshot.threats.push({ _docID: "threat-1", run_id: "defense-1" });
    snapshot.areas.push({
      _docID: "area-1",
      run_id: "defense-1",
      area_id: "defense-1:area-01",
      expected_total: "1",
    });
    snapshot.scans.push({
      _docID: "scan-1",
      run_id: "defense-1",
      area_id: "defense-1:area-01",
    });
    snapshot.triage.push({ _docID: "triage-1", run_id: "defense-1" });
    snapshot.assignments.push({
      _docID: "assignment-1",
      run_id: "defense-1",
      assignment_id: "finding-1:patch",
      finding_id: "finding-1",
    });
    snapshot.patches.push({
      _docID: "patch-1",
      run_id: "defense-1",
      patch_id: "finding-1:patch",
    });
    snapshot.reviews.push({
      run_id: "defense-1",
      patch_id: "finding-1:patch",
      verdict: "ACCEPT",
    });
    snapshot.reports.push({
      _docID: "report-1",
      run_id: "defense-1",
      confirmed_count: "1",
      accepted_patch_count: "1",
    });
    for (const [requestId, triggerId, sourceDocId] of [
      ["threat-request", "defend-threat-model", "job-1"],
      ["plan-request", "defend-plan", "threat-1"],
      ["scan-request", "defend-scan", "area-1"],
      ["triage-request", "defend-triage", undefined],
      ["patch-request", "defend-patch", "assignment-1"],
      ["review-request", "defend-patch-review", "patch-1"],
      ["report-request", "defend-report", undefined],
    ] as const) {
      snapshot.requests.push({
        request_id: requestId,
        caused_by_correlation: "defense-1",
        caused_by_trigger_id: triggerId,
        caused_by_source_doc_id: sourceDocId,
        lifecycle_state: "completed",
      });
    }

    const graph = projectDefenseGraph(snapshot);
    expect(graph.nodes.find((node) => node.kind === "area")).toMatchObject({
      state: "done",
      sourceDocId: "area-1",
    });
    expect(graph.nodes.find((node) => node.kind === "area")).not.toHaveProperty(
      "requestId",
    );
    const scanNode = graph.nodes.find((node) => node.kind === "scan");
    expect(scanNode).toMatchObject({
      state: "done",
      sourceDocId: "scan-1",
      requestId: "scan-request",
    });
    expect(graph.nodes.find((node) => node.kind === "review")).toMatchObject({
      state: "done",
      badges: ["ACCEPT"],
    });
    expect(graph.nodes.find((node) => node.kind === "report")).toMatchObject({
      state: "done",
      badges: ["1 confirmed", "1 accepted"],
    });
  });

  it("projects contract review, validation, and adversarial re-attack as agents", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.jobs.push({ run_id: "defense-2", area_min: "1" });
    snapshot.triage.push({ _docID: "triage-2", run_id: "defense-2" });
    snapshot.clusters.push({
      _docID: "cluster-doc-2",
      run_id: "defense-2",
      cluster_id: "defense-2:cluster-01",
      canonical_title: "Canonical root cause",
      member_finding_ids: "finding-a,finding-b",
      expected_total: "1",
    });
    snapshot.contractReviews.push({
      _docID: "contract-doc-2",
      run_id: "defense-2",
      review_id: "defense-2:cluster-01:contract",
      cluster_id: "defense-2:cluster-01",
      disposition: "actionable",
      expected_total: "1",
    });
    snapshot.assignments.push({
      _docID: "patch-assignment-2",
      run_id: "defense-2",
      assignment_id: "defense-2:cluster-01:patch",
      cluster_id: "defense-2:cluster-01",
      expected_total: "1",
    });
    snapshot.patches.push({
      _docID: "patch-doc-2",
      run_id: "defense-2",
      patch_id: "defense-2:cluster-01:patch",
      status: "drafted",
    });
    snapshot.validations.push({
      _docID: "validation-doc-2",
      run_id: "defense-2",
      validation_id: "defense-2:cluster-01:patch:validation",
      patch_id: "defense-2:cluster-01:patch",
      status: "passed",
      applies_cleanly: "yes",
    });
    snapshot.reviews.push({
      _docID: "review-doc-2",
      run_id: "defense-2",
      patch_id: "defense-2:cluster-01:patch",
      verdict: "ACCEPT",
    });
    snapshot.securityReviews.push({
      _docID: "security-doc-2",
      run_id: "defense-2",
      security_review_id: "defense-2:cluster-01:patch:security",
      patch_id: "defense-2:cluster-01:patch",
      verdict: "ACCEPT",
    });
    for (const [requestId, triggerId, sourceDocId] of [
      ["cluster-request", "defend-cluster", "triage-2"],
      ["contract-request", "defend-contract-review", "cluster-doc-2"],
      ["remediation-request", "defend-remediation-plan", undefined],
      ["patch-request", "defend-patch", "patch-assignment-2"],
      ["validation-request", "defend-patch-validation", "patch-doc-2"],
      ["review-request", "defend-patch-review", "validation-doc-2"],
      ["security-request", "defend-patch-security-review", "review-doc-2"],
    ] as const) {
      snapshot.requests.push({
        request_id: requestId,
        caused_by_correlation: "defense-2",
        caused_by_trigger_id: triggerId,
        caused_by_source_doc_id: sourceDocId,
        lifecycle_state: "completed",
      });
    }

    const graph = projectDefenseGraph(snapshot);
    expect(graph.nodes.find((node) => node.kind === "contract-review")).toMatchObject({
      requestId: "contract-request",
      state: "done",
      badges: ["actionable"],
    });
    expect(graph.nodes.find((node) => node.kind === "validation")).toMatchObject({
      requestId: "validation-request",
      state: "done",
      badges: ["passed", "applies yes"],
    });
    expect(graph.nodes.find((node) => node.kind === "security-review")).toMatchObject({
      requestId: "security-request",
      state: "done",
      badges: ["ACCEPT"],
    });
  });

  it("uses the closed security ledger total to show report readiness", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.contractPipelineAvailable = true;
    snapshot.jobs.push({ run_id: "defense-3", area_min: "1" });
    snapshot.assignments.push(
      {
        run_id: "defense-3",
        assignment_id: "patch-1",
        expected_total: "2",
      },
      {
        run_id: "defense-3",
        assignment_id: "patch-2",
        expected_total: "2",
      },
    );
    snapshot.securityReviews.push({
      run_id: "defense-3",
      security_review_id: "security-1",
      patch_id: "patch-1",
      expected_total: "1",
    });
    snapshot.requests.push({
      request_id: "report-request",
      caused_by_correlation: "defense-3",
      caused_by_trigger_id: "defend-report",
      lifecycle_state: "processing",
    });

    const graph = projectDefenseGraph(snapshot);
    expect(graph.nodes.find((node) => node.kind === "report")).toMatchObject({
      state: "live",
      requestId: "report-request",
    });
  });

  it("does not anticipate a report until security ids and totals close", () => {
    const snapshot = emptyDefenseSnapshot();
    snapshot.contractPipelineAvailable = true;
    snapshot.jobs.push({ run_id: "defense-4", area_min: "1" });
    snapshot.assignments.push(
      { run_id: "defense-4", assignment_id: "patch-1", expected_total: "2" },
      { run_id: "defense-4", assignment_id: "patch-2", expected_total: "2" },
    );
    snapshot.securityReviews.push({
      run_id: "defense-4",
      security_review_id: "security-1",
      patch_id: "patch-1",
      expected_total: "1",
    });

    expect(
      projectDefenseGraph(snapshot).nodes.find((node) => node.kind === "report")
        ?.state,
    ).toBe("waiting-group");

    snapshot.securityReviews[0].expected_total = "2";
    snapshot.securityReviews.push({
      run_id: "defense-4",
      security_review_id: "security-2",
      patch_id: "patch-2",
      expected_total: "2",
    });
    expect(
      projectDefenseGraph(snapshot).nodes.find((node) => node.kind === "report")
        ?.state,
    ).toBe("expected");
  });
});
