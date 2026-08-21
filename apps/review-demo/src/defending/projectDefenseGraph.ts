import type { AgentRequestRow } from "../graph/types.ts";
import type {
  DefenseGraph,
  DefenseNode,
  DefenseNodeState,
  DefenseSnapshot,
} from "./types.ts";

const FAILED = new Set(["failed", "dead", "interrupted", "superseded"]);
const DONE = new Set(["completed", "complete"]);

export type DefenseProjectOptions = {
  pinnedRunId?: string | null;
};

function stateFor(
  request: AgentRequestRow | undefined,
  documentExists: boolean,
): DefenseNodeState {
  const lifecycle = request?.lifecycle_state ?? request?.status ?? "";
  if (lifecycle === "inputRequired") {
    return "input-required";
  }
  const normalized = lifecycle.toLowerCase();
  if (FAILED.has(normalized)) {
    return "failed";
  }
  if (DONE.has(normalized) || (documentExists && !request)) {
    return "done";
  }
  if (request || documentExists) {
    return "live";
  }
  return "expected";
}

function coordinatorState(
  request: AgentRequestRow | undefined,
  documentExists: boolean,
): DefenseNodeState {
  const state = stateFor(request, documentExists);
  return state === "done" && !documentExists ? "live" : state;
}

function requestFor(
  requests: AgentRequestRow[],
  triggerId: string,
  sourceDocId?: string,
): AgentRequestRow | undefined {
  return requests
    .filter((request) => {
      if (request.caused_by_trigger_id !== triggerId) {
        return false;
      }
      return !sourceDocId || request.caused_by_source_doc_id === sourceDocId;
    })
    .sort((left, right) =>
      (left.created_at ?? "").localeCompare(right.created_at ?? ""),
    )
    .at(-1);
}

function verifierRequestFor(
  requests: AgentRequestRow[],
  parentRequestId: string | undefined,
  findingId: string,
  assignmentDocId?: string,
): AgentRequestRow | undefined {
  return requests
    .filter((request) => {
      if (request.behavior_id !== "defend-verifier") {
        return false;
      }
      if (
        assignmentDocId &&
        request.caused_by_trigger_id === "defend-verifier" &&
        request.caused_by_source_doc_id === assignmentDocId
      ) {
        return true;
      }
      if (
        parentRequestId &&
        request.caused_by_parent_request_id !== parentRequestId
      ) {
        return false;
      }
      const content = request.content ?? "";
      return (
        content.includes(`finding_id: ${findingId}`) ||
        content.includes(`finding_id=${findingId}`) ||
        content.includes(`\"finding_id\":\"${findingId}\"`) ||
        content.includes(`\`${findingId}\``)
      );
    })
    .sort((left, right) =>
      (left.created_at ?? "").localeCompare(right.created_at ?? ""),
    )
    .at(-1);
}

function verifierActivity(
  request: AgentRequestRow | undefined,
  verdictExists: boolean,
  completionStatus?: string,
): string {
  if (completionStatus && completionStatus !== "verified") {
    return completionStatus.replaceAll("_", " ");
  }
  if (verdictExists) {
    return "verified";
  }
  if (!request) {
    return "queued";
  }
  const lifecycle = (request.lifecycle_state ?? request.status ?? "").toLowerCase();
  if (FAILED.has(lifecycle)) {
    return "failed";
  }
  if (lifecycle === "processing" || lifecycle === "running") {
    return "running";
  }
  if (lifecycle === "inputrequired" || lifecycle === "input-required") {
    return "input required";
  }
  if (DONE.has(lifecycle)) {
    return completionStatus === "verified"
      ? "verified completion · verdict missing"
      : "completed · receipt pending";
  }
  return "queued";
}

function node(
  partial: Omit<DefenseNode, "badges"> & { badges?: string[] },
): DefenseNode {
  return { badges: [], ...partial };
}

function rowsBeyondParentMultiplicity<T>(
  rows: T[],
  rowKey: (row: T) => string,
  parentKeys: string[],
): Array<{ index: number; row: T }> {
  const remaining = new Map<string, number>();
  for (const key of parentKeys) {
    remaining.set(key, (remaining.get(key) ?? 0) + 1);
  }
  const extras: Array<{ index: number; row: T }> = [];
  rows.forEach((row, index) => {
    const key = rowKey(row);
    const slots = remaining.get(key) ?? 0;
    if (slots > 0) {
      remaining.set(key, slots - 1);
    } else {
      extras.push({ index, row });
    }
  });
  return extras;
}

function selectJob(snapshot: DefenseSnapshot, pinnedRunId?: string | null) {
  if (pinnedRunId) {
    const pinned = snapshot.jobs.find((job) => job.run_id === pinnedRunId);
    if (pinned) {
      return pinned;
    }
  }
  return snapshot.jobs
    .slice()
    .sort((left, right) => {
      const latest = (runId: string) =>
        snapshot.requests
          .filter((request) => request.caused_by_correlation === runId)
          .map((request) => request.created_at ?? "")
          .sort()
          .at(-1) ?? "";
      return latest(left.run_id).localeCompare(latest(right.run_id));
    })
    .at(-1);
}

export function projectDefenseGraph(
  snapshot: DefenseSnapshot,
  options: DefenseProjectOptions = {},
): DefenseGraph {
  const job = selectJob(snapshot, options.pinnedRunId);
  if (!job) {
    return skeleton(null, 8);
  }

  const runId = job.run_id;
  const requests = snapshot.requests.filter(
    (request) =>
      !request.caused_by_correlation || request.caused_by_correlation === runId,
  );
  const threat = snapshot.threats.find((row) => row.run_id === runId);
  const areas = snapshot.areas
    .filter((row) => row.run_id === runId)
    .slice()
    .sort((left, right) => left.area_id.localeCompare(right.area_id));
  const scans = snapshot.scans.filter((row) => row.run_id === runId);
  const candidates = snapshot.candidates.filter((row) => row.run_id === runId);
  const verificationAssignments = snapshot.verificationAssignments.filter(
    (row) => row.run_id === runId,
  );
  const verificationCompletions = snapshot.verificationCompletions.filter(
    (row) => row.run_id === runId,
  );
  const verdicts = snapshot.verdicts.filter((row) => row.run_id === runId);
  const findings = snapshot.findings.filter((row) => row.run_id === runId);
  const triage = snapshot.triage.find((row) => row.run_id === runId);
  const clusters = snapshot.clusters
    .filter((row) => row.run_id === runId)
    .slice()
    .sort((left, right) => left.cluster_id.localeCompare(right.cluster_id));
  const contractReviews = snapshot.contractReviews.filter(
    (row) => row.run_id === runId,
  );
  const assignments = snapshot.assignments
    .filter((row) => row.run_id === runId)
    .slice()
    .sort((left, right) =>
      left.assignment_id.localeCompare(right.assignment_id),
    );
  const patches = snapshot.patches.filter((row) => row.run_id === runId);
  const validations = snapshot.validations.filter((row) => row.run_id === runId);
  const reviews = snapshot.reviews.filter((row) => row.run_id === runId);
  const securityReviews = snapshot.securityReviews.filter(
    (row) => row.run_id === runId,
  );
  const report = snapshot.reports.find((row) => row.run_id === runId);

  const threatRequest = requestFor(requests, "defend-threat-model", job._docID);
  const planRequest = requestFor(requests, "defend-plan", threat?._docID);
  const triageRequest = requestFor(requests, "defend-triage");
  const clusterRequest = requestFor(requests, "defend-cluster", triage?._docID);
  const remediationPlanRequest = requestFor(requests, "defend-remediation-plan");
  const verificationPlanRequest = requestFor(
    requests,
    "defend-verification-plan",
  );
  const reportRequest = requestFor(requests, "defend-report");
  const expectedAreas = positiveInt(
    areas[0]?.expected_total ?? job.area_min,
    4,
  );

  const nodes: DefenseNode[] = [
    node({
      id: `job:${runId}`,
      kind: "job",
      label: "Defense job",
      detail: job.focus,
      state: "done",
      runId,
      sourceDocId: job._docID,
    }),
    node({
      id: `threat:${runId}`,
      kind: "threat",
      label: "Threat model",
      detail: threat?.system_context,
      state: stateFor(threatRequest, Boolean(threat)),
      runId,
      requestId: threatRequest?.request_id,
      sessionId: threatRequest?.session_id ?? undefined,
      sourceDocId: threat?._docID,
      badges: threat
        ? [threat.provenance_status ?? "written"]
        : [],
    }),
    node({
      id: `plan:${runId}`,
      kind: "plan",
      label: "Plan areas",
      state: stateFor(planRequest, areas.length === expectedAreas),
      runId,
      requestId: planRequest?.request_id,
      sessionId: planRequest?.session_id ?? undefined,
      badges: [`${areas.length}/${expectedAreas} areas`],
    }),
  ];

  if (areas.length === 0) {
    for (let index = 0; index < expectedAreas; index += 1) {
      const key = `pending-${index}`;
      nodes.push(
        node({
          id: `area:${key}`,
          kind: "area",
          label: `Area ${index + 1}`,
          state: "expected",
          runId,
        }),
      );
    }
  } else {
    for (const [index, area] of areas.entries()) {
      const scan = scans.find((row) => row.area_id === area.area_id);
      const request = requestFor(requests, "defend-scan", area._docID);
      const findingCount = candidates.filter(
        (row) => row.area_id === area.area_id,
      ).length;
      nodes.push(
        node({
          id: `area:${area.area_id}`,
          kind: "area",
          label: `Area ${index + 1}`,
          detail: area.focus ?? area.area_id,
          state: "done",
          runId,
          sourceDocId: area._docID,
          badges: [
            ...(area.threat_ids ? [area.threat_ids] : []),
            ...(area.status ? [area.status] : []),
          ],
        }),
        node({
          id: `scan:${area.area_id}`,
          kind: "scan",
          label: `Scan ${index + 1}`,
          detail: area.focus ?? area.area_id,
          state: stateFor(request, Boolean(scan)),
          runId,
          requestId: request?.request_id,
          sessionId: request?.session_id ?? undefined,
          sourceDocId: scan?._docID,
          badges: [
            ...(scan?.status ? [scan.status] : []),
            ...(findingCount > 0
              ? [`${findingCount} candidate${findingCount === 1 ? "" : "s"}`]
              : []),
            ...(scan?.finding_count && scan.finding_count !== String(findingCount)
              ? [`declared ${scan.finding_count}`]
              : []),
          ],
        }),
      );
    }
    for (let index = areas.length; index < expectedAreas; index += 1) {
      const key = `pending-${index}`;
      nodes.push(
        node({
          id: `area:${key}`,
          kind: "area",
          label: `Area ${index + 1}`,
          state: "expected",
          runId,
        }),
      );
    }
  }

  for (const { index, row: scan } of rowsBeyondParentMultiplicity(
    scans,
    (row) => row.area_id,
    areas.map((row) => row.area_id),
  )) {
    nodes.push(
      node({
        id: `scan:orphan:${scan._docID ?? `${scan.area_id}:${index}`}`,
        kind: "scan",
        label: "Orphan scan",
        detail: scan.area_id,
        state: "done",
        runId,
        sourceDocId: scan._docID,
        badges: [
          "orphan/duplicate ledger row",
          ...(scan.status ? [scan.status] : []),
        ],
      }),
    );
  }

  const areaIds = areas.map((row) => row.area_id);
  const scanAreaIds = scans.map((row) => row.area_id);
  const areaLedgerClosed =
    totalsClose(areas, expectedAreas) && uniqueIds(areaIds);
  const scansClosed =
    areaLedgerClosed &&
    totalsClose(scans, expectedAreas) &&
    sameIds(areaIds, scanAreaIds);
  const discoveryInconsistent =
    (areas.length >= expectedAreas || scans.length >= expectedAreas) &&
    !scansClosed;
  const graphNativeVerification = Boolean(
    verificationPlanRequest ||
      verificationAssignments.length > 0 ||
      requests.some((request) => request.caused_by_trigger_id === "defend-verifier"),
  );
  const sortedCandidates = candidates
    .slice()
    .sort((left, right) => left.finding_id.localeCompare(right.finding_id));
  const candidateWork = sortedCandidates.map((candidate) => {
    const assignment = verificationAssignments.find(
      (row) => row.finding_id === candidate.finding_id,
    );
    const verdict = verdicts.find(
      (row) => row.finding_id === candidate.finding_id,
    );
    const completion = assignment
      ? verificationCompletions.find(
          (row) => row.assignment_id === assignment.assignment_id,
        )
      : undefined;
    const verifierRequest = verifierRequestFor(
      requests,
      triageRequest?.request_id,
      candidate.finding_id,
      assignment?._docID,
    );
    return { assignment, candidate, completion, verdict, verifierRequest };
  });
  const emptyAssignment = verificationAssignments.find(
    (row) => row.status === "skipped" && row.finding_id === "none",
  );
  const emptyCompletion = emptyAssignment
    ? verificationCompletions.find(
        (row) => row.assignment_id === emptyAssignment.assignment_id,
      )
    : undefined;
  const emptyVerifierRequest = emptyAssignment
    ? verifierRequestFor(
        requests,
        triageRequest?.request_id,
        "none",
        emptyAssignment._docID,
      )
    : undefined;
  const isolatedVerifierCount = candidateWork.filter(
    ({ verifierRequest }) => verifierRequest,
  ).length + (emptyVerifierRequest ? 1 : 0);
  const runningVerifierCount = candidateWork.filter(
    ({ completion, verifierRequest, verdict }) =>
      verifierActivity(verifierRequest, Boolean(verdict), completion?.status) ===
      "running",
  ).length +
    (emptyVerifierRequest &&
    verifierActivity(emptyVerifierRequest, false, emptyCompletion?.status) ===
      "running"
      ? 1
      : 0);
  const queuedVerifierCount = candidateWork.filter(
    ({ completion, verifierRequest, verdict }) =>
      verifierActivity(verifierRequest, Boolean(verdict), completion?.status) ===
      "queued",
  ).length +
    (emptyAssignment &&
    verifierActivity(emptyVerifierRequest, false, emptyCompletion?.status) ===
      "queued"
      ? 1
      : 0);
  const isolatedVerifierTopology = Boolean(
    graphNativeVerification ||
      triageRequest?.content?.includes("candidate-verifier") ||
      triageRequest?.content?.includes("spawn_subagent"),
  );
  const legacySerialTriage = Boolean(
    scansClosed &&
      candidates.length > 0 &&
      triageRequest &&
      !isolatedVerifierTopology &&
      isolatedVerifierCount === 0 &&
      !triage,
  );
  if (graphNativeVerification) {
    nodes.push(
      node({
        id: `verification-plan:${runId}`,
        kind: "verification-plan",
        label: "Verification work set",
        state: stateFor(
          verificationPlanRequest,
          verificationAssignments.length > 0,
        ),
        runId,
        requestId: verificationPlanRequest?.request_id,
        sessionId: verificationPlanRequest?.session_id ?? undefined,
        badges: [
          `${verificationAssignments.length} assignments`,
          "document fan-out",
        ],
      }),
    );
  }
  const expectedVerdicts = positiveInt(
    verificationAssignments[0]?.expected_total,
    candidates.length || 1,
  );
  const expectedFindingIds =
    candidates.length === 0 && emptyAssignment
      ? ["none"]
      : candidates.map((row) => row.finding_id);
  const verificationAssignmentsClosed =
    totalsClose(verificationAssignments, expectedVerdicts) &&
    sameIds(
      verificationAssignments.map((row) => row.finding_id),
      expectedFindingIds,
    );
  const verificationClosed =
    graphNativeVerification &&
    verificationAssignmentsClosed &&
    totalsClose(verificationCompletions, expectedVerdicts) &&
    sameIds(
      verificationAssignments.map((row) => row.assignment_id),
      verificationCompletions.map((row) => row.assignment_id),
    );
  const verificationInconsistent =
    verificationAssignments.length >= expectedVerdicts &&
    verificationCompletions.length >= expectedVerdicts &&
    !verificationClosed;
  nodes.push(
    node({
      id: `triage:${runId}`,
      kind: "triage",
      label: "Adversarial triage",
      state: triageRequest
        ? coordinatorState(triageRequest, Boolean(triage))
        : graphNativeVerification
          ? verificationClosed
            ? coordinatorState(triageRequest, Boolean(triage))
            : "waiting-group"
          : scansClosed
            ? coordinatorState(triageRequest, Boolean(triage))
            : "waiting-group",
      runId,
      requestId: triageRequest?.request_id,
      sessionId: triageRequest?.session_id ?? undefined,
      sourceDocId: triage?._docID,
      badges: [
        ...(graphNativeVerification
          ? [
              `${verificationCompletions.length}/${expectedVerdicts} complete`,
              `${verdicts.length}/${candidates.length} verdicts`,
            ]
          : [`${scans.length}/${expectedAreas} scans`]),
        ...(!scansClosed && candidates.length > 0
          ? [`${candidates.length} candidates queued`]
          : []),
        ...(discoveryInconsistent ? ["inconsistent discovery ledger"] : []),
        ...(legacySerialTriage
          ? ["serial triage", "active candidate untracked"]
          : []),
        ...(scansClosed &&
        candidates.length > 0 &&
        !legacySerialTriage &&
        !graphNativeVerification
          ? [`${verdicts.length}/${candidates.length} verdicts`]
          : []),
        ...(isolatedVerifierCount > 0
          ? [`${runningVerifierCount} running`, `${queuedVerifierCount} queued`]
          : []),
        ...(findings.length > 0 ? [`${findings.length} confirmed`] : []),
        ...(triage?.scan_ledger_status ? [triage.scan_ledger_status] : []),
        ...(verificationInconsistent
          ? ["inconsistent verification ledger"]
          : []),
      ],
    }),
  );

  if (scansClosed || graphNativeVerification) {
    if (emptyAssignment) {
      nodes.push(
        node({
          id: "candidate:none",
          kind: "candidate",
          label: "Empty candidate set",
          detail: "No candidate rows were written",
          state: "done",
          runId,
          badges: ["empty-set sentinel"],
        }),
        node({
          id: "verification-assignment:none",
          kind: "verification-assignment",
          label: "Sentinel assignment",
          detail: emptyAssignment.assignment_id,
          state: "done",
          runId,
          sourceDocId: emptyAssignment._docID,
          badges: [emptyAssignment.status ?? "skipped"],
        }),
      );
      if (emptyVerifierRequest) {
        nodes.push(
          node({
            id: "verifier:none",
            kind: "verifier",
            label: "Sentinel verifier",
            detail: "Closes the empty work set",
            state: stateFor(emptyVerifierRequest, Boolean(emptyCompletion)),
            runId,
            requestId: emptyVerifierRequest.request_id,
            sessionId: emptyVerifierRequest.session_id ?? undefined,
            badges: [
              verifierActivity(
                emptyVerifierRequest,
                false,
                emptyCompletion?.status,
              ),
            ],
          }),
        );
      }
    }
    for (const [
      index,
      { assignment, candidate, completion, verdict, verifierRequest },
    ] of
      candidateWork.entries()) {
      const activity = legacySerialTriage
        ? "activity untracked"
        : verifierActivity(
            verifierRequest,
            Boolean(verdict),
            completion?.status,
          );
      nodes.push(
        node({
          id: `candidate:${candidate.finding_id}`,
          kind: "candidate",
          label: `Candidate ${index + 1}`,
          detail: candidate.title ?? candidate.finding_id,
          state: "done",
          runId,
          sourceDocId: candidate._docID,
          badges: [
            ...(candidate.claimed_severity
              ? [candidate.claimed_severity]
              : []),
            ...(candidate.area_id ? [candidate.area_id] : []),
            activity,
          ],
        }),
      );
      if (assignment) {
        nodes.push(
          node({
            id: `verification-assignment:${candidate.finding_id}`,
            kind: "verification-assignment",
            label: `Assignment ${index + 1}`,
            detail: assignment.assignment_id,
            state: "done",
            runId,
            sourceDocId: assignment._docID,
            badges: assignment.status ? [assignment.status] : [],
          }),
        );
      }
      if (verifierRequest) {
        nodes.push(
          node({
            id: `verifier:${candidate.finding_id}`,
            kind: "verifier",
            label: `Verifier ${index + 1}`,
            detail: candidate.finding_id,
            state: stateFor(
              verifierRequest,
              Boolean(verdict) || Boolean(completion),
            ),
            runId,
            requestId: verifierRequest.request_id,
            sessionId: verifierRequest.session_id ?? undefined,
            badges: [
              verifierActivity(
                verifierRequest,
                Boolean(verdict),
                completion?.status,
              ),
            ],
          }),
        );
      }
      if (verdict) {
        nodes.push(
          node({
            id: `verdict:${candidate.finding_id}`,
            kind: "verdict",
            label: `Verdict ${index + 1}`,
            detail: verdict.title ?? candidate.finding_id,
            state: "done",
            runId,
            sourceDocId: verdict._docID,
            badges: [
              ...(verdict.verdict ? [verdict.verdict] : []),
              ...(verdict.severity ? [verdict.severity] : []),
            ],
          }),
        );
      }
    }
  }

  for (const { index, row: assignment } of rowsBeyondParentMultiplicity(
    verificationAssignments,
    (row) => row.finding_id,
    candidates.length > 0 ? candidates.map((row) => row.finding_id) : ["none"],
  )) {
    const completion = verificationCompletions.find(
      (row) => row.assignment_id === assignment.assignment_id,
    );
    const verifierRequest = verifierRequestFor(
      requests,
      triageRequest?.request_id,
      assignment.finding_id,
      assignment._docID,
    );
    nodes.push(
      node({
        id: `verification-assignment:orphan:${assignment._docID ?? index}`,
        kind: "verification-assignment",
        label: "Orphan assignment",
        detail: assignment.assignment_id,
        state: "done",
        runId,
        sourceDocId: assignment._docID,
        badges: [
          "orphan/duplicate ledger row",
          ...(assignment.status ? [assignment.status] : []),
        ],
      }),
    );
    if (verifierRequest || completion) {
      nodes.push(
        node({
          id: `verifier:orphan:${assignment._docID ?? index}`,
          kind: "verifier",
          label: "Orphan verifier",
          detail: assignment.finding_id,
          state: stateFor(verifierRequest, Boolean(completion)),
          runId,
          requestId: verifierRequest?.request_id,
          sessionId: verifierRequest?.session_id ?? undefined,
          badges: [
            "orphan/duplicate ledger row",
            verifierActivity(verifierRequest, false, completion?.status),
          ],
        }),
      );
    }
  }
  for (const { index, row: completion } of rowsBeyondParentMultiplicity(
    verificationCompletions,
    (row) => row.assignment_id,
    verificationAssignments.map((row) => row.assignment_id),
  )) {
    nodes.push(
      node({
        id: `verifier-completion:orphan:${completion._docID ?? index}`,
        kind: "verifier",
        label: "Orphan verifier completion",
        detail: completion.assignment_id,
        state: "done",
        runId,
        sourceDocId: completion._docID,
        badges: [
          "orphan/duplicate ledger row",
          ...(completion.status ? [completion.status.replaceAll("_", " ")] : []),
        ],
      }),
    );
  }
  for (const { index, row: verdict } of rowsBeyondParentMultiplicity(
    verdicts,
    (row) => row.finding_id,
    candidates.map((row) => row.finding_id),
  )) {
    nodes.push(
      node({
        id: `verdict:orphan:${verdict._docID ?? index}`,
        kind: "verdict",
        label: "Orphan verdict",
        detail: verdict.finding_id,
        state: "done",
        runId,
        sourceDocId: verdict._docID,
        badges: [
          "orphan/duplicate ledger row",
          ...(verdict.verdict ? [verdict.verdict] : []),
        ],
      }),
    );
  }

  const contractAwarePipeline = Boolean(
    snapshot.contractPipelineAvailable ||
      clusters.length > 0 ||
      contractReviews.length > 0 ||
      clusterRequest ||
      requests.some((request) =>
        [
          "defend-contract-review",
          "defend-remediation-plan",
          "defend-patch-validation",
          "defend-patch-security-review",
        ].includes(request.caused_by_trigger_id ?? ""),
      ),
  );
  if (contractAwarePipeline) {
    nodes.push(
      node({
        id: `cluster-plan:${runId}`,
        kind: "cluster-plan",
        label: "Root-cause reducer",
        state: triage
          ? stateFor(clusterRequest, clusters.length > 0)
          : "waiting-group",
        runId,
        requestId: clusterRequest?.request_id,
        sessionId: clusterRequest?.session_id ?? undefined,
        badges: clusters.length > 0 ? [`${clusters.length} clusters`] : [],
      }),
    );
    if (clusters.length === 0) {
      nodes.push(
        node({
          id: "cluster:pending",
          kind: "cluster",
          label: "Root-cause cluster",
          state: "expected",
          runId,
        }),
        node({
          id: "contract-review:pending",
          kind: "contract-review",
          label: "Contract review",
          state: "expected",
          runId,
        }),
      );
    } else {
      for (const [index, cluster] of clusters.entries()) {
        const contractReview = contractReviews.find(
          (row) => row.cluster_id === cluster.cluster_id,
        );
        const contractRequest = requestFor(
          requests,
          "defend-contract-review",
          cluster._docID,
        );
        nodes.push(
          node({
            id: `cluster:${cluster.cluster_id}`,
            kind: "cluster",
            label: `Root cause ${index + 1}`,
            detail: cluster.canonical_title ?? cluster.cluster_id,
            state: "done",
            runId,
            sourceDocId: cluster._docID,
            badges: [
              ...(cluster.status ? [cluster.status] : []),
              ...(cluster.severity ? [cluster.severity] : []),
              ...(cluster.member_finding_ids
                ? [`${memberCount(cluster.member_finding_ids)} findings`]
                : []),
            ],
          }),
          node({
            id: `contract-review:${cluster.cluster_id}`,
            kind: "contract-review",
            label: `Contract ${index + 1}`,
            detail: cluster.cluster_id,
            state: stateFor(contractRequest, Boolean(contractReview)),
            runId,
            requestId: contractRequest?.request_id,
            sessionId: contractRequest?.session_id ?? undefined,
            sourceDocId: contractReview?._docID,
            badges: contractReview?.disposition
              ? [contractReview.disposition]
              : [],
          }),
        );
      }
    }
    const expectedContracts = positiveInt(
      clusters[0]?.expected_total,
      clusters.length || 1,
    );
    const contractsClosed =
      totalsClose(clusters, expectedContracts) &&
      totalsClose(contractReviews, expectedContracts) &&
      sameIds(
        clusters.map((row) => row.cluster_id),
        contractReviews.map((row) => row.cluster_id),
      );
    const contractsInconsistent =
      clusters.length >= expectedContracts &&
      contractReviews.length >= expectedContracts &&
      !contractsClosed;
    nodes.push(
      node({
        id: `remediation-plan:${runId}`,
        kind: "remediation-plan",
        label: "Remediation work set",
        state: remediationPlanRequest
          ? stateFor(remediationPlanRequest, assignments.length > 0)
          : contractsClosed
            ? stateFor(remediationPlanRequest, assignments.length > 0)
            : "waiting-group",
        runId,
        requestId: remediationPlanRequest?.request_id,
        sessionId: remediationPlanRequest?.session_id ?? undefined,
        badges: [
          `${contractReviews.length}/${expectedContracts} contracts`,
          ...(assignments.length > 0
            ? [`${assignments.length} assignments`]
            : []),
          ...(contractsInconsistent ? ["inconsistent contract ledger"] : []),
        ],
      }),
    );
  }

  for (const { index, row: review } of rowsBeyondParentMultiplicity(
    contractReviews,
    (row) => row.cluster_id,
    clusters.map((row) => row.cluster_id),
  )) {
    nodes.push(
      node({
        id: `contract-review:orphan:${review._docID ?? index}`,
        kind: "contract-review",
        label: "Orphan contract review",
        detail: review.review_id,
        state: "done",
        runId,
        sourceDocId: review._docID,
        badges: [
          "orphan/duplicate ledger row",
          ...(review.disposition ? [review.disposition] : []),
        ],
      }),
    );
  }

  if (assignments.length === 0) {
    nodes.push(
      node({
        id: "assignment:pending",
        kind: "assignment",
        label: "Patch set",
        state: "expected",
        runId,
      }),
      node({
        id: "patch:pending",
        kind: "patch",
        label: "Draft",
        state: "expected",
        runId,
      }),
      ...(contractAwarePipeline
        ? [
            node({
              id: "validation:pending",
              kind: "validation",
              label: "Validation",
              state: "expected",
              runId,
            }),
          ]
        : []),
      node({
        id: "review:pending",
        kind: "review",
        label: "Review",
        state: "expected",
        runId,
      }),
      ...(contractAwarePipeline
        ? [
            node({
              id: "security-review:pending",
              kind: "security-review",
              label: "Re-attack",
              state: "expected",
              runId,
            }),
          ]
        : []),
    );
  } else {
    for (const [index, assignment] of assignments.entries()) {
      const patch = patches.find(
        (row) => row.patch_id === assignment.assignment_id,
      );
      const review = reviews.find(
        (row) => row.patch_id === assignment.assignment_id,
      );
      const validation = validations.find(
        (row) => row.patch_id === assignment.assignment_id,
      );
      const securityReview = securityReviews.find(
        (row) => row.patch_id === assignment.assignment_id,
      );
      const patchRequest = requestFor(
        requests,
        "defend-patch",
        assignment._docID,
      );
      const reviewRequest = requestFor(
        requests,
        "defend-patch-review",
        validation?._docID,
      );
      const validationRequest = requestFor(
        requests,
        "defend-patch-validation",
        patch?._docID,
      );
      const securityReviewRequest = requestFor(
        requests,
        "defend-patch-security-review",
        review?._docID,
      );
      nodes.push(
        node({
          id: `assignment:${assignment.assignment_id}`,
          kind: "assignment",
          label: `Remediation ${index + 1}`,
          detail: assignment.cluster_id ?? assignment.finding_id,
          state: "done",
          runId,
          sourceDocId: assignment._docID,
          badges: assignment.status ? [assignment.status] : [],
        }),
        node({
          id: `patch:${assignment.assignment_id}`,
          kind: "patch",
          label: `Patch ${index + 1}`,
          detail: assignment.finding_id,
          state: stateFor(patchRequest, Boolean(patch)),
          runId,
          requestId: patchRequest?.request_id,
          sessionId: patchRequest?.session_id ?? undefined,
          sourceDocId: patch?._docID,
          badges: patch?.status ? [patch.status] : [],
        }),
        ...(contractAwarePipeline
          ? [
              node({
                id: `validation:${assignment.assignment_id}`,
                kind: "validation",
                label: `Validate ${index + 1}`,
                detail: assignment.cluster_id ?? assignment.finding_id,
                state: stateFor(validationRequest, Boolean(validation)),
                runId,
                requestId: validationRequest?.request_id,
                sessionId: validationRequest?.session_id ?? undefined,
                sourceDocId: validation?._docID,
                badges: [
                  ...(validation?.status ? [validation.status] : []),
                  ...(validation?.applies_cleanly
                    ? [`applies ${validation.applies_cleanly}`]
                    : []),
                  ...(validation?.provenance_match
                    ? [`provenance ${validation.provenance_match}`]
                    : []),
                ],
              }),
            ]
          : []),
        node({
          id: `review:${assignment.assignment_id}`,
          kind: "review",
          label: `Review ${index + 1}`,
          detail: assignment.finding_id,
          state: stateFor(reviewRequest, Boolean(review)),
          runId,
          requestId: reviewRequest?.request_id,
          sessionId: reviewRequest?.session_id ?? undefined,
          sourceDocId: review?._docID,
          badges: [
            ...(review?.verdict ? [review.verdict] : []),
            ...(review?.receipt_match
              ? [`receipt ${review.receipt_match}`]
              : []),
          ],
        }),
        ...(contractAwarePipeline
          ? [
              node({
                id: `security-review:${assignment.assignment_id}`,
                kind: "security-review",
                label: `Re-attack ${index + 1}`,
                detail: assignment.cluster_id ?? assignment.finding_id,
                state: stateFor(securityReviewRequest, Boolean(securityReview)),
                runId,
                requestId: securityReviewRequest?.request_id,
                sessionId: securityReviewRequest?.session_id ?? undefined,
                sourceDocId: securityReview?._docID,
                badges: [
                  ...(securityReview?.verdict ? [securityReview.verdict] : []),
                  ...(securityReview?.receipt_match
                    ? [`receipt ${securityReview.receipt_match}`]
                    : []),
                ],
              }),
            ]
          : []),
      );
    }
  }

  for (const { index, row: patch } of rowsBeyondParentMultiplicity(
    patches,
    (row) => row.patch_id,
    assignments.map((row) => row.assignment_id),
  )) {
    const validationRequest = requestFor(
      requests,
      "defend-patch-validation",
      patch._docID,
    );
    nodes.push(
      node({
        id: `patch:orphan:${patch._docID ?? index}`,
        kind: "patch",
        label: "Orphan patch",
        detail: patch.patch_id,
        state: "done",
        runId,
        sourceDocId: patch._docID,
        badges: [
          "orphan/duplicate ledger row",
          ...(patch.status ? [patch.status] : []),
        ],
      }),
    );
    if (validationRequest) {
      nodes.push(
        node({
          id: `validation:orphan-request:${patch._docID ?? index}`,
          kind: "validation",
          label: "Orphan validation request",
          detail: patch.patch_id,
          state: stateFor(validationRequest, false),
          runId,
          requestId: validationRequest.request_id,
          sessionId: validationRequest.session_id ?? undefined,
          badges: ["orphan/duplicate ledger row"],
        }),
      );
    }
  }
  for (const { index, row: validation } of rowsBeyondParentMultiplicity(
    validations,
    (row) => row.patch_id,
    assignments.map((row) => row.assignment_id),
  )) {
    const sourcePatch = patches.find((row) => row.patch_id === validation.patch_id);
    const validationRequest = requestFor(
      requests,
      "defend-patch-validation",
      sourcePatch?._docID,
    );
    const reviewRequest = requestFor(
      requests,
      "defend-patch-review",
      validation._docID,
    );
    nodes.push(
      node({
        id: `validation:orphan:${validation._docID ?? index}`,
        kind: "validation",
        label: "Orphan validation",
        detail: validation.validation_id,
        state: stateFor(validationRequest, true),
        runId,
        requestId: validationRequest?.request_id,
        sessionId: validationRequest?.session_id ?? undefined,
        sourceDocId: validation._docID,
        badges: [
          "orphan/duplicate ledger row",
          ...(validation.status ? [validation.status] : []),
        ],
      }),
    );
    if (reviewRequest) {
      nodes.push(
        node({
          id: `review:orphan-request:${validation._docID ?? index}`,
          kind: "review",
          label: "Orphan review request",
          detail: validation.patch_id,
          state: stateFor(reviewRequest, false),
          runId,
          requestId: reviewRequest.request_id,
          sessionId: reviewRequest.session_id ?? undefined,
          badges: ["orphan/duplicate ledger row"],
        }),
      );
    }
  }
  for (const { index, row: review } of rowsBeyondParentMultiplicity(
    reviews,
    (row) => row.patch_id,
    assignments.map((row) => row.assignment_id),
  )) {
    const sourceValidation = validations.find(
      (row) => row.patch_id === review.patch_id,
    );
    const reviewRequest = requestFor(
      requests,
      "defend-patch-review",
      sourceValidation?._docID,
    );
    const securityRequest = requestFor(
      requests,
      "defend-patch-security-review",
      review._docID,
    );
    nodes.push(
      node({
        id: `review:orphan:${review._docID ?? index}`,
        kind: "review",
        label: "Orphan patch review",
        detail: review.patch_id,
        state: stateFor(reviewRequest, true),
        runId,
        requestId: reviewRequest?.request_id,
        sessionId: reviewRequest?.session_id ?? undefined,
        sourceDocId: review._docID,
        badges: [
          "orphan/duplicate ledger row",
          ...(review.verdict ? [review.verdict] : []),
        ],
      }),
    );
    if (securityRequest) {
      nodes.push(
        node({
          id: `security-review:orphan-request:${review._docID ?? index}`,
          kind: "security-review",
          label: "Orphan re-attack request",
          detail: review.patch_id,
          state: stateFor(securityRequest, false),
          runId,
          requestId: securityRequest.request_id,
          sessionId: securityRequest.session_id ?? undefined,
          badges: ["orphan/duplicate ledger row"],
        }),
      );
    }
  }
  for (const { index, row: review } of rowsBeyondParentMultiplicity(
    securityReviews,
    (row) => row.patch_id,
    assignments.map((row) => row.assignment_id),
  )) {
    const sourceReview = reviews.find((row) => row.patch_id === review.patch_id);
    const securityRequest = requestFor(
      requests,
      "defend-patch-security-review",
      sourceReview?._docID,
    );
    nodes.push(
      node({
        id: `security-review:orphan:${review._docID ?? index}`,
        kind: "security-review",
        label: "Orphan security review",
        detail: review.patch_id,
        state: stateFor(securityRequest, true),
        runId,
        requestId: securityRequest?.request_id,
        sessionId: securityRequest?.session_id ?? undefined,
        sourceDocId: review._docID,
        badges: [
          "orphan/duplicate ledger row",
          ...(review.verdict ? [review.verdict] : []),
        ],
      }),
    );
  }

  const expectedSecurityReviews = positiveInt(
    securityReviews[0]?.expected_total ?? assignments[0]?.expected_total,
    assignments.length || 1,
  );
  const securityLedgerClosed =
    totalsClose(securityReviews, expectedSecurityReviews) &&
    sameIds(
      assignments.map((row) => row.assignment_id),
      securityReviews.map((row) => row.patch_id),
    );
  const reviewsClosed = contractAwarePipeline
    ? assignments.length > 0 && securityLedgerClosed
    : assignments.length > 0 && reviews.length === assignments.length;
  nodes.push(
    node({
      id: `report:${runId}`,
      kind: "report",
      label: "Defense report",
      state: reportRequest
        ? stateFor(reportRequest, Boolean(report))
        : reviewsClosed
          ? stateFor(reportRequest, Boolean(report))
          : "waiting-group",
      runId,
      requestId: reportRequest?.request_id,
      sessionId: reportRequest?.session_id ?? undefined,
      sourceDocId: report?._docID,
      badges: report
        ? [
            ...(report.audit_status ? [report.audit_status] : []),
            `${report.confirmed_count ?? "0"} confirmed`,
            `${report.accepted_patch_count ?? "0"} accepted`,
          ]
        : [],
    }),
  );

  return { runId, nodes };
}

function memberCount(value: string): number {
  const trimmed = value.trim();
  if (!trimmed || trimmed === "none") {
    return 0;
  }
  try {
    const parsed = JSON.parse(trimmed);
    if (Array.isArray(parsed)) {
      return parsed.length;
    }
  } catch {
    // Newline/comma-delimited ids are also accepted by the pack schema.
  }
  return trimmed.split(/[\n,]+/).filter((item) => item.trim()).length;
}

function totalsClose(
  rows: Array<{ expected_total?: string }>,
  expected: number,
): boolean {
  return (
    expected > 0 &&
    rows.length === expected &&
    rows.every((row) => positiveInt(row.expected_total, 0) === expected)
  );
}

function uniqueIds(ids: string[]): boolean {
  return new Set(ids).size === ids.length;
}

function sameIds(left: string[], right: string[]): boolean {
  const sortedLeft = left.slice().sort();
  const sortedRight = right.slice().sort();
  return (
    uniqueIds(sortedLeft) &&
    uniqueIds(sortedRight) &&
    sortedLeft.length === sortedRight.length &&
    sortedLeft.every((id, index) => id === sortedRight[index])
  );
}

function skeleton(runId: string | null, areaCount: number): DefenseGraph {
  const id = runId ?? "pending";
  const nodes: DefenseNode[] = [
    node({
      id: `job:${id}`,
      kind: "job",
      label: "Defense job",
      state: "expected",
      runId: id,
    }),
    node({
      id: `threat:${id}`,
      kind: "threat",
      label: "Threat model",
      state: "expected",
      runId: id,
    }),
    node({
      id: `plan:${id}`,
      kind: "plan",
      label: "Plan areas",
      state: "expected",
      runId: id,
    }),
  ];
  for (let index = 0; index < areaCount; index += 1) {
    nodes.push(
      node({
        id: `area:pending-${index}`,
        kind: "area",
        label: `Area ${index + 1}`,
        state: "expected",
        runId: id,
      }),
    );
  }
  nodes.push(
    node({
      id: `triage:${id}`,
      kind: "triage",
      label: "Adversarial triage",
      state: "expected",
      runId: id,
    }),
    node({
      id: `cluster-plan:${id}`,
      kind: "cluster-plan",
      label: "Root-cause reducer",
      state: "expected",
      runId: id,
    }),
    node({
      id: "cluster:pending",
      kind: "cluster",
      label: "Root-cause cluster",
      state: "expected",
      runId: id,
    }),
    node({
      id: "contract-review:pending",
      kind: "contract-review",
      label: "Contract review",
      state: "expected",
      runId: id,
    }),
    node({
      id: `remediation-plan:${id}`,
      kind: "remediation-plan",
      label: "Remediation work set",
      state: "expected",
      runId: id,
    }),
    node({
      id: "assignment:pending",
      kind: "assignment",
      label: "Patch set",
      state: "expected",
      runId: id,
    }),
    node({
      id: "patch:pending",
      kind: "patch",
      label: "Draft",
      state: "expected",
      runId: id,
    }),
    node({
      id: "validation:pending",
      kind: "validation",
      label: "Validation",
      state: "expected",
      runId: id,
    }),
    node({
      id: "review:pending",
      kind: "review",
      label: "Review",
      state: "expected",
      runId: id,
    }),
    node({
      id: "security-review:pending",
      kind: "security-review",
      label: "Re-attack",
      state: "expected",
      runId: id,
    }),
    node({
      id: `report:${id}`,
      kind: "report",
      label: "Defense report",
      state: "expected",
      runId: id,
    }),
  );
  return { runId, nodes };
}

function positiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}
