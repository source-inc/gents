import type { BackendHealth } from "@source-inc/gents-desktop-client";
import { deriveDisplayState } from "@source-inc/gents-desktop-operations";
import type { DesktopApiAdapter } from "@source-inc/gents-desktop-client";
import type {
  DesktopClientUpdatedHandler,
  DesktopClientUpdatedListenerFactory,
} from "@source-inc/gents-desktop-client";
import type {
  CascadeCancelPreview,
  DesktopClientSnapshot,
  DesktopOperationsSnapshot,
  DesktopSessionSnapshot,
  DeploymentView,
  InitSummary,
  InferenceBackendView,
  InterruptRequestResult,
  MCPServiceHealthView,
  McpServiceProbeResult,
  PeerAddRequest,
  SubagentTreeView,
  TaskRunResult,
  ToolServiceTestResult,
} from "@source-inc/gents-desktop-client";
import type {
  HeldToolCallView,
  ResolveHoldResult,
} from "@source-inc/gents-desktop-client";

const AGENT_DID = "did:key:z6MkBombadilAgent";
const DEFAULT_BEHAVIOR_ID = "default";
const STARTED_AT = "2026-06-17T00:00:00.000Z";
const THIRTY_DAYS_AGO = new Date(Date.now() - 30 * 86_400_000).toISOString();
const TWO_HOURS_AGO = new Date(Date.now() - 2 * 3_600_000).toISOString();

export type DesktopUiHarnessScenario =
  | "default"
  | "empty-fleet"
  | "loading"
  | "bridge-unavailable"
  | "save-error"
  | "backend-health-error"
  | "long-content"
  | "active-turn"
  | "cascade-turn"
  | "coding";

export type DesktopUiHarnessOptions = {
  scenario?: string | null;
};

type DesktopUiHarness = {
  adapter: DesktopApiAdapter;
  listenerFactory: DesktopClientUpdatedListenerFactory;
  scenario: DesktopUiHarnessScenario;
};

export function createDesktopUiHarness(
  options: DesktopUiHarnessOptions = {},
): DesktopUiHarness {
  const scenario = normalizeScenario(options.scenario);
  let heldToolCalls: HeldToolCallView[] = [];
  const listeners = new Set<DesktopClientUpdatedHandler>();
  const sessions = new Map<string, DesktopSessionSnapshot>();
  const sessionLineage = new Map<
    string,
    {
      taskId?: string | null;
      taskName?: string | null;
      triggerId?: string | null;
      triggerKind?: string | null;
    }
  >();
  let requestSeq = 1;
  let sessionSeq = 1;
  let rowCount = 42;
  let deployment = createDeployment();
  let removed = false;

  const greeting =
    scenario === "long-content"
      ? longHarnessMessage()
      : "I am your desktop UI test agent. This seeded turn gives the transcript a stable row for duplicate-message checks.";
  const activeTurn = scenario === "active-turn" || scenario === "cascade-turn";
  sessions.set("session-intro", {
    sessionId: "session-intro",
    agentDid: AGENT_DID,
    behaviorId: DEFAULT_BEHAVIOR_ID,
    title: "introduction-and-greetings",
    createdAt: THIRTY_DAYS_AGO,
    updatedAt: TWO_HOURS_AGO,
    previewText: greeting,
    status: activeTurn ? "processing" : "completed",
    turnState: activeTurn ? "processing" : "completed",
    latestRequestId: "request-intro",
    latestResponse: {
      status: activeTurn ? "streaming" : "completed",
      content: greeting,
      tokenCount: 24,
      materializedMessageSequence: 1,
      materializedAt: STARTED_AT,
      completedAt: activeTurn ? null : STARTED_AT,
      backendId: "backend-openai",
    },
    pendingTurn: null,
    activeResponseOverlay: null,
    timelineItems: [
      {
        kind: "assistantMessage",
        itemKey: "intro-assistant",
        sequence: 1,
        content: greeting,
        timestamp: "2026-06-03T14:05:00Z",
      },
      ...(activeTurn
        ? [
            {
              kind: "toolGroup" as const,
              itemKey: "live-tools",
              messageSequence: 2,
              tools: [
                {
                  itemKey: "live-exec",
                  toolName: "gents_exec",
                  statusKind: "running",
                  presentation: {
                    kind: "command" as const,
                    command: "cargo test -p gents",
                    exitCode: null,
                    timedOut: false,
                    failed: false,
                    durationMs: null,
                    cwd: null,
                    executionMode: "read_only",
                    networkMode: "disabled",
                    stdout: "",
                    stderr: "",
                    fallbackOutput: null,
                  },
                  partialOutputTail:
                    "Compiling gents v0.7.0\ntest lifecycle::claims ... ok\ntest lifecycle::persistence ... ok",
                  partialOutputSeq: 4096,
                },
              ],
            },
          ]
        : []),
      ...(scenario === "coding"
        ? [
            {
              kind: "toolGroup" as const,
              itemKey: "intro-code-tools",
              messageSequence: 1,
              tools: [
                {
                  itemKey: "intro-edit-file",
                  toolName: "edit_file",
                  statusKind: "success",
                  presentation: {
                    kind: "fileEdit" as const,
                    operation: "edit_file",
                    path: "src/parser.rs",
                    created: false,
                    replacementsApplied: 1,
                    diff: [
                      { kind: "del", text: "fn parse() -> Ast { todo!() }" },
                      { kind: "add", text: "fn parse() -> Ast { Ast::default() }" },
                    ],
                    fallbackOutput: null,
                  },
                },
                {
                  itemKey: "intro-bash",
                  toolName: "bash",
                  statusKind: "success",
                  presentation: {
                    kind: "command" as const,
                    command: "cargo test parser",
                    exitCode: 0,
                    timedOut: false,
                    failed: false,
                    durationMs: null,
                    cwd: null,
                    executionMode: "read_only",
                    networkMode: "disabled",
                    stdout: "test result: ok. 2 passed",
                    stderr: "",
                    fallbackOutput: null,
                  },
                },
                {
                  itemKey: "intro-subagent",
                  toolName: "spawn_subagent",
                  statusKind: "success",
                  presentation: {
                    kind: "subagent" as const,
                    action: "spawn",
                    name: "reviewer",
                    childRequestId: "request-reviewer",
                    description: "Review the parser change for correctness.",
                    output: "Child request created.",
                  },
                  awaitMode: "blocking",
                },
                {
                  itemKey: "intro-process",
                  toolName: "spawn_process",
                  statusKind: "running",
                  presentation: {
                    kind: "process" as const,
                    action: "spawn",
                    target: "cargo test --workspace",
                    description: "Run the complete validation suite.",
                    output: null,
                  },
                  awaitMode: "background",
                  cancelPolicy: "cascade",
                },
                {
                  itemKey: "intro-mcp",
                  toolName: "call_tool",
                  statusKind: "success",
                  presentation: {
                    kind: "mcp" as const,
                    serviceId: "github",
                    selectedToolName: "search_issues",
                    arguments: '{"query":"mobile sync"}',
                    output: '{"count":2}',
                  },
                },
              ],
            },
          ]
        : []),
    ],
  });
  syncConversations();

  function notify(reason: string) {
    window.setTimeout(() => {
      for (const listener of listeners) {
        void listener({ reason });
      }
    }, 0);
  }

  function snapshot() {
    const deployments = scenario === "empty-fleet" || removed ? [] : [deployment];
    const health = {
      status: "healthy",
      connectedPeerCount: 1,
      replicatorCount: 1,
      consecutiveFailures: 0,
      lastOkAt: STARTED_AT,
      lastError: null,
      lastFailureAt: null,
    };
    const next: DesktopClientSnapshot = {
      bootstrap: {
        defaultAgentHome: "/tmp/gents-bombadil/agent",
        initAgentName: "Bombadil UI Agent",
        initAgentDid: AGENT_DID,
        initToolCeiling: "ReadWrite",
        initToolRoot: "/tmp/gents-bombadil/workspace",
        desktopHome: "/tmp/gents-bombadil/desktop",
        peerDirectoryPath: "/tmp/gents-bombadil/peers.json",
        nodeDataDir: "/tmp/gents-bombadil/node",
        logFilePath: "/tmp/gents-bombadil/desktop.log",
        agentHomeExists: true,
        desktopHomeExists: true,
        peerDirectoryExists: true,
        savedPeers:
          scenario === "empty-fleet"
            ? []
            : [
                {
                  peerId: deployment.peerId,
                  label: deployment.label,
                  agentDid: deployment.agentDid,
                  addr: deployment.addr,
                  graphql: deployment.graphql,
                  source: deployment.source,
                },
              ],
      },
      client: {
        localPeerId: "peer-bombadil-local",
        listenAddresses: ["/ip4/127.0.0.1/tcp/9292"],
        p2pHealth: health,
        bootstrapErrors: [],
        lastMutationError: null,
        focusedRequestId: null,
        configuredPeerCount: deployments.length,
        dialedPeerCount: deployments.length,
        peerIssueCount: 0,
        rowCount,
        approxSerializedBytes: rowCount * 512,
        deployments,
      },
    };
    return clone(next);
  }

  function syncConversations() {
    deployment = {
      ...deployment,
      conversations: Array.from(sessions.values()).map((session) => ({
        sessionId: session.sessionId,
        title: session.title,
        previewText: session.previewText,
        status: session.status,
        behaviorId: session.behaviorId,
        latestRequestId: session.latestRequestId,
        taskId: sessionLineage.get(session.sessionId)?.taskId ?? null,
        taskName: sessionLineage.get(session.sessionId)?.taskName ?? null,
        triggerId: sessionLineage.get(session.sessionId)?.triggerId ?? null,
        triggerKind: sessionLineage.get(session.sessionId)?.triggerKind ?? null,
        createdAt: session.createdAt ?? THIRTY_DAYS_AGO,
        updatedAt: session.updatedAt ?? TWO_HOURS_AGO,
        turnState: session.turnState,
        messageCount: session.timelineItems.filter(
          (item) => item.kind === "userMessage" || item.kind === "assistantMessage",
        ).length,
        toolCallCount: session.timelineItems.filter((item) => item.kind === "toolGroup")
          .length,
      })),
    };
  }

  function upsertDeployment(nextPeer: PeerAddRequest) {
    deployment = {
      ...deployment,
      peerId: nextPeer.agentDid === AGENT_DID ? deployment.peerId : "peer-added",
      label: nextPeer.label.trim() || deployment.label,
      agentDid: nextPeer.agentDid.trim() || deployment.agentDid,
      addr: nextPeer.addr.trim() || deployment.addr,
      graphql: nextPeer.graphql?.trim() || deployment.graphql,
      agentPrincipal: {
        ...deployment.agentPrincipal,
        agentDid: nextPeer.agentDid.trim() || deployment.agentDid,
        displayName: nextPeer.label.trim() || deployment.label,
      },
    };
  }

  function createSessionFromPrompt(
    prompt: string,
    behaviorId?: string | null,
    lineage?: {
      taskId?: string | null;
      taskName?: string | null;
      triggerId?: string | null;
      triggerKind?: string | null;
    },
  ) {
    const sessionId = `session-${++sessionSeq}`;
    const requestId = `request-${++requestSeq}`;
    const title = prompt.trim().slice(0, 48) || "manual-task-run";
    const response = `Bombadil harness response ${requestSeq}: received "${title}".`;
    const session: DesktopSessionSnapshot = {
      sessionId,
      agentDid: deployment.agentDid,
      behaviorId: behaviorId || deployment.defaultBehaviorId,
      title,
      previewText: response,
      status: "completed",
      turnState: "completed",
      latestRequestId: requestId,
      latestResponse: {
        status: "completed",
        content: response,
        tokenCount: 32,
        materializedMessageSequence: 2,
        materializedAt: new Date().toISOString(),
        completedAt: new Date().toISOString(),
        backendId: "backend-openai",
      },
      pendingTurn: null,
      activeResponseOverlay: null,
      timelineItems: [
        {
          kind: "userMessage",
          itemKey: `${requestId}-user`,
          sequence: 1,
          content: prompt,
          timestamp: new Date().toISOString(),
        },
        {
          kind: "assistantMessage",
          itemKey: `${requestId}-assistant`,
          sequence: 2,
          content: response,
          timestamp: new Date().toISOString(),
        },
      ],
    };
    sessions.set(sessionId, session);
    if (lineage) {
      sessionLineage.set(sessionId, lineage);
    }
    rowCount += 4;
    syncConversations();
    notify("store");
    return { session, requestId };
  }

  const adapter: DesktopApiAdapter = {
    async fetchDesktopSnapshot() {
      if (scenario === "bridge-unavailable") {
        throw new Error(
          "Desktop native bridge is unavailable in the UI harness scenario.",
        );
      }
      if (scenario === "loading") {
        await wait(250);
      }
      return snapshot();
    },
    async initLocalStandardRuntime(request) {
      const label = request.label.trim() || "Bombadil UI Agent";
      const summary: InitSummary = {
        status: "ready",
        source: "bombadil-harness",
        statusEndpoint: "http://127.0.0.1:9181/status",
        agentHome: "/tmp/gents-bombadil/agent",
        desktopHome: "/tmp/gents-bombadil/desktop",
        peerDirectory: "/tmp/gents-bombadil/peers.json",
        label,
        agentName: label,
        agentDid: AGENT_DID,
        graphql: "http://127.0.0.1:9181/api/v0/graphql",
        p2pTransport: "memory",
        p2pPeerId: "peer-bombadil-local",
        p2pListenAddress: "/ip4/127.0.0.1/tcp/9292",
        peerRecordId: "peer-record-bombadil",
        nextSteps: [],
      };
      return summary;
    },
    async startDesktopClient() {
      notify("runtime");
      return snapshot();
    },
    async shutdownDesktopClient() {
      notify("runtime");
      return snapshot();
    },
    async setSelectedAgent() {
      return undefined;
    },
    async addPeer(request) {
      upsertDeployment(request);
      notify("peers");
      return snapshot();
    },
    async pairBearer(request) {
      const label = request.label?.trim() || "Amy";
      upsertDeployment({
        label,
        agentDid: AGENT_DID,
        addr: "127.0.0.1:56000/p2p/amy-bearer-peer",
        graphql: null,
      });
      deployment = {
        ...deployment,
        source: "bearer-pairing",
        graphql: null,
      };
      notify("peers");
      const next = snapshot();
      return {
        ...next,
        pairing: {
          peerId: deployment.peerId,
          label,
          addr: deployment.addr,
          issuerDid: AGENT_DID,
          claimantDid: "did:key:zPhone",
          networkId: "amy-network",
          template: "conversation",
          connected: true,
          claimSubmitted: true,
          endpointPublished: true,
          replicationConfigured: true,
        },
      };
    },
    async removePeer(peerId) {
      if (peerId !== deployment.peerId) {
        throw new Error(`peer ${peerId} not found`);
      }
      removed = true;
      notify("peers");
      return snapshot();
    },
    async renamePeer(peerId, label) {
      if (peerId !== deployment.peerId) {
        throw new Error(`peer ${peerId} not found`);
      }
      deployment = { ...deployment, label };
      notify("peers");
      return snapshot();
    },
    async listWorkspace(subpath) {
      const path = (subpath ?? "").replace(/^\/+|\/+$/g, "");
      if (path.includes("..")) {
        throw new Error("path escapes the workspace root");
      }
      const tree: Record<
        string,
        { name: string; kind: "dir" | "file"; size?: number }[]
      > = {
        "": [
          { name: "src", kind: "dir" },
          { name: "Cargo.toml", kind: "file", size: 812 },
          { name: "README.md", kind: "file", size: 2048 },
        ],
        src: [
          { name: "lib.rs", kind: "file", size: 4096 },
          { name: "main.rs", kind: "file", size: 1024 },
        ],
      };
      const entries = tree[path];
      if (!entries) {
        throw new Error(`cannot list ${path}: no such directory`);
      }
      return {
        root: "/tmp/agent-tool-root",
        subpath: path,
        entries,
        truncated: false,
      };
    },
    async resendRequest(requestId) {
      return { requestId: `${requestId}-resend`, sessionId: "session-intro" };
    },
    async retryRequest(requestId) {
      return {
        requestId: `${requestId}-retry`,
        sessionId: "session-intro",
        agentDid: AGENT_DID,
        behaviorId: DEFAULT_BEHAVIOR_ID,
      };
    },
    async fetchRequestTimeline(agentDid, requestId) {
      if (agentDid !== AGENT_DID) {
        throw new Error(`no deployment for ${agentDid}`);
      }
      return {
        request_id: requestId,
        session_id: "session-intro",
        agent_did: agentDid,
        behavior_id: DEFAULT_BEHAVIOR_ID,
        child_request_ids: [],
        events: [
          {
            kind: "request",
            request_id: requestId,
            lifecycle_state: "Completed",
            timestamp: STARTED_AT,
          },
          {
            kind: "message",
            role: "user",
            content: "hello there",
            sequence: 1,
            session_id: "session-intro",
            timestamp: STARTED_AT,
          },
          {
            kind: "tool_call",
            tool_name: "gents_exec",
            tool_call_id: "tc-1",
            session_id: "session-intro",
            status: "completed",
            lifecycle_state: "Completed",
            args: "{}",
            result: "ok",
            started_at: STARTED_AT,
          },
          {
            kind: "response",
            status: "materialized",
            session_id: "session-intro",
            timestamp: STARTED_AT,
          },
        ],
      };
    },
    async explainToolSurface(agentDid, behaviorId) {
      if (agentDid !== AGENT_DID) {
        throw new Error(
          "tool-surface explanation for remote agents is not yet supported",
        );
      }
      return {
        behaviorId,
        enabled: true,
        toolSelectionId: "default-tools",
        toolSelectionSource: "document",
        toolPolicyVersion: null,
        toolPolicySemantics: "legacy-permissive",
        ceilingSource: "init_json",
        mcpServicesOnline: true,
        surface: {
          tool_names: ["read_file", "list_files", "gents_exec"],
          included: { read_file: ["ceiling allows readonly file tools"] },
          excluded: { write_file: ["ceiling is readonly"] },
          unavailable: {},
          warnings: [],
        },
      };
    },
    async fetchNetworkStatus() {
      return {
        localPeerId: "12D3KooWBombadilLocalPeer",
        listenAddresses: ["/ip4/127.0.0.1/tcp/9292/p2p/12D3KooWBombadilLocalPeer"],
        connectedPeers: [deployment.peerId],
        replicators: [
          {
            peerId: deployment.peerId,
            address: deployment.addr,
            collections: ["AgentRequest", "AgentResponse", "AgentMessage"],
            status: 0,
            lastStatusChange: TWO_HOURS_AGO,
          },
        ],
        savedPeers: [
          {
            peerId: deployment.peerId,
            label: deployment.label,
            addr: deployment.addr,
            agentDid: deployment.agentDid,
            source: deployment.source,
          },
        ],
      };
    },
    async fetchPeerStatus() {
      return {
        label: "Bombadil UI Agent",
        agentDid: AGENT_DID,
        addr: "/ip4/127.0.0.1/tcp/9292",
        graphql: "http://127.0.0.1:9181/api/v0/graphql",
      };
    },
    async probePeerAddress() {
      return {
        label: "Bombadil UI Agent",
        agentDid: AGENT_DID,
        addr: "/ip4/127.0.0.1/tcp/9292",
        graphql: "http://127.0.0.1:9181/api/v0/graphql",
      };
    },
    async repairP2P() {
      notify("runtime");
      return snapshot();
    },
    async fetchSessionSnapshot(sessionId) {
      const session = sessions.get(sessionId);
      return session ? clone(session) : null;
    },
    async sendChatMessage(request) {
      const content = request.content.trim();
      if (!content) {
        throw new Error("message content is required");
      }

      if (request.sessionId && sessions.has(request.sessionId)) {
        const existing = sessions.get(request.sessionId)!;
        const nextSequence = existing.timelineItems.length + 1;
        const requestId = `request-${++requestSeq}`;
        const response = `Bombadil harness response ${requestSeq}: received "${content.slice(
          0,
          48,
        )}".`;
        const updated: DesktopSessionSnapshot = {
          ...existing,
          previewText: response,
          status: "completed",
          turnState: "completed",
          latestRequestId: requestId,
          latestResponse: {
            status: "completed",
            content: response,
            tokenCount: 32,
            materializedMessageSequence: nextSequence + 1,
            materializedAt: new Date().toISOString(),
            completedAt: new Date().toISOString(),
            backendId: "backend-openai",
          },
          timelineItems: [
            ...existing.timelineItems,
            {
              kind: "userMessage",
              itemKey: `${requestId}-user`,
              sequence: nextSequence,
              content,
              timestamp: new Date().toISOString(),
            },
            {
              kind: "assistantMessage",
              itemKey: `${requestId}-assistant`,
              sequence: nextSequence + 1,
              content: response,
              timestamp: new Date().toISOString(),
            },
          ],
        };
        sessions.set(request.sessionId, updated);
        rowCount += 4;
        syncConversations();
        notify("store");
        return {
          sessionId: request.sessionId,
          requestId,
          agentDid: request.agentDid,
          behaviorId: request.behaviorId ?? null,
        };
      }

      const { session, requestId } = createSessionFromPrompt(
        content,
        request.behaviorId,
      );
      const result: TaskRunResult = {
        requestDocId: `${requestId}-doc`,
        requestId,
        sessionId: session.sessionId,
        agentDid: request.agentDid,
        behaviorId: request.behaviorId || DEFAULT_BEHAVIOR_ID,
        status: "completed",
        lifecycleState: "completed",
      };
      return result;
    },
    async renameConversation(request) {
      const session = sessions.get(request.sessionId);
      if (session) {
        sessions.set(request.sessionId, {
          ...session,
          title: request.title.trim() || session.title,
        });
        syncConversations();
        notify("store");
      }
    },
    async saveAgentConfig(request) {
      deployment = {
        ...deployment,
        label: request.displayName.trim() || deployment.label,
        defaultBehaviorId: request.defaultBehaviorId || deployment.defaultBehaviorId,
        agentPrincipal: {
          ...deployment.agentPrincipal,
          displayName: request.displayName.trim() || deployment.label,
          defaultBehaviorId: request.defaultBehaviorId || deployment.defaultBehaviorId,
          enabled: request.enabled ?? deployment.agentPrincipal.enabled,
        },
      };
      return snapshot();
    },
    async saveBehaviorConfig(request) {
      if (scenario === "save-error") {
        throw new Error("Harness rejected behavior save for sad-path coverage.");
      }
      const behaviorId = request.behaviorId.trim() || `behavior-${requestSeq}`;
      const nextBehavior = {
        behaviorId,
        displayName: request.displayName.trim() || behaviorId,
        systemPrompt: request.systemPrompt,
        backendId: request.backendId,
        modelName: null,
        toolSelectionId: request.toolSelectionId,
        inferenceProfileId: request.inferenceProfileId,
        compactionStrategy: request.compactionStrategy,
        compactionThreshold: request.compactionThreshold,
        enabled: request.enabled ?? true,
        isDefault: behaviorId === deployment.defaultBehaviorId,
        skillRefs: request.skillRefs,
        skillExcludes: request.skillExcludes,
      };
      deployment = {
        ...deployment,
        behaviors: upsertBy(
          deployment.behaviors,
          "behaviorId",
          behaviorId,
          nextBehavior,
        ),
      };
      return snapshot();
    },
    async saveSkillConfig(request) {
      const skillId = request.skillId.trim() || `skill-${requestSeq}`;
      const name = request.name.trim() || skillId;
      deployment = {
        ...deployment,
        skills: upsertBy(deployment.skills ?? [], "skillId", skillId, {
          ...request,
          skillId,
          agentDid: request.agentDid || deployment.agentDid,
          scope: request.scope || "behavior",
          name,
          displayName: request.displayName?.trim() || name,
          enabled: request.enabled ?? true,
          createdAt: STARTED_AT,
        }),
      };
      return snapshot();
    },
    async deleteTaskConfig(request) {
      const schedules = deployment.schedules.filter(
        (schedule) => schedule.taskId === request.taskId,
      ).length;
      const triggers = deployment.eventTriggers.filter(
        (trigger) => trigger.taskId === request.taskId,
      ).length;
      if (schedules + triggers > 0) {
        throw new Error(
          `task "${request.taskId}" is referenced by ${schedules} schedule(s) and ${triggers} event trigger(s); delete or detach those first`,
        );
      }
      deployment = {
        ...deployment,
        tasks: deployment.tasks.filter((task) => task.taskId !== request.taskId),
      };
      notify("config");
      return snapshot();
    },
    async deleteScheduleConfig(request) {
      deployment = {
        ...deployment,
        schedules: deployment.schedules.filter(
          (schedule) => schedule.scheduleId !== request.scheduleId,
        ),
      };
      notify("config");
      return snapshot();
    },
    async deleteEventTriggerConfig(request) {
      deployment = {
        ...deployment,
        eventTriggers: deployment.eventTriggers.filter(
          (trigger) => trigger.triggerId !== request.triggerId,
        ),
      };
      notify("config");
      return snapshot();
    },
    async deleteBackendConfig(request) {
      const referencing = deployment.behaviors
        .filter((behavior) => behavior.backendId === request.backendId)
        .map((behavior) => behavior.behaviorId);
      if (referencing.length) {
        throw new Error(
          `backend "${request.backendId}" is referenced by behavior(s) ${referencing.join(", ")}; point them elsewhere first`,
        );
      }
      deployment = {
        ...deployment,
        inferenceBackends: deployment.inferenceBackends.filter(
          (backend) => backend.backendId !== request.backendId,
        ),
      };
      notify("config");
      return snapshot();
    },
    async deleteInferenceProfileConfig(request) {
      const referencing = deployment.behaviors
        .filter((behavior) => behavior.inferenceProfileId === request.profileId)
        .map((behavior) => behavior.behaviorId);
      if (referencing.length) {
        throw new Error(
          `profile "${request.profileId}" is referenced by behavior(s) ${referencing.join(", ")}; point them elsewhere first`,
        );
      }
      deployment = {
        ...deployment,
        inferenceProfiles: deployment.inferenceProfiles.filter(
          (profile) => profile.profileId !== request.profileId,
        ),
      };
      notify("config");
      return snapshot();
    },
    async deleteToolSelectionConfig(request) {
      const referencing = deployment.behaviors
        .filter((behavior) => behavior.toolSelectionId === request.selectionId)
        .map((behavior) => behavior.behaviorId);
      if (referencing.length) {
        throw new Error(
          `tool selection "${request.selectionId}" is referenced by behavior(s) ${referencing.join(", ")}; point them elsewhere first`,
        );
      }
      deployment = {
        ...deployment,
        toolSelections: deployment.toolSelections.filter(
          (selection) => selection.selectionId !== request.selectionId,
        ),
      };
      notify("config");
      return snapshot();
    },
    async deleteToolServiceConfig(request) {
      const referencing = deployment.toolSelections
        .filter((selection) =>
          (selection.allowedMcpServiceIds ?? []).includes(request.serviceId),
        )
        .map((selection) => selection.selectionId);
      if (referencing.length) {
        throw new Error(
          `tool service "${request.serviceId}" is allowed by tool selection(s) ${referencing.join(", ")}; remove it there first`,
        );
      }
      deployment = {
        ...deployment,
        toolServices: deployment.toolServices.filter(
          (service) => service.serviceId !== request.serviceId,
        ),
      };
      notify("config");
      return snapshot();
    },
    async deleteBehaviorConfig(request) {
      const isDefault =
        deployment.behaviors.find(
          (behavior) => behavior.behaviorId === request.behaviorId,
        )?.isDefault ??
        deployment.agentPrincipal.defaultBehaviorId === request.behaviorId;
      if (isDefault) {
        throw new Error(
          `behavior "${request.behaviorId}" is the agent's default behavior; make another behavior the default first`,
        );
      }
      deployment = {
        ...deployment,
        behaviors: deployment.behaviors.filter(
          (behavior) => behavior.behaviorId !== request.behaviorId,
        ),
      };
      notify("config");
      return snapshot();
    },
    async deleteSkillConfig(request) {
      const skillId = request.skillId.trim();
      deployment = {
        ...deployment,
        skills: (deployment.skills ?? []).filter((skill) => skill.skillId !== skillId),
        behaviors: deployment.behaviors.map((behavior) => ({
          ...behavior,
          skillRefs: (behavior.skillRefs ?? []).filter((id) => id !== skillId),
          skillExcludes: (behavior.skillExcludes ?? []).filter((id) => id !== skillId),
        })),
      };
      return snapshot();
    },
    async saveBackendConfig(request) {
      const backendId = request.backendId.trim() || `backend-${requestSeq}`;
      deployment = {
        ...deployment,
        inferenceBackends: upsertBy(
          deployment.inferenceBackends,
          "backendId",
          backendId,
          {
            backendId,
            name: request.name.trim() || backendId,
            providerKind: request.providerKind,
            endpoint: request.endpoint,
            apiKeyConfigured: Boolean(request.apiKey),
            apiKeyEnvVar: request.apiKeyEnvVar,
            maxConcurrent: request.maxConcurrent,
            maxQueueDepth: request.maxQueueDepth,
            enabled: request.enabled ?? true,
            models: request.models,
            probeStatus: "healthy",
          },
        ),
      };
      return snapshot();
    },
    async saveInferenceProfileConfig(request) {
      const profileId = request.profileId.trim() || `profile-${requestSeq}`;
      deployment = {
        ...deployment,
        inferenceProfiles: upsertBy(
          deployment.inferenceProfiles,
          "profileId",
          profileId,
          {
            ...request,
            profileId,
            displayName: request.displayName.trim() || profileId,
          },
        ),
      };
      return snapshot();
    },
    async saveToolSelectionConfig(request) {
      const selectionId = request.selectionId.trim() || `tools-${requestSeq}`;
      const prior = deployment.toolSelections.find(
        (selection) => selection.selectionId === selectionId,
      );
      deployment = {
        ...deployment,
        toolSelections: upsertBy(
          deployment.toolSelections,
          "selectionId",
          selectionId,
          {
            ...prior,
            ...request,
            selectionId,
            displayName: request.displayName.trim() || selectionId,
          },
        ),
      };
      return snapshot();
    },
    async saveToolServiceConfig(request) {
      const serviceId = request.serviceId.trim() || `service-${requestSeq}`;
      deployment = {
        ...deployment,
        toolServiceRegistries: upsertBy(
          deployment.toolServiceRegistries,
          "serviceId",
          serviceId,
          {
            ...request,
            serviceId,
            displayName: request.displayName.trim() || serviceId,
          },
        ),
      };
      return snapshot();
    },
    async testToolService(request) {
      const result: ToolServiceTestResult = {
        serviceId: request.serviceId,
        endpoint: `http://${request.hostname || "localhost"}:${request.mcpPort || 7331}${
          request.mcpPath || "/mcp"
        }`,
        status: "ok",
        toolCount: 1,
        tools: [{ name: "whoami", description: "Returns bound caller identity" }],
      };
      return result;
    },
    async saveTaskConfig(request) {
      const taskId = request.taskId.trim() || `task-${requestSeq}`;
      deployment = {
        ...deployment,
        tasks: upsertBy(deployment.tasks, "taskId", taskId, {
          ...request,
          taskId,
          name: request.name.trim() || taskId,
          recentRuns: {
            totalFires: 0,
            scheduleCount: deployment.schedules.filter((s) => s.taskId === taskId)
              .length,
            eventTriggerCount: deployment.eventTriggers.filter(
              (trigger) => trigger.taskId === taskId,
            ).length,
          },
          runHistory: [],
        }),
      };
      return snapshot();
    },
    async saveScheduleConfig(request) {
      const scheduleId = request.scheduleId.trim() || `schedule-${requestSeq}`;
      deployment = {
        ...deployment,
        schedules: upsertBy(deployment.schedules, "scheduleId", scheduleId, {
          ...request,
          scheduleId,
          fireCount: 0,
        }),
      };
      return snapshot();
    },
    async runSchedule(request) {
      const schedule = deployment.schedules.find(
        (row) => row.scheduleId === request.scheduleId,
      );
      return runHarnessTask(schedule?.taskId ?? "scheduled-task");
    },
    async saveEventTriggerConfig(request) {
      const triggerId = request.triggerId.trim() || `trigger-${requestSeq}`;
      deployment = {
        ...deployment,
        eventTriggers: upsertBy(deployment.eventTriggers, "triggerId", triggerId, {
          ...request,
          triggerId,
          fireCount: 0,
        }),
      };
      return snapshot();
    },
    async runTask(request) {
      return runHarnessTask(request.taskId);
    },
    async listSubagentTree(request) {
      const tree: SubagentTreeView = {
        rootRequestId: request.rootRequestId,
        truncated: false,
        nodes: [
          {
            requestId: request.rootRequestId,
            sessionId: findSessionByRequest(request.rootRequestId)?.sessionId ?? null,
            agentDid: deployment.agentDid,
            behaviorId: deployment.defaultBehaviorId,
            lifecycleState: "completed",
            status: "completed",
            subagentDepth: 0,
          },
        ],
        edges: [],
      };
      return tree;
    },
    async listBackendsWithHealth() {
      if (scenario === "backend-health-error") {
        throw new Error("Harness backend health bridge unavailable.");
      }
      const backends: InferenceBackendView[] = deployment.inferenceBackends;
      const rows: BackendHealth[] = backends.map((backend) => ({
        backendId: backend.backendId,
        name: backend.name ?? backend.backendId,
        providerKind: backend.providerKind ?? "openai",
        endpoint: backend.endpoint ?? "http://127.0.0.1:8000/v1",
        enabled: backend.enabled ?? true,
        probeStatus: backend.probeStatus ?? "healthy",
        displayState: deriveDisplayState(
          backend.enabled ?? true,
          backend.probeStatus ?? "healthy",
        ),
        lastProbe: STARTED_AT,
        maxConcurrent: backend.maxConcurrent ?? 4,
        maxQueueDepth: backend.maxQueueDepth ?? 16,
        models: backend.models,
        recentCalls: [],
      }));
      return rows;
    },
    async listMcpServicesWithHealth() {
      const registries = deployment.toolServiceRegistries;
      const services: MCPServiceHealthView[] = registries.map((service) => ({
        serviceId: service.serviceId,
        agentDid: deployment.agentDid,
        endpoint: `${service.hostname ?? "localhost"}:${service.mcpPort ?? 7331}${
          service.mcpPath ?? "/mcp"
        }`,
        status: "healthy",
        failureCount: 0,
        kMax: 3,
        backoffUntil: null,
        lastProbeAt: STARTED_AT,
        lastSeen: STARTED_AT,
        updatedAt: STARTED_AT,
        lastErrorClass: null,
        lastErrorMessage: null,
      }));
      return services;
    },
    async probeMcpService(serviceId) {
      const result: McpServiceProbeResult = {
        serviceId,
        status: "healthy",
        latencyMs: 3,
        lastError: null,
      };
      return result;
    },
    async fetchOperationsSnapshot(request) {
      const operations: DesktopOperationsSnapshot = {
        fetchedAt: new Date().toISOString(),
        agentDid: request.agentDid ?? deployment.agentDid,
        liveness: {
          expiredProcessingCount: 0,
          requests: [],
          activeToolCalls: [],
          activeNativeExecutorsAvailable: true,
          activeNativeExecutors: [],
        },
        livenessUnavailableReason: null,
        backgroundedTools: [],
        stuckDiagnostics: [],
        lineage: null,
      };
      return operations;
    },
    async previewInterruptCascade(request) {
      const cascadeChildren =
        scenario === "cascade-turn"
          ? [
              {
                requestId: "request-child-1",
                sessionId: "session-child-1",
                behaviorId: "ops",
                lifecycleState: "processing",
                parentRequestId: request.requestId,
                parentToolCallId: "tool-call-child-1",
                toolName: "delegate",
                awaitMode: "foreground",
                cancelPolicy: "cascade",
              },
            ]
          : [];
      const preview: CascadeCancelPreview = {
        rootRequestId: request.requestId,
        previewSignature: `preview-${request.requestId}`,
        rootState: activeTurn ? "processing" : "completed",
        willInterrupt: cascadeChildren,
        willDetach: [],
        alreadyTerminal: [
          ...(activeTurn
            ? []
            : [
                {
                  requestId: request.requestId,
                  lifecycleState: "completed",
                  behaviorId: deployment.defaultBehaviorId,
                },
              ]),
        ],
        unknownPolicy: [],
      };
      return preview;
    },
    async interruptRequest(request) {
      const result: InterruptRequestResult = {
        requestId: request.requestId,
        accepted: true,
        interruptRequestedAt: new Date().toISOString(),
        alreadyInterrupted: false,
        stalePreview: false,
        preview: null,
      };
      return result;
    },
    async listToolCallHolds() {
      return heldToolCalls;
    },
    async resolveToolCallHold(request) {
      const held = heldToolCalls.find((hold) => hold.toolCallId === request.toolCallId);
      if (!held) {
        throw new Error(`tool call ${request.toolCallId} is not awaiting approval`);
      }
      heldToolCalls = heldToolCalls.filter(
        (hold) => hold.toolCallId !== request.toolCallId,
      );
      const result: ResolveHoldResult = {
        approvalId: `approval-${request.toolCallId}-harness`,
        toolCallId: request.toolCallId,
        decision: request.approve ? "approved" : "denied",
      };
      return result;
    },
  };

  const listenerFactory: DesktopClientUpdatedListenerFactory = async (handler) => {
    listeners.add(handler);
    return () => {
      listeners.delete(handler);
    };
  };

  function runHarnessTask(taskId: string): TaskRunResult {
    const task = deployment.tasks.find((row) => row.taskId === taskId);
    const { session, requestId } = createSessionFromPrompt(
      `Run task ${taskId}`,
      deployment.defaultBehaviorId,
      {
        taskId,
        taskName: task?.name ?? taskId,
      },
    );
    return {
      requestDocId: `${requestId}-doc`,
      requestId,
      sessionId: session.sessionId,
      agentDid: deployment.agentDid,
      behaviorId: deployment.defaultBehaviorId ?? DEFAULT_BEHAVIOR_ID,
      status: "completed",
      lifecycleState: "completed",
    };
  }

  function findSessionByRequest(requestId: string) {
    return Array.from(sessions.values()).find(
      (session) => session.latestRequestId === requestId,
    );
  }

  return { adapter, listenerFactory, scenario };
}

function createDeployment(): DeploymentView {
  return {
    peerId: "peer-bombadil-local",
    label: "Bombadil UI Agent",
    agentDid: AGENT_DID,
    addr: "/ip4/127.0.0.1/tcp/9292",
    source: "bombadil-harness",
    graphql: "http://127.0.0.1:9181/api/v0/graphql",
    dialSucceeded: true,
    pairingReady: true,
    lastError: null,
    defaultBehaviorId: DEFAULT_BEHAVIOR_ID,
    agentPrincipal: {
      agentDid: AGENT_DID,
      displayName: "Bombadil UI Agent",
      defaultBehaviorId: DEFAULT_BEHAVIOR_ID,
      enabled: true,
      createdAt: STARTED_AT,
      createdBy: "bombadil",
    },
    runtime: {
      processState: "running",
      reconcilePhase: "idle",
      lastReconcileResult: "ok",
      lastReconcileError: null,
      updatedAt: THIRTY_DAYS_AGO,
      behaviorExecutorCapacity: 4,
      behaviorExecutorQueueDepth: 0,
      runnableBehaviorCount: 2,
      unavailableBehaviorCount: 0,
    },
    behaviors: [
      {
        behaviorId: DEFAULT_BEHAVIOR_ID,
        displayName: "Default",
        systemPrompt: "You are a deterministic UI-test agent.",
        backendId: "backend-openai",
        modelName: "gpt-4.1-mini",
        toolSelectionId: "tools-default",
        inferenceProfileId: "profile-default",
        compactionStrategy: "rolling",
        compactionThreshold: 0.75,
        enabled: true,
        isDefault: true,
        skillRefs: ["host-diagnostics"],
        skillExcludes: [],
      },
      {
        behaviorId: "ops",
        displayName: "Ops",
        systemPrompt: "You inspect runtime and fleet health.",
        backendId: "backend-openai",
        modelName: "gpt-4.1-mini",
        toolSelectionId: "tools-default",
        inferenceProfileId: "profile-default",
        compactionStrategy: "rolling",
        compactionThreshold: 0.75,
        enabled: true,
        isDefault: false,
        skillRefs: [],
        skillExcludes: [],
      },
    ],
    behaviorEnvironments: [
      {
        behaviorId: DEFAULT_BEHAVIOR_ID,
        displayName: "Default",
        enabled: true,
        isDefault: true,
        modelName: "gpt-4.1-mini",
        inferenceProfileName: "Default profile",
        workspaceRoot: "/tmp/gents-bombadil/workspace",
        fileAccess: "read-only",
        bashAccess: "read-only",
        networkAccess: "disabled",
        skillNames: ["Host diagnostics"],
        sessionCount: 0,
        activeSessionCount: 0,
      },
      {
        behaviorId: "ops",
        displayName: "Ops",
        enabled: true,
        isDefault: false,
        modelName: "gpt-4.1-mini",
        inferenceProfileName: "Default profile",
        workspaceRoot: "/tmp/gents-bombadil/workspace",
        fileAccess: "read-only",
        bashAccess: "read-only",
        networkAccess: "disabled",
        skillNames: [],
        sessionCount: 0,
        activeSessionCount: 0,
      },
    ],
    inferenceBackends: [
      {
        backendId: "backend-openai",
        name: "OpenAI Harness",
        providerKind: "openai",
        endpoint: "http://127.0.0.1:8000/v1",
        apiKeyConfigured: true,
        apiKeyEnvVar: "OPENAI_API_KEY",
        maxConcurrent: 4,
        maxQueueDepth: 16,
        enabled: true,
        models: ["gpt-4.1-mini"],
        probeStatus: "healthy",
      },
    ],
    inferenceProfiles: [
      {
        profileId: "profile-default",
        displayName: "Default profile",
        contextWindow: 128000,
        maxOutputTokens: 4096,
        maxTurns: 24,
        temperature: 0.2,
        streamBatchMs: 100,
        streamLivenessTimeoutSecs: 30,
        deadlineDurationSecs: 300,
      },
    ],
    toolSelections: [
      {
        selectionId: "tools-default",
        agentDid: AGENT_DID,
        displayName: "Default tools",
        enableFileTools: true,
        fileToolsMode: "ReadOnly",
        fileToolRoot: "/tmp/gents-bombadil/workspace",
        enableBash: true,
        bashMode: "ReadOnly",
        commandExecutionPolicy: "AllowListed",
        commandAllowedArgvPrefixes: ["rg", "git status", "cargo test"],
        commandForbiddenArgvPrefixes: ["rm -rf", "git reset --hard"],
        commandNetworkMode: "Disabled",
        cliToolNames: ["rg", "git"],
        enableMetaTools: true,
        allowedMcpServiceIds: ["mcp-observability"],
        delegateTo: [],
        backgroundableToolNames: ["cargo test"],
        subagentTargets: [],
        subagentSpawnEnabled: true,
        subagentSteeringEnabled: true,
        subagentBackgroundEnabled: true,
        crossDeploymentSpawnTimeoutSeconds: 30,
        enableMemory: false,
        enableSessionHistoryTool: true,
        enableDefraQuery: true,
        defraQueryCollections: ["AgentRequest", "AgentResponse"],
        writeTools: [
          '{"tool_name":"upsert_note","collection":"Note","description":"","fields":[]}',
          '{"tool_name":"delete_task","collection":"Task","description":"","fields":[]}',
        ],
        toolPolicyVersion: "tool-policy/v1",
      },
    ],
    toolServiceRegistries: [
      {
        serviceId: "mcp-observability",
        displayName: "Observability MCP",
        description: "Fleet health MCP service",
        hostname: "localhost",
        tailscaleIp: null,
        lanIp: "127.0.0.1",
        mcpPort: 7331,
        mcpPath: "/mcp",
        status: "healthy",
        version: "0.1.0",
        updatedAt: STARTED_AT,
      },
    ],
    skills: [
      {
        skillId: "host-diagnostics",
        agentDid: AGENT_DID,
        scope: "behavior",
        name: "Host diagnostics",
        description: "Inspect host health and write a concise operational report.",
        instructions: "Inspect host health, telemetry freshness, and recent errors.",
        toolRefs: ["mcp-observability.inspect_host", "mcp-observability.query_logs"],
        displayName: "Host diagnostics",
        enabled: true,
        createdAt: STARTED_AT,
      },
      {
        skillId: "fleet-summary",
        agentDid: AGENT_DID,
        scope: "principal",
        name: "Fleet summary",
        description: "Summarize fleet state for operator handoff.",
        instructions:
          "Compare backend health, silent hosts, and posted steward status.",
        toolRefs: ["mcp-observability.fleet_status"],
        displayName: "Fleet summary",
        enabled: true,
        createdAt: STARTED_AT,
      },
    ],
    tasks: [
      {
        taskId: "host-check",
        name: "Host check",
        description: "Inspect host health and summarize findings.",
        behaviorId: DEFAULT_BEHAVIOR_ID,
        promptTemplate: "Inspect this host and report health.",
        enabled: true,
        outputSchemaRef: null,
        recentRuns: {
          totalFires: 0,
          lastAttemptAt: null,
          lastStatus: null,
          lastError: null,
          scheduleCount: 1,
          eventTriggerCount: 0,
        },
        runHistory: [],
      },
    ],
    schedules: [
      {
        scheduleId: "host-check-every-6h",
        taskId: "host-check",
        intervalSecs: 21600,
        enabled: true,
        concurrency: "serial",
        nextRunAt: null,
        lastAttemptAt: null,
        lastStatus: null,
        lastError: null,
        fireCount: 0,
      },
    ],
    eventTriggers: [],
    conversations: [],
  };
}

function upsertBy<T extends Record<K, string>, K extends keyof T>(
  rows: T[],
  key: K,
  value: string,
  next: T,
) {
  const index = rows.findIndex((row) => row[key] === value);
  if (index < 0) {
    return [...rows, next];
  }
  return rows.map((row, i) => (i === index ? next : row));
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function normalizeScenario(value?: string | null): DesktopUiHarnessScenario {
  switch (value) {
    case "empty-fleet":
    case "loading":
    case "bridge-unavailable":
    case "save-error":
    case "backend-health-error":
    case "long-content":
    case "active-turn":
    case "cascade-turn":
    case "coding":
      return value;
    default:
      return "default";
  }
}

function longHarnessMessage() {
  return [
    "I am your desktop UI test agent with a deliberately long transcript row.",
    "This content exercises wrapping, scrolling, markdown layout, and composer stability without relying on a real model.",
    "The message keeps going so deterministic browser tests can capture a stable long-content chat state.",
    "Observation: fleet healthy, backend available, MCP service reachable, no duplicate assistant rows expected.",
  ].join("\n\n");
}

function wait(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}
