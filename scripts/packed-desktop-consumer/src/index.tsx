import type { ReactNode } from "react";

import { ChatComposer, projectChatShell } from "@source-inc/gents-desktop-chat";
import {
  createDesktopClient,
  createDesktopStore,
  NARROW_BREAKPOINT_PX,
  type DesktopClientSnapshot,
} from "@source-inc/gents-desktop-client";
import type { BridgeContract } from "@source-inc/gents-desktop-client/generated/BridgeContract";
import {
  FleetDashboard,
  parsePeerConnectionJson,
} from "@source-inc/gents-desktop-fleet";
import {
  InferenceSetupWizard,
  LocalRuntimeConnect,
} from "@source-inc/gents-desktop-fleet/local-runtime";
import { HoldsPanel } from "@source-inc/gents-desktop-operations";
import {
  ConfirmDialog,
  CopyButton,
  formatMessageTime,
} from "@source-inc/gents-desktop-ui";

const client = createDesktopClient();
const store = createDesktopStore(client);
const generatedContract: BridgeContract | null = null;
const publicSnapshot: DesktopClientSnapshot | null = null;
const timestamp = formatMessageTime("2026-07-27T00:00:00Z");
const parsed = parsePeerConnectionJson(
  JSON.stringify({
    agent_did: "did:key:z6MkPackedConsumer",
    p2p_shareable_address: "/ip4/127.0.0.1/tcp/9171",
  }),
);

const publicComponents: ReactNode[] = [
  <ChatComposer
    activeRequestId={null}
    approxSerializedBytes={0}
    behaviorLabel={null}
    canSend={false}
    configuredPeerCount={0}
    dialedPeerCount={0}
    draft=""
    interruptVisible={false}
    rowCount={0}
    sendHint={null}
    sending={false}
    turnState={null}
    onDraftChange={() => undefined}
    onInterruptClick={() => undefined}
    onSend={() => undefined}
  />,
  <CopyButton getText={() => "packed"} />,
  <ConfirmDialog
    open={false}
    title="Packed"
    message="Consumer"
    onCancel={() => undefined}
    onConfirm={() => undefined}
  />,
  <HoldsPanel agentDid={null} api={client.api} />,
];

void FleetDashboard;
void InferenceSetupWizard;
void LocalRuntimeConnect;
void projectChatShell;
void store;
void generatedContract;
void publicSnapshot;
void timestamp;
void parsed;
void publicComponents;
void NARROW_BREAKPOINT_PX;
