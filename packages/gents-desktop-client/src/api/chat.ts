import { getDesktopApiAdapter } from "./adapter.js";
import type { DesktopApiAdapter } from "./types.js";

export function fetchSessionSnapshot(
  sessionId: string,
  agentDid?: string | null,
  requestId?: string | null,
  api?: DesktopApiAdapter,
) {
  return getDesktopApiAdapter(api).fetchSessionSnapshot(
    sessionId,
    agentDid,
    requestId,
  );
}

export function sendChatMessage(
  request: {
    agentDid: string;
    behaviorId?: string | null;
    sessionId?: string | null;
    content: string;
  },
  api?: DesktopApiAdapter,
) {
  return getDesktopApiAdapter(api).sendChatMessage(request);
}

export function renameConversation(
  request: { agentDid: string; sessionId: string; title: string },
  api?: DesktopApiAdapter,
) {
  return getDesktopApiAdapter(api).renameConversation(request);
}

export function resendRequest(requestId: string, api?: DesktopApiAdapter) {
  return getDesktopApiAdapter(api).resendRequest(requestId);
}

export function retryRequest(requestId: string, api?: DesktopApiAdapter) {
  return getDesktopApiAdapter(api).retryRequest(requestId);
}
