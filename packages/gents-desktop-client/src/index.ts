export {
  assertCompatibleBridgeContract,
  createDesktopClient,
  MINIMUM_BRIDGE_CONTRACT_VERSION,
  PACKAGE_VERSION,
  type DesktopBridgeContract,
  type DesktopClient,
} from "./client.js";
export {
  createDesktopStore,
  DEFAULT_TIMING,
  type DesktopStore,
  type DesktopStoreState,
  type TimingConfig,
} from "./store.js";
export {
  tauriTransport,
  bridgeCommand,
  type DesktopTransport,
  type ClientUpdateEvent,
  type Unlisten,
} from "./transport.js";
export {
  BridgeInvokeError,
  asBridgeErrorPayload,
  normalizeInvokeError,
  type BridgeErrorPayload,
} from "./errors.js";
export * from "./api.js";
export * from "./events.js";
export * from "./types.js";

export const NARROW_BREAKPOINT_PX = 760;
