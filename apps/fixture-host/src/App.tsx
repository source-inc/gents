import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
  type FormEvent,
} from "react";
import { invoke } from "@tauri-apps/api/core";
import {
  createDesktopClient,
  createDesktopStore,
  type DesktopSessionSnapshot,
} from "@source-inc/gents-desktop-client";
import {
  ChatComposer,
  ChatTranscriptPanel,
} from "@source-inc/gents-desktop-chat";
import { FleetDashboard } from "@source-inc/gents-desktop-fleet";

const DOMAIN = "fixture-domain";

function domainCmd(name: string) {
  return `plugin:${DOMAIN}|${name}`;
}

export function App() {
  const bridge = useMemo(() => createDesktopClient(), []);
  const store = useMemo(() => createDesktopStore(bridge), [bridge]);
  const storeState = useSyncExternalStore(store.subscribe, store.getState);
  const [log, setLog] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [draft, setDraft] = useState("");
  const [session, setSession] = useState<DesktopSessionSnapshot | null>(null);

  useEffect(
    () => () => {
      void store.stop();
    },
    [store],
  );

  const push = useCallback((line: string) => {
    setLog((prev) => [line, ...prev].slice(0, 40));
  }, []);

  const run = useCallback(
    async <T,>(label: string, fn: () => Promise<T>): Promise<T> => {
      setBusy(true);
      setError(null);
      try {
        const result = await fn();
        push(`${label}: ${JSON.stringify(result).slice(0, 400)}`);
        return result;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        push(`${label} ERROR: ${message}`);
        throw err;
      } finally {
        setBusy(false);
      }
    },
    [push],
  );

  const runQuiet = useCallback(
    (label: string, fn: () => Promise<unknown>) => {
      void run(label, fn).catch(() => undefined);
    },
    [run],
  );

  const snapshot = storeState.snapshot;
  const runtime = snapshot?.client ?? null;
  const deployments = runtime?.deployments ?? [];
  const selectedDeployment = deployments[0] ?? null;
  const selectedConversation = selectedDeployment?.conversations[0] ?? null;

  useEffect(() => {
    if (!selectedDeployment || !selectedConversation) {
      setSession(null);
      return;
    }

    let cancelled = false;
    void bridge
      .sessionSnapshot({
        sessionId: selectedConversation.sessionId,
        agentDid: selectedDeployment.agentDid,
      })
      .then((nextSession) => {
        if (!cancelled) setSession(nextSession);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        push(`session_snapshot ERROR: ${message}`);
      });
    return () => {
      cancelled = true;
    };
  }, [
    bridge,
    push,
    selectedConversation,
    selectedDeployment,
    storeState.generation,
  ]);

  function sendChat(event: FormEvent) {
    event.preventDefault();
    if (!selectedDeployment || !draft.trim()) return;
    const content = draft.trim();
    setDraft("");
    void run("chat_send", () =>
      bridge.chatSend({
        agentDid: selectedDeployment.agentDid,
        behaviorId: selectedDeployment.defaultBehaviorId ?? null,
        sessionId: selectedConversation?.sessionId ?? null,
        content,
      }),
    )
      .then(() => store.refresh())
      .catch(() => undefined);
  }

  return (
    <main>
      <h1 data-testid="fixture-title">Indigo Relay Fixture Host</h1>
      <p>
        Downstream shell: own bundle id, <code>AppDataDir</code> home,
        paired-remote bootstrap (no runtime-admin), co-resident file-backed
        domain plugin.
      </p>

      <div className="panel">
        <strong>Gents bridge</strong>
        <div>
          <button
            disabled={busy}
            data-testid="bridge-start"
            onClick={() => runQuiet("client_start", () => store.start())}
          >
            Start client
          </button>
          <button
            disabled={busy}
            data-testid="bridge-contract"
            onClick={() =>
              runQuiet("bridge_contract", () => bridge.bridgeContract())
            }
          >
            Contract
          </button>
          <button
            disabled={busy}
            data-testid="bridge-snapshot"
            onClick={() => runQuiet("client_snapshot", () => store.refresh())}
          >
            Snapshot
          </button>
        </div>
      </div>

      <div className="panel">
        <strong>Domain plugin</strong>
        <div>
          <button
            disabled={busy}
            data-testid="domain-home"
            onClick={() =>
              runQuiet("domain_home", () =>
                invoke(domainCmd("domain_home_path")),
              )
            }
          >
            Domain home path
          </button>
          <button
            disabled={busy}
            data-testid="domain-put"
            onClick={() =>
              runQuiet("domain_put", () =>
                invoke(domainCmd("domain_doc_put"), {
                  id: "kitchen-item-1",
                  body: JSON.stringify({ item: "oats", qty: 1 }),
                }),
              )
            }
          >
            Put domain doc
          </button>
          <button
            disabled={busy}
            data-testid="domain-list"
            onClick={() =>
              runQuiet("domain_list", () =>
                invoke(domainCmd("domain_doc_list")),
              )
            }
          >
            List domain docs
          </button>
        </div>
      </div>

      <section className="package-surface" data-testid="fixture-chat-surface">
        <h2>Chat package</h2>
        <ChatTranscriptPanel
          selectedSessionId={selectedConversation?.sessionId ?? null}
          session={session}
        />
        <ChatComposer
          activeRequestId={session?.latestRequestId ?? null}
          approxSerializedBytes={runtime?.approxSerializedBytes ?? 0}
          behaviorLabel={selectedDeployment?.defaultBehaviorId ?? null}
          canSend={Boolean(selectedDeployment) && Boolean(draft.trim())}
          configuredPeerCount={runtime?.configuredPeerCount ?? 0}
          dialedPeerCount={runtime?.dialedPeerCount ?? 0}
          draft={draft}
          interruptVisible={false}
          rowCount={runtime?.rowCount ?? 0}
          sendHint={selectedDeployment ? null : "Pair an agent before sending"}
          sending={busy}
          turnState={session?.turnState ?? null}
          onDraftChange={setDraft}
          onInterruptClick={() => undefined}
          onSend={sendChat}
        />
      </section>

      <section className="package-surface" data-testid="fixture-fleet-surface">
        <h2>Fleet package</h2>
        <FleetDashboard
          addingPeer={busy}
          api={bridge.api}
          brand={
            <div className="fixture-brand" data-testid="fixture-brand">
              <strong>Indigo Relay</strong>
              <span>Independent agent console</span>
            </div>
          }
          copy={{
            pairingQrHint:
              "Scan an invite generated from the Indigo Relay administration console.",
          }}
          bootstrap={snapshot?.bootstrap ?? null}
          deployments={deployments}
          loading={false}
          p2pHealth={runtime?.p2pHealth ?? null}
          repairingP2P={false}
          starting={!storeState.started && busy}
          onAddPeer={(request) =>
            run("peer_add", () => bridge.peerAdd(request))
          }
          onPairBearer={(request) =>
            run("peer_pair_bearer", () => bridge.api.pairBearer(request))
          }
          onProbePeerAddress={(address) =>
            run("peer_probe", () => bridge.api.probePeerAddress(address))
          }
          onOpenChat={(agentDid) => push(`open_chat: ${agentDid}`)}
          onOpenConfig={(agentDid) => push(`open_config: ${agentDid}`)}
          onRemovePeer={(peerId) =>
            run("peer_remove", () => bridge.api.removePeer(peerId)).then(() =>
              store.refresh(),
            )
          }
          onRenamePeer={(peerId, label) =>
            run("peer_rename", () => bridge.api.renamePeer(peerId, label)).then(
              () => store.refresh(),
            )
          }
          onRepairP2P={() =>
            run("repair_p2p", () => bridge.api.repairP2P()).then(() =>
              store.refresh(),
            )
          }
        />
      </section>

      {error ? (
        <p className="error" data-testid="fixture-error">
          {error}
        </p>
      ) : null}

      <div className="panel">
        <strong>Log</strong>
        <pre data-testid="fixture-log">{log.join("\n") || "(empty)"}</pre>
      </div>
    </main>
  );
}
