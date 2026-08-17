import { gunzipSync } from "fflate";
import jsQR from "jsqr";
import type { QRCode } from "jsqr";
import { useEffect, useRef, useState, type ReactNode } from "react";

// v1: magic + gzip(struct-as-map CBOR of the full BearerInviteToken) — the
// gunzipped bytes are byte-identical to what `encode_bearer` (Rust) would
// bs58-encode, so v1 decode never needs to understand CBOR structure.
const BEARER_QR_MAGIC = new Uint8Array([
  0x64, 0x61, 0x62, 0x65, 0x61, 0x72, 0x31, 0x7a, 0x00,
]);
// v2: magic + gzip(positional CBOR array). Smaller on the wire, but this
// scanner has to understand its shape to reconstruct the same struct-as-map
// CBOR (and therefore the same `dabear1-<bs58>` text token) v1 hands
// straight through. See crates/gents-cli/src/commands/p2p/invite.rs
// (`compact_bearer_qr_payload_v2` / `decode_compact_bearer_qr_payload_v2`)
// for the authoritative Rust-side encode/decode pair this mirrors.
const BEARER_QR_MAGIC_V2 = new Uint8Array([
  0x64, 0x61, 0x62, 0x65, 0x61, 0x72, 0x32, 0x7a, 0x00,
]);
const MAX_COMPACT_QR_GZIP_BYTES = 8 * 1024;
const MAX_COMPACT_QR_CBOR_BYTES = 16 * 1024;
const BASE58_ALPHABET =
  "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

// v2 positional array layout (see `compact_bearer_qr_payload_v2` in Rust):
// [v, issuer_did, peer_id|null, ticket, nonce, network_id, issued_at,
//  template, default_behavior_id|null, network, sig]
const V2_ARRAY_LENGTH = 11;
// network sub-array: [display_name, default_template, created_at, sig]
const V2_NETWORK_ARRAY_LENGTH = 4;

export type QrScannerDialogProps = {
  onClose: () => void;
  onScan: (value: string) => void;
  pairingHint?: ReactNode;
};

function startsWithBytes(value: Uint8Array, prefix: Uint8Array): boolean {
  return (
    value.length >= prefix.length &&
    prefix.every((byte, index) => value[index] === byte)
  );
}

function encodeBase58(bytes: Uint8Array): string {
  let zeroCount = 0;
  while (zeroCount < bytes.length && bytes[zeroCount] === 0) zeroCount += 1;
  if (zeroCount === bytes.length) return "1".repeat(zeroCount);

  const digits = [0];
  for (let index = zeroCount; index < bytes.length; index += 1) {
    let carry = bytes[index];
    for (let digit = 0; digit < digits.length; digit += 1) {
      carry += digits[digit] * 256;
      digits[digit] = carry % 58;
      carry = Math.floor(carry / 58);
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = Math.floor(carry / 58);
    }
  }

  return (
    "1".repeat(zeroCount) +
    digits
      .reverse()
      .map((digit) => BASE58_ALPHABET[digit])
      .join("")
  );
}

/** Gunzips `compressed`, trusting the gzip trailer's ISIZE field (the last 4
 * bytes of a gzip stream, little-endian) to preallocate the exact output
 * buffer and to bound decompressed size before inflating — the same guard
 * both compact QR versions rely on to reject decompression bombs. Returns
 * `null` if the payload is malformed or exceeds `MAX_COMPACT_QR_*`. */
function gunzipGuarded(compressed: Uint8Array): Uint8Array | null {
  if (compressed.length < 4 || compressed.length > MAX_COMPACT_QR_GZIP_BYTES) {
    return null;
  }
  const expectedSize = new DataView(
    compressed.buffer,
    compressed.byteOffset + compressed.byteLength - 4,
    4,
  ).getUint32(0, true);
  if (expectedSize > MAX_COMPACT_QR_CBOR_BYTES) return null;

  const out = gunzipSync(compressed, { out: new Uint8Array(expectedSize) });
  if (out.length !== expectedSize) return null;
  return out;
}

function decodeCompactV1(compressed: Uint8Array): string | null {
  try {
    const cbor = gunzipGuarded(compressed);
    if (!cbor) return null;
    return `dabear1-${encodeBase58(cbor)}`;
  } catch {
    return null;
  }
}

// --- v2: minimal CBOR reader/writer -----------------------------------
//
// Purpose-built for exactly the shapes `compact_bearer_qr_payload_v2` emits
// and the struct-as-map shape `decode_bearer` (Rust, unchanged) expects —
// not a general CBOR library. Reader: unsigned int, byte string, text
// string, definite-length array, null. Writer: the same, plus definite-
// length maps (for the reconstructed struct).

type CborValue = number | string | Uint8Array | null | CborValue[];

class CborReader {
  private pos = 0;
  constructor(private readonly bytes: Uint8Array) {}

  private byte(): number {
    if (this.pos >= this.bytes.length) {
      throw new Error("unexpected end of CBOR data");
    }
    return this.bytes[this.pos++];
  }

  private readUint(byteCount: number): number {
    let value = 0;
    for (let i = 0; i < byteCount; i += 1) value = value * 256 + this.byte();
    return value;
  }

  private readLength(additional: number): number {
    if (additional < 24) return additional;
    if (additional === 24) return this.readUint(1);
    if (additional === 25) return this.readUint(2);
    if (additional === 26) return this.readUint(4);
    throw new Error(`unsupported CBOR length encoding (additional=${additional})`);
  }

  readValue(): CborValue {
    const initial = this.byte();
    const majorType = initial >> 5;
    const additional = initial & 0x1f;

    switch (majorType) {
      case 0: // unsigned integer
        return this.readLength(additional);
      case 2: {
        // byte string
        const length = this.readLength(additional);
        const value = this.bytes.slice(this.pos, this.pos + length);
        this.pos += length;
        return value;
      }
      case 3: {
        // text string
        const length = this.readLength(additional);
        const value = this.bytes.slice(this.pos, this.pos + length);
        this.pos += length;
        return new TextDecoder().decode(value);
      }
      case 4: {
        // array
        const length = this.readLength(additional);
        const items: CborValue[] = [];
        for (let i = 0; i < length; i += 1) items.push(this.readValue());
        return items;
      }
      case 7:
        if (initial === 0xf6) return null; // null
        throw new Error(
          `unsupported CBOR simple value (0x${initial.toString(16)})`,
        );
      default:
        throw new Error(`unsupported CBOR major type ${majorType}`);
    }
  }
}

function asNumber(value: CborValue, field: string): number {
  if (typeof value !== "number") {
    throw new Error(`v2 payload: ${field} is not a number`);
  }
  return value;
}

function asText(value: CborValue, field: string): string {
  if (typeof value !== "string") {
    throw new Error(`v2 payload: ${field} is not text`);
  }
  return value;
}

function asBytes(value: CborValue, field: string): Uint8Array {
  if (!(value instanceof Uint8Array)) {
    throw new Error(`v2 payload: ${field} is not bytes`);
  }
  return value;
}

function asArray(value: CborValue, field: string): CborValue[] {
  if (!Array.isArray(value)) {
    throw new Error(`v2 payload: ${field} is not an array`);
  }
  return value;
}

/** 16 raw bytes -> canonical lowercase-hyphenated UUID string, matching
 * Rust's `Uuid::from_bytes(..).to_string()`. */
function bytesToUuidText(bytes: Uint8Array): string {
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join(
    "",
  );
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20, 32),
  ].join("-");
}

/** Unix epoch seconds -> RFC3339 at `SecondsFormat::Secs` precision with a
 * `Z` suffix, matching Rust's `to_rfc3339_opts(SecondsFormat::Secs, true)`.
 * `Date`'s ISO string always has a `.000` millisecond component for a
 * whole-second input, so stripping it recovers the exact same text. */
function epochSecondsToRfc3339(seconds: number): string {
  return new Date(seconds * 1000).toISOString().replace(/\.\d{3}Z$/, "Z");
}

/** Mirrors the plain-string branches of `peer_id_derivable_from_ticket`
 * (Rust): `id@host:port`, a legacy `.../p2p/<id>` suffix, or a bare id
 * (optionally `iroh://`-prefixed). Only reached when the v2 payload omitted
 * `peer_id`, which Rust only does when this same rule already matched — so
 * it is guaranteed to recover the original value here, not just guess it. */
function derivePeerIdFromTicket(ticket: string): string {
  function normalize(id: string): string {
    const trimmed = id.trim();
    return trimmed.startsWith("iroh://")
      ? trimmed.slice("iroh://".length)
      : trimmed;
  }

  const trimmed = ticket.trim();
  const atIndex = trimmed.indexOf("@");
  if (atIndex !== -1) return normalize(trimmed.slice(0, atIndex));

  const p2pIndex = trimmed.lastIndexOf("/p2p/");
  if (p2pIndex !== -1) return normalize(trimmed.slice(p2pIndex + "/p2p/".length));

  return normalize(trimmed);
}

function cborUintHeader(majorType: number, value: number): number[] {
  if (value < 24) return [(majorType << 5) | value];
  if (value < 256) return [(majorType << 5) | 24, value];
  if (value < 65536) {
    return [(majorType << 5) | 25, (value >>> 8) & 0xff, value & 0xff];
  }
  return [
    (majorType << 5) | 26,
    (value >>> 24) & 0xff,
    (value >>> 16) & 0xff,
    (value >>> 8) & 0xff,
    value & 0xff,
  ];
}

function cborUint(value: number): number[] {
  return cborUintHeader(0, value);
}

function cborBytes(value: Uint8Array): number[] {
  return [...cborUintHeader(2, value.length), ...value];
}

function cborText(value: string): number[] {
  const bytes = new TextEncoder().encode(value);
  return [...cborUintHeader(3, bytes.length), ...bytes];
}

type ReconstructedNetwork = {
  networkId: string;
  adminDid: string;
  displayName: string;
  defaultTemplate: string;
  createdAt: string;
  sig: Uint8Array;
};

type ReconstructedBearerToken = {
  v: number;
  issuerDid: string;
  peerId: string;
  ticket: string;
  nonce: string;
  networkId: string;
  issuedAt: string;
  template: string;
  defaultBehaviorId: string | null;
  network: ReconstructedNetwork;
  sig: Uint8Array;
};

/** Rebuilds the same struct-as-map CBOR bytes `encode_bearer` (Rust) would
 * produce for this token: a definite-length map keyed by the exact
 * `BearerInviteToken` field names, `default_behavior_id` present only when
 * set. `decode_bearer` reads CBOR maps by key (via serde's struct
 * deserialization), so key order doesn't matter here — only presence and
 * names do. */
function encodeBearerTokenAsMapCbor(token: ReconstructedBearerToken): Uint8Array {
  const entries: number[][] = [];
  const push = (key: string, value: number[]) => {
    entries.push([...cborText(key), ...value]);
  };

  push("v", cborUint(token.v));
  push("issuer_did", cborText(token.issuerDid));
  push("peer_id", cborText(token.peerId));
  push("ticket", cborText(token.ticket));
  push("nonce", cborText(token.nonce));
  push("network_id", cborText(token.networkId));
  push("issued_at", cborText(token.issuedAt));
  push("template", cborText(token.template));
  if (token.defaultBehaviorId !== null) {
    push("default_behavior_id", cborText(token.defaultBehaviorId));
  }

  const networkFields: number[][] = [
    [cborText("network_id"), cborText(token.network.networkId)].flat(),
    [cborText("admin_did"), cborText(token.network.adminDid)].flat(),
    [cborText("display_name"), cborText(token.network.displayName)].flat(),
    [
      cborText("default_template"),
      cborText(token.network.defaultTemplate),
    ].flat(),
    [cborText("created_at"), cborText(token.network.createdAt)].flat(),
    [cborText("sig"), cborBytes(token.network.sig)].flat(),
  ];
  push("network", [
    ...cborUintHeader(5, networkFields.length),
    ...networkFields.flat(),
  ]);

  push("sig", cborBytes(token.sig));

  return Uint8Array.from([
    ...cborUintHeader(5, entries.length),
    ...entries.flat(),
  ]);
}

function decodeCompactV2(compressed: Uint8Array): string | null {
  try {
    const cbor = gunzipGuarded(compressed);
    if (!cbor) return null;

    const items = asArray(new CborReader(cbor).readValue(), "payload");
    if (items.length !== V2_ARRAY_LENGTH) return null;

    const v = asNumber(items[0], "v");
    const issuerDid = asText(items[1], "issuer_did");
    const ticket = asText(items[3], "ticket");
    const networkId = asText(items[5], "network_id");
    const template = asText(items[7], "template");
    const sig = asBytes(items[10], "sig");

    const peerIdRaw = items[2];
    const peerId =
      peerIdRaw === null
        ? derivePeerIdFromTicket(ticket)
        : asText(peerIdRaw, "peer_id");

    const nonceRaw = items[4];
    const nonce =
      nonceRaw instanceof Uint8Array
        ? bytesToUuidText(nonceRaw)
        : asText(nonceRaw, "nonce");

    const issuedAtRaw = items[6];
    const issuedAt =
      typeof issuedAtRaw === "number"
        ? epochSecondsToRfc3339(issuedAtRaw)
        : asText(issuedAtRaw, "issued_at");

    const defaultBehaviorRaw = items[8];
    const defaultBehaviorId =
      defaultBehaviorRaw === null
        ? null
        : asText(defaultBehaviorRaw, "default_behavior_id");

    const networkItems = asArray(items[9], "network");
    if (networkItems.length !== V2_NETWORK_ARRAY_LENGTH) return null;

    const token: ReconstructedBearerToken = {
      v,
      issuerDid,
      peerId,
      ticket,
      nonce,
      networkId,
      issuedAt,
      template,
      defaultBehaviorId,
      network: {
        networkId,
        adminDid: issuerDid,
        displayName: asText(networkItems[0], "network.display_name"),
        defaultTemplate: asText(networkItems[1], "network.default_template"),
        createdAt: asText(networkItems[2], "network.created_at"),
        sig: asBytes(networkItems[3], "network.sig"),
      },
      sig,
    };

    return `dabear1-${encodeBase58(encodeBearerTokenAsMapCbor(token))}`;
  } catch {
    return null;
  }
}

export function decodePairingQrPayload(
  result: Pick<QRCode, "binaryData" | "data">,
): string | null {
  const text = result.data.trim();
  if (text.startsWith("dabear1-")) return text;

  const bytes = Uint8Array.from(result.binaryData);
  if (startsWithBytes(bytes, BEARER_QR_MAGIC_V2)) {
    return decodeCompactV2(bytes.subarray(BEARER_QR_MAGIC_V2.length));
  }
  if (startsWithBytes(bytes, BEARER_QR_MAGIC)) {
    return decodeCompactV1(bytes.subarray(BEARER_QR_MAGIC.length));
  }
  return null;
}

export function QrScannerDialog({
  onClose,
  onScan,
  pairingHint,
}: QrScannerDialogProps) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const onCloseRef = useRef(onClose);
  const onScanRef = useRef(onScan);
  const [error, setError] = useState<string | null>(null);

  onCloseRef.current = onClose;
  onScanRef.current = onScan;

  useEffect(() => {
    let cancelled = false;
    let frame: number | null = null;
    let stream: MediaStream | null = null;

    async function startCamera() {
      try {
        stream = await navigator.mediaDevices.getUserMedia({
          audio: false,
          video: {
            facingMode: { ideal: "environment" },
            height: { ideal: 1080 },
            width: { ideal: 1920 },
          },
        });
        if (cancelled) {
          stream.getTracks().forEach((track) => track.stop());
          return;
        }

        const video = videoRef.current;
        if (!video) return;
        video.srcObject = stream;
        video.setAttribute("playsinline", "true");
        await video.play();
        frame = window.requestAnimationFrame(scanFrame);
      } catch (cause) {
        setError(
          cause instanceof Error
            ? cause.message
            : "Camera access is unavailable. Paste the invite instead.",
        );
      }
    }

    function scanFrame() {
      if (cancelled) return;
      const video = videoRef.current;
      const canvas = canvasRef.current;
      if (
        video &&
        canvas &&
        video.readyState >= HTMLMediaElement.HAVE_ENOUGH_DATA &&
        video.videoWidth > 0 &&
        video.videoHeight > 0
      ) {
        canvas.width = video.videoWidth;
        canvas.height = video.videoHeight;
        const context = canvas.getContext("2d", { willReadFrequently: true });
        if (context) {
          context.drawImage(video, 0, 0, canvas.width, canvas.height);
          const image = context.getImageData(0, 0, canvas.width, canvas.height);
          const result = jsQR(image.data, image.width, image.height, {
            inversionAttempts: "attemptBoth",
          });
          if (result) {
            try {
              const token = decodePairingQrPayload(result);
              if (token) {
                onScanRef.current(token);
                onCloseRef.current();
                return;
              }
            } catch {
              setError(
                "That pairing QR could not be decoded. Mint a fresh invite or paste its token.",
              );
            }
          }
        }
      }
      frame = window.requestAnimationFrame(scanFrame);
    }

    void startCamera();
    return () => {
      cancelled = true;
      if (frame !== null) window.cancelAnimationFrame(frame);
      stream?.getTracks().forEach((track) => track.stop());
    };
  }, []);

  return (
    <div
      aria-label="Scan pairing invite"
      aria-modal="true"
      className="fleet-qr-backdrop"
      data-testid="fleet-qr-scanner"
      role="dialog"
    >
      <section className="fleet-qr-dialog panel">
        <header>
          <div>
            <p className="eyebrow">Secure pairing</p>
            <h3>Scan agent invite</h3>
          </div>
          <button
            aria-label="Close camera"
            className="ghost-button"
            onClick={onClose}
            type="button"
          >
            Close
          </button>
        </header>
        <div className="fleet-qr-viewport">
          <video muted ref={videoRef} />
          <span aria-hidden="true" className="fleet-qr-guide" />
        </div>
        {error ? <p className="fleet-inline-error">{error}</p> : null}
        <p className="muted">
          {pairingHint ?? (
            <>Point the camera at the pairing QR code shown by your agent.</>
          )}
        </p>
        <canvas aria-hidden="true" ref={canvasRef} />
      </section>
    </div>
  );
}
