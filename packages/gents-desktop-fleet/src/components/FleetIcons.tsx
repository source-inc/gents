import type { ToolIconKind } from "../fleetMetrics.js";

export function ChatIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M21 15a4 4 0 0 1-4 4H8l-5 3V7a4 4 0 0 1 4-4h10a4 4 0 0 1 4 4z" />
    </svg>
  );
}

export function ConfigIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M4 6h16" />
      <path d="M4 12h16" />
      <path d="M4 18h16" />
      <path d="M8 6v.01" />
      <path d="M14 12v.01" />
      <path d="M10 18v.01" />
    </svg>
  );
}

export function PencilIcon() {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      <path d="M12 20h9" />
      <path d="m16.5 3.5 4 4L7 21H3v-4L16.5 3.5Z" />
    </svg>
  );
}

export function TrashIcon() {
  return (
    <svg aria-hidden="true" viewBox="0 0 24 24">
      <path d="M3 6h18" />
      <path d="M8 6V4h8v2" />
      <path d="m18 6-1 14H7L6 6" />
      <path d="M10 11v6" />
      <path d="M14 11v6" />
    </svg>
  );
}

export function ToolIconGlyph({ kind }: { kind: ToolIconKind }) {
  if (kind === "file") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
        <path d="M14 2v6h6" />
      </svg>
    );
  }
  if (kind === "bash") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="m4 7 5 5-5 5" />
        <path d="M12 19h8" />
      </svg>
    );
  }
  if (kind === "meta") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <circle cx="6" cy="12" r="2" />
        <circle cx="18" cy="6" r="2" />
        <circle cx="18" cy="18" r="2" />
        <path d="M8 12l8-6" />
        <path d="M8 12l8 6" />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M4 5h16v14H4z" />
      <path d="M8 9h.01" />
      <path d="M11 9h.01" />
      <path d="M14 9h.01" />
      <path d="M8 14h8" />
    </svg>
  );
}
