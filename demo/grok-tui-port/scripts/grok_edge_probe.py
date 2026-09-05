#!/usr/bin/env python3
"""Live, dependency-free probes for the Gents-owned Grok leader socket."""

from __future__ import annotations

import argparse
import json
import os
import socket
import struct
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from grok_probe_common import (
    PortableSocketPath,
    graphql_escape,
    graphql_query,
    poll_until_deadline,
    require,
    self_test_portable_socket_path,
)


DEFAULT_MODEL = "GLM-5.3-Flash-NVFP4"
DEFAULT_CONTEXT_WINDOW = 262_144
MAX_FRAME = 64 * 1024 * 1024

# The two notification rails a Grok leader client must observe.
#
# Standard ACP content/tool updates ride `session/update`. Live Grok
# subagent lifecycle events ride the extension notification
# `_x.ai/session_notification`; `_x.ai/session/update` is only a replay-path
# alias the pager accepts, not the exact live rail. The two rails have
# SEPARATE applied-event high-water semantics: extension notifications may
# omit `totalTokens` entirely, and transient progress may carry no metadata
# at all, so no single global monotonic event id/token counter may be
# demanded across both rails.
STANDARD_UPDATE_METHOD = "session/update"
EXT_SESSION_NOTIFICATION_METHOD = "_x.ai/session_notification"
EXT_SESSION_UPDATE_ALIAS_METHOD = "_x.ai/session/update"
SUBAGENT_LIFECYCLE_METHODS = (
    EXT_SESSION_NOTIFICATION_METHOD,
    EXT_SESSION_UPDATE_ALIAS_METHOD,
)


def message_rail(message: dict[str, Any]) -> str | None:
    """Classify one notification by its method into a high-water rail.

    Returns `"standard"` for `session/update`, `"extension"` for the exact
    live subagent lifecycle rail (`x.ai/session_notification`) and its
    replay-path alias (`x.ai/session/update`), and `None` for anything else
    (the rails' high-waters are untouched by unrelated ext notifications).
    """
    method = message.get("method")
    if method == STANDARD_UPDATE_METHOD:
        return "standard"
    if method in SUBAGENT_LIFECYCLE_METHODS:
        return "extension"
    return None


class LeaderClient:
    def __init__(self, path: str, timeout: float, model: str) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        bridge: PortableSocketPath | None = None
        try:
            bridge = PortableSocketPath(path)
            self.sock.connect(bridge.connect_path)
        except BaseException:
            self.sock.close()
            raise
        finally:
            if bridge is not None:
                try:
                    bridge.cleanup()
                except BaseException:
                    self.sock.close()
                    raise
        self.next_id = 1
        self.model = model

    def close(self) -> None:
        try:
            self.send_frame({"type": "disconnect"})
        finally:
            self.sock.close()

    def send_frame(self, value: dict[str, Any]) -> None:
        body = json.dumps(value, separators=(",", ":")).encode()
        require(len(body) <= MAX_FRAME, "outgoing frame exceeds 64 MiB")
        self.sock.sendall(struct.pack(">I", len(body)) + body)

    def recv_exact(self, length: int) -> bytes:
        chunks: list[bytes] = []
        remaining = length
        while remaining:
            chunk = self.sock.recv(remaining)
            if not chunk:
                raise EOFError("leader socket closed")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def recv_frame(self) -> dict[str, Any]:
        length = struct.unpack(">I", self.recv_exact(4))[0]
        require(length <= MAX_FRAME, f"incoming frame is too large: {length}")
        value = json.loads(self.recv_exact(length))
        require(isinstance(value, dict), "leader frame is not a JSON object")
        return value

    def register(self) -> dict[str, Any]:
        self.send_frame(
            {
                "type": "register",
                "client_type": "gents-edge-probe",
                "mode": "headless",
                "capabilities": {
                    "yolo_mode": True,
                    "auto_mode": False,
                    "default_model": self.model,
                    "client_version": "gents-edge-probe/1",
                    "code_nav_enabled": False,
                    "terminal": False,
                    "fs_read": False,
                    "fs_write": False,
                    "status_line": False,
                },
            }
        )
        registered = self.recv_frame()
        require(registered.get("type") == "registered", "register was not acknowledged")
        require(registered.get("ready") is True, "leader is not ready")
        require(registered.get("leader_protocol_version") == 1, "wrong leader protocol")
        return registered

    def ping(self) -> None:
        self.send_frame({"type": "ping"})
        require(self.recv_frame() == {"type": "pong"}, "ping did not return pong")

    def send_acp(self, payload: dict[str, Any]) -> None:
        # ACP's SDK prefixes extension methods on the wire and strips the
        # prefix before invoking Grok's internal x.ai handlers.
        method = payload.get("method", "")
        if method.startswith("x.ai/"):
            payload = {**payload, "method": "_" + method}
        self.send_frame(
            {
                "type": "acp",
                "payload": json.dumps(payload, separators=(",", ":")),
            }
        )

    def recv_acp(self) -> dict[str, Any]:
        frame = self.recv_frame()
        require(frame.get("type") == "acp", f"unexpected leader frame: {frame}")
        payload = json.loads(frame["payload"])
        require(isinstance(payload, dict), "ACP payload is not a JSON object")
        return payload

    def request(
        self,
        method: str,
        params: dict[str, Any],
        on_message: Callable[[dict[str, Any]], None] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        request_id = self.next_id
        self.next_id += 1
        self.send_acp(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )
        notifications: list[dict[str, Any]] = []
        while True:
            message = self.recv_acp()
            if message.get("id") == request_id:
                return message, notifications
            notifications.append(message)
            if on_message is not None:
                on_message(message)

    def notify(self, method: str, params: dict[str, Any]) -> None:
        self.send_acp({"jsonrpc": "2.0", "method": method, "params": params})


def require_exact_mcp_initialized(message: Any, session_id: str) -> None:
    """Validate the exact zero-MCP completion emitted by session/new."""
    require(isinstance(message, dict), f"x.ai/mcp_initialized is not an object: {message!r}")
    require(
        set(message) == {"jsonrpc", "method", "params"},
        f"x.ai/mcp_initialized envelope keys drifted: {message!r}",
    )
    require(
        message.get("jsonrpc") == "2.0" and message.get("method") == "_x.ai/mcp_initialized",
        f"session/new emitted the wrong MCP completion envelope: {message!r}",
    )
    params = message.get("params")
    require(isinstance(params, dict), f"x.ai/mcp_initialized params are not an object: {params!r}")
    require(
        set(params) == {"sessionId", "mcpToolCount", "elapsedMs"}
        and params.get("sessionId") == session_id
        and params.get("mcpToolCount") == 0
        and not isinstance(params.get("mcpToolCount"), bool)
        and params.get("elapsedMs") == 0
        and not isinstance(params.get("elapsedMs"), bool),
        f"x.ai/mcp_initialized params drifted: {params!r}",
    )


def self_test_mcp_initialized_validator() -> dict[str, int]:
    """Exercise the same exact-envelope validator used by the live handshake."""
    session_id = "mcp-self-test-session"
    good = {
        "jsonrpc": "2.0",
        "method": "_x.ai/mcp_initialized",
        "params": {"sessionId": session_id, "mcpToolCount": 0, "elapsedMs": 0},
    }
    require_exact_mcp_initialized(good, session_id)
    rejects = [
        {**good, "id": 1},
        {**good, "jsonrpc": "1.0"},
        {**good, "method": "x.ai/mcp_initialized"},  # internal name is not a wire method
        {**good, "params": []},
        {**good, "params": {"mcpToolCount": 0, "elapsedMs": 0}},
        {**good, "params": {"sessionId": "other", "mcpToolCount": 0, "elapsedMs": 0}},
        {**good, "params": {"sessionId": session_id, "mcpToolCount": True, "elapsedMs": 0}},
        {**good, "params": {"sessionId": session_id, "mcpToolCount": 1, "elapsedMs": 0}},
        {**good, "params": {"sessionId": session_id, "mcpToolCount": 0, "elapsedMs": True}},
        {**good, "params": {"sessionId": session_id, "mcpToolCount": 0, "elapsedMs": 1}},
        {
            **good,
            "params": {
                "sessionId": session_id,
                "mcpToolCount": 0,
                "elapsedMs": 0,
                "extra": None,
            },
        },
    ]
    rejected = 0
    for fixture in rejects:
        try:
            require_exact_mcp_initialized(fixture, session_id)
        except AssertionError:
            rejected += 1
        else:
            raise AssertionError(f"MCP completion validator accepted drift: {fixture!r}")
    require(rejected == len(rejects), "MCP completion self-test reject count drifted")
    return {"accepted": 1, "rejected": rejected}


def initialize(
    client: LeaderClient, cwd: str, context_window: int
) -> tuple[str, dict[str, Any]]:
    registered = client.register()
    client.ping()
    initialized, _ = client.request(
        "initialize",
        {"protocolVersion": 1, "clientInfo": {"name": "gents-edge-probe"}},
    )
    require("error" not in initialized, f"initialize failed: {initialized}")
    result = initialized["result"]
    require(result["agentCapabilities"]["loadSession"] is True, "persisted session replay must be advertised")
    require(result["authMethods"][0]["id"] == "gents.runtime", "auth method drift")
    require(
        isinstance(result["authMethods"][0].get("name"), str)
        and result["authMethods"][0]["name"],
        "auth method must advertise a non-empty name",
    )

    authenticated, _ = client.request("authenticate", {"methodId": "gents.runtime"})
    require(authenticated.get("result", {}).get("_meta", {}).get("provider") == "gents", "auth failed")

    preferred = f"grok-edge-{uuid.uuid4().hex[:16]}"
    created, session_notifications = client.request(
        "session/new",
        {
            "cwd": cwd,
            "mcpServers": [],
            "_meta": {
                "sessionId": preferred,
                "modelId": client.model,
            },
        },
    )
    require("error" not in created, f"session/new failed: {created}")
    require(not session_notifications, "session/new emitted a notification before session routing was ready")
    require_exact_mcp_initialized(client.recv_acp(), preferred)
    session = created["result"]
    require(session["sessionId"] == preferred, "preferred session id was not honored")
    require(session["models"]["currentModelId"] == client.model, "current model mismatch")
    model = session["models"]["availableModels"][0]
    require(model["modelId"] == client.model, "catalog model mismatch")
    require(model["meta"]["totalContextTokens"] == context_window, "context window mismatch")
    require(model["meta"].get("acceptsImages") is False, "catalog must be text-only")
    require(model["meta"].get("inputModalities") == ["text"], "catalog modality drift")
    require(
        model["meta"].get("supportsReasoningEffort") is False,
        "catalog must not advertise unplumbed per-session reasoning effort",
    )
    require(
        "reasoningEfforts" not in model["meta"],
        "catalog must omit an unsupported reasoning-effort list",
    )
    # The mode capabilities are connection-scoped, not request-scoped: the
    # request above carries none of the three keys, so every value must be
    # derived from the register capabilities (yolo_mode=true, auto_mode=false,
    # terminal=false).
    meta = session["_meta"]
    require(meta.get("yoloMode") is True, "session _meta yoloMode must come from registration")
    require(meta.get("autoMode") is False, "session _meta autoMode must come from registration")
    require(
        meta.get("clientTerminal") is False,
        "session _meta clientTerminal must come from registration",
    )
    return preferred, registered


def expect_error(client: LeaderClient, method: str, session_id: str) -> int:
    response, _ = client.request(method, {"sessionId": session_id})
    require("error" in response, f"{method} unexpectedly succeeded")
    return int(response["error"]["code"])


def probe_handshake(
    client: LeaderClient,
    session_id: str,
    context_window: int,
    high_water: SessionHighWater,
) -> dict[str, Any]:
    switched, notifications = client.request(
        "session/set_model",
        {
            "sessionId": session_id,
            "modelId": client.model,
        },
    )
    require("error" not in switched, f"session/set_model failed: {switched}")
    model_update = next(
        (message for message in notifications if message.get("method") == "_x.ai/models/update"),
        None,
    )
    require(
        model_update is not None,
        "session/set_model did not emit x.ai/models/update",
    )
    model_catalog = model_update.get("params", {})
    require("models" not in model_catalog, "models/update params must be flat")
    require(
        isinstance(model_catalog.get("availableModels"), list),
        "models/update lacks flat availableModels",
    )
    require(
        model_catalog.get("currentModelId") == client.model,
        "models/update currentModelId mismatch",
    )
    observed_model = next(
        (
            model
            for model in model_catalog["availableModels"]
            if isinstance(model, dict)
            and model.get("modelId") == model_catalog["currentModelId"]
        ),
        None,
    )
    require(observed_model is not None, "models/update current model is absent from catalog")
    observed_context_window = observed_model.get("meta", {}).get("totalContextTokens")
    require(
        observed_context_window == context_window,
        "models/update current model context window mismatch",
    )

    mode, notifications = client.request(
        "session/set_mode", {"sessionId": session_id, "modeId": "yolo"}
    )
    require("error" not in mode, f"session/set_mode failed: {mode}")
    mode_update = next(
        (message.get("params", {}).get("update", {}) for message in notifications
         if message.get("params", {}).get("update", {}).get("sessionUpdate") == "current_mode_update"),
        None,
    )
    require(
        mode_update is not None and mode_update.get("currentModeId") == "yolo",
        "session/set_mode did not emit current_mode_update",
    )
    # The handshake's session/update events participate in the standard
    # rail's connection-wide high-water: their counters must sit strictly
    # below every later turn's counters on the same rail.
    high_water.observe_all(notifications)

    load, _ = client.request(
        "session/load", {"sessionId": session_id, "cwd": str(Path.cwd()), "mcpServers": []}
    )
    require(load.get("error", {}).get("code") == -32601, "session/load must fail explicitly")
    unsupported = {
        method: expect_error(client, method, session_id)
        for method in (
            "x.ai/interject",
            "x.ai/compact_conversation",
            "terminal/create",
        )
    }
    subagent, _ = client.request(
        "x.ai/subagent/get", {"subagentId": "missing"}
    )
    # Exact real Grok DTO, checked by the ONE shared method-discriminated
    # validator (`require_exact_stub_result`) that the inline self-test
    # also measures its fixtures through: the generated
    # `GetSubagentResponse` serializes a missing id as a single nullable
    # snapshot — `{"snapshot": null}` — with no invented subagentId echo
    # and no outcome wrapper.
    require("result" in subagent, f"x.ai/subagent/get returned no result: {subagent}")
    require_exact_stub_result("x.ai/subagent/get", subagent.get("result"))
    running, _ = client.request("x.ai/subagent/list_running", {"sessionId": session_id})
    # Exact real Grok DTO through the same shared validator: the generated
    # `ListRunningSubagentsResponse` serializes the empty list under
    # `subagents` — `{"subagents": []}` — never a `running` key.
    require("result" in running, f"x.ai/subagent/list_running returned no result: {running}")
    require_exact_stub_result("x.ai/subagent/list_running", running.get("result"))
    # The ext cancel control is exercised live here, separately from the
    # standard-rail session/cancel notification used by the cancel edge:
    # x.ai/subagent/cancel is a request (not a notification) whose params
    # stay camelCase, and a missing child answers the direct
    # CancelSubagentResponse — subagentId echo, cancelled false, and the
    # not_found outcome kind — never an outcome wrapper.
    cancel, _ = client.request(
        "x.ai/subagent/cancel", {"subagentId": "missing"}
    )
    require("result" in cancel, f"x.ai/subagent/cancel returned no result: {cancel}")
    require_exact_stub_result("x.ai/subagent/cancel", cancel.get("result"))
    for method in ("x.ai/subagent/get", "x.ai/subagent/cancel"):
        malformed, _ = client.request(method, {})
        require(malformed.get("error", {}).get("code") == -32602,
                f"{method} must require subagentId: {malformed}")
    foreign, _ = client.request("x.ai/subagent/list_running", {"sessionId": "not-registered"})
    require(foreign.get("error", {}).get("code") == -32602,
            f"subagent controls must reject foreign connection sessions: {foreign}")

    # `terminal/wait_for_exit` is exercised on its own: the reference pager
    # answers it with the exact METHOD_NOT_FOUND message
    # "pager does not handle WaitForTerminalExit".
    wait_for_exit, _ = client.request("terminal/wait_for_exit", {"sessionId": session_id})
    require(
        wait_for_exit.get("error", {}).get("code") == -32601,
        f"terminal/wait_for_exit must fail with code -32601, got: {wait_for_exit}",
    )
    require(
        wait_for_exit.get("error", {}).get("message")
        == "pager does not handle WaitForTerminalExit",
        f"terminal/wait_for_exit must carry the pager's exact message, got: {wait_for_exit}",
    )
    return {
        "model": model_catalog["currentModelId"],
        "context_window": observed_context_window,
        "unsupported": unsupported,
        "models_update": model_catalog,
        "current_mode_update": mode_update,
        "subagent_get": subagent["result"],
        "subagent_list_running": running["result"],
        "subagent_cancel": cancel["result"],
        "wait_for_exit": wait_for_exit["error"]["message"],
    }


def require_u64(value: Any, what: str) -> int:
    """Assert one JSON value is an exact u64 counter (never a bool)."""
    require(
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= U64_MAX,
        f"{what} must be an unsigned u64 integer in [0, 2**64-1]; got {value!r}",
    )
    return value


# The exact audited result shapes of the three stub self-test methods,
# keyed by method name. The live handshake (`probe_handshake`) and the
# inline self-test both route every get/list_running/cancel result through
# the SAME method-discriminated validator, so the self-test evidence is
# measured by the real contract check — never by a bare inequality.
STUB_METHOD_AUDITED_RESULTS: dict[str, dict[str, Any]] = {
    # x.ai/subagent/get missing id: the generated GetSubagentResponse is a
    # single nullable snapshot — no outcome wrapper, no id echo.
    "x.ai/subagent/get": {"snapshot": None},
    # The list_running empty result is keyed `subagents` — never `running`
    # and never an outcome wrapper.
    "x.ai/subagent/list_running": {"subagents": []},
    # x.ai/subagent/cancel missing id: the direct camelCase
    # CancelSubagentResponse echoes the requested id, cancelled is false,
    # and the outcome kind is not_found.
    "x.ai/subagent/cancel": {
        "subagentId": "missing",
        "cancelled": False,
        "outcome": {"kind": "not_found"},
    },
}


def require_exact_stub_result(
    method: str, result: Any, subagent_id: str = "missing"
) -> None:
    """Assert one stub get/list_running/cancel result is the exact audited
    DTO for that method.

    This is the ONE shared validator used by BOTH the live handshake
    (`probe_handshake`) and the inline self-test: the live edge and the
    fixture evidence cannot drift apart, because they run the same
    method-discriminated check. The cancel result echoes the requested
    `subagent_id` (the live handshake probes a missing child; the
    self-test fixtures pin their own id), and every other field is
    compared for exact equality against the audited shape.
    """
    # The JSON-RPC result contains the shell's ExtMethodResult envelope.
    require(isinstance(result, dict) and set(result) == {"result"},
            f"{method} requires the stock extension result envelope: {result!r}")
    result = result["result"]
    require(
        method in STUB_METHOD_AUDITED_RESULTS,
        f"unknown stub result method {method!r}; expected one of "
        f"{sorted(STUB_METHOD_AUDITED_RESULTS)}",
    )
    expected = STUB_METHOD_AUDITED_RESULTS[method]
    if method == "x.ai/subagent/cancel":
        require(
            isinstance(result, dict),
            f"{method} result must be an object; got {result!r}",
        )
        require(
            result.get("subagentId") == subagent_id,
            f"{method} result must echo subagentId {subagent_id!r}; got "
            f"{result.get('subagentId')!r}",
        )
        require(
            result.get("cancelled") is False,
            f"{method} result cancelled must be false; got {result.get('cancelled')!r}",
        )
        require(
            result.get("outcome") == {"kind": "not_found"},
            f"{method} result outcome must be the exact not_found outcome; got "
            f"{result.get('outcome')!r}",
        )
        require(
            set(result.keys()) == set(expected.keys()),
            f"{method} result keys must be exactly {sorted(expected.keys())}; got "
            f"{sorted(result.keys())}",
        )
        return
    require(
        result == expected,
        f"{method} result must be the exact audited shape {expected!r}; got {result!r}",
    )


def event_sequence(update: dict[str, Any]) -> int:
    event_id = update.get("params", {}).get("_meta", {}).get("eventId", "")
    require(
        isinstance(event_id, str) and bool(event_id),
        f"eventId must be a non-empty string; got {event_id!r}",
    )
    parts = event_id.rsplit("-", 1)
    require(len(parts) == 2, f"invalid eventId: {event_id!r}")
    prefix, tail = parts
    # The counter half must be bare digits: `evt--1` would otherwise parse
    # as -1 (via Python sign rules) and negative counters are not u64. The
    # prefix must be non-empty and must not itself end in `-`, so a doubled
    # separator can never smuggle a sign into the counter half.
    require(
        bool(prefix) and not prefix.endswith("-"),
        f"invalid eventId: {event_id!r} (counter half must follow a single "
        f"separator after a non-empty prefix)",
    )
    require(
        bool(tail) and tail.isdigit(),
        f"invalid eventId: {event_id!r} (counter half {tail!r} is not an "
        f"unsigned decimal integer)",
    )
    counter = int(tail)
    # The pager's event counter is a u64: anything above 2**64-1 rejects.
    return require_u64(counter, f"eventId counter from {event_id!r}")


class SessionHighWater:
    """Per-rail monotonicity state across every turn.

    The standard (`session/update`) and extension
    (`x.ai/session_notification`) rails have SEPARATE applied-event
    high-water semantics, so each rail tracks its own last `eventId` counter
    and its own last `totalTokens` value. Within a rail, every new event
    counter must be strictly greater than the last previously observed
    counter on that rail — not merely sorted within one request — and an
    observed `totalTokens` must never decrease. The standard rail demands a
    `totalTokens` value wherever the contract carries it; the extension rail
    may omit `totalTokens` entirely (and transient progress may carry no
    metadata at all), so a missing value there is simply not folded. An
    extension envelope may omit `_meta` or the `eventId` key — absent means
    skipped — but a PRESENT `eventId` must be a non-empty string whose
    counter parses through `event_sequence` as a bounded u64; a malformed
    present id rejects and is never silently skipped.
    """

    def __init__(self) -> None:
        self.last_counter: dict[str, int | None] = {"standard": None, "extension": None}
        self.last_total_tokens: dict[str, int | None] = {"standard": None, "extension": None}

    def observe(self, message: dict[str, Any]) -> None:
        """Fold one notification into its own rail's high-water state.

        The standard rail requires a well-formed `eventId`; the extension
        rail tolerates envelopes without event metadata — `_meta` or the
        `eventId` key may be absent, and an absent eventId is simply
        skipped. But a PRESENT `eventId` is contract, not best-effort: it
        must be a non-empty string whose counter parses through
        `event_sequence` as a bounded u64, and a malformed present id is
        never caught or skipped.
        """
        rail = message_rail(message)
        require(rail is not None, "observe() called with a non-rail message")
        assert rail is not None
        meta = message.get("params", {}).get("_meta", {})
        counter: int | None = None
        if rail != "extension" or (isinstance(meta, dict) and "eventId" in meta):
            # A PRESENT eventId must be well-formed: a non-empty string
            # whose counter half follows a single separator and parses as
            # a bounded u64. Malformed present ids reject right here.
            counter = event_sequence(message)

        total_tokens = meta.get("totalTokens") if isinstance(meta, dict) else None
        if total_tokens is not None:
            # Explicit u64 validation: negative, overflowing, and boolean
            # values are rejected before the high-water comparison runs.
            require_u64(total_tokens, f"{rail} rail _meta.totalTokens")

        # Validate both independently present fields before mutating either
        # high-water, so a bad total cannot partially fold a valid eventId
        # (and an absent extension eventId never skips a present total).
        if counter is not None:
            last = self.last_counter[rail]
            require(
                last is None or counter > last,
                f"{rail} rail eventId counter {counter} did not strictly exceed the "
                f"previous high-water {last}",
            )
        if total_tokens is not None:
            high = self.last_total_tokens[rail]
            require(
                high is None or total_tokens >= high,
                f"{rail} rail totalTokens {total_tokens} moved backwards "
                f"(high-water {high})",
            )

        if counter is not None:
            self.last_counter[rail] = counter
        if total_tokens is not None:
            self.last_total_tokens[rail] = total_tokens

    def observe_all(self, notifications: list[dict[str, Any]]) -> None:
        """Fold every rail notification in a batch into the high-water state."""
        for message in notifications:
            if message_rail(message) is not None:
                self.observe(message)



def _update_kind(message: dict[str, Any]) -> Any:
    return message.get("params", {}).get("update", {}).get("sessionUpdate")


def validate_turn(
    notifications: list[dict[str, Any]],
    prompt_id: str,
    stop_reason: str,
    high_water: SessionHighWater,
) -> dict[str, Any]:
    updates = [message for message in notifications if message.get("method") == STANDARD_UPDATE_METHOD]
    require(updates, "turn emitted no session/update notifications")
    require(
        all(message.get("params", {}).get("_meta", {}).get("promptId") == prompt_id for message in updates),
        "turn update promptId drift",
    )
    # Cross-turn monotonicity, per rail: every standard-rail counter must
    # strictly exceed the last previously observed standard counter (same
    # for the extension rail within itself), and an observed totalTokens
    # must never decrease within its rail. Extension notifications may omit
    # totalTokens or metadata entirely, so they fold only what they carry.
    high_water.observe_all(notifications)
    # The standard rail carries a u64 `_meta.totalTokens` wherever the
    # contract does (every content/tool update of a live turn), so each
    # standard update of the turn must carry a well-formed integer value.
    for message in updates:
        total_tokens = message.get("params", {}).get("_meta", {}).get("totalTokens")
        # Explicit u64 bounds: negative, overflowing, and boolean values are
        # rejected with the exact u64 diagnostic, never silently accepted.
        require_u64(
            total_tokens,
            f"standard-rail session/update {_update_kind(message)!r} "
            "_meta.totalTokens",
        )
    kinds = [message["params"]["update"]["sessionUpdate"] for message in updates]
    require(kinds[0] == "user_message_chunk", "user echo was not the first turn update")
    for message in updates:
        update = message["params"]["update"]
        if update["sessionUpdate"] == "available_commands_update":
            require(
                isinstance(update.get("availableCommands"), list),
                "available_commands_update lacks availableCommands",
            )
            require(
                isinstance(update.get("_meta", {}).get("tools"), list),
                "available_commands_update lacks _meta.tools",
            )
        if update["sessionUpdate"] in (
            "user_message_chunk",
            "agent_message_chunk",
            "agent_thought_chunk",
        ):
            require("content" in update, "chunk uses contentBlock instead of content")
            require("contentBlock" not in update, "chunk leaked obsolete contentBlock field")
    if stop_reason == "end_turn":
        require("agent_message_chunk" in kinds, "completed turn emitted no assistant text")
    text = "".join(
        message["params"]["update"].get("content", {}).get("text", "")
        for message in updates
        if message["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
    )
    assistant_chunks = [
        {
            "index": index,
            "text": message["params"]["update"].get("content", {}).get("text", ""),
        }
        for index, message in enumerate(updates)
        if message["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
    ]
    tool_calls_by_id: dict[str, dict[str, Any]] = {}
    for message in updates:
        update = message["params"]["update"]
        tool_call_id = update.get("toolCallId")
        if update["sessionUpdate"] == "tool_call" and tool_call_id:
            require("meta" not in update, "tool_call leaked obsolete meta key")
            if "_meta" in update:
                require(isinstance(update["_meta"], dict), "tool_call _meta is not an object")
                require(
                    "x-ai/tool" not in update["_meta"] and "xai/tool" not in update["_meta"],
                    "tool_call _meta used a noncanonical tool metadata key",
                )
                if "x.ai/tool" in update["_meta"]:
                    require(
                        isinstance(update["_meta"]["x.ai/tool"], dict),
                        "tool_call _meta x.ai/tool value is not an object",
                    )
            tool_calls_by_id[tool_call_id] = dict(update)
        elif update["sessionUpdate"] == "tool_call_update" and tool_call_id:
            require(tool_call_id in tool_calls_by_id, "tool_call_update preceded tool_call")
            require("fields" not in update, "tool_call_update used obsolete fields wrapper")
            changed = {
                key: value
                for key, value in update.items()
                if key not in ("sessionUpdate", "toolCallId")
            }
            tool_calls_by_id[tool_call_id].update(changed)
    return {
        "updates": len(updates),
        "kinds": kinds,
        "assistant_text": text,
        "assistant_chunks": assistant_chunks,
        "total_tokens": high_water.last_total_tokens["standard"],
        "tool_calls": list(tool_calls_by_id.values()),
    }


def probe_prompt(
    client: LeaderClient,
    session_id: str,
    text: str,
    high_water: SessionHighWater,
) -> dict[str, Any]:
    prompt_id = str(uuid.uuid4())
    response, notifications = client.request(
        "session/prompt",
        {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": text}],
            "_meta": {"promptId": prompt_id, "screenMode": "inline", "sendNow": True},
        },
    )
    require(response.get("result", {}).get("stopReason") == "end_turn", f"prompt failed: {response}")
    result = validate_turn(notifications, prompt_id, "end_turn", high_water)
    result["prompt_id"] = prompt_id
    return result


def probe_tool(
    client: LeaderClient, session_id: str, high_water: SessionHighWater
) -> dict[str, Any]:
    marker = "GENTS_POST_TOOL_FINAL:"
    result = probe_prompt(
        client,
        session_id,
        "Use the list_files tool on the current directory. Only after the tool result arrives, "
        f"reply on one line beginning exactly {marker} followed by the first entry name. "
        "Do not emit that marker before calling the tool.",
        high_water,
    )
    require("tool_call" in result["kinds"], "tool turn emitted no tool_call update")
    tool_call = result["tool_calls"][0]
    require(tool_call.get("toolCallId"), "tool_call lacks toolCallId")
    require(tool_call.get("kind") in ("read", "search"), "read tool has the wrong Grok kind")
    require(tool_call.get("status") in ("in_progress", "completed"), "tool status is invalid")
    require(isinstance(tool_call.get("rawInput"), dict), "tool_call lacks structured rawInput")
    last_tool = max(i for i, kind in enumerate(result["kinds"]) if kind == "tool_call")
    post_tool_text = "".join(
        chunk["text"]
        for chunk in result["assistant_chunks"]
        if chunk["index"] > last_tool
    )
    require(post_tool_text, "tool turn emitted no assistant chunk after the tool update")
    require(marker in post_tool_text, "post-tool final marker was lost")
    result["tool_observed"] = True
    result["post_tool_final_observed"] = True
    return result


def probe_subprocess(
    client: LeaderClient, session_id: str, high_water: SessionHighWater
) -> dict[str, Any]:
    marker = "gents-subprocess-probe"
    result = probe_prompt(
        client,
        session_id,
        f"Run the shell command `echo {marker}` exactly once, then briefly confirm completion.",
        high_water,
    )
    execute_calls = [call for call in result["tool_calls"] if call.get("kind") == "execute"]
    require(execute_calls, "subprocess turn emitted no execute tool_call")
    matching = [
        call
        for call in execute_calls
        if marker in str(call.get("rawInput", {}).get("command", ""))
        and marker in json.dumps(call.get("rawOutput", {}), sort_keys=True)
    ]
    require(matching, "execute tool_call did not preserve the probe command and output")
    result["subprocess_observed"] = True
    return result


def probe_cancel(
    client: LeaderClient, session_id: str, high_water: SessionHighWater
) -> dict[str, Any]:
    request_id = client.next_id
    client.next_id += 1
    prompt_id = str(uuid.uuid4())
    client.send_acp(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [
                    {
                        "type": "text",
                        "text": "Write fifty thousand numbered one-word lines without abbreviating or stopping early.",
                    }
                ],
                "_meta": {"promptId": prompt_id, "screenMode": "inline"},
            },
        }
    )
    notifications: list[dict[str, Any]] = []
    cancelled = False
    while True:
        message = client.recv_acp()
        if message.get("id") == request_id:
            response = message
            break
        notifications.append(message)
        update = message.get("params", {}).get("update", {})
        if not cancelled and update.get("sessionUpdate") == "user_message_chunk":
            client.notify(
                "session/cancel",
                {
                    "sessionId": session_id,
                    "_meta": {
                        "promptId": prompt_id,
                        "cancelSubagents": True,
                        "cancelTrigger": "edge_probe",
                        "rewindIfNoOutput": False,
                        "rewindIfPristine": False,
                    },
                },
            )
            cancelled = True
    require(cancelled, "cancel notification was never sent")
    require(response.get("result", {}).get("stopReason") == "cancelled", f"cancel failed: {response}")
    result = validate_turn(notifications, prompt_id, "cancelled", high_water)
    result["prompt_id"] = prompt_id
    return result


SUBAGENT_MARKER = "port-live-worker"

# The exact outer `SessionNotification` envelope of a live subagent
# lifecycle event, as audited against the Grok pager's ACP handler: the
# JSON-RPC notification top level is exactly `{jsonrpc, method, params}`
# with `jsonrpc == "2.0"` and no `id`, and the camelCase outer params are
# exactly the required `sessionId` and `update` plus the optional `_meta`
# — nothing else. This LIVE probe accepts only the exact live method
# `x.ai/session_notification`; `x.ai/session/update` remains documented as
# replay-path compatibility and is never accepted here.
SESSION_NOTIFICATION_REQUIRED_PARAMS = ("sessionId", "update")
SESSION_NOTIFICATION_OPTIONAL_PARAMS = ("_meta",)
SESSION_NOTIFICATION_TOP_LEVEL_KEYS = ("jsonrpc", "method", "params")
JSONRPC_VERSION = "2.0"


def require_exact_session_notification_envelope(
    message: dict[str, Any], parent_session_id: str
) -> dict[str, Any]:
    """Assert the exact outer `SessionNotification` envelope of one live
    subagent lifecycle event, and return its `update` object.

    The JSON-RPC notification TOP LEVEL is validated exactly, not just
    `params`: the message is an object with exactly the keys
    `{jsonrpc, method, params}`, `jsonrpc` is the exact string `"2.0"`,
    there is no `id` (a notification is never a request — `id: null` is
    still an `id` and rejects), and no extra top-level key survives. The
    method must be the exact live `x.ai/session_notification` (the replay
    alias `x.ai/session/update` is never accepted by this live probe).
    `params` keys are exactly required `sessionId`, `update` plus optional
    `_meta`; `sessionId` must be a non-empty string equal to the parent
    probe session; `update` must be an object. Wrong `session_id` casing,
    a wrong or missing parent session, unknown extras, a wrong `jsonrpc`
    version, any `id`, extra top-level keys, a wrong method, and the
    replay alias are all rejected.
    """
    require(
        isinstance(message, dict),
        f"lifecycle notification must be a JSON object; got {message!r}",
    )
    top_level = set(message.keys())
    require(
        top_level == set(SESSION_NOTIFICATION_TOP_LEVEL_KEYS),
        f"lifecycle notification top level must be exactly "
        f"{list(SESSION_NOTIFICATION_TOP_LEVEL_KEYS)}; got: {sorted(top_level)}",
    )
    require(
        "id" not in message,
        f"a JSON-RPC notification carries no id; got id: {message.get('id')!r}",
    )
    jsonrpc = message["jsonrpc"]
    require(
        jsonrpc == JSONRPC_VERSION,
        f"lifecycle notification jsonrpc must be the exact string "
        f"{JSONRPC_VERSION!r}; got {jsonrpc!r}",
    )
    method = message["method"]
    require(
        method == EXT_SESSION_NOTIFICATION_METHOD,
        f"live subagent lifecycle event must ride the exact live method "
        f"{EXT_SESSION_NOTIFICATION_METHOD!r}; got {method!r} "
        f"(x.ai/session/update is only a documented replay-path alias and is "
        f"never accepted by this live probe)",
    )
    params = message.get("params")
    require(
        isinstance(params, dict),
        f"{EXT_SESSION_NOTIFICATION_METHOD} params must be an object; got {params!r}",
    )
    actual = set(params.keys())
    missing = sorted(set(SESSION_NOTIFICATION_REQUIRED_PARAMS) - actual)
    unexpected = sorted(actual - set(SESSION_NOTIFICATION_REQUIRED_PARAMS) - set(SESSION_NOTIFICATION_OPTIONAL_PARAMS))
    require(
        not missing,
        f"{EXT_SESSION_NOTIFICATION_METHOD} envelope is missing required "
        f"param(s) {missing}; got: {sorted(actual)}",
    )
    require(
        not unexpected,
        f"{EXT_SESSION_NOTIFICATION_METHOD} envelope carries param(s) outside "
        f"the exact set (required {list(SESSION_NOTIFICATION_REQUIRED_PARAMS)} + "
        f"optional {list(SESSION_NOTIFICATION_OPTIONAL_PARAMS)}): {unexpected}; "
        f"got: {sorted(actual)}",
    )
    session_id = params["sessionId"]
    require(
        isinstance(session_id, str) and bool(session_id),
        f"{EXT_SESSION_NOTIFICATION_METHOD} envelope sessionId must be a "
        f"non-empty string; got {session_id!r}",
    )
    require(
        session_id == parent_session_id,
        f"{EXT_SESSION_NOTIFICATION_METHOD} envelope sessionId {session_id!r} "
        f"must equal the parent probe session {parent_session_id!r}",
    )
    update = params["update"]
    require(
        isinstance(update, dict),
        f"{EXT_SESSION_NOTIFICATION_METHOD} envelope update must be an object; "
        f"got {update!r}",
    )
    return update


def extract_subagent_lifecycle(
    notifications: list[dict[str, Any]],
    parent_session_id: str,
) -> dict[str, Any]:
    """Pull the spawned/progress/finished lifecycle off the two rails.

    The early `task`-titled standard `tool_call` (the pager-local
    foreground-wait marker) is read from the standard rail; the spawned/
    progress/finished lifecycle is read from the extension rail. Every
    lifecycle candidate first passes the exact outer
    `SessionNotification` validator
    (`require_exact_session_notification_envelope`) — method, exact params
    key set, non-empty parent-equal `sessionId`, object `update` — and
    then the matching exact inner DTO validator runs for EVERY candidate,
    including repeated progress updates. After the scan, every candidate
    is correlated to the same parent and spawned child. Returns the first
    correlated wire evidence for each stage plus the observation order so
    callers can assert chronology.
    """
    lifecycle: dict[str, Any] = {
        "spawned": None,
        "progress": None,
        "finished": None,
        "task_tool_call": None,
        "observation_index": {},
    }
    candidates: dict[str, list[dict[str, Any]]] = {
        "spawned": [],
        "progress": [],
        "finished": [],
    }
    for index, message in enumerate(notifications):
        update = message.get("params", {}).get("update", {})
        kind = update.get("sessionUpdate")
        method = message.get("method")
        if kind in ("subagent_spawned", "subagent_progress", "subagent_finished"):
            # The outer envelope is validated exactly, before any inner
            # DTO validator runs: wrong method (including the replay
            # alias), wrong/missing parent session, wrong `session_id`
            # casing, extras, or a non-object update all fail here.
            envelope_update = require_exact_session_notification_envelope(
                message, parent_session_id
            )
            stage = kind.removeprefix("subagent_")
            if stage == "spawned":
                require_exact_spawned_dto(envelope_update)
            elif stage == "progress":
                require_exact_progress_dto(envelope_update)
            else:
                require_exact_finished_dto(envelope_update)
            candidate = {"method": method, "update": envelope_update}
            candidates[stage].append(candidate)
            if lifecycle[stage] is None:
                lifecycle[stage] = candidate
                lifecycle["observation_index"][stage] = index
        elif (
            method == STANDARD_UPDATE_METHOD
            and kind == "tool_call"
            and update.get("title") in ("task", "Task", "spawn_subagent")
            and lifecycle["task_tool_call"] is None
        ):
            lifecycle["task_tool_call"] = update
            lifecycle["observation_index"]["task_tool_call"] = index

    if any(candidates.values()):
        require(
            bool(candidates["spawned"]),
            "subagent progress/finished lifecycle appeared without a spawned event",
        )
        child_session_id = candidates["spawned"][0]["update"]["child_session_id"]
        for candidate in candidates["spawned"]:
            update = candidate["update"]
            require(
                update["parent_session_id"] == parent_session_id
                and update["child_session_id"] == child_session_id,
                f"subagent_spawned candidate drifted from parent/child correlation: {update}",
            )
        for candidate in candidates["progress"]:
            update = candidate["update"]
            require(
                update["parent_session_id"] == parent_session_id
                and update["subagent_id"] == child_session_id
                and update["child_session_id"] == child_session_id,
                f"subagent_progress candidate drifted from parent/child correlation: {update}",
            )
        for candidate in candidates["finished"]:
            update = candidate["update"]
            require(
                update["subagent_id"] == child_session_id
                and update["child_session_id"] == child_session_id,
                f"subagent_finished candidate drifted from child correlation: {update}",
            )
    return lifecycle


U32_MAX = 0xFFFFFFFF
U64_MAX = 0xFFFFFFFFFFFFFFFF

SUBAGENT_FINISHED_STATUSES = ("completed", "failed", "cancelled")


def _require_u32(update: dict[str, Any], field: str, kind: str) -> None:
    value = update[field]
    require(
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= U32_MAX,
        f"{kind} {field} must be a nonnegative u32 integer; got {value!r}",
    )


def _require_u64(update: dict[str, Any], field: str, kind: str) -> None:
    value = update[field]
    require(
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= U64_MAX,
        f"{kind} {field} must be a nonnegative u64 integer; got {value!r}",
    )


def _require_nonempty_string(update: dict[str, Any], field: str, kind: str) -> None:
    value = update[field]
    require(
        isinstance(value, str) and bool(value),
        f"{kind} {field} must be a non-empty string; got {value!r}",
    )


def _require_optional_string(update: dict[str, Any], field: str, kind: str) -> None:
    value = update.get(field)
    require(
        value is None or isinstance(value, str),
        f"{kind} optional {field} must be absent, null, or a string as its serde "
        f"Option<String> allows; got {value!r}",
    )


def _require_field_set(
    update: dict[str, Any],
    kind: str,
    required: set[str],
    optional: set[str] = frozenset(()),  # type: ignore[assignment]
) -> set[str]:
    """Assert the exact serde-serialized field set for one variant.

    `required` fields are always serialized by the variant's struct and must
    be present; `optional` fields are serde `Option`/default fields and may
    be absent, null, or present. Anything else (unknown extras, legacy
    camelCase names) is rejected. Returns the actual key set.
    """
    actual = set(update.keys())
    missing = sorted(required - actual)
    unexpected = sorted(actual - required - set(optional))
    require(
        not missing,
        f"{kind} DTO is missing required field(s) {missing}; got: {sorted(actual)}",
    )
    require(
        not unexpected,
        f"{kind} DTO carries field(s) outside the exact set "
        f"(required {sorted(required)} + optional {sorted(optional)}): "
        f"{unexpected}; got: {sorted(actual)}",
    )
    return actual


def require_exact_finished_dto(update: dict[str, Any]) -> None:
    """Assert the exact serde schema of the live `subagent_finished` DTO.

    Pure schema validation against the authoritative Grok serde source
    (`SessionUpdate::SubagentFinished`): the tagged enum applies
    `rename_all="snake_case"` to the variant tag (`sessionUpdate`) and
    variant names only, so the variant FIELDS serialize with their Rust
    snake_case names. The exact always-serialized required fields are
    `sessionUpdate`, `subagent_id`, `child_session_id`, `status`,
    `tool_calls`, `turns`, `duration_ms`, `tokens_used`, `will_wake`;
    `error` and `output` are serde options and may be absent, null, or a
    string. `status` must be exactly one of `completed|failed|cancelled` —
    a failed or cancelled DTO is still a WELL-FORMED exact DTO, so this
    validator deliberately does NOT require the completed outcome: that
    is a separate live-probe concern (see `require_live_success_dto`).
    `parent_session_id` is forbidden on the finished variant, and
    `subagent_id == child_session_id`.
    """
    required = {
        "sessionUpdate",
        "subagent_id",
        "child_session_id",
        "status",
        "tool_calls",
        "turns",
        "duration_ms",
        "tokens_used",
        "will_wake",
    }
    optional = {"error", "output"}
    _require_field_set(update, "subagent_finished", required, optional)
    require(
        update["sessionUpdate"] == "subagent_finished",
        f"subagent_finished DTO tag must read 'subagent_finished'; got "
        f"{update['sessionUpdate']!r}",
    )
    require(
        "parent_session_id" not in update,
        "subagent_finished DTO must NOT carry parent_session_id",
    )
    _require_nonempty_string(update, "subagent_id", "subagent_finished")
    _require_nonempty_string(update, "child_session_id", "subagent_finished")
    require(
        update["subagent_id"] == update["child_session_id"],
        f"subagent_id must be the child session id; got subagent_id="
        f"{update['subagent_id']!r} child_session_id={update['child_session_id']!r}",
    )
    require(
        update["status"] in SUBAGENT_FINISHED_STATUSES,
        f"subagent_finished status must be one of "
        f"{SUBAGENT_FINISHED_STATUSES}; got {update['status']!r}",
    )
    _require_optional_string(update, "error", "subagent_finished")
    _require_optional_string(update, "output", "subagent_finished")
    _require_u32(update, "tool_calls", "subagent_finished")
    _require_u32(update, "turns", "subagent_finished")
    _require_u64(update, "duration_ms", "subagent_finished")
    _require_u64(update, "tokens_used", "subagent_finished")
    require(
        isinstance(update["will_wake"], bool),
        f"subagent_finished will_wake must be a bool; got {update['will_wake']!r}",
    )


def require_live_success_dto(update: dict[str, Any]) -> None:
    """Require this foreground live probe's successful finished outcome.

    Separate from the schema validator: `require_exact_finished_dto` proves
    the DTO is exactly the authoritative serde shape (and a `failed` or
    `cancelled` DTO passes it as well-formed); this check then requires
    what this particular live foreground success edge must observe —
    `status == "completed"` and a foreground `will_wake is False`. A valid
    failed DTO is recognized as well-formed but still fails this edge.
    """
    require(
        update["status"] == "completed",
        f"foreground subagent finished with status {update['status']!r} "
        f"(well-formed exact DTO, but not the completed outcome this live "
        f"success edge requires)",
    )
    require(
        update["will_wake"] is False,
        f"foreground subagent_finished will_wake must read false; got "
        f"{update['will_wake']!r}",
    )


def require_exact_spawned_dto(update: dict[str, Any]) -> None:
    """Assert the exact serde schema of the live `subagent_spawned` DTO.

    Against the authoritative `SessionUpdate::SubagentSpawned`: the
    required always-serialized fields are `sessionUpdate`, `subagent_id`,
    `parent_session_id`, `child_session_id`, `subagent_type`,
    `description`. The serde-option/default extras — `parent_prompt_id`,
    `effective_context_source`, `context_normalized`,
    `capability_mode`, `persona`, `role`, `model`, `resumed_from`,
    `workflow_run_id` — may each be absent (note `context_normalized` is
    `skip_serializing_if = Not::not`, so a false value is omitted), null,
    or present with its serde-allowed value. Unknown extras and the legacy
    camelCase names are rejected.
    """
    required = {
        "sessionUpdate",
        "subagent_id",
        "parent_session_id",
        "child_session_id",
        "subagent_type",
        "description",
    }
    optional = {
        "parent_prompt_id",
        "effective_context_source",
        "context_normalized",
        "capability_mode",
        "persona",
        "role",
        "model",
        "resumed_from",
        "workflow_run_id",
    }
    _require_field_set(update, "subagent_spawned", required, optional)
    require(
        update["sessionUpdate"] == "subagent_spawned",
        f"subagent_spawned DTO tag must read 'subagent_spawned'; got "
        f"{update['sessionUpdate']!r}",
    )
    _require_nonempty_string(update, "subagent_id", "subagent_spawned")
    _require_nonempty_string(update, "parent_session_id", "subagent_spawned")
    _require_nonempty_string(update, "child_session_id", "subagent_spawned")
    _require_nonempty_string(update, "subagent_type", "subagent_spawned")
    _require_nonempty_string(update, "description", "subagent_spawned")
    require(
        update["subagent_id"] == update["child_session_id"],
        f"subagent_spawned subagent_id must be the child session id; got "
        f"subagent_id={update['subagent_id']!r} "
        f"child_session_id={update['child_session_id']!r}",
    )
    for field in (
        "parent_prompt_id",
        "effective_context_source",
        "capability_mode",
        "persona",
        "role",
        "model",
        "resumed_from",
        "workflow_run_id",
    ):
        _require_optional_string(update, field, "subagent_spawned")
    if "context_normalized" in update:
        require(
            isinstance(update["context_normalized"], bool),
            f"subagent_spawned context_normalized must be a bool when present; "
            f"got {update['context_normalized']!r}",
        )


def require_exact_progress_dto(update: dict[str, Any]) -> None:
    """Assert the exact serde schema of the live `subagent_progress` DTO.

    Against the authoritative `SessionUpdate::SubagentProgress`: every
    field is required and always serialized — `sessionUpdate`,
    `subagent_id`, `parent_session_id`, `child_session_id`,
    `duration_ms`, `turn_count`, `tool_call_count`, `tokens_used`,
    `context_window_tokens`, `context_usage_pct`, `tools_used`,
    `error_count`. `context_usage_pct` is a u8 percentage (0-100).
    Unknown extras and the legacy camelCase names are rejected.
    """
    required = {
        "sessionUpdate",
        "subagent_id",
        "parent_session_id",
        "child_session_id",
        "duration_ms",
        "turn_count",
        "tool_call_count",
        "tokens_used",
        "context_window_tokens",
        "context_usage_pct",
        "tools_used",
        "error_count",
    }
    _require_field_set(update, "subagent_progress", required)
    require(
        update["sessionUpdate"] == "subagent_progress",
        f"subagent_progress DTO tag must read 'subagent_progress'; got "
        f"{update['sessionUpdate']!r}",
    )
    _require_nonempty_string(update, "subagent_id", "subagent_progress")
    _require_nonempty_string(update, "parent_session_id", "subagent_progress")
    _require_nonempty_string(update, "child_session_id", "subagent_progress")
    require(
        update["subagent_id"] == update["child_session_id"],
        f"subagent_progress subagent_id must be the child session id; got "
        f"subagent_id={update['subagent_id']!r} "
        f"child_session_id={update['child_session_id']!r}",
    )
    _require_u64(update, "duration_ms", "subagent_progress")
    _require_u32(update, "turn_count", "subagent_progress")
    _require_u32(update, "tool_call_count", "subagent_progress")
    _require_u64(update, "tokens_used", "subagent_progress")
    _require_u64(update, "context_window_tokens", "subagent_progress")
    pct = update["context_usage_pct"]
    require(
        isinstance(pct, int) and not isinstance(pct, bool) and 0 <= pct <= 100,
        f"subagent_progress context_usage_pct must be a percentage in [0, 100]; "
        f"got {pct!r}",
    )
    require(
        isinstance(update["tools_used"], list)
        and all(isinstance(name, str) for name in update["tools_used"]),
        f"subagent_progress tools_used must be a list of strings; got "
        f"{update['tools_used']!r}",
    )
    _require_u32(update, "error_count", "subagent_progress")


def self_test_subagent_lifecycle_validators() -> dict[str, int]:
    """Prove the exact DTO validators against the authoritative serde shape.

    This inline fixture calls the REAL validators — there is no separate
    fields-only checker, so nothing can bypass `require_exact_finished_dto`
    and no valid outcome can be silently conflated with schema validity.
    Every case below exercises the same functions the live foreground
    probe runs on actual wire envelopes:
    - a completed DTO with both optionals absent is an exact DTO and a live
      success;
    - a completed DTO with `output` is still exact;
    - a failed DTO with `error` is an exact WELL-FORMED DTO (schema) yet
      the live success outcome check rejects it — the separation that
      removes the old false confidence;
    - a cancelled DTO is likewise accepted as exact and rejected live;
    - camelCase legacy fields, missing required tokens_used/will_wake,
      forbidden parent_session_id, identity mismatch, and negative /
      overflowing / boolean counters all fail the schema validator;
    - exact spawned/progress good fixtures pass their validators, while
      camelCase casing, unknown extras, missing required fields, and
      wrong field types fail them.
    - an over-limit published Unix socket remains connectable through the
      same private short-alias bridge used by the live probe.
    """

    self_test_portable_socket_path()

    # Every wrapper increments a counter for every real-validator accept and
    # reject, and every live-outcome rejection. The returned evidence is the
    # MEASURED count of executed cases — nothing is hard-coded, so the
    # fixture body below can be edited without silently desynchronizing the
    # reported evidence (the expected totals asserted at the end are derived
    # from the fixtures themselves, not from prose).
    counters: dict[str, int] = {
        "finished_accepted": 0,
        "finished_rejected_schema": 0,
        "finished_rejected_live_outcome": 0,
        "spawned_accepted": 0,
        "spawned_rejected": 0,
        "progress_accepted": 0,
        "progress_rejected": 0,
        "envelope_accepted": 0,
        "envelope_rejected": 0,
        "u64_validator_accepted": 0,
        "u64_validator_rejected": 0,
        "high_water_rejected": 0,
        "stub_result_accepted": 0,
        "stub_result_rejected": 0,
    }

    def accepted(update: dict[str, Any]) -> None:
        require_exact_finished_dto(update)
        counters["finished_accepted"] += 1

    def rejected_exact(update: dict[str, Any]) -> None:
        try:
            require_exact_finished_dto(update)
        except AssertionError as error:
            # The schema validator must reject this fixture on schema
            # grounds, never by conflating DTO validity with the completed
            # outcome (the old false-confidence bug): the vocabulary check
            # ("status must be one of ...") is schema, but a bare completed
            # requirement is not.
            require(
                "foreground subagent finished with status" not in str(error)
                and "not the completed outcome" not in str(error),
                f"schema validator wrongly encoded the live outcome: {error}",
            )
            counters["finished_rejected_schema"] += 1
            return
        raise AssertionError(f"finished fixture must be rejected: {update}")

    def rejected_live(update: dict[str, Any]) -> None:
        require_exact_finished_dto(update)
        try:
            require_live_success_dto(update)
        except AssertionError:
            counters["finished_rejected_live_outcome"] += 1
            return
        raise AssertionError(f"live success fixture must be rejected: {update}")

    def spawned_accepted(update: dict[str, Any]) -> None:
        require_exact_spawned_dto(update)
        counters["spawned_accepted"] += 1

    def spawned_rejected(update: dict[str, Any]) -> None:
        try:
            require_exact_spawned_dto(update)
        except AssertionError:
            counters["spawned_rejected"] += 1
            return
        raise AssertionError(f"spawned fixture must be rejected: {update}")

    def progress_accepted(update: dict[str, Any]) -> None:
        require_exact_progress_dto(update)
        counters["progress_accepted"] += 1

    def progress_rejected(update: dict[str, Any]) -> None:
        try:
            require_exact_progress_dto(update)
        except AssertionError:
            counters["progress_rejected"] += 1
            return
        raise AssertionError(f"progress fixture must be rejected: {update}")

    def envelope_accepted(message: dict[str, Any], parent: str) -> None:
        require_exact_session_notification_envelope(message, parent)
        counters["envelope_accepted"] += 1

    def envelope_rejected(message: dict[str, Any], parent: str) -> None:
        try:
            require_exact_session_notification_envelope(message, parent)
        except AssertionError:
            counters["envelope_rejected"] += 1
            return
        raise AssertionError(f"envelope fixture must be rejected: {message}")

    def u64_accepted(value: Any) -> None:
        require_u64(value, "self-test u64 fixture")
        counters["u64_validator_accepted"] += 1

    def u64_rejected(value: Any) -> None:
        try:
            require_u64(value, "self-test u64 fixture")
        except AssertionError:
            counters["u64_validator_rejected"] += 1
            return
        raise AssertionError(f"u64 fixture must be rejected: {value!r}")

    def high_water_rejected(observer: Callable[[], None]) -> None:
        try:
            observer()
        except AssertionError:
            counters["high_water_rejected"] += 1
            return
        raise AssertionError("high-water fixture must be rejected")

    def stub_result_accepted(method: str, result: Any, what: str) -> None:
        # The good fixtures run through the SAME shared validator the live
        # handshake uses — an accept is only counted when the real
        # contract check passes, never by bare equality bookkeeping.
        require_exact_stub_result(method, result, subagent_id="missing-child-1")
        counters["stub_result_accepted"] += 1

    def stub_result_rejected(method: str, result: Any, what: str) -> None:
        # A wrong fixture is only a MEASURED reject when the shared exact
        # validator itself rejects it — never merely because
        # `wrong != expected` (that comparison proves nothing about the
        # validator and would count drift as evidence).
        try:
            require_exact_stub_result(method, result, subagent_id="missing-child-1")
        except AssertionError:
            counters["stub_result_rejected"] += 1
            return
        raise AssertionError(f"stub result fixture must be rejected: {what}: {result!r}")

    def mutate(base: dict[str, Any], **changes: Any) -> dict[str, Any]:
        clone = dict(base)
        clone.update(changes)
        return clone

    # ------------------------------------------------------------------
    # Fixture collections. Every case below is executed through the REAL
    # validators by the loops at the end of this function, and the
    # expected totals asserted there are derived from these collections
    # (the length of each list, plus the extractor accepts counted from
    # the lifecycle batch itself) — no hand-typed count can drift.
    # ------------------------------------------------------------------

    # The authoritative always-serialized finished field set (serde: no
    # skip_serializing_if on these; `error`/`output` are Options).
    finished_completed = {
        "sessionUpdate": "subagent_finished",
        "subagent_id": "sa-1",
        "child_session_id": "sa-1",
        "status": "completed",
        "tool_calls": 2,
        "turns": 1,
        "duration_ms": 1500,
        "tokens_used": 750,
        "will_wake": False,
    }
    failed_with_error = mutate(
        finished_completed, status="failed", error="failed with error"
    )
    cancelled_dto = mutate(finished_completed, status="cancelled")

    # Exact finished DTOs: completed with both optionals absent, explicit
    # null optionals (serde-legal for the Options), output present, and
    # the well-formed failed/cancelled DTOs (exact schema, non-success
    # outcome — the separation that removes the old false confidence).
    finished_accept_fixtures = [
        finished_completed,
        mutate(finished_completed, error=None, output=None),
        mutate(finished_completed, output="worker ran"),
        failed_with_error,
        cancelled_dto,
    ]
    # The live foreground success edge accepts exactly the completed ones.
    finished_live_success_fixtures = [
        finished_completed,
        mutate(finished_completed, output="worker ran"),
    ]
    finished_live_reject_fixtures = [failed_with_error, cancelled_dto]

    # Schema-level rejections (the validator must not pass on any of
    # these, and none of these rejections may be an outcome rejection).
    finished_schema_reject_fixtures = [
        # camelCase legacy field names.
        mutate(
            dict(finished_completed),
            subagentId="sa-1",
            childSessionId="sa-1",
            child_session_id=None,
            subagent_id=None,
        ),
        # required tokens_used missing (old pre-tokens_used replay shape
        # is NOT the live serialized wire).
        {key: value for key, value in finished_completed.items() if key != "tokens_used"},
        # required will_wake missing (its serde default is
        # deserialization-only; the serialized live wire always carries it).
        {key: value for key, value in finished_completed.items() if key != "will_wake"},
        # forbidden parent_session_id on the finished variant.
        mutate(finished_completed, parent_session_id="parent-1"),
        # identity mismatch: subagent_id != child_session_id.
        mutate(finished_completed, subagent_id="other"),
        # unknown extra field.
        mutate(finished_completed, extra_field="x"),
        # invalid status vocabulary.
        mutate(finished_completed, status="succeeded"),
        # nonnegative bounded counters: negative, overflow, and bool all fail.
        mutate(finished_completed, tokens_used=-1),
        mutate(finished_completed, tokens_used=U64_MAX + 1),
        mutate(finished_completed, tokens_used=True),
        mutate(finished_completed, tool_calls=-1),
        mutate(finished_completed, tool_calls=U32_MAX + 1),
        mutate(finished_completed, tool_calls=False),
        mutate(finished_completed, turns=-2),
        mutate(finished_completed, turns=U32_MAX + 1),
        mutate(finished_completed, turns=True),
        mutate(finished_completed, duration_ms=-5),
        mutate(finished_completed, duration_ms=U64_MAX + 1),
        mutate(finished_completed, duration_ms=True),
        # will_wake must be a bool (not 0/1, not a string).
        mutate(finished_completed, will_wake=0),
        mutate(finished_completed, will_wake="false"),
        # optional error/output must be absent/null/string.
        mutate(finished_completed, error=123),
        mutate(finished_completed, output=["x"]),
        # tag drift.
        mutate(finished_completed, sessionUpdate="SubagentFinished"),
    ]

    # The authoritative always-serialized spawned field set.
    spawned_good = {
        "sessionUpdate": "subagent_spawned",
        "subagent_id": "sa-1",
        "parent_session_id": "parent-1",
        "child_session_id": "sa-1",
        "subagent_type": "general-purpose",
        "description": "probe child",
    }
    # false context_normalized is omitted on the wire (skip_serializing_if
    # = Not::not), so the minimal fixture is the false case; the true case
    # serializes the field; every serde-option extra may be present; and
    # explicit null optionals are serde-legal.
    spawned_accept_fixtures = [
        spawned_good,
        mutate(spawned_good, context_normalized=True),
        mutate(
            spawned_good,
            context_normalized=True,
            parent_prompt_id="pp-1",
            effective_context_source="new",
            capability_mode="read-only",
            persona="p",
            role="researcher",
            model="GLM-5.3-Flash-NVFP4",
            resumed_from="sa-0",
            workflow_run_id="wf-1",
        ),
        mutate(spawned_good, parent_prompt_id=None),
    ]
    spawned_reject_fixtures = [
        # camelCase legacy spawned shape.
        {
            "sessionUpdate": "subagent_spawned",
            "subagentId": "sa-1",
            "parentSessionId": "parent-1",
            "childSessionId": "sa-1",
            "subagentType": "general-purpose",
            "description": "probe child",
        },
        # missing required field.
        {key: value for key, value in spawned_good.items() if key != "description"},
        # unknown extra.
        mutate(spawned_good, extra_field="x"),
        # identity mismatch.
        mutate(spawned_good, subagent_id="other"),
        # wrong optional type.
        mutate(spawned_good, model=123),
        # context_normalized must be a bool when present.
        mutate(spawned_good, context_normalized="yes"),
        # tag drift.
        mutate(spawned_good, sessionUpdate="SubagentSpawned"),
    ]

    # The authoritative all-required progress field set.
    progress_good = {
        "sessionUpdate": "subagent_progress",
        "subagent_id": "sa-1",
        "parent_session_id": "parent-1",
        "child_session_id": "sa-1",
        "duration_ms": 100,
        "turn_count": 1,
        "tool_call_count": 1,
        "tokens_used": 100,
        "context_window_tokens": 262_144,
        "context_usage_pct": 0,
        "tools_used": ["list_files"],
        "error_count": 0,
    }
    progress_accept_fixtures = [
        progress_good,
        mutate(progress_good, tools_used=[]),
    ]
    progress_reject_fixtures = [
        # camelCase legacy progress shape.
        {
            "sessionUpdate": "subagent_progress",
            "subagentId": "sa-1",
            "parentSessionId": "parent-1",
            "childSessionId": "sa-1",
            "durationMs": 100,
            "turnCount": 1,
            "toolCallCount": 1,
            "tokensUsed": 100,
            "contextWindowTokens": 262_144,
            "contextUsagePct": 0,
            "toolsUsed": ["list_files"],
            "errorCount": 0,
        },
        # missing required field.
        {key: value for key, value in progress_good.items() if key != "error_count"},
        # unknown extra.
        mutate(progress_good, extra_field="x"),
        # identity mismatch.
        mutate(progress_good, subagent_id="other"),
        # percentage bounds.
        mutate(progress_good, context_usage_pct=101),
        mutate(progress_good, context_usage_pct=-1),
        mutate(progress_good, context_usage_pct=True),
        # counter bounds/types.
        mutate(progress_good, tokens_used=-1),
        mutate(progress_good, tokens_used=U64_MAX + 1),
        mutate(progress_good, turn_count=U32_MAX + 1),
        mutate(progress_good, tool_call_count=True),
        mutate(progress_good, duration_ms=-1),
        # tools_used must be a list of strings.
        mutate(progress_good, tools_used="list_files"),
        mutate(progress_good, tools_used=[1]),
        # tag drift.
        mutate(progress_good, sessionUpdate="SubagentProgress"),
    ]

    # ------------------------------------------------------------------
    # The exact outer SessionNotification envelope (live rail only). The
    # JSON-RPC notification top level is exactly {jsonrpc, method, params}
    # with jsonrpc "2.0" and no id.
    # ------------------------------------------------------------------
    envelope_parent = "probe-parent-1"
    envelope_update = {"sessionUpdate": "subagent_spawned", "subagent_id": "sa-1"}
    good_envelope = {
        "jsonrpc": JSONRPC_VERSION,
        "method": EXT_SESSION_NOTIFICATION_METHOD,
        "params": {"sessionId": envelope_parent, "update": envelope_update},
    }
    envelope_accept_fixtures = [
        # the minimal exact envelope (no _meta).
        (good_envelope, envelope_parent),
        # _meta is the one optional param.
        (
            mutate(
                good_envelope,
                params={"sessionId": envelope_parent, "update": envelope_update, "_meta": {}},
            ),
            envelope_parent,
        ),
        # _meta may carry the pager's event metadata keys.
        (
            mutate(
                good_envelope,
                params={
                    "sessionId": envelope_parent,
                    "update": envelope_update,
                    "_meta": {"eventId": f"{envelope_parent}-1", "totalTokens": 4},
                },
            ),
            envelope_parent,
        ),
    ]
    envelope_reject_fixtures = [
        # the replay alias is never accepted by this LIVE probe.
        (mutate(good_envelope, method=EXT_SESSION_UPDATE_ALIAS_METHOD), envelope_parent),
        # wrong method.
        (mutate(good_envelope, method=STANDARD_UPDATE_METHOD), envelope_parent),
        (mutate(good_envelope, method="x.ai/session_notification"), envelope_parent),
        (mutate(good_envelope, method=None), envelope_parent),
        # missing jsonrpc (the top level is no longer exact).
        (
            {key: value for key, value in good_envelope.items() if key != "jsonrpc"},
            envelope_parent,
        ),
        # wrong jsonrpc version.
        (mutate(good_envelope, jsonrpc="1.0"), envelope_parent),
        (mutate(good_envelope, jsonrpc="2"), envelope_parent),
        (mutate(good_envelope, jsonrpc=2.0), envelope_parent),
        # non-string jsonrpc.
        (mutate(good_envelope, jsonrpc=2), envelope_parent),
        (mutate(good_envelope, jsonrpc=None), envelope_parent),
        (mutate(good_envelope, jsonrpc=True), envelope_parent),
        # any id — a notification is never a request; id: null is still an id.
        (mutate(good_envelope, id=None), envelope_parent),
        (mutate(good_envelope, id=1), envelope_parent),
        (mutate(good_envelope, id="req-1"), envelope_parent),
        # extra top-level keys.
        (mutate(good_envelope, extra="x"), envelope_parent),
        (mutate(good_envelope, result={}), envelope_parent),
        # wrong session_id casing.
        (
            mutate(good_envelope, params={"session_id": envelope_parent, "update": envelope_update}),
            envelope_parent,
        ),
        (
            mutate(good_envelope, params={"SessionId": envelope_parent, "update": envelope_update}),
            envelope_parent,
        ),
        # missing parent session.
        (mutate(good_envelope, params={"update": envelope_update}), envelope_parent),
        # wrong parent session.
        (good_envelope, "other-parent-1"),
        # empty or non-string parent session.
        (mutate(good_envelope, params={"sessionId": "", "update": envelope_update}), envelope_parent),
        (mutate(good_envelope, params={"sessionId": 7, "update": envelope_update}), envelope_parent),
        # missing update.
        (mutate(good_envelope, params={"sessionId": envelope_parent}), envelope_parent),
        # non-object update.
        (mutate(good_envelope, params={"sessionId": envelope_parent, "update": "x"}), envelope_parent),
        (mutate(good_envelope, params={"sessionId": envelope_parent, "update": None}), envelope_parent),
        # unknown extras.
        (
            mutate(
                good_envelope,
                params={"sessionId": envelope_parent, "update": envelope_update, "extra": "x"},
            ),
            envelope_parent,
        ),
        (
            mutate(
                good_envelope,
                params={
                    "sessionId": envelope_parent,
                    "update": envelope_update,
                    "_meta": {},
                    "subagentId": "sa-1",
                },
            ),
            envelope_parent,
        ),
        # non-object params.
        (mutate(good_envelope, params=[envelope_parent, envelope_update]), envelope_parent),
        # non-object message.
        (["not", "an", "object"], envelope_parent),
    ]

    # The extractor runs the same outer validator for every lifecycle kind
    # before any inner DTO validator: a well-formed lifecycle batch with
    # the exact envelope yields all three stages in order.
    lifecycle_spawned = mutate(spawned_good, parent_session_id=envelope_parent)
    lifecycle_progress = mutate(progress_good, parent_session_id=envelope_parent)
    lifecycle_batch = [
        {
            "jsonrpc": JSONRPC_VERSION,
            "method": "_x.ai/models/update",
            "params": {"availableModels": [], "currentModelId": "fixture-model"},
        },
        {
            "jsonrpc": JSONRPC_VERSION,
            "method": STANDARD_UPDATE_METHOD,
            "params": {
                "sessionId": envelope_parent,
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "tc-1",
                    "title": "task",
                    "kind": "read",
                    "status": "in_progress",
                    "rawInput": {},
                    "_meta": {"subagentBackground": False},
                },
                "_meta": {},
            },
        },
        mutate(
            good_envelope,
            params={"sessionId": envelope_parent, "update": lifecycle_spawned, "_meta": {}},
        ),
        mutate(
            good_envelope,
            params={"sessionId": envelope_parent, "update": lifecycle_progress, "_meta": {}},
        ),
        mutate(good_envelope, params={"sessionId": envelope_parent, "update": finished_completed, "_meta": {}}),
    ]
    # The extractor rejects the replay alias and a wrong parent session:
    # the stage stays missing instead of accepting a bad envelope.
    extractor_reject_batches = [
        [
            {
                "jsonrpc": JSONRPC_VERSION,
                "method": EXT_SESSION_UPDATE_ALIAS_METHOD,
                "params": {"sessionId": envelope_parent, "update": spawned_good},
            }
        ],
        [mutate(good_envelope, params={"sessionId": "other", "update": lifecycle_spawned})],
        # A malformed later progress update must not hide behind the first
        # retained good progress DTO.
        lifecycle_batch
        + [
            mutate(
                good_envelope,
                params={
                    "sessionId": envelope_parent,
                    "update": mutate(lifecycle_progress, tokens_used=True),
                },
            )
        ],
        # Every well-shaped repeated update must still correlate to the
        # spawned child, not merely pass its local DTO schema.
        lifecycle_batch
        + [
            mutate(
                good_envelope,
                params={
                    "sessionId": envelope_parent,
                    "update": mutate(
                        lifecycle_progress,
                        subagent_id="other-child",
                        child_session_id="other-child",
                    ),
                },
            )
        ],
    ]

    # ------------------------------------------------------------------
    # Unsigned wire high-waters: direct fixtures for both bounds, the
    # valid max, and every rejected shape.
    # ------------------------------------------------------------------
    u64_accept_values = [0, U64_MAX, 1, 262_144]
    u64_reject_values = [-1, U64_MAX + 1, True, False, 1.5, "12", None]

    def event_message(event_id: Any) -> dict[str, Any]:
        return {
            "method": STANDARD_UPDATE_METHOD,
            "params": {"_meta": {"eventId": event_id}},
        }

    # event_sequence: the counter half of an eventId is a u64.
    event_sequence_fixtures = [("s-1", 1), (f"s-{U64_MAX}", U64_MAX)]
    # A bool counter never reaches the wire as a number, but if it did the
    # parser rejects it: `evt-True` fails the integer parse. A doubled
    # separator cannot smuggle a sign, an empty tail is not a counter, and
    # a bare prefix is not an eventId.
    bad_event_ids = ("evt--1", f"evt-{U64_MAX + 1}", "evt-1.5", "evt-True", "evt-", "evt", "")

    def fresh_standard(event_id: Any, total_tokens: Any = 10) -> dict[str, Any]:
        return {
            "method": STANDARD_UPDATE_METHOD,
            "params": {
                "sessionId": "s-1",
                "update": {"sessionUpdate": "agent_message_chunk"},
                "_meta": {"eventId": event_id, "totalTokens": total_tokens},
            },
        }

    # Single-observe rejects on a fresh standard rail: negative /
    # overflowing / boolean event counters and totals.
    rail_reject_fixtures = [
        ("s--1", 10),
        (f"s-{U64_MAX + 1}", 10),
        ("s-1", -1),
        ("s-1", U64_MAX + 1),
        ("s-1", True),
    ]
    # Multi-observe rejects: the last observation of each sequence must
    # fail (a non-increasing counter; a decreasing totalTokens).
    rail_sequence_reject_fixtures = [
        [("s-5", 10), ("s-5", 10)],
        [("s-1", 20), ("s-2", 19)],
    ]
    # The valid u64 max accepts on a fresh rail; each fixture proves both
    # the counter and the total are exact u64 maxima, so each entry
    # contributes two u64 accepts below.
    rail_max_fixtures = [(f"s-{U64_MAX}", U64_MAX, U64_MAX, U64_MAX)]

    def fresh_extension(
        event_id: Any = ..., include_meta: bool = True, total_tokens: Any = None
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "sessionId": "s-1",
            "update": {"sessionUpdate": "subagent_progress"},
        }
        meta: dict[str, Any] = {}
        if event_id is not ...:
            meta["eventId"] = event_id
        if total_tokens is not None:
            meta["totalTokens"] = total_tokens
        if include_meta:
            params["_meta"] = meta
        return {"method": EXT_SESSION_NOTIFICATION_METHOD, "params": params}

    # Extension-rail eventId contract: `_meta` and the `eventId` key may be
    # ABSENT (transient progress — accepted and skipped), but a PRESENT
    # `eventId` must be a non-empty string whose counter parses through
    # `event_sequence` as a bounded u64. Every malformed present id below
    # must REJECT — the old code caught and skipped exactly these.
    # Each accept fixture is a distinct real case: (include_meta, event_id)
    # — no `_meta` at all, `_meta` present with no `eventId` key, and a
    # well-formed present `eventId`.
    extension_event_id_accept_fixtures = [
        # absent _meta entirely.
        (False, ...),
        # present _meta with no eventId key.
        (True, ...),
        # a well-formed present eventId folds its counter.
        (True, "s-1"),
    ]
    extension_event_id_reject_fixtures = [
        # counter overflow beyond u64.
        f"s-{U64_MAX + 1}",
        # bool eventId (non-string).
        True,
        # non-string eventId.
        7,
        # empty-string eventId.
        "",
        # doubled separator smuggling a negative counter.
        "s--1",
        # malformed text: no separator, non-digit tail, bare prefix.
        "no-separator-here",
        "s-1.5",
        "s-",
    ]
    # A transient extension notification may omit eventId while still
    # carrying totalTokens. That independently present value remains a
    # bounded, monotonic u64 and must never be skipped with the counter.
    extension_absent_event_total_accept_fixtures = [0, U64_MAX]
    extension_absent_event_total_reject_fixtures = [-1, U64_MAX + 1, True]
    extension_absent_event_total_sequence_reject_fixtures = [(10, 9)]

    # ------------------------------------------------------------------
    # Exact stub result fixtures (get / list_running / cancel), pinned to
    # the shim's own generated-contract shapes so casing or shape drift in
    # either direction fails this gate. Every fixture — good and wrong —
    # runs through the SAME shared method-discriminated validator
    # (`require_exact_stub_result`) that the live handshake uses, so the
    # self-test evidence is measured by the real contract check.
    # ------------------------------------------------------------------
    # x.ai/subagent/get missing id: the generated GetSubagentResponse is a
    # single nullable snapshot — no outcome wrapper, no id echo. The
    # list_running empty result is keyed `subagents` — never `running` and
    # never an outcome wrapper. x.ai/subagent/cancel missing id: the
    # direct camelCase CancelSubagentResponse echoes the requested id,
    # cancelled is false, and the outcome kind is not_found.
    get_audited_result = {"snapshot": None}
    list_audited_result = {"subagents": []}
    cancel_audited_result = {
        "subagentId": "missing-child-1",
        "cancelled": False,
        "outcome": {"kind": "not_found"},
    }
    stub_exact_fixtures = [
        ("x.ai/subagent/get missing-id result fixture", "x.ai/subagent/get", get_audited_result),
        (
            "x.ai/subagent/list_running empty-list result fixture",
            "x.ai/subagent/list_running",
            list_audited_result,
        ),
        (
            "x.ai/subagent/cancel missing-child result fixture",
            "x.ai/subagent/cancel",
            cancel_audited_result,
        ),
    ]
    # Drift guards: each wrong shape must be REJECTED BY THE SHARED
    # VALIDATOR ITSELF (a measured reject), so casing or shape drift in
    # either direction fails this gate. A wrong fixture is never counted
    # as evidence merely because it differs from the audited result.
    stub_drift_fixtures = [
        ("get", "x.ai/subagent/get", {"snapshot": None, "subagentId": "missing-child-1"}),
        ("get", "x.ai/subagent/get", {"outcome": {"kind": "not_found"}}),
        ("get", "x.ai/subagent/get", {"snapshot": {}}),
        ("get", "x.ai/subagent/get", {"running": []}),
        ("list_running", "x.ai/subagent/list_running", {"running": []}),
        ("list_running", "x.ai/subagent/list_running", {"subagents": None}),
        ("list_running", "x.ai/subagent/list_running", {"outcome": {"kind": "not_found"}}),
        # missing id echo.
        (
            "cancel",
            "x.ai/subagent/cancel",
            {"cancelled": False, "outcome": {"kind": "not_found"}},
        ),
        # missing outcome.
        (
            "cancel",
            "x.ai/subagent/cancel",
            {"subagentId": "missing-child-1", "cancelled": False},
        ),
        # wrong casing: snake_case id, not the direct camelCase result.
        (
            "cancel",
            "x.ai/subagent/cancel",
            {"subagent_id": "missing-child-1", "cancelled": False, "outcome": {"kind": "not_found"}},
        ),
        # wrong outcome kind.
        (
            "cancel",
            "x.ai/subagent/cancel",
            {"subagentId": "missing-child-1", "cancelled": False, "outcome": {"kind": "cancelled"}},
        ),
        # outcome-wrapper drift: the result is direct, not nested.
        ("cancel", "x.ai/subagent/cancel", {"result": dict(cancel_audited_result)}),
    ]
    stub_exact_fixtures = [
        (label, method, {"result": dto}) for label, method, dto in stub_exact_fixtures
    ]
    stub_drift_fixtures = [
        (label, method, {"result": dto}) for label, method, dto in stub_drift_fixtures
    ]
    stub_drift_fixtures.append(("missing envelope", "x.ai/subagent/get", get_audited_result))

    # ------------------------------------------------------------------
    # Execute every fixture through the REAL validators, counting each
    # accept/reject dynamically.
    # ------------------------------------------------------------------
    for update in finished_accept_fixtures:
        accepted(update)
    for update in finished_live_success_fixtures:
        require_live_success_dto(update)
    for update in finished_live_reject_fixtures:
        rejected_live(update)
    for update in finished_schema_reject_fixtures:
        rejected_exact(update)
    for update in spawned_accept_fixtures:
        spawned_accepted(update)
    for update in spawned_reject_fixtures:
        spawned_rejected(update)
    for update in progress_accept_fixtures:
        progress_accepted(update)
    for update in progress_reject_fixtures:
        progress_rejected(update)
    for message, parent in envelope_accept_fixtures:
        envelope_accepted(message, parent)
    for message, parent in envelope_reject_fixtures:
        envelope_rejected(message, parent)

    # Self-audit: the outcome separation itself. A well-formed failed DTO
    # must NOT be rejectable by the schema validator — that was the old
    # false-confidence bug — and the live check must reject exactly it.
    require_exact_finished_dto(failed_with_error)
    try:
        require_live_success_dto(failed_with_error)
    except AssertionError as error:
        require("not the completed outcome" in str(error), f"wrong live failure: {error}")
    else:
        raise AssertionError("live success check accepted a failed DTO")

    extracted = extract_subagent_lifecycle(lifecycle_batch, envelope_parent)
    require(extracted["spawned"] is not None, "extractor lost the spawned stage")
    require(extracted["progress"] is not None, "extractor lost the progress stage")
    require(extracted["finished"] is not None, "extractor lost the finished stage")
    require(extracted["task_tool_call"] is not None, "extractor lost the task tool_call")
    require(
        extracted["observation_index"]["task_tool_call"]
        < extracted["observation_index"]["spawned"]
        < extracted["observation_index"]["progress"]
        < extracted["observation_index"]["finished"],
        f"extractor chronology drifted: {extracted['observation_index']}",
    )
    # The exact live envelope is validated for all three lifecycle kinds:
    # one more real-envelope accept per extension-rail message in the
    # batch, counted from the batch fixture itself.
    extractor_envelope_accepts = sum(
        1
        for message in lifecycle_batch
        if message.get("method") == EXT_SESSION_NOTIFICATION_METHOD
    )
    counters["envelope_accepted"] += extractor_envelope_accepts
    for batch in extractor_reject_batches:
        high_water_rejected(
            lambda batch=batch: extract_subagent_lifecycle(batch, envelope_parent)
        )

    for value in u64_accept_values:
        u64_accepted(value)
    for value in u64_reject_values:
        u64_rejected(value)
    for event_id, expected_sequence in event_sequence_fixtures:
        measured_sequence = event_sequence(event_message(event_id))
        require(
            measured_sequence == expected_sequence,
            f"event_sequence fixture drift: {event_id}",
        )
        u64_accepted(measured_sequence)
    for bad_id in bad_event_ids:
        high_water_rejected(lambda bad_id=bad_id: event_sequence(event_message(bad_id)))

    # SessionHighWater.observe: the standard rail accepts a valid later
    # counter and an equal-or-later total.
    fresh = SessionHighWater()
    fresh.observe(fresh_standard("s-1", 10))
    fresh.observe(fresh_standard("s-2", 10))
    fresh.observe(fresh_standard("s-3", 11))
    require(fresh.last_counter["standard"] == 3, "standard counter fixture drift")
    require(fresh.last_total_tokens["standard"] == 11, "standard token fixture drift")
    # the valid u64 max accepts on a fresh rail.
    for event_id, total_tokens, expected_counter, expected_total in rail_max_fixtures:
        max_water = SessionHighWater()
        max_water.observe(fresh_standard(event_id, total_tokens))
        require(
            max_water.last_counter["standard"] == expected_counter,
            "max counter fixture drift",
        )
        require(
            max_water.last_total_tokens["standard"] == expected_total,
            "max token fixture drift",
        )
        u64_accepted(max_water.last_counter["standard"])
        u64_accepted(max_water.last_total_tokens["standard"])
    for event_id, total_tokens in rail_reject_fixtures:
        high_water_rejected(
            lambda event_id=event_id, total_tokens=total_tokens: SessionHighWater().observe(
                fresh_standard(event_id, total_tokens)
            )
        )

    # Extension-rail eventId contract, measured through the REAL observe:
    # absent `_meta`/`eventId` accepts and folds nothing; a well-formed
    # present eventId folds its counter; every malformed present id
    # REJECTS (never caught, never skipped).
    for include_meta, event_id in extension_event_id_accept_fixtures:
        rail = SessionHighWater()
        rail.observe(fresh_extension(event_id, include_meta=include_meta))
        if event_id is ...:
            require(
                rail.last_counter["extension"] is None,
                "absent extension eventId must not fold a counter",
            )
        else:
            require(
                rail.last_counter["extension"] is not None,
                f"well-formed extension eventId must fold: {event_id!r}",
            )
    for event_id in extension_event_id_reject_fixtures:
        high_water_rejected(
            lambda event_id=event_id: SessionHighWater().observe(fresh_extension(event_id))
        )
    for total_tokens in extension_absent_event_total_accept_fixtures:
        rail = SessionHighWater()
        rail.observe(fresh_extension(total_tokens=total_tokens))
        require(
            rail.last_counter["extension"] is None,
            "absent extension eventId must not invent a counter",
        )
        require(
            rail.last_total_tokens["extension"] == total_tokens,
            f"present extension totalTokens must fold without eventId: {total_tokens!r}",
        )
    for total_tokens in extension_absent_event_total_reject_fixtures:
        high_water_rejected(
            lambda total_tokens=total_tokens: SessionHighWater().observe(
                fresh_extension(total_tokens=total_tokens)
            )
        )
    for first, second in extension_absent_event_total_sequence_reject_fixtures:
        def observe_extension_totals(first: int = first, second: int = second) -> None:
            rail = SessionHighWater()
            rail.observe(fresh_extension(total_tokens=first))
            rail.observe(fresh_extension(total_tokens=second))

        high_water_rejected(observe_extension_totals)

    def observe_all(rail: SessionHighWater, sequence: list[tuple[Any, Any]]) -> None:
        for event_id, total_tokens in sequence:
            rail.observe(fresh_standard(event_id, total_tokens))

    for sequence in rail_sequence_reject_fixtures:
        high_water_rejected(
            lambda sequence=sequence: observe_all(SessionHighWater(), sequence)
        )
    # the extension rail keeps a separate high-water from the standard rail.
    split = SessionHighWater()
    split.observe(fresh_standard("s-9", 5))
    split.observe(
        {
            "method": EXT_SESSION_NOTIFICATION_METHOD,
            "params": {
                "sessionId": "s-1",
                "update": {"sessionUpdate": "subagent_progress"},
                "_meta": {"eventId": "s-1", "totalTokens": 1},
            },
        }
    )
    require(
        split.last_counter == {"standard": 9, "extension": 1},
        f"rails must keep separate counters; got {split.last_counter}",
    )
    require(
        split.last_total_tokens == {"standard": 5, "extension": 1},
        f"rails must keep separate token high-waters; got {split.last_total_tokens}",
    )
    # a missing totalTokens on the extension rail is simply not folded.
    no_tokens = SessionHighWater()
    no_tokens.observe(
        {
            "method": EXT_SESSION_NOTIFICATION_METHOD,
            "params": {
                "sessionId": "s-1",
                "update": {"sessionUpdate": "subagent_progress"},
                "_meta": {"eventId": "s-2"},
            },
        }
    )
    require(
        no_tokens.last_total_tokens["extension"] is None,
        "absent extension totalTokens must not be folded",
    )
    require(no_tokens.last_counter["extension"] == 2, "extension counter drift")
    # transient extension progress may omit metadata entirely.
    no_meta = SessionHighWater()
    no_meta.observe(
        {
            "method": EXT_SESSION_NOTIFICATION_METHOD,
            "params": {
                "sessionId": "s-1",
                "update": {"sessionUpdate": "subagent_progress"},
            },
        }
    )
    require(no_meta.last_counter["extension"] is None, "metadata-less progress must not fold")

    for what, method, audited_result in stub_exact_fixtures:
        # The good fixtures run through the SAME shared validator the live
        # handshake uses — an accept is only counted when the real
        # contract check passes, never by bare equality bookkeeping.
        try:
            stub_result_accepted(method, audited_result, what)
        except AssertionError as error:
            raise AssertionError(f"{what} must be accepted by the shared validator: {error}") from error
    for what, method, wrong_result in stub_drift_fixtures:
        # Each wrong fixture is a MEASURED reject: the shared validator
        # itself must reject it. The drift guard below is only a fixture
        # sanity check — it never counts as evidence on its own.
        require(
            wrong_result != {"result": STUB_METHOD_AUDITED_RESULTS[method]},
            f"{what} drift guard collided: {wrong_result}",
        )
        stub_result_rejected(method, wrong_result, what)

    # ------------------------------------------------------------------
    # Honest evidence: the expected totals below are derived from the
    # fixture collections above, and the measured counters must match.
    # ------------------------------------------------------------------
    expected_totals = {
        "finished_accepted": len(finished_accept_fixtures),
        "finished_rejected_schema": len(finished_schema_reject_fixtures),
        "finished_rejected_live_outcome": len(finished_live_reject_fixtures),
        "spawned_accepted": len(spawned_accept_fixtures),
        "spawned_rejected": len(spawned_reject_fixtures),
        "progress_accepted": len(progress_accept_fixtures),
        "progress_rejected": len(progress_reject_fixtures),
        "envelope_accepted": len(envelope_accept_fixtures) + extractor_envelope_accepts,
        "envelope_rejected": len(envelope_reject_fixtures),
        "u64_validator_accepted": len(u64_accept_values)
        + len(event_sequence_fixtures)
        + 2 * len(rail_max_fixtures),
        "u64_validator_rejected": len(u64_reject_values),
        "high_water_rejected": len(bad_event_ids)
        + len(extractor_reject_batches)
        + len(rail_reject_fixtures)
        + len(rail_sequence_reject_fixtures)
        + len(extension_event_id_reject_fixtures)
        + len(extension_absent_event_total_reject_fixtures)
        + len(extension_absent_event_total_sequence_reject_fixtures),
        "stub_result_accepted": len(stub_exact_fixtures),
        "stub_result_rejected": len(stub_drift_fixtures),
    }
    drifted = {
        key: (counters[key], expected)
        for key, expected in expected_totals.items()
        if counters[key] != expected
    }
    require(
        not drifted,
        f"validator self-test counters drifted from the executed fixtures "
        f"(measured, expected): {drifted}; measured: {counters}",
    )
    return counters


def probe_subagent(
    client: LeaderClient,
    session_id: str,
    high_water: SessionHighWater,
    graphql: str | None,
) -> dict[str, Any]:
    """Launch one real foreground subagent and prove the full wire + document contract.

    The turn asks the parent to spawn the `port-live-worker` subagent target
    in the foreground. From the actual wire envelopes the probe asserts:
    the early standard `task`-titled tool_call with
    `_meta.subagentBackground: false`; the exact live extension-rail
    `x.ai/session_notification` spawned/progress/finished lifecycle
    (snake_case variant fields under the camelCase `sessionId/update/_meta`
    envelope); child-session identity; the exact serde schemas of all three
    lifecycle DTOs (validated by `require_exact_spawned_dto`,
    `require_exact_progress_dto`, and `require_exact_finished_dto`, which
    accept any well-formed `completed|failed|cancelled` outcome); the
    finished DTO's required always-serialized `tokens_used` and
    `will_wake`, optional `error`/`output`, and forbidden
    `parent_session_id`; then — separately, because this is the live
    foreground SUCCESS edge — `status == "completed"` and
    `will_wake is False` via `require_live_success_dto`, so a well-formed
    failed or cancelled DTO is still rejected as a non-success outcome;
    the task terminal `tool_call_update`
    carrying the same tool call id; and — when a GraphQL endpoint is
    available — the durable parent/child request linkage. The task
    tool_call must precede the extension spawn in the observations,
    which is the real contract (the pager registers its blocking foreground
    wait from the standard tool_call before the subagent lifecycle begins).
    """
    prompt_text = (
        f"Use the spawn_subagent tool exactly once to spawn the subagent target named "
        f"'{SUBAGENT_MARKER}' with await_mode foreground and the prompt: 'Reply with exactly "
        f"one short sentence confirming the subagent worker ran.' Wait for the subagent to "
        f"finish in the foreground, then reply on one line beginning exactly "
        f"SUBAGENT_EDGE_DONE followed by the subagent's answer. Do not spawn more than one "
        f"subagent and do not use any other tool."
    )
    prompt_id = str(uuid.uuid4())
    response, notifications = client.request(
        "session/prompt",
        {
            "sessionId": session_id,
            "prompt": [{"type": "text", "text": prompt_text}],
            "_meta": {"promptId": prompt_id, "screenMode": "inline", "sendNow": True},
        },
    )
    require(response.get("result", {}).get("stopReason") == "end_turn", f"subagent prompt failed: {response}")
    result = validate_turn(notifications, prompt_id, "end_turn", high_water)
    result["prompt_id"] = prompt_id
    # Inline fixture self-test: prove the exact DTO validators themselves
    # against the authoritative serde shape BEFORE trusting them on the
    # live envelopes — completed/failed/cancelled outcome separation,
    # camelCase rejection, missing required fields, identity, and counter
    # bounds. This calls the REAL validators; no fields-only bypass exists.
    result["validator_self_test"] = self_test_subagent_lifecycle_validators()

    lifecycle = extract_subagent_lifecycle(notifications, session_id)

    # 1. The early standard-rail task tool_call with _meta.subagentBackground:false.
    task_call = lifecycle["task_tool_call"]
    require(
        task_call is not None,
        "subagent turn emitted no standard-rail tool_call titled task/Task/spawn_subagent "
        "(the pager-local foreground wait marker); observed standard updates: "
        f"{result['kinds']}",
    )
    assert task_call is not None
    task_call_id = task_call.get("toolCallId")
    require(
        isinstance(task_call_id, str) and task_call_id,
        f"task tool_call lacks a non-empty toolCallId; got: {task_call}",
    )
    require("meta" not in task_call, f"task tool_call leaked obsolete meta key: {task_call}")
    task_meta = task_call.get("_meta")
    require(
        isinstance(task_meta, dict),
        f"task tool_call lacks an _meta object carrying subagentBackground; got: {task_meta!r}",
    )
    require(
        task_meta.get("subagentBackground") is False,
        f"foreground task tool_call _meta.subagentBackground must be false; got: {task_meta!r}",
    )
    result["task_tool_call_id"] = task_call_id

    # 2. The exact live extension-rail spawned/progress/finished lifecycle.
    #    Every lifecycle stage was already validated against the exact outer
    #    `SessionNotification` envelope (method, params key set, parent
    #    session, object update) inside `extract_subagent_lifecycle` — the
    #    method assertions below confirm the exact live rail per stage.
    require(
        lifecycle["spawned"] is not None,
        "subagent turn emitted no subagent_spawned lifecycle event on the live "
        "extension rail (the exact outer SessionNotification envelope must ride "
        f"{EXT_SESSION_NOTIFICATION_METHOD}); "
        f"observed methods: {[sorted({m.get('method') for m in notifications})]}",
    )
    spawned = lifecycle["spawned"]
    require(
        spawned["method"] == EXT_SESSION_NOTIFICATION_METHOD,
        f"subagent_spawned must ride the exact live extension rail "
        f"{EXT_SESSION_NOTIFICATION_METHOD!r} (x.ai/session/update is only a "
        f"documented replay-path alias and is never accepted by this live "
        f"probe); got method {spawned['method']!r}",
    )
    spawned_update = spawned["update"]
    # Exact serde schema validation of the spawned DTO (snake_case variant
    # fields, camelCase envelope, no legacy names, no unknown extras).
    require_exact_spawned_dto(spawned_update)
    child_session_id = spawned_update["child_session_id"]
    require(
        spawned_update["parent_session_id"] == session_id,
        f"subagent_spawned parent_session_id must name the probe session; got: {spawned_update}",
    )
    result["child_session_id"] = child_session_id
    result["spawned_method"] = spawned["method"]

    # 3. Progress: present on the extension rail when the child ran long
    #    enough; transient progress may omit metadata entirely.
    if lifecycle["progress"] is not None:
        progress = lifecycle["progress"]
        require(
            progress["method"] == EXT_SESSION_NOTIFICATION_METHOD,
            f"subagent_progress must ride the exact live extension rail "
            f"{EXT_SESSION_NOTIFICATION_METHOD!r} (x.ai/session/update is only a "
            f"documented replay-path alias and is never accepted by this live "
            f"probe); got {progress['method']!r}",
        )
        progress_update = progress["update"]
        # Exact serde schema validation of the progress DTO (its field set is
        # fully required; counters bounded; percentage in [0, 100]).
        require_exact_progress_dto(progress_update)
        require(
            progress_update["subagent_id"] == child_session_id
            and progress_update["child_session_id"] == child_session_id,
            f"subagent_progress identity drifted from the spawned child session; got: {progress_update}",
        )
        require(
            progress_update["parent_session_id"] == session_id,
            f"subagent_progress parent_session_id must name the probe session; got: {progress_update}",
        )

    # 4. Finished: exact serde schema first, then this live foreground
    #    success edge's outcome. The schema validator accepts a well-formed
    #    failed/cancelled DTO; the separate outcome check requires
    #    status == "completed" and will_wake is False — so a valid failed
    #    DTO is recognized as well-formed yet still fails this live edge.
    require(
        lifecycle["finished"] is not None,
        "subagent turn emitted no subagent_finished lifecycle event on the live "
        "extension rail",
    )
    finished = lifecycle["finished"]
    require(
        finished["method"] == EXT_SESSION_NOTIFICATION_METHOD,
        f"subagent_finished must ride the exact live extension rail "
        f"{EXT_SESSION_NOTIFICATION_METHOD!r} (x.ai/session/update is only a "
        f"documented replay-path alias and is never accepted by this live "
        f"probe); got {finished['method']!r}",
    )
    finished_update = finished["update"]
    require(
        finished_update.get("child_session_id") == child_session_id,
        f"subagent_finished child_session_id {finished_update.get('child_session_id')!r} does not "
        f"match the spawned child session {child_session_id!r}",
    )
    require_exact_finished_dto(finished_update)
    require_live_success_dto(finished_update)

    # 5. The task terminal update with the same tool call id: a
    #    flattened `tool_call_update` on the standard rail carrying the
    #    terminal status for the task call id observed above.
    terminal_task_updates = [
        message
        for message in notifications
        if message.get("method") == STANDARD_UPDATE_METHOD
        and message.get("params", {}).get("update", {}).get("sessionUpdate") == "tool_call_update"
        and message.get("params", {}).get("update", {}).get("toolCallId") == task_call_id
    ]
    require(
        terminal_task_updates,
        f"no standard-rail tool_call_update carried the task tool call id {task_call_id!r}; "
        f"observed updates: {result['kinds']}",
    )
    for message in terminal_task_updates:
        status = message.get("params", {}).get("update", {}).get("status")
        require(
            status in ("completed", "failed"),
            f"task tool_call_update must carry a terminal status; got {status!r}",
        )

    # 6. Chronology: the task tool_call precedes the extension spawn in the
    #    observations — the real contract, because the pager registers its
    #    blocking foreground wait from the standard tool_call first.
    task_index = lifecycle["observation_index"]["task_tool_call"]
    spawned_index = lifecycle["observation_index"]["spawned"]
    require(
        task_index < spawned_index,
        f"the task tool_call (observation {task_index}) must precede the extension "
        f"subagent_spawned (observation {spawned_index}) in the observations",
    )
    if lifecycle["progress"] is not None:
        require(
            spawned_index < lifecycle["observation_index"]["progress"],
            "subagent_spawned must precede subagent_progress in the observations",
        )
    finished_index = lifecycle["observation_index"]["finished"]
    require(
        finished_index > spawned_index,
        "subagent_spawned must precede subagent_finished in the observations",
    )

    result["subagent_edge_observed"] = True
    result["finished_dto"] = {
        key: finished_update.get(key)
        for key in (
            "subagent_id",
            "child_session_id",
            "status",
            "tool_calls",
            "turns",
            "duration_ms",
            "tokens_used",
            "will_wake",
        )
    }
    result["progress_observed"] = lifecycle["progress"] is not None
    result["task_precedes_spawn"] = True

    # 7. Durable parent/child request linkage, when a GraphQL endpoint is
    #    available: the child AgentRequest links back to the parent request
    #    by caused_by_parent_request_id, and the spawn AgentToolCall carries
    #    the child_request_id on the parent's request.
    if graphql:
        documents = query_subagent_documents(graphql, session_id)
        child_requests = documents.get("child_requests", [])
        require(
            child_requests,
            "no durable child AgentRequest links back to this session's parent requests",
        )
        linked = [row for row in child_requests if row.get("session_id") == child_session_id]
        require(
            linked,
            f"no durable child AgentRequest carries the observed child session id "
            f"{child_session_id!r}; child rows: {child_requests}",
        )
        child_row = linked[0]
        parent_request_id = child_row.get("caused_by_parent_request_id")
        require(
            isinstance(parent_request_id, str) and parent_request_id,
            f"child AgentRequest lacks caused_by_parent_request_id; got: {child_row}",
        )
        require(
            child_row.get("behavior_id") == "port-live-worker",
            f"child AgentRequest behavior_id must be port-live-worker; got "
            f"{child_row.get('behavior_id')!r}",
        )
        spawn_calls = matching_spawn_calls(documents, child_row)
        require(
            spawn_calls,
            f"no durable spawn AgentToolCall links parent request {parent_request_id!r} to "
            f"child request {child_row.get('request_id')!r}; spawn rows: "
            f"{documents.get('spawn_tool_calls', [])}",
        )
        require(
            child_row.get("lifecycle_state") in ("completed", "interrupted", "failed", "dead", "superseded"),
            f"child AgentRequest lifecycle_state must be terminal; got "
            f"{child_row.get('lifecycle_state')!r}",
        )
        result["documents"] = {
            "child_request_id": child_row.get("request_id"),
            "parent_request_id": parent_request_id,
            "child_behavior_id": child_row.get("behavior_id"),
            "child_lifecycle_state": child_row.get("lifecycle_state"),
        }
    return result


def matching_spawn_calls(
    documents: dict[str, Any], child_row: dict[str, Any]
) -> list[dict[str, Any]]:
    """Return spawn rows proving the child's exact durable parent edge."""
    return [
        row
        for row in documents.get("spawn_tool_calls", [])
        if isinstance(row, dict)
        and row.get("tool_name") in ("spawn_subagent", "task", "Task")
        and row.get("request_id") == child_row.get("caused_by_parent_request_id")
        and row.get("child_request_id") == child_row.get("request_id")
    ]


def query_subagent_documents(
    endpoint: str,
    session_id: str,
    query_fn: Callable[..., dict[str, Any]] = graphql_query,
) -> dict[str, Any]:
    """Query the durable child/spawn rows correlated with one probe session."""
    escaped = graphql_escape(session_id)
    query = f"""{{
      SpawnToolCalls: AgentToolCall(filter: {{session_id: {{_eq: \"{escaped}\"}}}}, order: {{message_sequence: ASC}}, limit: 65) {{
        request_id tool_call_id tool_name child_request_id
      }}
    }}"""
    data = query_fn(endpoint, query, timeout=10)
    spawn_rows = data.get("SpawnToolCalls", [])
    require(isinstance(spawn_rows, list), "SpawnToolCalls query did not return a list")
    # Fetch one sentinel row beyond the accepted bound so exactly 64 calls is
    # distinguishable from a silently truncated result.
    require(len(spawn_rows) <= 64, "probe session exceeded the bounded tool-call query")
    child_ids = sorted(
        {
            row.get("child_request_id")
            for row in spawn_rows
            if isinstance(row, dict)
            and isinstance(row.get("child_request_id"), str)
            and row["child_request_id"]
        }
    )
    if not child_ids:
        return {"child_requests": [], "spawn_tool_calls": spawn_rows}

    child_literals = ", ".join(f'"{graphql_escape(child_id)}"' for child_id in child_ids)
    child_data = query_fn(
        endpoint,
        f'''{{ AgentRequest(filter: {{request_id: {{_in: [{child_literals}]}}}}, limit: 65) {{
          request_id session_id behavior_id lifecycle_state caused_by_parent_request_id
        }} }}''',
        timeout=10,
    )
    child_rows = child_data.get("AgentRequest")
    require(isinstance(child_rows, list), "AgentRequest query did not return a list")
    require(len(child_rows) <= 64, "child AgentRequest query exceeded its bounded result set")
    children = [row for row in child_rows if isinstance(row, dict)]
    returned_ids = [row.get("request_id") for row in children]
    require(
        all(isinstance(request_id, str) and request_id in child_ids for request_id in returned_ids),
        f"child AgentRequest query returned an unrequested id: {returned_ids}",
    )
    require(len(set(returned_ids)) == len(returned_ids), "duplicate child AgentRequest rows")
    require(set(returned_ids) == set(child_ids), "child AgentRequest query omitted a spawn child")
    return {"child_requests": children, "spawn_tool_calls": spawn_rows}


def self_test_subagent_document_query() -> dict[str, int]:
    """Exercise the bounded query and exact durable-parent correlation."""
    accepted = 0
    rejected = 0

    def expect_reject(run: Callable[[], Any]) -> None:
        nonlocal rejected
        try:
            run()
        except AssertionError:
            rejected += 1
        else:
            raise AssertionError("invalid subagent document query fixture was accepted")

    def spawn_row(index: int, child_id: str | None = None) -> dict[str, Any]:
        return {
            "request_id": f"parent-{index}",
            "tool_call_id": f"tool-{index}",
            "tool_name": "task",
            "child_request_id": child_id or f"child-{index}",
        }

    def child_row(index: int, request_id: str | None = None) -> dict[str, Any]:
        return {
            "request_id": request_id or f"child-{index}",
            "session_id": f"child-session-{index}",
            "behavior_id": "port-live-worker",
            "lifecycle_state": "completed",
            "caused_by_parent_request_id": f"parent-{index}",
        }

    def run_fixture(
        spawn_rows: list[dict[str, Any]],
        child_rows: list[dict[str, Any]],
        session_id: str = "session",
    ) -> tuple[dict[str, Any], list[str]]:
        calls: list[str] = []

        def query(_endpoint: str, document: str, *, timeout: float) -> dict[str, Any]:
            require(timeout == 10, "subagent query timeout drifted")
            calls.append(document)
            if "SpawnToolCalls" in document:
                require(graphql_escape(session_id) in document, "session id was not escaped")
                return {"SpawnToolCalls": spawn_rows}
            require("_in:" in document, "child requests must use one batched exact-id query")
            return {"AgentRequest": child_rows}

        return query_subagent_documents("fixture", session_id, query), calls

    escaped_child = 'child-"-\\'
    escaped_result, escaped_calls = run_fixture(
        [spawn_row(1, escaped_child), dict(spawn_row(1, escaped_child), tool_call_id="tool-2")],
        [child_row(1, escaped_child)],
        'session-"-\\',
    )
    require(len(escaped_calls) == 2, "deduplicated children must use one batched query")
    require(graphql_escape(escaped_child) in escaped_calls[1], "child id was not escaped")
    require(len(escaped_result["child_requests"]) == 1, "deduplicated child lookup drifted")
    accepted += 1

    rows_64 = [spawn_row(index) for index in range(64)]
    result_64, _ = run_fixture(rows_64, [child_row(index) for index in range(64)])
    require(len(result_64["child_requests"]) == 64, "64-child boundary must be accepted")
    accepted += 1

    expect_reject(
        lambda: query_subagent_documents(
            "fixture",
            "session",
            lambda _endpoint, _query, **_kwargs: {"SpawnToolCalls": rows_64 + [spawn_row(64)]},
        )
    )
    expect_reject(lambda: run_fixture([spawn_row(0)], [child_row(0), child_row(0)]))
    expect_reject(
        lambda: run_fixture([spawn_row(0)], [child_row(0, "unrequested-child")])
    )

    child = child_row(0)
    require(
        not matching_spawn_calls(
            {"spawn_tool_calls": [dict(spawn_row(0), request_id="wrong-parent")]}, child
        ),
        "a spawn from a different parent request must not prove the child edge",
    )
    require(
        matching_spawn_calls({"spawn_tool_calls": [spawn_row(0)]}, child),
        "the exact durable parent/tool/child edge must be accepted",
    )
    accepted += 1
    return {"accepted": accepted, "rejected": rejected}


def query_documents(endpoint: str, session_id: str) -> dict[str, Any]:
    escaped = graphql_escape(session_id)
    query = f"""{{
      AgentSession(filter: {{session_id: {{_eq: \"{escaped}\"}}}}) {{
        session_id behavior_id status started ended
      }}
      AgentRequest(filter: {{session_id: {{_eq: \"{escaped}\"}}}}, order: {{created_at: ASC}}) {{
        request_id content metadata status lifecycle_state terminalized_at interrupt_requested_at
      }}
      AgentResponse(filter: {{session_id: {{_eq: \"{escaped}\"}}}}, order: {{created_at: ASC}}) {{
        request_id status token_count completed_at interrupted_at error_message
      }}
      AgentMessage(filter: {{session_id: {{_eq: \"{escaped}\"}}}}, order: {{sequence: ASC}}) {{
        request_id sequence role content
      }}
      AgentToolCall(filter: {{session_id: {{_eq: \"{escaped}\"}}}}, order: {{started_at: ASC}}) {{
        request_id tool_call_id tool_name args result status lifecycle_state completed_at
      }}
    }}"""
    data = graphql_query(endpoint, query, timeout=10)
    require(data.get("AgentSession"), "no AgentSession document for probe session")
    return data


def query_documents_until_turns(
    endpoint: str,
    session_id: str,
    expected_turns: int,
    require_interrupted: bool = False,
    timeout_seconds: float = 5.0,
    query_documents_fn: Callable[[str, str], dict[str, Any]] = query_documents,
) -> dict[str, Any]:
    """Wait for request and response materialization for a completed wire turn."""
    deadline = time.monotonic() + timeout_seconds

    def ready(documents: dict[str, Any]) -> bool:
        cardinality_ready = (
            len(documents.get("AgentRequest", [])) == expected_turns
            and len(documents.get("AgentResponse", [])) == expected_turns
        )
        interruption_ready = not require_interrupted or (
            cardinality_ready
            and bool(documents["AgentRequest"][-1].get("interrupt_requested_at"))
            and bool(documents["AgentResponse"][-1].get("interrupted_at"))
        )
        return cardinality_ready and interruption_ready

    _ready, documents = poll_until_deadline(
        lambda: query_documents_fn(endpoint, session_id),
        ready,
        deadline=deadline,
        interval=0.05,
    )
    return documents


def self_test_document_polling() -> dict[str, int]:
    calls = 0
    incomplete = {"AgentRequest": [], "AgentResponse": []}

    def query_once(_endpoint: str, _session_id: str) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return incomplete

    observed = query_documents_until_turns(
        "http://unused.invalid/graphql",
        "session",
        expected_turns=1,
        timeout_seconds=0,
        query_documents_fn=query_once,
    )
    require(calls == 1, "edge deadline must retain its one final observation")
    require(observed is incomplete, "edge deadline did not return its latest snapshot")
    return {"accepted": 1, "rejected": 0}


def is_persisted_subprocess_probe(call: dict[str, Any]) -> bool:
    """Recognize the command marker only in the expected string columns."""
    args = call.get("args")
    result = call.get("result")
    return (
        isinstance(args, str)
        and "gents-subprocess-probe" in args
        and isinstance(result, str)
        and "gents-subprocess-probe" in result
    )


def self_test_persisted_subprocess_probe() -> dict[str, int]:
    marker = "gents-subprocess-probe"
    require(
        is_persisted_subprocess_probe({"args": marker, "result": marker}),
        "valid subprocess document marker was rejected",
    )
    invalid = (
        {"args": None, "result": marker},
        {"args": marker, "result": None},
        {"args": {"command": marker}, "result": [marker]},
    )
    require(
        not any(is_persisted_subprocess_probe(call) for call in invalid),
        "non-string subprocess document fields were accepted",
    )
    return {"accepted": 1, "rejected": len(invalid)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", help="Grok leader Unix socket (not needed for --edge offline)")
    parser.add_argument("--graphql", help="Optional Gents GraphQL endpoint for document assertions")
    parser.add_argument("--cwd", default=str(Path.cwd()), help="session/new cwd")
    parser.add_argument("--timeout", type=float, default=600.0, help="socket timeout seconds")
    parser.add_argument(
        "--model",
        default=os.environ.get("GENTS_GROK_PORT_MODEL", DEFAULT_MODEL),
        help="expected model id (default: GENTS_GROK_PORT_MODEL or pack default)",
    )
    parser.add_argument(
        "--context-window",
        type=int,
        default=int(os.environ.get("GENTS_GROK_PORT_CONTEXT_WINDOW", DEFAULT_CONTEXT_WINDOW)),
        help="expected advertised context window",
    )
    parser.add_argument(
        "--edge",
        choices=("offline", "handshake", "prompt", "tool", "subprocess", "subagent", "cancel", "all"),
        default="all",
    )
    parser.add_argument("--prompt", default="Reply with exactly: gents glm edge probe ok")
    args = parser.parse_args()

    if args.edge == "offline":
        print(json.dumps({
            "edge": "offline",
            "mcp_initialized_self_test": self_test_mcp_initialized_validator(),
            "validator_self_test": self_test_subagent_lifecycle_validators(),
            "subagent_document_query_self_test": self_test_subagent_document_query(),
            "subprocess_document_self_test": self_test_persisted_subprocess_probe(),
            "document_poll_self_test": self_test_document_polling(),
        }, indent=2, sort_keys=True))
        return 0
    require(args.socket, "--socket is required unless --edge offline is selected")

    started = time.monotonic()
    client = LeaderClient(args.socket, args.timeout, args.model)
    # One connection/session high-water per rail across the handshake and
    # every turn: within the standard (`session/update`) rail and within the
    # extension (`x.ai/session_notification`) rail, event counters must
    # strictly increase and observed totalTokens must never decrease. The
    # two rails are tracked separately — extension notifications may omit
    # totalTokens, and transient progress may omit metadata entirely, so no
    # single global counter may be demanded across both rails.
    high_water = SessionHighWater()
    output: dict[str, Any] = {
        "edge": args.edge,
        "socket": args.socket,
        "model": args.model,
        "context_window": args.context_window,
    }
    try:
        session_id, registered = initialize(client, args.cwd, args.context_window)
        output["session_id"] = session_id
        output["leader"] = {
            "client_id": registered["client_id"],
            "version": registered["leader_binary_version"],
        }
        if args.edge in ("handshake", "all"):
            output["handshake"] = probe_handshake(
                client, session_id, args.context_window, high_water
            )
        if args.edge in ("prompt", "all"):
            output["prompt"] = probe_prompt(client, session_id, args.prompt, high_water)
        if args.edge in ("tool", "all"):
            output["tool"] = probe_tool(client, session_id, high_water)
        if args.edge in ("subprocess", "all"):
            output["subprocess"] = probe_subprocess(client, session_id, high_water)
        if args.edge in ("subagent", "all"):
            output["subagent"] = probe_subagent(
                client, session_id, high_water, args.graphql
            )
        if args.edge in ("cancel", "all"):
            output["cancel"] = probe_cancel(client, session_id, high_water)
        if args.graphql:
            expected_turns = (
                int(args.edge in ("prompt", "all"))
                + int(args.edge in ("tool", "all"))
                + int(args.edge in ("subprocess", "all"))
                + int(args.edge in ("subagent", "all"))
                + int(args.edge in ("cancel", "all"))
            )
            documents = query_documents_until_turns(
                args.graphql,
                session_id,
                expected_turns,
                require_interrupted=args.edge in ("cancel", "all"),
            )
            requests = documents.get("AgentRequest", [])
            responses = documents.get("AgentResponse", [])
            require(len(requests) == expected_turns, "unexpected AgentRequest count")
            require(len(responses) == expected_turns, "unexpected AgentResponse count")
            if args.edge in ("cancel", "all"):
                require(requests[-1].get("interrupt_requested_at"), "cancel request lacks interrupt marker")
                require(responses[-1].get("interrupted_at"), "cancel response lacks interrupted marker")
            if args.edge in ("tool", "all"):
                require(documents.get("AgentToolCall"), "tool edge lacks AgentToolCall document")
            if args.edge in ("subprocess", "all"):
                subprocess_calls = [
                    call
                    for call in documents.get("AgentToolCall", [])
                    if is_persisted_subprocess_probe(call)
                ]
                require(
                    subprocess_calls,
                    "subprocess edge lacks persisted command and output",
                )
            output["documents"] = {
                "sessions": len(documents.get("AgentSession", [])),
                "requests": len(requests),
                "responses": len(responses),
                "messages": len(documents.get("AgentMessage", [])),
                "tool_calls": len(documents.get("AgentToolCall", [])),
            }
    finally:
        client.close()
    output["elapsed_seconds"] = round(time.monotonic() - started, 3)
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AssertionError, EOFError, OSError, ValueError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}), file=sys.stderr)
        raise SystemExit(1) from error
